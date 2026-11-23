// Package commandstore is the durable record of vehicle commands: who
// requested what for which device, whether it was authorized, every
// delivery attempt, every report the device sent back, and the resulting
// lifecycle state. It is the single source of truth for a command's
// outcome; the dispatcher (internal/dispatcher) drives it.
//
// # Lifecycle
//
//	requested ──► eligible ──► sent ──► received ──► accepted ──► succeeded
//	    │             │          │          │            │      ├► failed
//	    │             │          │          │            │      └► unknown
//	    │             │          │          └────────────┴──► rejected
//	    └─────────────┴──► expired   (deadline passed; never transmitted)
//	                  └──► unknown   (deadline passed; may have been transmitted)
//	sent/received/accepted ──► unknown (result deadline passed without a final report)
//	unknown ──► succeeded | failed | rejected (a late final report resolves it)
//
// Forward jumps are allowed (a COMMAND_RESULT "accepted" may arrive before
// "received"); backward moves are not. Reports are ranked
// (reliable.Stage.Rank) and a report not above the recorded rank is stale.
//
// # Uncertainty
//
// unknown is not failed: a command is unknown whenever the server cannot
// establish whether the device executed it (transmitted but never
// acknowledged, or acknowledged but no final result, or the device itself
// reported an interrupted execution). expired is reserved for commands
// that were provably never transmitted. Both set needs_attention.
//
// Device reports are the device's account of what happened. succeeded
// means the device read back the target state from its sensor; it is not
// an independent verification of the vehicle's physical state.
package commandstore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/flavioanjo/fleetcore/internal/reliable"
	"github.com/flavioanjo/fleetcore/internal/security"
	"github.com/flavioanjo/fleetcore/internal/sqlitedb"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

// State is a command lifecycle state.
type State string

// States.
const (
	Requested State = "requested" // durably requested and authorized
	Eligible  State = "eligible"  // may be transmitted (not expired; device not busy with another actuation)
	Sent      State = "sent"      // at least one delivery attempt was transmitted
	Received  State = "received"  // the device reported it durably recorded the command
	Accepted  State = "accepted"  // the device passed authorization and safety checks
	Rejected  State = "rejected"  // the device refused it; not executed (final)
	Succeeded State = "succeeded" // the device executed it and its sensor confirmed the effect (final)
	Failed    State = "failed"    // executed and not achieved (final)
	Unknown   State = "unknown"   // outcome cannot be established; needs attention
	Expired   State = "expired"   // deadline passed before any transmission; not executed (final)
)

// Final reports whether no further transition can happen (unknown may
// still be resolved by a late report, so it is not final).
func (s State) Final() bool {
	return s == Rejected || s == Succeeded || s == Failed || s == Expired
}

// inFlight states hold the device's single actuation slot.
var inFlight = []State{Eligible, Sent, Received, Accepted}

// allowed lists the permitted transitions.
var allowed = map[State][]State{
	Requested: {Eligible, Expired, Unknown},
	Eligible:  {Sent, Received, Accepted, Rejected, Succeeded, Failed, Unknown, Expired},
	Sent:      {Received, Accepted, Rejected, Succeeded, Failed, Unknown},
	Received:  {Accepted, Rejected, Succeeded, Failed, Unknown},
	Accepted:  {Succeeded, Failed, Unknown},
	Unknown:   {Rejected, Succeeded, Failed},
}

func canMove(from, to State) bool {
	for _, s := range allowed[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Errors.
var (
	ErrNotFound          = errors.New("commandstore: command not found")
	ErrUnauthorized      = errors.New("commandstore: principal not authorized for this command")
	ErrInvalidRequest    = errors.New("commandstore: invalid command request")
	ErrRequestConflict   = errors.New("commandstore: request id reused with different parameters")
	ErrInvalidTransition = errors.New("commandstore: invalid state transition")
	ErrClosed            = errors.New("commandstore: store is closed")
)

// Principal is an authenticated requester. Authenticating the principal
// (an operator console, an API token) is outside this package; it
// receives the identity and its roles from that layer.
type Principal struct {
	ID    string
	Roles []string
}

func (p Principal) has(role string) bool {
	for _, r := range p.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// Policy maps command types to the role required to request them, and
// bounds their time to live.
type Policy struct {
	Roles  map[reliable.CommandType]string
	MinTTL time.Duration
	MaxTTL map[reliable.CommandType]time.Duration
	// ResultGrace is how long after expiry a final report is still awaited
	// (and redelivery is used to poll for it) before the command becomes
	// unknown.
	ResultGrace time.Duration
	// RequireJustification lists types that need a non-empty justification.
	RequireJustification map[reliable.CommandType]bool
}

// DefaultPolicy is the reference policy: actuations need specific roles
// and a justification, and live briefly.
func DefaultPolicy() Policy {
	return Policy{
		Roles: map[reliable.CommandType]string{
			reliable.CommandBlock:           "security_operator",
			reliable.CommandUnblock:         "security_operator",
			reliable.CommandOpenCargo:       "cargo_operator",
			reliable.CommandRequestPosition: "fleet_viewer",
		},
		MinTTL: 5 * time.Second,
		MaxTTL: map[reliable.CommandType]time.Duration{
			reliable.CommandBlock: 15 * time.Minute, reliable.CommandUnblock: 15 * time.Minute,
			reliable.CommandOpenCargo: 5 * time.Minute, reliable.CommandRequestPosition: 5 * time.Minute,
		},
		ResultGrace: 2 * time.Minute,
		RequireJustification: map[reliable.CommandType]bool{
			reliable.CommandBlock: true, reliable.CommandUnblock: true, reliable.CommandOpenCargo: true,
		},
	}
}

// Request is a command request.
type Request struct {
	RequestID     string // idempotency key, unique per principal
	Principal     Principal
	DeviceID      string
	Type          reliable.CommandType
	Args          []byte
	TTL           time.Duration
	Justification string
}

// Options configures a Store.
type Options struct {
	BusyTimeout time.Duration
	Now         func() time.Time
	Policy      Policy
}

// Store is the SQLite command store. It is safe for concurrent use, also by
// several processes on one file.
type Store struct {
	db        *sqlitedb.DB
	w, r      *sql.DB
	now       func() time.Time
	policy    Policy
	authority *security.Authority

	mu     sync.RWMutex
	closed bool
}

// Open opens (creating if needed) the command database. The authority
// signs every command created through this store.
func Open(ctx context.Context, path string, authority *security.Authority, opts Options) (*Store, error) {
	if authority == nil {
		return nil, errors.New("commandstore: a command authority is required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Policy.Roles == nil {
		opts.Policy = DefaultPolicy()
	}
	db, err := sqlitedb.Open(ctx, "commandstore", path, sqlitedb.Options{BusyTimeout: opts.BusyTimeout, Now: opts.Now}, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, w: db.W, r: db.R, now: opts.Now, policy: opts.Policy, authority: authority}, nil
}

// Close closes the database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

func (s *Store) acquire() (func(), error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrClosed
	}
	return s.mu.RUnlock, nil
}

// Command is a stored command with its current lifecycle state.
type Command struct {
	reliable.Command
	RequestID      string
	Roles          []string
	Justification  string
	ResultDeadline time.Time
	AuthorityKeyID uint32
	Signature      []byte

	State          State
	StateReason    string
	NeedsAttention bool
	DeviceStage    reliable.Stage // highest stage the device reported (0 if none)
	DeviceReason   reliable.Reason
	Observed       uint8
	ResolvedLate   bool // a final report arrived after the command had become unknown
	Attempts       int
	NextAttempt    time.Time
	FirstSent      time.Time
	LastSent       time.Time
	ReceivedAt     time.Time
	DecidedAt      time.Time
	CompletedAt    time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func ms(t time.Time) int64 { return t.UnixMilli() }
func fromMs(v sql.NullInt64) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return time.UnixMilli(v.Int64).UTC()
}

// validPrincipal accepts identities that are unambiguous in the audit
// trail and in logs: non-empty, bounded, valid UTF-8, printable (no control
// characters that could forge audit or log lines) and without leading or
// trailing spaces (so "ops:alice" and "ops:alice " are not two principals).
func validPrincipal(id string) bool {
	if id == "" || len(id) > reliable.MaxPrincipal || !utf8.ValidString(id) || strings.TrimSpace(id) != id {
		return false
	}
	for _, r := range id {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func (s *Store) validate(req Request) error {
	switch {
	case req.RequestID == "" || len(req.RequestID) > 128:
		return fmt.Errorf("%w: request id must be 1..128 bytes", ErrInvalidRequest)
	case !validPrincipal(req.Principal.ID):
		return fmt.Errorf("%w: principal id must be 1..%d bytes of printable UTF-8 without surrounding spaces", ErrInvalidRequest, reliable.MaxPrincipal)
	case telemetry.DeviceID(req.DeviceID).Validate() != nil:
		return fmt.Errorf("%w: device id", ErrInvalidRequest)
	case !req.Type.Valid():
		return fmt.Errorf("%w: unknown command type", ErrInvalidRequest)
	case !reliable.ValidArgs(req.Type, req.Args):
		return fmt.Errorf("%w: invalid arguments for %s", ErrInvalidRequest, req.Type)
	case req.TTL < s.policy.MinTTL || req.TTL > s.policy.MaxTTL[req.Type]:
		return fmt.Errorf("%w: ttl %v outside [%v, %v] for %s", ErrInvalidRequest, req.TTL, s.policy.MinTTL, s.policy.MaxTTL[req.Type], req.Type)
	case s.policy.RequireJustification[req.Type] && strings.TrimSpace(req.Justification) == "":
		return fmt.Errorf("%w: %s requires a justification", ErrInvalidRequest, req.Type)
	}
	return nil
}

// Create records an authorized, signed command for req. It is idempotent
// on (principal, request id): repeating an identical request returns the
// existing command with created=false; reusing the id with different
// parameters fails with ErrRequestConflict. An unauthorized request is
// recorded in the denied-requests audit table and fails with
// ErrUnauthorized; it is never signed or stored as a command.
func (s *Store) Create(ctx context.Context, req Request) (Command, bool, error) {
	if err := s.validate(req); err != nil {
		return Command{}, false, err
	}
	release, err := s.acquire()
	if err != nil {
		return Command{}, false, err
	}
	defer release()
	now := s.now().UTC()

	role := s.policy.Roles[req.Type]
	if role == "" || !req.Principal.has(role) {
		_, derr := s.w.ExecContext(ctx, `INSERT INTO denied_requests (principal, request_id, device_id, command_type, reason, at_ns)
			VALUES (?, ?, ?, ?, ?, ?)`, req.Principal.ID, req.RequestID, req.DeviceID, int64(req.Type),
			fmt.Sprintf("missing role %q", role), now.UnixNano())
		return Command{}, false, errors.Join(fmt.Errorf("%w: %s requires role %q", ErrUnauthorized, req.Type, role), derr)
	}

	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return Command{}, false, err
	}
	defer tx.Rollback() //nolint:errcheck
	existing, err := getTx(ctx, tx, `principal = ? AND request_id = ?`, req.Principal.ID, req.RequestID)
	if err == nil {
		if existing.DeviceID != req.DeviceID || existing.Type != req.Type || string(existing.Args) != string(req.Args) {
			return Command{}, false, ErrRequestConflict
		}
		return existing, false, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Command{}, false, err
	}

	var id reliable.CommandID
	if _, err := rand.Read(id[:]); err != nil {
		return Command{}, false, err
	}
	issued := time.UnixMilli(now.UnixMilli()).UTC()
	rc := reliable.Command{ID: id, DeviceID: req.DeviceID, Type: req.Type, Args: append([]byte{}, req.Args...),
		IssuedAt: issued, ExpiresAt: issued.Add(req.TTL.Truncate(time.Millisecond)), Principal: req.Principal.ID}
	if err := rc.Check(); err != nil {
		return Command{}, false, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	sig := s.authority.Sign(rc.Statement())
	roles := append([]string(nil), req.Principal.Roles...)
	sort.Strings(roles)
	deadline := rc.ExpiresAt.Add(s.policy.ResultGrace)
	if _, err := tx.ExecContext(ctx, `INSERT INTO commands
		(seq, command_id, principal, request_id, roles, justification, device_id, command_type, args,
		 issued_at_ms, expires_at_ms, result_deadline_ms, authority_key_id, signature,
		 state, next_attempt_ms, created_at_ns, updated_at_ns)
		VALUES ((SELECT coalesce(max(seq), 0) + 1 FROM commands), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id[:], req.Principal.ID, req.RequestID, strings.Join(roles, ","), req.Justification, req.DeviceID,
		int64(req.Type), rc.Args, ms(rc.IssuedAt), ms(rc.ExpiresAt), ms(deadline), int64(s.authority.KeyID()), sig,
		string(Requested), ms(issued), now.UnixNano(), now.UnixNano()); err != nil {
		return Command{}, false, fmt.Errorf("commandstore: insert: %w", err)
	}
	if err := history(ctx, tx, id, "", Requested, "operator", fmt.Sprintf("requested by %s (%s)", req.Principal.ID, strings.Join(roles, ",")), now); err != nil {
		return Command{}, false, err
	}
	c, err := getTx(ctx, tx, `command_id = ?`, id[:])
	if err != nil {
		return Command{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Command{}, false, err
	}
	return c, true, nil
}

func history(ctx context.Context, tx *sql.Tx, id reliable.CommandID, from, to State, source, detail string, at time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO command_history (command_id, at_ns, from_state, to_state, source, detail)
		VALUES (?, ?, ?, ?, ?, ?)`, id[:], at.UnixNano(), string(from), string(to), source, detail)
	if err != nil {
		return fmt.Errorf("commandstore: history: %w", err)
	}
	return nil
}

const cmdCols = `command_id, principal, request_id, roles, justification, device_id, command_type, args,
	issued_at_ms, expires_at_ms, result_deadline_ms, authority_key_id, signature, state, state_reason,
	needs_attention, device_stage, attempts, next_attempt_ms, first_sent_ms, last_sent_ms, received_at_ms,
	decided_at_ms, completed_at_ms, device_reason, observed, resolved_late, created_at_ns, updated_at_ns`

type querier interface {
	QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row
	QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error)
}

type rowScanner interface{ Scan(dest ...any) error }

func scanCommand(row rowScanner) (Command, error) {
	var c Command
	var id, args, sig []byte
	var roles, state string
	var typ, issued, expires, deadline, keyID, attention, stage, next, resolved, created, updated int64
	var firstSent, lastSent, recv, decided, completed, devReason, observed sql.NullInt64
	err := row.Scan(&id, &c.Principal, &c.RequestID, &roles, &c.Justification, &c.DeviceID, &typ, &args,
		&issued, &expires, &deadline, &keyID, &sig, &state, &c.StateReason, &attention, &stage, &c.Attempts, &next,
		&firstSent, &lastSent, &recv, &decided, &completed, &devReason, &observed, &resolved, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ErrNotFound
	}
	if err != nil {
		return c, err
	}
	copy(c.ID[:], id)
	c.Type, c.Args, c.Signature = reliable.CommandType(typ), args, sig
	if roles != "" {
		c.Roles = strings.Split(roles, ",")
	}
	c.IssuedAt, c.ExpiresAt, c.ResultDeadline = time.UnixMilli(issued).UTC(), time.UnixMilli(expires).UTC(), time.UnixMilli(deadline).UTC()
	c.AuthorityKeyID, c.State, c.NeedsAttention = uint32(keyID), State(state), attention == 1
	c.DeviceStage = reliable.Stage(stage)
	c.DeviceReason, c.Observed, c.ResolvedLate = reliable.Reason(devReason.Int64), uint8(observed.Int64), resolved == 1
	c.NextAttempt = time.UnixMilli(next).UTC()
	c.FirstSent, c.LastSent, c.ReceivedAt = fromMs(firstSent), fromMs(lastSent), fromMs(recv)
	c.DecidedAt, c.CompletedAt = fromMs(decided), fromMs(completed)
	c.CreatedAt, c.UpdatedAt = time.Unix(0, created).UTC(), time.Unix(0, updated).UTC()
	return c, nil
}

func getTx(ctx context.Context, q querier, where string, args ...any) (Command, error) {
	return scanCommand(q.QueryRowContext(ctx, `SELECT `+cmdCols+` FROM commands WHERE `+where, args...))
}

// Get returns a command.
func (s *Store) Get(ctx context.Context, id reliable.CommandID) (Command, error) {
	release, err := s.acquire()
	if err != nil {
		return Command{}, err
	}
	defer release()
	return getTx(ctx, s.r, `command_id = ?`, id[:])
}

// Filter selects commands for List.
type Filter struct {
	DeviceID       string
	States         []State
	NeedsAttention bool
	Limit          int
}

// List returns commands matching f, oldest first.
func (s *Store) List(ctx context.Context, f Filter) ([]Command, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	var where []string
	var args []any
	if f.DeviceID != "" {
		where, args = append(where, `device_id = ?`), append(args, f.DeviceID)
	}
	if len(f.States) > 0 {
		where = append(where, `state IN (?`+strings.Repeat(`, ?`, len(f.States)-1)+`)`)
		for _, st := range f.States {
			args = append(args, string(st))
		}
	}
	if f.NeedsAttention {
		where = append(where, `needs_attention = 1`)
	}
	q := `SELECT ` + cmdCols + ` FROM commands`
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, ` AND `)
	}
	if f.Limit <= 0 {
		f.Limit = 1000
	}
	q += ` ORDER BY seq LIMIT ?`
	return listQ(ctx, s.r, q, append(args, f.Limit)...)
}

func listQ(ctx context.Context, q querier, query string, args ...any) ([]Command, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Command
	for rows.Next() {
		c, err := scanCommand(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// HistoryEntry is one recorded transition.
type HistoryEntry struct {
	At             time.Time
	From, To       State
	Source, Detail string
}

// History returns a command's transitions in order.
func (s *Store) History(ctx context.Context, id reliable.CommandID) ([]HistoryEntry, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	rows, err := s.r.QueryContext(ctx, `SELECT at_ns, from_state, to_state, source, detail FROM command_history
		WHERE command_id = ? ORDER BY id`, id[:])
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []HistoryEntry
	for rows.Next() {
		var h HistoryEntry
		var at int64
		var from, to string
		if err := rows.Scan(&at, &from, &to, &h.Source, &h.Detail); err != nil {
			return nil, err
		}
		h.At, h.From, h.To = time.Unix(0, at).UTC(), State(from), State(to)
		out = append(out, h)
	}
	return out, rows.Err()
}

// Attempt is one recorded delivery attempt.
type Attempt struct {
	N         int
	StartedAt time.Time
	Outcome   string
	Transport string
	Detail    string
}

// Attempts returns a command's delivery attempts.
func (s *Store) Attempts(ctx context.Context, id reliable.CommandID) ([]Attempt, error) {
	release, err := s.acquire()
	if err != nil {
		return nil, err
	}
	defer release()
	rows, err := s.r.QueryContext(ctx, `SELECT attempt, started_at_ms, outcome, transport, detail FROM command_attempts
		WHERE command_id = ? ORDER BY attempt`, id[:])
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		var at int64
		if err := rows.Scan(&a.N, &at, &a.Outcome, &a.Transport, &a.Detail); err != nil {
			return nil, err
		}
		a.StartedAt = time.UnixMilli(at).UTC()
		out = append(out, a)
	}
	return out, rows.Err()
}

// DeniedCount returns the number of audited denied requests.
func (s *Store) DeniedCount(ctx context.Context) (int, error) {
	var n int
	err := s.r.QueryRowContext(ctx, `SELECT count(*) FROM denied_requests`).Scan(&n)
	return n, err
}
