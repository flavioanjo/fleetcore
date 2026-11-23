package device

import (
	"context"
	"time"

	"github.com/flavioanjo/fleetcore/internal/reliable"
)

// Latency summarizes durations.
type Latency struct {
	Count    uint64
	Sum, Max time.Duration
}

func (l *Latency) add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	l.Count++
	l.Sum += d
	if d > l.Max {
		l.Max = d
	}
}

// Mean is the average duration (0 without samples).
func (l Latency) Mean() time.Duration {
	if l.Count == 0 {
		return 0
	}
	return l.Sum / time.Duration(l.Count)
}

// Metrics are the device counters since Open. Queue contents (pending,
// failed, expired by class) come from the store and survive restarts; the
// counters below are per process.
type Metrics struct {
	Boots           uint64
	Queued          map[reliable.EventType]uint64
	QueueFull       uint64 // enqueue refused: capacity for the class exhausted
	EnqueueFailed   uint64 // enqueue transaction failed (storage)
	Transmissions   uint64 // event datagrams handed to the transport
	Retransmissions uint64 // transmissions of events already transmitted before
	SendErrors      uint64 // transport refused the datagram (offline)
	Heartbeats      uint64
	Acked           uint64 // events confirmed durably stored
	Failed          uint64 // events permanently rejected (kept as failed)
	Expired         uint64 // routine/diagnostic events expired (kept as expired)
	PurgedFailed    uint64 // failed/expired rows removed by the retention limit
	RetryAcks       uint64 // server answered "retry later"
	AcksUnmatched   uint64 // authenticated ACKs for unknown UIDs or other content
	AcksDuplicate   uint64 // ACKs for already finished events

	DownlinkRejected uint64 // failed authentication or decoding
	DownlinkStale    uint64 // authenticated for an earlier session
	StaleAttempts    uint64 // command attempts past their freshness bound

	CommandsReceived     uint64
	DuplicateDeliveries  uint64
	AuthorityRejected    uint64 // invalid command authority signature
	CommandsRejected     uint64
	CommandsSucceeded    uint64
	CommandsFailed       uint64
	CommandsUnknown      uint64
	InterruptedRecovered uint64
	StatusReReports      uint64
	Actuations           uint64 // actuator drives requested by this process

	CriticalQueueDelay Latency // enqueue → first transmission, critical events
	CriticalAckLatency Latency // enqueue → ACK, critical events
	AckLatency         Latency // enqueue → ACK, other events
}

// QueueState is the durable queue content.
type QueueState struct {
	Pending  map[reliable.Class]int
	Acked    int // retained acknowledged rows
	Failed   int
	Expired  int
	Attempts int // total attempts of pending events
}

// Metrics returns a copy of the process counters.
func (d *Device) Metrics() Metrics {
	d.mu.Lock()
	defer d.mu.Unlock()
	m := d.m
	m.Queued = map[reliable.EventType]uint64{}
	for k, v := range d.m.Queued {
		m.Queued[k] = v
	}
	return m
}

// Queue returns the durable queue content.
func (d *Device) Queue(ctx context.Context) (QueueState, error) {
	q := QueueState{Pending: map[reliable.Class]int{}}
	rows, err := d.store.db.R.QueryContext(ctx, `SELECT state, class, count(*), sum(attempts) FROM outbox GROUP BY state, class`)
	if err != nil {
		return q, err
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var class, n, attempts int64
		if err := rows.Scan(&state, &class, &n, &attempts); err != nil {
			return q, err
		}
		switch state {
		case "pending":
			q.Pending[reliable.Class(class)] += int(n)
			q.Attempts += int(attempts)
		case "acked":
			q.Acked += int(n)
		case "failed":
			q.Failed += int(n)
		case "expired":
			q.Expired += int(n)
		}
	}
	return q, rows.Err()
}

// PendingTotal is the number of pending events.
func (q QueueState) PendingTotal() int {
	n := 0
	for _, v := range q.Pending {
		n += v
	}
	return n
}

// EventState returns the queue state of one event ("pending", "acked",
// "failed", "expired") and its attempts, or ok=false if unknown (never
// queued, or purged by retention).
func (d *Device) EventState(ctx context.Context, uid uint64) (state string, attempts int, ok bool) {
	err := d.store.db.R.QueryRowContext(ctx, `SELECT state, attempts FROM outbox WHERE uid = ?`, int64(uid)).Scan(&state, &attempts)
	return state, attempts, err == nil
}

// CommandStage returns the device's recorded stage of a command.
func (d *Device) CommandStage(ctx context.Context, id reliable.CommandID) (reliable.Stage, reliable.Reason, bool) {
	var st, r int64
	err := d.store.db.R.QueryRowContext(ctx, `SELECT stage, reason FROM commands WHERE command_id = ?`, id[:]).Scan(&st, &r)
	return reliable.Stage(st), reliable.Reason(r), err == nil
}
