// Command fleet-fusion reads telemetry samples as JSON Lines (the format
// fleet-sim writes), runs them through the fusion engine and writes
// structured JSON Lines results.
//
// Every output line is an object with a "type" field:
//
//	event        the disposition of one input sample (see -events)
//	estimate     a fused distance estimate for one epoch
//	input_error  an input line that could not be decoded (never dropped silently)
//	summary      per-device totals at the end of the input (with -summary)
//
// Exit status: 0 on success, 1 if any input line could not be decoded or
// on I/O failure, 2 on usage errors.
//
// Example:
//
//	fleet-sim -vehicles 2 -samples 100 | fleet-fusion -summary
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/flavioanjo/fleetcore/internal/fusion"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

type inputError struct {
	Type  string `json:"type"`
	Line  int    `json:"line"`
	Error string `json:"error"`
}

type deviceSummary struct {
	Type          string                 `json:"type"`
	DeviceID      telemetry.DeviceID     `json:"device_id"`
	TripMM        telemetry.Millimetres  `json:"trip_mm"`
	UncertaintyMM telemetry.Millimetres  `json:"uncertainty_mm"`
	Confidence    fusion.Confidence      `json:"confidence"`
	Estimates     int                    `json:"estimates"`
	Outcomes      map[fusion.Outcome]int `json:"outcomes"`
	Warnings      map[fusion.Code]int    `json:"warnings"`
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	def := fusion.DefaultConfig()
	fs := flag.NewFlagSet("fleet-fusion", flag.ContinueOnError)
	fs.SetOutput(stderr)
	in := fs.String("in", "-", "input file, or - for standard input")
	eventsMode := fs.String("events", "issues", "event reports to write: all, issues (anything but plain in-order acceptance) or none")
	summary := fs.Bool("summary", false, "write one summary line per device at the end")
	strict := fs.Bool("strict", false, "stop at the first undecodable input line")
	window := fs.Duration("window", def.ReorderWindow, "reorder window (device time)")
	maxBuf := fs.Int("max-buffer", def.MaxBufferedPerSession, "reorder buffer capacity per device session")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: fleet-fusion [flags] < samples.jsonl")
		fmt.Fprintln(stderr, "Fuses telemetry samples (JSON Lines) and writes events and estimates as JSON Lines.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "fleet-fusion: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	switch *eventsMode {
	case "all", "issues", "none":
	default:
		fmt.Fprintf(stderr, "fleet-fusion: -events must be all, issues or none\n")
		return 2
	}

	cfg := def
	cfg.ReorderWindow, cfg.MaxBufferedPerSession = *window, *maxBuf
	eng, err := fusion.New(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "fleet-fusion: %v\n", err)
		return 2
	}

	r := stdin
	if *in != "-" {
		f, err := os.Open(*in)
		if err != nil {
			fmt.Fprintf(stderr, "fleet-fusion: %v\n", err)
			return 1
		}
		defer f.Close()
		r = f
	}

	w := bufio.NewWriter(stdout)
	enc := json.NewEncoder(w)
	sums := map[telemetry.DeviceID]*deviceSummary{}
	sum := func(id telemetry.DeviceID) *deviceSummary {
		s := sums[id]
		if s == nil {
			s = &deviceSummary{Type: "summary", DeviceID: id, Outcomes: map[fusion.Outcome]int{}, Warnings: map[fusion.Code]int{}, Confidence: fusion.ConfidenceNone}
			sums[id] = s
		}
		return s
	}
	var writeErr error
	write := func(v any) {
		if writeErr == nil {
			writeErr = enc.Encode(v)
		}
	}
	emit := func(rs []fusion.Report) {
		for _, rep := range rs {
			if rep.Type == fusion.ReportEstimate {
				est := rep.Estimate
				s := sum(est.DeviceID)
				s.TripMM, s.UncertaintyMM, s.Confidence = est.TripMM, est.UncertaintyMM, est.Confidence
				s.Estimates++
				countWarnings(s, est.Diagnostics)
				write(rep)
				continue
			}
			ev := rep.Event
			s := sum(ev.DeviceID)
			if ev.Outcome != fusion.OutcomeBuffered {
				s.Outcomes[ev.Outcome]++
			}
			countWarnings(s, ev.Diagnostics)
			if *eventsMode == "all" || (*eventsMode == "issues" && (ev.Outcome != fusion.OutcomeAccepted || len(ev.Diagnostics) > 0)) {
				write(rep)
			}
		}
	}

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	line, inputErrors := 0, 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if line == 1 {
			b = bytes.TrimPrefix(b, []byte("\xef\xbb\xbf")) // UTF-8 BOM added by some shells
		}
		if len(bytes.TrimSpace(b)) == 0 {
			continue
		}
		var s telemetry.Sample
		if err := json.Unmarshal(b, &s); err != nil {
			inputErrors++
			write(inputError{Type: "input_error", Line: line, Error: err.Error()})
			if *strict {
				break
			}
			continue
		}
		emit(eng.Process(s)) // validation failures come back as rejected event reports
	}
	if err := sc.Err(); err != nil {
		inputErrors++
		write(inputError{Type: "input_error", Line: line + 1, Error: err.Error()})
	}
	emit(eng.Flush())
	if *summary {
		ids := make([]telemetry.DeviceID, 0, len(sums))
		for id := range sums {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			write(sums[id])
		}
	}
	if writeErr == nil {
		writeErr = w.Flush()
	}
	if writeErr != nil {
		fmt.Fprintf(stderr, "fleet-fusion: write output: %v\n", writeErr)
		return 1
	}
	if inputErrors > 0 {
		fmt.Fprintf(stderr, "fleet-fusion: %d input line(s) could not be decoded (reported as input_error)\n", inputErrors)
		return 1
	}
	return 0
}

func countWarnings(s *deviceSummary, ds []fusion.Diagnostic) {
	for _, d := range ds {
		if d.Level == fusion.LevelWarning {
			s.Warnings[d.Code]++
		}
	}
}
