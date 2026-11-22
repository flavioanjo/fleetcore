// Command fleet-sim prints deterministic synthetic fleet telemetry as JSON
// Lines (one telemetry sample per line) on standard output.
//
// Example:
//
//	fleet-sim -seed 7 -vehicles 2 -samples 3 -interval 5s
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/flavioanjo/fleetcore/internal/simulator"
	"github.com/flavioanjo/fleetcore/internal/telemetry"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes the CLI and returns the process exit code:
// 0 success, 1 runtime failure, 2 usage error.
func run(args []string, stdout, stderr io.Writer) int {
	def := simulator.DefaultConfig()
	fs := flag.NewFlagSet("fleet-sim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	seed := fs.Uint64("seed", def.Seed, "random seed; equal flags produce identical output")
	vehicles := fs.Int("vehicles", def.Vehicles, fmt.Sprintf("number of simulated vehicles (1-%d)", simulator.MaxVehicles))
	samples := fs.Int("samples", def.Samples, "sampling rounds per vehicle; each round emits one sample per source")
	interval := fs.Duration("interval", def.Interval, "time between rounds (multiple of 100ms, 100ms-1h)")
	start := fs.String("start", def.Start.Format(time.RFC3339), "simulated start time (RFC 3339, UTC)")
	sources := fs.String("sources", "gps,can,pulse", "comma-separated sources to emit, in output order")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: fleet-sim [flags]")
		fmt.Fprintln(stderr, "Prints synthetic telemetry samples as JSON Lines.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "fleet-sim: unexpected argument %q\n", fs.Arg(0))
		return 2
	}

	cfg := simulator.Config{Seed: *seed, Vehicles: *vehicles, Samples: *samples, Interval: *interval}
	t, err := time.Parse(time.RFC3339, *start)
	if err != nil {
		fmt.Fprintf(stderr, "fleet-sim: -start: %v\n", err)
		return 2
	}
	cfg.Start = t.UTC()
	for _, name := range strings.Split(*sources, ",") {
		src, err := telemetry.ParseSource(strings.TrimSpace(name))
		if err != nil {
			fmt.Fprintf(stderr, "fleet-sim: -sources: %v\n", err)
			return 2
		}
		cfg.Sources = append(cfg.Sources, src)
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "fleet-sim: %v\n", err)
		return 2
	}

	w := bufio.NewWriter(stdout)
	enc := json.NewEncoder(w)
	if err := simulator.Run(cfg, func(s telemetry.Sample) error { return enc.Encode(s) }); err != nil {
		fmt.Fprintf(stderr, "fleet-sim: %v\n", err)
		return 1
	}
	if err := w.Flush(); err != nil {
		fmt.Fprintf(stderr, "fleet-sim: write output: %v\n", err)
		return 1
	}
	return 0
}
