// Command fleet-eval measures fusion accuracy against simulator ground
// truth for every source combination and several seeds, and prints a
// Markdown table. It is an evaluation tool, not part of the data path.
//
// Example:
//
//	fleet-eval -seeds 1,2,3 -vehicles 10 -samples 2000 -interval 10s
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/flavioanjo/fleetcore/internal/evaluation"
	"github.com/flavioanjo/fleetcore/internal/fusion"
	"github.com/flavioanjo/fleetcore/internal/simulator"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("fleet-eval", flag.ContinueOnError)
	fs.SetOutput(stderr)
	seeds := fs.String("seeds", "1,2,3,4,5", "comma-separated simulation seeds")
	vehicles := fs.Int("vehicles", 10, "vehicles per run")
	samples := fs.Int("samples", 2000, "rounds per vehicle")
	interval := fs.Duration("interval", 10*time.Second, "simulation interval")
	perturb := fs.Bool("perturb", false, "also run with delivery perturbations (reorder, duplicates, drops, late events)")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: fleet-eval [flags]")
		fmt.Fprintln(stderr, "Prints fusion accuracy against simulator ground truth as a Markdown table.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0 // help was requested: not a usage error
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "fleet-eval: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	var seedList []uint64
	for _, s := range strings.Split(*seeds, ",") {
		v, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
		if err != nil {
			fmt.Fprintf(stderr, "fleet-eval: -seeds: %v\n", err)
			return 2
		}
		seedList = append(seedList, v)
	}

	var results []evaluation.Result
	for _, combo := range evaluation.Combos() {
		for _, seed := range seedList {
			cfg := simulator.DefaultConfig()
			cfg.Seed, cfg.Vehicles, cfg.Samples, cfg.Interval, cfg.Sources = seed, *vehicles, *samples, *interval, combo
			scs := []evaluation.Scenario{{Name: fmt.Sprintf("seed %d", seed), Sim: cfg}}
			if *perturb {
				scs = append(scs, evaluation.Scenario{Name: fmt.Sprintf("seed %d perturbed", seed), Sim: cfg, Perturb: evaluation.DefaultPerturbation(seed)})
			}
			for _, sc := range scs {
				r, err := evaluation.Run(sc, fusion.DefaultConfig())
				if err != nil {
					fmt.Fprintf(stderr, "fleet-eval: %v\n", err)
					return 1
				}
				results = append(results, r)
			}
		}
	}
	fmt.Fprint(stdout, evaluation.FormatTable(results))
	return 0
}
