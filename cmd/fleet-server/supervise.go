package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"time"
)

// service is an essential part of the server (a gateway, the command
// dispatcher, the alerting consumer). run must return when ctx is
// cancelled, after finishing or safely abandoning its work in progress;
// returning at any other time, with or without an error, is a failure.
type service struct {
	name string
	run  func(ctx context.Context) error
}

// ServiceError reports the essential service whose failure stopped the
// server.
type ServiceError struct {
	Service string
	Err     error
}

func (e *ServiceError) Error() string { return e.Service + ": " + e.Err.Error() }
func (e *ServiceError) Unwrap() error { return e.Err }

// errStoppedUnexpectedly is the failure of a service that returned nil
// although no stop was requested.
var errStoppedUnexpectedly = errors.New("stopped without a stop request")

// supervise runs every service until ctx is cancelled (orderly stop) or one
// of them stops on its own (failure: a returned error, a panic, or a
// return without a stop request). In both cases it cancels the context of
// all services and waits for every one of them to return, so that the
// caller can close the stores they use afterwards.
//
// It returns the first failure (nil after an orderly stop in which every
// service returned nil) and whether all services returned. If some did not
// return within grace of the stop, they are still using the stores: the
// caller must not close them, and must exit with a failure status.
func supervise(ctx context.Context, log *slog.Logger, grace time.Duration, services []service) (failure error, allStopped bool) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		name string
		err  error
	}
	results := make(chan result, len(services))
	running := map[string]bool{}
	for _, s := range services {
		running[s.name] = true
		go func(s service) {
			var err error
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("panic: %v\n%s", p, debug.Stack())
				}
				results <- result{s.name, err}
			}()
			err = s.run(runCtx)
		}(s)
	}

	stopRequested := ctx.Done()
	var deadline <-chan time.Time
	stopping := false
	beginStop := func() {
		stopping = true
		stopRequested = nil
		cancel()
		deadline = time.After(grace)
	}
	for len(running) > 0 {
		select {
		case r := <-results:
			delete(running, r.name)
			switch {
			case !stopping:
				err := r.err
				if err == nil {
					err = errStoppedUnexpectedly
				}
				failure = &ServiceError{Service: r.name, Err: err}
				log.Error("essential service failed; stopping the server", "service", r.name, "err", err)
				beginStop()
			case r.err != nil:
				log.Error("service failed while stopping", "service", r.name, "err", r.err)
				if failure == nil {
					failure = &ServiceError{Service: r.name, Err: r.err}
				}
			default:
				log.Info("service stopped", "service", r.name)
			}
		case <-stopRequested:
			log.Info("stop requested; finishing work in progress")
			beginStop()
		case <-deadline:
			names := make([]string, 0, len(running))
			for n := range running {
				names = append(names, n)
			}
			sort.Strings(names)
			log.Error("services did not stop within the grace period; their stores are left open", "services", names, "grace", grace)
			if failure == nil {
				failure = fmt.Errorf("services did not stop within %v: %v", grace, names)
			}
			return failure, false
		}
	}
	return failure, true
}
