package flow

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"golang.org/x/sync/errgroup"
)

// ParallelConfig holds the configuration for a Parallel node.
type ParallelConfig[I, O any] struct {
	// Processors is the list of functions to execute concurrently.
	// Every processor receives the same input and runs independently.
	Processors []func(context.Context, I) (O, error)

	// ContinueOnError controls behaviour when a processor fails.
	// If false (default), the first error cancels all remaining processors.
	// If true, all processors run to completion and errors are stored per-result.
	ContinueOnError bool

	// ConcurrencyLimit caps the number of processors running at once.
	//   - 0 or negative: no limit (all processors start simultaneously)
	//   - positive: at most this many processors run concurrently
	ConcurrencyLimit int
}

// validate checks that the configuration is usable.
func (cfg *ParallelConfig[I, O]) validate() error {
	if cfg == nil {
		return errors.New("parallel config cannot be nil")
	}

	if len(cfg.Processors) == 0 {
		return errors.New("at least one processor is required")
	}

	return nil
}

var _ Node[any, []Result[any]] = (*Parallel[any, any])(nil)

// Parallel executes multiple processors concurrently, each receiving the same
// input and producing an independent result.
//
// The output is a slice of Result values, one per processor, preserving the
// original order regardless of completion order.
type Parallel[I, O any] struct {
	processors       []func(context.Context, I) (O, error)
	continueOnError  bool
	concurrencyLimit int
}

// NewParallel creates a Parallel node from the given configuration.
// Returns an error if the configuration is invalid.
func NewParallel[I, O any](cfg ParallelConfig[I, O]) (*Parallel[I, O], error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid parallel config: %w", err)
	}

	return &Parallel[I, O]{
		processors:       slices.Clone(cfg.Processors),
		continueOnError:  cfg.ContinueOnError,
		concurrencyLimit: cfg.ConcurrencyLimit,
	}, nil
}

// effectiveConcurrency returns the concurrency level to use for this run,
// capped at the number of processors so we never spin up unnecessary goroutines.
func (p *Parallel[I, O]) effectiveConcurrency() int {
	if p.concurrencyLimit <= 0 {
		return len(p.processors)
	}

	return min(p.concurrencyLimit, len(p.processors))
}

// Run executes all processors concurrently and collects their results.
//
// Results are returned in the same order as the processors, regardless of
// which goroutine finishes first.
// If ContinueOnError is false, the first failure cancels remaining processors
// and the error is returned directly; no partial result slice is produced.
func (p *Parallel[I, O]) Run(ctx context.Context, input I) ([]Result[O], error) {
	results := make([]Result[O], len(p.processors))

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(p.effectiveConcurrency())

	for index, processor := range p.processors {
		group.Go(func() error {
			result, err := processor(groupCtx, input)

			if err != nil && !p.continueOnError {
				return fmt.Errorf("processor %d failed: %w", index, err)
			}

			results[index] = Result[O]{
				Value: result,
				Error: err,
			}

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	return results, nil
}
