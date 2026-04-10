package flow

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// IterationConfig holds the configuration for an Iteration node.
type IterationConfig[I, O any] struct {
	// Processor is applied to every element in the input slice.
	// It receives the element's zero-based index and its value.
	Processor func(context.Context, int, I) (O, error)

	// ContinueOnError controls behaviour when an element's processor fails.
	// If false (default), the first error stops all further processing.
	// If true, all elements are processed and errors are stored per-result.
	ContinueOnError bool

	// ConcurrencyLimit controls how many elements are processed at once.
	//   -  0: sequential (default when not set)
	//   -  1: sequential
	//   - >1: concurrent with this many workers
	//   - <0: unlimited concurrency (one goroutine per element)
	ConcurrencyLimit int
}

// validate checks the configuration and applies defaults.
func (cfg *IterationConfig[I, O]) validate() error {
	if cfg == nil {
		return errors.New("iteration config cannot be nil")
	}

	if cfg.Processor == nil {
		return errors.New("processor cannot be nil")
	}

	// Default to sequential processing.
	if cfg.ConcurrencyLimit == 0 {
		cfg.ConcurrencyLimit = 1
	}

	return nil
}

var _ Node[[]any, []Result[any]] = (*Iteration[any, any])(nil)

// Iteration applies a processor to every element of an input slice, either
// sequentially or concurrently depending on ConcurrencyLimit.
//
// The output is a slice of Result values, one per input element, in the same
// order as the input regardless of processing order.
type Iteration[I, O any] struct {
	processor        func(context.Context, int, I) (O, error)
	continueOnError  bool
	concurrencyLimit int
}

// NewIteration creates an Iteration node from the given configuration.
// Returns an error if the configuration is invalid.
func NewIteration[I, O any](cfg IterationConfig[I, O]) (*Iteration[I, O], error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid iteration config: %w", err)
	}

	return &Iteration[I, O]{
		processor:        cfg.Processor,
		continueOnError:  cfg.ContinueOnError,
		concurrencyLimit: cfg.ConcurrencyLimit,
	}, nil
}

// effectiveConcurrency returns the actual number of concurrent workers to use,
// bounded by the number of elements to avoid spinning up unnecessary goroutines.
func (it *Iteration[I, O]) effectiveConcurrency(elements []I) int {
	if it.concurrencyLimit < 0 {
		return len(elements)
	}

	if it.concurrencyLimit == 0 {
		return 1
	}

	return min(it.concurrencyLimit, len(elements))
}

// runSequential processes elements one at a time in index order.
func (it *Iteration[I, O]) runSequential(ctx context.Context, elements []I) ([]Result[O], error) {
	results := make([]Result[O], len(elements))

	for index, element := range elements {
		result, err := it.processor(ctx, index, element)

		if err != nil && !it.continueOnError {
			return nil, fmt.Errorf("iteration failed at index %d: %w", index, err)
		}

		results[index] = Result[O]{
			Value: result,
			Error: err,
		}
	}

	return results, nil
}

// runConcurrent processes elements concurrently up to the given worker limit.
func (it *Iteration[I, O]) runConcurrent(ctx context.Context, elements []I, concurrency int) ([]Result[O], error) {
	results := make([]Result[O], len(elements))

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrency)

	for index, element := range elements {
		group.Go(func() error {
			result, err := it.processor(groupCtx, index, element)

			if err != nil && !it.continueOnError {
				return fmt.Errorf("iteration failed at index %d: %w", index, err)
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

// Run processes every element in the input slice and returns a result for each.
//
// Sequential or concurrent execution is chosen automatically based on
// ConcurrencyLimit. Results are returned in input order.
func (it *Iteration[I, O]) Run(ctx context.Context, input []I) ([]Result[O], error) {
	concurrency := it.effectiveConcurrency(input)

	if concurrency == 1 {
		return it.runSequential(ctx, input)
	}

	return it.runConcurrent(ctx, input, concurrency)
}
