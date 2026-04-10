package flow

import (
	"context"
	"errors"
	"fmt"
)

// DefaultMaxIterations is the iteration cap applied when LoopConfig.MaxIterations
// is left at zero. It prevents accidental infinite loops during development.
const DefaultMaxIterations = 1024

// LoopConfig holds the configuration for a Loop node.
type LoopConfig[T any] struct {
	// Processor is called on every iteration. It receives the current iteration
	// index (zero-based) and the value produced by the previous iteration (or the
	// initial input on the first call). It returns:
	//   - output: the value to pass into the next iteration
	//   - done:   true to stop the loop and return output
	//   - err:    a non-nil error to abort immediately
	Processor func(ctx context.Context, iteration int, input T) (output T, done bool, err error)

	// MaxIterations caps the total number of iterations to prevent infinite loops.
	//   -  0: uses DefaultMaxIterations (1024)
	//   - >0: custom cap
	//   - <0: invalid, returns an error
	MaxIterations int
}

// validate checks the configuration and applies defaults.
func (cfg *LoopConfig[T]) validate() error {
	if cfg == nil {
		return errors.New("loop config cannot be nil")
	}

	if cfg.Processor == nil {
		return errors.New("loop processor cannot be nil")
	}

	if cfg.MaxIterations < 0 {
		return errors.New("max iterations must not be negative")
	}

	if cfg.MaxIterations == 0 {
		cfg.MaxIterations = DefaultMaxIterations
	}

	return nil
}

var _ Node[any, any] = (*Loop[any])(nil)

// Loop repeatedly applies its processor to a value until the processor signals
// completion or the iteration cap is reached.
type Loop[T any] struct {
	processor     func(context.Context, int, T) (T, bool, error)
	maxIterations int
}

// NewLoop creates a Loop node from the given configuration.
// Returns an error if the configuration is invalid.
func NewLoop[T any](cfg LoopConfig[T]) (*Loop[T], error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid loop config: %w", err)
	}

	return &Loop[T]{
		processor:     cfg.Processor,
		maxIterations: cfg.MaxIterations,
	}, nil
}

// Run executes the loop starting from the given input value.
//
// The loop terminates when:
//   - the processor returns done = true (normal completion)
//   - the processor returns a non-nil error (immediate abort)
//   - MaxIterations is reached without the processor returning done = true
func (l *Loop[T]) Run(ctx context.Context, input T) (T, error) {
	var (
		iteration int
		current   = input
		done      bool
		err       error
	)

	for iteration < l.maxIterations {
		current, done, err = l.processor(ctx, iteration, current)
		if err != nil {
			return current, fmt.Errorf("loop failed at iteration %d: %w", iteration, err)
		}

		if done {
			return current, nil
		}

		iteration++
	}

	return current, fmt.Errorf(
		"loop exceeded max iterations (%d): termination condition not met",
		l.maxIterations,
	)
}
