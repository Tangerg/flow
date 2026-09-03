package flow

import (
	"context"
	"fmt"
)

// DefaultMaxIterations caps a [Loop] when no limit is configured, guarding
// against an accidental infinite loop.
const DefaultMaxIterations = 1024

// LoopConfig configures [Loop]. Its zero value uses [DefaultMaxIterations].
type LoopConfig struct {
	// MaxIterations caps the number of iterations. Zero uses
	// [DefaultMaxIterations]; negative values are invalid.
	MaxIterations int
}

// Validate rejects a negative MaxIterations; zero means [DefaultMaxIterations].
func (c LoopConfig) Validate() error {
	return nonNegativeCount("max iterations", c.MaxIterations)
}

// Loop repeatedly applies body to a value until body reports done, ctx is
// cancelled, or the iteration cap is reached.
//
// body receives the zero-based iteration index and the value from the previous
// iteration (or the initial input on the first call). It returns the next value,
// a done flag, and an error. On error, Loop returns the value from before the
// failing iteration. If the context is cancelled before an iteration commits,
// cancellation takes precedence and that iteration is likewise rolled back.
// Reaching the cap without done returns an error wrapping [ErrMaxIterations]. A
// zero [LoopConfig] uses [DefaultMaxIterations].
func Loop[T any](
	body func(ctx context.Context, iter int, in T) (out T, done bool, err error),
	cfg LoopConfig,
) Node[T, T] {
	maxIterations := cfg.MaxIterations
	if maxIterations == 0 {
		maxIterations = DefaultMaxIterations
	}
	return loopNode[T]{body: body, maxIterations: maxIterations}
}

type loopNode[T any] struct {
	body          func(context.Context, int, T) (T, bool, error)
	maxIterations int
}

func (l loopNode[T]) Run(ctx context.Context, in T) (T, error) {
	cur := in
	if err := l.Validate(); err != nil {
		return cur, err
	}
	for i := range l.maxIterations {
		if err := context.Cause(ctx); err != nil {
			return cur, err
		}
		next, done, err := l.body(ctx, i, cur)
		// Cancellation may race with the body returning. It wins before this
		// iteration is committed, matching Map's parent-cancellation rule and
		// preserving the last completed value.
		if contextErr := context.Cause(ctx); contextErr != nil {
			return cur, contextErr
		}
		if err != nil {
			return cur, err
		}
		cur = next
		if done {
			return cur, nil
		}
	}
	return cur, fmt.Errorf("%w: limit %d", ErrMaxIterations, l.maxIterations)
}

func (l loopNode[T]) Validate() error {
	if l.body == nil {
		return ErrNilFunc
	}
	return (LoopConfig{MaxIterations: l.maxIterations}).Validate()
}
