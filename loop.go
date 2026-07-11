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
	// MaxIterations caps the number of iterations. A non-positive value uses
	// [DefaultMaxIterations].
	MaxIterations int
}

// Loop repeatedly applies body to a value until body reports done, ctx is
// cancelled, or the iteration cap is reached.
//
// body receives the zero-based iteration index and the value from the previous
// iteration (or the initial input on the first call). It returns the next value,
// a done flag, and an error. On error, Loop returns the value from before the
// failing iteration. Reaching the cap without done returns an error wrapping
// [ErrMaxIterations]. The optional cfg is a single configuration; if several are
// passed, the first applies.
func Loop[T any](
	body func(ctx context.Context, iter int, in T) (out T, done bool, err error),
	cfg ...LoopConfig,
) Node[T, T] {
	max := DefaultMaxIterations
	if len(cfg) > 0 && cfg[0].MaxIterations > 0 {
		max = cfg[0].MaxIterations
	}
	return loopNode[T]{body: body, max: max}
}

type loopNode[T any] struct {
	body func(context.Context, int, T) (T, bool, error)
	max  int
}

func (l loopNode[T]) Run(ctx context.Context, in T) (T, error) {
	cur := in
	if l.body == nil {
		return cur, ErrNilFunc
	}
	for i := range l.max {
		if err := ctx.Err(); err != nil {
			return cur, err
		}
		next, done, err := l.body(ctx, i, cur)
		if err != nil {
			return cur, err
		}
		cur = next
		if done {
			return cur, nil
		}
	}
	return cur, fmt.Errorf("%w (%d)", ErrMaxIterations, l.max)
}
