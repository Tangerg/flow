package flow

import (
	"context"
	"fmt"
)

// DefaultMaxIterations caps a [Loop] when no limit is configured, guarding
// against an accidental infinite loop.
const DefaultMaxIterations = 1024

// Loop repeatedly applies body to a value until body reports done, ctx is
// cancelled, or the iteration cap is reached (see [WithMaxIterations]).
//
// body receives the zero-based iteration index and the value from the previous
// iteration (or the initial input on the first call). It returns the next value,
// a done flag, and an error. On error, Loop returns the value from before the
// failing iteration. Reaching the cap without done returns an error wrapping
// [ErrMaxIterations].
func Loop[T any](
	body func(ctx context.Context, iter int, in T) (out T, done bool, err error),
	opts ...LoopOption,
) Node[T, T] {
	c := loopConfig{maxIterations: DefaultMaxIterations}
	for _, opt := range opts {
		if opt != nil {
			opt.applyLoop(&c)
		}
	}
	return loopNode[T]{body: body, max: c.maxIterations}
}

type loopConfig struct {
	maxIterations int
}

// LoopOption configures a [Loop]. Options are created by this package.
type LoopOption interface {
	applyLoop(*loopConfig)
}

type loopOptionFunc func(*loopConfig)

func (f loopOptionFunc) applyLoop(c *loopConfig) { f(c) }

// WithMaxIterations sets the maximum number of iterations. A value <= 0 restores
// the default ([DefaultMaxIterations]).
func WithMaxIterations(n int) LoopOption {
	return loopOptionFunc(func(c *loopConfig) {
		if n <= 0 {
			n = DefaultMaxIterations
		}
		c.maxIterations = n
	})
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
