package workflow

import (
	"context"

	"github.com/Tangerg/flow"
)

// Loop runs body repeatedly, threading the Store through each iteration, until
// done reports true (checked after each run), ctx is cancelled, or the default
// iteration cap is reached. done receives the zero-based iteration index and
// the Store produced by that iteration.
func Loop(body Step, done Condition) Step {
	return loopLimit(0, body, done)
}

// LoopN is like [Loop] but stops after at most limit iterations. A non-positive
// limit uses [flow.DefaultMaxIterations].
func LoopN(limit int, body Step, done Condition) Step {
	return loopLimit(limit, body, done)
}

func loopLimit(limit int, body Step, done Condition) Step {
	l := loop{body: body}
	if done == nil {
		l.node = flow.NodeFunc[Store, Store](func(_ context.Context, s Store) (Store, error) {
			return s, flow.ErrNilFunc
		})
		return l
	}
	bodyNode := func(ctx context.Context, iter int, s Store) (Store, bool, error) {
		next, err := runStep(ctx, body, s)
		if err != nil {
			return s, false, err
		}
		stop, err := done(ctx, iter, next)
		return next, stop, err
	}
	if limit > 0 {
		l.node = flow.LoopN(limit, bodyNode)
	} else {
		l.node = flow.Loop(bodyNode)
	}
	return l
}

// loop is the [Step] produced by [Loop].
type loop struct {
	body Step
	node Step
}

func (l loop) Run(ctx context.Context, s Store) (Store, error) { return l.node.Run(ctx, s) }

func (l loop) Describe() Description {
	return Description{Kind: "loop", Children: []Description{Describe(l.body)}}
}
