package workflow

import (
	"context"

	"github.com/Tangerg/flow"
)

// LoopConfig configures [Loop]. Its zero value uses [flow.DefaultMaxIterations].
type LoopConfig struct {
	// MaxIterations caps the number of iterations. A non-positive value uses
	// [flow.DefaultMaxIterations].
	MaxIterations int
}

// Loop runs body repeatedly, threading the Store through each iteration, until
// done reports true (checked after each run), ctx is cancelled, or the iteration
// cap is reached. done receives the zero-based iteration index and the Store
// produced by that iteration. The optional cfg is a single configuration.
func Loop(body Step, done Condition, cfg ...LoopConfig) Step {
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
	var lc flow.LoopConfig
	if len(cfg) > 0 {
		lc.MaxIterations = cfg[0].MaxIterations
	}
	l.node = flow.Loop(bodyNode, lc)
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
