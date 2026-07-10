package workflow

import (
	"context"
	"errors"

	"github.com/Tangerg/flow/core"
)

// Loop runs body repeatedly, threading the Store through each iteration, until
// done reports true (checked after each run), ctx is cancelled, or the iteration
// cap is reached (see core.WithMaxIterations, core.ErrMaxIterations). done
// receives the zero-based iteration index and the Store produced by that
// iteration. It composes with core.Loop.
func Loop(body Step, done func(ctx context.Context, iter int, s Store) bool, opts ...core.LoopOption) Step {
	l := loop{body: body}
	if done == nil {
		l.node = core.Func[Store, Store](func(_ context.Context, s Store) (Store, error) {
			return s, errors.New("workflow: nil loop condition")
		})
		return l
	}
	l.node = core.Loop(func(ctx context.Context, iter int, s Store) (Store, bool, error) {
		next, err := runStep(ctx, body, s)
		if err != nil {
			return s, false, err
		}
		return next, done(ctx, iter, next), nil
	}, opts...)
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
