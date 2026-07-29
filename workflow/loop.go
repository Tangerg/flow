package workflow

import (
	"context"
	"fmt"

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
// produced by that iteration. A zero [LoopConfig] uses
// [flow.DefaultMaxIterations].
//
// Because body runs more than once, each iteration adds a scope segment naming
// its index, which is what lets an observer tell the iterations apart and a
// [Journal] resume in the middle of one.
//
// Each iteration's stop decision is recorded in the Journal and reused on a run
// that resumes, so a condition that is not a pure function of the Store cannot
// make a resumed loop stop at a different place than the original.
//
// id names the loop for those records and for [Describe]; it must be unique among
// steps that can run in the same execution.
func Loop(id string, body Step, done Condition, cfg LoopConfig) Step {
	l := loopStep{id: id, body: body}
	if done == nil {
		l.node = flow.NodeFunc[Store, Store](func(_ context.Context, s Store) (Store, error) {
			return s, flow.ErrNilFunc
		})
		return l
	}
	bodyNode := func(ctx context.Context, iter int, s Store) (Store, bool, error) {
		if id == "" {
			return s, false, &StepError{ID: id, Op: OpValidate, Err: ErrInvalidStepID}
		}
		scoped := WithScope(ctx, indexScope("", iter))
		next, err := runStep(scoped, body, s)
		if err != nil {
			return s, false, err
		}
		stop, err := l.stop(scoped, iter, next, done)
		return next, stop, err
	}
	var lc flow.LoopConfig
	lc.MaxIterations = cfg.MaxIterations
	l.node = flow.Loop(bodyNode, lc)
	return l
}

// loop is the [Step] produced by [Loop].
type loopStep struct {
	id   string
	body Step
	node Step
}

// stop returns whether the loop ends after this iteration, reusing the recorded
// decision when the run is resuming.
func (l loopStep) stop(ctx context.Context, iter int, s Store, done Condition) (bool, error) {
	journal := runFrom(ctx).journal()
	if journal != nil {
		if recorded, ok := journal.lookup(scope(ctx), l.id); ok {
			if stop, ok := recorded.(bool); ok {
				return stop, nil
			}
			return false, &StepError{
				ID:  l.id,
				Op:  OpRun,
				Err: fmt.Errorf("%w: journaled loop decision is %T, want bool", ErrTypeMismatch, recorded),
			}
		}
	}

	stop, err := done(ctx, iter, s)
	if err != nil {
		if suspensions, only := asSuspensions(err); only {
			return false, joinSuspensions(identifySuspensions(suspensions, l.id, scope(ctx)))
		}
		return false, &StepError{ID: l.id, Op: OpRun, Err: err}
	}
	journal.record(scope(ctx), l.id, stop)
	return stop, nil
}

func (l loopStep) Run(ctx context.Context, s Store) (Store, error) { return l.node.Run(ctx, s) }

func (l loopStep) Describe() Description {
	return Description{ID: l.id, Kind: "loop", Children: []Description{Describe(l.body)}}
}
