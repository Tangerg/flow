package workflow

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
)

// LoopConfig configures [Loop]. Its zero value uses [flow.DefaultMaxIterations].
type LoopConfig struct {
	// MaxIterations caps the number of iterations. Zero uses
	// [flow.DefaultMaxIterations]; negative values are invalid.
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
// If the body or stop condition suspends, Loop returns the Store produced so far
// by that iteration. An ordinary failure retains the Store from before the
// failing iteration, matching [flow.Loop].
//
// id names the loop for those records and for [Describe]; it must be unique among
// steps that can run in the same execution. An empty ID, nil body, or nil
// condition is rejected before the body runs.
func Loop(id string, body Step, done Condition, cfg LoopConfig) Step {
	return loopStep{id: id, body: body, done: done, config: cfg}
}

// loopStep is the [Step] produced by [Loop].
type loopStep struct {
	id     string
	body   Step
	done   Condition
	config LoopConfig
}

func (l loopStep) Run(ctx context.Context, s Store) (Store, error) {
	ctx = ensureRun(ctx)
	switch {
	case l.id == "":
		return s, &StepError{ID: l.id, Op: OpValidate, Err: ErrInvalidStepID}
	case isNilNode(l.body):
		return s, &StepError{ID: l.id, Op: OpValidate, Err: ErrNilStep}
	case l.done == nil:
		return s, &StepError{ID: l.id, Op: OpValidate, Err: flow.ErrNilFunc}
	case l.config.MaxIterations < 0:
		return s, &StepError{
			ID: l.id,
			Op: OpValidate,
			Err: fmt.Errorf(
				"%w: max iterations must be non-negative, got %d",
				flow.ErrInvalidConfig,
				l.config.MaxIterations,
			),
		}
	}
	if err := runFrom(ctx).validateDefinition(l); err != nil {
		return s, err
	}
	if err := runFrom(ctx).claim(scope(ctx), l.id); err != nil {
		return s, &StepError{ID: l.id, Op: OpValidate, Err: err}
	}

	return l.runIterations(ctx, s)
}

// runIterations owns workflow-specific loop semantics. In particular, a
// suspension is a third outcome: writes returned by a waiting body or by the
// body preceding a waiting condition remain visible to the caller. Ordinary
// failures retain flow.Loop's rollback-to-the-previous-iteration behavior.
func (l loopStep) runIterations(ctx context.Context, store Store) (Store, error) {
	limit := l.config.MaxIterations
	if limit == 0 {
		limit = flow.DefaultMaxIterations
	}

	current := store
	for iteration := range limit {
		if err := ctx.Err(); err != nil {
			return current, err
		}

		body := (scopedStep{step: l.body}).indexed(l.id, iteration)
		next, err := body.run(ctx, current)
		if err != nil {
			if SuspendedOnly(err) {
				return next, err
			}
			return current, err
		}

		stop, err := l.stop(body.childContext(ctx), iteration, next)
		if err != nil {
			if SuspendedOnly(err) {
				return next, err
			}
			return current, err
		}
		current = next
		if stop {
			return current, nil
		}
	}
	return current, fmt.Errorf("%w: limit %d", flow.ErrMaxIterations, limit)
}

// stop returns whether the loop ends after this iteration, reusing the recorded
// decision when the run is resuming.
func (l loopStep) stop(ctx context.Context, iter int, s Store) (bool, error) {
	run := runFrom(ctx)
	if err := run.claim(scope(ctx), l.id); err != nil {
		return false, &StepError{ID: l.id, Op: OpValidate, Err: err}
	}
	if recorded, ok := run.replay(scope(ctx), l.id); ok {
		if stop, ok := recorded.(bool); ok {
			return stop, nil
		}
		return false, &StepError{
			ID: l.id,
			Op: OpRun,
			Err: fmt.Errorf(
				"%w: journaled loop decision has type %T; want bool",
				ErrTypeMismatch,
				recorded,
			),
		}
	}

	stop, err := l.done(ctx, iter, s)
	if err != nil {
		if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
			return false, suspensions.identify(l.id, scope(ctx)).err()
		}
		return false, &StepError{ID: l.id, Op: OpRun, Err: err}
	}
	if err := run.journal().record(scope(ctx), l.id, stop); err != nil {
		return false, &StepError{ID: l.id, Op: OpRun, Err: err}
	}
	return stop, nil
}

func (l loopStep) Describe() Description {
	return Description{ID: l.id, Kind: "loop", Children: []Description{Describe(l.body)}}
}

func (l loopStep) definition() stepDefinition {
	return stepDefinition{kind: definitionLoop, id: l.id, body: l.body}
}
