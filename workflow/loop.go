package workflow

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
)

// LoopConfig is [flow.LoopConfig] under the workflow package's semantic name.
// Both loop forms therefore share one iteration-limit and zero-value contract.
type LoopConfig = flow.LoopConfig

// Loop runs body repeatedly, threading the Store through each iteration, until
// done reports true (checked after each run), ctx is cancelled, or the iteration
// cap is reached. done receives the zero-based iteration index and the Store
// produced by that iteration. A zero [LoopConfig] uses
// [flow.DefaultMaxIterations].
//
// Because body runs more than once, each iteration adds an indexed scope frame
// with the loop ID and iteration index. That lets an observer tell iterations
// apart and a [Journal] resume in the middle of one.
//
// Each iteration's stop decision is recorded in the Journal and reused on a run
// that resumes, so a condition that is not a pure function of the Store cannot
// make a resumed loop stop at a different place than the original.
//
// If the body or stop condition suspends, Loop returns the Store produced so far
// by that iteration. An ordinary failure retains the Store from before the
// failing iteration, matching [flow.Loop]. Parent cancellation also takes
// precedence before an iteration commits and retains that prior Store. Reaching
// the iteration cap returns a [StepError] wrapping [flow.ErrMaxIterations].
//
// id names the loop for those records and for [Describe]; it must be unique among
// steps that can run in the same execution. An empty or non-UTF-8 ID, nil body,
// or nil condition is rejected before the body runs.
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
	if err := l.Validate(); err != nil {
		return s, err
	}
	if err := validateChildScope(scope(ctx)); err != nil {
		return s, newStepError(ctx, l.id, OpValidate, err)
	}
	if err := runFrom(ctx).claim(scope(ctx), l.id); err != nil {
		return s, newStepError(ctx, l.id, OpValidate, err)
	}

	return l.runIterations(ctx, s)
}

func (l loopStep) validate() error {
	if err := validateStepID(l.id); err != nil {
		return &StepError{ID: l.id, Op: OpValidate, Err: err}
	}
	if isNilNode(l.body) {
		return &StepError{ID: l.id, Op: OpValidate, Err: ErrNilStep}
	}
	if l.done == nil {
		return &StepError{ID: l.id, Op: OpValidate, Err: flow.ErrNilFunc}
	}
	if err := l.config.Validate(); err != nil {
		return &StepError{
			ID:  l.id,
			Op:  OpValidate,
			Err: err,
		}
	}
	return nil
}

func (l loopStep) Validate() error { return validateDefinition(l) }

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
		if err := context.Cause(ctx); err != nil {
			return current, err
		}

		body := (scopedStep{step: l.body}).indexed(l.id, iteration)
		next, err := body.run(ctx, current)
		if contextErr := context.Cause(ctx); contextErr != nil {
			return current, contextErr
		}
		if err != nil {
			if SuspendedOnly(err) {
				return next, err
			}
			return current, err
		}

		stop, err := l.stop(body.childContext(ctx), iteration, next)
		if contextErr := context.Cause(ctx); contextErr != nil {
			return current, contextErr
		}
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
	return current, newStepError(
		ctx,
		l.id,
		OpRun,
		fmt.Errorf("%w: limit %d", flow.ErrMaxIterations, limit),
	)
}

// stop returns whether the loop ends after this iteration, reusing the recorded
// decision when the run is resuming.
func (l loopStep) stop(ctx context.Context, iter int, s Store) (bool, error) {
	run := runFrom(ctx)
	if err := run.claim(scope(ctx), l.id); err != nil {
		return false, newStepError(ctx, l.id, OpValidate, err)
	}
	recorded, replayed, err := run.replay(ctx, scope(ctx), l.id)
	if err != nil {
		return false, err
	}
	if replayed {
		if stop, ok := recorded.(bool); ok {
			return stop, nil
		}
		return false, newStepError(
			ctx,
			l.id,
			OpRun,
			fmt.Errorf(
				"%w: journaled loop decision has type %T; want bool",
				ErrTypeMismatch,
				recorded,
			),
		)
	}

	stop, err := l.done(ctx, iter, s)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return false, contextErr
	}
	if err != nil {
		if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
			return false, suspensions.identify(l.id, scope(ctx)).err()
		}
		return false, newStepError(ctx, l.id, OpRun, err)
	}
	journalErr := run.journal().record(scope(ctx), l.id, stop)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return false, contextErr
	}
	if journalErr != nil {
		return false, newStepError(ctx, l.id, OpRun, journalErr)
	}
	return stop, nil
}

func (l loopStep) Describe() Description {
	return Description{ID: l.id, Kind: KindLoop, Children: []Description{describe(l.body)}}
}

func (l loopStep) definition() stepDefinition {
	return stepDefinition{kind: definitionLoop, id: l.id, body: l.body}
}
