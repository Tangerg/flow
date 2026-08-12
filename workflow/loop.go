package workflow

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
)

// LoopConfig configures [Loop]. ID, Body, and Condition are required; a zero
// MaxIterations uses [flow.DefaultMaxIterations], matching [flow.LoopConfig].
type LoopConfig struct {
	// ID names the loop in the Journal and in [Describe]. It must be unique
	// among steps that can run in the same execution.
	ID string
	// Body runs once per iteration, threading the Store through.
	Body Step
	// Condition is checked after each iteration and receives the Store that
	// iteration produced. It runs inside the iteration's indexed scope, so
	// [Scope] reports the zero-based iteration index when a decision needs it.
	Condition Condition
	// MaxIterations caps the number of iterations. Zero uses
	// [flow.DefaultMaxIterations]; negative values are invalid.
	MaxIterations int
}

// Loop runs [LoopConfig.Body] repeatedly, threading the Store through each
// iteration, until [LoopConfig.Condition] reports true (checked after each run), ctx
// is cancelled, or the iteration cap is reached.
//
// Because the body runs more than once, each iteration adds an indexed scope
// frame with the loop ID and iteration index. That lets an observer tell iterations
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
// An empty or non-UTF-8 ID, nil Body, nil Condition, or negative MaxIterations
// is rejected before the body runs.
func Loop(cfg LoopConfig) Step {
	return loopStep{config: cfg}
}

// loopStep is the [Step] produced by [Loop].
type loopStep struct {
	config LoopConfig
}

func (l loopStep) Run(ctx context.Context, s Store) (Store, error) {
	ctx = ensureRun(ctx)
	if err := l.Validate(); err != nil {
		return s, err
	}
	if err := validateChildScope(scope(ctx)); err != nil {
		return s, newStepError(ctx, l.config.ID, OpValidate, err)
	}
	if err := runFrom(ctx).claim(scope(ctx), l.config.ID); err != nil {
		return s, newStepError(ctx, l.config.ID, OpValidate, err)
	}

	execution := loopExecution{
		loop:    l,
		run:     runFrom(ctx),
		current: s,
		limit:   l.iterationLimit(),
	}
	return execution.runIterations(ctx)
}

func (l loopStep) validate() error {
	if err := validateBody(l.config.ID, l.config.Body); err != nil {
		return err
	}
	if err := validateNode(l.config.Condition); err != nil {
		return newValidationError(l.config.ID, err)
	}
	// The kernel owns the meaning of the iteration cap, so its config validates
	// the value this one carries rather than restating the rule.
	if err := (flow.LoopConfig{MaxIterations: l.config.MaxIterations}).Validate(); err != nil {
		return newValidationError(
			l.config.ID,
			err)
	}
	return nil
}

func (l loopStep) Validate() error { return validateDefinition(l) }

func (l loopStep) iterationLimit() int {
	limit := l.config.MaxIterations
	if limit == 0 {
		limit = flow.DefaultMaxIterations
	}
	return limit
}

// loopExecution owns the mutable Store snapshot and decision history of one
// loop invocation. current is the last committed iteration. A suspension is
// the sole transition that may expose an uncommitted next Store; ordinary
// failure and parent cancellation leave current at the previous iteration.
type loopExecution struct {
	loop    loopStep
	run     *runState
	current Store
	limit   int
}

func (l *loopExecution) runIterations(ctx context.Context) (Store, error) {
	for iteration := range l.limit {
		if err := context.Cause(ctx); err != nil {
			return l.current, err
		}
		stop, err := l.advance(ctx, iteration)
		if err != nil {
			return l.current, err
		}
		if stop {
			return l.current, nil
		}
	}
	return l.current, newStepError(
		ctx,
		l.loop.config.ID,
		OpRun,
		fmt.Errorf("%w: limit %d", flow.ErrMaxIterations, l.limit),
	)
}

// advance runs one body and its stop decision. It commits next only after both
// succeed. A suspension deliberately promotes next before returning, preserving
// the waiting body's writes; every other error keeps the previous current.
func (l *loopExecution) advance(ctx context.Context, iteration int) (bool, error) {
	body := (scopedStep{step: l.loop.config.Body}).indexed(l.loop.config.ID, iteration)
	next, err := body.run(ctx, l.current)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return false, contextErr
	}
	if err != nil {
		if SuspendedOnly(err) {
			l.current = next
		}
		return false, err
	}

	stop, err := l.stop(body.childContext(ctx), next)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return false, contextErr
	}
	if err != nil {
		if SuspendedOnly(err) {
			l.current = next
		}
		return false, err
	}
	l.current = next
	return stop, nil
}

// stop returns whether the loop ends after this iteration, reusing the recorded
// decision when the run is resuming.
func (l *loopExecution) stop(ctx context.Context, s Store) (bool, error) {
	if err := l.run.claim(scope(ctx), l.loop.config.ID); err != nil {
		return false, newStepError(ctx, l.loop.config.ID, OpValidate, err)
	}
	recorded, replayed, err := l.run.replay(ctx, scope(ctx), l.loop.config.ID)
	if err != nil {
		return false, err
	}
	if replayed {
		if stop, ok := recorded.(bool); ok {
			return stop, nil
		}
		return false, newStepError(
			ctx,
			l.loop.config.ID,
			OpRun,
			fmt.Errorf(
				"%w: journaled loop decision has type %T; want bool",
				ErrTypeMismatch,
				recorded,
			),
		)
	}

	stop, err := l.loop.config.Condition.Run(ctx, s)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return false, contextErr
	}
	if err != nil {
		if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
			return false, suspensions.identify(l.loop.config.ID, scope(ctx)).err()
		}
		return false, newStepError(ctx, l.loop.config.ID, OpRun, err)
	}
	journalErr := l.run.journal().record(scope(ctx), l.loop.config.ID, stop)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return false, contextErr
	}
	if journalErr != nil {
		return false, newStepError(ctx, l.loop.config.ID, OpRun, journalErr)
	}
	return stop, nil
}

func (l loopStep) Describe() Description {
	return Description{ID: l.config.ID, Kind: KindLoop, Children: []Description{describe(l.config.Body)}}
}

func (l loopStep) definition() stepDefinition {
	return stepDefinition{kind: definitionLoop, id: l.config.ID, body: l.config.Body}
}
