package workflow

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
)

// Step is a workflow node: it reads its inputs from the [Store] and returns a
// Store extended with its output. A Step is a flow.Node[Store, Store], so it
// composes with flow's primitives; steps built by this package also implement
// [Describer].
//
// As with a read operation returning both bytes and a terminal error, the Store
// returned with a non-nil error can carry meaningful completed state. A
// caller-defined Step must define that result deliberately rather than treating
// it as an ignored return slot. Transparent control flow such as [Sequence] and
// [Branch] preserves the selected child's result; composites that introduce a
// commit boundary document what they retain or roll back. Parent cancellation
// is stricter: when it is observed as a child returns, built-in composites give
// it precedence and discard that child's unaccepted result.
//
// That makes a Step's rollback different from a plain Node's, which matters to a
// caller-defined composite invoking children: [flow.RunChild] applies the same
// cancellation rule but rolls back to a zero output, while a composite here keeps
// the Store it handed the cancelled child, because that Store is what already
// completed.
//
// Built-in composites validate their complete visible definition before work
// begins and do not admit another child after observing parent cancellation.
// They also participate in [flow.Validate], so the same checks remain visible
// when Steps are nested inside root-package composites. In the other direction,
// built-in workflow composites honor a caller-defined Step's optional Validate
// method without assuming anything about its hidden structure. Validation can
// inspect only immutable definition state and therefore cannot suspend; a
// validator that returns [ErrSuspended] is reported as [flow.ErrInvalidConfig].
type Step = flow.Node[Store, Store]

// scopedStep owns one child-step invocation in a repeated execution scope.
// Context remains an explicit method argument, following the standard context
// contract instead of being retained in a struct.
type scopedStep struct {
	step  Step
	frame ScopeFrame
}

// run invokes the child under its scope. Composites validate their body before
// constructing a scopedStep, so step is never nil here.
func (s scopedStep) run(ctx context.Context, store Store) (Store, error) {
	return s.step.Run(s.childContext(ctx), store)
}

func (s scopedStep) indexed(id string, index int) scopedStep {
	s.frame = ScopeFrame{
		ID:      id,
		Indexed: true,
		// #nosec G115 -- callers supply indexes produced by non-negative range loops.
		Index: uint64(index),
	}
	return s
}

func (s scopedStep) childContext(parent context.Context) context.Context {
	return withScopeFrame(parent, s.frame)
}

type stepList []Step

func (s stepList) validate() error {
	for index, step := range s {
		if isNilNode(step) {
			return &flow.IndexError{Index: index, Err: ErrNilStep}
		}
	}
	return nil
}

// isNilNode recognizes flow's nil function adapter hidden in an interface.
// NodeFunc cannot run without a function. Caller-defined concrete types,
// including named function and pointer types, own their nil-receiver behavior.
func isNilNode[I, O any](node flow.Node[I, O]) bool {
	if node == nil {
		return true
	}
	function, ok := node.(flow.NodeFunc[I, O])
	return ok && function == nil
}

func (s stepList) run(ctx context.Context, store Store) (Store, error) {
	current := store
	for _, step := range s {
		if err := context.Cause(ctx); err != nil {
			return current, err
		}
		next, err := step.Run(ctx, current)
		if contextErr := context.Cause(ctx); contextErr != nil {
			return current, contextErr
		}
		if err != nil {
			return next, err
		}
		current = next
	}
	return current, context.Cause(ctx)
}

func (s stepList) describe() []Description {
	descriptions := make([]Description, len(s))
	for index, step := range s {
		descriptions[index] = describe(step)
	}
	return descriptions
}

// admitBoundary performs the admission every named boundary shares: reject an
// invalid definition, claim the execution identity, and report either failure as
// a terminal event, because a boundary that is not admitted never reported a
// start. Validating before replay is deliberate — a completed [Journal] record
// must not be able to hide an invalid ID, binder, or composed Node.
//
// It returns the run for the work that follows, and any cancellation observed
// once admitted, so no boundary begins work under a cancelled context. A nil run
// discards the events, which is how a boundary called outside [Run] behaves.
func admitBoundary(ctx context.Context, id string, invalid error) (*runState, error) {
	run := runFrom(ctx)
	if invalid != nil {
		run.emit(ctx, Event{Kind: EventFailed, ID: id, Err: invalid})
		return run, invalid
	}
	if err := run.claim(boundaryKey(ctx, id)); err != nil {
		err = newStepError(ctx, id, OpValidate, err)
		run.emit(ctx, Event{Kind: EventFailed, ID: id, Err: err})
		return run, err
	}
	return run, context.Cause(ctx)
}

// bodyOutputError names a failed read of a composite's body output. Iteration and
// Subgraph both project one, and the condition reads the same from either even
// though they report it at different boundaries.
func bodyOutputError(output Ref, err error) error {
	return &detailError{
		detail: fmt.Sprintf("read body output %s", output),
		err:    err,
	}
}

// admitScopedStep is the admission a composite performs when it will push a new
// scope frame: reject an invalid definition, refuse a scope already at the depth
// limit, and claim the execution identity. Like [admitBoundary] it also reports
// any cancellation observed once admitted -- claiming an identity may wait on
// another goroutine -- so no composite begins work under a cancelled context and
// none of them has to remember to sample it again. Unlike admitBoundary it
// publishes nothing, because a composite is transparent and adds no lifecycle
// events of its own.
//
// Branch is deliberately not a caller. It selects a case in the current scope
// rather than pushing a frame, so running at the depth limit is legal for it.
func admitScopedStep(ctx context.Context, id string, invalid error) error {
	if invalid != nil {
		return invalid
	}
	if err := validateChildScope(scope(ctx)); err != nil {
		return newStepError(ctx, id, OpValidate, err)
	}
	if err := runFrom(ctx).claim(boundaryKey(ctx, id)); err != nil {
		return newStepError(ctx, id, OpValidate, err)
	}
	return context.Cause(ctx)
}
