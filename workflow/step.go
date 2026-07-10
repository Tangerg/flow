package workflow

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow/core"
)

// Step is a workflow node: it reads its inputs from the [Store] and returns a
// Store extended with its output. Steps compose with the core primitives, since
// a Step is just a core.Node[Store, Store].
type Step = core.Node[Store, Store]

// Ref points at a value in the [Store]: a node ID plus a path under it. The
// first path segment is the key written by that node; further segments index
// into nested data.
type Ref struct {
	NodeID string `json:"nodeID"`
	Path   string `json:"path"`
}

// OutputKey is the conventional key under which a step writes its result via
// [Adapt]. Downstream steps address it as Ref{NodeID: <id>, Path: OutputKey},
// optionally with a deeper path.
const OutputKey = "output"

// FromRef builds an input binder that reads a single value of type I from the
// Store at ref. It is the common case for [Adapt]'s bind argument.
func FromRef[I any](ref Ref) func(Store) (I, error) {
	return func(s Store) (I, error) {
		var zero I
		raw, ok := s.Get(ref.NodeID, ref.Path)
		if !ok {
			return zero, fmt.Errorf("input %s.%s not found", ref.NodeID, ref.Path)
		}
		v, ok := raw.(I)
		if !ok {
			return zero, fmt.Errorf("input %s.%s: type mismatch: got %T, want %T", ref.NodeID, ref.Path, raw, zero)
		}
		return v, nil
	}
}

// Adapt turns a statically typed leaf into a [Step]. On each run it binds the
// leaf's input from the Store, runs the leaf, and writes the result under
// (id, OutputKey). Errors are tagged with the step id for a readable path.
//
// This is the prep/exec/post split: bind reads the pool, leaf computes, Adapt
// writes back — the leaf itself stays free of any Store knowledge and is unit
// testable on its own.
func Adapt[I, O any](id string, bind func(Store) (I, error), leaf core.Node[I, O]) Step {
	return core.Func[Store, Store](func(ctx context.Context, s Store) (Store, error) {
		emit(ctx, NodeStarted{ID: id})
		in, err := bind(s)
		if err != nil {
			err = fmt.Errorf("workflow: step %q: %w", id, err)
			emit(ctx, NodeFailed{ID: id, Err: err})
			return s, err
		}
		out, err := leaf.Run(ctx, in)
		if err != nil {
			err = fmt.Errorf("workflow: step %q: %w", id, err)
			emit(ctx, NodeFailed{ID: id, Err: err})
			return s, err
		}
		emit(ctx, NodeCompleted{ID: id})
		return s.With(id, OutputKey, out), nil
	})
}

// Sequence runs steps in order, threading the Store through each. It is a thin
// specialization of core.Then over Step.
func Sequence(steps ...Step) Step {
	switch len(steps) {
	case 0:
		return core.Func[Store, Store](func(_ context.Context, s Store) (Store, error) { return s, nil })
	case 1:
		return steps[0]
	}
	step := steps[0]
	for _, next := range steps[1:] {
		step = core.Then(step, next)
	}
	return step
}

// runStep runs step, guarding against a nil Step so composites fail with
// [ErrNilStep] instead of panicking.
func runStep(ctx context.Context, step Step, s Store) (Store, error) {
	if step == nil {
		return s, ErrNilStep
	}
	return step.Run(ctx, s)
}
