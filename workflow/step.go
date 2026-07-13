package workflow

import (
	"context"
	"reflect"
	"slices"
	"strings"

	"github.com/Tangerg/flow"
)

// Step is a workflow node: it reads its inputs from the [Store] and returns a
// Store extended with its output. A Step is a flow.Node[Store, Store], so it
// composes with flow's primitives; steps built by this package also implement
// [Describer].
type Step = flow.Node[Store, Store]

// Ref points at a value in the [Store]: a node ID plus a path under it. The first
// path segment is the key written by that node; further segments index into
// nested data.
type Ref struct {
	NodeID string `json:"nodeID"`
	Path   string `json:"path"`
}

// At returns a reference to path under nodeID.
func At(nodeID, path string) Ref { return Ref{NodeID: nodeID, Path: path} }

const outputKey = "output"

// Output returns a reference to a step's conventional output value.
func Output(nodeID string) Ref { return At(nodeID, outputKey) }

// String returns the reference in nodeID.path form.
func (r Ref) String() string { return r.NodeID + "." + r.Path }

// Child returns a reference below r. An empty path returns r unchanged.
func (r Ref) Child(path string) Ref {
	if path == "" {
		return r
	}
	r.Path = strings.Trim(r.Path+"."+path, ".")
	return r
}

// Get loads and type-checks the value at ref. A missing value, nil assigned to
// a non-nilable T, or a type mismatch is returned as an error.
func Get[T any](s Store, ref Ref) (T, error) {
	var zero T
	target := reflect.TypeFor[T]()
	want := target.String()
	raw, ok := s.Lookup(ref)
	if !ok {
		return zero, &RefError{Ref: ref, Want: want, Err: ErrNotFound}
	}
	if raw == nil {
		switch target.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			return zero, nil
		default:
			return zero, &RefError{Ref: ref, Want: want, Err: ErrTypeMismatch}
		}
	}
	v, ok := raw.(T)
	if !ok {
		return zero, &RefError{Ref: ref, Want: want, Got: reflect.TypeOf(raw).String(), Err: ErrTypeMismatch}
	}
	return v, nil
}

// BindFunc reads a typed input from a Store. Create one with [From], or write
// one inline when a step needs to read several references.
type BindFunc[I any] func(Store) (I, error)

// From returns a BindFunc that reads a value of type I from ref.
func From[I any](ref Ref) BindFunc[I] {
	return func(s Store) (I, error) { return Get[I](s, ref) }
}

// Leaf turns a statically typed node into a [Step]. On each run it binds the
// node's input from the Store, runs it, and writes the result under
// [Output]. Errors are tagged with the step id, and lifecycle events are
// emitted (see [WithObserver]).
//
// This is the prep/exec/post split: bind reads the pool, node computes, the Step
// writes back — the node itself stays free of any Store knowledge and is unit
// testable on its own.
func Leaf[I, O any](id string, bind BindFunc[I], node flow.Node[I, O]) Step {
	return leafStep[I, O]{id: id, bind: bind, node: node}
}

// leaf is the [Step] produced by [Leaf].
type leafStep[I, O any] struct {
	id   string
	bind BindFunc[I]
	node flow.Node[I, O]
}

func (l leafStep[I, O]) Run(ctx context.Context, s Store) (Store, error) {
	emit(ctx, Event{Kind: EventStarted, ID: l.id})
	fail := func(op StepOp, err error) (Store, error) {
		err = &StepError{ID: l.id, Op: op, Err: err}
		emit(ctx, Event{Kind: EventFailed, ID: l.id, Err: err})
		return s, err
	}

	if l.id == "" {
		return fail(OpValidate, ErrInvalidStepID)
	}
	if l.bind == nil {
		return fail(OpBind, flow.ErrNilFunc)
	}
	in, err := l.bind(s)
	if err != nil {
		return fail(OpBind, err)
	}
	if l.node == nil {
		return fail(OpRun, flow.ErrNilNode)
	}
	out, err := l.node.Run(ctx, in)
	if err != nil {
		return fail(OpRun, err)
	}

	emit(ctx, Event{Kind: EventCompleted, ID: l.id})
	return s.WithOutput(l.id, out), nil
}

func (l leafStep[I, O]) Describe() Description {
	return Description{ID: l.id, Kind: "leaf"}
}

// Sequence runs steps in order, threading the Store through each.
func Sequence(steps ...Step) Step {
	return sequenceStep{steps: slices.Clone(steps)}
}

// sequence is the [Step] produced by [Sequence].
type sequenceStep struct {
	steps []Step
}

func (s sequenceStep) Run(ctx context.Context, st Store) (Store, error) {
	return runSteps(ctx, s.steps, st)
}

func (s sequenceStep) Describe() Description {
	return Description{Kind: "sequence", Children: describeAll(s.steps)}
}

// runStep runs step, guarding against a nil Step so composites fail with
// [ErrNilStep] instead of panicking.
func runStep(ctx context.Context, step Step, s Store) (Store, error) {
	if step == nil {
		return s, ErrNilStep
	}
	return step.Run(ctx, s)
}

func runSteps(ctx context.Context, steps []Step, s Store) (Store, error) {
	current := s
	for _, step := range steps {
		var err error
		current, err = runStep(ctx, step, current)
		if err != nil {
			return current, err
		}
	}
	return current, nil
}
