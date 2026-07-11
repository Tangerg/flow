package workflow

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/Tangerg/flow/core"
)

// Step is a workflow node: it reads its inputs from the [Store] and returns a
// Store extended with its output. A Step is a core.Node[Store, Store], so it
// composes with the core primitives; steps built by this package also implement
// [Describer].
type Step = core.Node[Store, Store]

// Ref points at a value in the [Store]: a node ID plus a path under it. The first
// path segment is the key written by that node; further segments index into
// nested data.
type Ref struct {
	NodeID string `json:"nodeID"`
	Path   string `json:"path"`
}

// At returns a reference to path under nodeID.
func At(nodeID, path string) Ref { return Ref{NodeID: nodeID, Path: path} }

// OutputKey is the conventional key under which a step writes its result via
// [Leaf]. Downstream steps address it as Ref{NodeID: <id>, Path: OutputKey},
// optionally with a deeper path.
const OutputKey = "output"

// Output returns a reference to a step's conventional output value.
func Output(nodeID string) Ref { return At(nodeID, OutputKey) }

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
	raw, ok := s.Lookup(ref)
	if !ok {
		return zero, fmt.Errorf("workflow: %s not found", ref)
	}
	if raw == nil {
		target := reflect.TypeFor[T]()
		switch target.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			return zero, nil
		default:
			return zero, fmt.Errorf("workflow: %s: type mismatch: got <nil>, want %s", ref, target)
		}
	}
	v, ok := raw.(T)
	if !ok {
		return zero, fmt.Errorf("workflow: %s: type mismatch: got %T, want %s", ref, raw, reflect.TypeFor[T]())
	}
	return v, nil
}

// Binder reads a typed input from a Store. Small custom binders can implement
// this interface directly; ordinary functions can use [BindFunc].
type Binder[I any] interface {
	Bind(Store) (I, error)
}

// BindFunc adapts a function into a [Binder], analogous to core.NodeFunc.
type BindFunc[I any] func(Store) (I, error)

// Bind calls f. A nil BindFunc returns [core.ErrNilFunc].
func (f BindFunc[I]) Bind(s Store) (I, error) {
	if f == nil {
		var zero I
		return zero, core.ErrNilFunc
	}
	return f(s)
}

// From returns a Binder that reads a value of type I from ref.
func From[I any](ref Ref) Binder[I] {
	return BindFunc[I](func(s Store) (I, error) { return Get[I](s, ref) })
}

// Leaf turns a statically typed node into a [Step]. On each run it binds the
// node's input from the Store, runs it, and writes the result under
// (id, OutputKey). Errors are tagged with the step id, and lifecycle events are
// emitted (see [WithObserver]).
//
// This is the prep/exec/post split: bind reads the pool, node computes, the Step
// writes back — the node itself stays free of any Store knowledge and is unit
// testable on its own.
func Leaf[I, O any](id string, bind Binder[I], node core.Node[I, O]) Step {
	return leaf[I, O]{id: id, bind: bind, node: node}
}

// leaf is the [Step] produced by [Leaf].
type leaf[I, O any] struct {
	id   string
	bind Binder[I]
	node core.Node[I, O]
}

func (l leaf[I, O]) Run(ctx context.Context, s Store) (Store, error) {
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
		return fail(OpBind, core.ErrNilFunc)
	}
	in, err := l.bind.Bind(s)
	if err != nil {
		return fail(OpBind, err)
	}
	if l.node == nil {
		return fail(OpRun, core.ErrNilNode)
	}
	out, err := l.node.Run(ctx, in)
	if err != nil {
		return fail(OpRun, err)
	}

	emit(ctx, Event{Kind: EventCompleted, ID: l.id})
	return s.WithOutput(l.id, out), nil
}

func (l leaf[I, O]) Describe() Description {
	return Description{ID: l.id, Kind: "leaf"}
}

// Sequence runs steps in order, threading the Store through each. It composes
// them with core.Then.
func Sequence(steps ...Step) Step {
	steps = slices.Clone(steps)
	s := sequence{steps: steps}
	switch len(steps) {
	case 0:
		s.composed = passthrough()
	case 1:
		s.composed = steps[0]
	default:
		node := steps[0]
		for _, next := range steps[1:] {
			node = core.Then(node, next)
		}
		s.composed = node
	}
	return s
}

// sequence is the [Step] produced by [Sequence]; it retains its steps for
// [Describe] and delegates execution to the core.Then chain built once.
type sequence struct {
	steps    []Step
	composed Step
}

func (s sequence) Run(ctx context.Context, st Store) (Store, error) {
	return runStep(ctx, s.composed, st)
}

func (s sequence) Describe() Description {
	return Description{Kind: "sequence", Children: describeAll(s.steps)}
}

func passthrough() Step {
	return core.NodeFunc[Store, Store](func(_ context.Context, s Store) (Store, error) { return s, nil })
}

// runStep runs step, guarding against a nil Step so composites fail with
// [ErrNilStep] instead of panicking.
func runStep(ctx context.Context, step Step, s Store) (Store, error) {
	if step == nil {
		return s, ErrNilStep
	}
	return step.Run(ctx, s)
}
