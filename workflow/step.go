package workflow

import (
	"context"
	"fmt"
	"reflect"
	"slices"

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
		if raw == nil {
			target := reflect.TypeFor[I]()
			switch target.Kind() {
			case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
				return zero, nil
			default:
				return zero, fmt.Errorf("input %s.%s: type mismatch: got <nil>, want %s", ref.NodeID, ref.Path, target)
			}
		}
		v, ok := raw.(I)
		if !ok {
			return zero, fmt.Errorf("input %s.%s: type mismatch: got %T, want %s", ref.NodeID, ref.Path, raw, reflect.TypeFor[I]())
		}
		return v, nil
	}
}

// Adapt turns a statically typed node into a [Step]. On each run it binds the
// node's input from the Store, runs it, and writes the result under
// (id, OutputKey). Errors are tagged with the step id, and lifecycle events are
// emitted (see [WithSink]).
//
// This is the prep/exec/post split: bind reads the pool, node computes, the Step
// writes back — the node itself stays free of any Store knowledge and is unit
// testable on its own.
func Adapt[I, O any](id string, bind func(Store) (I, error), node core.Node[I, O]) Step {
	return leaf[I, O]{id: id, bind: bind, node: node}
}

// leaf is the [Step] produced by [Adapt].
type leaf[I, O any] struct {
	id   string
	bind func(Store) (I, error)
	node core.Node[I, O]
}

func (l leaf[I, O]) Run(ctx context.Context, s Store) (Store, error) {
	emit(ctx, NodeStarted{ID: l.id})
	fail := func(op string, err error) (Store, error) {
		err = &StepError{ID: l.id, Op: op, Err: err}
		emit(ctx, NodeFailed{ID: l.id, Err: err})
		return s, err
	}

	if l.id == "" {
		return fail("validate", ErrInvalidStepID)
	}
	if l.bind == nil {
		return fail("bind", core.ErrNilFunc)
	}
	in, err := l.bind(s)
	if err != nil {
		return fail("bind", err)
	}
	if l.node == nil {
		return fail("run", core.ErrNilNode)
	}
	out, err := l.node.Run(ctx, in)
	if err != nil {
		return fail("run", err)
	}

	emit(ctx, NodeCompleted{ID: l.id})
	return s.With(l.id, OutputKey, out), nil
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
	return core.Func[Store, Store](func(_ context.Context, s Store) (Store, error) { return s, nil })
}

// runStep runs step, guarding against a nil Step so composites fail with
// [ErrNilStep] instead of panicking.
func runStep(ctx context.Context, step Step, s Store) (Store, error) {
	if step == nil {
		return s, ErrNilStep
	}
	return step.Run(ctx, s)
}
