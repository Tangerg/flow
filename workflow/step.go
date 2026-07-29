package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

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

// Get reads the value at ref as a T. A value of exactly T is returned as-is;
// otherwise Get converts it through its JSON representation, which is what makes
// a typed read survive a serialized Store. Reading 42 back as an int works even
// though JSON only has numbers, and the same holds at any path depth and for
// structs and typed slices.
//
// A missing value, nil assigned to a non-nilable T, or a value that cannot be
// converted to T is returned as an error wrapping [ErrNotFound] or
// [ErrTypeMismatch]. Conversion never rounds or reinterprets: reading 42.5 as an
// int fails, as does reading a number as a string.
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
	if v, ok := raw.(T); ok {
		return v, nil
	}

	v, err := convert[T](raw)
	if err != nil {
		return zero, &RefError{
			Ref:  ref,
			Want: want,
			Got:  reflect.TypeOf(raw).String(),
			Err:  fmt.Errorf("%w: %w", ErrTypeMismatch, err),
		}
	}
	return v, nil
}

// convert adapts a value to T through JSON. It is the read half of the Store's
// serialization contract: a Store that has been through JSON holds JSON-domain
// values — [json.Number], string, bool, []any, map[string]any — and a typed read
// has to convert rather than assert. Routing every conversion through JSON keeps
// one rule for every depth and shape instead of a table of special cases.
//
// Callers reach this only after an exact type assertion has failed, so the cost
// falls on resumed and deserialized workflows rather than on ordinary runs.
func convert[T any](raw any) (T, error) {
	var zero T
	encoded, err := json.Marshal(raw)
	if err != nil {
		return zero, fmt.Errorf("value is not JSON-representable: %w", err)
	}
	var v T
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&v); err != nil {
		return zero, err
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
// [Output]. Errors are tagged with the step id, lifecycle events are emitted, and
// a run's [Journal] can replay the step instead of repeating it (see [RunConfig]).
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
	// Without an observer there is nothing to time, so the clock is never read.
	run := runFrom(ctx)
	journal := run.journal()

	// A journal from an earlier run already holds this step's result, so the work
	// is not repeated. The record is keyed by scope as well as ID, which is what
	// makes this correct inside Loop and Iteration, where one step runs many
	// times.
	if journal != nil && l.id != "" {
		if value, ok := journal.lookup(Scope(ctx), l.id); ok {
			next := s.WithOutput(l.id, value)
			run.emit(ctx, Event{Kind: EventSkipped, ID: l.id, Store: next})
			return next, nil
		}
	}

	var started time.Time
	if run.observing() {
		started = time.Now()
		run.emit(ctx, Event{Kind: EventStarted, ID: l.id})
	}

	// A suspension is not a failure: it keeps its own shape, reports its own
	// event, and is not wrapped in a StepError.
	stop := func(err error) (Store, error) {
		suspension := suspensionOf(err)
		suspension.ID = l.id
		suspension.Path = Scope(ctx)
		if run.observing() {
			run.emit(ctx, Event{
				Kind:    EventSuspended,
				ID:      l.id,
				Elapsed: time.Since(started),
				Err:     suspension,
			})
		}
		return s, suspension
	}
	fail := func(op StepOp, err error) (Store, error) {
		if suspensionOf(err) != nil {
			return stop(err)
		}
		err = &StepError{ID: l.id, Op: op, Err: err}
		if run.observing() {
			run.emit(ctx, Event{
				Kind:    EventFailed,
				ID:      l.id,
				Elapsed: time.Since(started),
				Err:     err,
			})
		}
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

	next := s.WithOutput(l.id, out)
	journal.record(Scope(ctx), l.id, out)
	if run.observing() {
		run.emit(ctx, Event{
			Kind:    EventCompleted,
			ID:      l.id,
			Elapsed: time.Since(started),
			Store:   next,
		})
	}
	return next, nil
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
