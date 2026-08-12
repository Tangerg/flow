package flowx

import (
	"context"
	"slices"

	"github.com/Tangerg/flow"
)

// FanOut runs every node on the same input concurrently and returns their
// outputs in argument order. The first observed failure cancels the rest, with
// the same completion-timing semantics as [flow.Map]. A zero [flow.MapConfig]
// is unbounded. It is a thin convenience over flow.Map applied to the nodes as
// data. The complete visible definition is validated before any node starts and
// is also visible to [flow.Validate].
func FanOut[I, O any](nodes []flow.Node[I, O], cfg flow.MapConfig) flow.Node[I, []O] {
	return fanOutNode[I, O]{nodes: slices.Clone(nodes), cfg: cfg}
}

type fanOutNode[I, O any] struct {
	nodes []flow.Node[I, O]
	cfg   flow.MapConfig
}

func (f fanOutNode[I, O]) Run(ctx context.Context, in I) ([]O, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	apply := flow.NodeFunc[flow.Node[I, O], O](func(ctx context.Context, node flow.Node[I, O]) (O, error) {
		return node.Run(ctx, in)
	})
	return flow.Map(apply, f.cfg).Run(ctx, f.nodes)
}

func (f fanOutNode[I, O]) Validate() error {
	for index, node := range f.nodes {
		if err := flow.Validate(node); err != nil {
			return &flow.IndexError{Index: index, Err: err}
		}
	}
	return f.cfg.Validate()
}

// Combine runs two differently typed nodes concurrently on the same input and
// merges their outputs. It is the heterogeneous fan-in that flow.Map (which is
// homogeneous) cannot express, while keeping both intermediate values statically
// typed. A nil merge or invalid visible child definition is rejected before
// either node runs. Parent cancellation observed after the nodes finish prevents
// merge from starting; cancellation during merge takes precedence over its
// result.
func Combine[I, A, B, O any](a flow.Node[I, A], b flow.Node[I, B], merge func(ctx context.Context, a A, b B) (O, error)) flow.Node[I, O] {
	return combineNode[I, A, B, O]{a: a, b: b, merge: merge}
}

// combineNode keeps Combine's immutable definition visible to validation. Each
// Run creates a combineExecution that owns its two mutable result slots, while
// flow.Map supplies admission, cancellation, and join semantics.
type combineNode[I, A, B, O any] struct {
	a     flow.Node[I, A]
	b     flow.Node[I, B]
	merge func(context.Context, A, B) (O, error)
}

func (c combineNode[I, A, B, O]) Run(ctx context.Context, in I) (O, error) {
	var zero O
	if err := c.Validate(); err != nil {
		return zero, err
	}
	execution := combineExecution[I, A, B, O]{combine: c, input: in}
	return execution.execute(ctx)
}

func (c combineNode[I, A, B, O]) Validate() error {
	if c.merge == nil {
		return flow.ErrNilFunc
	}
	if err := flow.Validate(c.a); err != nil {
		return err
	}
	return flow.Validate(c.b)
}

type combination[A, B any] struct {
	a A
	b B
}

type combineTask uint8

const (
	combineA combineTask = iota
	combineB
)

// combineExecution owns the mutable result slots of one Combine invocation.
// flow.Map may call Run concurrently, but each admitted task owns exactly one
// slot; execute joins both calls before reading either value or invoking merge.
type combineExecution[I, A, B, O any] struct {
	combine combineNode[I, A, B, O]
	input   I
	result  combination[A, B]
}

func (c *combineExecution[I, A, B, O]) execute(ctx context.Context) (O, error) {
	var zero O
	if _, err := flow.Map(c, flow.MapConfig{}).Run(ctx, []combineTask{combineA, combineB}); err != nil {
		return zero, err
	}
	if err := context.Cause(ctx); err != nil {
		return zero, err
	}
	output, err := c.combine.merge(ctx, c.result.a, c.result.b)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return zero, contextErr
	}
	return output, err
}

// Run executes one of the two fixed input branches. The task value is private
// input produced by execute, so no default case is part of the protocol.
func (c *combineExecution[I, A, B, O]) Run(ctx context.Context, task combineTask) (struct{}, error) {
	var err error
	switch task {
	case combineA:
		c.result.a, err = c.combine.a.Run(ctx, c.input)
	case combineB:
		c.result.b, err = c.combine.b.Run(ctx, c.input)
	}
	return struct{}{}, err
}

// Chain composes any number of same-type nodes in sequence via flow.Then. It is
// the variadic convenience for the common same-type case; with no nodes it is a
// cancellation-aware pass-through. With one non-nil node it returns that node
// itself, so wrapping costs nothing.
//
// Chain never returns a nil Node. A nil element yields one that reports
// [flow.ErrNilNode] from Run and from [flow.Validate] instead of an interface nil
// that would panic when called. With two or more nodes flow.Then already gives
// that guarantee, and the single-node case matches it.
func Chain[T any](nodes ...flow.Node[T, T]) flow.Node[T, T] {
	switch len(nodes) {
	case 0:
		return flow.NodeFunc[T, T](func(ctx context.Context, in T) (T, error) {
			return in, context.Cause(ctx)
		})
	case 1:
		if nodes[0] == nil {
			return flow.NodeFunc[T, T](nil)
		}
		return nodes[0]
	}
	n := nodes[0]
	for _, next := range nodes[1:] {
		n = flow.Then(n, next)
	}
	return n
}

// Fallback runs primary; if it fails while the parent context remains live, it
// runs alternate with the same input. Cancellation of the outer operation is
// returned as-is, discards the result that raced with it, and does not trigger
// or get hidden by the fallback. Fallback applies to every other error; it
// cannot distinguish domain-specific third outcomes represented as errors.
func Fallback[I, O any](primary, alternate flow.Node[I, O]) flow.Node[I, O] {
	return fallbackNode[I, O]{primary: primary, alternate: alternate}
}

type fallbackNode[I, O any] struct {
	primary   flow.Node[I, O]
	alternate flow.Node[I, O]
}

func (f fallbackNode[I, O]) Run(ctx context.Context, in I) (O, error) {
	var zero O
	if err := f.Validate(); err != nil {
		return zero, err
	}
	if err := context.Cause(ctx); err != nil {
		return zero, err
	}
	out, err := f.primary.Run(ctx, in)
	if ctxErr := context.Cause(ctx); ctxErr != nil {
		return zero, ctxErr
	}
	if err == nil {
		return out, nil
	}
	out, err = f.alternate.Run(ctx, in)
	if ctxErr := context.Cause(ctx); ctxErr != nil {
		return zero, ctxErr
	}
	return out, err
}

func (f fallbackNode[I, O]) Validate() error {
	if err := flow.Validate(f.primary); err != nil {
		return err
	}
	return flow.Validate(f.alternate)
}
