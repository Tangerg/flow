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

// combineNode keeps Combine's definition visible to its own Run boundary.
// Building the derived Map/Then pipeline inside Run then delegates execution
// semantics without letting their cancellation admission hide invalid Combine
// arguments.
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

	gather := flow.NodeFunc[I, combination[A, B]](func(ctx context.Context, in I) (combination[A, B], error) {
		var result combination[A, B]
		tasks := flow.NodeFunc[int, struct{}](func(ctx context.Context, task int) (struct{}, error) {
			var err error
			switch task {
			case 0:
				result.a, err = c.a.Run(ctx, in)
			case 1:
				result.b, err = c.b.Run(ctx, in)
			}
			return struct{}{}, err
		})
		if _, err := flow.Map(tasks, flow.MapConfig{}).Run(ctx, []int{0, 1}); err != nil {
			return combination[A, B]{}, err
		}
		return result, nil
	})
	combine := flow.NodeFunc[combination[A, B], O](func(ctx context.Context, values combination[A, B]) (O, error) {
		return c.merge(ctx, values.a, values.b)
	})
	return flow.Then(gather, combine).Run(ctx, in)
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

// Chain composes any number of same-type nodes in sequence via flow.Then. It is
// the variadic convenience for the common same-type case; with no nodes it is a
// cancellation-aware pass-through. With one node it returns that node itself.
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
