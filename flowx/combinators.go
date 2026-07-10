package flowx

import (
	"context"
	"errors"

	"github.com/Tangerg/flow/core"
)

// FanOut runs every node on the same input concurrently and returns their
// outputs in node order. The first failure cancels the rest. It is core.Map
// applied to the nodes as data.
func FanOut[I, O any](nodes []core.Node[I, O], opts ...core.MapOption) core.Node[I, []O] {
	return core.Func[I, []O](func(ctx context.Context, in I) ([]O, error) {
		apply := core.Func[core.Node[I, O], O](func(ctx context.Context, n core.Node[I, O]) (O, error) {
			var zero O
			if n == nil {
				return zero, core.ErrNilNode
			}
			return n.Run(ctx, in)
		})
		return core.Map(apply, opts...).Run(ctx, nodes)
	})
}

// FanOutAll runs every node on the same input concurrently and collects a
// [Result] per node. The returned error is non-nil only on context cancellation.
func FanOutAll[I, O any](nodes []core.Node[I, O], opts ...core.MapOption) core.Node[I, []Result[O]] {
	return core.Func[I, []Result[O]](func(ctx context.Context, in I) ([]Result[O], error) {
		apply := core.Func[core.Node[I, O], Result[O]](func(ctx context.Context, n core.Node[I, O]) (Result[O], error) {
			var out O
			var err error
			if n == nil {
				err = core.ErrNilNode
			} else {
				out, err = n.Run(ctx, in)
			}
			return Result[O]{Value: out, Error: err}, nil
		})
		return core.Map(apply, opts...).Run(ctx, nodes)
	})
}

// MapAll applies node to every element and collects a [Result] per element. It is
// core.Map with each error folded into the result, so the underlying map never
// fails fast.
func MapAll[I, O any](node core.Node[I, O], opts ...core.MapOption) core.Node[[]I, []Result[O]] {
	wrapped := core.Func[I, Result[O]](func(ctx context.Context, in I) (Result[O], error) {
		var out O
		var err error
		if node == nil {
			err = core.ErrNilNode
		} else {
			out, err = node.Run(ctx, in)
		}
		return Result[O]{Value: out, Error: err}, nil
	})
	return core.Map(wrapped, opts...)
}

// Combine2 runs two differently typed nodes concurrently on the same input and
// merges their outputs. This is the heterogeneous fan-in that core.Map cannot
// express; it boxes internally but keeps a fully typed signature.
func Combine2[I, A, B, O any](a core.Node[I, A], b core.Node[I, B], merge func(ctx context.Context, a A, b B) (O, error)) core.Node[I, O] {
	return core.Func[I, O](func(ctx context.Context, in I) (O, error) {
		var zero O
		if merge == nil {
			return zero, core.ErrNilFunc
		}
		boxed := []core.Node[I, any]{
			core.Func[I, any](func(ctx context.Context, in I) (any, error) {
				if a == nil {
					return nil, core.ErrNilNode
				}
				return a.Run(ctx, in)
			}),
			core.Func[I, any](func(ctx context.Context, in I) (any, error) {
				if b == nil {
					return nil, core.ErrNilNode
				}
				return b.Run(ctx, in)
			}),
		}
		outs, err := FanOut(boxed).Run(ctx, in)
		if err != nil {
			return zero, err
		}
		av, _ := outs[0].(A)
		bv, _ := outs[1].(B)
		return merge(ctx, av, bv)
	})
}

// ErrNoNodes is returned by [Race] when given no nodes.
var ErrNoNodes = errors.New("flowx: no nodes")

// Race runs all nodes concurrently on the same input and returns the first
// successful result, cancelling the rest. If every node fails, it returns their
// joined errors. Race cannot be derived from core.Map (which waits for all), so
// it uses its own goroutines.
func Race[I, O any](nodes []core.Node[I, O]) core.Node[I, O] {
	return core.Func[I, O](func(ctx context.Context, in I) (O, error) {
		var zero O
		if len(nodes) == 0 {
			return zero, ErrNoNodes
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		type result struct {
			val O
			err error
		}
		ch := make(chan result, len(nodes))
		for _, n := range nodes {
			go func() {
				var r result
				if n == nil {
					r.err = core.ErrNilNode
				} else {
					r.val, r.err = n.Run(ctx, in)
				}
				ch <- r
			}()
		}

		var errs []error
		for range nodes {
			r := <-ch
			if r.err == nil {
				return r.val, nil // cancel() (deferred) stops the losers
			}
			errs = append(errs, r.err)
		}
		return zero, errors.Join(errs...)
	})
}

// Identity returns a node that returns its input unchanged — the neutral element
// for core.Then.
func Identity[T any]() core.Node[T, T] {
	return core.Func[T, T](func(_ context.Context, in T) (T, error) { return in, nil })
}

// Chain composes any number of same-type nodes in sequence via core.Then. With
// no nodes it is [Identity].
func Chain[T any](nodes ...core.Node[T, T]) core.Node[T, T] {
	switch len(nodes) {
	case 0:
		return Identity[T]()
	case 1:
		return nodes[0]
	}
	n := nodes[0]
	for _, next := range nodes[1:] {
		n = core.Then(n, next)
	}
	return n
}
