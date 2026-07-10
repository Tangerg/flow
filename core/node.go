package core

import "context"

// Node is the fundamental unit of work in a flow. It accepts an input of type I,
// performs some work, and returns an output of type O or an error.
//
// Nodes are the building blocks that composition helpers such as [Then] and
// [Map] combine into larger Nodes. Because a composite is itself a Node, a whole
// workflow is a single Node[I, O] that you Run.
//
// Implementations should be safe for concurrent use: the same Node value may be
// Run from multiple goroutines at once (for example inside [Map]).
// Keep per-run state in local variables rather than on the Node.
type Node[I, O any] interface {
	Run(ctx context.Context, in I) (O, error)
}

// Func adapts an ordinary function into a [Node], letting plain functions take
// part in a flow without a dedicated type — analogous to net/http's HandlerFunc.
//
//	double := core.Func[int, int](func(_ context.Context, x int) (int, error) {
//		return x * 2, nil
//	})
//	out, err := double.Run(ctx, 21) // 42, nil
type Func[I, O any] func(ctx context.Context, in I) (O, error)

// Func satisfies Node.
var _ Node[any, any] = Func[any, any](nil)

// Run calls the underlying function. A nil Func returns [ErrNilNode] instead of
// panicking, so a forgotten or zero-value Func fails loudly without taking down
// the surrounding flow.
func (f Func[I, O]) Run(ctx context.Context, in I) (O, error) {
	if f == nil {
		var zero O
		return zero, ErrNilNode
	}
	return f(ctx, in)
}

// run invokes n, guarding against a nil Node so that composites fail with
// [ErrNilNode] instead of panicking on a nil interface.
func run[I, O any](ctx context.Context, n Node[I, O], in I) (O, error) {
	if n == nil {
		var zero O
		return zero, ErrNilNode
	}
	return n.Run(ctx, in)
}
