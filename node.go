package flow

import (
	"context"
)

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
//
// Built-in composites validate their complete visible built-in definition
// before invoking any child. A caller-defined Node is an opaque boundary and
// remains responsible for validating its own state when Run is called. A nil
// pointer remains an ordinary Node value because its Run method may
// intentionally define nil-receiver behavior.
//
// Built-in composites do not admit another child after observing that ctx is
// cancelled. If cancellation is observed after a child returns, the parent
// context's cause takes precedence over that child's result.
type Node[I, O any] interface {
	Run(ctx context.Context, in I) (O, error)
}

// NodeFunc adapts an ordinary function into a [Node], analogous to
// [net/http.HandlerFunc].
//
//	double := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
//		return x * 2, nil
//	})
//	out, err := double.Run(ctx, 21) // 42, nil
type NodeFunc[I, O any] func(ctx context.Context, in I) (O, error)

// NodeFunc satisfies Node.
var _ Node[any, any] = NodeFunc[any, any](nil)

// Run calls f. A nil NodeFunc returns [ErrNilNode].
func (n NodeFunc[I, O]) Run(ctx context.Context, in I) (O, error) {
	if n == nil {
		var zero O
		return zero, ErrNilNode
	}
	return n(ctx, in)
}

// isNilNode recognizes this package's nil function adapter hidden in an
// interface. NodeFunc cannot do useful work without a function. Other concrete
// types, including caller-defined named function and pointer types, own their
// nil-receiver behavior through Run.
func isNilNode[I, O any](node Node[I, O]) bool {
	if node == nil {
		return true
	}
	function, ok := node.(NodeFunc[I, O])
	return ok && function == nil
}

// nodeValidator is implemented by composite nodes that can check their visible
// definition without doing work. The interface stays private: Validate is the
// public operation, while implementations opt in with the conventional method.
type nodeValidator interface {
	Validate() error
}

// Validate checks the complete definition visible through node without running
// it. Nil nodes and nil [NodeFunc] values return [ErrNilNode]. A composite may
// participate by implementing a side-effect-free Validate() error method;
// built-in composites do so recursively. Other caller-defined Nodes are opaque
// boundaries and are considered valid here, including named function types
// whose Run method deliberately defines nil-receiver behavior.
//
// Validation may run more than once and concurrently. An implementation must
// therefore inspect immutable definition state only. Execution-time checks
// that depend on input or external state belong in [Node.Run].
func Validate[I, O any](node Node[I, O]) error {
	if isNilNode(node) {
		return ErrNilNode
	}
	if validator, ok := node.(nodeValidator); ok {
		return validator.Validate()
	}
	return nil
}

// runNode applies the cancellation contract shared by sequential composites.
// Concurrent primitives own the equivalent checks in their schedulers.
func runNode[I, O any](ctx context.Context, node Node[I, O], input I) (O, error) {
	var zero O
	if err := context.Cause(ctx); err != nil {
		return zero, err
	}
	output, err := node.Run(ctx, input)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return zero, contextErr
	}
	return output, err
}
