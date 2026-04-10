package flow

import (
	"context"
	"errors"
	"fmt"
)

// Node is the fundamental building block of a workflow: it accepts an input of
// type I, performs some processing, and returns an output of type O.
type Node[I any, O any] interface {
	// Run executes the node with the given context and input.
	Run(ctx context.Context, input I) (O, error)
}

// Result holds the outcome of a single operation — either a value or an error.
// It is used when multiple operations run in parallel or over a collection, so
// that results can be collected even when individual items fail.
type Result[V any] struct {
	Value V
	Error error
}

// Pipe chains multiple dynamic-typed nodes into a single sequential pipeline.
// Each node's output becomes the next node's input at runtime.
//
// Unlike Pipe2–Pipe10, this function uses 'any' throughout and provides no
// compile-time type safety. Prefer the typed variants when types are known.
//
// Returns an error if no nodes are provided.
func Pipe(nodes ...Node[any, any]) (Node[any, any], error) {
	if len(nodes) == 0 {
		return nil, errors.New("pipe requires at least one node")
	}

	if len(nodes) == 1 {
		return nodes[0], nil
	}

	processors := make([]func(context.Context, any) (any, error), len(nodes))
	for i, n := range nodes {
		processors[i] = n.Run
	}

	sequence, err := NewSequence(processors...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pipe sequence: %w", err)
	}

	return sequence, nil
}

// Pipe2 chains two nodes into a type-safe pipeline.
// The output of the first node is passed as input to the second.
func Pipe2[I, M, O any](first Node[I, M], second Node[M, O]) Node[I, O] {
	return Func[I, O](func(ctx context.Context, input I) (O, error) {
		intermediate, err := first.Run(ctx, input)
		if err != nil {
			var zero O
			return zero, err
		}

		output, err := second.Run(ctx, intermediate)
		if err != nil {
			var zero O
			return zero, err
		}

		return output, nil
	})
}

// Pipe3 chains three nodes into a type-safe pipeline.
//
// Generic parameters:
//   - I: input type of the first node
//   - M1, M2: intermediate types between nodes
//   - O: output type of the final node
func Pipe3[I, M1, M2, O any](n1 Node[I, M1], n2 Node[M1, M2], n3 Node[M2, O]) Node[I, O] {
	return Pipe2(Pipe2(n1, n2), n3)
}

// Pipe4 chains four nodes into a type-safe pipeline.
func Pipe4[I, M1, M2, M3, O any](n1 Node[I, M1], n2 Node[M1, M2], n3 Node[M2, M3], n4 Node[M3, O]) Node[I, O] {
	return Pipe2(Pipe3(n1, n2, n3), n4)
}

// Pipe5 chains five nodes into a type-safe pipeline.
func Pipe5[I, M1, M2, M3, M4, O any](n1 Node[I, M1], n2 Node[M1, M2], n3 Node[M2, M3], n4 Node[M3, M4], n5 Node[M4, O]) Node[I, O] {
	return Pipe2(Pipe4(n1, n2, n3, n4), n5)
}

// Pipe6 chains six nodes into a type-safe pipeline.
func Pipe6[I, M1, M2, M3, M4, M5, O any](n1 Node[I, M1], n2 Node[M1, M2], n3 Node[M2, M3], n4 Node[M3, M4], n5 Node[M4, M5], n6 Node[M5, O]) Node[I, O] {
	return Pipe2(Pipe5(n1, n2, n3, n4, n5), n6)
}

// Pipe7 chains seven nodes into a type-safe pipeline.
func Pipe7[I, M1, M2, M3, M4, M5, M6, O any](n1 Node[I, M1], n2 Node[M1, M2], n3 Node[M2, M3], n4 Node[M3, M4], n5 Node[M4, M5], n6 Node[M5, M6], n7 Node[M6, O]) Node[I, O] {
	return Pipe2(Pipe6(n1, n2, n3, n4, n5, n6), n7)
}

// Pipe8 chains eight nodes into a type-safe pipeline.
func Pipe8[I, M1, M2, M3, M4, M5, M6, M7, O any](n1 Node[I, M1], n2 Node[M1, M2], n3 Node[M2, M3], n4 Node[M3, M4], n5 Node[M4, M5], n6 Node[M5, M6], n7 Node[M6, M7], n8 Node[M7, O]) Node[I, O] {
	return Pipe2(Pipe7(n1, n2, n3, n4, n5, n6, n7), n8)
}

// Pipe9 chains nine nodes into a type-safe pipeline.
func Pipe9[I, M1, M2, M3, M4, M5, M6, M7, M8, O any](n1 Node[I, M1], n2 Node[M1, M2], n3 Node[M2, M3], n4 Node[M3, M4], n5 Node[M4, M5], n6 Node[M5, M6], n7 Node[M6, M7], n8 Node[M7, M8], n9 Node[M8, O]) Node[I, O] {
	return Pipe2(Pipe8(n1, n2, n3, n4, n5, n6, n7, n8), n9)
}

// Pipe10 chains ten nodes into a type-safe pipeline.
func Pipe10[I, M1, M2, M3, M4, M5, M6, M7, M8, M9, O any](n1 Node[I, M1], n2 Node[M1, M2], n3 Node[M2, M3], n4 Node[M3, M4], n5 Node[M4, M5], n6 Node[M5, M6], n7 Node[M6, M7], n8 Node[M7, M8], n9 Node[M8, M9], n10 Node[M9, O]) Node[I, O] {
	return Pipe2(Pipe9(n1, n2, n3, n4, n5, n6, n7, n8, n9), n10)
}
