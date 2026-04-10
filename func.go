package flow

import (
	"context"
	"errors"
)

var _ Node[any, any] = (*Func[any, any])(nil)

// Func adapts a plain function into a Node, allowing functions to participate
// in a workflow pipeline without requiring a dedicated struct.
//
// Generic parameters:
//   - I: input type
//   - O: output type
//
// Example:
//
//	double := Func[int, int](func(ctx context.Context, x int) (int, error) {
//	    return x * 2, nil
//	})
//	result, err := double.Run(ctx, 5) // returns 10
type Func[I, O any] func(ctx context.Context, input I) (output O, err error)

// Run invokes the function with the provided context and input.
// Returns an error if the function itself is nil.
func (f Func[I, O]) Run(ctx context.Context, input I) (output O, err error) {
	if f == nil {
		var zero O
		return zero, errors.New("cannot run nil function: func is not initialized")
	}

	return f(ctx, input)
}
