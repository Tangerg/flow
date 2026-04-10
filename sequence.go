package flow

import (
	"context"
	"errors"
	"fmt"
)

var _ Node[any, any] = (*Sequence)(nil)

// Sequence executes a list of processors one after another, feeding each
// processor's output as the next processor's input.
//
// All processors use dynamic typing (any). For compile-time type safety,
// use Pipe2–Pipe10 instead.
type Sequence struct {
	processors []func(context.Context, any) (any, error)
}

// NewSequence creates a Sequence from the given processors.
// Processors are executed in the order provided.
// Returns an error if no processors are given.
func NewSequence(processors ...func(context.Context, any) (any, error)) (*Sequence, error) {
	if len(processors) == 0 {
		return nil, errors.New("at least one processor is required")
	}

	return &Sequence{
		processors: processors,
	}, nil
}

// Run passes the input through each processor in order.
// Execution stops and the error is returned as soon as any processor fails.
func (s *Sequence) Run(ctx context.Context, input any) (any, error) {
	current := input

	for index, processor := range s.processors {
		result, err := processor(ctx, current)
		if err != nil {
			return nil, fmt.Errorf("sequence failed at processor %d: %w", index, err)
		}

		current = result
	}

	return current, nil
}
