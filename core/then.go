package core

import "context"

// Then composes two nodes into one that runs first, feeds its output into
// second, and returns second's output. If first fails, second is not run.
//
// Chain more than two by nesting: Then(Then(a, b), c).
func Then[I, M, O any](first Node[I, M], second Node[M, O]) Node[I, O] {
	return then[I, M, O]{first: first, second: second}
}

type then[I, M, O any] struct {
	first  Node[I, M]
	second Node[M, O]
}

func (t then[I, M, O]) Run(ctx context.Context, in I) (O, error) {
	mid, err := run(ctx, t.first, in)
	if err != nil {
		var zero O
		return zero, err
	}
	return run(ctx, t.second, mid)
}
