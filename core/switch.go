package core

import (
	"context"
	"fmt"
	"maps"
)

// Switch routes the input to one of several nodes. It runs resolve to compute a
// key, then runs the case node registered for that key. Because resolve is itself
// a [Node], the router can be any composed node — a lookup, a classifier, or an
// LLM call. If resolve yields a key with no matching case, Run returns an error
// wrapping [ErrNoCase].
//
// K may be any comparable type, not just string, so enums and typed keys work.
func Switch[K comparable, I, O any](resolve Node[I, K], cases map[K]Node[I, O]) Node[I, O] {
	return switchNode[K, I, O]{resolve: resolve, cases: maps.Clone(cases)}
}

type switchNode[K comparable, I, O any] struct {
	resolve Node[I, K]
	cases   map[K]Node[I, O]
}

func (s switchNode[K, I, O]) Run(ctx context.Context, in I) (O, error) {
	var zero O
	key, err := run(ctx, s.resolve, in)
	if err != nil {
		return zero, err
	}
	node, ok := s.cases[key]
	if !ok {
		return zero, fmt.Errorf("%w: %v", ErrNoCase, key)
	}
	return run(ctx, node, in)
}
