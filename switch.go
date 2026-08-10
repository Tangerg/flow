package flow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Switch routes the input to one of several nodes. It runs resolve to compute a
// key, then runs the case node registered for that key. Because resolve is itself
// a [Node], the router can be any composed node — a lookup, a classifier, or an
// LLM call. If resolve yields a key with no matching case, Run returns an error
// wrapping [ErrNoCase]. An empty case set or nil case is rejected before resolve
// runs.
//
// K may be any comparable type, not just string, so enums and typed keys work.
// Parent cancellation takes precedence and prevents a selected case from
// starting after the resolver returns. Invalid cases are identified by key;
// when several are invalid, Validate returns all of them as a joined error.
func Switch[K comparable, I, O any](resolve Node[I, K], cases map[K]Node[I, O]) Node[I, O] {
	return switchNode[K, I, O]{resolve: resolve, cases: maps.Clone(cases)}
}

type switchNode[K comparable, I, O any] struct {
	resolve Node[I, K]
	cases   map[K]Node[I, O]
}

func (s switchNode[K, I, O]) Run(ctx context.Context, in I) (O, error) {
	var zero O
	if err := s.Validate(); err != nil {
		return zero, err
	}
	key, err := runNode(ctx, s.resolve, in)
	if err != nil {
		return zero, err
	}
	node, ok := s.cases[key]
	if !ok {
		return zero, fmt.Errorf("%w: key %#v", ErrNoCase, key)
	}
	return runNode(ctx, node, in)
}

func (s switchNode[K, I, O]) Validate() error {
	if err := Validate(s.resolve); err != nil {
		return err
	}
	if len(s.cases) == 0 {
		return fmt.Errorf("%w: switch requires at least one case", ErrInvalidConfig)
	}
	caseErrors := make([]error, 0)
	for key, node := range s.cases {
		if err := Validate(node); err != nil {
			caseErrors = append(caseErrors, fmt.Errorf(
				"flow: switch case %#v: %w",
				key,
				err,
			))
		}
	}
	switch len(caseErrors) {
	case 0:
		return nil
	case 1:
		return caseErrors[0]
	}
	// K need only be comparable, so case keys have no universal order. Sort the
	// complete diagnostics instead. Equal diagnostics need no tie-breaker: their
	// rendered order is indistinguishable, while errors.Join retains every cause.
	slices.SortFunc(caseErrors, func(left, right error) int {
		return strings.Compare(left.Error(), right.Error())
	})
	return errors.Join(caseErrors...)
}
