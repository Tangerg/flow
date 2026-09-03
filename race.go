package flow

import (
	"context"
	"errors"
	"slices"
)

// Race runs all nodes concurrently on the same input and returns the first
// successful result, cancelling the rest. Before Run returns it waits for every
// losing call to stop, so no goroutine retains the input after the operation
// completes. If every node fails, it returns their joined errors in input order,
// each wrapped in an [IndexError]. Cancellation is cooperative; losing nodes
// must honor their context. A losing node that ignores cancellation can
// therefore prevent Race from returning indefinitely. Parent cancellation
// observed before result commit takes precedence over a winner or child errors;
// Race still waits for every admitted node before returning the parent cause.
//
// Race is the disjunction concurrency primitive — the "first success wins" twin
// of [Map]'s "wait for all". It cannot be expressed by a wait-for-all map, so it
// is a primitive rather than a derived helper.
func Race[I, O any](nodes ...Node[I, O]) Node[I, O] {
	return raceNode[I, O]{nodes: slices.Clone(nodes)}
}

type raceNode[I, O any] struct {
	nodes []Node[I, O]
}

func (r raceNode[I, O]) Run(ctx context.Context, input I) (O, error) {
	var zero O
	if err := r.Validate(); err != nil {
		return zero, err
	}
	if err := context.Cause(ctx); err != nil {
		return zero, err
	}

	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	run := raceRun[O]{
		cancel:  cancel,
		results: r.startNodes(raceCtx, input),
		errs:    make([]error, len(r.nodes)),
	}
	return run.waitForAll(ctx)
}

func (r raceNode[I, O]) Validate() error {
	if len(r.nodes) == 0 {
		return ErrNoNodes
	}
	for index, node := range r.nodes {
		if err := Validate(node); err != nil {
			return &IndexError{Index: index, Err: err}
		}
	}
	return nil
}

func (r raceNode[I, O]) startNodes(ctx context.Context, input I) <-chan raceResult[O] {
	results := make(chan raceResult[O], len(r.nodes))
	for index, node := range r.nodes {
		go func() {
			value, err := RunChild(ctx, node, input)
			results <- raceResult[O]{index: index, value: value, err: err}
		}()
	}
	return results
}

type raceResult[O any] struct {
	index int
	value O
	err   error
}

// raceRun owns result collection for one race. It drains every started node,
// even after a winner or parent cancellation, so Run owns every goroutine it
// started.
type raceRun[O any] struct {
	cancel  context.CancelFunc
	results <-chan raceResult[O]
	errs    []error
	winner  O
	won     bool
}

// waitForAll is where the outcome is chosen, which is why parent cancellation is
// checked here and takes no part in the drain: it already reaches the nodes
// through the context this race derived, and deciding it twice would state one
// rule in two places that nothing outside can tell apart.
func (r *raceRun[O]) waitForAll(parent context.Context) (O, error) {
	for range len(r.errs) {
		r.record(<-r.results)
	}

	var zero O
	if err := context.Cause(parent); err != nil {
		return zero, err
	}
	if r.won {
		return r.winner, nil
	}
	return zero, errors.Join(r.errs...)
}

func (r *raceRun[O]) record(result raceResult[O]) {
	if r.won {
		return
	}
	if result.err == nil {
		r.winner = result.value
		r.won = true
		r.cancel()
		return
	}
	r.errs[result.index] = &IndexError{Index: result.index, Err: result.err}
}
