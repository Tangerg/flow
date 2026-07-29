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
// must honor their context.
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

func (race raceNode[I, O]) Run(ctx context.Context, input I) (O, error) {
	var zero O
	if err := race.validate(); err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	run := raceRun[O]{
		cancel:  cancel,
		results: race.startNodes(raceCtx, input),
		errs:    make([]error, len(race.nodes)),
	}
	return run.waitForAll(ctx)
}

func (race raceNode[I, O]) validate() error {
	if len(race.nodes) == 0 {
		return ErrNoNodes
	}
	for index, node := range race.nodes {
		if node == nil {
			return &IndexError{Index: index, Err: ErrNilNode}
		}
	}
	return nil
}

func (race raceNode[I, O]) startNodes(ctx context.Context, input I) <-chan raceResult[O] {
	results := make(chan raceResult[O], len(race.nodes))
	for index, node := range race.nodes {
		go func() {
			value, err := run(ctx, node, input)
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
	cancel    context.CancelFunc
	results   <-chan raceResult[O]
	errs      []error
	winner    O
	won       bool
	parentErr error
}

func (run *raceRun[O]) waitForAll(parent context.Context) (O, error) {
	for range len(run.errs) {
		run.record(run.nextResult(parent))
	}

	var zero O
	if err := parent.Err(); err != nil {
		return zero, err
	}
	if run.parentErr != nil {
		return zero, run.parentErr
	}
	if run.won {
		return run.winner, nil
	}
	return zero, errors.Join(run.errs...)
}

func (run *raceRun[O]) nextResult(parent context.Context) raceResult[O] {
	if run.won || run.parentErr != nil {
		return <-run.results
	}
	select {
	case result := <-run.results:
		return result
	case <-parent.Done():
		run.parentErr = parent.Err()
		run.cancel()
		return <-run.results
	}
}

func (run *raceRun[O]) record(result raceResult[O]) {
	if result.err == nil && !run.won && run.parentErr == nil {
		run.winner = result.value
		run.won = true
		run.cancel()
		return
	}
	if !run.won && run.parentErr == nil {
		run.errs[result.index] = &IndexError{Index: result.index, Err: result.err}
	}
}
