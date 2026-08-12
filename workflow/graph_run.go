package workflow

import (
	"context"
	"slices"
)

type graphNodeState uint8

const (
	graphNodePending graphNodeState = iota
	graphNodeRunning
	graphNodeCompleted
)

// graphExecution owns all mutable state of one compiled graph invocation.
// Keeping it separate from graphStep makes the compiled definition safe for
// concurrent reuse without locks.
type graphExecution struct {
	graph   graphStep
	input   Store
	counts  []int
	states  []graphNodeState
	changes [][]storeChange
	ready   []int
	head    int
	active  int

	failure     error
	suspensions suspensionList
}

type graphOutcome struct {
	index int
	input Store
	store Store
	err   error
}

// graphCall is one admitted scheduler task. It checks cancellation at the last
// possible point before entering caller code, closing the window between
// scheduling a ready node and its goroutine actually starting.
type graphCall struct {
	index int
	input Store
	step  Step
}

func (g graphCall) run(ctx context.Context) graphOutcome {
	if err := context.Cause(ctx); err != nil {
		return graphOutcome{index: g.index, input: g.input, store: g.input, err: err}
	}
	store, err := g.step.Run(ctx, g.input)
	return graphOutcome{index: g.index, input: g.input, store: store, err: err}
}

func (g *graphExecution) run(ctx context.Context) (Store, error) {
	if len(g.graph.steps) == 0 {
		return g.input, nil
	}

	// Every node's input derives from this one, and ready nodes run concurrently.
	g.input = g.input.sharedBase()
	g.counts = slices.Clone(g.graph.dependencyCounts)
	g.states = make([]graphNodeState, len(g.graph.steps))
	g.changes = make([][]storeChange, len(g.graph.steps))
	g.ready = make([]int, 0, len(g.graph.steps))
	for index, count := range g.counts {
		if count == 0 {
			g.ready = append(g.ready, index)
		}
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	outcomes := make(chan graphOutcome, len(g.graph.steps))
	limit := g.graph.limit
	if limit == 0 || limit > len(g.graph.steps) {
		limit = len(g.graph.steps)
	}

	for {
		if g.failure == nil && ctx.Err() == nil {
			g.startReady(runCtx, outcomes, limit)
		}
		if g.active == 0 {
			break
		}

		outcome := <-outcomes
		if g.accept(outcome, ctx.Err() != nil) {
			cancel(g.failure)
		}
	}

	return g.result(ctx)
}

// accept records one finished node. It reports whether this outcome introduced
// the first failure, which tells the scheduler to cancel the remaining calls.
func (g *graphExecution) accept(outcome graphOutcome, parentCanceled bool) bool {
	g.active--
	if g.failure != nil || parentCanceled {
		return false
	}
	if outcome.err == nil {
		g.complete(outcome)
		return false
	}
	if waiting, only := (suspensionTree{err: outcome.err}).suspensions(); only {
		g.suspensions = append(g.suspensions, waiting...)
		return false
	}
	g.failure = outcome.err
	return true
}

func (g *graphExecution) result(ctx context.Context) (Store, error) {
	result := g.completedStore()
	if err := context.Cause(ctx); err != nil {
		return result, err
	}
	if g.failure != nil {
		return result, g.failure
	}
	return result, g.suspensions.err()
}

func (g *graphExecution) startReady(
	ctx context.Context,
	outcomes chan<- graphOutcome,
	limit int,
) {
	for g.active < limit && g.head < len(g.ready) {
		if ctx.Err() != nil {
			return
		}
		index := g.ready[g.head]
		g.head++
		g.states[index] = graphNodeRunning
		g.active++
		call := graphCall{
			index: index,
			input: g.nodeInput(index),
			step:  g.graph.steps[index],
		}
		go func() {
			outcomes <- call.run(ctx)
		}()
	}
}

func (g *graphExecution) nodeInput(index int) Store {
	dependencies := g.graph.dependencyNodeIndexes[index]
	input := g.input
	for _, dependency := range dependencies {
		input = input.withChanges(g.changes[dependency])
	}
	return input
}

func (g *graphExecution) complete(outcome graphOutcome) {
	g.states[outcome.index] = graphNodeCompleted
	g.changes[outcome.index] = outcome.store.changesSince(outcome.input)
	for _, dependent := range g.graph.dependentNodeIndexes[outcome.index] {
		g.counts[dependent]--
		if g.counts[dependent] == 0 {
			g.ready = append(g.ready, dependent)
		}
	}
}

func (g *graphExecution) completedStore() Store {
	var changes []storeChange
	for index := range g.changes {
		if g.states[index] == graphNodeCompleted {
			changes = append(changes, g.changes[index]...)
		}
	}
	return g.input.withChanges(changes)
}
