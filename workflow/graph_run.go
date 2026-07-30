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
	graphNodeSuspended
)

// graphExecution owns all mutable state of one compiled graph invocation.
// Keeping it separate from graphStep makes the compiled definition safe for
// concurrent reuse without locks.
type graphExecution struct {
	graph  graphStep
	input  Store
	counts []int
	states []graphNodeState
	stores []Store
	writes [][]storeWrite
	ready  []int
	head   int
	active int
}

type graphOutcome struct {
	index int
	input Store
	store Store
	err   error
}

func (g *graphExecution) run(ctx context.Context) (Store, error) {
	if len(g.graph.steps) == 0 {
		return g.input, nil
	}

	g.counts = slices.Clone(g.graph.dependencyCounts)
	g.states = make([]graphNodeState, len(g.graph.steps))
	g.stores = make([]Store, len(g.graph.steps))
	g.writes = make([][]storeWrite, len(g.graph.steps))
	g.ready = make([]int, 0, len(g.graph.steps))
	for index, count := range g.counts {
		if count == 0 {
			g.ready = append(g.ready, index)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	outcomes := make(chan graphOutcome, len(g.graph.steps))
	limit := g.graph.limit
	if limit == 0 || limit > len(g.graph.steps) {
		limit = len(g.graph.steps)
	}

	var failure error
	var suspensions suspensionList
	for {
		if failure == nil && ctx.Err() == nil {
			g.startReady(runCtx, outcomes, limit)
		}
		if g.active == 0 {
			break
		}

		outcome := <-outcomes
		g.active--
		if failure != nil || ctx.Err() != nil {
			continue
		}
		if outcome.err == nil {
			g.complete(outcome)
			continue
		}
		if waiting, only := (suspensionTree{err: outcome.err}).suspensions(); only {
			g.states[outcome.index] = graphNodeSuspended
			g.writes[outcome.index] = outcome.store.writesSince(outcome.input)
			suspensions = append(suspensions, waiting...)
			continue
		}
		failure = outcome.err
		cancel()
	}

	result := g.completedStore()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if failure != nil {
		return result, failure
	}
	return result, suspensions.err()
}

func (g *graphExecution) startReady(
	ctx context.Context,
	outcomes chan<- graphOutcome,
	limit int,
) {
	for g.active < limit && g.head < len(g.ready) {
		index := g.ready[g.head]
		g.head++
		g.states[index] = graphNodeRunning
		g.active++
		input := g.nodeInput(index)
		step := g.graph.steps[index]
		go func() {
			store, err := step.Run(ctx, input)
			outcomes <- graphOutcome{
				index: index,
				input: input,
				store: store,
				err:   err,
			}
		}()
	}
}

func (g *graphExecution) nodeInput(index int) Store {
	dependencies := g.graph.dependencyNodeIndexes[index]
	switch len(dependencies) {
	case 0:
		return g.input
	case 1:
		return g.stores[dependencies[0]]
	}
	stores := make([]Store, 0, len(dependencies))
	for _, dependency := range dependencies {
		stores = append(stores, g.stores[dependency])
	}
	return g.input.merge(stores...)
}

func (g *graphExecution) complete(outcome graphOutcome) {
	g.states[outcome.index] = graphNodeCompleted
	g.stores[outcome.index] = outcome.store
	g.writes[outcome.index] = outcome.store.writesSince(outcome.input)
	for _, dependent := range g.graph.dependentNodeIndexes[outcome.index] {
		g.counts[dependent]--
		if g.counts[dependent] == 0 {
			g.ready = append(g.ready, dependent)
		}
	}
}

func (g *graphExecution) completedStore() Store {
	var writes []storeWrite
	for index := range g.stores {
		if g.states[index] == graphNodeCompleted ||
			g.states[index] == graphNodeSuspended {
			writes = append(writes, g.writes[index]...)
		}
	}
	return g.input.withWrites(writes)
}
