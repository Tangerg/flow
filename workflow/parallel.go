package workflow

import (
	"context"
	"maps"
	"slices"

	"github.com/Tangerg/flow/core"
)

// Parallel runs every branch concurrently on the same input Store and merges
// their resulting Stores into one. Because the Store structure is persistent,
// branches can safely share it when stored values obey Store's immutability
// contract. The first branch to fail cancels the rest and its error is returned;
// already-running branches must cooperate with context cancellation. Bound the
// concurrency with core.WithConcurrency.
//
// Parallel merges only cells actually written by each branch; cells merely
// inherited from the input snapshot cannot overwrite another branch's work. On
// a same-cell conflict a later branch's value wins. Parallel derives its fan-out
// from core.Map applied to the branches as data.
func Parallel(branches []Step, opts ...core.MapOption) Step {
	branches = slices.Clone(branches)
	p := parallel{branches: branches}
	p.node = core.Func[Store, Store](func(ctx context.Context, s Store) (Store, error) {
		results, err := core.Map(
			core.Func[Step, Store](func(ctx context.Context, b Step) (Store, error) {
				return runStep(ctx, b, s)
			}),
			opts...,
		).Run(ctx, branches)
		if err != nil {
			return s, err
		}
		return mergeStores(s, results...), nil
	})
	return p
}

// parallel is the [Step] produced by [Parallel].
type parallel struct {
	branches []Step
	node     Step
}

func (p parallel) Run(ctx context.Context, s Store) (Store, error) { return p.node.Run(ctx, s) }

func (p parallel) Describe() Description {
	return Description{Kind: "parallel", Children: describeAll(p.branches)}
}

// mergeStores returns a new Store containing base plus each branch's writes. On
// a same-cell conflict a later branch wins.
func mergeStores(base Store, others ...Store) Store {
	out := maps.Clone(base.data)
	if out == nil {
		out = make(map[string]map[string]cell)
	}
	cloned := make(map[string]bool, len(others))

	for _, other := range others {
		for nodeID, inner := range other.data {
			for key, candidate := range inner {
				original, existed := base.data[nodeID][key]
				if existed && candidate.revision == original.revision {
					continue // inherited from base; this branch did not write it
				}
				dst := out[nodeID]
				if !cloned[nodeID] {
					dst = maps.Clone(dst)
					if dst == nil {
						dst = make(map[string]cell, 1)
					}
					out[nodeID] = dst
					cloned[nodeID] = true
				}
				dst[key] = candidate // branch write; later branches win conflicts
			}
		}
	}
	return Store{data: out}
}
