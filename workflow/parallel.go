package workflow

import (
	"context"
	"maps"

	"github.com/Tangerg/flow/core"
)

// Parallel runs every branch concurrently on the same input Store and merges
// their resulting Stores into one. Because the Store is immutable, the branches
// share the input safely and never race. The first branch to fail cancels the
// rest and its error is returned; bound the concurrency with
// core.WithConcurrency.
//
// Branches should write under distinct node IDs; on a key collision a later
// branch's value wins. Parallel derives its fan-out from core.Map applied to the
// branches as data.
func Parallel(branches []Step, opts ...core.MapOption) Step {
	return core.Func[Store, Store](func(ctx context.Context, s Store) (Store, error) {
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
}

// mergeStores returns a new Store combining base and others. On a key collision
// a later store wins.
func mergeStores(base Store, others ...Store) Store {
	out := make(map[string]map[string]any)
	add := func(src Store) {
		for nodeID, inner := range src.data {
			dst := out[nodeID]
			if dst == nil {
				dst = make(map[string]any, len(inner))
				out[nodeID] = dst
			}
			maps.Copy(dst, inner)
		}
	}
	add(base)
	for _, o := range others {
		add(o)
	}
	return Store{data: out}
}
