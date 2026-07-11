package workflow

import (
	"context"
	"slices"

	"github.com/Tangerg/flow"
)

// Parallel runs every branch concurrently on the same input Store and merges
// their resulting Stores into one. Because the Store structure is persistent,
// branches can safely share it when stored values obey Store's immutability
// contract. The first branch to fail cancels the rest and its error is returned;
// already-running branches must cooperate with context cancellation.
//
// Parallel merges only cells actually written by each branch; cells merely
// inherited from the input snapshot cannot overwrite another branch's work. On
// a same-cell conflict a later branch's value wins. Parallel derives its fan-out
// from flow.Map applied to the branches as data.
func Parallel(branches ...Step) Step {
	return parallelN(0, branches)
}

// ParallelN is like [Parallel] but runs at most limit branches concurrently. A
// non-positive limit is unbounded.
func ParallelN(limit int, branches ...Step) Step {
	return parallelN(limit, branches)
}

func parallelN(limit int, branches []Step) Step {
	return parallel{branches: slices.Clone(branches), limit: limit}
}

// parallel is the [Step] produced by [Parallel].
type parallel struct {
	branches []Step
	limit    int
}

func (p parallel) Run(ctx context.Context, s Store) (Store, error) {
	switch len(p.branches) {
	case 0:
		return s, ctx.Err()
	case 1:
		if err := ctx.Err(); err != nil {
			return s, err
		}
		result, err := runStep(ctx, p.branches[0], s)
		if err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return s, contextErr
			}
			return s, &flow.IndexError{Index: 0, Err: err}
		}
		if err := ctx.Err(); err != nil {
			return s, err
		}
		return mergeStores(s, result), nil
	}

	branchInput := s
	if branchInput.depth >= storeOverlayLimit {
		branchInput = branchInput.compact()
	}
	mapper := flow.Map[Step, Store](branchRunner{input: branchInput})
	if p.limit > 0 {
		mapper = flow.MapN[Step, Store](p.limit, branchRunner{input: branchInput})
	}
	results, err := mapper.Run(ctx, p.branches)
	if err != nil {
		return s, err
	}
	return mergeStores(branchInput, results...), nil
}

type branchRunner struct {
	input Store
}

func (r branchRunner) Run(ctx context.Context, branch Step) (Store, error) {
	return runStep(ctx, branch, r.input)
}

func (p parallel) Describe() Description {
	return Description{Kind: "parallel", Children: describeAll(p.branches)}
}

// mergeStores returns a new Store containing base plus each branch's writes. On
// a same-cell conflict a later branch wins.
func mergeStores(base Store, others ...Store) Store {
	out := base
	var baseData map[storeKey]cell
	for _, other := range others {
		if other.snapshot == base.snapshot && other.delta != nil && other.delta.parent == base.delta {
			if out.snapshot == base.snapshot && out.delta == base.delta {
				out = other
			} else {
				out = out.withDelta(other.delta.key, other.delta.cell)
			}
			continue
		}
		if writes, ok := deltaWritesSince(base, other); ok {
			for _, write := range writes {
				out = out.withDelta(write.key, write.cell)
			}
			continue
		}

		// A branch may return a Store unrelated to its input or compact a long
		// overlay. Fall back to revision comparison in that uncommon case.
		if baseData == nil {
			baseData = base.materialize()
		}
		for identity, candidate := range other.materialize() {
			original, existed := baseData[identity]
			if existed && candidate.revision == original.revision {
				continue
			}
			out = out.withDelta(identity, candidate)
		}
	}
	if out.depth > storeOverlayLimit*2 {
		return out.compact()
	}
	return out
}

// deltaWritesSince returns the final write to each cell changed by other after
// base. It succeeds when both Stores share a snapshot and other's overlay
// descends from base's overlay.
func deltaWritesSince(base, other Store) ([]*storeDelta, bool) {
	if other.snapshot != base.snapshot {
		return nil, false
	}

	var writes []*storeDelta
	for delta := other.delta; delta != base.delta; delta = delta.parent {
		if delta == nil {
			return nil, false
		}
		seen := false
		for _, write := range writes {
			if write.key == delta.key {
				seen = true
				break
			}
		}
		if !seen {
			writes = append(writes, delta)
		}
	}
	slices.Reverse(writes)
	return writes, true
}
