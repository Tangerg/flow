package workflow

import (
	"context"
	"fmt"
	"slices"

	"github.com/Tangerg/flow"
)

// ParallelConfig configures [Parallel]. Its zero value runs every branch
// concurrently.
type ParallelConfig struct {
	// Concurrency caps the number of branches running at once. Zero is
	// unbounded; negative values are invalid.
	Concurrency int
}

// Parallel runs every branch concurrently on the same input Store and merges
// their resulting Stores into one. Because the Store structure is persistent,
// branches can safely share it when stored values obey Store's immutability
// contract.
//
// The first branch to fail cancels the rest and its error is returned;
// already-running branches must cooperate with context cancellation. A
// suspension is not a failure: the remaining branches run to completion, every
// branch that finished has its writes merged, and the suspensions are returned
// together (see [Suspensions]). Cancelling work because another branch is waiting
// would discard it and, worse, repeat its side effects on the run that resumes.
//
// Parallel merges only cells actually written by each branch; cells merely
// inherited from the input snapshot cannot overwrite another branch's work. On
// a same-cell conflict a later branch's value wins. A zero [ParallelConfig] runs
// every branch concurrently. It rejects a nil branch before running any branch.
func Parallel(branches []Step, cfg ParallelConfig) Step {
	return parallelStep{branches: slices.Clone(branches), limit: cfg.Concurrency}
}

// parallel is the [Step] produced by [Parallel].
type parallelStep struct {
	branches []Step
	limit    int
}

func (p parallelStep) Run(ctx context.Context, s Store) (Store, error) {
	for i, branch := range p.branches {
		if branch == nil {
			return s, &flow.IndexError{Index: i, Err: ErrNilStep}
		}
	}
	if p.limit < 0 {
		return s, fmt.Errorf("%w: negative concurrency", flow.ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return s, err
	}
	switch len(p.branches) {
	case 0:
		return s, nil
	case 1:
		result, err := runStep(ctx, p.branches[0], s)
		if err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return s, contextErr
			}
			if suspensions, only := asSuspensions(err); only {
				return mergeStores(s, result), joinSuspensions(suspensions)
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

	// A suspension comes back as a value so it does not cancel the siblings; a
	// real failure is still returned as an error, which keeps flow.Map's
	// fail-fast cancellation exactly as it was.
	mapper := flow.Map[Step, branchOutcome](branchRunner{input: branchInput}, flow.MapConfig{Concurrency: p.limit})
	outcomes, err := mapper.Run(ctx, p.branches)
	if err != nil {
		return s, err
	}

	var (
		completed   []Store
		suspensions []*Suspension
	)
	for _, outcome := range outcomes {
		completed = append(completed, outcome.store)
		suspensions = append(suspensions, outcome.suspensions...)
	}
	// Merge what finished even when something is still waiting, so the run that
	// resumes sees the completed work instead of repeating it.
	return mergeStores(branchInput, completed...), joinSuspensions(suspensions)
}

// branchOutcome is one branch's result. A suspension travels as a value because
// it is not a failure; anything else travels as this node's error.
type branchOutcome struct {
	store       Store
	suspensions []*Suspension
}

type branchRunner struct {
	input Store
}

func (r branchRunner) Run(ctx context.Context, branch Step) (branchOutcome, error) {
	result, err := runStep(ctx, branch, r.input)
	if err == nil {
		return branchOutcome{store: result}, nil
	}
	if suspensions, only := asSuspensions(err); only {
		return branchOutcome{store: result, suspensions: suspensions}, nil
	}
	return branchOutcome{}, err
}

func (p parallelStep) Describe() Description {
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
		for _, write := range changedStoreWrites(baseData, other.materialize()) {
			out = out.withDelta(write.key, write.cell)
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
