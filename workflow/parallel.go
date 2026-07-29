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
// every branch concurrently. Before running, it rejects nil branches and
// duplicate IDs in steps built by this package. Runtime identity checks cover
// IDs hidden inside caller-defined steps.
func Parallel(branches []Step, cfg ParallelConfig) Step {
	return parallelStep{branches: stepList(slices.Clone(branches)), limit: cfg.Concurrency}
}

// parallelStep is the [Step] produced by [Parallel].
type parallelStep struct {
	branches stepList
	limit    int
}

func (p parallelStep) Run(ctx context.Context, s Store) (Store, error) {
	ctx = ensureRun(ctx)
	if err := p.validate(); err != nil {
		return s, err
	}
	if err := runFrom(ctx).validateDefinition(p); err != nil {
		return s, err
	}
	if err := ctx.Err(); err != nil {
		return s, err
	}
	switch len(p.branches) {
	case 0:
		return s, nil
	case 1:
		return p.runOne(ctx, s)
	default:
		return p.runMany(ctx, s)
	}
}

func (p parallelStep) validate() error {
	if err := p.branches.validate(); err != nil {
		return err
	}
	if p.limit < 0 {
		return fmt.Errorf(
			"%w: concurrency must be non-negative, got %d",
			flow.ErrInvalidConfig,
			p.limit,
		)
	}
	return nil
}

func (p parallelStep) runOne(ctx context.Context, s Store) (Store, error) {
	result, err := p.branches[0].Run(ctx, s)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return s, contextErr
		}
		if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
			return s.merge(result), suspensions.err()
		}
		return s, &flow.IndexError{Index: 0, Err: err}
	}
	if err := ctx.Err(); err != nil {
		return s, err
	}
	return s.merge(result), nil
}

func (p parallelStep) runMany(ctx context.Context, s Store) (Store, error) {
	branchInput := s
	if branchInput.depth >= storeOverlayLimit {
		branchInput = branchInput.compact()
	}

	// A suspension comes back as a value so it does not cancel the siblings; a
	// real failure is still returned as an error, which keeps flow.Map's
	// fail-fast cancellation exactly as it was.
	mapper := flow.Map(
		branchRunner{input: branchInput},
		flow.MapConfig{Concurrency: p.limit},
	)
	outcomes, err := mapper.Run(ctx, p.branches)
	if err != nil {
		return s, err
	}

	completed := make([]Store, 0, len(outcomes))
	var suspensions suspensionList
	for _, outcome := range outcomes {
		completed = append(completed, outcome.store)
		suspensions = append(suspensions, outcome.suspensions...)
	}
	// Merge what finished even when something is still waiting, so the run that
	// resumes sees the completed work instead of repeating it.
	return branchInput.merge(completed...), suspensions.err()
}

// branchOutcome is one branch's result. A suspension travels as a value because
// it is not a failure; anything else travels as this node's error.
type branchOutcome struct {
	store       Store
	suspensions suspensionList
}

type branchRunner struct {
	input Store
}

func (r branchRunner) Run(ctx context.Context, branch Step) (branchOutcome, error) {
	result, err := branch.Run(ctx, r.input)
	if err == nil {
		return branchOutcome{store: result}, nil
	}
	if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
		return branchOutcome{store: result, suspensions: suspensions}, nil
	}
	return branchOutcome{}, err
}

func (p parallelStep) Describe() Description {
	return Description{Kind: "parallel", Children: p.branches.describe()}
}

func (p parallelStep) workflowDefinition() stepDefinition {
	return stepDefinition{kind: definitionSteps, steps: p.branches}
}
