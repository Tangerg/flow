package workflow

import (
	"context"
	"slices"

	"github.com/Tangerg/flow"
)

// ParallelConfig configures [Parallel]. A zero Concurrency runs every branch
// concurrently, matching [flow.MapConfig].
type ParallelConfig struct {
	// Steps are the branches to run concurrently on the same input Store.
	Steps []Step
	// Concurrency caps the number of concurrent branches. Zero is unbounded;
	// negative values are invalid.
	Concurrency int
}

// Parallel runs every branch concurrently on the same input Store and merges
// their resulting Stores into one. Because the Store structure is persistent,
// branches can safely share it when stored values obey Store's immutability
// contract.
//
// The first observed branch failure cancels the rest and is returned; when
// branches fail concurrently, completion timing decides which failure is
// observed first. Parallel waits for every admitted branch, so already-running
// branches must cooperate with context cancellation. An ordinary failure or
// parent cancellation returns the input Store; a successful sibling may
// nevertheless have committed a Journal record, which the next run will replay.
// A suspension is not a failure:
// the remaining branches run to completion, every branch that finished has its
// writes merged, and the suspensions are returned together (see [Suspensions]).
// Cancelling work because another branch is waiting would discard it and,
// worse, repeat its side effects on the run that resumes.
//
// Parallel merges only changes descended from the shared input: ordinary
// writes, plus namespace cleanup owned by an engine boundary such as [Graph].
// Cells merely inherited from the input cannot overwrite another branch's
// work, and a merge cannot resurrect a stale cell that a Graph removed. A
// caller-defined branch that returns an unrelated or separately decoded Store
// intentionally presents all of its cells as new writes; cells absent from that
// unrelated Store do not delete the input. On a same-cell conflict the branch
// declared later wins, whichever one finished first, so one definition and one
// input merge the same way however the branches were scheduled.
// A zero [ParallelConfig] runs every branch concurrently.
// Before running, it rejects nil branches and duplicate IDs in steps built by
// this package. Built-in steps hidden inside caller-defined steps validate and
// claim their identities when invoked.
func Parallel(cfg ParallelConfig) Step {
	return parallelStep{
		branches: stepList(slices.Clone(cfg.Steps)),
		limit:    cfg.Concurrency,
	}
}

type parallelStep struct {
	branches stepList
	limit    int
}

func (p parallelStep) Run(ctx context.Context, s Store) (Store, error) {
	ctx = ensureRun(ctx)
	if err := p.Validate(); err != nil {
		return s, err
	}
	if err := context.Cause(ctx); err != nil {
		return s, err
	}
	if len(p.branches) == 0 {
		return s, nil
	}
	return p.runBranches(ctx, s)
}

func (p parallelStep) validate() error {
	if err := p.branches.validate(); err != nil {
		return err
	}
	return (flow.MapConfig{Concurrency: p.limit}).Validate()
}

func (p parallelStep) Validate() error { return validateDefinition(p) }

// runBranches runs every branch, however many there are. One branch takes this
// path too: flow.Map already runs a single element on the calling goroutine
// rather than through an errgroup, so a fast path here would buy two allocations
// and owe a second copy of what a branch outcome means — the suspension
// classification, the merge, and the index a real failure is reported under.
func (p parallelStep) runBranches(ctx context.Context, s Store) (Store, error) {
	branchInput := s.sharedBase()

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
		if contextErr := context.Cause(ctx); contextErr != nil {
			return s, contextErr
		}
		completed = append(completed, outcome.store)
		suspensions = append(suspensions, outcome.suspensions...)
	}
	// Merge what finished even when something is still waiting, so the run that
	// resumes sees the completed work instead of repeating it.
	result := branchInput.merge(completed...)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return s, contextErr
	}
	return result, suspensions.err()
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

func (b branchRunner) Run(ctx context.Context, branch Step) (branchOutcome, error) {
	result, err := branch.Run(ctx, b.input)
	if err == nil {
		return branchOutcome{store: result}, nil
	}
	if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
		return branchOutcome{store: result, suspensions: suspensions}, nil
	}
	return branchOutcome{}, err
}

func (p parallelStep) Describe() Description {
	return Description{Kind: KindParallel, Children: p.branches.describe()}
}

func (p parallelStep) definition() stepDefinition {
	return stepDefinition{kind: definitionSteps, steps: p.branches}
}
