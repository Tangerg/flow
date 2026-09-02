package workflow

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/Tangerg/flow"
)

// BranchConfig configures [Branch]. Every field is required.
type BranchConfig struct {
	// ID names the branch in the Journal and in [Describe]. It must be unique
	// among steps that can run in the same execution.
	ID string
	// Resolver picks a case name from the Store.
	Resolver Resolver
	// Cases are the steps to choose from, keyed by the name Resolver returns.
	Cases map[string]Step
}

// Branch routes the Store to one of several steps. It runs [BranchConfig.Resolver] to
// pick a branch name from the Store, then runs the step registered under that name. If
// resolve yields a name with no matching case, Run fails (see flow.ErrNoCase).
// Resolver is a flow.Node[Store, string], so typed composition can produce the
// decision without a resolver-specific execution protocol.
//
// The chosen name is recorded in the run's [Journal], and a run that resumes
// takes the recorded branch without calling resolve again. That matters because
// a resolver need not be a pure function of the Store — a classifier or a model
// may answer differently the second time — and a resumed run that took the other
// branch would leave outputs from both in the Store. Recording the decision also
// spares the second call. A resolver result that names no case is not recorded,
// so an invalid transient result cannot poison later runs. A nil case is
// rejected before the resolver runs, as is an empty case set or case name.
//
// Once selected, the case is a transparent child boundary: Branch returns its
// Store and error unchanged. That Store may carry completed state alongside an
// ordinary failure or suspension. Parent cancellation observed when the case
// returns takes precedence and retains the Store from before the case instead.
//
// Describe lists cases by name, independent of map iteration order.
func Branch(cfg BranchConfig) Step {
	return branchStep{
		id:      cfg.ID,
		resolve: cfg.Resolver,
		cases:   maps.Clone(cfg.Cases),
	}
}

// branchStep is the [Step] produced by [Branch].
type branchStep struct {
	id      string
	resolve Resolver
	cases   map[string]Step
}

func (b branchStep) Run(ctx context.Context, s Store) (Store, error) {
	ctx = ensureRun(ctx)
	execution := branchExecution{
		branch: b,
		key:    boundaryKey(ctx, b.id),
		input:  s,
		run:    runFrom(ctx),
	}
	return execution.execute(ctx)
}

// branchExecution owns the selection and persistence boundary of one Branch
// invocation. A case becomes admissible only after its name has been validated
// against the definition and, for a fresh decision, committed to the configured
// Journal when resumption is enabled. key is the identity this invocation is
// known by, taken once because a branch adds no scope of its own: claiming it,
// recording the decision, and naming a wait are all the same boundary.
type branchExecution struct {
	branch branchStep
	key    JournalKey
	input  Store
	run    *runState
}

func (b *branchExecution) execute(ctx context.Context) (Store, error) {
	if err := b.validate(ctx); err != nil {
		return b.input, err
	}
	step, err := b.selectCase(ctx)
	if err != nil {
		return b.input, err
	}
	result, err := step.Run(ctx, b.input)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return b.input, contextErr
	}
	return result, err
}

func (b *branchExecution) validate(ctx context.Context) error {
	if err := b.branch.Validate(); err != nil {
		return err
	}
	if err := b.run.claim(b.key); err != nil {
		return newStepError(ctx, b.branch.id, OpValidate, err)
	}
	return context.Cause(ctx)
}

func (b *branchExecution) selectCase(ctx context.Context) (Step, error) {
	name, replayed, err := b.decide(ctx)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return nil, contextErr
	}
	if err != nil {
		return nil, err
	}
	step, ok := b.branch.cases[name]
	if !ok {
		return nil, newStepError(
			ctx,
			b.branch.id,
			OpRun,
			fmt.Errorf("%w: resolver selected %q", flow.ErrNoCase, name),
		)
	}

	// A decision is durable only after it names an actual case. Recording an
	// unknown name would poison the Journal and make every later run fail before
	// the resolver had a chance to recover.
	if !replayed {
		journalErr := b.run.journal().record(b.key, name)
		if contextErr := context.Cause(ctx); contextErr != nil {
			return nil, contextErr
		}
		if journalErr != nil {
			return nil, newStepError(ctx, b.branch.id, OpRun, journalErr)
		}
	}
	if contextErr := context.Cause(ctx); contextErr != nil {
		return nil, contextErr
	}
	return step, nil
}

func (b branchStep) validate() error {
	if err := validateStepID(b.id); err != nil {
		return newValidationError(b.id, err)
	}
	if err := validateNode(b.resolve); err != nil {
		return newValidationError(b.id, err)
	}
	if len(b.cases) == 0 {
		return newValidationError(
			b.id,
			fmt.Errorf("%w: branch requires at least one case", flow.ErrInvalidConfig))
	}
	for _, name := range slices.Sorted(maps.Keys(b.cases)) {
		if err := validateName(nameBranchCase, name); err != nil {
			return newValidationError(
				b.id,
				fmt.Errorf("%w: %w", flow.ErrInvalidConfig, err))
		}
		if isNilNode(b.cases[name]) {
			return newValidationError(
				b.id,
				fmt.Errorf("case %q: %w", name, ErrNilStep))
		}
	}
	return nil
}

func (b branchStep) Validate() error { return validateDefinition(b) }

// decide returns the branch to take, reusing the recorded decision when the run
// is resuming. Run records a fresh decision only after verifying the case.
func (b *branchExecution) decide(ctx context.Context) (string, bool, error) {
	name, replayed, err := b.run.replayDecision[string](ctx, KindBranch, b.branch.id)
	if err != nil {
		return "", false, err
	}
	if replayed {
		return name, true, nil
	}

	name, err = b.branch.resolve.Run(ctx, b.input)
	if err != nil {
		if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
			return "", false, suspensions.errAt(b.key)
		}
		return "", false, newStepError(ctx, b.branch.id, OpRun, err)
	}
	return name, false, nil
}

func (b branchStep) Describe() Description {
	children := make([]Description, 0, len(b.cases))
	for _, name := range slices.Sorted(maps.Keys(b.cases)) {
		d := describe(b.cases[name])
		d.Label = name
		children = append(children, d)
	}
	return Description{ID: b.id, Kind: KindBranch, Children: children}
}

func (b branchStep) definition() stepDefinition {
	return stepDefinition{
		kind:  definitionBranch,
		id:    b.id,
		cases: b.cases,
	}
}
