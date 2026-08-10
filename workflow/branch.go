package workflow

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/Tangerg/flow"
)

// Branch routes the Store to one of several steps. It runs resolve to pick a
// branch name from the Store, then runs the step registered under that name. If
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
// id names the branch for that record and for [Describe]; it must be unique
// among steps that can run in the same execution. Describe lists cases by name,
// independent of map iteration order.
func Branch(id string, resolve Resolver, cases map[string]Step) Step {
	return branchStep{id: id, resolve: resolve, cases: maps.Clone(cases)}
}

// branchStep is the [Step] produced by [Branch].
type branchStep struct {
	id      string
	resolve Resolver
	cases   map[string]Step
}

func (b branchStep) Run(ctx context.Context, s Store) (Store, error) {
	ctx = ensureRun(ctx)
	if err := b.Validate(); err != nil {
		return s, err
	}
	if err := runFrom(ctx).claim(scope(ctx), b.id); err != nil {
		return s, newStepError(ctx, b.id, OpValidate, err)
	}
	if contextErr := context.Cause(ctx); contextErr != nil {
		return s, contextErr
	}

	name, replayed, err := b.decide(ctx, s)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return s, contextErr
	}
	if err != nil {
		return s, err
	}
	step, ok := b.cases[name]
	if !ok {
		return s, newStepError(
			ctx,
			b.id,
			OpRun,
			fmt.Errorf("%w: resolver selected %q", flow.ErrNoCase, name),
		)
	}
	// A decision is durable only after it names an actual case. Recording an
	// unknown name would poison the Journal and make every later run fail before
	// the resolver had a chance to recover.
	if !replayed {
		journalErr := runFrom(ctx).journal().record(scope(ctx), b.id, name)
		if contextErr := context.Cause(ctx); contextErr != nil {
			return s, contextErr
		}
		if journalErr != nil {
			return s, newStepError(ctx, b.id, OpRun, journalErr)
		}
	}
	if contextErr := context.Cause(ctx); contextErr != nil {
		return s, contextErr
	}
	result, err := step.Run(ctx, s)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return s, contextErr
	}
	return result, err
}

func (b branchStep) validate() error {
	if err := validateStepID(b.id); err != nil {
		return &StepError{ID: b.id, Op: OpValidate, Err: err}
	}
	if err := validateNode(b.resolve); err != nil {
		return &StepError{ID: b.id, Op: OpValidate, Err: err}
	}
	if len(b.cases) == 0 {
		return &StepError{
			ID:  b.id,
			Op:  OpValidate,
			Err: fmt.Errorf("%w: branch requires at least one case", flow.ErrInvalidConfig),
		}
	}
	for _, name := range slices.Sorted(maps.Keys(b.cases)) {
		if err := validateName("branch case name", name); err != nil {
			return &StepError{
				ID:  b.id,
				Op:  OpValidate,
				Err: fmt.Errorf("%w: %w", flow.ErrInvalidConfig, err),
			}
		}
		if isNilNode(b.cases[name]) {
			return &StepError{
				ID:  b.id,
				Op:  OpValidate,
				Err: fmt.Errorf("case %q: %w", name, ErrNilStep),
			}
		}
	}
	return nil
}

func (b branchStep) Validate() error { return validateDefinition(b) }

// decide returns the branch to take, reusing the recorded decision when the run
// is resuming. Run records a fresh decision only after verifying the case.
func (b branchStep) decide(ctx context.Context, s Store) (string, bool, error) {
	recorded, replayed, err := runFrom(ctx).replay(ctx, scope(ctx), b.id)
	if err != nil {
		return "", false, err
	}
	if replayed {
		if name, ok := recorded.(string); ok {
			return name, true, nil
		}
		return "", false, newStepError(
			ctx,
			b.id,
			OpRun,
			fmt.Errorf(
				"%w: journaled branch decision has type %T; want string",
				ErrTypeMismatch,
				recorded,
			),
		)
	}

	name, err := b.resolve.Run(ctx, s)
	if err != nil {
		if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
			return "", false, suspensions.identify(b.id, scope(ctx)).err()
		}
		return "", false, newStepError(ctx, b.id, OpRun, err)
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
