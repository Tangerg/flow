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
//
// The chosen name is recorded in the run's [Journal], and a run that resumes
// takes the recorded branch without calling resolve again. That matters because
// a resolver need not be a pure function of the Store — a classifier or a model
// may answer differently the second time — and a resumed run that took the other
// branch would leave outputs from both in the Store. Recording the decision also
// spares the second call. A resolver result that names no case is not recorded,
// so an invalid transient result cannot poison later runs. A nil case is
// rejected before the resolver runs.
//
// id names the branch for that record and for [Describe]; it must be unique
// among steps that can run in the same execution.
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
	if b.id == "" {
		return s, &StepError{ID: b.id, Op: OpValidate, Err: ErrInvalidStepID}
	}
	if b.resolve == nil {
		return s, &StepError{ID: b.id, Op: OpValidate, Err: flow.ErrNilFunc}
	}
	for _, name := range slices.Sorted(maps.Keys(b.cases)) {
		if b.cases[name] == nil {
			return s, &StepError{
				ID:  b.id,
				Op:  OpValidate,
				Err: fmt.Errorf("case %q: %w", name, ErrNilStep),
			}
		}
	}
	if err := runFrom(ctx).validateDefinition(b); err != nil {
		return s, err
	}
	if err := runFrom(ctx).claim(scope(ctx), b.id); err != nil {
		return s, &StepError{ID: b.id, Op: OpValidate, Err: err}
	}

	name, replayed, err := b.decide(ctx, s)
	if err != nil {
		return s, err
	}
	step, ok := b.cases[name]
	if !ok {
		return s, &StepError{
			ID:  b.id,
			Op:  OpRun,
			Err: fmt.Errorf("%w: resolver selected %q", flow.ErrNoCase, name),
		}
	}
	// A decision is durable only after it names an actual case. Recording an
	// unknown name would poison the Journal and make every later run fail before
	// the resolver had a chance to recover.
	if !replayed {
		if err := runFrom(ctx).journal().record(scope(ctx), b.id, name); err != nil {
			return s, &StepError{ID: b.id, Op: OpRun, Err: err}
		}
	}
	return step.Run(ctx, s)
}

// decide returns the branch to take, reusing the recorded decision when the run
// is resuming. Run records a fresh decision only after verifying the case.
func (b branchStep) decide(ctx context.Context, s Store) (string, bool, error) {
	if recorded, ok := runFrom(ctx).replay(scope(ctx), b.id); ok {
		if name, ok := recorded.(string); ok {
			return name, true, nil
		}
		return "", false, &StepError{
			ID: b.id,
			Op: OpRun,
			Err: fmt.Errorf(
				"%w: journaled branch decision has type %T; want string",
				ErrTypeMismatch,
				recorded,
			),
		}
	}

	name, err := b.resolve(ctx, s)
	if err != nil {
		if suspensions, only := (suspensionTree{err: err}).suspensions(); only {
			return "", false, suspensions.identify(b.id, scope(ctx)).err()
		}
		return "", false, &StepError{ID: b.id, Op: OpRun, Err: err}
	}
	return name, false, nil
}

func (b branchStep) Describe() Description {
	children := make([]Description, 0, len(b.cases))
	for _, name := range slices.Sorted(maps.Keys(b.cases)) {
		d := Describe(b.cases[name])
		d.Label = name
		children = append(children, d)
	}
	return Description{ID: b.id, Kind: "branch", Children: children}
}

func (b branchStep) workflowDefinition() stepDefinition {
	return stepDefinition{
		kind:  definitionBranch,
		id:    b.id,
		cases: b.cases,
	}
}
