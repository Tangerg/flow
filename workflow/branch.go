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
// spares the second call.
//
// id names the branch for that record and for [Describe]; it must be unique
// among steps that can run in the same execution.
func Branch(id string, resolve Resolver, cases map[string]Step) Step {
	cases = maps.Clone(cases)
	if resolve == nil {
		resolve = func(context.Context, Store) (string, error) { return "", flow.ErrNilFunc }
	}
	return branchStep{id: id, resolve: resolve, cases: cases}
}

// branch is the [Step] produced by [Branch].
type branchStep struct {
	id      string
	resolve Resolver
	cases   map[string]Step
}

func (b branchStep) Run(ctx context.Context, s Store) (Store, error) {
	if b.id == "" {
		return s, &StepError{ID: b.id, Op: OpValidate, Err: ErrInvalidStepID}
	}

	name, err := b.decide(ctx, s)
	if err != nil {
		return s, err
	}
	step, ok := b.cases[name]
	if !ok {
		return s, fmt.Errorf("%w: %q", flow.ErrNoCase, name)
	}
	return runStep(ctx, step, s)
}

// decide returns the branch to take, reusing the recorded decision when the run
// is resuming and recording a fresh one otherwise.
func (b branchStep) decide(ctx context.Context, s Store) (string, error) {
	journal := runFrom(ctx).journal()
	if journal != nil {
		if recorded, ok := journal.lookup(Scope(ctx), b.id); ok {
			if name, ok := recorded.(string); ok {
				return name, nil
			}
			return "", &StepError{
				ID:  b.id,
				Op:  OpRun,
				Err: fmt.Errorf("%w: journaled branch decision is %T, want string", ErrTypeMismatch, recorded),
			}
		}
	}

	name, err := b.resolve(ctx, s)
	if err != nil {
		if suspension := suspensionOf(err); suspension != nil {
			suspension.ID = b.id
			suspension.Path = Scope(ctx)
			return "", suspension
		}
		return "", &StepError{ID: b.id, Op: OpRun, Err: err}
	}
	journal.record(Scope(ctx), b.id, name)
	return name, nil
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
