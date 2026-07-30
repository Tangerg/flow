package workflow

import (
	"fmt"
	"maps"
	"slices"
)

type definitionKind uint8

const (
	definitionNamed definitionKind = iota
	definitionSteps
	definitionBranch
	definitionLoop
	definitionIteration
	definitionSubgraph
)

// stepDefinition is the package-private execution shape of a built-in Step.
// It deliberately stays separate from Description: introspection is a public
// presentation API and must not become part of execution correctness.
type stepDefinition struct {
	kind  definitionKind
	id    string
	steps stepList
	cases map[string]Step
	body  Step
}

// definitionStep can only be implemented inside this package. Caller-defined
// steps remain opaque and are covered by runState.claim at execution time.
type definitionStep interface {
	workflowDefinition() stepDefinition
}

type definitionValidator struct{}

func (d definitionValidator) validate(step Step) error {
	return d.validateStep(
		step,
		make(map[string]struct{}),
		0,
	)
}

func (d definitionValidator) validateStep(
	step Step,
	ids map[string]struct{},
	depth int,
) error {
	if depth >= MaxNestingDepth {
		return fmt.Errorf(
			"%w: workflow definition depth exceeds limit %d",
			ErrMaxDepth,
			MaxNestingDepth,
		)
	}
	defined, ok := step.(definitionStep)
	if !ok {
		return nil
	}
	definition := defined.workflowDefinition()

	switch definition.kind {
	case definitionSteps:
		return d.validateSteps(definition.steps, ids, depth+1)
	case definitionBranch:
		if err := d.claim(definition.id, ids); err != nil {
			return err
		}
		return d.validateCases(definition.cases, ids, depth+1)
	case definitionLoop:
		if err := d.claim(definition.id, ids); err != nil {
			return err
		}
		// A loop body is scoped by loop ID and iteration index. Its IDs do not
		// collide with the surrounding workflow or another loop body. The loop
		// ID remains reserved because its stop decision uses the same scope.
		bodyIDs := map[string]struct{}{definition.id: {}}
		return d.validateStep(definition.body, bodyIDs, depth+1)
	case definitionIteration:
		if err := d.claim(definition.id, ids); err != nil {
			return err
		}
		// An iteration body has its own Store and Journal scope.
		return d.validateStep(
			definition.body,
			make(map[string]struct{}),
			depth+1,
		)
	case definitionSubgraph:
		if err := d.claim(definition.id, ids); err != nil {
			return err
		}
		// A subgraph body has an isolated Store and a scope derived from the
		// subgraph ID, so its execution identities are local to that instance.
		return d.validateStep(
			definition.body,
			make(map[string]struct{}),
			depth+1,
		)
	default:
		return d.claim(definition.id, ids)
	}
}

func (d definitionValidator) validateSteps(
	steps stepList,
	ids map[string]struct{},
	depth int,
) error {
	for _, step := range steps {
		if err := d.validateStep(step, ids, depth); err != nil {
			return err
		}
	}
	return nil
}

func (d definitionValidator) validateCases(
	cases map[string]Step,
	ids map[string]struct{},
	depth int,
) error {
	// Only one case runs. Cases may reuse IDs with one another, but every case
	// must remain conflict-free with the path before the branch and with steps
	// that follow it.
	introduced := make(map[string]struct{})
	for _, name := range slices.Sorted(maps.Keys(cases)) {
		caseIDs := maps.Clone(ids)
		if err := d.validateStep(cases[name], caseIDs, depth); err != nil {
			return err
		}
		for id := range caseIDs {
			if _, existed := ids[id]; !existed {
				introduced[id] = struct{}{}
			}
		}
	}
	maps.Copy(ids, introduced)
	return nil
}

func (definitionValidator) claim(id string, ids map[string]struct{}) error {
	if id == "" {
		return &StepError{ID: id, Op: OpValidate, Err: ErrInvalidStepID}
	}
	if _, duplicate := ids[id]; duplicate {
		return &StepError{ID: id, Op: OpValidate, Err: ErrDuplicateStep}
	}
	ids[id] = struct{}{}
	return nil
}
