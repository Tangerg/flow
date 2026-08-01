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

// definedStep can only be implemented inside this package. Caller-defined
// steps remain opaque and are covered by runState.claim at execution time.
type definedStep interface {
	definition() stepDefinition
}

type definitionValidator struct {
	ids map[string]struct{}
}

func (d *definitionValidator) validate(step Step) error {
	d.ids = make(map[string]struct{})
	return d.validateStep(step, 0)
}

func (d *definitionValidator) validateStep(
	step Step,
	depth int,
) error {
	if depth >= MaxNestingDepth {
		return fmt.Errorf(
			"%w: workflow definition depth exceeds limit %d",
			ErrMaxDepth,
			MaxNestingDepth,
		)
	}
	defined, ok := step.(definedStep)
	if !ok {
		return nil
	}
	definition := defined.definition()

	switch definition.kind {
	case definitionSteps:
		return d.validateSteps(definition.steps, depth+1)
	case definitionBranch:
		if err := d.claim(definition.id); err != nil {
			return err
		}
		return d.validateCases(definition.cases, depth+1)
	case definitionLoop:
		if err := d.claim(definition.id); err != nil {
			return err
		}
		// A loop body is scoped by loop ID and iteration index. Its IDs do not
		// collide with the surrounding workflow or another loop body. The loop
		// ID remains reserved because its stop decision uses the same scope.
		bodyValidator := definitionValidator{
			ids: map[string]struct{}{definition.id: {}},
		}
		return bodyValidator.validateStep(definition.body, depth+1)
	case definitionIteration:
		if err := d.claim(definition.id); err != nil {
			return err
		}
		// An iteration body has its own Store and Journal scope.
		bodyValidator := definitionValidator{ids: make(map[string]struct{})}
		return bodyValidator.validateStep(definition.body, depth+1)
	case definitionSubgraph:
		if err := d.claim(definition.id); err != nil {
			return err
		}
		// A subgraph body has an isolated Store and a scope derived from the
		// subgraph ID, so its execution identities are local to that instance.
		bodyValidator := definitionValidator{ids: make(map[string]struct{})}
		return bodyValidator.validateStep(definition.body, depth+1)
	default:
		return d.claim(definition.id)
	}
}

func (d *definitionValidator) validateSteps(
	steps stepList,
	depth int,
) error {
	for _, step := range steps {
		if err := d.validateStep(step, depth); err != nil {
			return err
		}
	}
	return nil
}

func (d *definitionValidator) validateCases(
	cases map[string]Step,
	depth int,
) error {
	// Only one case runs. Cases may reuse IDs with one another, but every case
	// must remain conflict-free with the path before the branch and with steps
	// that follow it.
	introduced := make(map[string]struct{})
	for _, name := range slices.Sorted(maps.Keys(cases)) {
		caseValidator := definitionValidator{ids: maps.Clone(d.ids)}
		if err := caseValidator.validateStep(cases[name], depth); err != nil {
			return err
		}
		for id := range caseValidator.ids {
			if _, existed := d.ids[id]; !existed {
				introduced[id] = struct{}{}
			}
		}
	}
	maps.Copy(d.ids, introduced)
	return nil
}

func (d *definitionValidator) claim(id string) error {
	if id == "" {
		return &StepError{ID: id, Op: OpValidate, Err: ErrInvalidStepID}
	}
	if _, duplicate := d.ids[id]; duplicate {
		return &StepError{ID: id, Op: OpValidate, Err: ErrDuplicateStep}
	}
	d.ids[id] = struct{}{}
	return nil
}
