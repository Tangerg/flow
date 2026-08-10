package workflow

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/Tangerg/flow"
)

type definitionKind uint8

const (
	definitionNamed definitionKind = iota
	definitionSteps
	definitionBranch
	definitionLoop
	definitionIteration
	definitionSubgraph
	definitionGraph
)

// stepDefinition is the package-private execution shape of a built-in Step.
// It deliberately stays separate from Description: introspection is a public
// presentation API and must not become part of execution correctness.
type stepDefinition struct {
	kind definitionKind
	id   string
	// output reports whether this sealed boundary publishes Output(id). It is
	// meaningful for the kinds accepted by nodeBoundary and lets compilation
	// verify that registered schema metadata does not promise or hide a value.
	output bool
	steps  stepList
	cases  map[string]Step
	body   Step
	// inputs and bodyOutput describe collection boundaries. They stay out of the
	// public Description tree but let static validation reject a result that a
	// completely visible body cannot produce.
	inputs     Inputs
	bodyOutput Ref
}

// definedStep can only be implemented inside this package. Its two methods keep
// local construction checks and recursive identity checks in one traversal.
// Caller-defined steps remain structurally opaque, but their own optional
// Validate method is still honored through flow.Validate. Any built-in
// boundaries they hide validate themselves and claim their execution identities
// at run time.
type definedStep interface {
	Step
	Describer
	validate() error
	definition() stepDefinition
}

// nodeBoundary reports whether the definition exposes one Store-sealed node to
// a Graph or leaf Spec. Named leaves and waits own no cells beyond their ID;
// Iteration collects its element body without exposing its Store, while
// Subgraph runs its body in an isolated Store. Structural, branch, loop, and
// graph definitions can expose child cells and must cross a registry boundary
// through Subgraph instead.
func (s stepDefinition) nodeBoundary() bool {
	switch s.kind {
	case definitionNamed, definitionIteration, definitionSubgraph:
		return true
	default:
		return false
	}
}

type definitionValidator struct {
	ids map[string]struct{}
}

// validateDefinition checks local construction and the static shape visible
// from one composite boundary. At a caller-defined Step it honors that Step's
// own validation contract but cannot traverse its structure; traversal begins
// again when the Step invokes another built-in composite. Caching one verdict
// for the whole run would let an opaque boundary suppress that inner
// composite's validation and turn a construction error into partial execution.
func validateDefinition(step Step) error {
	validator := definitionValidator{}
	return normalizeValidationError(validator.validate(step))
}

// validateNode is the only bridge from flow's generic validation convention to
// workflow definition semantics. In particular, a workflow validator cannot
// turn immutable definition inspection into a resumable run-time outcome.
func validateNode[I, O any](node flow.Node[I, O]) error {
	return normalizeValidationError(flow.Validate(node))
}

func normalizeValidationError(err error) error {
	if !errors.Is(err, ErrSuspended) {
		return err
	}
	// Validation inspects immutable definition state and cannot legitimately
	// wait for a run-time value. Keep a broken validator from turning an invalid
	// definition into a resumable third outcome at an enclosing composite.
	return fmt.Errorf(
		"%w: validation returned a suspension: %s",
		flow.ErrInvalidConfig,
		err.Error(),
	)
}

func (d *definitionValidator) validate(step Step) error {
	d.ids = make(map[string]struct{})
	return d.validateStep(step, 0)
}

func (d *definitionValidator) validateStep(
	step Step,
	depth int,
) error {
	if depth > MaxNestingDepth {
		return fmt.Errorf(
			"%w: workflow definition depth exceeds limit %d",
			ErrMaxDepth,
			MaxNestingDepth,
		)
	}
	defined, ok := step.(definedStep)
	if !ok {
		return validateNode(step)
	}
	if err := defined.validate(); err != nil {
		return err
	}
	return d.validateShape(defined.definition(), depth)
}

func (d *definitionValidator) validateShape(
	definition stepDefinition,
	depth int,
) error {
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
		// Each element inherits the outer Store but has its own Journal scope and
		// publishes only the collected iteration output.
		bodyValidator := definitionValidator{ids: make(map[string]struct{})}
		if err := bodyValidator.validateStep(definition.body, depth+1); err != nil {
			return err
		}
		return definition.validateIterationOutput()
	case definitionSubgraph:
		if err := d.claim(definition.id); err != nil {
			return err
		}
		// A subgraph body has an isolated Store and a scope derived from the
		// subgraph ID, so its execution identities are local to that instance.
		bodyValidator := definitionValidator{ids: make(map[string]struct{})}
		if err := bodyValidator.validateStep(definition.body, depth+1); err != nil {
			return err
		}
		return definition.validateSubgraphOutput()
	case definitionGraph:
		// A Graph may contain gates, so a successful run does not promise that
		// every declared node produced an output. Its own compiler validates the
		// visible node definitions and execution identities.
		return d.validateSteps(definition.steps, depth+1)
	default:
		return d.claim(definition.id)
	}
}

func (s stepDefinition) validateSubgraphOutput() error {
	outputs, known := guaranteedOutputs(s.body)
	if !known {
		return nil
	}
	if subgraphOutputGuaranteed(s.inputs, outputs, s.bodyOutput) {
		return nil
	}
	return &StepError{
		ID: s.id,
		Op: OpValidate,
		Err: fmt.Errorf(
			"%w: subgraph body output %s is not produced by its sealed body or inputs",
			flow.ErrInvalidConfig,
			s.bodyOutput,
		),
	}
}

func (s stepDefinition) validateIterationOutput() error {
	outputs, known := guaranteedOutputs(s.body)
	if !known {
		return nil
	}
	if iterationOutputGuaranteed(s.id, outputs, s.bodyOutput) {
		return nil
	}
	return &StepError{
		ID: s.id,
		Op: OpValidate,
		Err: fmt.Errorf(
			"%w: iteration body output %s is not produced by its visible body and is not a valid item or index value",
			flow.ErrInvalidConfig,
			s.bodyOutput,
		),
	}
}

// subgraphOutputGuaranteed and iterationOutputGuaranteed are the shared
// projection rules for code-built definitions and serialized Specs. Keeping
// the rules independent of either representation prevents validation and
// compilation from accepting different data-flow contracts.
func subgraphOutputGuaranteed(inputs Inputs, outputs nodeSet, ref Ref) bool {
	_, seed := inputs[ref.NodeID]
	_, produced := outputs[ref.NodeID]
	return (seed || produced) && ref.withinOutput()
}

func iterationOutputGuaranteed(id string, outputs nodeSet, ref Ref) bool {
	if TypeAny.acceptsCellPath(ref, id, itemKey) ||
		TypeNumber.acceptsCellPath(ref, id, indexKey) {
		return true
	}
	_, produced := outputs[ref.NodeID]
	return produced && ref.withinOutput()
}

// guaranteedOutputs returns the conventional output cells that a successful
// built-in definition must add, independently of the Store it receives. The
// bool is false at an opaque caller-defined boundary or a conditional Graph,
// where rejecting an output would require guessing about code this package
// cannot see.
func guaranteedOutputs(step Step) (nodeSet, bool) {
	defined, ok := step.(definedStep)
	if !ok {
		return nil, false
	}
	definition := defined.definition()
	switch definition.kind {
	case definitionNamed:
		outputs := make(nodeSet)
		if definition.output {
			outputs[definition.id] = struct{}{}
		}
		return outputs, true
	case definitionSteps:
		return guaranteedStepListOutputs(definition.steps)
	case definitionBranch:
		return guaranteedBranchOutputs(definition.cases)
	case definitionLoop:
		return guaranteedOutputs(definition.body)
	case definitionIteration, definitionSubgraph:
		return nodeSet{definition.id: {}}, true
	case definitionGraph:
		return nil, false
	default:
		return nil, false
	}
}

func guaranteedStepListOutputs(steps stepList) (nodeSet, bool) {
	outputs := make(nodeSet)
	for _, step := range steps {
		child, known := guaranteedOutputs(step)
		if !known {
			return nil, false
		}
		maps.Copy(outputs, child)
	}
	return outputs, true
}

func guaranteedBranchOutputs(cases map[string]Step) (nodeSet, bool) {
	var common nodeSet
	for _, name := range slices.Sorted(maps.Keys(cases)) {
		outputs, known := guaranteedOutputs(cases[name])
		if !known {
			return nil, false
		}
		if common == nil {
			common = outputs
			continue
		}
		for id := range common {
			if _, present := outputs[id]; !present {
				delete(common, id)
			}
		}
	}
	return common, true
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
	if _, duplicate := d.ids[id]; duplicate {
		return &StepError{ID: id, Op: OpValidate, Err: ErrDuplicateStep}
	}
	d.ids[id] = struct{}{}
	return nil
}
