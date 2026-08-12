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

// definitionValidator tracks the execution identities one definition claims. Its
// zero value is the empty state.
//
// The common definition names a single boundary — a leaf validates itself this
// way on every execution — and one claim cannot collide with anything, so the
// first ID stays inline and no set is allocated to hold it. firstID doubles as
// the presence flag: [validateStepID] rejects an empty ID and always runs before
// a claim, so the empty string cannot be one.
type definitionValidator struct {
	firstID string
	ids     definitionIDs
}

// validateDefinition checks local construction and the static shape visible
// from one composite boundary. At a caller-defined Step it honors that Step's
// own validation contract but cannot traverse its structure; traversal begins
// again when the Step invokes another built-in composite. Caching one verdict
// for the whole run would let an opaque boundary suppress that inner
// composite's validation and turn a construction error into partial execution.
func validateDefinition(step Step) error {
	validator := definitionValidator{}
	return normalizeDefinitionError("validation", validator.validate(step))
}

// validateNode is the only bridge from flow's generic validation convention to
// workflow definition semantics. In particular, a workflow validator cannot
// turn immutable definition inspection into a resumable run-time outcome.
func validateNode[I, O any](node flow.Node[I, O]) error {
	return normalizeDefinitionError("validation", flow.Validate(node))
}

// validateBody checks what every named composite holding exactly one body
// shares: a usable execution identity and a present body. [Loop], [Iteration],
// and [Subgraph] each add their own checks after it, so the shared requirement
// is stated once instead of reappearing in each of them.
func validateBody(id string, body Step) error {
	if err := validateStepID(id); err != nil {
		return newValidationError(id, err)
	}
	if isNilNode(body) {
		return newValidationError(id, ErrNilStep)
	}
	return nil
}

// normalizeDefinitionError keeps every definition-construction extension point
// on the same side of the execution boundary. Suspension is meaningful only
// after a run has begun; a validator or factory returning one has produced an
// invalid definition, not a resumable outcome. The original error is rendered
// rather than wrapped so errors.Is cannot misclassify it as ErrSuspended.
func normalizeDefinitionError(source string, err error) error {
	if !errors.Is(err, ErrSuspended) {
		return err
	}
	return fmt.Errorf(
		"%w: %s returned a suspension: %s",
		flow.ErrInvalidConfig,
		source,
		err.Error(),
	)
}

func (d *definitionValidator) validate(step Step) error {
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
		bodyValidator := definitionValidator{firstID: definition.id}
		return bodyValidator.validateStep(definition.body, depth+1)
	case definitionIteration:
		if err := d.claim(definition.id); err != nil {
			return err
		}
		// Each element inherits the outer Store but has its own Journal scope and
		// publishes only the collected iteration output.
		bodyValidator := definitionValidator{}
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
		bodyValidator := definitionValidator{}
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
	outputs := guaranteedOutputs(s.body)
	if !outputs.known {
		return nil
	}
	if subgraphOutputGuaranteed(s.inputs, outputs, s.bodyOutput) {
		return nil
	}
	return newValidationError(
		s.id,
		fmt.Errorf(
			"%w: subgraph body output %s is not produced by its sealed body or inputs",
			flow.ErrInvalidConfig,
			s.bodyOutput,
		))
}

func (s stepDefinition) validateIterationOutput() error {
	outputs := guaranteedOutputs(s.body)
	if !outputs.known {
		return nil
	}
	if iterationOutputGuaranteed(s.id, outputs, s.bodyOutput) {
		return nil
	}
	return newValidationError(
		s.id,
		fmt.Errorf(
			"%w: iteration body output %s is not produced by its visible body and is not a valid item or index value",
			flow.ErrInvalidConfig,
			s.bodyOutput,
		))
}

// subgraphOutputGuaranteed and iterationOutputGuaranteed are the shared
// projection rules for code-built definitions and serialized Specs. Keeping
// the rules independent of either representation prevents validation and
// compilation from accepting different data-flow contracts.
func subgraphOutputGuaranteed(inputs Inputs, outputs outputGuarantee, ref Ref) bool {
	_, seed := inputs[ref.NodeID]
	produced := outputs.contains(ref.NodeID)
	return (seed || produced) && ref.withinOutput()
}

func iterationOutputGuaranteed(id string, outputs outputGuarantee, ref Ref) bool {
	if TypeAny.acceptsCellPath(ref, id, itemKey) ||
		TypeNumber.acceptsCellPath(ref, id, indexKey) {
		return true
	}
	return outputs.contains(ref.NodeID) && ref.withinOutput()
}

// outputGuarantee is the set of conventional output cells a successful
// definition must add. known is false at an opaque extension boundary or a
// conditional graph, where rejecting a projection would require guessing.
// Keeping uncertainty with the set prevents callers from accidentally treating
// an unknown contract as a known empty one.
type outputGuarantee struct {
	nodes nodeSet
	known bool
}

func knownOutputs(ids ...string) outputGuarantee {
	nodes := make(nodeSet, len(ids))
	for _, id := range ids {
		nodes[id] = struct{}{}
	}
	return outputGuarantee{nodes: nodes, known: true}
}

func (o outputGuarantee) contains(id string) bool {
	_, present := o.nodes[id]
	return o.known && present
}

// union combines outputs produced by steps that all run. Unknown is absorbing:
// one opaque child makes the complete guaranteed set unknowable.
func (o outputGuarantee) union(other outputGuarantee) outputGuarantee {
	if !o.known || !other.known {
		return outputGuarantee{}
	}
	nodes := maps.Clone(o.nodes)
	maps.Copy(nodes, other.nodes)
	return outputGuarantee{nodes: nodes, known: true}
}

// intersection keeps outputs produced on either of two mutually exclusive
// paths. Unknown is absorbing for the same reason as union.
func (o outputGuarantee) intersection(other outputGuarantee) outputGuarantee {
	if !o.known || !other.known {
		return outputGuarantee{}
	}
	nodes := make(nodeSet)
	for id := range o.nodes {
		if other.contains(id) {
			nodes[id] = struct{}{}
		}
	}
	return outputGuarantee{nodes: nodes, known: true}
}

// guaranteedOutputs returns the conventional output cells that a successful
// built-in definition must add, independently of the Store it receives. The
// guarantee is unknown at an opaque caller-defined boundary or a conditional
// Graph, where rejecting an output would require guessing about code this
// package cannot see.
func guaranteedOutputs(step Step) outputGuarantee {
	defined, ok := step.(definedStep)
	if !ok {
		return outputGuarantee{}
	}
	definition := defined.definition()
	switch definition.kind {
	case definitionNamed:
		if definition.output {
			return knownOutputs(definition.id)
		}
		return knownOutputs()
	case definitionSteps:
		return guaranteedStepListOutputs(definition.steps)
	case definitionBranch:
		return guaranteedBranchOutputs(definition.cases)
	case definitionLoop:
		return guaranteedOutputs(definition.body)
	case definitionIteration, definitionSubgraph:
		return knownOutputs(definition.id)
	case definitionGraph:
		return outputGuarantee{}
	default:
		return outputGuarantee{}
	}
}

func guaranteedStepListOutputs(steps stepList) outputGuarantee {
	outputs := knownOutputs()
	for _, step := range steps {
		outputs = outputs.union(guaranteedOutputs(step))
	}
	return outputs
}

func guaranteedBranchOutputs(cases map[string]Step) outputGuarantee {
	var common outputGuarantee
	first := true
	for _, name := range slices.Sorted(maps.Keys(cases)) {
		outputs := guaranteedOutputs(cases[name])
		if first {
			common = outputs
			first = false
			continue
		}
		common = common.intersection(outputs)
	}
	if first {
		return knownOutputs()
	}
	return common
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
	claimed := d.claimedIDs()
	introduced := newDefinitionIDs()
	for _, name := range slices.Sorted(maps.Keys(cases)) {
		caseValidator := definitionValidator{ids: claimed.clone()}
		if err := caseValidator.validateStep(cases[name], depth); err != nil {
			return err
		}
		introduced.addAll(claimed.additions(caseValidator.claimedIDs()))
	}
	claimed.addAll(introduced)
	return nil
}

// claimedIDs returns the claimed identities as a set, materializing an inline
// first claim. Only branch cases need one, to clone and diff against, and the
// branch has claimed its own ID by then — so either the set already exists or
// firstID holds that ID, and staying inline would save nothing.
func (d *definitionValidator) claimedIDs() definitionIDs {
	if d.ids == nil {
		d.ids = newDefinitionIDs(d.firstID)
	}
	return d.ids
}

// claim records id as taken, reporting a conflict with one already claimed. The
// caller has validated id, so it is never empty.
func (d *definitionValidator) claim(id string) error {
	switch {
	case d.ids != nil:
		if !d.ids.claim(id) {
			return newValidationError(id, ErrDuplicateStep)
		}
	case d.firstID == "":
		d.firstID = id
	case d.firstID == id:
		return newValidationError(id, ErrDuplicateStep)
	default:
		d.ids = newDefinitionIDs(d.firstID, id)
	}
	return nil
}
