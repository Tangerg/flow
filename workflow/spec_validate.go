package workflow

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/Tangerg/flow"
)

// ValidateSpec checks a nested Spec without building it. It verifies its
// structure, registrations, well-formed references, unique IDs, registered
// node config schemas and ports, statically knowable collection projections,
// nesting up to [MaxNestingDepth], and that each kind carries only fields
// meaningful to that kind. A leaf type without a registered [NodeSchema] keeps
// its output shape unknown until compilation builds the concrete boundary.
func (r *Registry) ValidateSpec(spec Spec) error {
	return r.snapshot().validateSpec(spec)
}

func (r registrySnapshot) validateSpec(root Spec) error {
	validator := specValidator{
		registry: r,
		stepIDs:  newDefinitionIDs(),
	}
	return validator.validate(root)
}

// specValidator owns the recursive state and invariants of nested Spec
// validation. Step IDs remain path-local because mutually exclusive branch
// cases and independently scoped repeated bodies may deliberately reuse an
// output ID.
type specValidator struct {
	registry registrySnapshot
	stepIDs  definitionIDs
	depth    int
}

func (s *specValidator) validate(spec Spec) error {
	if s.depth > MaxNestingDepth {
		return spec.fieldError("", fmt.Errorf(
			"%w: nesting depth %d exceeds limit %d",
			ErrMaxDepth,
			s.depth,
			MaxNestingDepth,
		))
	}

	var (
		allowedFields   []string
		validateVariant func() error
	)
	switch spec.Kind {
	case KindLeaf:
		allowedFields = []string{fieldID, fieldType, fieldConfig, fieldInputs}
		validateVariant = func() error { return s.validateLeaf(spec) }
	case KindSequence:
		allowedFields = []string{fieldSteps}
		validateVariant = func() error { return s.validateSteps(spec.Steps) }
	case KindParallel:
		allowedFields = []string{fieldSteps, fieldConcurrency}
		validateVariant = func() error { return s.validateSteps(spec.Steps) }
	case KindBranch:
		allowedFields = []string{fieldID, fieldResolver, fieldCases}
		validateVariant = func() error { return s.validateBranch(spec) }
	case KindLoop:
		allowedFields = []string{fieldID, fieldBody, fieldCondition, fieldMaxIterations}
		validateVariant = func() error { return s.validateLoop(spec) }
	case KindIteration:
		allowedFields = []string{fieldID, fieldInput, fieldBody, fieldBodyOutput, fieldConcurrency}
		validateVariant = func() error { return s.validateIteration(spec) }
	case KindSubgraph:
		allowedFields = []string{fieldID, fieldInputs, fieldBody, fieldBodyOutput}
		validateVariant = func() error { return s.validateSubgraph(spec) }
	default:
		return spec.fieldError(
			fieldKind,
			fmt.Errorf("unknown kind %q", spec.Kind),
		)
	}

	if field := spec.unexpectedField(allowedFields); field != "" {
		return spec.fieldError(field, fmt.Errorf(
			"field %q is not valid for a %q spec",
			field,
			spec.Kind,
		))
	}
	if err := spec.validateConstraints(); err != nil {
		return err
	}
	return validateVariant()
}

func (s Spec) validateConstraints() error {
	if err := (flow.MapConfig{Concurrency: s.Concurrency}).Validate(); err != nil {
		return s.fieldError(fieldConcurrency, err)
	}
	if err := (flow.LoopConfig{MaxIterations: s.MaxIterations}).Validate(); err != nil {
		return s.fieldError(fieldMaxIterations, err)
	}
	return nil
}

func (s *specValidator) validateSteps(specs []Spec) error {
	child := s.child(s.stepIDs)
	for index, spec := range specs {
		if err := child.validate(spec); err != nil {
			return locateSpecError(err, fieldSteps, strconv.Itoa(index))
		}
	}
	return nil
}

func (s *specValidator) validateLoop(spec Spec) error {
	if err := s.claimID(spec); err != nil {
		return err
	}
	if spec.Body == nil {
		return spec.fieldError(
			fieldBody,
			errors.New("loop body is required"),
		)
	}
	if err := validateName("condition name", spec.Condition); err != nil {
		return spec.fieldError(fieldCondition, err)
	}
	if _, ok := s.registry.lookupCondition(spec.Condition); !ok {
		return spec.fieldError(
			fieldCondition,
			fmt.Errorf("unknown condition %q", spec.Condition),
		)
	}
	// The loop body runs under a scope derived from the loop ID and iteration
	// index, so its IDs are local and may be reused outside this loop. Reserve
	// the loop ID itself because each iteration records the stop decision under
	// that ID in the same scope.
	bodyValidator := s.child(newDefinitionIDs(spec.ID))
	return locateSpecError(bodyValidator.validate(*spec.Body), fieldBody)
}

func (s *specValidator) validateLeaf(spec Spec) error {
	if err := s.claimID(spec); err != nil {
		return err
	}
	if err := validateName("node type", spec.Type); err != nil {
		return spec.fieldError(fieldType, err)
	}
	if _, ok := s.registry.lookupNode(spec.Type); !ok {
		return spec.fieldError(fieldType, fmt.Errorf("%w %q", ErrUnknownNodeType, spec.Type))
	}
	registered, schemaKnown := s.registry.lookupNodeSchema(spec.Type)
	if err := registered.validateConfig(spec.Config); err != nil {
		return spec.fieldError(fieldConfig, err)
	}
	if err := spec.Inputs.validatePorts(); err != nil {
		return spec.fieldError(fieldInputs, err)
	}
	if schemaKnown {
		// A nested Spec has no cross-node index, so ports are checked for
		// completeness only; edge types are checked when a flat Graph names both
		// ends.
		schema := registered.schema
		if err := schema.validateInputs(spec.Inputs, func(Ref) (ValueType, bool) {
			return "", false
		}); err != nil {
			return spec.fieldError(fieldInputs, err)
		}
	}
	return nil
}

func (s *specValidator) validateBranch(spec Spec) error {
	if err := s.claimID(spec); err != nil {
		return err
	}
	if len(spec.Cases) == 0 {
		return spec.fieldError(
			fieldCases,
			errors.New("at least one branch case is required"),
		)
	}
	if err := validateName("resolver name", spec.Resolver); err != nil {
		return spec.fieldError(fieldResolver, err)
	}
	if _, ok := s.registry.lookupResolver(spec.Resolver); !ok {
		return spec.fieldError(
			fieldResolver,
			fmt.Errorf("unknown resolver %q", spec.Resolver),
		)
	}

	// At most one case runs, so sibling cases may reuse an ID. Each case is
	// checked against the path before the branch, and their introduced IDs are
	// visible to the steps after it.
	introduced := newDefinitionIDs()
	for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
		if err := validateName("branch case name", name); err != nil {
			return spec.fieldError(fieldCases, err)
		}
		caseValidator := s.child(s.stepIDs.clone())
		if err := caseValidator.validate(spec.Cases[name]); err != nil {
			return locateSpecError(err, fieldCases, name)
		}
		introduced.addAll(s.stepIDs.additions(caseValidator.stepIDs))
	}
	s.stepIDs.addAll(introduced)
	return nil
}

func (s *specValidator) validateIteration(spec Spec) error {
	if err := s.claimID(spec); err != nil {
		return err
	}
	if spec.Input == (Ref{}) {
		return spec.fieldError(
			fieldInput,
			errors.New("iteration input is required"),
		)
	}
	if spec.Body == nil {
		return spec.fieldError(
			fieldBody,
			errors.New("iteration body is required"),
		)
	}
	if spec.BodyOutput == (Ref{}) {
		return spec.fieldError(
			fieldBodyOutput,
			errors.New("iteration body output is required"),
		)
	}
	if err := spec.Input.Validate(); err != nil {
		return spec.fieldError(fieldInput, err)
	}
	if err := spec.BodyOutput.Validate(); err != nil {
		return spec.fieldError(fieldBodyOutput, err)
	}
	// Each element gets its own Store snapshot and indexed Journal scope. The
	// snapshot retains outer cells; Iteration isolates writes between elements,
	// not the body namespace.
	bodyValidator := s.child(newDefinitionIDs())
	if err := bodyValidator.validate(*spec.Body); err != nil {
		return locateSpecError(err, fieldBody)
	}
	outputs := s.guaranteedOutputs(*spec.Body)
	if outputs.known && !iterationOutputGuaranteed(spec.ID, outputs, spec.BodyOutput) {
		return spec.fieldError(
			fieldBodyOutput,
			fmt.Errorf(
				"%w: iteration body output %s is not produced by its visible body and is not a valid item or index value",
				flow.ErrInvalidConfig,
				spec.BodyOutput,
			),
		)
	}
	return nil
}

func (s *specValidator) validateSubgraph(spec Spec) error {
	if err := s.claimID(spec); err != nil {
		return err
	}
	if spec.Body == nil {
		return spec.fieldError(
			fieldBody,
			errors.New("subgraph body is required"),
		)
	}
	if spec.BodyOutput == (Ref{}) {
		return spec.fieldError(
			fieldBodyOutput,
			errors.New("subgraph body output is required"),
		)
	}
	if err := spec.Inputs.validateSeeds(); err != nil {
		return spec.fieldError(fieldInputs, err)
	}
	if err := spec.BodyOutput.Validate(); err != nil {
		return spec.fieldError(fieldBodyOutput, err)
	}
	bodyValidator := s.child(newDefinitionIDs())
	if err := bodyValidator.validate(*spec.Body); err != nil {
		return locateSpecError(err, fieldBody)
	}
	outputs := s.guaranteedOutputs(*spec.Body)
	if outputs.known && !subgraphOutputGuaranteed(spec.Inputs, outputs, spec.BodyOutput) {
		return spec.fieldError(
			fieldBodyOutput,
			fmt.Errorf(
				"%w: subgraph body output %s is not produced by its sealed body or inputs",
				flow.ErrInvalidConfig,
				spec.BodyOutput,
			),
		)
	}
	return nil
}

// guaranteedOutputs reports the complete set of conventional output cells a
// successful Spec must add. The guarantee is unknown when an unregistered leaf
// schema makes that set unknowable without executing its factory. Validation
// stays conservative at that extension boundary; compilation later checks the
// concrete Step returned by the factory.
func (s *specValidator) guaranteedOutputs(spec Spec) outputGuarantee {
	switch spec.Kind {
	case KindLeaf:
		registered, known := s.registry.lookupNodeSchema(spec.Type)
		if !known {
			return outputGuarantee{}
		}
		if registered.schema.Output != "" {
			return knownOutputs(spec.ID)
		}
		return knownOutputs()
	case KindSequence, KindParallel:
		return s.guaranteedStepOutputs(spec.Steps)
	case KindBranch:
		return s.guaranteedCaseOutputs(spec.Cases)
	case KindLoop:
		if spec.Body == nil {
			return outputGuarantee{}
		}
		return s.guaranteedOutputs(*spec.Body)
	case KindIteration, KindSubgraph:
		return knownOutputs(spec.ID)
	default:
		return outputGuarantee{}
	}
}

func (s *specValidator) guaranteedStepOutputs(specs []Spec) outputGuarantee {
	outputs := knownOutputs()
	for _, spec := range specs {
		outputs = outputs.union(s.guaranteedOutputs(spec))
	}
	return outputs
}

func (s *specValidator) guaranteedCaseOutputs(cases map[string]Spec) outputGuarantee {
	var common outputGuarantee
	first := true
	for _, name := range slices.Sorted(maps.Keys(cases)) {
		outputs := s.guaranteedOutputs(cases[name])
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

func (s *specValidator) child(stepIDs definitionIDs) specValidator {
	return specValidator{
		registry: s.registry,
		stepIDs:  stepIDs,
		depth:    s.depth + 1,
	}
}

func (s *specValidator) claimID(spec Spec) error {
	if err := validateStepID(spec.ID); err != nil {
		return spec.fieldError(fieldID, err)
	}
	if !s.stepIDs.claim(spec.ID) {
		return spec.fieldError(fieldID, ErrDuplicateStep)
	}
	return nil
}

// unexpectedField keeps the programmatic Spec API as strict as the JSON
// schema. A populated field that a kind ignores is almost always a typo, and
// silently accepting it makes code-built and JSON-built workflows disagree.
func (s Spec) unexpectedField(allowed []string) string {
	for _, field := range s.populatedFields() {
		if !slices.Contains(allowed, field) {
			return field
		}
	}
	return ""
}

func (s Spec) populatedFields() []string {
	candidates := [...]struct {
		name      string
		populated bool
	}{
		{name: fieldID, populated: s.ID != ""},
		{name: fieldType, populated: s.Type != ""},
		{name: fieldConfig, populated: len(s.Config) > 0},
		{name: fieldInput, populated: s.Input != (Ref{})},
		{name: fieldInputs, populated: len(s.Inputs) > 0},
		{name: fieldSteps, populated: len(s.Steps) > 0},
		{name: fieldResolver, populated: s.Resolver != ""},
		{name: fieldCases, populated: len(s.Cases) > 0},
		{name: fieldBody, populated: s.Body != nil},
		{name: fieldCondition, populated: s.Condition != ""},
		{name: fieldMaxIterations, populated: s.MaxIterations != 0},
		{name: fieldBodyOutput, populated: s.BodyOutput != (Ref{})},
		{name: fieldConcurrency, populated: s.Concurrency != 0},
	}
	fields := make([]string, 0, len(candidates))
	for _, field := range candidates {
		if field.populated {
			fields = append(fields, field.name)
		}
	}
	return fields
}

func (s Spec) fieldError(field string, err error) error {
	return &SpecError{Kind: s.Kind, ID: s.ID, Field: field, Err: err}
}
