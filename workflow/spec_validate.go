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

// specKindFields is the field matrix of [Spec]: which of its fields each kind
// may populate. Kind itself is always required and never listed. Keeping it here
// as data rather than inside the dispatch switch shows the whole matrix in one
// place, and lets TestSpecFieldMatricesAgreeWithTheSpecStruct check it against
// the Spec struct so a newly added field cannot stay silently unguarded.
var specKindFields = map[Kind][]string{
	KindLeaf:      {fieldID, fieldType, fieldConfig, fieldInputs},
	KindSequence:  {fieldSteps},
	KindParallel:  {fieldSteps, fieldConcurrency},
	KindBranch:    {fieldID, fieldResolver, fieldCases},
	KindLoop:      {fieldID, fieldBody, fieldCondition, fieldMaxIterations},
	KindIteration: {fieldID, fieldInput, fieldBody, fieldBodyOutput, fieldConcurrency},
	KindSubgraph:  {fieldID, fieldInputs, fieldBody, fieldBodyOutput},
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

// checkDepth enforces the one nesting limit a Spec has, wherever it is reached.
// Validation and encoding walk the same tree and must stop at the same depth for
// the same stated reason, so both ask here rather than restating the bound next
// to their own copy of the reason -- see
// TestSpec_enforcesProgrammaticNestingLimit.
func (s Spec) checkDepth(depth int) error {
	if depth <= MaxNestingDepth {
		return nil
	}
	return s.fieldError("", fmt.Errorf(
		"%w: nesting depth %d exceeds limit %d",
		ErrMaxDepth,
		depth,
		MaxNestingDepth,
	))
}

func (s *specValidator) validate(spec Spec) error {
	if err := spec.checkDepth(s.depth); err != nil {
		return err
	}

	var validateVariant func() error
	switch spec.Kind {
	case KindLeaf:
		validateVariant = func() error { return s.validateLeaf(spec) }
	case KindSequence, KindParallel:
		validateVariant = func() error { return s.validateSteps(spec.Steps) }
	case KindBranch:
		validateVariant = func() error { return s.validateBranch(spec) }
	case KindLoop:
		validateVariant = func() error { return s.validateLoop(spec) }
	case KindIteration:
		validateVariant = func() error { return s.validateIteration(spec) }
	case KindSubgraph:
		validateVariant = func() error { return s.validateSubgraph(spec) }
	default:
		return spec.unknownKindError()
	}

	if field := spec.unexpectedField(specKindFields[spec.Kind]); field != "" {
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

// requireKindFields reports the first field a kind needs and does not have.
//
// The rule belongs to validation, but compilation dereferences Body and so has to
// defend the same requirement rather than trust its caller — see
// TestSpecCompiler_defendsItsValidatedInputContract. Stating it once here lets
// both ask instead of keeping two copies of every message that can drift apart.
func (s Spec) requireKindFields() error {
	switch s.Kind {
	case KindBranch:
		if len(s.Cases) == 0 {
			return s.fieldError(fieldCases, errors.New("at least one branch case is required"))
		}
	case KindLoop:
		if s.Body == nil {
			return s.fieldError(fieldBody, errors.New("loop body is required"))
		}
	case KindIteration:
		switch {
		case s.Input == (Ref{}):
			return s.fieldError(fieldInput, errors.New("iteration input is required"))
		case s.Body == nil:
			return s.fieldError(fieldBody, errors.New("iteration body is required"))
		case s.BodyOutput == (Ref{}):
			return s.fieldError(fieldBodyOutput, errors.New("iteration body output is required"))
		}
	case KindSubgraph:
		switch {
		case s.Body == nil:
			return s.fieldError(fieldBody, errors.New("subgraph body is required"))
		case s.BodyOutput == (Ref{}):
			return s.fieldError(fieldBodyOutput, errors.New("subgraph body output is required"))
		}
	default:
		// Leaf, sequence, and parallel need nothing beyond the fields their kind
		// is allowed to carry at all.
	}
	return nil
}

// requireResolver and requireCondition resolve a registered name, reporting the
// same failure for validation and compilation. Compilation needs the value while
// validation needs only presence, so they share the lookup rather than the
// message.
func (r registrySnapshot) requireResolver(spec Spec) (Resolver, error) {
	resolver, ok := r.lookupResolver(spec.Resolver)
	if !ok {
		return nil, spec.fieldError(
			fieldResolver,
			fmt.Errorf("unknown resolver %q", spec.Resolver),
		)
	}
	return resolver, nil
}

func (r registrySnapshot) requireCondition(spec Spec) (Condition, error) {
	condition, ok := r.lookupCondition(spec.Condition)
	if !ok {
		return nil, spec.fieldError(
			fieldCondition,
			fmt.Errorf("unknown condition %q", spec.Condition),
		)
	}
	return condition, nil
}

// unknownKindError is what both the validating and the compiling switch report
// for a kind neither of them handles.
func (s Spec) unknownKindError() error {
	return s.fieldError(fieldKind, fmt.Errorf("unknown kind %q", s.Kind))
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

// admit is what every named kind does first: claim the identity the document
// gives it, then check the fields that kind cannot do without. A leaf requires
// none, so the second check is a no-op there and the order stays uniform.
func (s *specValidator) admit(spec Spec) error {
	if err := s.claimID(spec); err != nil {
		return err
	}
	return spec.requireKindFields()
}

func (s *specValidator) validateLoop(spec Spec) error {
	if err := s.admit(spec); err != nil {
		return err
	}
	if err := validateName(nameCondition, spec.Condition); err != nil {
		return spec.fieldError(fieldCondition, err)
	}
	if _, err := s.registry.requireCondition(spec); err != nil {
		return err
	}
	// The loop body runs under a scope derived from the loop ID and iteration
	// index, so its IDs are local and may be reused outside this loop. Reserve
	// the loop ID itself because each iteration records the stop decision under
	// that ID in the same scope.
	bodyValidator := s.child(newDefinitionIDs(spec.ID))
	return locateSpecError(bodyValidator.validate(*spec.Body), fieldBody)
}

func (s *specValidator) validateLeaf(spec Spec) error {
	if err := s.admit(spec); err != nil {
		return err
	}
	if err := validateName(nameNodeType, spec.Type); err != nil {
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
	if err := s.admit(spec); err != nil {
		return err
	}
	if err := validateName(nameResolver, spec.Resolver); err != nil {
		return spec.fieldError(fieldResolver, err)
	}
	if _, err := s.registry.requireResolver(spec); err != nil {
		return err
	}

	// At most one case runs, so sibling cases may reuse an ID. Each case is
	// checked against the path before the branch, and their introduced IDs are
	// visible to the steps after it.
	introduced := newDefinitionIDs()
	for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
		if err := validateName(nameBranchCase, name); err != nil {
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

// validateOwnBody checks the reference the composite will project and then
// validates a body that runs in its own identity namespace, mirroring
// definitionValidator.validateOwnBody for the Spec form. An iteration element
// keeps the outer Store snapshot under an indexed Journal scope while a subgraph
// body is sealed, but neither inherits the surrounding step IDs.
func (s *specValidator) validateOwnBody(spec Spec) error {
	if err := spec.BodyOutput.Validate(); err != nil {
		return spec.fieldError(fieldBodyOutput, err)
	}
	bodyValidator := s.child(newDefinitionIDs())
	if err := bodyValidator.validate(*spec.Body); err != nil {
		return locateSpecError(err, fieldBody)
	}
	return nil
}

func (s *specValidator) validateIteration(spec Spec) error {
	if err := s.admit(spec); err != nil {
		return err
	}
	if err := spec.Input.Validate(); err != nil {
		return spec.fieldError(fieldInput, err)
	}
	if err := s.validateOwnBody(spec); err != nil {
		return err
	}
	outputs := s.guaranteedOutputs(*spec.Body)
	if outputs.known && !iterationOutputGuaranteed(spec.ID, outputs, spec.BodyOutput) {
		return spec.fieldError(fieldBodyOutput, iterationOutputError(spec.BodyOutput))
	}
	return nil
}

func (s *specValidator) validateSubgraph(spec Spec) error {
	if err := s.admit(spec); err != nil {
		return err
	}
	if err := spec.Inputs.validateSeeds(); err != nil {
		return spec.fieldError(fieldInputs, err)
	}
	if err := s.validateOwnBody(spec); err != nil {
		return err
	}
	outputs := s.guaranteedOutputs(*spec.Body)
	if outputs.known && !subgraphOutputGuaranteed(spec.Inputs, outputs, spec.BodyOutput) {
		return spec.fieldError(fieldBodyOutput, subgraphOutputError(spec.BodyOutput))
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
	return unionOutputs(specs, s.guaranteedOutputs)
}

func (s *specValidator) guaranteedCaseOutputs(cases map[string]Spec) outputGuarantee {
	return intersectOutputs(cases, s.guaranteedOutputs)
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
