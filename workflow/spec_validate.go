package workflow

import (
	"fmt"
	"maps"
	"slices"
)

// ValidateSpec checks a nested Spec without building it. It verifies its
// structure, registrations, references, unique IDs, registered node config
// schemas, and that each kind carries only fields meaningful to that kind.
func (r *Registry) ValidateSpec(spec Spec) error {
	return r.validateSpec(spec)
}

func (r *Registry) validateSpec(root Spec) error {
	validator := specValidator{
		registry: r,
		stepIDs:  make(map[string]struct{}),
	}
	return validator.validate(root)
}

// specValidator owns the recursive state and invariants of nested Spec
// validation. Step IDs remain path-local because mutually exclusive branch
// cases and independently scoped repeated bodies may deliberately reuse an
// output ID.
type specValidator struct {
	registry *Registry
	stepIDs  map[string]struct{}
}

func (s *specValidator) validate(spec Spec) error {
	if err := spec.validateFields(); err != nil {
		return err
	}
	if err := spec.validateConstraints(); err != nil {
		return err
	}

	switch spec.Kind {
	case KindLeaf:
		return s.validateLeaf(spec)
	case KindSequence, KindParallel:
		return s.validateSteps(spec.Steps)
	case KindBranch:
		return s.validateBranch(spec)
	case KindLoop:
		return s.validateLoop(spec)
	case KindIteration:
		return s.validateIteration(spec)
	case KindSubgraph:
		return s.validateSubgraph(spec)
	default:
		return spec.fieldError(
			fieldKind,
			fmt.Errorf("%w: unknown kind %q", ErrInvalidSpec, spec.Kind),
		)
	}
}

func (s Spec) validateFields() error {
	if field := s.unexpectedField(); field != "" {
		return s.fieldError(field, fmt.Errorf(
			"%w: field %q is not valid for a %q spec",
			ErrInvalidSpec,
			field,
			s.Kind,
		))
	}
	return nil
}

func (s Spec) validateConstraints() error {
	if s.Concurrency < 0 {
		return s.fieldError(
			fieldConcurrency,
			fmt.Errorf(
				"%w: concurrency must be non-negative, got %d",
				ErrInvalidSpec,
				s.Concurrency,
			),
		)
	}
	if s.MaxIterations < 0 {
		return s.fieldError(
			fieldMaxIterations,
			fmt.Errorf(
				"%w: max iterations must be non-negative, got %d",
				ErrInvalidSpec,
				s.MaxIterations,
			),
		)
	}
	return nil
}

func (s *specValidator) validateSteps(specs []Spec) error {
	for _, spec := range specs {
		if err := s.validate(spec); err != nil {
			return err
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
			fmt.Errorf("%w: loop body is required", ErrInvalidSpec),
		)
	}
	if _, ok := s.registry.lookupCondition(spec.Condition); !ok {
		return spec.fieldError(
			fieldCondition,
			fmt.Errorf("%w: unknown condition %q", ErrInvalidSpec, spec.Condition),
		)
	}
	// The loop body runs under a scope derived from the loop ID and iteration
	// index, so its IDs are local and may be reused outside this loop. Reserve
	// the loop ID itself because each iteration records the stop decision under
	// that ID in the same scope.
	bodyValidator := specValidator{
		registry: s.registry,
		stepIDs:  map[string]struct{}{spec.ID: {}},
	}
	return bodyValidator.validate(*spec.Body)
}

func (s *specValidator) validateLeaf(spec Spec) error {
	if err := s.claimID(spec); err != nil {
		return err
	}
	if spec.Type == "" {
		return spec.fieldError(
			fieldType,
			fmt.Errorf("%w: node type is empty", ErrInvalidSpec),
		)
	}
	if _, ok := s.registry.lookupNode(spec.Type); !ok {
		return spec.fieldError(fieldType, fmt.Errorf("%w %q", ErrUnknownNodeType, spec.Type))
	}
	registered, _ := s.registry.lookupNodeSchema(spec.Type)
	if err := registered.validateConfig(spec.Config); err != nil {
		return spec.fieldError(fieldConfig, fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	if err := spec.Inputs.validate(); err != nil {
		return spec.fieldError(fieldInputs, fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	// A nested Spec has no cross-node index, so ports are checked for
	// completeness only; edge types are checked when a flat Graph names both
	// ends.
	schema := registered.schema
	if err := schema.validateInputs(spec.Inputs, func(Ref) (ValueType, bool) {
		return "", false
	}); err != nil {
		return spec.fieldError(fieldInputs, err)
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
			fmt.Errorf("%w: at least one branch case is required", ErrInvalidSpec),
		)
	}
	if _, ok := s.registry.lookupResolver(spec.Resolver); !ok {
		return spec.fieldError(
			fieldResolver,
			fmt.Errorf("%w: unknown resolver %q", ErrInvalidSpec, spec.Resolver),
		)
	}

	// At most one case runs, so sibling cases may reuse an ID. Each case is
	// checked against the path before the branch, and their introduced IDs are
	// visible to the steps after it.
	introduced := make(map[string]struct{})
	for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
		if name == "" {
			return spec.fieldError(
				fieldCases,
				fmt.Errorf("%w: branch case name is empty", ErrInvalidSpec),
			)
		}
		caseValidator := specValidator{
			registry: s.registry,
			stepIDs:  maps.Clone(s.stepIDs),
		}
		if err := caseValidator.validate(spec.Cases[name]); err != nil {
			return err
		}
		for id := range caseValidator.stepIDs {
			if _, existed := s.stepIDs[id]; !existed {
				introduced[id] = struct{}{}
			}
		}
	}
	maps.Copy(s.stepIDs, introduced)
	return nil
}

func (s *specValidator) validateIteration(spec Spec) error {
	if err := s.claimID(spec); err != nil {
		return err
	}
	if spec.Input == (Ref{}) {
		return spec.fieldError(
			fieldInput,
			fmt.Errorf("%w: iteration input is required", ErrInvalidSpec),
		)
	}
	if spec.Body == nil {
		return spec.fieldError(
			fieldBody,
			fmt.Errorf("%w: iteration body is required", ErrInvalidSpec),
		)
	}
	if spec.BodyOutput == (Ref{}) {
		return spec.fieldError(
			fieldBodyOutput,
			fmt.Errorf("%w: iteration body output is required", ErrInvalidSpec),
		)
	}
	if err := spec.Input.validate(); err != nil {
		return spec.fieldError(fieldInput, fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	if err := spec.BodyOutput.validate(); err != nil {
		return spec.fieldError(fieldBodyOutput, fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	// An iteration body's Store and Journal scope are local to one element.
	bodyValidator := specValidator{
		registry: s.registry,
		stepIDs:  make(map[string]struct{}),
	}
	return bodyValidator.validate(*spec.Body)
}

func (s *specValidator) validateSubgraph(spec Spec) error {
	if err := s.claimID(spec); err != nil {
		return err
	}
	if spec.Body == nil {
		return spec.fieldError(
			fieldBody,
			fmt.Errorf("%w: subgraph body is required", ErrInvalidSpec),
		)
	}
	if spec.BodyOutput == (Ref{}) {
		return spec.fieldError(
			fieldBodyOutput,
			fmt.Errorf("%w: subgraph body output is required", ErrInvalidSpec),
		)
	}
	if err := spec.Inputs.validate(); err != nil {
		return spec.fieldError(fieldInputs, fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	if err := spec.BodyOutput.validate(); err != nil {
		return spec.fieldError(fieldBodyOutput, fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	bodyValidator := specValidator{
		registry: s.registry,
		stepIDs:  make(map[string]struct{}),
	}
	return bodyValidator.validate(*spec.Body)
}

func (s *specValidator) claimID(spec Spec) error {
	if spec.ID == "" {
		return spec.fieldError(fieldID, ErrInvalidStepID)
	}
	if _, exists := s.stepIDs[spec.ID]; exists {
		return spec.fieldError(fieldID, ErrDuplicateStep)
	}
	s.stepIDs[spec.ID] = struct{}{}
	return nil
}

// unexpectedField keeps the programmatic Spec API as strict as the JSON
// schema. A populated field that a kind ignores is almost always a typo, and
// silently accepting it makes code-built and JSON-built workflows disagree.
func (s Spec) unexpectedField() string {
	allowed := s.allowedFields()
	if allowed == nil {
		return ""
	}
	for _, field := range s.populatedFields() {
		if !slices.Contains(allowed, field) {
			return field
		}
	}
	return ""
}

func (s Spec) allowedFields() []string {
	switch s.Kind {
	case KindLeaf:
		return []string{fieldID, fieldType, fieldConfig, fieldInputs}
	case KindSequence:
		return []string{fieldSteps}
	case KindParallel:
		return []string{fieldSteps, fieldConcurrency}
	case KindBranch:
		return []string{fieldID, fieldResolver, fieldCases}
	case KindLoop:
		return []string{fieldID, fieldBody, fieldCondition, fieldMaxIterations}
	case KindIteration:
		return []string{fieldID, fieldInput, fieldBody, fieldBodyOutput, fieldConcurrency}
	case KindSubgraph:
		return []string{fieldID, fieldInputs, fieldBody, fieldBodyOutput}
	default:
		return nil
	}
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
