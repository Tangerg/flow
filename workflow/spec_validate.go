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
	validator := specValidator{registry: r}
	return validator.validate(root, make(map[string]struct{}))
}

// specValidator owns the recursive state and invariants of nested Spec
// validation. Step IDs remain path-local because mutually exclusive branch
// cases and independently scoped repeated bodies may deliberately reuse an
// output ID.
type specValidator struct {
	registry *Registry
}

func (s specValidator) validate(spec Spec, stepIDs map[string]struct{}) error {
	if err := spec.validateFields(); err != nil {
		return err
	}
	if err := spec.validateConstraints(); err != nil {
		return err
	}

	switch spec.Kind {
	case KindLeaf:
		return s.validateLeaf(spec, stepIDs)
	case KindSequence, KindParallel:
		return s.validateSteps(spec.Steps, stepIDs)
	case KindBranch:
		return s.validateBranch(spec, stepIDs)
	case KindLoop:
		return s.validateLoop(spec, stepIDs)
	case KindIteration:
		return s.validateIteration(spec, stepIDs)
	default:
		return spec.fieldError(
			"kind",
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
			"concurrency",
			fmt.Errorf(
				"%w: concurrency must be non-negative, got %d",
				ErrInvalidSpec,
				s.Concurrency,
			),
		)
	}
	if s.MaxIterations < 0 {
		return s.fieldError(
			"maxIterations",
			fmt.Errorf(
				"%w: max iterations must be non-negative, got %d",
				ErrInvalidSpec,
				s.MaxIterations,
			),
		)
	}
	return nil
}

func (s specValidator) validateSteps(specs []Spec, stepIDs map[string]struct{}) error {
	for _, spec := range specs {
		if err := s.validate(spec, stepIDs); err != nil {
			return err
		}
	}
	return nil
}

func (s specValidator) validateLoop(spec Spec, stepIDs map[string]struct{}) error {
	if err := spec.claimID(stepIDs); err != nil {
		return err
	}
	if spec.Body == nil {
		return spec.fieldError(
			"body",
			fmt.Errorf("%w: loop body is required", ErrInvalidSpec),
		)
	}
	if _, ok := s.registry.lookupCondition(spec.Condition); !ok {
		return spec.fieldError(
			"condition",
			fmt.Errorf("%w: unknown condition %q", ErrInvalidSpec, spec.Condition),
		)
	}
	// The loop body runs under a scope derived from the loop ID and iteration
	// index, so its IDs are local and may be reused outside this loop. Reserve
	// the loop ID itself because each iteration records the stop decision under
	// that ID in the same scope.
	return s.validate(*spec.Body, map[string]struct{}{spec.ID: {}})
}

func (s specValidator) validateLeaf(spec Spec, stepIDs map[string]struct{}) error {
	if err := spec.claimID(stepIDs); err != nil {
		return err
	}
	if spec.Type == "" {
		return spec.fieldError(
			"type",
			fmt.Errorf("%w: node type is empty", ErrInvalidSpec),
		)
	}
	if _, ok := s.registry.lookupLeaf(spec.Type); !ok {
		return spec.fieldError("type", fmt.Errorf("%w %q", ErrUnknownNodeType, spec.Type))
	}
	registered, _ := s.registry.lookupNodeSchema(spec.Type)
	if err := registered.validateConfig(spec.Config); err != nil {
		return spec.fieldError("config", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	inputs, err := spec.Inputs.withDefault(spec.Input)
	if err != nil {
		return spec.fieldError("inputs", err)
	}
	if err := inputs.validate(); err != nil {
		return spec.fieldError("inputs", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	// A nested Spec has no cross-node index, so ports are checked for
	// completeness only; edge types are checked when a flat Graph names both
	// ends.
	schema := registered.schema
	if err := schema.validateInputs(inputs, func(Ref) (ValueType, bool) {
		return "", false
	}); err != nil {
		return spec.fieldError("inputs", err)
	}
	return nil
}

func (s specValidator) validateBranch(spec Spec, stepIDs map[string]struct{}) error {
	if err := spec.claimID(stepIDs); err != nil {
		return err
	}
	if len(spec.Cases) == 0 {
		return spec.fieldError(
			"cases",
			fmt.Errorf("%w: at least one branch case is required", ErrInvalidSpec),
		)
	}
	if _, ok := s.registry.lookupResolver(spec.Resolver); !ok {
		return spec.fieldError(
			"resolver",
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
				"cases",
				fmt.Errorf("%w: branch case name is empty", ErrInvalidSpec),
			)
		}
		caseIDs := maps.Clone(stepIDs)
		if err := s.validate(spec.Cases[name], caseIDs); err != nil {
			return err
		}
		for id := range caseIDs {
			if _, existed := stepIDs[id]; !existed {
				introduced[id] = struct{}{}
			}
		}
	}
	maps.Copy(stepIDs, introduced)
	return nil
}

func (s specValidator) validateIteration(spec Spec, stepIDs map[string]struct{}) error {
	if err := spec.claimID(stepIDs); err != nil {
		return err
	}
	if spec.Input == (Ref{}) {
		return spec.fieldError(
			"input",
			fmt.Errorf("%w: iteration input is required", ErrInvalidSpec),
		)
	}
	if spec.Body == nil {
		return spec.fieldError(
			"body",
			fmt.Errorf("%w: iteration body is required", ErrInvalidSpec),
		)
	}
	if spec.BodyOutput == (Ref{}) {
		return spec.fieldError(
			"bodyOutput",
			fmt.Errorf("%w: iteration body output is required", ErrInvalidSpec),
		)
	}
	if err := spec.Input.validate(); err != nil {
		return spec.fieldError("input", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	if err := spec.BodyOutput.validate(); err != nil {
		return spec.fieldError("bodyOutput", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	// An iteration body's Store and Journal scope are local to one element.
	return s.validate(*spec.Body, make(map[string]struct{}))
}

func (s Spec) claimID(stepIDs map[string]struct{}) error {
	if s.ID == "" {
		return s.fieldError("id", ErrInvalidStepID)
	}
	if _, exists := stepIDs[s.ID]; exists {
		return s.fieldError("id", ErrDuplicateStep)
	}
	stepIDs[s.ID] = struct{}{}
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
		return []string{"id", "type", "config", "input", "inputs"}
	case KindSequence:
		return []string{"steps"}
	case KindParallel:
		return []string{"steps", "concurrency"}
	case KindBranch:
		return []string{"id", "resolver", "cases"}
	case KindLoop:
		return []string{"id", "body", "condition", "maxIterations"}
	case KindIteration:
		return []string{"id", "input", "body", "bodyOutput", "concurrency"}
	default:
		return nil
	}
}

func (s Spec) populatedFields() []string {
	fields := make([]string, 0, 13)
	for _, field := range []struct {
		name      string
		populated bool
	}{
		{name: "id", populated: s.ID != ""},
		{name: "type", populated: s.Type != ""},
		{name: "config", populated: len(s.Config) > 0},
		{name: "input", populated: s.Input != (Ref{})},
		{name: "inputs", populated: len(s.Inputs) > 0},
		{name: "steps", populated: len(s.Steps) > 0},
		{name: "resolver", populated: s.Resolver != ""},
		{name: "cases", populated: len(s.Cases) > 0},
		{name: "body", populated: s.Body != nil},
		{name: "condition", populated: s.Condition != ""},
		{name: "maxIterations", populated: s.MaxIterations != 0},
		{name: "bodyOutput", populated: s.BodyOutput != (Ref{})},
		{name: "concurrency", populated: s.Concurrency != 0},
	} {
		if field.populated {
			fields = append(fields, field.name)
		}
	}
	return fields
}

func (s Spec) fieldError(field string, err error) error {
	return &SpecError{Kind: s.Kind, ID: s.ID, Field: field, Err: err}
}
