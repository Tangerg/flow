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

func (v specValidator) validate(spec Spec, stepIDs map[string]struct{}) error {
	if err := spec.validateFields(); err != nil {
		return err
	}
	if err := spec.validateConstraints(); err != nil {
		return err
	}

	switch spec.Kind {
	case KindLeaf:
		return v.validateLeaf(spec, stepIDs)
	case KindSequence, KindParallel:
		return v.validateSteps(spec.Steps, stepIDs)
	case KindBranch:
		return v.validateBranch(spec, stepIDs)
	case KindLoop:
		return v.validateLoop(spec, stepIDs)
	case KindIteration:
		return v.validateIteration(spec, stepIDs)
	default:
		return spec.fieldError(
			"kind",
			fmt.Errorf("%w: unknown kind %q", ErrInvalidSpec, spec.Kind),
		)
	}
}

func (spec Spec) validateFields() error {
	if field := spec.unexpectedField(); field != "" {
		return spec.fieldError(field, fmt.Errorf(
			"%w: field %q is not valid for a %q spec",
			ErrInvalidSpec,
			field,
			spec.Kind,
		))
	}
	return nil
}

func (spec Spec) validateConstraints() error {
	if spec.Concurrency < 0 {
		return spec.fieldError(
			"concurrency",
			fmt.Errorf(
				"%w: concurrency must be non-negative, got %d",
				ErrInvalidSpec,
				spec.Concurrency,
			),
		)
	}
	if spec.MaxIterations < 0 {
		return spec.fieldError(
			"maxIterations",
			fmt.Errorf(
				"%w: max iterations must be non-negative, got %d",
				ErrInvalidSpec,
				spec.MaxIterations,
			),
		)
	}
	return nil
}

func (v specValidator) validateSteps(specs []Spec, stepIDs map[string]struct{}) error {
	for _, spec := range specs {
		if err := v.validate(spec, stepIDs); err != nil {
			return err
		}
	}
	return nil
}

func (v specValidator) validateLoop(spec Spec, stepIDs map[string]struct{}) error {
	if err := spec.claimID(stepIDs); err != nil {
		return err
	}
	if spec.Body == nil {
		return spec.fieldError(
			"body",
			fmt.Errorf("%w: loop body is required", ErrInvalidSpec),
		)
	}
	if _, ok := v.registry.lookupCondition(spec.Condition); !ok {
		return spec.fieldError(
			"condition",
			fmt.Errorf("%w: unknown condition %q", ErrInvalidSpec, spec.Condition),
		)
	}
	// The loop body runs under a scope derived from the loop ID and iteration
	// index, so its IDs are local and may be reused outside this loop. Reserve
	// the loop ID itself because each iteration records the stop decision under
	// that ID in the same scope.
	return v.validate(*spec.Body, map[string]struct{}{spec.ID: {}})
}

func (v specValidator) validateLeaf(spec Spec, stepIDs map[string]struct{}) error {
	if err := spec.claimID(stepIDs); err != nil {
		return err
	}
	if spec.Type == "" {
		return spec.fieldError(
			"type",
			fmt.Errorf("%w: node type is empty", ErrInvalidSpec),
		)
	}
	if _, ok := v.registry.lookupLeaf(spec.Type); !ok {
		return spec.fieldError("type", fmt.Errorf("%w %q", ErrUnknownNodeType, spec.Type))
	}
	registered, _ := v.registry.lookupNodeSchema(spec.Type)
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

func (v specValidator) validateBranch(spec Spec, stepIDs map[string]struct{}) error {
	if err := spec.claimID(stepIDs); err != nil {
		return err
	}
	if len(spec.Cases) == 0 {
		return spec.fieldError(
			"cases",
			fmt.Errorf("%w: at least one branch case is required", ErrInvalidSpec),
		)
	}
	if _, ok := v.registry.lookupResolver(spec.Resolver); !ok {
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
		if err := v.validate(spec.Cases[name], caseIDs); err != nil {
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

func (v specValidator) validateIteration(spec Spec, stepIDs map[string]struct{}) error {
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
	return v.validate(*spec.Body, make(map[string]struct{}))
}

func (spec Spec) claimID(stepIDs map[string]struct{}) error {
	if spec.ID == "" {
		return spec.fieldError("id", ErrInvalidStepID)
	}
	if _, exists := stepIDs[spec.ID]; exists {
		return spec.fieldError("id", ErrDuplicateStep)
	}
	stepIDs[spec.ID] = struct{}{}
	return nil
}

// unexpectedField keeps the programmatic Spec API as strict as the JSON
// schema. A populated field that a kind ignores is almost always a typo, and
// silently accepting it makes code-built and JSON-built workflows disagree.
func (spec Spec) unexpectedField() string {
	allowed := spec.allowedFields()
	if allowed == nil {
		return ""
	}
	for _, field := range spec.populatedFields() {
		if !slices.Contains(allowed, field) {
			return field
		}
	}
	return ""
}

func (spec Spec) allowedFields() []string {
	switch spec.Kind {
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

func (spec Spec) populatedFields() []string {
	fields := make([]string, 0, 13)
	for _, field := range []struct {
		name      string
		populated bool
	}{
		{name: "id", populated: spec.ID != ""},
		{name: "type", populated: spec.Type != ""},
		{name: "config", populated: len(spec.Config) > 0},
		{name: "input", populated: spec.Input != (Ref{})},
		{name: "inputs", populated: len(spec.Inputs) > 0},
		{name: "steps", populated: len(spec.Steps) > 0},
		{name: "resolver", populated: spec.Resolver != ""},
		{name: "cases", populated: len(spec.Cases) > 0},
		{name: "body", populated: spec.Body != nil},
		{name: "condition", populated: spec.Condition != ""},
		{name: "maxIterations", populated: spec.MaxIterations != 0},
		{name: "bodyOutput", populated: spec.BodyOutput != (Ref{})},
		{name: "concurrency", populated: spec.Concurrency != 0},
	} {
		if field.populated {
			fields = append(fields, field.name)
		}
	}
	return fields
}

func (spec Spec) fieldError(field string, err error) error {
	return &SpecError{Kind: spec.Kind, ID: spec.ID, Field: field, Err: err}
}
