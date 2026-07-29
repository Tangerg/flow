package workflow

import (
	"fmt"
	"maps"
	"slices"
)

func (r *Registry) validateSpec(root Spec) error {
	validator := specValidator{registry: r}
	return validator.validate(root, make(map[string]struct{}))
}

// specValidator owns the recursive state and invariants of nested Spec
// validation. ids remains path-local because mutually exclusive branch cases
// may deliberately reuse an output ID.
type specValidator struct {
	registry *Registry
}

func (v specValidator) validate(spec Spec, ids map[string]struct{}) error {
	if field := spec.unexpectedField(); field != "" {
		return spec.err(field, fmt.Errorf(
			"%w: field %q is not valid for kind %q",
			ErrInvalidSpec, field, spec.Kind,
		))
	}
	if spec.Concurrency < 0 {
		return spec.err("concurrency", fmt.Errorf("%w: must not be negative", ErrInvalidSpec))
	}
	if spec.MaxIterations < 0 {
		return spec.err("maxIterations", fmt.Errorf("%w: must not be negative", ErrInvalidSpec))
	}

	switch spec.Kind {
	case KindLeaf:
		return v.validateLeaf(spec, ids)
	case KindSequence, KindParallel:
		for _, child := range spec.Steps {
			if err := v.validate(child, ids); err != nil {
				return err
			}
		}
	case KindBranch:
		return v.validateBranch(spec, ids)
	case KindLoop:
		if err := spec.addID(ids); err != nil {
			return err
		}
		if spec.Body == nil {
			return spec.err("body", fmt.Errorf("%w: required", ErrInvalidSpec))
		}
		if _, ok := v.registry.condition(spec.Condition); !ok {
			return spec.err("condition", fmt.Errorf("%w: unknown condition %q", ErrInvalidSpec, spec.Condition))
		}
		return v.validate(*spec.Body, ids)
	case KindIteration:
		return v.validateIteration(spec, ids)
	default:
		return spec.err("kind", fmt.Errorf("%w: unknown kind %q", ErrInvalidSpec, spec.Kind))
	}
	return nil
}

func (v specValidator) validateLeaf(spec Spec, ids map[string]struct{}) error {
	if err := spec.addID(ids); err != nil {
		return err
	}
	if spec.Type == "" {
		return spec.err("type", fmt.Errorf("%w: empty", ErrInvalidSpec))
	}
	if _, ok := v.registry.leafFactory(spec.Type); !ok {
		return spec.err("type", fmt.Errorf("%w %q", ErrUnknownNodeType, spec.Type))
	}
	if err := v.registry.registeredNodeSchema(spec.Type).validateConfig(spec.Config); err != nil {
		return spec.err("config", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	inputs, err := spec.Inputs.withDefault(spec.Input)
	if err != nil {
		return spec.err("inputs", err)
	}
	if err := inputs.validate("leaf"); err != nil {
		return spec.err("inputs", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	// A nested Spec has no cross-node index, so ports are checked for
	// completeness only; edge types are checked when a flat Graph names both
	// ends.
	if err := v.registry.validatePorts(spec.Type, inputs, func(Ref) (ValueType, bool) {
		return "", false
	}); err != nil {
		return spec.err("inputs", err)
	}
	return nil
}

func (v specValidator) validateBranch(spec Spec, ids map[string]struct{}) error {
	if err := spec.addID(ids); err != nil {
		return err
	}
	if len(spec.Cases) == 0 {
		return spec.err("cases", fmt.Errorf("%w: requires at least one case", ErrInvalidSpec))
	}
	if _, ok := v.registry.resolver(spec.Resolver); !ok {
		return spec.err("resolver", fmt.Errorf("%w: unknown resolver %q", ErrInvalidSpec, spec.Resolver))
	}

	// At most one case runs, so sibling cases may reuse an ID. Each case is
	// checked against the path before the branch, and their introduced IDs are
	// visible to the steps after it.
	introduced := make(map[string]struct{})
	for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
		if name == "" {
			return spec.err("cases", fmt.Errorf("%w: empty case name", ErrInvalidSpec))
		}
		caseIDs := maps.Clone(ids)
		if err := v.validate(spec.Cases[name], caseIDs); err != nil {
			return err
		}
		maps.Copy(introduced, caseIDs)
	}
	maps.Copy(ids, introduced)
	return nil
}

func (v specValidator) validateIteration(spec Spec, ids map[string]struct{}) error {
	if err := spec.addID(ids); err != nil {
		return err
	}
	if spec.Body == nil || spec.Input == nil || spec.BodyOutput == nil {
		return spec.err("iteration", fmt.Errorf("%w: input, body, and bodyOutput are required", ErrInvalidSpec))
	}
	if err := spec.Input.validate("iteration input"); err != nil {
		return spec.err("input", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	if err := spec.BodyOutput.validate("iteration bodyOutput"); err != nil {
		return spec.err("bodyOutput", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
	}
	// An iteration body's Store and Journal scope are local to one element.
	return v.validate(*spec.Body, make(map[string]struct{}))
}

func (spec Spec) addID(ids map[string]struct{}) error {
	if spec.ID == "" {
		return spec.err("id", ErrInvalidStepID)
	}
	if _, exists := ids[spec.ID]; exists {
		return spec.err("id", ErrDuplicateStep)
	}
	ids[spec.ID] = struct{}{}
	return nil
}

// unexpectedField keeps the programmatic Spec API as strict as the JSON
// schema. A populated field that a kind ignores is almost always a typo, and
// silently accepting it makes code-built and JSON-built workflows disagree.
func (spec Spec) unexpectedField() string {
	var allowed []string
	switch spec.Kind {
	case KindLeaf:
		allowed = []string{"id", "type", "config", "input", "inputs"}
	case KindSequence:
		allowed = []string{"steps"}
	case KindParallel:
		allowed = []string{"steps", "concurrency"}
	case KindBranch:
		allowed = []string{"id", "resolver", "cases"}
	case KindLoop:
		allowed = []string{"id", "body", "condition", "maxIterations"}
	case KindIteration:
		allowed = []string{"id", "input", "body", "bodyOutput", "concurrency"}
	default:
		return ""
	}

	populated := []struct {
		name string
		set  bool
	}{
		{"id", spec.ID != ""},
		{"type", spec.Type != ""},
		{"config", len(spec.Config) > 0},
		{"input", spec.Input != nil},
		{"inputs", len(spec.Inputs) > 0},
		{"steps", len(spec.Steps) > 0},
		{"resolver", spec.Resolver != ""},
		{"cases", len(spec.Cases) > 0},
		{"body", spec.Body != nil},
		{"condition", spec.Condition != ""},
		{"maxIterations", spec.MaxIterations != 0},
		{"bodyOutput", spec.BodyOutput != nil},
		{"concurrency", spec.Concurrency != 0},
	}
	for _, field := range populated {
		if field.set && !slices.Contains(allowed, field.name) {
			return field.name
		}
	}
	return ""
}

func (spec Spec) err(field string, err error) error {
	return &SpecError{Kind: spec.Kind, ID: spec.ID, Field: field, Err: err}
}
