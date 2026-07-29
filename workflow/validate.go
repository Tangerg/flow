package workflow

import (
	"fmt"
	"maps"
	"slices"
)

// validateSpec checks the complete nested definition before any factories run.
// In particular, step IDs must be unique among steps that can run in the same
// execution, so concurrent branches cannot silently overwrite one another in the
// Store. Mutually exclusive [Branch] cases are the exception; see below.
//
// ids accumulates the IDs introduced so far along the current execution path.
func (r *Registry) validateSpec(root Spec) error {
	var walk func(Spec, map[string]struct{}) error
	walk = func(spec Spec, ids map[string]struct{}) error {
		if field := unexpectedSpecField(spec); field != "" {
			return specError(spec, field, fmt.Errorf(
				"%w: field %q is not valid for kind %q",
				ErrInvalidSpec, field, spec.Kind,
			))
		}
		if spec.Concurrency < 0 {
			return specError(spec, "concurrency", fmt.Errorf("%w: must not be negative", ErrInvalidSpec))
		}
		if spec.MaxIterations < 0 {
			return specError(spec, "maxIterations", fmt.Errorf("%w: must not be negative", ErrInvalidSpec))
		}

		addID := func(id string) error {
			if id == "" {
				return specError(spec, "id", ErrInvalidStepID)
			}
			if _, exists := ids[id]; exists {
				return specError(spec, "id", ErrDuplicateStep)
			}
			ids[id] = struct{}{}
			return nil
		}

		switch spec.Kind {
		case KindLeaf:
			if err := addID(spec.ID); err != nil {
				return err
			}
			if spec.Type == "" {
				return specError(spec, "type", fmt.Errorf("%w: empty", ErrInvalidSpec))
			}
			if _, ok := r.leafFactory(spec.Type); !ok {
				return specError(spec, "type", fmt.Errorf("%w %q", ErrUnknownNodeType, spec.Type))
			}
			if err := validateConfig(r.registeredNodeSchema(spec.Type).configValidator, spec.Config); err != nil {
				return specError(spec, "config", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
			}
			inputs, err := resolveInputs(spec.Input, spec.Inputs)
			if err != nil {
				return specError(spec, "inputs", err)
			}
			if err := validatePortRefs(inputs, "leaf"); err != nil {
				return specError(spec, "inputs", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
			}
			// A nested Spec has no cross-node index, so ports are checked for
			// completeness only; edge types are checked when a flat Graph names
			// both ends.
			if err := r.validatePorts(spec.Type, inputs, func(Ref) (ValueType, bool) {
				return "", false
			}); err != nil {
				return specError(spec, "inputs", err)
			}
		case KindSequence, KindParallel:
			for _, child := range spec.Steps {
				if err := walk(child, ids); err != nil {
					return err
				}
			}
		case KindBranch:
			if err := addID(spec.ID); err != nil {
				return err
			}
			if len(spec.Cases) == 0 {
				return specError(spec, "cases", fmt.Errorf("%w: requires at least one case", ErrInvalidSpec))
			}
			if _, ok := r.resolver(spec.Resolver); !ok {
				return specError(spec, "resolver", fmt.Errorf("%w: unknown resolver %q", ErrInvalidSpec, spec.Resolver))
			}
			// At most one case runs, so sibling cases may reuse an ID. That is
			// how a branch converges: every case writes the same output
			// reference, and a downstream step reads it without knowing which
			// case ran. Each case is still checked against everything outside
			// the branch, and the union of what the cases introduce is visible
			// afterwards.
			introduced := make(map[string]struct{})
			for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
				if name == "" {
					return specError(spec, "cases", fmt.Errorf("%w: empty case name", ErrInvalidSpec))
				}
				caseIDs := maps.Clone(ids)
				if err := walk(spec.Cases[name], caseIDs); err != nil {
					return err
				}
				maps.Copy(introduced, caseIDs)
			}
			maps.Copy(ids, introduced)
		case KindLoop:
			if err := addID(spec.ID); err != nil {
				return err
			}
			if spec.Body == nil {
				return specError(spec, "body", fmt.Errorf("%w: required", ErrInvalidSpec))
			}
			if _, ok := r.condition(spec.Condition); !ok {
				return specError(spec, "condition", fmt.Errorf("%w: unknown condition %q", ErrInvalidSpec, spec.Condition))
			}
			return walk(*spec.Body, ids)
		case KindIteration:
			if err := addID(spec.ID); err != nil {
				return err
			}
			if spec.Body == nil || spec.Input == nil || spec.BodyOutput == nil {
				return specError(spec, "iteration", fmt.Errorf("%w: input, body, and bodyOutput are required", ErrInvalidSpec))
			}
			if err := validateRef(*spec.Input, "iteration input"); err != nil {
				return specError(spec, "input", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
			}
			if err := validateRef(*spec.BodyOutput, "iteration bodyOutput"); err != nil {
				return specError(spec, "bodyOutput", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
			}
			// An iteration body's Store is local to one element and its Journal
			// records live under the iteration's scope. Body IDs therefore cannot
			// collide with IDs outside the body; only collisions within one
			// element execution matter.
			return walk(*spec.Body, make(map[string]struct{}))
		default:
			return specError(spec, "kind", fmt.Errorf("%w: unknown kind %q", ErrInvalidSpec, spec.Kind))
		}
		return nil
	}
	return walk(root, make(map[string]struct{}))
}

// unexpectedSpecField keeps the programmatic Spec API as strict as the JSON
// schema. A populated field that a kind ignores is almost always a typo, and
// silently accepting it makes code-built and JSON-built workflows disagree.
func unexpectedSpecField(spec Spec) string {
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

func specError(spec Spec, field string, err error) error {
	return &SpecError{Kind: spec.Kind, ID: spec.ID, Field: field, Err: err}
}

func validateRef(ref Ref, field string) error {
	if ref.NodeID == "" {
		return fmt.Errorf("workflow: %s requires nodeID", field)
	}
	pointer, ok := scanPointer(ref.Path)
	if !ok {
		return fmt.Errorf("workflow: %s path must be a non-empty JSON Pointer", field)
	}
	for {
		_, present, valid := pointer.next()
		if !valid {
			return fmt.Errorf("workflow: %s path must be a non-empty JSON Pointer", field)
		}
		if !present {
			break
		}
	}
	return nil
}
