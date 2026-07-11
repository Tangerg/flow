package workflow

import (
	"fmt"
	"maps"
	"slices"
)

// validateSpec checks the complete nested definition before any factories run.
// In particular, IDs are unique across the tree so parallel branches cannot
// silently overwrite one another in the Store.
func (r *Registry) validateSpec(root Spec) error {
	ids := make(map[string]struct{})
	var walk func(Spec) error
	walk = func(spec Spec) error {
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
			if spec.Input != nil {
				if err := validateRef(*spec.Input, "leaf input"); err != nil {
					return specError(spec, "input", fmt.Errorf("%w: %w", ErrInvalidSpec, err))
				}
			}
		case KindSequence, KindParallel:
			for _, child := range spec.Steps {
				if err := walk(child); err != nil {
					return err
				}
			}
		case KindBranch:
			if len(spec.Cases) == 0 {
				return specError(spec, "cases", fmt.Errorf("%w: requires at least one case", ErrInvalidSpec))
			}
			if _, ok := r.resolver(spec.Resolver); !ok {
				return specError(spec, "resolver", fmt.Errorf("%w: unknown resolver %q", ErrInvalidSpec, spec.Resolver))
			}
			for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
				if name == "" {
					return specError(spec, "cases", fmt.Errorf("%w: empty case name", ErrInvalidSpec))
				}
				if err := walk(spec.Cases[name]); err != nil {
					return err
				}
			}
		case KindLoop:
			if spec.Body == nil {
				return specError(spec, "body", fmt.Errorf("%w: required", ErrInvalidSpec))
			}
			if _, ok := r.condition(spec.Condition); !ok {
				return specError(spec, "condition", fmt.Errorf("%w: unknown condition %q", ErrInvalidSpec, spec.Condition))
			}
			return walk(*spec.Body)
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
			return walk(*spec.Body)
		default:
			return specError(spec, "kind", fmt.Errorf("%w: unknown kind %q", ErrInvalidSpec, spec.Kind))
		}
		return nil
	}
	return walk(root)
}

func specError(spec Spec, field string, err error) error {
	return &SpecError{Kind: spec.Kind, ID: spec.ID, Field: field, Err: err}
}

func validateRef(ref Ref, field string) error {
	if ref.NodeID == "" || ref.Path == "" {
		return fmt.Errorf("workflow: %s requires nodeID and path", field)
	}
	return nil
}
