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
			return fmt.Errorf("workflow: %s concurrency must not be negative", spec.Kind)
		}
		if spec.MaxIterations < 0 {
			return fmt.Errorf("workflow: maxIterations must not be negative")
		}

		addID := func(id string) error {
			if id == "" {
				return ErrInvalidStepID
			}
			if _, exists := ids[id]; exists {
				return fmt.Errorf("workflow: duplicate step ID %q", id)
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
				return fmt.Errorf("workflow: leaf %q has an empty type", spec.ID)
			}
			if _, ok := r.leafFactory(spec.Type); !ok {
				return fmt.Errorf("workflow: unknown leaf type %q", spec.Type)
			}
			if spec.Input != nil {
				if err := validateRef(*spec.Input, "leaf input"); err != nil {
					return err
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
				return fmt.Errorf("workflow: branch requires at least one case")
			}
			if _, ok := r.resolver(spec.Resolver); !ok {
				return fmt.Errorf("workflow: unknown resolver %q", spec.Resolver)
			}
			for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
				if name == "" {
					return fmt.Errorf("workflow: branch case name is empty")
				}
				if err := walk(spec.Cases[name]); err != nil {
					return err
				}
			}
		case KindLoop:
			if spec.Body == nil {
				return fmt.Errorf("workflow: loop requires a body")
			}
			if _, ok := r.condition(spec.Condition); !ok {
				return fmt.Errorf("workflow: unknown condition %q", spec.Condition)
			}
			return walk(*spec.Body)
		case KindIteration:
			if err := addID(spec.ID); err != nil {
				return err
			}
			if spec.Body == nil || spec.Input == nil || spec.BodyOutput == nil {
				return fmt.Errorf("workflow: iteration requires input, body, and bodyOutput")
			}
			if err := validateRef(*spec.Input, "iteration input"); err != nil {
				return err
			}
			if err := validateRef(*spec.BodyOutput, "iteration bodyOutput"); err != nil {
				return err
			}
			return walk(*spec.Body)
		default:
			return fmt.Errorf("workflow: unknown spec kind %q", spec.Kind)
		}
		return nil
	}
	return walk(root)
}

func validateRef(ref Ref, field string) error {
	if ref.NodeID == "" || ref.Path == "" {
		return fmt.Errorf("workflow: %s requires nodeID and path", field)
	}
	return nil
}
