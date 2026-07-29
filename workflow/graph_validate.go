package workflow

import "fmt"

// ValidateGraph checks a Graph without compiling it: unique IDs, known node
// types, config schemas, cycles, fully wired input ports, and type-compatible
// edges. It is intended to power a visual editor's live feedback.
func (r *Registry) ValidateGraph(graph Graph) error {
	_, err := r.validateGraph(graph)
	return err
}

func (r *Registry) validateGraph(graph Graph) (graphPlan, error) {
	plan, err := graph.plan()
	if err != nil {
		return graphPlan{}, err
	}

	for _, node := range graph.Nodes {
		if _, ok := r.lookupLeaf(node.Type); !ok {
			return graphPlan{}, &GraphError{
				NodeID: node.ID,
				Field:  "type",
				Err:    fmt.Errorf("%w %q", ErrUnknownNodeType, node.Type),
			}
		}

		registered := r.lookupNodeSchema(node.Type)
		if err := registered.validateConfig(node.Config); err != nil {
			return graphPlan{}, &GraphError{
				NodeID: node.ID,
				Field:  "config",
				Err:    fmt.Errorf("%w: %w", ErrInvalidGraph, err),
			}
		}
		if err := registered.schema.validateInputs(
			plan.inputsByNode[node.ID],
			func(ref Ref) (ValueType, bool) {
				producer, internal := plan.nodesByID[ref.NodeID]
				if !internal {
					// External input from the seed Store has no registered type.
					return "", false
				}
				// NodeSchema describes only the conventional output as a whole.
				// A nested member or custom cell has no declared type.
				if ref.Path != outputPath {
					return TypeAny, true
				}
				return r.lookupNodeSchema(producer.Type).schema.Output, true
			},
		); err != nil {
			return graphPlan{}, &GraphError{NodeID: node.ID, Field: "inputs", Err: err}
		}
	}
	return plan, nil
}
