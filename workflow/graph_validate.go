package workflow

import (
	"fmt"
	"slices"
)

// ValidateGraph checks a Graph without compiling it: unique IDs, known node
// types, config schemas, cycles, routing outlets, fully wired input ports, and
// type-compatible edges. It is intended to power a visual editor's live
// feedback.
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
		if _, ok := r.lookupNode(node.Type); !ok {
			return graphPlan{}, &GraphError{
				NodeID: node.ID,
				Field:  fieldType,
				Err:    fmt.Errorf("%w %q", ErrUnknownNodeType, node.Type),
			}
		}

		registered, _ := r.lookupNodeSchema(node.Type)
		if err := registered.validateConfig(node.Config); err != nil {
			return graphPlan{}, &GraphError{
				NodeID: node.ID,
				Field:  fieldConfig,
				Err:    fmt.Errorf("%w: %w", ErrInvalidGraph, err),
			}
		}
		if err := registered.schema.validateInputs(
			node.Inputs,
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
				producerSchema, _ := r.lookupNodeSchema(producer.Type)
				return producerSchema.schema.Output, true
			},
		); err != nil {
			return graphPlan{}, &GraphError{NodeID: node.ID, Field: fieldInputs, Err: err}
		}
		if err := r.validateGates(node, plan); err != nil {
			return graphPlan{}, err
		}
	}
	return plan, nil
}

func (r *Registry) validateGates(node GraphNode, plan graphPlan) error {
	for _, gate := range node.When {
		source := plan.nodesByID[gate.NodeID]
		registered, ok := r.lookupNodeSchema(source.Type)
		if !ok || len(registered.schema.Outlets) == 0 {
			return &GraphError{
				NodeID: node.ID,
				Field:  fieldWhen,
				Err: fmt.Errorf(
					"%w: routing node %q type %q declares no outlets",
					ErrInvalidGraph,
					gate.NodeID,
					source.Type,
				),
			}
		}
		if !slices.Contains(registered.schema.Outlets, gate.Outlet) {
			return &GraphError{
				NodeID: node.ID,
				Field:  fieldWhen,
				Err: fmt.Errorf(
					"%w %q on routing node %q",
					ErrUnknownOutlet,
					gate.Outlet,
					gate.NodeID,
				),
			}
		}
	}
	return nil
}
