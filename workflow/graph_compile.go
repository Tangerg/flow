package workflow

import "fmt"

// CompileGraph validates a flat Graph, builds its leaves, and returns a Step.
// It rejects duplicate IDs, missing dependencies, cycles, unknown node types,
// invalid node configs, nil factory results, and incompatible registered
// schemas, then runs each topological layer's nodes concurrently. Build errors
// are reported as GraphError values at the graph node and field that caused them.
func (r *Registry) CompileGraph(graph Graph) (Step, error) {
	plan, err := r.validateGraph(graph)
	if err != nil {
		return nil, err
	}

	var layerSteps []Step
	for _, layer := range plan.layers {
		steps := make([]Step, 0, len(layer))
		for _, nodeID := range layer {
			leaf, field, err := (leafCompiler{registry: r}).compile(
				plan.nodesByID[nodeID].leafSpec(),
			)
			if err != nil {
				return nil, &GraphError{
					NodeID: nodeID,
					Field:  field,
					Err:    fmt.Errorf("%w: %w", ErrInvalidGraph, err),
				}
			}
			steps = append(steps, leaf)
		}
		if len(steps) == 1 {
			layerSteps = append(layerSteps, steps[0])
		} else {
			layerSteps = append(layerSteps, Parallel(steps, ParallelConfig{}))
		}
	}
	return Sequence(layerSteps...), nil
}

// CompileGraphJSON validates data against [GraphJSONSchema], strictly
// unmarshals it into a Graph, and compiles it.
func (r *Registry) CompileGraphJSON(data []byte) (Step, error) {
	if err := ValidateGraphJSON(data); err != nil {
		return nil, err
	}
	var graph Graph
	if err := jsonDocument(data).decode(&graph); err != nil {
		return nil, &GraphError{
			Field: "json",
			Err:   fmt.Errorf("%w: %w", ErrInvalidGraph, err),
		}
	}
	return r.CompileGraph(graph)
}

func (n NodeSpec) leafSpec() Spec {
	return Spec{
		Kind:   KindLeaf,
		ID:     n.ID,
		Type:   n.Type,
		Input:  n.Input,
		Inputs: n.Inputs,
		Config: n.Config,
	}
}
