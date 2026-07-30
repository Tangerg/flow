package workflow

import (
	"context"
	"fmt"
	"slices"
)

// CompileGraph validates a flat Graph, builds its leaves, and returns a Step.
// It rejects duplicate IDs, missing dependencies, cycles, unknown node types,
// invalid node configs, nil factory results, incompatible registered schemas,
// and invalid routing gates, then runs each topological layer's nodes
// concurrently. Build errors are reported as GraphError values at the graph
// node and field that caused them.
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
			if len(plan.nodesByID[nodeID].When) > 0 {
				leaf = r.gate(plan.nodesByID[nodeID], plan, leaf)
			}
			steps = append(steps, leaf)
		}
		if len(steps) == 1 {
			layerSteps = append(layerSteps, steps[0])
		} else {
			layerSteps = append(layerSteps, Parallel(
				steps,
				ParallelConfig{Concurrency: graph.Concurrency},
			))
		}
	}
	return compiledGraph(plan, Sequence(layerSteps...)), nil
}

type graphStep struct {
	decoratedStep
	nodeIDs map[string]struct{}
}

func compiledGraph(plan graphPlan, step Step) Step {
	nodeIDs := make(map[string]struct{}, len(plan.nodesByID))
	for nodeID := range plan.nodesByID {
		nodeIDs[nodeID] = struct{}{}
	}
	return graphStep{
		decoratedStep: decoratedStep{step: step},
		nodeIDs:       nodeIDs,
	}
}

func (g graphStep) Run(ctx context.Context, store Store) (Store, error) {
	return g.step.Run(ctx, store.withoutNodes(g.nodeIDs))
}

func (r *Registry) gate(node NodeSpec, plan graphPlan, step Step) Step {
	gates := make([]compiledGate, len(node.When))
	for index, gate := range node.When {
		source := plan.nodesByID[gate.NodeID]
		schema, _ := r.lookupNodeSchema(source.Type)
		gates[index] = compiledGate{
			Gate:    gate,
			outlets: slices.Clone(schema.schema.Outlets),
		}
	}
	return gated(node.ID, gates, node.Trigger, step)
}

// CompileGraphJSON validates data against [GraphJSONSchema], strictly
// unmarshals it into a Graph, and compiles it.
func (r *Registry) CompileGraphJSON(data []byte) (Step, error) {
	var graph Graph
	if err := schemaLoader(loadGraphSchema).decode(jsonDocument(data), &graph); err != nil {
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
