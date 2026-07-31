package workflow

import (
	"context"
	"fmt"
	"slices"
)

// CompileGraph validates a flat Graph, builds its nodes, and returns a Step.
// It rejects duplicate IDs, missing dependencies, cycles, unknown node types,
// invalid node configs, nil factory results, incompatible registered schemas,
// and invalid routing gates. A compiled graph starts a node as soon as all of
// its dependencies complete, subject to the graph-wide concurrency limit.
// Build errors are reported as GraphError values at the graph node and field
// that caused them.
func (r *Registry) CompileGraph(graph Graph) (Step, error) {
	plan, err := r.validateGraph(graph)
	if err != nil {
		return nil, err
	}

	steps := make(stepList, len(graph.Nodes))
	for index, node := range graph.Nodes {
		step, field, err := (leafCompiler{registry: r}).compile(node.nodeSpec())
		if err != nil {
			return nil, &GraphError{
				NodeID: node.ID,
				Field:  field,
				Err:    fmt.Errorf("%w: %w", ErrInvalidGraph, err),
			}
		}
		if len(node.When) > 0 {
			step = r.gate(node, plan, step)
		}
		steps[index] = step
	}
	return compiledGraph(plan, steps, graph.Concurrency), nil
}

type graphStep struct {
	steps                 stepList
	dependencyCounts      []int
	dependencyNodeIndexes [][]int
	dependentNodeIndexes  [][]int
	nodeIDs               map[string]struct{}
	limit                 int
}

func compiledGraph(plan graphPlan, steps stepList, limit int) Step {
	nodeIDs := make(map[string]struct{}, len(plan.nodesByID))
	for nodeID := range plan.nodesByID {
		nodeIDs[nodeID] = struct{}{}
	}
	return graphStep{
		steps:                 steps,
		dependencyCounts:      slices.Clone(plan.dependencyCounts),
		dependencyNodeIndexes: cloneIndexes(plan.dependencyNodeIndexes),
		dependentNodeIndexes:  cloneIndexes(plan.dependentNodeIndexes),
		nodeIDs:               nodeIDs,
		limit:                 limit,
	}
}

func cloneIndexes(indexes [][]int) [][]int {
	cloned := make([][]int, len(indexes))
	for index, values := range indexes {
		cloned[index] = slices.Clone(values)
	}
	return cloned
}

func (g graphStep) Run(ctx context.Context, store Store) (Store, error) {
	ctx = ensureRun(ctx)
	input := store.withoutNodes(g.nodeIDs)
	if err := runFrom(ctx).validateDefinition(g); err != nil {
		return input, err
	}
	if err := ctx.Err(); err != nil {
		return input, err
	}
	return (&graphExecution{graph: g, input: input}).run(ctx)
}

func (g graphStep) Describe() Description {
	return Description{Kind: "graph", Children: g.steps.describe()}
}

func (g graphStep) definition() stepDefinition {
	return stepDefinition{kind: definitionSteps, steps: g.steps}
}

func (r *Registry) gate(node GraphNode, plan graphPlan, step Step) Step {
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

func (n GraphNode) nodeSpec() Spec {
	return Spec{
		Kind:   KindLeaf,
		ID:     n.ID,
		Type:   n.Type,
		Inputs: n.Inputs,
		Config: n.Config,
	}
}
