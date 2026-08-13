package workflow

import (
	"context"
	"fmt"
	"slices"
)

// CompileGraph validates a flat Graph, builds its nodes, and returns a Step.
// It rejects duplicate IDs, missing dependencies, cycles, unknown node types,
// invalid node configs, nil or unsealed factory results, mismatched factory
// identities, impossible data edges, incompatible registered schemas, and
// invalid routing gates. A compiled graph starts a node as soon as all of its
// dependencies complete, subject to the graph-wide concurrency limit.
// Node-local build and definition errors are reported as GraphError values at
// the graph node and field that caused them. Errors spanning the constructed
// definition as a whole have an empty path, node ID, and field.
func (r *Registry) CompileGraph(graph Graph) (Step, error) {
	return r.snapshot().compileGraph(graph)
}

func (r registrySnapshot) compileGraph(graph Graph) (Step, error) {
	plan, err := r.validateGraph(graph)
	if err != nil {
		return nil, err
	}

	steps := make(stepList, len(graph.Nodes))
	outputs := make(map[string]bool, len(graph.Nodes))
	for index, node := range graph.Nodes {
		step, field, err := (leafCompiler{registry: r}).compile(node.nodeSpec())
		if err != nil {
			return nil, locateNode(index, node).fieldError(field, err)
		}
		if len(node.When) > 0 {
			step = r.gate(node, plan, step)
		}
		steps[index] = step
		outputs[node.ID] = step.definition().output
	}
	if err := plan.validateBuiltOutputs(graph, outputs); err != nil {
		return nil, err
	}
	compiled := plan.compile(steps, graph.Concurrency)
	if err := validateNode(compiled); err != nil {
		return nil, &GraphError{Err: err}
	}
	return compiled, nil
}

// validateBuiltOutputs closes the gap between metadata-only validation and the
// concrete boundaries returned by NodeFactory. ValidateGraph deliberately does
// not execute user factories, but CompileGraph has now built every node and can
// reject an internal edge whose producer cannot create its owned output cell —
// even when that node type has no registered schema.
func (g graphPlan) validateBuiltOutputs(graph Graph, outputs map[string]bool) error {
	for index, node := range graph.Nodes {
		for _, port := range node.Inputs.PortNames() {
			ref := node.Inputs[port]
			if _, internal := g.nodesByID[ref.NodeID]; !internal || outputs[ref.NodeID] {
				continue
			}
			return locateNode(index, node).fieldError(fieldInputs, fmt.Errorf(
				"%w: input port %q reads %s from a node whose factory produces no output",
				ErrIncompatibleType,
				port,
				ref,
			))
		}
	}
	return nil
}

type graphStep struct {
	steps                 stepList
	dependencyNodeIndexes [][]int
	dependentNodeIndexes  [][]int
	nodeIDs               nodeSet
	limit                 int
}

// A graphStep exists only after Graph and every built node have passed their
// own validation. Its recursive definition still exposes those nodes to the
// whole-graph identity and depth checks.
func (graphStep) validate() error { return nil }

func (g graphStep) Validate() error { return validateDefinition(g) }

// compile turns the plan into the Step that runs it. Everything the step needs to
// schedule -- who waits for whom, whose completion releases whom, and which node
// namespace the graph owns -- is the plan's own knowledge, so the plan builds it
// rather than handing its fields to a function that reads nothing else.
func (g graphPlan) compile(steps stepList, limit int) Step {
	return graphStep{
		steps:                 steps,
		dependencyNodeIndexes: cloneIndexes(g.dependencyNodeIndexes),
		dependentNodeIndexes:  cloneIndexes(g.dependentNodeIndexes),
		nodeIDs:               g.nodeIDs(),
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
	if err := context.Cause(ctx); err != nil {
		return store, err
	}
	input := store.withoutNodes(g.nodeIDs)
	return (&graphExecution{graph: g, input: input}).run(ctx)
}

func (g graphStep) Describe() Description {
	return Description{Kind: KindGraph, Children: g.steps.describe()}
}

func (g graphStep) definition() stepDefinition {
	return stepDefinition{kind: definitionGraph, steps: g.steps}
}

func (r registrySnapshot) gate(node GraphNode, plan graphPlan, step definedStep) definedStep {
	gates := make([]compiledGate, len(node.When))
	for index, gate := range node.When {
		source := plan.nodesByID[gate.NodeID]
		schema, _ := r.lookupNodeSchema(source.Type)
		gates[index] = compiledGate{
			Gate:    gate,
			outlets: slices.Clone(schema.schema.Outlets),
		}
	}
	return gated(gates, node.Trigger, step)
}

// CompileGraphJSON validates data against [GraphJSONSchema], strictly
// unmarshals it into a Graph, and compiles it.
func (r *Registry) CompileGraphJSON(data []byte) (Step, error) {
	graph, err := decodeGraphDocument(data)
	if err != nil {
		return nil, graphJSONError(err)
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
