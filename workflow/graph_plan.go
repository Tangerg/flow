package workflow

import (
	"fmt"
	"slices"
)

// plan validates the graph structurally. Registry-level semantic checks build
// on the resulting plan.
func (g Graph) plan() (graphPlan, error) {
	planner := graphPlanner{
		graph: g,
		plan: graphPlan{
			nodesByID:             make(map[string]NodeSpec, len(g.Nodes)),
			inputsByNode:          make(map[string]Inputs, len(g.Nodes)),
			dependencyCounts:      make([]int, len(g.Nodes)),
			dependencyNodeIndexes: make([][]int, len(g.Nodes)),
			dependentNodeIndexes:  make([][]int, len(g.Nodes)),
		},
		indexByID: make(map[string]int, len(g.Nodes)),
	}
	return planner.build()
}

// graphPlan is the stable output of graph planning. Mutable traversal state
// remains on graphPlanner and cannot leak into validation or compilation.
type graphPlan struct {
	nodesByID             map[string]NodeSpec
	inputsByNode          map[string]Inputs
	dependencyCounts      []int
	dependencyNodeIndexes [][]int
	dependentNodeIndexes  [][]int
}

// graphPlanner owns the indexes and counters mutated during one planning pass.
type graphPlanner struct {
	graph     Graph
	plan      graphPlan
	indexByID map[string]int
}

func (g *graphPlanner) build() (graphPlan, error) {
	if g.graph.Concurrency < 0 {
		return graphPlan{}, &GraphError{
			Field: "concurrency",
			Err: fmt.Errorf(
				"%w: concurrency must be non-negative, got %d",
				ErrInvalidGraph,
				g.graph.Concurrency,
			),
		}
	}
	if err := g.indexNodes(); err != nil {
		return graphPlan{}, err
	}
	if err := g.connectNodes(); err != nil {
		return graphPlan{}, err
	}
	for index := range g.plan.dependencyNodeIndexes {
		slices.Sort(g.plan.dependencyNodeIndexes[index])
	}
	if err := g.validateAcyclic(); err != nil {
		return graphPlan{}, err
	}
	return g.plan, nil
}

func (g *graphPlanner) indexNodes() error {
	for index, node := range g.graph.Nodes {
		switch {
		case node.ID == "":
			return &GraphError{
				Field: "id",
				Err:   fmt.Errorf("%w: node at index %d has an empty ID", ErrInvalidGraph, index),
			}
		case node.Type == "":
			return &GraphError{
				NodeID: node.ID,
				Field:  "type",
				Err:    fmt.Errorf("%w: node type is empty", ErrInvalidGraph),
			}
		}
		if _, duplicate := g.plan.nodesByID[node.ID]; duplicate {
			return &GraphError{NodeID: node.ID, Field: "id", Err: ErrDuplicateNode}
		}
		inputs, err := node.Inputs.withDefault(node.Input)
		if err != nil {
			return &GraphError{NodeID: node.ID, Field: "inputs", Err: err}
		}
		if err := inputs.validate(); err != nil {
			return &GraphError{
				NodeID: node.ID,
				Field:  "inputs",
				Err:    fmt.Errorf("%w: %w", ErrInvalidGraph, err),
			}
		}
		if err := g.validateGates(node); err != nil {
			return err
		}
		g.plan.inputsByNode[node.ID] = inputs
		g.plan.nodesByID[node.ID] = node
		g.indexByID[node.ID] = index
	}
	return nil
}

func (*graphPlanner) validateGates(node NodeSpec) error {
	if !node.Trigger.valid() {
		return &GraphError{
			NodeID: node.ID,
			Field:  "trigger",
			Err: fmt.Errorf(
				"%w: unknown trigger %q",
				ErrInvalidGraph,
				node.Trigger,
			),
		}
	}
	if len(node.When) == 0 {
		if node.Trigger == TriggerAny {
			return &GraphError{
				NodeID: node.ID,
				Field:  "trigger",
				Err: fmt.Errorf(
					"%w: trigger %q requires at least one gate",
					ErrInvalidGraph,
					node.Trigger,
				),
			}
		}
		return nil
	}

	seen := make(map[Gate]struct{}, len(node.When))
	sources := make(map[string]string, len(node.When))
	for _, gate := range node.When {
		switch {
		case gate.NodeID == "":
			return &GraphError{
				NodeID: node.ID,
				Field:  "when",
				Err:    fmt.Errorf("%w: gate source node ID is empty", ErrInvalidGraph),
			}
		case gate.Outlet == "":
			return &GraphError{
				NodeID: node.ID,
				Field:  "when",
				Err:    fmt.Errorf("%w: gate outlet is empty", ErrInvalidGraph),
			}
		}
		if _, duplicate := seen[gate]; duplicate {
			return &GraphError{
				NodeID: node.ID,
				Field:  "when",
				Err: fmt.Errorf(
					"%w: gate %q/%q is declared more than once",
					ErrInvalidGraph,
					gate.NodeID,
					gate.Outlet,
				),
			}
		}
		seen[gate] = struct{}{}

		if previous, duplicateSource := sources[gate.NodeID]; duplicateSource &&
			node.Trigger == TriggerAll {
			return &GraphError{
				NodeID: node.ID,
				Field:  "when",
				Err: fmt.Errorf(
					"%w: trigger %q requires routing node %q to select both %q and %q",
					ErrInvalidGraph,
					TriggerAll,
					gate.NodeID,
					previous,
					gate.Outlet,
				),
			}
		}
		sources[gate.NodeID] = gate.Outlet
	}
	return nil
}

func (g *graphPlanner) connectNodes() error {
	for nodeIndex, node := range g.graph.Nodes {
		connected := make(map[string]struct{})
		for _, ref := range g.plan.inputsByNode[node.ID].Refs() {
			if err := g.connectInput(nodeIndex, node.ID, ref.NodeID, connected); err != nil {
				return err
			}
		}
		for _, gate := range node.When {
			if err := g.connectGate(
				nodeIndex,
				node.ID,
				gate.NodeID,
				connected,
			); err != nil {
				return err
			}
		}
		explicit := make(map[string]struct{}, len(node.DependsOn))
		for _, dependency := range node.DependsOn {
			if err := g.validateExplicit(node.ID, dependency, explicit); err != nil {
				return err
			}
			if err := g.connectExplicit(
				nodeIndex,
				node.ID,
				dependency,
				connected,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *graphPlanner) connectGate(
	nodeIndex int,
	nodeID, dependency string,
	connected map[string]struct{},
) error {
	dependencyIndex, exists := g.indexByID[dependency]
	if !exists {
		return &GraphError{
			NodeID: nodeID,
			Field:  "when",
			Err:    fmt.Errorf("%w %q", ErrUnknownNode, dependency),
		}
	}
	return g.connectDependency(
		nodeIndex,
		nodeID,
		dependency,
		dependencyIndex,
		"when",
		connected,
	)
}

func (*graphPlanner) validateExplicit(
	nodeID, dependency string,
	seen map[string]struct{},
) error {
	if dependency == "" {
		return &GraphError{
			NodeID: nodeID,
			Field:  "dependsOn",
			Err:    fmt.Errorf("%w: dependency ID is empty", ErrInvalidGraph),
		}
	}
	if _, duplicate := seen[dependency]; duplicate {
		return &GraphError{
			NodeID: nodeID,
			Field:  "dependsOn",
			Err: fmt.Errorf(
				"%w: dependency %q is listed more than once",
				ErrInvalidGraph,
				dependency,
			),
		}
	}
	seen[dependency] = struct{}{}
	return nil
}

func (g *graphPlanner) connectInput(
	nodeIndex int,
	nodeID, dependency string,
	connected map[string]struct{},
) error {
	dependencyIndex, internal := g.indexByID[dependency]
	if !internal {
		return nil
	}
	return g.connectDependency(
		nodeIndex,
		nodeID,
		dependency,
		dependencyIndex,
		"inputs",
		connected,
	)
}

func (g *graphPlanner) connectExplicit(
	nodeIndex int,
	nodeID, dependency string,
	connected map[string]struct{},
) error {
	dependencyIndex, exists := g.indexByID[dependency]
	if !exists {
		return &GraphError{
			NodeID: nodeID,
			Field:  "dependsOn",
			Err:    fmt.Errorf("%w %q", ErrUnknownNode, dependency),
		}
	}
	return g.connectDependency(
		nodeIndex,
		nodeID,
		dependency,
		dependencyIndex,
		"dependsOn",
		connected,
	)
}

func (g *graphPlanner) connectDependency(
	nodeIndex int,
	nodeID, dependency string,
	dependencyIndex int,
	field string,
	connected map[string]struct{},
) error {
	if dependency == nodeID {
		return &GraphError{
			NodeID: nodeID,
			Field:  field,
			Err:    fmt.Errorf("%w: node depends on itself", ErrCycle),
		}
	}
	if _, duplicate := connected[dependency]; duplicate {
		return nil
	}
	connected[dependency] = struct{}{}
	g.plan.dependencyCounts[nodeIndex]++
	g.plan.dependencyNodeIndexes[nodeIndex] = append(
		g.plan.dependencyNodeIndexes[nodeIndex],
		dependencyIndex,
	)
	g.plan.dependentNodeIndexes[dependencyIndex] = append(
		g.plan.dependentNodeIndexes[dependencyIndex],
		nodeIndex,
	)
	return nil
}

func (g *graphPlanner) validateAcyclic() error {
	// Kahn's algorithm validates the graph in O(V+E). It works on a copy because
	// the original counts are the immutable execution plan reused by every run.
	dependencyCounts := slices.Clone(g.plan.dependencyCounts)
	ready := make([]int, 0, len(g.graph.Nodes))
	for nodeIndex, count := range dependencyCounts {
		if count == 0 {
			ready = append(ready, nodeIndex)
		}
	}

	processed := 0
	for head := 0; head < len(ready); head++ {
		node := ready[head]
		processed++
		for _, dependent := range g.plan.dependentNodeIndexes[node] {
			dependencyCounts[dependent]--
			if dependencyCounts[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
	}
	if processed != len(g.graph.Nodes) {
		return &GraphError{Err: ErrCycle}
	}
	return nil
}
