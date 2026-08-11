package workflow

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/Tangerg/flow"
)

// plan validates the graph structurally. Registry-level semantic checks build
// on the resulting plan.
func (g Graph) plan() (graphPlan, error) {
	planner := graphPlanner{
		graph: g,
		plan: graphPlan{
			nodesByID:             make(map[string]GraphNode, len(g.Nodes)),
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
	nodesByID             map[string]GraphNode
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

// gateValidator owns the cross-gate state for one graph node. Keeping this
// separate from graphPlanner makes the node-local routing rules independent of
// dependency indexing.
type gateValidator struct {
	path    string
	nodeID  string
	trigger Trigger
	gates   []Gate
	seen    map[Gate]struct{}
	sources map[string]string
}

func (g *graphPlanner) build() (graphPlan, error) {
	if err := (flow.MapConfig{Concurrency: g.graph.Concurrency}).Validate(); err != nil {
		return graphPlan{}, &GraphError{
			Field: fieldConcurrency,
			Err:   err,
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
		path := graphNodePath(index)
		if err := validateStepID(node.ID); err != nil {
			return &GraphError{
				Path:  path,
				Field: fieldID,
				Err:   err,
			}
		}
		if err := validateName("node type", node.Type); err != nil {
			return &GraphError{
				Path:   path,
				NodeID: node.ID,
				Field:  fieldType,
				Err:    err,
			}
		}
		if _, duplicate := g.plan.nodesByID[node.ID]; duplicate {
			return &GraphError{
				Path:   path,
				NodeID: node.ID,
				Field:  fieldID,
				Err:    ErrDuplicateNode,
			}
		}
		if err := node.Inputs.validatePorts(); err != nil {
			return &GraphError{
				Path:   path,
				NodeID: node.ID,
				Field:  fieldInputs,
				Err:    err,
			}
		}
		gates := gateValidator{
			path:    path,
			nodeID:  node.ID,
			trigger: node.Trigger,
			gates:   node.When,
		}
		if err := gates.validate(); err != nil {
			return err
		}
		g.plan.nodesByID[node.ID] = node
		g.indexByID[node.ID] = index
	}
	return nil
}

func (g *gateValidator) validate() error {
	if !g.trigger.valid() {
		return g.fieldError(fieldTrigger, fmt.Errorf(
			"unknown trigger %q",
			g.trigger,
		))
	}
	if len(g.gates) == 0 {
		if g.trigger == TriggerAny {
			return g.fieldError(fieldTrigger, fmt.Errorf(
				"trigger %q requires at least one gate",
				g.trigger,
			))
		}
		return nil
	}

	g.seen = make(map[Gate]struct{}, len(g.gates))
	g.sources = make(map[string]string, len(g.gates))
	for _, gate := range g.gates {
		if err := g.validateGate(gate); err != nil {
			return err
		}
	}
	return nil
}

func (g *gateValidator) validateGate(gate Gate) error {
	if err := validateName("gate source node ID", gate.NodeID); err != nil {
		return g.fieldError(fieldWhen, err)
	}
	if err := validateName("gate outlet", gate.Outlet); err != nil {
		return g.fieldError(fieldWhen, err)
	}
	if _, duplicate := g.seen[gate]; duplicate {
		return g.fieldError(fieldWhen, fmt.Errorf(
			"gate %q/%q is declared more than once",
			gate.NodeID,
			gate.Outlet,
		))
	}
	g.seen[gate] = struct{}{}

	if previous, duplicateSource := g.sources[gate.NodeID]; duplicateSource &&
		g.trigger == TriggerAll {
		return g.fieldError(fieldWhen, fmt.Errorf(
			"trigger %q requires routing node %q to select both %q and %q",
			TriggerAll,
			gate.NodeID,
			previous,
			gate.Outlet,
		))
	}
	g.sources[gate.NodeID] = gate.Outlet
	return nil
}

func (g *gateValidator) fieldError(field string, err error) error {
	return &GraphError{
		Path:   g.path,
		NodeID: g.nodeID,
		Field:  field,
		Err:    err,
	}
}

func (g *graphPlanner) connectNodes() error {
	for nodeIndex, node := range g.graph.Nodes {
		connector := nodeConnector{
			planner:   g,
			nodeIndex: nodeIndex,
			path:      graphNodePath(nodeIndex),
			nodeID:    node.ID,
			connected: make(map[string]struct{}),
			explicit:  make(map[string]struct{}, len(node.DependsOn)),
		}
		if err := connector.connect(node); err != nil {
			return err
		}
	}
	return nil
}

// nodeConnector owns the dependency state for one graph node. A dependency may
// arrive through data, routing, or DependsOn, but it is inserted into the plan
// once; explicit duplicates remain errors because they are definition typos.
type nodeConnector struct {
	planner   *graphPlanner
	nodeIndex int
	path      string
	nodeID    string
	connected map[string]struct{}
	explicit  map[string]struct{}
}

func (n *nodeConnector) connect(node GraphNode) error {
	for _, ref := range node.Inputs.Refs() {
		if err := n.connectInput(ref.NodeID); err != nil {
			return err
		}
	}
	for _, gate := range node.When {
		if err := n.connectGate(gate.NodeID); err != nil {
			return err
		}
	}
	for _, dependency := range node.DependsOn {
		if err := n.connectExplicit(dependency); err != nil {
			return err
		}
	}
	return nil
}

func (n *nodeConnector) connectGate(dependency string) error {
	dependencyIndex, exists := n.planner.indexByID[dependency]
	if !exists {
		return n.fieldError(fieldWhen, fmt.Errorf("%w %q", ErrUnknownNode, dependency))
	}
	return n.connectDependency(dependency, dependencyIndex, fieldWhen)
}

func (n *nodeConnector) connectExplicit(dependency string) error {
	if err := validateName("dependency ID", dependency); err != nil {
		return n.fieldError(fieldDependsOn, err)
	}
	if _, duplicate := n.explicit[dependency]; duplicate {
		return n.fieldError(fieldDependsOn, fmt.Errorf(
			"dependency %q is listed more than once",
			dependency,
		))
	}
	n.explicit[dependency] = struct{}{}
	if _, implied := n.connected[dependency]; implied {
		return n.fieldError(fieldDependsOn, fmt.Errorf(
			"dependency %q is already implied by an input or gate",
			dependency,
		))
	}

	dependencyIndex, exists := n.planner.indexByID[dependency]
	if !exists {
		return n.fieldError(fieldDependsOn, fmt.Errorf("%w %q", ErrUnknownNode, dependency))
	}
	return n.connectDependency(dependency, dependencyIndex, fieldDependsOn)
}

func (n *nodeConnector) connectInput(dependency string) error {
	dependencyIndex, internal := n.planner.indexByID[dependency]
	if !internal {
		return nil
	}
	return n.connectDependency(dependency, dependencyIndex, fieldInputs)
}

func (n *nodeConnector) connectDependency(
	dependency string,
	dependencyIndex int,
	field string,
) error {
	if dependency == n.nodeID {
		return n.fieldError(field, fmt.Errorf("%w: node depends on itself", ErrCycle))
	}
	if _, duplicate := n.connected[dependency]; duplicate {
		return nil
	}
	n.connected[dependency] = struct{}{}
	n.planner.plan.dependencyCounts[n.nodeIndex]++
	n.planner.plan.dependencyNodeIndexes[n.nodeIndex] = append(
		n.planner.plan.dependencyNodeIndexes[n.nodeIndex],
		dependencyIndex,
	)
	n.planner.plan.dependentNodeIndexes[dependencyIndex] = append(
		n.planner.plan.dependentNodeIndexes[dependencyIndex],
		n.nodeIndex,
	)
	return nil
}

func (n *nodeConnector) fieldError(field string, err error) error {
	return &GraphError{
		Path:   n.path,
		NodeID: n.nodeID,
		Field:  field,
		Err:    err,
	}
}

func graphNodePath(index int) string {
	return pointerPath{fieldNodes, strconv.Itoa(index)}.encode()
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
