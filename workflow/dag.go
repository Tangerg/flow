package workflow

import (
	"encoding/json"
	"fmt"
)

// NodeSpec describes one node in a flat [Graph]: a leaf built by the registry
// plus the edges into it. Dependencies are inferred from Input (when it points
// at another graph node) and from DependsOn. Input may reference an external
// seed Store value; every explicit DependsOn entry must name a graph node.
type NodeSpec struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Input     *Ref            `json:"input,omitempty"`
	Config    json.RawMessage `json:"config,omitempty"`
	DependsOn []string        `json:"dependsOn,omitempty"`
}

// Graph is a flat, arbitrarily wired DAG of leaf nodes — the shape a visual
// editor produces. Unlike a nested [Spec], any node may depend on any other as
// long as the result is acyclic. [Registry.CompileGraph] topologically layers it and
// builds Sequence(Parallel(layer)...) so independent nodes run concurrently.
type Graph struct {
	Nodes []NodeSpec `json:"nodes"`
}

// CompileGraph validates a flat Graph, builds its leaves, and returns a Step.
// It rejects duplicate IDs, missing dependencies, cycles, unknown node types,
// and incompatible registered schemas, then runs each topological layer's
// nodes concurrently.
func (r *Registry) CompileGraph(g Graph) (Step, error) {
	if err := r.ValidateGraph(g); err != nil {
		return nil, err
	}
	layers, byID, err := r.plan(g)
	if err != nil {
		return nil, err
	}

	var steps []Step
	for _, layer := range layers {
		branch := make([]Step, 0, len(layer))
		for _, id := range layer {
			leaf, err := r.buildLeaf(nodeToSpec(byID[id]))
			if err != nil {
				return nil, err
			}
			branch = append(branch, leaf)
		}
		if len(branch) == 1 {
			steps = append(steps, branch[0])
		} else {
			steps = append(steps, Parallel(branch...))
		}
	}
	return Sequence(steps...), nil
}

// CompileGraphJSON strictly unmarshals data into a [Graph] and compiles it.
func (r *Registry) CompileGraphJSON(data []byte) (Step, error) {
	var g Graph
	if err := decodeStrict(data, &g); err != nil {
		return nil, fmt.Errorf("workflow: invalid graph: %w", err)
	}
	return r.CompileGraph(g)
}

// plan validates the graph structurally (unique IDs, no cycles) and returns its
// topological layers along with a lookup of nodes by ID. It is shared by Compile
// and ValidateGraph.
func (r *Registry) plan(g Graph) (layers [][]string, byID map[string]NodeSpec, err error) {
	byID = make(map[string]NodeSpec, len(g.Nodes))
	for _, n := range g.Nodes {
		if n.ID == "" {
			return nil, nil, fmt.Errorf("workflow: graph node with empty ID")
		}
		if _, dup := byID[n.ID]; dup {
			return nil, nil, fmt.Errorf("workflow: duplicate node ID %q", n.ID)
		}
		if n.Input != nil {
			if err := validateRef(*n.Input, fmt.Sprintf("node %q input", n.ID)); err != nil {
				return nil, nil, err
			}
		}
		byID[n.ID] = n
	}

	indegree := make(map[string]int, len(g.Nodes))
	dependents := make(map[string][]string, len(g.Nodes))
	for _, n := range g.Nodes {
		seen := map[string]bool{}
		addDep := func(dep string, allowExternal bool) error {
			if dep == "" || seen[dep] {
				return nil
			}
			if dep == n.ID {
				return fmt.Errorf("workflow: node %q depends on itself", n.ID)
			}
			if _, ok := byID[dep]; !ok {
				if allowExternal {
					return nil
				}
				return fmt.Errorf("workflow: node %q depends on unknown node %q", n.ID, dep)
			}
			seen[dep] = true
			indegree[n.ID]++
			dependents[dep] = append(dependents[dep], n.ID)
			return nil
		}
		if n.Input != nil {
			if err := addDep(n.Input.NodeID, true); err != nil {
				return nil, nil, err
			}
		}
		for _, d := range n.DependsOn {
			if err := addDep(d, false); err != nil {
				return nil, nil, err
			}
		}
	}

	processed := make(map[string]bool, len(g.Nodes))
	for len(processed) < len(g.Nodes) {
		// Collect every node whose dependencies are all satisfied (in spec order
		// for a stable layout).
		var layer []string
		for _, n := range g.Nodes {
			if !processed[n.ID] && indegree[n.ID] == 0 {
				layer = append(layer, n.ID)
			}
		}
		if len(layer) == 0 {
			return nil, nil, fmt.Errorf("workflow: graph has a cycle")
		}
		for _, id := range layer {
			processed[id] = true
		}
		// Release dependents only after the whole layer is collected (barrier).
		for _, id := range layer {
			for _, dep := range dependents[id] {
				indegree[dep]--
			}
		}
		layers = append(layers, layer)
	}
	return layers, byID, nil
}

func nodeToSpec(n NodeSpec) Spec {
	return Spec{Kind: KindLeaf, ID: n.ID, Type: n.Type, Input: n.Input, Config: n.Config}
}
