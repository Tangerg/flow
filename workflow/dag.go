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
// invalid node configs, and incompatible registered schemas, then runs each
// topological layer's nodes concurrently.
func (r *Registry) CompileGraph(g Graph) (Step, error) {
	layers, byID, err := r.validateGraph(g)
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

// CompileGraphJSON validates data against [GraphJSONSchema], strictly
// unmarshals it into a Graph, and compiles it.
func (r *Registry) CompileGraphJSON(data []byte) (Step, error) {
	if err := ValidateGraphJSON(data); err != nil {
		return nil, err
	}
	var g Graph
	if err := decodeStrict(data, &g); err != nil {
		return nil, &GraphError{Field: "json", Err: fmt.Errorf("%w: %w", ErrInvalidGraph, err)}
	}
	return r.CompileGraph(g)
}

// plan validates the graph structurally (unique IDs, no cycles) and returns its
// topological layers along with a lookup of nodes by ID. It is shared by Compile
// and ValidateGraph.
func (r *Registry) plan(g Graph) (layers [][]string, byID map[string]NodeSpec, err error) {
	byID = make(map[string]NodeSpec, len(g.Nodes))
	indexByID := make(map[string]int, len(g.Nodes))
	for i, n := range g.Nodes {
		if n.ID == "" {
			return nil, nil, &GraphError{Field: "id", Err: fmt.Errorf("%w: empty", ErrInvalidGraph)}
		}
		if _, dup := byID[n.ID]; dup {
			return nil, nil, &GraphError{NodeID: n.ID, Field: "id", Err: ErrDuplicateNode}
		}
		if n.Input != nil {
			if err := validateRef(*n.Input, fmt.Sprintf("node %q input", n.ID)); err != nil {
				return nil, nil, &GraphError{NodeID: n.ID, Field: "input", Err: fmt.Errorf("%w: %w", ErrInvalidGraph, err)}
			}
		}
		byID[n.ID] = n
		indexByID[n.ID] = i
	}

	indegree := make([]int, len(g.Nodes))
	dependents := make([][]int, len(g.Nodes))
	for i, n := range g.Nodes {
		seen := map[string]bool{}
		addDep := func(dep string, allowExternal bool) error {
			if dep == "" || seen[dep] {
				return nil
			}
			if dep == n.ID {
				return &GraphError{NodeID: n.ID, Field: "dependsOn", Err: fmt.Errorf("%w: self dependency", ErrCycle)}
			}
			depIndex, ok := indexByID[dep]
			if !ok {
				if allowExternal {
					return nil
				}
				return &GraphError{NodeID: n.ID, Field: "dependsOn", Err: fmt.Errorf("%w %q", ErrUnknownNode, dep)}
			}
			seen[dep] = true
			indegree[i]++
			dependents[depIndex] = append(dependents[depIndex], i)
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

	// Kahn's algorithm computes each node's barrier level in O(V+E). Levels are
	// materialized in a final spec-order pass so independent nodes retain the
	// deterministic order in which the caller declared them.
	queue := make([]int, 0, len(g.Nodes))
	for i, degree := range indegree {
		if degree == 0 {
			queue = append(queue, i)
		}
	}

	levels := make([]int, len(g.Nodes))
	processed := 0
	maxLevel := 0
	for head := 0; head < len(queue); head++ {
		node := queue[head]
		processed++
		for _, dependent := range dependents[node] {
			nextLevel := levels[node] + 1
			if levels[dependent] < nextLevel {
				levels[dependent] = nextLevel
				if maxLevel < nextLevel {
					maxLevel = nextLevel
				}
			}
			indegree[dependent]--
			if indegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}
	if processed != len(g.Nodes) {
		return nil, nil, &GraphError{Err: ErrCycle}
	}
	if len(g.Nodes) == 0 {
		return nil, byID, nil
	}

	layers = make([][]string, maxLevel+1)
	for i, n := range g.Nodes {
		layers[levels[i]] = append(layers[levels[i]], n.ID)
	}
	return layers, byID, nil
}

func nodeToSpec(n NodeSpec) Spec {
	return Spec{Kind: KindLeaf, ID: n.ID, Type: n.Type, Input: n.Input, Config: n.Config}
}
