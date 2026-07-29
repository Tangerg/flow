package workflow

import (
	"encoding/json"
	"fmt"
	"slices"
)

// NodeSpec describes one node in a flat [Graph]: a leaf built by the registry
// plus the edges into it. Dependencies are inferred from every wired input port
// that points at another graph node, and from DependsOn. An input may reference
// an external seed Store value; every explicit DependsOn entry must name a graph
// node.
//
// Input wires [DefaultPort] and is sugar for the common single-input node;
// Inputs wires ports by name. Setting the default port both ways is rejected as
// [ErrDuplicatePort].
type NodeSpec struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Input     *Ref            `json:"input,omitempty"`
	Inputs    Inputs          `json:"inputs,omitempty"`
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
// invalid node configs, nil factory results, and incompatible registered
// schemas, then runs each topological layer's nodes concurrently. Build errors
// are reported as GraphError values at the graph node and field that caused them.
func (r *Registry) CompileGraph(g Graph) (Step, error) {
	plan, err := r.validateGraph(g)
	if err != nil {
		return nil, err
	}

	var steps []Step
	for _, layer := range plan.layers {
		branch := make([]Step, 0, len(layer))
		for _, id := range layer {
			leaf, field, err := r.makeLeaf(plan.byID[id].spec())
			if err != nil {
				return nil, &GraphError{
					NodeID: id,
					Field:  field,
					Err:    fmt.Errorf("%w: %w", ErrInvalidGraph, err),
				}
			}
			branch = append(branch, leaf)
		}
		if len(branch) == 1 {
			steps = append(steps, branch[0])
		} else {
			steps = append(steps, Parallel(branch, ParallelConfig{}))
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
	if err := jsonDocument(data).decode(&g); err != nil {
		return nil, &GraphError{Field: "json", Err: fmt.Errorf("%w: %w", ErrInvalidGraph, err)}
	}
	return r.CompileGraph(g)
}

// plan validates the graph structurally. Registry-level semantic checks build
// on the resulting plan.
func (g Graph) plan() (graphPlan, error) {
	plan := graphPlan{
		graph:      g,
		byID:       make(map[string]NodeSpec, len(g.Nodes)),
		wiring:     make(map[string]Inputs, len(g.Nodes)),
		indexByID:  make(map[string]int, len(g.Nodes)),
		indegree:   make([]int, len(g.Nodes)),
		dependents: make([][]int, len(g.Nodes)),
	}
	if err := plan.index(); err != nil {
		return graphPlan{}, err
	}
	if err := plan.connect(); err != nil {
		return graphPlan{}, err
	}
	var err error
	plan.layers, err = plan.order()
	if err != nil {
		return graphPlan{}, err
	}
	return plan, nil
}

// graphPlan owns the mutable state of one planning pass. Keeping these related
// indexes together makes it impossible to advance one phase with a mismatched
// graph or dependency table.
type graphPlan struct {
	graph      Graph
	byID       map[string]NodeSpec
	wiring     map[string]Inputs
	indexByID  map[string]int
	indegree   []int
	dependents [][]int
	layers     [][]string
}

func (p *graphPlan) index() error {
	for i, n := range p.graph.Nodes {
		switch {
		case n.ID == "":
			return &GraphError{Field: "id", Err: fmt.Errorf("%w: empty", ErrInvalidGraph)}
		case n.Type == "":
			return &GraphError{NodeID: n.ID, Field: "type", Err: fmt.Errorf("%w: empty", ErrInvalidGraph)}
		}
		if _, duplicate := p.byID[n.ID]; duplicate {
			return &GraphError{NodeID: n.ID, Field: "id", Err: ErrDuplicateNode}
		}
		inputs, err := n.Inputs.withDefault(n.Input)
		if err != nil {
			return &GraphError{NodeID: n.ID, Field: "inputs", Err: err}
		}
		if err := inputs.validate(fmt.Sprintf("node %q", n.ID)); err != nil {
			return &GraphError{NodeID: n.ID, Field: "inputs", Err: fmt.Errorf("%w: %w", ErrInvalidGraph, err)}
		}
		p.wiring[n.ID] = inputs
		p.byID[n.ID] = n
		p.indexByID[n.ID] = i
	}
	return nil
}

func (p *graphPlan) connect() error {
	for i, n := range p.graph.Nodes {
		seen := map[string]bool{}
		for _, ref := range p.wiring[n.ID].Refs() {
			if err := p.addDependency(i, n.ID, ref.NodeID, true, seen); err != nil {
				return err
			}
		}
		explicit := make(map[string]struct{}, len(n.DependsOn))
		for _, d := range n.DependsOn {
			if d == "" {
				return &GraphError{
					NodeID: n.ID,
					Field:  "dependsOn",
					Err:    fmt.Errorf("%w: empty dependency", ErrInvalidGraph),
				}
			}
			if _, duplicate := explicit[d]; duplicate {
				return &GraphError{
					NodeID: n.ID,
					Field:  "dependsOn",
					Err:    fmt.Errorf("%w: duplicate dependency %q", ErrInvalidGraph, d),
				}
			}
			explicit[d] = struct{}{}
			if err := p.addDependency(i, n.ID, d, false, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *graphPlan) addDependency(nodeIndex int, nodeID, dependency string, allowExternal bool, seen map[string]bool) error {
	if dependency == "" || seen[dependency] {
		return nil
	}
	if dependency == nodeID {
		return &GraphError{NodeID: nodeID, Field: "dependsOn", Err: fmt.Errorf("%w: self dependency", ErrCycle)}
	}
	dependencyIndex, ok := p.indexByID[dependency]
	if !ok {
		if allowExternal {
			return nil
		}
		return &GraphError{NodeID: nodeID, Field: "dependsOn", Err: fmt.Errorf("%w %q", ErrUnknownNode, dependency)}
	}
	seen[dependency] = true
	p.indegree[nodeIndex]++
	p.dependents[dependencyIndex] = append(p.dependents[dependencyIndex], nodeIndex)
	return nil
}

func (p *graphPlan) order() ([][]string, error) {
	// Kahn's algorithm computes each node's barrier level in O(V+E). Levels are
	// materialized in a final spec-order pass so independent nodes retain the
	// deterministic order in which the caller declared them.
	queue := make([]int, 0, len(p.graph.Nodes))
	for i, degree := range p.indegree {
		if degree == 0 {
			queue = append(queue, i)
		}
	}

	levels := make([]int, len(p.graph.Nodes))
	processed := 0
	maxLevel := 0
	for head := 0; head < len(queue); head++ {
		node := queue[head]
		processed++
		for _, dependent := range p.dependents[node] {
			nextLevel := levels[node] + 1
			if levels[dependent] < nextLevel {
				levels[dependent] = nextLevel
				if maxLevel < nextLevel {
					maxLevel = nextLevel
				}
			}
			p.indegree[dependent]--
			if p.indegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}
	if processed != len(p.graph.Nodes) {
		return nil, &GraphError{Err: ErrCycle}
	}
	if len(p.graph.Nodes) == 0 {
		return nil, nil
	}

	layers := make([][]string, maxLevel+1)
	for i, n := range p.graph.Nodes {
		layers[levels[i]] = append(layers[levels[i]], n.ID)
	}
	return layers, nil
}

func (n NodeSpec) spec() Spec {
	return Spec{Kind: KindLeaf, ID: n.ID, Type: n.Type, Input: n.Input, Inputs: n.Inputs, Config: n.Config}
}

// Inputs returns the external references the Graph reads: wired input ports
// whose nodeID names no node in the graph. These are the values a caller must
// seed into the [Store] before running, so an editor can render them as the
// workflow's parameters.
//
// The result is deduplicated and ordered by reference. A malformed graph yields
// the references it can still resolve; use [Registry.ValidateGraph] to reject it.
func (g Graph) Inputs() []Ref {
	internal := make(map[string]struct{}, len(g.Nodes))
	for _, n := range g.Nodes {
		internal[n.ID] = struct{}{}
	}

	seen := make(map[Ref]struct{})
	external := make([]Ref, 0, len(g.Nodes))
	for _, n := range g.Nodes {
		inputs, err := n.Inputs.withDefault(n.Input)
		if err != nil {
			continue
		}
		for _, ref := range inputs.Refs() {
			if _, ok := internal[ref.NodeID]; ok {
				continue
			}
			if _, duplicate := seen[ref]; duplicate {
				continue
			}
			seen[ref] = struct{}{}
			external = append(external, ref)
		}
	}
	slices.SortFunc(external, Ref.compare)
	return external
}

// MissingInputs returns the references from [Graph.Inputs] that s does not
// resolve. An empty result means the Store satisfies every external read.
func (g Graph) MissingInputs(s Store) []Ref {
	missing := make([]Ref, 0, len(g.Nodes))
	for _, ref := range g.Inputs() {
		if _, ok := s.Lookup(ref); !ok {
			missing = append(missing, ref)
		}
	}
	return missing
}
