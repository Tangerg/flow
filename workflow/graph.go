package workflow

import (
	"encoding/json"
	"slices"
)

// NodeSpec describes one node in a flat [Graph]: a leaf built by the registry
// plus the edges into it. Dependencies are inferred from every wired input port
// that points at another graph node, and from DependsOn. An input may reference
// an external seed Store value; every explicit DependsOn entry must name a graph
// node.
//
// Input wires [DefaultPort] and is sugar for the common single-input node; its
// zero value means absent. Inputs wires ports by name. Setting the default port
// both ways is rejected as [ErrDuplicatePort]. When gates execution on routing
// outputs; its zero Trigger requires every gate, while [TriggerAny] requires one.
type NodeSpec struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Input     Ref             `json:"input,omitzero"`
	Inputs    Inputs          `json:"inputs,omitempty"`
	Config    json.RawMessage `json:"config,omitempty"`
	DependsOn []string        `json:"dependsOn,omitempty"`
	When      []Gate          `json:"when,omitempty"`
	Trigger   Trigger         `json:"trigger,omitempty"`
}

// Graph is a flat, arbitrarily wired DAG of leaf nodes — the shape a visual
// editor produces. Unlike a nested [Spec], any node may depend on any other as
// long as the result is acyclic. [Registry.CompileGraph] topologically layers it
// and runs independent nodes concurrently. Routing nodes select conditional
// targets through [NodeSpec.When]. Concurrency limits each layer; zero means
// unbounded.
//
// A compiled Graph owns Store cells named by its node IDs. Each invocation
// clears those cells and reconstructs them from current execution or Journal
// replay, so a Store returned by an earlier invocation is safe to reuse with new
// external inputs.
type Graph struct {
	Nodes       []NodeSpec `json:"nodes"`
	Concurrency int        `json:"concurrency,omitempty"`
}

// Inputs returns the external references the Graph reads: wired input ports
// whose nodeID names no node in the graph. These are the values a caller must
// seed into the [Store] before running, so an editor can render them as the
// workflow's parameters.
//
// The result is deduplicated and ordered by reference. A malformed graph yields
// the references it can still resolve; use [Registry.ValidateGraph] to reject it.
func (g Graph) Inputs() []Ref {
	internalNodeIDs := make(map[string]struct{}, len(g.Nodes))
	for _, node := range g.Nodes {
		internalNodeIDs[node.ID] = struct{}{}
	}

	seen := make(map[Ref]struct{})
	externalRefs := make([]Ref, 0, len(g.Nodes))
	for _, node := range g.Nodes {
		inputs, err := node.Inputs.withDefault(node.Input)
		if err != nil {
			continue
		}
		for _, ref := range inputs.Refs() {
			if _, internal := internalNodeIDs[ref.NodeID]; internal {
				continue
			}
			if _, duplicate := seen[ref]; duplicate {
				continue
			}
			seen[ref] = struct{}{}
			externalRefs = append(externalRefs, ref)
		}
	}
	slices.SortFunc(externalRefs, Ref.compare)
	return externalRefs
}

// MissingInputs returns the references from [Graph.Inputs] that s does not
// resolve. An empty result means the Store satisfies every external read.
func (g Graph) MissingInputs(store Store) []Ref {
	missing := make([]Ref, 0, len(g.Nodes))
	for _, ref := range g.Inputs() {
		if _, ok := store.Lookup(ref); !ok {
			missing = append(missing, ref)
		}
	}
	return missing
}
