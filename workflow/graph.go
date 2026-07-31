package workflow

import (
	"encoding/json"
	"slices"
)

// GraphNode describes one node in a flat [Graph]: a leaf built by the registry
// plus the edges into it. Inputs wires every data edge by port name. Dependencies
// are inferred from ports that point at another graph node and from DependsOn.
// An input may reference an external seed Store value; every explicit DependsOn
// entry must name a graph node.
//
// A single-input node uses [DefaultInput] to wire [DefaultPort] like every other
// named port. When gates execution on routing outputs; its zero Trigger requires
// every gate, while [TriggerAny] requires one.
type GraphNode struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Inputs    Inputs          `json:"inputs,omitempty"`
	Config    json.RawMessage `json:"config,omitempty"`
	DependsOn []string        `json:"dependsOn,omitempty"`
	When      []Gate          `json:"when,omitempty"`
	Trigger   Trigger         `json:"trigger,omitempty"`
}

// Graph is a flat, arbitrarily wired DAG of registered nodes — the shape a visual
// editor produces. Unlike a nested [Spec], any node may depend on any other as
// long as the result is acyclic. [Registry.CompileGraph] starts each node as
// soon as its dependencies complete. Routing nodes select conditional targets
// through [GraphNode.When]. Concurrency limits the whole graph; zero means
// unbounded.
//
// Each node sees the invocation's input Store plus the completed Stores of its
// declared dependencies, merged in graph declaration order. It cannot observe
// an unrelated node merely because that node happened to finish first. Final
// results use the same order, so same-cell conflicts never depend on goroutine
// scheduling.
//
// A suspension blocks only its descendants; other ready work and writes
// returned with the suspension are retained for Journal-backed resumption. A
// failure cancels running siblings, prevents new nodes from starting, and
// returns the writes of nodes that had already completed along with the error.
//
// A compiled Graph owns Store cells named by its node IDs. Each invocation
// clears those cells and reconstructs them from current execution or Journal
// replay, so a Store returned by an earlier invocation is safe to reuse with new
// external inputs.
type Graph struct {
	Nodes       []GraphNode `json:"nodes"`
	Concurrency int         `json:"concurrency,omitempty"`
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

	externalRefs := make([]Ref, 0, len(g.Nodes))
	for _, node := range g.Nodes {
		for _, ref := range node.Inputs.Refs() {
			if _, internal := internalNodeIDs[ref.NodeID]; internal {
				continue
			}
			externalRefs = append(externalRefs, ref)
		}
	}
	// Sorting is part of the contract, so duplicates end up adjacent and need no
	// separate set to detect.
	slices.SortFunc(externalRefs, Ref.compare)
	return slices.Compact(externalRefs)
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
