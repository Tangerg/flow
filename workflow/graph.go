package workflow

import (
	"encoding/json"
	"slices"
)

// GraphNode describes one Store-sealed node in a flat [Graph], plus the edges
// into it. A [NodeFactory] supplies the execution boundary; composite regions
// cross this boundary through [Subgraph]. Inputs wires every data edge by port
// name. Dependencies are inferred from ports that point at another graph node
// and gates. DependsOn adds only pure control edges and must not repeat one of
// those inferred dependencies. An input may reference an external seed Store
// value; every explicit DependsOn entry must name a graph node.
//
// A single-input node uses [OneInput] to wire [DefaultPort] like every other
// named port. When gates execution on routing outputs; nil and empty slices both
// mean no gates. Its zero Trigger requires every declared gate, while
// [TriggerAny] requires at least one. Every gate source remains a dependency:
// trigger evaluation starts only after all of them complete.
type GraphNode struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
	Inputs Inputs `json:"inputs,omitempty"`
	// Config is absent only when it has zero length. Non-empty bytes must contain
	// one complete JSON value.
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
// unbounded. The zero Graph is an empty identity definition.
//
// Each node sees the invocation's input Store plus only the writes produced by
// its declared direct dependencies, applied in graph declaration order. It
// cannot observe a transitive or unrelated node merely because that node
// happened to finish first.
//
// A suspension blocks only its descendants; other ready work may finish, and
// accepted completions remain in the returned Store. Their journaled boundaries
// replay on resumption; opaque work with no such boundary runs again. A failure
// cancels running siblings, prevents new nodes from starting, and returns the
// writes whose completion the scheduler had already accepted along with the
// error. Parent cancellation follows the same commit rule and its cause takes
// precedence. Cancellation is cooperative: the Graph waits for every admitted
// node to return, so a node that ignores its context can prevent the run from
// finishing. A failure becomes the cancellation cause seen by already-running
// siblings.
//
// A compiled Graph owns Store cells named by its node IDs. Once a call is
// admitted, it clears those cells and reconstructs them from current execution
// or Journal replay, so a Store returned by an earlier invocation is safe to
// reuse with new external inputs. A call cancelled before admission returns its
// input unchanged; after admission, its returned Store has the owned cells
// cleared and only accepted completions restored. That cleanup remains a Store
// change when the Graph is nested in [Parallel], so fan-out merging cannot
// resurrect an output that the current Graph invocation bypassed.
//
// When several admitted nodes fail concurrently, completion timing decides
// which failure the scheduler observes and returns first. Deterministic Store
// merging does not impose an artificial order on concurrent application work.
//
// Treat Nodes and their maps, slices, and raw config as immutable while calling
// Inputs, MissingInputs, Registry.ValidateGraph, or Registry.CompileGraph. A
// compiled Step does not retain the Graph or any of those mutable values.
//
//nolint:recvcheck // UnmarshalJSON must be a pointer method to satisfy json.Unmarshaler.
type Graph struct {
	Nodes       []GraphNode `json:"nodes"`
	Concurrency int         `json:"concurrency,omitempty"`
}

// MarshalJSON encodes g only when the complete document can cross the strict
// JSON boundary unchanged and within [MaxNestingDepth]. Registry-dependent
// definition checks remain the responsibility of [Registry.ValidateGraph] or
// compilation.
func (g Graph) MarshalJSON() ([]byte, error) {
	return (graphJSONEncoder{graph: g}).marshal()
}

// UnmarshalJSON atomically replaces g with one strictly decoded Graph. It uses
// the same JSON Schema, duplicate-member, Unicode, integer, unknown-field, and
// nesting rules as [ValidateGraphJSON] and [Registry.CompileGraphJSON].
func (g *Graph) UnmarshalJSON(data []byte) error {
	return decodeJSONInto(g, data, decodeGraphDocument, graphJSONError)
}

// nodeIDs is the namespace the graph declares. A malformed graph still has one --
// duplicate IDs collapse, which is what a caller reading external references
// needs and not what validation needs.
func (g Graph) nodeIDs() nodeSet {
	ids := make(nodeSet, len(g.Nodes))
	for _, node := range g.Nodes {
		ids[node.ID] = struct{}{}
	}
	return ids
}

// Inputs returns the external references the Graph may read: wired input ports
// whose nodeID names no node in the graph. An editor can render this complete
// potential-input set without compiling or running the definition. Inputs of a
// conditional node remain present even when a particular run bypasses that
// node, so this is not an unconditional required-parameter set.
//
// The result is deduplicated, ordered by reference, and a fresh slice the
// caller owns. A malformed graph yields the references it can still resolve;
// use [Registry.ValidateGraph] to reject it.
func (g Graph) Inputs() []Ref {
	internal := g.nodeIDs()

	externalRefs := make([]Ref, 0, len(g.Nodes))
	for _, node := range g.Nodes {
		for _, ref := range node.Inputs.Refs() {
			if internal.has(ref.NodeID) {
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

// MissingInputs returns the references from [Graph.Inputs] that [Store.Lookup]
// does not resolve. The returned slice is a copy. An empty result means the Store satisfies every potential
// external read. A non-empty result does not by itself prevent a run: a
// conditional node that would read one of those references may be bypassed.
func (g Graph) MissingInputs(store Store) []Ref {
	missing := make([]Ref, 0, len(g.Nodes))
	for _, ref := range g.Inputs() {
		if _, ok := store.Lookup(ref); !ok {
			missing = append(missing, ref)
		}
	}
	return missing
}
