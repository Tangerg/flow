package diagram

import (
	"cmp"
	"fmt"
	"html"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/Tangerg/flow/workflow"
)

// ASCII returns a deterministic, line-oriented rendering of graph.
func ASCII(graph workflow.Graph) string {
	return newRenderer(graph).ascii()
}

// Mermaid returns a deterministic Mermaid flowchart for graph.
func Mermaid(graph workflow.Graph) string {
	return newRenderer(graph).mermaid()
}

type edgeKind uint8

const (
	dataEdge edgeKind = iota
	dependencyEdge
	gateEdge
)

type edge struct {
	source      string
	ref         workflow.Ref
	targetIndex int
	label       string
	kind        edgeKind
}

// externalNode keeps renderer identity structured. Ref.String is deliberately
// only a display form: a node ID containing "#/" can make two different Refs
// render alike, and using that text as a map key would merge their edges.
type externalNode struct {
	source   string
	ref      workflow.Ref
	valueRef bool
}

func (e externalNode) label() string {
	if e.valueRef {
		return e.ref.String()
	}
	return e.source
}

// compare orders externals by what the diagram shows, then by the identity
// behind it, because two externals can render the same label: a node ID may be
// spelled like a Ref. Once labels tie, kindOrder decides between the two kinds,
// and only two value refs can still tie -- two sources sharing a label are the
// same source, because a source's label is itself, so the map already holds one
// of them. That is why the source is not compared here: it could never decide.
// See TestMermaid_ordersASourceBeforeAValueRefSharingItsLabel.
func (e externalNode) compare(other externalNode) int {
	return cmp.Or(
		strings.Compare(e.label(), other.label()),
		cmp.Compare(e.kindOrder(), other.kindOrder()),
		cmp.Compare(e.ref.NodeID, other.ref.NodeID),
		cmp.Compare(e.ref.Path, other.ref.Path),
	)
}

// kindOrder puts a whole external source before a path into one. Which comes
// first is a choice; that it is always the same one is not, because externals are
// collected from a map and two of them may render alike.
func (e externalNode) kindOrder() int {
	if e.valueRef {
		return 1
	}
	return 0
}

func (e edge) external() externalNode {
	if e.kind == dataEdge {
		return externalNode{ref: e.ref, valueRef: true}
	}
	return externalNode{source: e.source}
}

type renderer struct {
	graph       workflow.Graph
	nodeIndexes map[string]int
	edges       []edge
	externals   []externalNode
}

func newRenderer(graph workflow.Graph) *renderer {
	r := &renderer{
		graph:       graph,
		nodeIndexes: make(map[string]int, len(graph.Nodes)),
	}
	for index, node := range graph.Nodes {
		if _, exists := r.nodeIndexes[node.ID]; !exists {
			r.nodeIndexes[node.ID] = index
		}
	}
	r.collectEdges()
	r.collectExternals()
	return r
}

func (r *renderer) collectEdges() {
	for targetIndex, node := range r.graph.Nodes {
		for _, port := range node.Inputs.PortNames() {
			r.addDataEdge(targetIndex, port, node.Inputs[port])
		}
		for _, dependency := range node.DependsOn {
			r.edges = append(r.edges, edge{
				source:      dependency,
				targetIndex: targetIndex,
				label:       "depends",
				kind:        dependencyEdge,
			})
		}
		for _, gate := range node.When {
			label := "when:all=" + gate.Outlet
			if node.Trigger == workflow.TriggerAny {
				label = "when:any=" + gate.Outlet
			}
			r.edges = append(r.edges, edge{
				source:      gate.NodeID,
				targetIndex: targetIndex,
				label:       label,
				kind:        gateEdge,
			})
		}
	}
}

func (r *renderer) addDataEdge(targetIndex int, port string, ref workflow.Ref) {
	label := port
	if ref != workflow.Output(ref.NodeID) {
		label += ": " + ref.Path
	}
	r.edges = append(r.edges, edge{
		source:      ref.NodeID,
		ref:         ref,
		targetIndex: targetIndex,
		label:       label,
		kind:        dataEdge,
	})
}

func (r *renderer) collectExternals() {
	external := make(map[externalNode]struct{})
	for _, edge := range r.edges {
		if _, internal := r.nodeIndexes[edge.source]; internal {
			continue
		}
		external[edge.external()] = struct{}{}
	}
	r.externals = slices.SortedFunc(maps.Keys(external), externalNode.compare)
}

func (r *renderer) ascii() string {
	var output strings.Builder
	output.WriteString("nodes:\n")
	if len(r.graph.Nodes) == 0 {
		output.WriteString("  (none)\n")
	} else {
		for _, node := range r.graph.Nodes {
			fmt.Fprintf(
				&output,
				"  %s [%s]\n",
				strconv.Quote(node.ID),
				strconv.Quote(node.Type),
			)
		}
	}

	output.WriteString("edges:\n")
	if len(r.edges) == 0 {
		output.WriteString("  (none)\n")
		return output.String()
	}
	for _, edge := range r.edges {
		source := edge.source
		if edge.kind == dataEdge {
			source = edge.ref.String()
		}
		fmt.Fprintf(
			&output,
			"  %s --%s--> %s\n",
			strconv.Quote(source),
			strconv.Quote(edge.label),
			strconv.Quote(r.graph.Nodes[edge.targetIndex].ID),
		)
	}
	return output.String()
}

func (r *renderer) mermaid() string {
	var output strings.Builder
	output.WriteString("flowchart LR\n")
	for index, node := range r.graph.Nodes {
		fmt.Fprintf(
			&output,
			"  n%d[\"%s<br/>%s\"]\n",
			index,
			mermaidLabel(node.ID),
			mermaidLabel(node.Type),
		)
	}

	externalIndexes := make(map[externalNode]int, len(r.externals))
	for index, external := range r.externals {
		externalIndexes[external] = index
		fmt.Fprintf(
			&output,
			"  x%d[\"%s\"]\n",
			index,
			mermaidLabel(external.label()),
		)
	}

	for _, edge := range r.edges {
		source := r.mermaidSource(edge, externalIndexes)
		target := fmt.Sprintf("n%d", edge.targetIndex)
		arrow := "-->"
		if edge.kind != dataEdge {
			arrow = "-.->"
		}
		fmt.Fprintf(
			&output,
			"  %s %s|%s| %s\n",
			source,
			arrow,
			mermaidLabel(edge.label),
			target,
		)
	}
	return output.String()
}

func (r *renderer) mermaidSource(
	edge edge,
	externalIndexes map[externalNode]int,
) string {
	if index, internal := r.nodeIndexes[edge.source]; internal {
		return fmt.Sprintf("n%d", index)
	}
	return fmt.Sprintf("x%d", externalIndexes[edge.external()])
}

func mermaidLabel(label string) string {
	// Mermaid is a UTF-8 text format. Replacement is appropriate at this
	// presentation boundary: external identity remains structured, while the
	// rendered document must not carry malformed text.
	label = strings.ToValidUTF8(label, "\uFFFD")
	label = strings.ReplaceAll(label, "\r\n", "\n")
	label = strings.ReplaceAll(label, "\r", "\n")
	label = strings.ReplaceAll(label, "\u2028", "\n")
	label = strings.ReplaceAll(label, "\u2029", "\n")
	label = strings.Map(func(character rune) rune {
		if character != '\n' && unicode.IsControl(character) {
			return '\uFFFD'
		}
		return character
	}, label)
	label = html.EscapeString(label)
	label = strings.ReplaceAll(label, "|", "&#124;")
	return strings.ReplaceAll(label, "\n", "<br/>")
}
