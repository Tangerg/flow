package diagram

import (
	"fmt"
	"html"
	"maps"
	"slices"
	"strconv"
	"strings"

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
	source string
	ref    workflow.Ref
	target string
	label  string
	kind   edgeKind
}

type renderer struct {
	graph       workflow.Graph
	nodeIndexes map[string]int
	edges       []edge
	externals   []string
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
	for _, node := range r.graph.Nodes {
		for _, port := range node.Inputs.PortNames() {
			r.addDataEdge(node.ID, port, node.Inputs[port])
		}
		for _, dependency := range node.DependsOn {
			r.edges = append(r.edges, edge{
				source: dependency,
				target: node.ID,
				label:  "depends",
				kind:   dependencyEdge,
			})
		}
		for _, gate := range node.When {
			label := "when:all=" + gate.Outlet
			if node.Trigger == workflow.TriggerAny {
				label = "when:any=" + gate.Outlet
			}
			r.edges = append(r.edges, edge{
				source: gate.NodeID,
				target: node.ID,
				label:  label,
				kind:   gateEdge,
			})
		}
	}
}

func (r *renderer) addDataEdge(target, port string, ref workflow.Ref) {
	label := port
	if ref != workflow.Output(ref.NodeID) {
		label += ": " + ref.Path
	}
	r.edges = append(r.edges, edge{
		source: ref.NodeID,
		ref:    ref,
		target: target,
		label:  label,
		kind:   dataEdge,
	})
}

func (r *renderer) collectExternals() {
	external := make(map[string]struct{})
	for _, edge := range r.edges {
		if _, internal := r.nodeIndexes[edge.source]; internal {
			continue
		}
		label := edge.source
		if edge.kind == dataEdge {
			label = edge.ref.String()
		}
		external[label] = struct{}{}
	}
	r.externals = slices.Sorted(maps.Keys(external))
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
			strconv.Quote(edge.target),
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

	externalIndexes := make(map[string]int, len(r.externals))
	for index, label := range r.externals {
		externalIndexes[label] = index
		fmt.Fprintf(
			&output,
			"  x%d[\"%s\"]\n",
			index,
			mermaidLabel(label),
		)
	}

	for _, edge := range r.edges {
		source := r.mermaidSource(edge, externalIndexes)
		target := fmt.Sprintf("n%d", r.nodeIndexes[edge.target])
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
	externalIndexes map[string]int,
) string {
	if index, internal := r.nodeIndexes[edge.source]; internal {
		return fmt.Sprintf("n%d", index)
	}
	label := edge.source
	if edge.kind == dataEdge {
		label = edge.ref.String()
	}
	return fmt.Sprintf("x%d", externalIndexes[label])
}

func mermaidLabel(label string) string {
	label = html.EscapeString(label)
	label = strings.ReplaceAll(label, "|", "&#124;")
	return strings.ReplaceAll(label, "\n", "<br/>")
}
