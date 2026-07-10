package workflow

import (
	"fmt"
	"strings"
)

// Description is a node's self-description. Composite nodes include their
// children, so a Description forms a tree that can be walked for introspection
// or rendered for visualization.
type Description struct {
	ID       string        `json:"id,omitempty"`
	Label    string        `json:"label,omitempty"`
	Kind     string        `json:"kind"`
	Children []Description `json:"children,omitempty"`
}

// Describer is implemented by steps that can describe their own structure. Every
// step this package builds (via [Adapt], [Sequence], [Branch], [Loop],
// [Parallel], [Iteration]) implements it.
type Describer interface {
	Describe() Description
}

// Describe returns step's Description, or an opaque leaf for steps that do not
// implement [Describer] (for example a bare core.Func supplied by the caller).
func Describe(step Step) Description {
	if d, ok := step.(Describer); ok {
		return d.Describe()
	}
	return Description{Kind: "opaque"}
}

func describeAll(steps []Step) []Description {
	out := make([]Description, len(steps))
	for i, s := range steps {
		out[i] = Describe(s)
	}
	return out
}

// Mermaid renders a step's structure as a Mermaid flowchart definition, suitable
// for visualizing a compiled workflow.
func Mermaid(step Step) string {
	var b strings.Builder
	b.WriteString("flowchart TD\n")

	counter := 0
	var walk func(d Description) string
	walk = func(d Description) string {
		counter++
		id := fmt.Sprintf("n%d", counter)
		label := d.Kind
		if d.ID != "" {
			label = d.Kind + ":" + d.ID
		}
		fmt.Fprintf(&b, "  %s[%q]\n", id, label)
		for _, child := range d.Children {
			cid := walk(child)
			if child.Label != "" {
				fmt.Fprintf(&b, "  %s -->|%q| %s\n", id, child.Label, cid)
			} else {
				fmt.Fprintf(&b, "  %s --> %s\n", id, cid)
			}
		}
		return id
	}
	walk(Describe(step))
	return b.String()
}

// MermaidGraph renders the actual dependency edges of a flat Graph. Use
// Mermaid for a compiled Step's composite execution tree and MermaidGraph when
// the original DAG topology matters.
func MermaidGraph(g Graph) string {
	var b strings.Builder
	b.WriteString("flowchart TD\n")

	ids := make(map[string]string, len(g.Nodes))
	for i, node := range g.Nodes {
		mid := fmt.Sprintf("n%d", i+1)
		ids[node.ID] = mid
		label := node.Type + ":" + node.ID
		fmt.Fprintf(&b, "  %s[%q]\n", mid, label)
	}

	for _, node := range g.Nodes {
		to := ids[node.ID]
		seen := make(map[string]bool)
		writeEdge := func(dep string) {
			from, ok := ids[dep]
			if !ok || seen[dep] {
				return
			}
			seen[dep] = true
			fmt.Fprintf(&b, "  %s --> %s\n", from, to)
		}
		if node.Input != nil {
			writeEdge(node.Input.NodeID)
		}
		for _, dep := range node.DependsOn {
			writeEdge(dep)
		}
	}
	return b.String()
}
