package diagram_test

import (
	"strings"
	"testing"

	"github.com/Tangerg/flow/workflow"
	"github.com/Tangerg/flow/workflow/diagram"
)

func testGraph() workflow.Graph {
	return workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID:     "route",
			Type:   "switch",
			Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
		},
		{
			ID:   "approve",
			Type: "send",
			Inputs: workflow.Inputs{
				"body": workflow.Output("route").Child("body"),
			},
			DependsOn: []string{"audit"},
			When:      []workflow.Gate{workflow.When("route", "yes")},
		},
		{
			ID:      "reject",
			Type:    "stop",
			When:    []workflow.Gate{workflow.When("route", "no")},
			Trigger: workflow.TriggerAny,
		},
	}}
}

func TestASCII(t *testing.T) {
	const want = `nodes:
  "route" ["switch"]
  "approve" ["send"]
  "reject" ["stop"]
edges:
  "start#/output" --"in"--> "route"
  "route#/output/body" --"body: /output/body"--> "approve"
  "audit" --"depends"--> "approve"
  "route" --"when:all=yes"--> "approve"
  "route" --"when:any=no"--> "reject"
`
	if got := diagram.ASCII(testGraph()); got != want {
		t.Fatalf("ASCII:\n%s\nwant:\n%s", got, want)
	}
}

func TestMermaid(t *testing.T) {
	// Node identifiers repeat across the golden diagram because every edge names
	// its endpoints; they are data, not prose.
	//
	//nolint:dupword // Repeated node IDs are the expected Mermaid output.
	const want = `flowchart LR
  n0["route<br/>switch"]
  n1["approve<br/>send"]
  n2["reject<br/>stop"]
  x0["audit"]
  x1["start#/output"]
  x1 -->|in| n0
  n0 -->|body: /output/body| n1
  x0 -.->|depends| n1
  n0 -.->|when:all=yes| n1
  n0 -.->|when:any=no| n2
`
	if got := diagram.Mermaid(testGraph()); got != want {
		t.Fatalf("Mermaid:\n%s\nwant:\n%s", got, want)
	}
}

func TestEmptyAndEscapedGraphs(t *testing.T) {
	if got := diagram.ASCII(workflow.Graph{}); got != "nodes:\n  (none)\nedges:\n  (none)\n" {
		t.Fatalf("empty ASCII = %q", got)
	}
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID:   "say\n\"hello\"",
			Type: "<script>",
		},
		{
			ID:   "next",
			Type: "sink",
			When: []workflow.Gate{workflow.When("say\n\"hello\"", "a|b")},
		},
	}}
	const want = "" +
		"flowchart LR\n" +
		"  n0[\"say<br/>&#34;hello&#34;<br/>&lt;script&gt;\"]\n" +
		"  n1[\"next<br/>sink\"]\n" +
		"  n0 -.->|when:all=a&#124;b| n1\n"
	if got := diagram.Mermaid(graph); got != want {
		t.Fatalf("escaped Mermaid = %q; want %q", got, want)
	}
}

// A node ID may contain the separator a Ref renders with, so several references
// can display the same label while denoting different cells. Three of them share
// one label here, split at each separator in turn: the node ID is then the only
// thing that orders them, and reading it wrong would assign an edge to the wrong
// box rather than fail outright.
func TestMermaid_keepsExternalIdentitySeparateFromItsDisplayLabel(t *testing.T) {
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID:     "first",
			Type:   "sink",
			Inputs: workflow.OneInput(workflow.At("a", "b#", "c#", "d")),
		},
		{
			ID:     "second",
			Type:   "sink",
			Inputs: workflow.OneInput(workflow.At("a#/b", "c#", "d")),
		},
		{
			ID:     "third",
			Type:   "sink",
			Inputs: workflow.OneInput(workflow.At("a#/b#/c", "d")),
		},
	}}

	got := diagram.Mermaid(graph)
	const sharedLabel = `a#/b#/c#/d`
	if strings.Count(got, `["`+sharedLabel+`"]`) != 3 ||
		!strings.Contains(got, "  x0 -->|in: /b#/c#/d| n0\n") ||
		!strings.Contains(got, "  x1 -->|in: /c#/d| n1\n") ||
		!strings.Contains(got, "  x2 -->|in: /d| n2\n") {
		t.Fatalf("Mermaid merged structured external identities:\n%s", got)
	}
}

// TestMermaid_ordersASourceBeforeAValueRefSharingItsLabel covers the collision
// the sibling test above does not: there both externals are value refs, so the
// Ref behind each breaks the tie. Here a dependency names a node spelled like a
// Ref, so a whole external source and a path into one render the same label with
// no Ref to compare — and externals are collected from a map, so without a fixed
// order between the kinds the same graph would not always draw the same diagram.
func TestMermaid_ordersASourceBeforeAValueRefSharingItsLabel(t *testing.T) {
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID:     "reader",
			Type:   "sink",
			Inputs: workflow.OneInput(workflow.Output("seed")),
		},
		{
			ID:        "waiter",
			Type:      "sink",
			DependsOn: []string{"seed#/output"},
		},
	}}

	got := diagram.Mermaid(graph)
	if strings.Count(got, `["seed#/output"]`) != 2 ||
		!strings.Contains(got, "  x0 -.->|depends| n1\n") ||
		!strings.Contains(got, "  x1 -->|in| n0\n") {
		t.Fatalf("Mermaid did not order the source before the value ref:\n%s", got)
	}
}

// TestMermaid_ordersExternalsByLabelBeforeKind pins which of the two links comes
// first. Every other case here gives two externals the same label, where the label
// cannot decide anything; this one gives them different labels that the kinds would
// order the other way round -- a value ref sorts before a source by label and after
// it by kind.
func TestMermaid_ordersExternalsByLabelBeforeKind(t *testing.T) {
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID:     "reader",
			Type:   "sink",
			Inputs: workflow.OneInput(workflow.At("a", "value")),
		},
		{
			ID:        "waiter",
			Type:      "sink",
			DependsOn: []string{"b"},
		},
	}}

	got := diagram.Mermaid(graph)
	if !strings.Contains(got, "  x0[\"a#/value\"]\n") || !strings.Contains(got, "  x1[\"b\"]\n") {
		t.Fatalf("Mermaid ordered externals by kind before label:\n%s", got)
	}
}

func TestMermaid_keepsDeclarationTargetsSeparateFromDuplicateIDs(t *testing.T) {
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "duplicate", Type: "first"},
		{
			ID:     "duplicate",
			Type:   "second",
			Inputs: workflow.OneInput(workflow.Output("external")),
		},
	}}

	got := diagram.Mermaid(graph)
	if !strings.Contains(got, "  x0 -->|in| n1\n") {
		t.Fatalf("Mermaid redirected the second declaration's edge:\n%s", got)
	}
}

func TestMermaid_replacesInvalidUTF8OnlyInPresentation(t *testing.T) {
	invalid := string([]byte{0xff})
	graph := workflow.Graph{Nodes: []workflow.GraphNode{{ID: invalid, Type: "source"}}}
	if got, want := diagram.Mermaid(graph), "flowchart LR\n  n0[\"�<br/>source\"]\n"; got != want {
		t.Fatalf("Mermaid = %q; want %q", got, want)
	}
}

func TestMermaid_normalizesLineAndControlCharacters(t *testing.T) {
	graph := workflow.Graph{Nodes: []workflow.GraphNode{{
		ID:   "a\r\nb\rc\u2028d\u2029e\x00f",
		Type: "source",
	}}}
	want := "flowchart LR\n  n0[\"a<br/>b<br/>c<br/>d<br/>e�f<br/>source\"]\n"
	if got := diagram.Mermaid(graph); got != want {
		t.Fatalf("Mermaid = %q; want %q", got, want)
	}
}
