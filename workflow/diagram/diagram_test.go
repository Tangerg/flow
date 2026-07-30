package diagram_test

import (
	"testing"

	"github.com/Tangerg/flow/workflow"
	"github.com/Tangerg/flow/workflow/diagram"
)

func testGraph() workflow.Graph {
	return workflow.Graph{Nodes: []workflow.NodeSpec{
		{
			ID:    "route",
			Type:  "switch",
			Input: workflow.Output("start"),
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
	graph := workflow.Graph{Nodes: []workflow.NodeSpec{
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
