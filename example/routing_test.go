package example_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
	"github.com/Tangerg/flow/workflow/diagram"
)

type decisionConfig struct {
	Message string `json:"message"`
}

func conditionalRegistry() *workflow.Registry {
	route := workflow.Factory(func(struct{}) (flow.Node[int, string], error) {
		return flow.NodeFunc[int, string](
			func(_ context.Context, score int) (string, error) {
				if score >= 80 {
					return "approve", nil
				}
				return "review", nil
			},
		), nil
	})
	decision := workflow.Factory(
		func(cfg decisionConfig) (flow.Node[int, string], error) {
			return flow.NodeFunc[int, string](
				func(_ context.Context, score int) (string, error) {
					return fmt.Sprintf("%s %d", cfg.Message, score), nil
				},
			), nil
		},
	)
	merge := workflow.BindFactory(
		func(_ struct{}, inputs workflow.Inputs) (workflow.Binder[string], error) {
			approve, approveOK := inputs.Ref("approve")
			review, reviewOK := inputs.Ref("review")
			if !approveOK || !reviewOK {
				return nil, fmt.Errorf("%w: want approve and review", workflow.ErrMissingPort)
			}
			return workflow.FirstOf[string](approve, review), nil
		},
		func(struct{}) (flow.Node[string, string], error) {
			return flow.NodeFunc[string, string](
				func(_ context.Context, value string) (string, error) {
					return value, nil
				},
			), nil
		},
	)
	configSchema := json.RawMessage(`{
		"type":"object",
		"properties":{"message":{"type":"string"}},
		"required":["message"],
		"additionalProperties":false
	}`)

	return workflow.NewRegistry().
		MustRegisterNode("route", route).
		MustRegisterSchema("route", workflow.NodeSchema{
			Inputs:  workflow.OnePort(workflow.TypeNumber),
			Output:  workflow.TypeString,
			Outlets: []string{"approve", "review"},
		}).
		MustRegisterNode("decision", decision).
		MustRegisterSchema("decision", workflow.NodeSchema{
			Inputs:       workflow.OnePort(workflow.TypeNumber),
			Output:       workflow.TypeString,
			ConfigSchema: configSchema,
		}).
		MustRegisterNode("merge", merge).
		MustRegisterSchema("merge", workflow.NodeSchema{
			Inputs: workflow.Ports{
				"approve": workflow.TypeString,
				"review":  workflow.TypeString,
			},
			Output: workflow.TypeString,
		})
}

func conditionalGraph() workflow.Graph {
	return workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID: "route", Type: "route",
			Inputs: workflow.OneInput(workflow.Output("score")),
		},
		{
			ID: "approve", Type: "decision",
			Inputs: workflow.OneInput(workflow.Output("score")),
			Config: json.RawMessage(`{"message":"approved"}`),
			When:   []workflow.Gate{workflow.When("route", "approve")},
		},
		{
			ID: "review", Type: "decision",
			Inputs: workflow.OneInput(workflow.Output("score")),
			Config: json.RawMessage(`{"message":"review"}`),
			When:   []workflow.Gate{workflow.When("route", "review")},
		},
		{
			ID: "result", Type: "merge",
			Inputs: workflow.Inputs{
				"approve": workflow.Output("approve"),
				"review":  workflow.Output("review"),
			},
			When: []workflow.Gate{
				workflow.When("route", "approve"),
				workflow.When("route", "review"),
			},
			Trigger: workflow.TriggerAny,
		},
	}}
}

// Graph gates model mutually exclusive arms explicitly. The selected arm runs,
// the other is bypassed, and FirstOf reads whichever arm produced an output.
func Example_conditionalGraph() {
	step, err := conditionalRegistry().CompileGraph(conditionalGraph())
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	out, err := step.Run(
		context.Background(),
		workflow.NewStore().WithOutput("score", 92),
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	result, err := workflow.Get[string](out, workflow.Output("result"))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(result)

	// Output: approved 92
}

// The optional diagram package renders a Graph without becoming part of its
// execution contract.
func Example_graphDiagram() {
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route", Inputs: workflow.OneInput(workflow.Output("score"))},
		{
			ID: "approve", Type: "decision",
			When: []workflow.Gate{workflow.When("route", "approve")},
		},
	}}
	fmt.Print(diagram.Mermaid(graph))

	// Output:
	// flowchart LR
	//   n0["route<br/>route"]
	//   n1["approve<br/>decision"]
	//   x0["score#/output"]
	//   x0 -->|in| n0
	//   n0 -.->|when:all=approve| n1
}
