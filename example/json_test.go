package example_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// The same DAG can arrive as JSON. CompileGraphJSON applies the bundled graph
// schema, duplicate-key checks, registry validation, and node config schemas
// before it builds a Step.
func Example_jsonDSL() {
	type addConfig struct {
		N int `json:"n"`
	}
	add := workflow.Factory(func(cfg addConfig) (flow.Node[int, int], error) {
		return flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
			return in + cfg.N, nil
		}), nil
	})
	registry := workflow.NewRegistry().
		MustRegisterNode("add", add).
		MustRegisterSchema("add", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeNumber),
			Output: workflow.TypeNumber,
			ConfigSchema: json.RawMessage(`{
				"type":"object",
				"properties":{"n":{"type":"integer"}},
				"required":["n"],
				"additionalProperties":false
			}`),
		})

	graph := []byte(`{
		"nodes":[
			{"id":"a","type":"add",
			 "inputs":{"in":{"nodeID":"start","path":"/output"}},
			 "config":{"n":10}},
			{"id":"b","type":"add",
			 "inputs":{"in":{"nodeID":"a","path":"/output"}},
			 "config":{"n":5}}
		]
	}`)

	// Editors can consume this Draft 2020-12 schema before sending JSON.
	fmt.Println("schema:", json.Valid(workflow.GraphJSONSchema()))
	step, err := registry.CompileGraphJSON(graph)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	out, err := step.Run(
		context.Background(),
		workflow.NewStore().WithOutput("start", 1),
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	result, err := workflow.Get[int](out, workflow.Output("b"))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println("result:", result)

	// Output:
	// schema: true
	// result: 16
}
