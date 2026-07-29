package example_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// A Graph is a flat DAG. Dependencies are inferred from input ports: "twice"
// and "plusTen" share a layer, while "total" waits for both.
func Example_dag() {
	type unaryConfig struct {
		Value int `json:"value"`
	}
	unary := func(op func(int, int) int) workflow.LeafFactory {
		return workflow.Factory(func(cfg unaryConfig) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
				return op(in, cfg.Value), nil
			}), nil
		})
	}

	type pair struct {
		Left  int
		Right int
	}
	sum := workflow.BindFactory(
		func(_ struct{}, inputs workflow.Inputs) (workflow.BindFunc[pair], error) {
			left, leftOK := inputs.Ref("left")
			right, rightOK := inputs.Ref("right")
			if !leftOK || !rightOK {
				return nil, fmt.Errorf("%w: want left and right", workflow.ErrMissingPort)
			}
			return func(store workflow.Store) (pair, error) {
				a, err := workflow.Get[int](store, left)
				if err != nil {
					return pair{}, err
				}
				b, err := workflow.Get[int](store, right)
				return pair{Left: a, Right: b}, err
			}, nil
		},
		func(struct{}) (flow.Node[pair, int], error) {
			return flow.NodeFunc[pair, int](func(_ context.Context, in pair) (int, error) {
				return in.Left + in.Right, nil
			}), nil
		},
	)

	configSchema := json.RawMessage(`{
		"type":"object",
		"properties":{"value":{"type":"integer"}},
		"required":["value"],
		"additionalProperties":false
	}`)
	registry := workflow.NewRegistry().
		MustRegisterLeaf("add", unary(func(a, b int) int { return a + b })).
		MustRegisterLeaf("multiply", unary(func(a, b int) int { return a * b })).
		MustRegisterLeaf("sum", sum).
		MustRegisterSchema("add", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeNumber), Output: workflow.TypeNumber,
			ConfigSchema: configSchema,
		}).
		MustRegisterSchema("multiply", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeNumber), Output: workflow.TypeNumber,
			ConfigSchema: configSchema,
		}).
		MustRegisterSchema("sum", workflow.NodeSchema{
			Inputs: workflow.Ports{"left": workflow.TypeNumber, "right": workflow.TypeNumber},
			Output: workflow.TypeNumber,
		})

	graph := workflow.Graph{Concurrency: 2, Nodes: []workflow.NodeSpec{
		{
			ID: "twice", Type: "multiply",
			Input:  workflow.Output("start"),
			Config: json.RawMessage(`{"value":2}`),
		},
		{
			ID: "plusTen", Type: "add",
			Input:  workflow.Output("start"),
			Config: json.RawMessage(`{"value":10}`),
		},
		{
			ID: "total", Type: "sum",
			Inputs: workflow.Inputs{
				"left":  workflow.Output("twice"),
				"right": workflow.Output("plusTen"),
			},
		},
	}}

	fmt.Println("missing:", graph.MissingInputs(workflow.NewStore()))
	step, err := registry.CompileGraph(graph)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	out, err := step.Run(
		context.Background(),
		workflow.NewStore().WithOutput("start", 5),
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	total, err := workflow.Get[int](out, workflow.Output("total"))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println("total:", total)

	// Output:
	// missing: [start#/output]
	// total: 25
}
