package example_test

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// A Subgraph gives a reusable body isolated state and identity. Registering the
// boundary as a node type lets a Graph validate its external wiring without
// exposing the body's cells.
func Example_subgraph() {
	// The body uses local seed and step IDs. It can be instantiated repeatedly
	// because Subgraph gives each instance an isolated Store and scope.
	body := workflow.LeafFunc(
		"double",
		workflow.Output("value"),
		func(_ context.Context, input int) (int, error) {
			return input * 2, nil
		},
	)

	sum := workflow.BindFactory(
		func(_ struct{}, inputs workflow.Inputs) (workflow.BindFunc[[2]int], error) {
			left, leftOK := inputs.Ref("left")
			right, rightOK := inputs.Ref("right")
			if !leftOK || !rightOK {
				return nil, fmt.Errorf(
					"%w: want left and right",
					workflow.ErrMissingPort,
				)
			}
			return func(store workflow.Store) ([2]int, error) {
				a, err := workflow.Get[int](store, left)
				if err != nil {
					return [2]int{}, err
				}
				b, err := workflow.Get[int](store, right)
				return [2]int{a, b}, err
			}, nil
		},
		func(struct{}) (flow.Node[[2]int, int], error) {
			return flow.NodeFunc[[2]int, int](
				func(_ context.Context, values [2]int) (int, error) {
					return values[0] + values[1], nil
				},
			), nil
		},
	)

	registry := workflow.NewRegistry().
		MustRegisterNode(
			"double-region",
			workflow.SubgraphFactory(body, workflow.Output("double")),
		).
		MustRegisterSchema("double-region", workflow.NodeSchema{
			Inputs: workflow.Ports{"value": workflow.TypeNumber},
			Output: workflow.TypeNumber,
		}).
		MustRegisterNode("sum", sum).
		MustRegisterSchema("sum", workflow.NodeSchema{
			Inputs: workflow.Ports{
				"left":  workflow.TypeNumber,
				"right": workflow.TypeNumber,
			},
			Output: workflow.TypeNumber,
		})

	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID: "left", Type: "double-region",
			Inputs: workflow.Inputs{"value": workflow.Output("leftInput")},
		},
		{
			ID: "right", Type: "double-region",
			Inputs: workflow.Inputs{"value": workflow.Output("rightInput")},
		},
		{
			ID: "total", Type: "sum",
			Inputs: workflow.Inputs{
				"left":  workflow.Output("left"),
				"right": workflow.Output("right"),
			},
		},
	}}

	step, err := registry.CompileGraph(graph)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	output, err := step.Run(
		context.Background(),
		workflow.NewStore().
			WithOutput("leftInput", 3).
			WithOutput("rightInput", 5),
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	total, err := workflow.Get[int](output, workflow.Output("total"))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	_, innerLeaked := output.Lookup(workflow.Output("double"))

	fmt.Println(graph.Inputs())
	fmt.Println(total, innerLeaked)

	// Output:
	// [leftInput#/output rightInput#/output]
	// 16 false
}
