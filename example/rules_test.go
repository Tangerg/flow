package example_test

import (
	"context"
	"fmt"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
	"github.com/Tangerg/flow/workflow/expr"
)

// Expressions keep routing policy in data while Branch keeps execution in
// ordinary Steps. Switch checks cases in order and uses its fallback otherwise.
func Example_rules() {
	resolve, err := expr.Switch(expr.SwitchSpec{
		Cases: []expr.Case{
			{When: "score.output < 60", Then: "review"},
			{When: "score.output >= 90", Then: "accept"},
		},
		Fallback: "revise",
	})
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	decision := func(id, message string) workflow.Step {
		return workflow.Leaf(
			id,
			workflow.From[int](workflow.Output("score")),
			flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) {
				return message, nil
			}),
		)
	}
	route := workflow.Branch("route", resolve, map[string]workflow.Step{
		"review": decision("review", "manual review"),
		"revise": decision("revise", "request changes"),
		"accept": decision("accept", "auto accept"),
	})

	out, err := route.Run(
		context.Background(),
		workflow.NewStore().WithOutput("score", 42),
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	result, err := workflow.Get[string](out, workflow.Output("review"))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(result)

	// Output: manual review
}
