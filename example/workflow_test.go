package example_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/Tangerg/flow/workflow"
)

// Workflow lifts typed Nodes into named Steps. Each Step reads a Ref from the
// immutable Store and writes its conventional output under its own ID.
func Example_workflow() {
	clean := workflow.LeafFunc(
		"clean",
		workflow.Output("input"),
		func(_ context.Context, in string) (string, error) {
			return strings.TrimSpace(in), nil
		},
	)
	greet := workflow.LeafFunc(
		"greet",
		workflow.Output("clean"),
		func(_ context.Context, name string) (string, error) {
			return "hello, " + name, nil
		},
	)

	pipeline := workflow.Sequence(clean, greet)
	out, err := pipeline.Run(
		context.Background(),
		workflow.NewStore().WithOutput("input", " Ada "),
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	message, err := workflow.Get[string](out, workflow.Output("greet"))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(message)

	// Output: hello, Ada
}
