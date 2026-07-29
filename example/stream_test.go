package example_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/Tangerg/flow/workflow"
)

// StreamLeaf keeps incremental output separate from the final value recorded
// in the Store. The Emitter is run-scoped, so the same compiled Step can be
// reused with different destinations.
func Example_streamingOutput() {
	generate := workflow.StreamLeaf(
		"generate",
		workflow.From[string](workflow.Output("name")),
		workflow.StreamNodeFunc[string, string, string](
			func(ctx context.Context, name string, yield func(string) bool) (string, error) {
				tokens := []string{"hello", ", ", name}
				var answer strings.Builder
				for _, token := range tokens {
					if !yield(token) {
						return "", context.Cause(ctx)
					}
					answer.WriteString(token)
				}
				return answer.String(), nil
			},
		),
	)

	out, err := workflow.Run(
		context.Background(),
		generate,
		workflow.NewStore().WithOutput("name", "Ada"),
		workflow.RunConfig{
			Emitter: workflow.EmitterFunc(
				func(_ context.Context, chunk workflow.Chunk) error {
					fmt.Printf("%s[%d]: %q\n", chunk.ID, chunk.Index, chunk.Value)
					return nil
				},
			),
		},
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	answer, err := workflow.Get[string](out, workflow.Output("generate"))
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println("final:", answer)

	// Output:
	// generate[0]: "hello"
	// generate[1]: ", "
	// generate[2]: "Ada"
	// final: hello, Ada
}
