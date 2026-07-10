package workflow_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/workflow"
)

// This example compiles a workflow from a JSON graph and runs it. The "addN"
// node type is registered once; the graph then wires two instances of it.
func ExampleRegistry_Compile() {
	reg := workflow.NewRegistry().RegisterLeaf("addN",
		func(id string, input workflow.Ref, config json.RawMessage) (workflow.Step, error) {
			var cfg struct {
				N int `json:"n"`
			}
			_ = json.Unmarshal(config, &cfg)
			leaf := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + cfg.N, nil })
			return workflow.Adapt(id, workflow.FromRef[int](input), leaf), nil
		},
	)

	graph := `{"nodes":[
	  {"id":"a","type":"addN","input":{"nodeID":"start","path":"output"},"config":{"n":10}},
	  {"id":"b","type":"addN","input":{"nodeID":"a","path":"output"},"config":{"n":5}}
	]}`

	step, err := reg.CompileJSON([]byte(graph))
	if err != nil {
		panic(err)
	}

	out, err := step.Run(context.Background(), workflow.NewStore().With("start", "output", 1))
	if err != nil {
		panic(err)
	}

	v, _ := out.Get("b", workflow.OutputKey)
	fmt.Println(v) // 1 + 10 + 5
	// Output: 16
}
