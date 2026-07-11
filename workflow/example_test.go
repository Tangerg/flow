package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/workflow"
)

func ExampleLeaf() {
	double := core.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in * 2, nil
	})
	step := workflow.Leaf("double", workflow.From[int](workflow.Output("input")), double)

	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("input", 21))
	if err != nil {
		fmt.Println(err)
		return
	}
	value, err := workflow.Get[int](out, workflow.Output("double"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(value)
	// Output: 42
}

// This example compiles a workflow from a JSON graph and runs it. The "addN"
// node type is registered once; the graph then wires two instances of it.
func ExampleRegistry_CompileGraphJSON() {
	reg := workflow.NewRegistry().MustRegisterLeaf("addN",
		func(id string, input workflow.Ref, config json.RawMessage) (workflow.Step, error) {
			var cfg struct {
				N int `json:"n"`
			}
			if err := json.Unmarshal(config, &cfg); err != nil {
				return nil, err
			}
			leaf := core.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + cfg.N, nil })
			return workflow.Leaf(id, workflow.From[int](input), leaf), nil
		},
	)

	graph := `{"nodes":[
	  {"id":"a","type":"addN","input":{"nodeID":"start","path":"output"},"config":{"n":10}},
	  {"id":"b","type":"addN","input":{"nodeID":"a","path":"output"},"config":{"n":5}}
	]}`

	step, err := reg.CompileGraphJSON([]byte(graph))
	if err != nil {
		panic(err)
	}

	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", 1))
	if err != nil {
		panic(err)
	}

	v, _ := out.Lookup(workflow.Output("b"))
	fmt.Println(v) // 1 + 10 + 5
	// Output: 16
}

func ExampleStore_json() {
	store := workflow.NewStore().WithOutput("step", "ok")
	data, err := json.Marshal(store)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))

	// Output:
	// {"step":{"output":"ok"}}
}

func ExampleStepError() {
	boom := errors.New("boom")
	step := workflow.Leaf("charge",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		core.NodeFunc[int, int](func(context.Context, int) (int, error) { return 0, boom }),
	)

	_, err := step.Run(context.Background(), workflow.NewStore())
	var stepErr *workflow.StepError
	fmt.Println(errors.As(err, &stepErr), stepErr.ID, stepErr.Op, errors.Is(err, boom))
	// Output: true charge run true
}
