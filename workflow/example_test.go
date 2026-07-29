package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func ExampleLeaf() {
	double := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
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

func ExampleSequence() {
	add := func(id, input string, n int) workflow.Step {
		return workflow.Leaf(
			id,
			workflow.From[int](workflow.Output(input)),
			flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
				return value + n, nil
			}),
		)
	}

	pipeline := workflow.Sequence(
		add("load", "input", 1),
		workflow.Parallel([]workflow.Step{
			add("save", "load", 10),
			add("audit", "load", 100),
		}),
	)

	out, err := pipeline.Run(context.Background(), workflow.NewStore().WithOutput("input", 1))
	if err != nil {
		fmt.Println(err)
		return
	}
	saved, _ := workflow.Get[int](out, workflow.Output("save"))
	audited, _ := workflow.Get[int](out, workflow.Output("audit"))
	fmt.Println(saved, audited)
	// Output: 12 102
}

// This example compiles a workflow from a JSON graph and runs it. The "addN"
// node type is registered once; the graph then wires two instances of it.
func ExampleRegistry_CompileGraphJSON() {
	type config struct {
		N int `json:"n"`
	}
	addN := workflow.Factory(func(cfg config) (flow.Node[int, int], error) {
		return flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
			return x + cfg.N, nil
		}), nil
	})
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN)

	graph := `{"nodes":[
	  {"id":"a","type":"addN","input":{"nodeID":"start","path":"output"},"config":{"n":10}},
	  {"id":"b","type":"addN","input":{"nodeID":"a","path":"output"},"config":{"n":5}}
	]}`

	step, err := reg.CompileGraphJSON([]byte(graph))
	if err != nil {
		fmt.Println(err)
		return
	}

	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", 1))
	if err != nil {
		fmt.Println(err)
		return
	}

	v, _ := out.Lookup(workflow.Output("b"))
	fmt.Println(v) // 1 + 10 + 5
	// Output: 16
}

// This example wires a node's two named input ports from a JSON graph. Because
// the ports are declared rather than smuggled through the node's config, the
// graph layer infers that "sum" depends on both producers and schedules it last.
func ExampleBindFactory() {
	sum := workflow.BindFactory(
		func(_ struct{}, inputs workflow.Inputs) (workflow.BindFunc[[2]int], error) {
			left, leftOK := inputs.Ref("left")
			right, rightOK := inputs.Ref("right")
			if !leftOK || !rightOK {
				return nil, fmt.Errorf("%w: want left and right", workflow.ErrMissingPort)
			}
			return func(s workflow.Store) ([2]int, error) {
				a, err := workflow.Get[int](s, left)
				if err != nil {
					return [2]int{}, err
				}
				b, err := workflow.Get[int](s, right)
				return [2]int{a, b}, err
			}, nil
		},
		func(struct{}) (flow.Node[[2]int, int], error) {
			return flow.NodeFunc[[2]int, int](func(_ context.Context, p [2]int) (int, error) {
				return p[0] + p[1], nil
			}), nil
		},
	)
	double := workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
		return flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil }), nil
	})

	reg := workflow.NewRegistry().
		MustRegisterLeaf("sum", sum).
		MustRegisterLeaf("double", double).
		MustRegisterSchema("sum", workflow.NodeSchema{
			Inputs: workflow.Ports{"left": workflow.TypeNumber, "right": workflow.TypeNumber},
			Output: workflow.TypeNumber,
		})

	graph := `{"nodes":[
	  {"id":"twice","type":"double","input":{"nodeID":"start","path":"output"}},
	  {"id":"total","type":"sum","inputs":{
	    "left":{"nodeID":"twice","path":"output"},
	    "right":{"nodeID":"start","path":"output"}
	  }}
	]}`

	step, err := reg.CompileGraphJSON([]byte(graph))
	if err != nil {
		fmt.Println(err)
		return
	}
	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", 7))
	if err != nil {
		fmt.Println(err)
		return
	}
	total, _ := workflow.Get[int](out, workflow.Output("total"))
	fmt.Println(total) // 7*2 + 7
	// Output: 21
}

// This example reports a graph's external inputs before running it, which is how
// an editor renders a workflow's parameters and how a caller pre-flights a run.
func ExampleGraphInputs() {
	graph := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "greet", Type: "template", Inputs: workflow.Inputs{
			"name":     workflow.At("params", "name"),
			"greeting": workflow.At("params", "greeting"),
		}},
	}}

	for _, ref := range workflow.GraphInputs(graph) {
		fmt.Println("input:", ref)
	}

	store := workflow.NewStore().With("params", "name", "Ada")
	fmt.Println("missing:", workflow.MissingInputs(graph, store))

	// Output:
	// input: params.greeting
	// input: params.name
	// missing: [params.greeting]
}

// This example records a run's steps from outside the package. Because an
// Observer receives the Store a step produced, durability and tracing can be
// built on top without workflow taking on either concern.
func ExampleWithConfig() {
	step := workflow.Sequence(
		workflow.Leaf("load", workflow.From[int](workflow.Output("start")),
			flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })),
		workflow.Leaf("save", workflow.From[int](workflow.Output("load")),
			flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 10, nil })),
	)

	previous := workflow.NewStore().WithOutput("start", 1)
	journal := workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		if event.Kind != workflow.EventCompleted {
			return
		}
		for _, write := range event.Store.Changes(previous) {
			fmt.Printf("%d %s wrote %s=%v\n", event.Seq, event.ID, write.Ref(), write.Value)
		}
		previous = event.Store
	})

	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Observer: journal})
	if _, err := step.Run(ctx, previous); err != nil {
		fmt.Println(err)
		return
	}

	// Output:
	// 2 load wrote load.output=2
	// 4 save wrote save.output=20
}

// This example stops a workflow for a human decision and finishes it in what
// could be a different process. The Journal is what makes the second run skip the
// work the first already did.
func ExampleAwait() {
	type draft struct {
		Title string `json:"title"`
		Words int    `json:"words"`
	}

	write := workflow.Leaf("write", workflow.From[string](workflow.Output("topic")),
		flow.NodeFunc[string, draft](func(_ context.Context, topic string) (draft, error) {
			fmt.Println("writing the draft")
			return draft{Title: topic, Words: 800}, nil
		}))
	publish := workflow.Leaf("publish", workflow.From[draft](workflow.Output("write")),
		flow.NodeFunc[draft, string](func(_ context.Context, d draft) (string, error) {
			return fmt.Sprintf("published %q (%d words)", d.Title, d.Words), nil
		}))
	pipeline := workflow.Sequence(write, workflow.Await("review", workflow.At("editor", "verdict")), publish)

	// First run: the draft is written, then the workflow waits.
	journal := workflow.NewJournal()
	paused, err := pipeline.Run(workflow.WithConfig(context.Background(), workflow.RunConfig{Journal: journal}),
		workflow.NewStore().WithOutput("topic", "ports"))
	if !errors.Is(err, workflow.ErrSuspended) {
		fmt.Println("unexpected:", err)
		return
	}
	for _, s := range workflow.Suspensions(err) {
		fmt.Printf("waiting: %s needs %s\n", s.ID, s.Await)
	}

	// Persist both halves of the run, as a durable resume would.
	storeJSON, _ := json.Marshal(paused)
	journalJSON, _ := json.Marshal(journal)

	// Second run: reload, supply the verdict, and finish. "writing the draft" is
	// not printed again.
	var store workflow.Store
	if err := json.Unmarshal(storeJSON, &store); err != nil {
		fmt.Println(err)
		return
	}
	resumed := workflow.NewJournal()
	if err := json.Unmarshal(journalJSON, resumed); err != nil {
		fmt.Println(err)
		return
	}

	out, err := pipeline.Run(workflow.WithConfig(context.Background(), workflow.RunConfig{Journal: resumed}),
		store.With("editor", "verdict", "approved"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(workflow.Get[string](out, workflow.Output("publish")))

	// Output:
	// writing the draft
	// waiting: review needs editor.verdict
	// published "ports" (800 words) <nil>
}

// This example shows that a branch waiting on one side does not cancel the other.
func ExampleParallel_suspension() {
	report := workflow.Leaf("report", workflow.From[int](workflow.Output("start")),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
			fmt.Println("building the report")
			return x * 2, nil
		}))
	sign := workflow.Await("signoff", workflow.Output("signature"))

	journal := workflow.NewJournal()
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Journal: journal})
	both := workflow.Parallel([]workflow.Step{report, sign})

	paused, err := both.Run(ctx, workflow.NewStore().WithOutput("start", 21))
	fmt.Println("suspended:", errors.Is(err, workflow.ErrSuspended))
	// The finished branch is in the merged Store, not thrown away.
	fmt.Println(workflow.Get[int](paused, workflow.Output("report")))

	// Resuming does not rebuild the report.
	out, err := both.Run(ctx, paused.WithOutput("signature", "ok"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(workflow.Get[int](out, workflow.Output("report")))

	// Output:
	// building the report
	// suspended: true
	// 42 <nil>
	// 42 <nil>
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
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 0, boom }),
	)

	_, err := step.Run(context.Background(), workflow.NewStore())
	var stepErr *workflow.StepError
	fmt.Println(errors.As(err, &stepErr), stepErr.ID, stepErr.Op, errors.Is(err, boom))
	// Output: true charge run true
}
