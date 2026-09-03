package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestCompileGraph_diamond(t *testing.T) {
	// start=0
	//   a = start + 1        (= 1)
	//   b = a + 10           (= 11)
	//   c = a + 100          (= 101)
	//   d = b + c            (= 112)   <- fan-in through two named ports
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterNode("sum", sumPorts())

	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}, Config: json.RawMessage(`{"n":1}`)},
		{ID: "b", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("a")}, Config: json.RawMessage(`{"n":10}`)},
		{ID: "c", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("a")}, Config: json.RawMessage(`{"n":100}`)},
		// No DependsOn: wired ports are dependencies.
		{ID: "d", Type: "sum", Inputs: workflow.Inputs{"a": workflow.Output("b"), "b": workflow.Output("c")}},
	}}

	step, err := reg.CompileGraph(g)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", 0))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("d")); !ok || v.(int) != 112 {
		t.Fatalf("d = %v, %v; want 112", v, ok)
	}

	// Introspection preserves the user's graph rather than exposing an internal
	// scheduling transform.
	description := workflow.Describe(step)
	if description.Kind != workflow.KindGraph || len(description.Children) != 4 {
		t.Fatalf("description = %+v; want graph with four nodes", description)
	}
	if last := description.Children[len(description.Children)-1]; last.ID != "d" {
		t.Fatalf("last node = %+v; want d", last)
	}
}

func TestCompileGraph_portsInferDependencies(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterNode("sum", sumPorts())

	// b is declared before its producer a, so passing proves execution order
	// comes from the wired ports rather than from declaration order.
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "b", Type: "sum", Inputs: workflow.Inputs{"a": workflow.Output("a"), "b": workflow.Output("start")}},
		{ID: "a", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}, Config: json.RawMessage(`{"n":1}`)},
	}}

	step, err := reg.CompileGraph(g)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", 5))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, err := out.Get[int](workflow.Output("b")); err != nil || v != 11 {
		t.Fatalf("b = %v, %v; want 11", v, err) // (5+1) + 5
	}
}

// TestCompileGraph_limitsConcurrencyAcrossTheWholeGraph pins the bound the graph
// scheduler keeps itself. Iteration and Parallel forward their limit to
// [flow.Map], whose bound TestMap_boundsConcurrency already covers, but this
// engine counts its own admitted work, so the rule is its own here.
//
// The assertion that carries the contract is a negative one -- the third node has
// not started -- and a waiting test cannot make it: a node that started may
// simply not have reached its first statement yet, so the check reports whichever
// it observes. [testing/synctest] settles it instead. Once the bubble is
// quiescent, every goroutine the scheduler admitted is durably blocked, so a node
// that is not counted is one the scheduler never admitted. The nodes park on a
// channel receive for that reason, which is durable blocking, unlike a mutex.
func TestCompileGraph_limitsConcurrencyAcrossTheWholeGraph(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var started atomic.Int32
		release := make(chan struct{})
		registry := workflow.NewRegistry().MustRegisterNode(
			"blocking",
			workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
				return flow.NodeFunc[int, int](
					func(ctx context.Context, input int) (int, error) {
						started.Add(1)
						select {
						case <-release:
							return input, nil
						case <-ctx.Done():
							return 0, ctx.Err()
						}
					},
				), nil
			}),
		)
		graph := workflow.Graph{
			Concurrency: 2,
			Nodes: []workflow.GraphNode{
				{ID: "a", Type: "blocking", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
				{ID: "b", Type: "blocking", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
				{ID: "c", Type: "blocking", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
			},
		}
		step, err := registry.CompileGraph(graph)
		if err != nil {
			t.Fatalf("CompileGraph: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			_, err := step.Run(
				t.Context(),
				workflow.NewStore().WithOutput("start", 1),
			)
			done <- err
		}()

		// Nothing can advance until this test releases the parked nodes, so the
		// count now is the whole of what the scheduler admitted.
		synctest.Wait()
		if got := started.Load(); got != 2 {
			t.Fatalf("%d nodes ran concurrently; want the declared limit of 2", got)
		}

		// One released slot admits exactly the one node that was waiting.
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
		if got := started.Load(); got != 3 {
			t.Fatalf("%d nodes ran in total; want all 3", got)
		}
	})
}

func TestCompileGraph_rejectsNegativeConcurrency(t *testing.T) {
	err := workflow.NewRegistry().ValidateGraph(workflow.Graph{Concurrency: -1})
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.Is(err, flow.ErrInvalidConfig) ||
		!errors.As(err, &graphErr) ||
		graphErr.Field != "concurrency" {
		t.Fatalf("error = %v; want concurrency GraphError", err)
	}
}

func TestCompileGraph_emptyGraphIsAnIdentity(t *testing.T) {
	step, err := workflow.NewRegistry().CompileGraph(workflow.Graph{})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	input := workflow.NewStore().WithOutput("seed", 1)
	output, err := step.Run(t.Context(), input)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, getErr := output.Get[int](workflow.Output("seed")); getErr != nil || value != 1 {
		t.Fatalf("seed = %v, %v; want 1", value, getErr)
	}
	if description := workflow.Describe(step); description.Kind != workflow.KindGraph || len(description.Children) != 0 {
		t.Fatalf("Describe = %+v; want empty graph", description)
	}
}

func TestCompileGraph_ownsTheFactoryDefinitionSnapshot(t *testing.T) {
	inputs := workflow.OneInput(workflow.Output("start"))
	config := json.RawMessage(`{"n":1}`)
	graph := workflow.Graph{Nodes: []workflow.GraphNode{{
		ID:     "node",
		Type:   "late-bound",
		Inputs: inputs,
		Config: config,
	}}}
	step, err := workflow.NewRegistry().
		MustRegisterNode("late-bound", lateBoundNodeSpecFactory()).
		CompileGraph(graph)
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	inputs[workflow.DefaultPort] = workflow.Output("changed-input")
	config[len(config)-2] = '9'
	graph.Nodes[0].ID = "changed-node"
	graph.Nodes[0].Type = "changed-type"

	out, err := step.Run(
		t.Context(),
		workflow.NewStore().WithOutput("start", 10).WithOutput("changed-input", 100),
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, getErr := out.Get[int](workflow.Output("node")); getErr != nil || value != 11 {
		t.Fatalf("node output = %d, %v; want 11, nil", value, getErr)
	}
	if _, ok := out.Lookup(workflow.Output("changed-node")); ok {
		t.Fatal("compiled Graph retained its caller's definition storage")
	}
}

func TestCompileGraph_checksContextBeforeStartingNodes(t *testing.T) {
	calls := 0
	registry := workflow.NewRegistry().MustRegisterNode("node", func(spec workflow.NodeSpec) (workflow.Step, error) {
		return workflow.Leaf(
			spec.ID,
			workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
				calls++
				return struct{}{}, nil
			}),
			flow.NodeFunc[struct{}, struct{}](func(context.Context, struct{}) (struct{}, error) {
				return struct{}{}, nil
			}),
		), nil
	})
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
		ID: "node", Type: "node",
	}}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := step.Run(ctx, workflow.NewStore()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v; want context cancellation", err)
	}
	if calls != 0 {
		t.Fatalf("bind calls = %d; want 0", calls)
	}
}

func TestCompileGraph_parentCancellationStopsRunningNodes(t *testing.T) {
	started := make(chan struct{})
	registry := workflow.NewRegistry().MustRegisterNode("blocking", func(spec workflow.NodeSpec) (workflow.Step, error) {
		return workflow.Leaf(
			spec.ID,
			workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
				return struct{}{}, nil
			}),
			flow.NodeFunc[struct{}, struct{}](func(ctx context.Context, _ struct{}) (struct{}, error) {
				close(started)
				<-ctx.Done()
				return struct{}{}, ctx.Err()
			}),
		), nil
	})
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
		ID: "blocking", Type: "blocking",
	}}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := step.Run(ctx, workflow.NewStore())
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v; want context cancellation", err)
	}
}

func TestCompileGraph_failureKeepsCompletedNodesAndCancelsSiblings(t *testing.T) {
	boom := errors.New("boom")
	blockedStarted := make(chan struct{})
	blockedCause := make(chan error, 1)
	descendantCalls := 0
	registry := workflow.NewRegistry().
		MustRegisterNode("complete", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, string](func(context.Context, struct{}) (string, error) {
					return "committed", nil
				}),
			), nil
		}).
		MustRegisterNode("fail", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, struct{}](func(context.Context, struct{}) (struct{}, error) {
					// The scheduler may cancel an admitted goroutine before it enters
					// user code. Wait until the sibling is actually running so this
					// test exercises failure propagation to running work.
					<-blockedStarted
					return struct{}{}, boom
				}),
			), nil
		}).
		MustRegisterNode("blocking", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, struct{}](func(ctx context.Context, _ struct{}) (struct{}, error) {
					close(blockedStarted)
					<-ctx.Done()
					blockedCause <- context.Cause(ctx)
					return struct{}{}, ctx.Err()
				}),
			), nil
		}).
		MustRegisterNode("descendant", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					descendantCalls++
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, struct{}](func(context.Context, struct{}) (struct{}, error) {
					return struct{}{}, nil
				}),
			), nil
		})
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "complete", Type: "complete"},
		{ID: "blocking", Type: "blocking"},
		{ID: "fail", Type: "fail", DependsOn: []string{"complete"}},
		{ID: "descendant", Type: "descendant", DependsOn: []string{"fail"}},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	output, err := step.Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v; want boom", err)
	}
	<-blockedStarted
	if cause := <-blockedCause; !errors.Is(cause, boom) {
		t.Fatalf("blocking sibling cause = %v; want failing node error", cause)
	}
	if value, getErr := output.Get[string](workflow.Output("complete")); getErr != nil || value != "committed" {
		t.Fatalf("completed output = %v, %v; want committed", value, getErr)
	}
	if descendantCalls != 0 {
		t.Fatalf("descendant calls = %d; want 0", descendantCalls)
	}
}

func TestCompileGraph_nodesSeeOnlyDeclaredDependencies(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("constant", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, int](func(context.Context, struct{}) (int, error) {
					return 1, nil
				}),
			), nil
		}).
		MustRegisterNode("copy", workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
				return value, nil
			}), nil
		})).
		MustRegisterNode("hidden-read", func(spec workflow.NodeSpec) (workflow.Step, error) {
			// This deliberately violates the factory contract by hiding a Store
			// reference instead of declaring it in spec.Inputs.
			return workflow.Leaf(
				spec.ID,
				workflow.Output("unrelated").Bind[int](),
				flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
					return value, nil
				}),
			), nil
		})
	step, err := registry.CompileGraph(workflow.Graph{
		Concurrency: 1,
		Nodes: []workflow.GraphNode{
			{ID: "unrelated", Type: "constant"},
			{
				ID:     "middle",
				Type:   "copy",
				Inputs: workflow.OneInput(workflow.Output("unrelated")),
			},
			{
				ID:        "reader",
				Type:      "hidden-read",
				DependsOn: []string{"middle"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	output, err := step.Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("Run error = %v; want hidden read to remain unavailable", err)
	}
	if value, getErr := output.Get[int](workflow.Output("unrelated")); getErr != nil || value != 1 {
		t.Fatalf("unrelated output = %v, %v; want retained value 1", value, getErr)
	}
	if value, getErr := output.Get[int](workflow.Output("middle")); getErr != nil || value != 1 {
		t.Fatalf("middle output = %v, %v; want retained value 1", value, getErr)
	}
}

func TestCompileGraph_rejectsExplicitDependencyAlreadyImpliedByInput(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{
			ID:        "b",
			Type:      "addN",
			Inputs:    workflow.Inputs{workflow.DefaultPort: workflow.Output("a")},
			DependsOn: []string{"a"},
		},
	}}
	_, err := reg.CompileGraph(graph)
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.As(err, &graphErr) || graphErr.NodeID != "b" || graphErr.Field != "dependsOn" {
		t.Fatalf("CompileGraph error = %v; want b dependsOn redundancy error", err)
	}
}

func TestCompileGraph_rejectsExplicitDependencyAlreadyImpliedByGate(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("route", routingFactory(func(int) string { return "yes" })).
		MustRegisterSchema("route", routingSchema("yes")).
		MustRegisterNode("target", addN())
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID:     "route",
			Type:   "route",
			Inputs: workflow.OneInput(workflow.Output("start")),
		},
		{
			ID:        "target",
			Type:      "target",
			Inputs:    workflow.OneInput(workflow.Output("start")),
			When:      []workflow.Gate{workflow.When("route", "yes")},
			DependsOn: []string{"route"},
		},
	}}

	_, err := registry.CompileGraph(graph)
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.As(err, &graphErr) || graphErr.NodeID != "target" || graphErr.Field != "dependsOn" {
		t.Fatalf("CompileGraph error = %v; want target dependsOn redundancy error", err)
	}
}

func TestCompileGraph_cycle(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("b")}},
		{ID: "b", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("a")}},
	}}
	if _, err := reg.CompileGraph(g); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestCompileGraph_duplicateID(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN"},
		{ID: "a", Type: "addN"},
	}}
	if _, err := reg.CompileGraph(g); err == nil {
		t.Fatal("expected duplicate ID error")
	}
}

func TestCompileGraphJSON(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	g := `{"nodes":[
	  {"id":"a","type":"addN","inputs":{"in":{"nodeID":"start","path":"/output"}},"config":{"n":2}},
	  {"id":"b","type":"addN","inputs":{"in":{"nodeID":"a","path":"/output"}},"config":{"n":3}}
	]}`

	step, err := reg.CompileGraphJSON([]byte(g))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", 0))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if v, _ := out.Lookup(workflow.Output("b")); v.(int) != 5 {
		t.Fatalf("b = %v; want 5", v)
	}
}

func TestCompileGraphJSON_rejectsUnknownAndTrailingData(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	for _, data := range []string{
		`{"nodes":[],"unknown":true}`,
		`{"nodes":[]} {"nodes":[]}`,
	} {
		if _, err := reg.CompileGraphJSON([]byte(data)); err == nil {
			t.Fatalf("CompileJSON(%q) unexpectedly succeeded", data)
		}
	}
}

func TestCompileGraph_keepsFactoryErrorsAtTheGraphBoundary(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	g := workflow.Graph{Nodes: []workflow.GraphNode{{
		ID:     "a",
		Type:   "addN",
		Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
		Config: json.RawMessage(`{"unknown":true}`),
	}}}

	_, err := reg.CompileGraph(g)
	var graphErr *workflow.GraphError
	var specErr *workflow.SpecError
	if !errors.As(err, &graphErr) || errors.As(err, &specErr) {
		t.Fatalf("err = %v; want GraphError and no SpecError", err)
	}
	if graphErr.NodeID != "a" || graphErr.Field != "config" ||
		graphErr.Path != "/nodes/0" ||
		!errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.Is(err, flow.ErrInvalidConfig) ||
		errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("err = %v; want a/config with graph and config sentinels", err)
	}
}

func TestCompileGraph_rejectsDataFromAFactoryBoundaryWithoutOutput(t *testing.T) {
	// use is built from a NodeSpec rather than by Factory, which admits only the
	// ports its config declares: the consumer below needs a second one.
	registry := workflow.NewRegistry().
		MustRegisterNode("wait", workflow.AwaitFactory()).
		MustRegisterNode("use", func(spec workflow.NodeSpec) (workflow.Step, error) {
			ref, _ := spec.Inputs.Default()
			return workflow.LeafFunc(
				spec.ID,
				ref,
				func(_ context.Context, value int) (int, error) { return value, nil },
			), nil
		})
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID:     "approval",
			Type:   "wait",
			Inputs: workflow.OneInput(workflow.Output("external")),
		},
		{
			ID:   "consumer",
			Type: "use",
			// The external port sorts first, and an external producer is not this
			// check's business: it has to pass over that port and keep going rather
			// than take it for the end of the node's inputs.
			Inputs: workflow.Inputs{
				"aux":                workflow.Output("external"),
				workflow.DefaultPort: workflow.Output("approval"),
			},
		},
	}}

	// Validation remains metadata-only and does not invoke user factories. The
	// concrete output contract becomes knowable during compilation.
	if err := registry.ValidateGraph(graph); err != nil {
		t.Fatalf("ValidateGraph without schemas: %v", err)
	}
	_, err := registry.CompileGraph(graph)
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.Is(err, workflow.ErrIncompatibleType) ||
		!errors.As(err, &graphErr) ||
		graphErr.NodeID != "consumer" || graphErr.Field != "inputs" {
		t.Fatalf("CompileGraph error = %v; want consumer inputs output-presence error", err)
	}
}

func TestCompileGraph_rejectsANilStepFromAFactory(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("broken", func(workflow.NodeSpec) (workflow.Step, error) {
		return nil, nil
	})
	_, err := reg.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{ID: "a", Type: "broken"}}})
	if !errors.Is(err, workflow.ErrNilStep) || !errors.Is(err, workflow.ErrInvalidGraph) {
		t.Fatalf("err = %v; want ErrNilStep and ErrInvalidGraph", err)
	}

	reg = workflow.NewRegistry().MustRegisterNode("broken", func(workflow.NodeSpec) (workflow.Step, error) {
		return flow.NodeFunc[workflow.Store, workflow.Store](nil), nil
	})
	_, err = reg.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{ID: "a", Type: "broken"}}})
	if !errors.Is(err, workflow.ErrNilStep) || !errors.Is(err, workflow.ErrInvalidGraph) {
		t.Fatalf("typed nil err = %v; want ErrNilStep and ErrInvalidGraph", err)
	}

	reg = workflow.NewRegistry().MustRegisterNode("broken", func(workflow.NodeSpec) (workflow.Step, error) {
		return workflow.Sequence(nil), nil
	})
	_, err = reg.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{ID: "a", Type: "broken"}}})
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrNilStep) ||
		!errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.As(err, &graphErr) ||
		graphErr.NodeID != "a" || graphErr.Field != "type" {
		t.Fatalf("nested nil err = %v; want node a ErrNilStep", err)
	}
}

func TestCompileGraph_rejectsSelfDependency(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", DependsOn: []string{"a"}},
	}}
	_, err := reg.CompileGraph(g)
	var graphErr *workflow.GraphError
	if !errors.As(err, &graphErr) || graphErr.Field != "dependsOn" {
		t.Fatalf("err = %v; want dependsOn GraphError", err)
	}
}

func TestCompileGraph_reportsSelfInputAsInputError(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	g := workflow.Graph{Nodes: []workflow.GraphNode{{
		ID:     "a",
		Type:   "addN",
		Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("a")},
	}}}
	_, err := reg.CompileGraph(g)
	var graphErr *workflow.GraphError
	if !errors.As(err, &graphErr) || graphErr.Field != "inputs" {
		t.Fatalf("err = %v; want inputs GraphError", err)
	}
}

func TestCompileGraph_rejectsUnknownExplicitDependency(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", DependsOn: []string{"typo"}},
	}}
	if _, err := reg.CompileGraph(g); err == nil {
		t.Fatal("expected unknown dependency error")
	}
}

func TestCompileGraph_programmaticValidationMatchesJSONSchema(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	tests := map[string]workflow.GraphNode{
		"empty type": {
			ID: "a",
		},
		"empty dependency": {
			ID: "a", Type: "addN", DependsOn: []string{""},
		},
		"duplicate dependency": {
			ID: "a", Type: "addN", DependsOn: []string{"parent", "parent"},
		},
		"empty port": {
			ID: "a", Type: "addN", Inputs: workflow.Inputs{"": workflow.Output("start")},
		},
	}
	for name, node := range tests {
		t.Run(name, func(t *testing.T) {
			g := workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "parent", Type: "addN"},
				node,
			}}
			err := reg.ValidateGraph(g)
			var graphErr *workflow.GraphError
			if !errors.Is(err, workflow.ErrInvalidGraph) ||
				!errors.As(err, &graphErr) || graphErr.Path != "/nodes/1" {
				t.Fatalf("err = %v; want GraphError at /nodes/1", err)
			}
		})
	}
}

func TestValidateGraph_rejectsMalformedConfigWithoutANodeSchema(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	for name, config := range map[string]json.RawMessage{
		"truncated":  json.RawMessage(`{"n":`),
		"whitespace": json.RawMessage(" \n\t"),
	} {
		t.Run(name, func(t *testing.T) {
			g := workflow.Graph{Nodes: []workflow.GraphNode{{
				ID: "a", Type: "addN", Config: config,
			}}}
			if err := reg.ValidateGraph(g); !errors.Is(err, workflow.ErrInvalidGraph) {
				t.Fatalf("err = %v; want ErrInvalidGraph", err)
			}
		})
	}
}

func TestCompileGraph_runsSchemaValidation(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterSchema("addN", workflow.NodeSchema{Inputs: workflow.OnePort(workflow.TypeNumber), Output: workflow.TypeNumber}).
		MustRegisterNode("stringNode", addN()).
		MustRegisterSchema("stringNode", workflow.NodeSchema{Inputs: workflow.OnePort(workflow.TypeString), Output: workflow.TypeString})
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{ID: "b", Type: "stringNode", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("a")}},
	}}
	if _, err := reg.CompileGraph(g); !errors.Is(err, workflow.ErrIncompatibleType) {
		t.Fatalf("err = %v; want ErrIncompatibleType", err)
	}
}

func TestCompileGraph_reportsUnwiredAndUnknownPorts(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("sum", sumPorts()).
		MustRegisterSchema("sum", workflow.NodeSchema{
			Inputs: workflow.Ports{"a": workflow.TypeNumber, "b": workflow.TypeNumber},
			Output: workflow.TypeNumber,
		})

	tests := map[string]struct {
		inputs workflow.Inputs
		want   error
	}{
		"unwired": {
			inputs: workflow.Inputs{"a": workflow.Output("start")},
			want:   workflow.ErrMissingPort,
		},
		"unknown": {
			inputs: workflow.Inputs{"a": workflow.Output("start"), "b": workflow.Output("start"), "c": workflow.Output("start")},
			want:   workflow.ErrUnknownPort,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			g := workflow.Graph{Nodes: []workflow.GraphNode{{ID: "n", Type: "sum", Inputs: tt.inputs}}}
			if err := reg.ValidateGraph(g); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v; want %v", err, tt.want)
			}
		})
	}
}

func TestCompileGraph_rejectsMalformedPortRef(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("sum", sumPorts())
	for name, ref := range map[string]workflow.Ref{
		"empty nodeID": {Path: "/output"},
		"empty path":   {NodeID: "start"},
	} {
		t.Run(name, func(t *testing.T) {
			g := workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "n", Type: "sum", Inputs: workflow.Inputs{"a": ref, "b": workflow.Output("start")}},
			}}
			err := reg.ValidateGraph(g)
			var graphErr *workflow.GraphError
			if !errors.Is(err, workflow.ErrInvalidGraph) ||
				!errors.As(err, &graphErr) || graphErr.Path != "/nodes/0" {
				t.Fatalf("err = %v; want GraphError at /nodes/0", err)
			}
		})
	}
}

func TestValidateGraph_identifiesEmptyNodeIDByPath(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	err := reg.ValidateGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "valid", Type: "addN"},
		{Type: "addN"},
	}})
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.Is(err, workflow.ErrInvalidStepID) ||
		!errors.As(err, &graphErr) ||
		graphErr.Field != "id" ||
		graphErr.Path != "/nodes/1" {
		t.Fatalf("err = %v; want empty node ID at /nodes/1", err)
	}
}

func TestValidateGraph_rejectsNonUTF8DefinitionIdentity(t *testing.T) {
	invalid := string([]byte{0xff})
	base := workflow.NewRegistry().
		MustRegisterNode("node", addN()).
		MustRegisterNode("route", routingFactory(func(int) string { return "yes" })).
		MustRegisterSchema("route", routingSchema("yes"))
	tests := map[string]struct {
		graph workflow.Graph
		field string
		want  error
	}{
		"node ID": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{{ID: invalid, Type: "node"}}},
			field: "id",
			want:  workflow.ErrInvalidStepID,
		},
		"node type": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{{ID: "node", Type: invalid}}},
			field: "type",
		},
		"input port": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{{
				ID: "node", Type: "node",
				Inputs: workflow.Inputs{invalid: workflow.Output("seed")},
			}}},
			field: "inputs",
		},
		"reference node ID": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{{
				ID: "node", Type: "node",
				Inputs: workflow.OneInput(workflow.Output(invalid)),
			}}},
			field: "inputs",
		},
		"reference path": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{{
				ID: "node", Type: "node",
				Inputs: workflow.OneInput(workflow.Ref{NodeID: "seed", Path: "/" + invalid}),
			}}},
			field: "inputs",
		},
		"dependency": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{{
				ID: "node", Type: "node", DependsOn: []string{invalid},
			}}},
			field: "dependsOn",
		},
		"gate source": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{{
				ID: "node", Type: "node", When: []workflow.Gate{workflow.When(invalid, "yes")},
			}}},
			field: "when",
		},
		"gate outlet": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "route", Type: "route"},
				{ID: "node", Type: "node", When: []workflow.Gate{workflow.When("route", invalid)}},
			}},
			field: "when",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := base.ValidateGraph(test.graph)
			var graphErr *workflow.GraphError
			if !errors.Is(err, workflow.ErrInvalidGraph) ||
				(test.want != nil && !errors.Is(err, test.want)) ||
				!errors.As(err, &graphErr) || graphErr.Field != test.field ||
				!strings.Contains(err.Error(), "not valid UTF-8") {
				t.Fatalf("ValidateGraph error = %v; want field %q UTF-8 error", err, test.field)
			}
		})
	}
}

func TestGraph_inputs(t *testing.T) {
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("seed")}},
		{ID: "b", Type: "sum", Inputs: workflow.Inputs{
			"a": workflow.Output("a"),          // internal
			"b": workflow.At("params", "rate"), // external
			// A second cell of the same external node, wired to a port whose name
			// orders the other way: the result is ordered by reference, so only
			// comparing the path after the node ID can put these two back in order.
			"c": workflow.At("params", "limit"),
		}},
		{ID: "c", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("seed")}}, // duplicate external
	}}

	got := g.Inputs()
	want := []workflow.Ref{
		workflow.At("params", "limit"),
		workflow.At("params", "rate"),
		workflow.Output("seed"),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Graph.Inputs = %v; want %v", got, want)
	}

	seeded := workflow.NewStore().WithOutput("seed", 1)
	wantMissing := []workflow.Ref{workflow.At("params", "limit"), workflow.At("params", "rate")}
	if missing := g.MissingInputs(seeded); !slices.Equal(missing, wantMissing) {
		t.Fatalf("Graph.MissingInputs = %v; want %v", missing, wantMissing)
	}

	complete := seeded.
		WithCell("params", "limit", 10).
		WithCell("params", "rate", 0.5)
	if missing := g.MissingInputs(complete); len(missing) != 0 {
		t.Fatalf("Graph.MissingInputs = %v; want none", missing)
	}
}

func TestGraph_inputsIncludePotentialReadsOfConditionalNodes(t *testing.T) {
	input := workflow.At("request", "declineReason")
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID:     "route",
			Type:   "router",
			Inputs: workflow.OneInput(workflow.Output("start")),
		},
		{
			ID:     "decline",
			Type:   "message",
			Inputs: workflow.OneInput(input),
			When:   []workflow.Gate{workflow.When("route", "declined")},
		},
	}}

	wantInputs := []workflow.Ref{input, workflow.Output("start")}
	if got := graph.Inputs(); !slices.Equal(got, wantInputs) {
		t.Fatalf("Graph.Inputs = %v; want %v", got, wantInputs)
	}
	seed := workflow.NewStore().WithOutput("start", 1)
	if got := graph.MissingInputs(seed); !slices.Equal(got, []workflow.Ref{input}) {
		t.Fatalf("Graph.MissingInputs = %v; want unresolved potential input %s", got, input)
	}

	registry := workflow.NewRegistry().
		MustRegisterNode("router", routingFactory(func(int) string { return "accepted" })).
		MustRegisterSchema("router", routingSchema("accepted", "declined")).
		MustRegisterNode("message", workflow.Factory(func(struct{}) (flow.Node[string, string], error) {
			return flow.NodeFunc[string, string](func(_ context.Context, message string) (string, error) {
				return message, nil
			}), nil
		}))
	step, err := registry.CompileGraph(graph)
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	output, err := step.Run(t.Context(), seed)
	if err != nil {
		t.Fatalf("Run without bypassed input: %v", err)
	}
	if _, ok := output.Lookup(workflow.Output("decline")); ok {
		t.Fatal("bypassed node unexpectedly produced output")
	}
}

func TestCompileGraph_descriptionPreservesDeclarationOrder(t *testing.T) {
	constant := func(spec workflow.NodeSpec) (workflow.Step, error) {
		return workflow.Leaf(
			spec.ID,
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
			flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) { return value, nil }),
		), nil
	}
	reg := workflow.NewRegistry().MustRegisterNode("constant", constant)
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "parent-a", Type: "constant"},
		{ID: "parent-b", Type: "constant"},
		{ID: "child-b", Type: "constant", DependsOn: []string{"parent-b"}},
		{ID: "child-a", Type: "constant", DependsOn: []string{"parent-a"}},
	}}

	step, err := reg.CompileGraph(g)
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	description := workflow.Describe(step)
	if description.Kind != workflow.KindGraph || len(description.Children) != 4 {
		t.Fatalf("description = %+v; want graph with four nodes", description)
	}
	ids := make([]string, 0, len(description.Children))
	for _, child := range description.Children {
		ids = append(ids, child.ID)
	}
	want := []string{"parent-a", "parent-b", "child-b", "child-a"}
	if !slices.Equal(ids, want) {
		t.Fatalf("description IDs = %v; want %v", ids, want)
	}
}

func TestCompileGraph_startsReadyDescendantWithoutWaitingForUnrelatedRoot(t *testing.T) {
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	afterCompleted := make(chan struct{})

	registry := workflow.NewRegistry().
		MustRegisterNode("slow", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, struct{}](func(ctx context.Context, _ struct{}) (struct{}, error) {
					close(slowStarted)
					select {
					case <-releaseSlow:
						return struct{}{}, nil
					case <-ctx.Done():
						return struct{}{}, ctx.Err()
					}
				}),
			), nil
		}).
		MustRegisterNode("constant", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, int](func(context.Context, struct{}) (int, error) {
					return 1, nil
				}),
			), nil
		}).
		MustRegisterNode("after", workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
				close(afterCompleted)
				return input + 1, nil
			}), nil
		}))

	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "slow", Type: "slow"},
		{ID: "fetch", Type: "constant"},
		{ID: "after-fetch", Type: "after", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("fetch")}},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := step.Run(t.Context(), workflow.NewStore())
		done <- err
	}()
	<-slowStarted
	select {
	case <-afterCompleted:
		// The dependency chain completed while the unrelated root remained
		// blocked. A layer barrier would deadlock here until releaseSlow.
	case <-time.After(time.Second):
		t.Fatal("after-fetch waited for the unrelated slow root")
	}
	close(releaseSlow)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestCompileGraph_suspensionBlocksDependentsButNotUnrelatedWork(t *testing.T) {
	journal := workflow.NewJournal()
	unrelatedCalls := 0
	targetCalls := 0
	registry := workflow.NewRegistry().
		MustRegisterNode("route", workflow.InterruptFactory()).
		MustRegisterSchema("route", workflow.NodeSchema{
			Output:  workflow.TypeString,
			Outlets: []string{"yes", "no"},
		}).
		MustRegisterNode("unrelated", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, int](func(context.Context, struct{}) (int, error) {
					unrelatedCalls++
					return unrelatedCalls, nil
				}),
			), nil
		}).
		MustRegisterNode("target", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, string](func(context.Context, struct{}) (string, error) {
					targetCalls++
					return "ran", nil
				}),
			), nil
		})

	step, err := registry.CompileGraph(workflow.Graph{
		Concurrency: 1,
		Nodes: []workflow.GraphNode{
			{ID: "route", Type: "route"},
			{ID: "unrelated", Type: "unrelated"},
			{ID: "target", Type: "target", When: []workflow.Gate{workflow.When("route", "yes")}},
		},
	})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	first, err := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{Journal: journal})
	suspensions := workflow.Suspensions(err)
	if len(suspensions) != 1 || suspensions[0].ID != "route" {
		t.Fatalf("first Run error = %v; want route suspension", err)
	}
	if targetCalls != 0 {
		t.Fatalf("target calls = %d; want 0 while route is suspended", targetCalls)
	}
	if value, getErr := first.Get[int](workflow.Output("unrelated")); getErr != nil || value != 1 {
		t.Fatalf("unrelated output = %v, %v; want 1", value, getErr)
	}

	if recordErr := journal.Record(suspensions[0].Key(), "yes"); recordErr != nil {
		t.Fatalf("Record: %v", recordErr)
	}
	second, err := workflow.Run(t.Context(), step, first, workflow.RunConfig{Journal: journal})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if unrelatedCalls != 1 {
		t.Fatalf("unrelated calls = %d; want replay without another call", unrelatedCalls)
	}
	if targetCalls != 1 {
		t.Fatalf("target calls = %d; want 1", targetCalls)
	}
	if value, getErr := second.Get[string](workflow.Output("target")); getErr != nil || value != "ran" {
		t.Fatalf("target output = %v, %v; want ran", value, getErr)
	}
}

// A DependsOn entry can be wrong in two ways, and the two read differently
// because they are different mistakes: repeating an entry says nothing new, while
// naming a node an input or gate already depends on says something the graph
// already knew. Distinguishing them relies on connect linking inputs and gates
// before DependsOn — reorder those loops and the second case silently becomes a
// deduplicated edge instead of a diagnostic.
func TestValidateGraph_distinguishesRedundantDependsOnEntries(t *testing.T) {
	route := workflow.Factory(func(struct{}) (flow.Node[int, string], error) {
		return flow.NodeFunc[int, string](
			func(context.Context, int) (string, error) { return "yes", nil }), nil
	})
	sink := workflow.Factory(func(struct{}) (flow.Node[any, any], error) {
		return flow.NodeFunc[any, any](
			func(_ context.Context, value any) (any, error) { return value, nil }), nil
	})
	registry := workflow.NewRegistry().
		MustRegisterNode("router", route).
		MustRegisterSchema("router", workflow.NodeSchema{
			Inputs:  workflow.OnePort(workflow.TypeAny),
			Output:  workflow.TypeString,
			Outlets: []string{"yes"},
		}).
		MustRegisterNode("sink", sink).
		MustRegisterSchema("sink", workflow.NodeSchema{
			Inputs: workflow.OnePort(workflow.TypeAny),
			Output: workflow.TypeAny,
		})

	tests := map[string]struct {
		node workflow.GraphNode
		want string
	}{
		"listed twice": {
			node: workflow.GraphNode{ID: "b", Type: "sink", DependsOn: []string{"a", "a"}},
			want: `dependency "a" is listed more than once`,
		},
		"implied by an input": {
			node: workflow.GraphNode{
				ID: "b", Type: "sink",
				Inputs:    workflow.OneInput(workflow.Output("a")),
				DependsOn: []string{"a"},
			},
			want: `dependency "a" is already implied by an input or gate`,
		},
		"implied by a gate": {
			node: workflow.GraphNode{
				ID: "b", Type: "sink",
				When:      []workflow.Gate{workflow.When("a", "yes")},
				DependsOn: []string{"a"},
			},
			want: `dependency "a" is already implied by an input or gate`,
		},
		"names itself": {
			node: workflow.GraphNode{ID: "b", Type: "sink", DependsOn: []string{"b"}},
			want: "node depends on itself",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			node := test.node
			if node.Inputs == nil {
				node.Inputs = workflow.OneInput(workflow.Output("seed"))
			}
			graph := workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "a", Type: "router", Inputs: workflow.OneInput(workflow.Output("seed"))},
				node,
			}}
			err := registry.ValidateGraph(graph)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateGraph = %v; want a message containing %q", err, test.want)
			}
		})
	}
}

// TestTheTwoGraphChecksRefuseTheSameDefects is the Graph counterpart of
// TestTheTwoValidatorsRefuseTheSameDefects, against the other pair of copies.
// A Graph's constraints are stated in the embedded JSON Schema — minimum
// lengths, the pointer pattern, unique dependencies and gates, a trigger that
// requires gates — and again in the Go validator, and nothing held the two to
// one verdict. Whichever way they drift, a caller who serializes a working Graph
// cannot load it back, or a document the schema accepted fails only later.
//
// Each defect is written once, as the Graph value, and asked of both routes.
// Accepting the baseline first is what keeps a refusal from being about
// something else.
func TestTheTwoGraphChecksRefuseTheSameDefects(t *testing.T) {
	// The first node routes and declares the outlet the gate cases name, so a
	// repeated or malformed gate is the only thing wrong with them: a gate
	// against a source with no declared outlets is refused for that instead, and
	// would leave the Go half of every gate case untested.
	registry := workflow.NewRegistry().
		MustRegisterNode("t", addN()).
		MustRegisterNode("route", routingFactory(func(int) string { return "x" })).
		MustRegisterSchema("route", routingSchema("x", "y"))
	wired := workflow.Inputs{workflow.DefaultPort: workflow.Output("seed")}
	source := workflow.GraphNode{ID: "a", Type: "route", Inputs: wired}
	node := func(id string) workflow.GraphNode {
		return workflow.GraphNode{ID: id, Type: "t", Inputs: wired}
	}
	// Every defect below is the second node of an otherwise valid pair, so the
	// baseline is what the corpus differs from by exactly one member.
	with := func(mutate func(*workflow.GraphNode)) workflow.Graph {
		target := node("b")
		mutate(&target)
		return workflow.Graph{Nodes: []workflow.GraphNode{source, target}}
	}
	baseline := workflow.Graph{Nodes: []workflow.GraphNode{source, node("b")}}
	assertBothGraphChecksAgree(t, registry, baseline, true)

	for name, graph := range map[string]workflow.Graph{
		"empty node ID":   with(func(n *workflow.GraphNode) { n.ID = "" }),
		"empty node type": with(func(n *workflow.GraphNode) { n.Type = "" }),
		"empty port name": with(func(n *workflow.GraphNode) {
			n.Inputs = workflow.Inputs{"": workflow.Output("seed")}
		}),
		"empty reference node": with(func(n *workflow.GraphNode) {
			n.Inputs = workflow.Inputs{workflow.DefaultPort: {Path: "/output"}}
		}),
		"unrooted reference path": with(func(n *workflow.GraphNode) {
			n.Inputs = workflow.Inputs{workflow.DefaultPort: {NodeID: "seed", Path: "output"}}
		}),
		"empty dependency": with(func(n *workflow.GraphNode) { n.DependsOn = []string{""} }),
		"repeated dependency": with(func(n *workflow.GraphNode) {
			n.DependsOn = []string{"a", "a"}
		}),
		"empty gate outlet": with(func(n *workflow.GraphNode) {
			n.When = []workflow.Gate{{NodeID: "a", Outlet: ""}}
		}),
		"empty gate node": with(func(n *workflow.GraphNode) {
			n.When = []workflow.Gate{{NodeID: "", Outlet: "x"}}
		}),
		"repeated gate": with(func(n *workflow.GraphNode) {
			n.When = []workflow.Gate{workflow.When("a", "x"), workflow.When("a", "x")}
		}),
		"trigger without gates": with(func(n *workflow.GraphNode) {
			n.Trigger = workflow.TriggerAny
		}),
		"unknown trigger": with(func(n *workflow.GraphNode) {
			n.Trigger = workflow.Trigger("some")
			n.When = []workflow.Gate{workflow.When("a", "x")}
		}),
	} {
		t.Run(name, func(t *testing.T) {
			assertBothGraphChecksAgree(t, registry, graph, false)
		})
	}
}

func assertBothGraphChecksAgree(
	t *testing.T,
	registry *workflow.Registry,
	graph workflow.Graph,
	valid bool,
) {
	t.Helper()
	data, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	goErr := registry.ValidateGraph(graph)
	schemaErr := workflow.ValidateGraphJSON(data)
	if (goErr == nil) != valid || (schemaErr == nil) != valid {
		t.Fatalf(
			"the two checks disagree on %s (want valid=%t):\n  ValidateGraph:     %v\n  ValidateGraphJSON: %v",
			data, valid, goErr, schemaErr,
		)
	}
}

// TestValidateGraph_cycleNamesTheNodesThatCannotBeOrdered pins the one graph
// diagnostic that carried no location and no names: every other defect names a
// field a caller can repair, and a cycle said only "graph cycle", leaving a
// fifty-node document to be searched by hand.
//
// The set it names is what Kahn's algorithm could not reach, which is the cycle
// plus whatever waits behind it — so the message says those nodes cannot be
// ordered rather than that they form the cycle. The trailing node below is in
// the list for that reason, and the independent one is not.
func TestValidateGraph_cycleNamesTheNodesThatCannotBeOrdered(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode("t", workflow.InterruptFactory())
	from := func(id, source string) workflow.GraphNode {
		return workflow.GraphNode{
			ID: id, Type: "t",
			Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output(source)},
		}
	}
	err := registry.ValidateGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		from("free", "seed"),
		from("a", "b"),
		from("b", "a"),
		from("after", "a"),
	}})
	if !errors.Is(err, workflow.ErrCycle) {
		t.Fatalf("ValidateGraph error = %v; want ErrCycle", err)
	}
	message := err.Error()
	for _, name := range []string{`"a"`, `"b"`, `"after"`} {
		if !strings.Contains(message, name) {
			t.Fatalf("error = %v; want it to name %s", err, name)
		}
	}
	if strings.Contains(message, `"free"`) {
		t.Fatalf("error = %v; names a node that could be ordered", err)
	}
	// A cycle spans the definition, so it keeps the whole-graph location every
	// other GraphError spends on a node and a field.
	var graphErr *workflow.GraphError
	if !errors.As(err, &graphErr) ||
		graphErr.Path != "" || graphErr.NodeID != "" || graphErr.Field != "" {
		t.Fatalf("GraphError = %+v; want the whole-graph location", graphErr)
	}
}
