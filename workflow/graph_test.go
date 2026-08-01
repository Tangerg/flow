package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
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
	if v, err := workflow.Get[int](out, workflow.Output("b")); err != nil || v != 11 {
		t.Fatalf("b = %v, %v; want 11", v, err) // (5+1) + 5
	}
}

func TestCompileGraph_limitsConcurrencyAcrossTheWholeGraph(t *testing.T) {
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	registry := workflow.NewRegistry().MustRegisterNode(
		"blocking",
		workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](
				func(ctx context.Context, input int) (int, error) {
					started <- struct{}{}
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
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		_, err := step.Run(
			ctx,
			workflow.NewStore().WithOutput("start", 1),
		)
		done <- err
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("two nodes did not fill the available concurrency slots")
		}
	}
	select {
	case <-started:
		t.Fatal("third node started before a concurrency slot was released")
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestCompileGraph_rejectsNegativeConcurrency(t *testing.T) {
	err := workflow.NewRegistry().ValidateGraph(workflow.Graph{Concurrency: -1})
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
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
	if value, getErr := workflow.Get[int](output, workflow.Output("seed")); getErr != nil || value != 1 {
		t.Fatalf("seed = %v, %v; want 1", value, getErr)
	}
	if description := workflow.Describe(step); description.Kind != workflow.KindGraph || len(description.Children) != 0 {
		t.Fatalf("Describe = %+v; want empty graph", description)
	}
}

func TestCompileGraph_checksContextBeforeStartingNodes(t *testing.T) {
	calls := 0
	registry := workflow.NewRegistry().MustRegisterNode("node", func(spec workflow.NodeSpec) (workflow.Step, error) {
		return workflow.Leaf(
			spec.ID,
			workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
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
			workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
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
	descendantCalls := 0
	registry := workflow.NewRegistry().
		MustRegisterNode("complete", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
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
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, struct{}](func(context.Context, struct{}) (struct{}, error) {
					return struct{}{}, boom
				}),
			), nil
		}).
		MustRegisterNode("blocking", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, struct{}](func(ctx context.Context, _ struct{}) (struct{}, error) {
					close(blockedStarted)
					<-ctx.Done()
					return struct{}{}, ctx.Err()
				}),
			), nil
		}).
		MustRegisterNode("descendant", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
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
	if value, getErr := workflow.Get[string](output, workflow.Output("complete")); getErr != nil || value != "committed" {
		t.Fatalf("completed output = %v, %v; want committed", value, getErr)
	}
	if descendantCalls != 0 {
		t.Fatalf("descendant calls = %d; want 0", descendantCalls)
	}
}

func TestCompileGraph_mergesCompletedStoresInDeclarationOrder(t *testing.T) {
	secondCompleted := make(chan struct{})
	registry := workflow.NewRegistry().
		MustRegisterNode("first", func(workflow.NodeSpec) (workflow.Step, error) {
			return flow.NodeFunc[workflow.Store, workflow.Store](
				func(_ context.Context, store workflow.Store) (workflow.Store, error) {
					<-secondCompleted
					return store.WithOutput("shared", "first"), nil
				},
			), nil
		}).
		MustRegisterNode("second", func(workflow.NodeSpec) (workflow.Step, error) {
			return flow.NodeFunc[workflow.Store, workflow.Store](
				func(_ context.Context, store workflow.Store) (workflow.Store, error) {
					close(secondCompleted)
					return store.WithOutput("shared", "second"), nil
				},
			), nil
		}).
		MustRegisterNode("reader", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.From[string](workflow.Output("shared")),
				flow.NodeFunc[string, string](func(_ context.Context, value string) (string, error) {
					return value, nil
				}),
			), nil
		})
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "first", Type: "first"},
		{ID: "second", Type: "second"},
		{ID: "reader", Type: "reader", DependsOn: []string{"second", "first"}},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	output, err := step.Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, getErr := workflow.Get[string](output, workflow.Output("shared")); getErr != nil || value != "second" {
		t.Fatalf("shared = %v, %v; want declaration-order winner second", value, getErr)
	}
	if value, getErr := workflow.Get[string](output, workflow.Output("reader")); getErr != nil || value != "second" {
		t.Fatalf("reader = %v, %v; want dependency-order winner second", value, getErr)
	}
}

func TestCompileGraph_nodesSeeOnlyDeclaredDependencies(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("constant", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, int](func(context.Context, struct{}) (int, error) {
					return 1, nil
				}),
			), nil
		}).
		MustRegisterNode("hidden-read", func(spec workflow.NodeSpec) (workflow.Step, error) {
			// This deliberately violates the factory contract by hiding a Store
			// reference instead of declaring it in spec.Inputs.
			return workflow.Leaf(
				spec.ID,
				workflow.From[int](workflow.Output("unrelated")),
				flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
					return value, nil
				}),
			), nil
		})
	step, err := registry.CompileGraph(workflow.Graph{
		Concurrency: 1,
		Nodes: []workflow.GraphNode{
			{ID: "unrelated", Type: "constant"},
			{ID: "reader", Type: "hidden-read"},
		},
	})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	output, err := step.Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("Run error = %v; want hidden read to remain unavailable", err)
	}
	if value, getErr := workflow.Get[int](output, workflow.Output("unrelated")); getErr != nil || value != 1 {
		t.Fatalf("unrelated output = %v, %v; want retained value 1", value, getErr)
	}
}

func TestCompileGraph_deduplicatesExplicitAndInferredDependency(t *testing.T) {
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
	if _, err := reg.CompileGraph(graph); err != nil {
		t.Fatalf("CompileGraph: %v", err)
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
		!errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("err = %v; want a/config with both invalid sentinels", err)
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
			if err := reg.ValidateGraph(g); !errors.Is(err, workflow.ErrInvalidGraph) {
				t.Fatalf("err = %v; want ErrInvalidGraph", err)
			}
		})
	}
}

func TestValidateGraph_rejectsMalformedConfigWithoutANodeSchema(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	g := workflow.Graph{Nodes: []workflow.GraphNode{{
		ID: "a", Type: "addN", Config: json.RawMessage(`{"n":`),
	}}}
	if err := reg.ValidateGraph(g); !errors.Is(err, workflow.ErrInvalidGraph) {
		t.Fatalf("err = %v; want ErrInvalidGraph", err)
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
			if err := reg.ValidateGraph(g); !errors.Is(err, workflow.ErrInvalidGraph) {
				t.Fatalf("err = %v; want ErrInvalidGraph", err)
			}
		})
	}
}

func TestValidateGraph_identifiesEmptyNodeIDByIndex(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())
	err := reg.ValidateGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "valid", Type: "addN"},
		{Type: "addN"},
	}})
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.As(err, &graphErr) ||
		graphErr.Field != "id" ||
		!strings.Contains(err.Error(), "index 1") {
		t.Fatalf("err = %v; want empty node ID at index 1", err)
	}
}

func TestGraph_inputs(t *testing.T) {
	g := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("seed")}},
		{ID: "b", Type: "sum", Inputs: workflow.Inputs{
			"a": workflow.Output("a"),          // internal
			"b": workflow.At("params", "rate"), // external
		}},
		{ID: "c", Type: "addN", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("seed")}}, // duplicate external
	}}

	got := g.Inputs()
	want := []workflow.Ref{workflow.At("params", "rate"), workflow.Output("seed")}
	if !slices.Equal(got, want) {
		t.Fatalf("Graph.Inputs = %v; want %v", got, want)
	}

	seeded := workflow.NewStore().WithOutput("seed", 1)
	if missing := g.MissingInputs(seeded); !slices.Equal(missing, []workflow.Ref{workflow.At("params", "rate")}) {
		t.Fatalf("Graph.MissingInputs = %v; want params.rate", missing)
	}

	complete := seeded.WithCell("params", "rate", 0.5)
	if missing := g.MissingInputs(complete); len(missing) != 0 {
		t.Fatalf("Graph.MissingInputs = %v; want none", missing)
	}
}

func TestCompileGraph_descriptionPreservesDeclarationOrder(t *testing.T) {
	constant := func(spec workflow.NodeSpec) (workflow.Step, error) {
		return workflow.Leaf(
			spec.ID,
			workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
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
	var ids []string
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
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
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
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
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
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
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
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
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
	if value, getErr := workflow.Get[int](first, workflow.Output("unrelated")); getErr != nil || value != 1 {
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
	if value, getErr := workflow.Get[string](second, workflow.Output("target")); getErr != nil || value != "ran" {
		t.Fatalf("target output = %v, %v; want ran", value, getErr)
	}
}

func TestCompileGraph_preservesWritesReturnedWithSuspension(t *testing.T) {
	registry := workflow.NewRegistry().MustRegisterNode(
		"composite",
		func(workflow.NodeSpec) (workflow.Step, error) {
			completed := workflow.Leaf(
				"completed",
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, string](func(context.Context, struct{}) (string, error) {
					return "kept", nil
				}),
			)
			return workflow.Parallel(
				[]workflow.Step{completed, workflow.Interrupt("wait", "continue?")},
				workflow.ParallelConfig{},
			), nil
		},
	)
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
		ID: "composite", Type: "composite",
	}}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	output, err := step.Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("Run error = %v; want suspension", err)
	}
	if value, getErr := workflow.Get[string](output, workflow.Output("completed")); getErr != nil || value != "kept" {
		t.Fatalf("completed output = %v, %v; want kept", value, getErr)
	}
}
