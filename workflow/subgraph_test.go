package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestSubgraph_sealsItsStoreAndProjectsOneOutput(t *testing.T) {
	body := workflow.LeafFunc(
		"double",
		workflow.Output("value"),
		func(_ context.Context, input int) (int, error) {
			return input * 2, nil
		},
	)
	step := workflow.Subgraph(workflow.SubgraphConfig{
		ID:         "calculation",
		Inputs:     workflow.Inputs{"value": workflow.Output("seed")},
		Body:       body,
		BodyOutput: workflow.Output("double"),
	})
	input := workflow.NewStore().
		WithOutput("seed", 21).
		WithOutput("value", "outer value").
		WithOutput("double", "outer double")

	output, err := step.Run(t.Context(), input)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, getErr := workflow.Get[int](output, workflow.Output("calculation")); getErr != nil || value != 42 {
		t.Fatalf("calculation output = %v, %v; want 42", value, getErr)
	}
	for _, id := range []string{"value", "double"} {
		value, ok := output.Lookup(workflow.Output(id))
		want := "outer " + id
		if !ok || value != want {
			t.Fatalf("outer %s = %v, %v; want %q", id, value, ok, want)
		}
	}
}

func TestSubgraph_reusesOneBodyUnderIndependentScopes(t *testing.T) {
	body := workflow.LeafFunc(
		"inner",
		workflow.Output("value"),
		func(_ context.Context, input int) (int, error) {
			return input + 1, nil
		},
	)
	step := workflow.Parallel([]workflow.Step{
		workflow.Subgraph(workflow.SubgraphConfig{
			ID: "left", Inputs: workflow.Inputs{"value": workflow.Output("a")},
			Body: body, BodyOutput: workflow.Output("inner"),
		}),
		workflow.Subgraph(workflow.SubgraphConfig{
			ID: "right", Inputs: workflow.Inputs{"value": workflow.Output("b")},
			Body: body, BodyOutput: workflow.Output("inner"),
		}),
	}, workflow.ParallelConfig{})

	output, err := step.Run(
		t.Context(),
		workflow.NewStore().WithOutput("a", 1).WithOutput("b", 10),
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if left, err := workflow.Get[int](output, workflow.Output("left")); err != nil || left != 2 {
		t.Fatalf("left = %v, %v; want 2", left, err)
	}
	if right, err := workflow.Get[int](output, workflow.Output("right")); err != nil || right != 11 {
		t.Fatalf("right = %v, %v; want 11", right, err)
	}
	description := workflow.Describe(workflow.Subgraph(workflow.SubgraphConfig{
		ID: "described", Body: body, BodyOutput: workflow.Output("inner"),
	}))
	if description.Kind != workflow.KindSubgraph ||
		description.ID != "described" ||
		len(description.Children) != 1 ||
		description.Children[0].ID != "inner" {
		t.Fatalf("Describe = %+v; want subgraph with inner child", description)
	}
}

func TestSubgraph_resumeReplaysInnerBodyAcrossJSONRoundTrip(t *testing.T) {
	step := workflow.Subgraph(workflow.SubgraphConfig{
		ID:         "approval",
		Body:       workflow.Interrupt("answer", map[string]any{"question": "approve?"}),
		BodyOutput: workflow.Output("answer"),
	})
	journal := workflow.NewJournal()
	paused, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("keep", "outer"),
		workflow.RunConfig{Journal: journal},
	)
	suspensions := workflow.Suspensions(err)
	if len(suspensions) != 1 ||
		suspensions[0].ID != "answer" ||
		!slices.Equal(suspensions[0].Scope, []string{"approval"}) {
		t.Fatalf("Run error = %v; want answer suspension in approval scope", err)
	}
	if _, ok := paused.Lookup(workflow.Output("approval")); ok {
		t.Fatal("suspended subgraph published a partial output")
	}

	storeJSON, err := json.Marshal(paused)
	if err != nil {
		t.Fatalf("marshal Store: %v", err)
	}
	journalJSON, err := json.Marshal(journal)
	if err != nil {
		t.Fatalf("marshal Journal: %v", err)
	}
	var restoredStore workflow.Store
	if unmarshalErr := json.Unmarshal(storeJSON, &restoredStore); unmarshalErr != nil {
		t.Fatalf("unmarshal Store: %v", unmarshalErr)
	}
	restoredJournal := workflow.NewJournal()
	if unmarshalErr := json.Unmarshal(journalJSON, restoredJournal); unmarshalErr != nil {
		t.Fatalf("unmarshal Journal: %v", unmarshalErr)
	}
	if recordErr := restoredJournal.Record(suspensions[0].Key(), "yes"); recordErr != nil {
		t.Fatalf("Record: %v", recordErr)
	}

	resumed, err := workflow.Run(
		t.Context(),
		step,
		restoredStore,
		workflow.RunConfig{Journal: restoredJournal},
	)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if value, getErr := workflow.Get[string](resumed, workflow.Output("approval")); getErr != nil || value != "yes" {
		t.Fatalf("approval = %v, %v; want yes", value, getErr)
	}
	if value, getErr := workflow.Get[string](resumed, workflow.Output("keep")); getErr != nil || value != "outer" {
		t.Fatalf("outer value = %v, %v; want outer", value, getErr)
	}
}

func TestSubgraph_validatesBoundaryBeforeRunningBody(t *testing.T) {
	bodyCalls := 0
	body := workflow.Leaf(
		"body",
		workflow.BindFunc[int](func(workflow.Store) (int, error) {
			bodyCalls++
			return 1, nil
		}),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
			return 1, nil
		}),
	)
	var nilBody flow.NodeFunc[workflow.Store, workflow.Store]
	tests := map[string]struct {
		step workflow.Step
		want error
	}{
		"empty ID": {
			step: workflow.Subgraph(workflow.SubgraphConfig{
				Body: body, BodyOutput: workflow.Output("body"),
			}),
			want: workflow.ErrInvalidStepID,
		},
		"nil body": {
			step: workflow.Subgraph(workflow.SubgraphConfig{
				ID: "sub", BodyOutput: workflow.Output("body"),
			}),
			want: workflow.ErrNilStep,
		},
		"typed nil body": {
			step: workflow.Subgraph(workflow.SubgraphConfig{
				ID: "sub", Body: nilBody, BodyOutput: workflow.Output("body"),
			}),
			want: workflow.ErrNilStep,
		},
		"invalid input": {
			step: workflow.Subgraph(workflow.SubgraphConfig{
				ID: "sub", Inputs: workflow.Inputs{"in": {}},
				Body: body, BodyOutput: workflow.Output("body"),
			}),
			want: workflow.ErrInvalidSpec,
		},
		"invalid body output": {
			step: workflow.Subgraph(workflow.SubgraphConfig{
				ID: "sub", Body: body,
			}),
			want: workflow.ErrInvalidSpec,
		},
		"missing outer input": {
			step: workflow.Subgraph(workflow.SubgraphConfig{
				ID: "sub", Inputs: workflow.Inputs{"in": workflow.Output("missing")},
				Body: body, BodyOutput: workflow.Output("body"),
			}),
			want: workflow.ErrNotFound,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := test.step.Run(t.Context(), workflow.NewStore())
			if !errors.Is(err, test.want) {
				t.Fatalf("Run error = %v; want %v", err, test.want)
			}
		})
	}
	if bodyCalls != 0 {
		t.Fatalf("body bind calls = %d; want 0", bodyCalls)
	}
}

func TestSubgraph_reportsAMissingBodyOutput(t *testing.T) {
	step := workflow.Subgraph(workflow.SubgraphConfig{
		ID: "sub",
		Body: flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				return store, nil
			},
		),
		BodyOutput: workflow.Output("missing"),
	})
	_, err := step.Run(t.Context(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.Is(err, workflow.ErrNotFound) ||
		!errors.As(err, &stepErr) ||
		stepErr.ID != "sub" ||
		stepErr.Op != workflow.OpRun {
		t.Fatalf("Run error = %v; want subgraph output StepError", err)
	}
}

func TestSubgraph_rejectsAnInvalidInnerDefinitionBeforeBinding(t *testing.T) {
	bindCalls := 0
	duplicate := func() workflow.Step {
		return workflow.Leaf(
			"duplicate",
			workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
				bindCalls++
				return struct{}{}, nil
			}),
			flow.NodeFunc[struct{}, struct{}](func(context.Context, struct{}) (struct{}, error) {
				return struct{}{}, nil
			}),
		)
	}
	step := workflow.Subgraph(workflow.SubgraphConfig{
		ID:         "sub",
		Body:       workflow.Sequence(duplicate(), duplicate()),
		BodyOutput: workflow.Output("duplicate"),
	})
	if _, err := step.Run(t.Context(), workflow.NewStore()); !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("Run error = %v; want ErrDuplicateStep", err)
	}
	if bindCalls != 0 {
		t.Fatalf("body bind calls = %d; want 0", bindCalls)
	}
}

func TestSubgraph_staticDefinitionRejectsAnOuterIDCollision(t *testing.T) {
	outer := workflow.Leaf(
		"same",
		workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
			return struct{}{}, nil
		}),
		flow.NodeFunc[struct{}, struct{}](func(context.Context, struct{}) (struct{}, error) {
			return struct{}{}, nil
		}),
	)
	subgraph := workflow.Subgraph(workflow.SubgraphConfig{
		ID: "same",
		Body: flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				return store.WithOutput("value", 1), nil
			},
		),
		BodyOutput: workflow.Output("value"),
	})
	if _, err := workflow.Sequence(outer, subgraph).Run(
		t.Context(),
		workflow.NewStore(),
	); !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("Run error = %v; want ErrDuplicateStep", err)
	}
}

func TestSubgraph_runtimeClaimCatchesOpaqueDoubleInvocation(t *testing.T) {
	subgraph := workflow.Subgraph(workflow.SubgraphConfig{
		ID:         "sub",
		Body:       workflow.Interrupt("answer", "question"),
		BodyOutput: workflow.Output("answer"),
	})
	wrapper := flow.NodeFunc[workflow.Store, workflow.Store](
		func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
			next, err := subgraph.Run(ctx, store)
			if !errors.Is(err, workflow.ErrSuspended) {
				return next, err
			}
			return subgraph.Run(ctx, next)
		},
	)
	_, err := workflow.Run(t.Context(), wrapper, workflow.NewStore(), workflow.RunConfig{})
	if !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("Run error = %v; want ErrDuplicateStep", err)
	}
}

func TestSubgraphFactory_rejectsConfigAndInvalidBoundary(t *testing.T) {
	body := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store, nil
		},
	)
	if _, err := workflow.SubgraphFactory(body, workflow.Output("value"))(
		workflow.NodeSpec{ID: "sub", Config: json.RawMessage(`{}`)},
	); !errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("config error = %v; want ErrInvalidSpec", err)
	}
	if _, err := workflow.SubgraphFactory(nil, workflow.Output("value"))(
		workflow.NodeSpec{ID: "sub"},
	); !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("nil body error = %v; want ErrNilStep", err)
	}
	var nilBody flow.NodeFunc[workflow.Store, workflow.Store]
	if _, err := workflow.SubgraphFactory(nilBody, workflow.Output("value"))(
		workflow.NodeSpec{ID: "sub"},
	); !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("typed nil body error = %v; want ErrNilStep", err)
	}
}

func TestSubgraphFactory_makesBoundaryVisibleToGraphValidation(t *testing.T) {
	body := workflow.LeafFunc(
		"inner",
		workflow.Output("value"),
		func(_ context.Context, input int) (int, error) {
			return input * 2, nil
		},
	)
	registry := workflow.NewRegistry().
		MustRegisterNode("number", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, int](func(context.Context, struct{}) (int, error) {
					return 3, nil
				}),
			), nil
		}).
		MustRegisterSchema("number", workflow.NodeSchema{Output: workflow.TypeNumber}).
		MustRegisterNode("text", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BindFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, string](func(context.Context, struct{}) (string, error) {
					return "wrong", nil
				}),
			), nil
		}).
		MustRegisterSchema("text", workflow.NodeSchema{Output: workflow.TypeString}).
		MustRegisterNode("sealed", workflow.SubgraphFactory(body, workflow.Output("inner"))).
		MustRegisterSchema("sealed", workflow.NodeSchema{
			Inputs: workflow.Ports{"value": workflow.TypeNumber},
			Output: workflow.TypeNumber,
		})

	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "source", Type: "number"},
		{ID: "sub", Type: "sealed", Inputs: workflow.Inputs{"value": workflow.Output("source")}},
	}}
	step, err := registry.CompileGraph(graph)
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	output, err := step.Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, getErr := workflow.Get[int](output, workflow.Output("sub")); getErr != nil || value != 6 {
		t.Fatalf("sub = %v, %v; want 6", value, getErr)
	}

	cycle := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "left", Type: "sealed", Inputs: workflow.Inputs{"value": workflow.Output("right")}},
		{ID: "right", Type: "sealed", Inputs: workflow.Inputs{"value": workflow.Output("left")}},
	}}
	if err := registry.ValidateGraph(cycle); !errors.Is(err, workflow.ErrCycle) {
		t.Fatalf("cycle error = %v; want ErrCycle", err)
	}

	external := workflow.Graph{Nodes: []workflow.GraphNode{{
		ID: "sub", Type: "sealed",
		Inputs: workflow.Inputs{"value": workflow.Output("request")},
	}}}
	if got := external.Inputs(); !slices.Equal(got, []workflow.Ref{workflow.Output("request")}) {
		t.Fatalf("Graph.Inputs = %v; want request output", got)
	}

	incompatible := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "source", Type: "text"},
		{ID: "sub", Type: "sealed", Inputs: workflow.Inputs{"value": workflow.Output("source")}},
	}}
	if err := registry.ValidateGraph(incompatible); !errors.Is(err, workflow.ErrIncompatibleType) {
		t.Fatalf("type error = %v; want ErrIncompatibleType", err)
	}
}

func TestCompiledGraph_withSubgraphIsSafeForConcurrentReuse(t *testing.T) {
	body := workflow.LeafFunc(
		"inner",
		workflow.Output("value"),
		func(_ context.Context, input int) (int, error) {
			return input * 2, nil
		},
	)
	registry := workflow.NewRegistry().
		MustRegisterNode("sealed", workflow.SubgraphFactory(body, workflow.Output("inner"))).
		MustRegisterSchema("sealed", workflow.NodeSchema{
			Inputs: workflow.Ports{"value": workflow.TypeNumber},
			Output: workflow.TypeNumber,
		})
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
		ID: "sub", Type: "sealed",
		Inputs: workflow.Inputs{"value": workflow.Output("seed")},
	}}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	errs := make(chan error, 32)
	var runs sync.WaitGroup
	for input := range 32 {
		runs.Go(func() {
			output, err := step.Run(
				t.Context(),
				workflow.NewStore().WithOutput("seed", input),
			)
			if err != nil {
				errs <- err
				return
			}
			value, err := workflow.Get[int](output, workflow.Output("sub"))
			if err != nil {
				errs <- err
				return
			}
			if value != input*2 {
				errs <- errors.New("concurrent run returned the wrong value")
			}
		})
	}
	runs.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestCompileSpecJSON_subgraph(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("double", workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
				return input * 2, nil
			}), nil
		}))
	data := []byte(`{
		"kind": "subgraph",
		"id": "sub",
		"inputs": {"value": {"nodeID": "seed", "path": "/output"}},
		"body": {
			"kind": "leaf",
			"id": "double",
			"type": "double",
			"inputs": {"in": {"nodeID": "value", "path": "/output"}}
		},
		"bodyOutput": {"nodeID": "double", "path": "/output"}
	}`)

	step, err := registry.CompileSpecJSON(data)
	if err != nil {
		t.Fatalf("CompileSpecJSON: %v", err)
	}
	output, err := step.Run(t.Context(), workflow.NewStore().WithOutput("seed", 4))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, getErr := workflow.Get[int](output, workflow.Output("sub")); getErr != nil || value != 8 {
		t.Fatalf("sub = %v, %v; want 8", value, getErr)
	}
}
