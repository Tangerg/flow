package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func routingFactory(selectOutlet func(int) string) workflow.LeafFactory {
	return workflow.Factory(func(struct{}) (flow.Node[int, string], error) {
		return flow.NodeFunc[int, string](
			func(_ context.Context, input int) (string, error) {
				return selectOutlet(input), nil
			},
		), nil
	})
}

func routingSchema(outlets ...string) workflow.NodeSchema {
	return workflow.NodeSchema{
		Inputs:  workflow.OnePort(workflow.TypeNumber),
		Output:  workflow.TypeString,
		Outlets: outlets,
	}
}

func TestCompileGraph_routesAndRemovesStaleBranchOutputs(t *testing.T) {
	var yesCalls atomic.Int64
	var noCalls atomic.Int64
	registry := workflow.NewRegistry().
		MustRegisterLeaf("route", routingFactory(func(input int) string {
			if input >= 0 {
				return "yes"
			}
			return "no"
		})).
		MustRegisterSchema("route", routingSchema("yes", "no")).
		MustRegisterLeaf("yes", workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
				yesCalls.Add(1)
				return input + 10, nil
			}), nil
		})).
		MustRegisterLeaf("no", workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
				noCalls.Add(1)
				return input - 10, nil
			}), nil
		}))

	graph := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "route", Type: "route", Input: workflow.Output("start")},
		{
			ID: "yes", Type: "yes", Input: workflow.Output("start"),
			When: []workflow.Gate{workflow.When("route", "yes")},
		},
		{
			ID: "no", Type: "no", Input: workflow.Output("start"),
			When: []workflow.Gate{workflow.When("route", "no")},
		},
	}}
	step, compileErr := registry.CompileGraph(graph)
	if compileErr != nil {
		t.Fatalf("CompileGraph: %v", compileErr)
	}

	events := make(chan workflow.Event, 8)
	first, compileErr := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", 1),
		workflow.RunConfig{Observer: workflow.ObserverFunc(
			func(_ context.Context, event workflow.Event) {
				events <- event
			},
		)},
	)
	if compileErr != nil {
		t.Fatalf("first run: %v", compileErr)
	}
	if got, err := workflow.Get[int](first, workflow.Output("yes")); err != nil || got != 11 {
		t.Fatalf("yes output = %d, %v; want 11, nil", got, err)
	}
	if _, ok := first.Lookup(workflow.Output("no")); ok {
		t.Fatal("unselected no branch produced output")
	}
	if yesCalls.Load() != 1 || noCalls.Load() != 0 {
		t.Fatalf("calls = yes:%d no:%d; want 1,0", yesCalls.Load(), noCalls.Load())
	}
	close(events)
	if !hasEvent(events, workflow.EventBypassed, "no") {
		t.Fatal("no branch did not report EventBypassed")
	}

	// Reusing a previous result must not leak the old selected arm into a new
	// run. The compiled Graph owns and rebuilds every internal node cell.
	second, compileErr := step.Run(t.Context(), first.WithOutput("start", -1))
	if compileErr != nil {
		t.Fatalf("second run: %v", compileErr)
	}
	if _, ok := second.Lookup(workflow.Output("yes")); ok {
		t.Fatal("stale yes output survived the next graph run")
	}
	if got, err := workflow.Get[int](second, workflow.Output("no")); err != nil || got != -11 {
		t.Fatalf("no output = %d, %v; want -11, nil", got, err)
	}
	if yesCalls.Load() != 1 || noCalls.Load() != 1 {
		t.Fatalf("calls = yes:%d no:%d; want 1,1", yesCalls.Load(), noCalls.Load())
	}
}

func TestCompileGraph_triggerAnyAndFirstOfMerge(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterLeaf("route", routingFactory(func(int) string { return "left" })).
		MustRegisterSchema("route", routingSchema("left", "right")).
		MustRegisterLeaf("copy", addN()).
		MustRegisterLeaf("merge", func(spec workflow.LeafSpec) (workflow.Step, error) {
			left, _ := spec.Inputs.Ref("left")
			right, _ := spec.Inputs.Ref("right")
			return workflow.Leaf(
				spec.ID,
				workflow.FirstOf[int](left, right),
				flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
					return value, nil
				}),
			), nil
		})
	graph := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "route", Type: "route", Input: workflow.Output("start")},
		{
			ID: "left", Type: "copy", Input: workflow.Output("start"),
			When: []workflow.Gate{workflow.When("route", "left")},
		},
		{
			ID: "right", Type: "copy", Input: workflow.Output("start"),
			When: []workflow.Gate{workflow.When("route", "right")},
		},
		{
			ID: "merge", Type: "merge",
			Inputs: workflow.Inputs{
				"left":  workflow.Output("left"),
				"right": workflow.Output("right"),
			},
			When: []workflow.Gate{
				workflow.When("route", "left"),
				workflow.When("route", "right"),
			},
			Trigger: workflow.TriggerAny,
		},
	}}

	step, err := registry.CompileGraph(graph)
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	out, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", 42))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := workflow.Get[int](out, workflow.Output("merge")); err != nil || got != 42 {
		t.Fatalf("merge = %d, %v; want 42, nil", got, err)
	}
}

func TestCompileGraph_recomputesGatesAfterJournalReplay(t *testing.T) {
	var routeCalls atomic.Int64
	registry := workflow.NewRegistry().
		MustRegisterLeaf("route", routingFactory(func(int) string {
			routeCalls.Add(1)
			return "approve"
		})).
		MustRegisterSchema("route", routingSchema("approve", "reject")).
		MustRegisterLeaf("wait", workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
				return 0, workflow.Suspend(map[string]any{"input": input})
			}), nil
		})).
		MustRegisterLeaf("reject", addN())
	graph := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "route", Type: "route", Input: workflow.Output("start")},
		{
			ID: "wait", Type: "wait", Input: workflow.Output("start"),
			When: []workflow.Gate{workflow.When("route", "approve")},
		},
		{
			ID: "reject", Type: "reject", Input: workflow.Output("start"),
			When: []workflow.Gate{workflow.When("route", "reject")},
		},
	}}
	step, compileErr := registry.CompileGraph(graph)
	if compileErr != nil {
		t.Fatalf("CompileGraph: %v", compileErr)
	}
	journal := workflow.NewJournal()
	input := workflow.NewStore().WithOutput("start", 7)
	paused, compileErr := workflow.Run(
		t.Context(),
		step,
		input,
		workflow.RunConfig{Journal: journal},
	)
	if !errors.Is(compileErr, workflow.ErrSuspended) {
		t.Fatalf("first run error = %v; want ErrSuspended", compileErr)
	}
	wait := workflow.Suspensions(compileErr)[0]
	if err := journal.Record(wait.Key(), 99); err != nil {
		t.Fatalf("Record: %v", err)
	}
	out, compileErr := workflow.Run(
		t.Context(),
		step,
		paused,
		workflow.RunConfig{Journal: journal},
	)
	if compileErr != nil {
		t.Fatalf("resume: %v", compileErr)
	}
	if routeCalls.Load() != 1 {
		t.Fatalf("route calls = %d; want replay without a second call", routeCalls.Load())
	}
	if got, err := workflow.Get[int](out, workflow.Output("wait")); err != nil || got != 99 {
		t.Fatalf("wait output = %d, %v; want 99, nil", got, err)
	}
	if _, ok := out.Lookup(workflow.Output("reject")); ok {
		t.Fatal("rejected branch produced output after resume")
	}
}

func TestCompileGraph_suspendedRouterStopsBeforeGateEvaluation(t *testing.T) {
	var targetCalls atomic.Int64
	registry := workflow.NewRegistry().
		MustRegisterLeaf("route", workflow.Factory(
			func(struct{}) (flow.Node[int, string], error) {
				return flow.NodeFunc[int, string](
					func(_ context.Context, input int) (string, error) {
						return "", workflow.Suspend(map[string]any{"input": input})
					},
				), nil
			},
		)).
		MustRegisterSchema("route", routingSchema("yes")).
		MustRegisterLeaf("target", workflow.Factory(
			func(struct{}) (flow.Node[int, int], error) {
				return flow.NodeFunc[int, int](
					func(_ context.Context, input int) (int, error) {
						targetCalls.Add(1)
						return input, nil
					},
				), nil
			},
		))
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "route", Type: "route", Input: workflow.Output("start")},
		{
			ID: "target", Type: "target", Input: workflow.Output("start"),
			When: []workflow.Gate{workflow.When("route", "yes")},
		},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	events := make(chan workflow.Event, 4)
	_, err = workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", 1),
		workflow.RunConfig{Observer: workflow.ObserverFunc(
			func(_ context.Context, event workflow.Event) {
				events <- event
			},
		)},
	)
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("Run error = %v; want ErrSuspended", err)
	}
	if targetCalls.Load() != 0 {
		t.Fatalf("target calls = %d; want 0", targetCalls.Load())
	}
	close(events)
	for event := range events {
		if event.ID == "target" {
			t.Fatalf("target emitted %+v after its source suspended", event)
		}
	}
}

func TestCompileGraph_doesNotInferBypassFromMissingInput(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterLeaf("route", routingFactory(func(int) string { return "no" })).
		MustRegisterSchema("route", routingSchema("yes", "no")).
		MustRegisterLeaf("copy", addN())
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "route", Type: "route", Input: workflow.Output("start")},
		{
			ID: "selected-only", Type: "copy", Input: workflow.Output("start"),
			When: []workflow.Gate{workflow.When("route", "yes")},
		},
		{
			ID: "ungated", Type: "copy",
			Input: workflow.Output("selected-only"),
		},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	_, err = step.Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("Run error = %v; want ErrNotFound", err)
	}
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "ungated" ||
		stepErr.Op != workflow.OpBind {
		t.Fatalf("Run error = %v; want ungated bind StepError", err)
	}
}

func TestCompileGraph_propagatesBypassThroughConditionalRegions(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterLeaf("route", routingFactory(func(int) string { return "no" })).
		MustRegisterSchema("route", routingSchema("yes", "no")).
		MustRegisterLeaf("nested-route", routingFactory(func(int) string { return "next" })).
		MustRegisterSchema("nested-route", routingSchema("next")).
		MustRegisterLeaf("target", addN())
	graph := workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "route", Type: "route", Input: workflow.Output("start")},
		{
			ID: "nested", Type: "nested-route", Input: workflow.Output("start"),
			When: []workflow.Gate{workflow.When("route", "yes")},
		},
		{
			ID: "target", Type: "target", Input: workflow.Output("start"),
			When: []workflow.Gate{workflow.When("nested", "next")},
		},
	}}
	step, err := registry.CompileGraph(graph)
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	out, err := step.Run(
		t.Context(),
		workflow.NewStore().
			WithOutput("start", 1).
			WithOutput("nested", "next").
			WithOutput("target", 99),
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, ok := out.Lookup(workflow.Output("nested")); ok {
		t.Fatal("bypassed nested router retained a stale output")
	}
	if _, ok := out.Lookup(workflow.Output("target")); ok {
		t.Fatal("target downstream of a bypassed router ran")
	}
}

func TestCompileGraph_gatedStepPreservesDuplicateIDValidation(t *testing.T) {
	var calls atomic.Int64
	registry := workflow.NewRegistry().
		MustRegisterLeaf("route", func(workflow.LeafSpec) (workflow.Step, error) {
			return workflow.LeafFunc(
				"duplicate",
				workflow.Output("start"),
				func(_ context.Context, _ int) (string, error) {
					calls.Add(1)
					return "yes", nil
				},
			), nil
		}).
		MustRegisterSchema("route", routingSchema("yes")).
		MustRegisterLeaf("target", func(workflow.LeafSpec) (workflow.Step, error) {
			return workflow.LeafFunc(
				"duplicate",
				workflow.Output("start"),
				func(_ context.Context, input int) (int, error) {
					calls.Add(1)
					return input, nil
				},
			), nil
		})
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "route", Type: "route", Input: workflow.Output("start")},
		{
			ID: "target", Type: "target", Input: workflow.Output("start"),
			When: []workflow.Gate{workflow.When("route", "yes")},
		},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	_, err = step.Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("Run error = %v; want ErrDuplicateStep", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("node calls = %d; want validation before execution", calls.Load())
	}
}

func TestCompileGraph_rejectsRuntimeRoutingContractViolations(t *testing.T) {
	tests := map[string]workflow.LeafFactory{
		"unknown outlet": routingFactory(func(int) string { return "other" }),
		"wrong output type": func(spec workflow.LeafSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.From[int](spec.Inputs[workflow.DefaultPort]),
				flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
					return input, nil
				}),
			), nil
		},
		"missing output": func(workflow.LeafSpec) (workflow.Step, error) {
			return flow.NodeFunc[workflow.Store, workflow.Store](
				func(_ context.Context, store workflow.Store) (workflow.Store, error) {
					return store, nil
				},
			), nil
		},
	}
	for name, factory := range tests {
		t.Run(name, func(t *testing.T) {
			registry := workflow.NewRegistry().
				MustRegisterLeaf("route", factory).
				MustRegisterSchema("route", routingSchema("yes")).
				MustRegisterLeaf("target", addN())
			graph := workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "route", Type: "route", Input: workflow.Output("start")},
				{
					ID: "target", Type: "target", Input: workflow.Output("start"),
					When: []workflow.Gate{workflow.When("route", "yes")},
				},
			}}
			step, err := registry.CompileGraph(graph)
			if err != nil {
				t.Fatalf("CompileGraph: %v", err)
			}
			events := make(chan workflow.Event, 4)
			_, err = workflow.Run(
				t.Context(),
				step,
				workflow.NewStore().WithOutput("start", 1),
				workflow.RunConfig{Observer: workflow.ObserverFunc(
					func(_ context.Context, event workflow.Event) {
						events <- event
					},
				)},
			)
			switch name {
			case "unknown outlet":
				if !errors.Is(err, workflow.ErrUnknownOutlet) {
					t.Fatalf("error = %v; want ErrUnknownOutlet", err)
				}
			default:
				if !errors.Is(err, workflow.ErrTypeMismatch) &&
					!errors.Is(err, workflow.ErrNotFound) {
					t.Fatalf("error = %v; want typed routing output error", err)
				}
			}
			close(events)
			if !hasEvent(events, workflow.EventFailed, "target") {
				t.Fatal("gate failure did not report EventFailed")
			}
		})
	}
}

func TestCompileGraph_validatesEveryGateBeforeApplyingTrigger(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterLeaf("good-route", routingFactory(func(int) string { return "yes" })).
		MustRegisterSchema("good-route", routingSchema("yes")).
		MustRegisterLeaf("bad-route", routingFactory(func(int) string { return "undeclared" })).
		MustRegisterSchema("bad-route", routingSchema("no")).
		MustRegisterLeaf("target", addN())
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.NodeSpec{
		{ID: "good", Type: "good-route", Input: workflow.Output("start")},
		{ID: "bad", Type: "bad-route", Input: workflow.Output("start")},
		{
			ID: "target", Type: "target", Input: workflow.Output("start"),
			When: []workflow.Gate{
				workflow.When("good", "yes"),
				workflow.When("bad", "no"),
			},
			Trigger: workflow.TriggerAny,
		},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	_, err = step.Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if !errors.Is(err, workflow.ErrUnknownOutlet) {
		t.Fatalf("Run error = %v; want ErrUnknownOutlet", err)
	}
}

func TestValidateGraph_rejectsInvalidGates(t *testing.T) {
	baseRegistry := func() *workflow.Registry {
		return workflow.NewRegistry().
			MustRegisterLeaf("route", routingFactory(func(int) string { return "yes" })).
			MustRegisterSchema("route", workflow.NodeSchema{
				Output:  workflow.TypeString,
				Outlets: []string{"yes", "no"},
			}).
			MustRegisterLeaf("target", addN())
	}
	tests := map[string]struct {
		graph   workflow.Graph
		want    error
		field   string
		prepare func(*workflow.Registry)
	}{
		"unknown trigger": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "a", Type: "target", Trigger: "sometimes"},
			}},
			want: workflow.ErrInvalidGraph, field: "trigger",
		},
		"any without gates": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "a", Type: "target", Trigger: workflow.TriggerAny},
			}},
			want: workflow.ErrInvalidGraph, field: "trigger",
		},
		"empty source": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "a", Type: "target", When: []workflow.Gate{{Outlet: "yes"}}},
			}},
			want: workflow.ErrInvalidGraph, field: "when",
		},
		"empty outlet": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "a", Type: "target", When: []workflow.Gate{{NodeID: "route"}}},
			}},
			want: workflow.ErrInvalidGraph, field: "when",
		},
		"duplicate gate": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "route", Type: "route"},
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{
						workflow.When("route", "yes"),
						workflow.When("route", "yes"),
					},
					Trigger: workflow.TriggerAny,
				},
			}},
			want: workflow.ErrInvalidGraph, field: "when",
		},
		"contradictory all": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "route", Type: "route"},
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{
						workflow.When("route", "yes"),
						workflow.When("route", "no"),
					},
				},
			}},
			want: workflow.ErrInvalidGraph, field: "when",
		},
		"unknown source": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("missing", "yes")},
				},
			}},
			want: workflow.ErrUnknownNode, field: "when",
		},
		"self source": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("a", "yes")},
				},
			}},
			want: workflow.ErrCycle, field: "when",
		},
		"conditional cycle": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "route", Type: "route", Input: workflow.Output("a")},
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("route", "yes")},
				},
			}},
			want: workflow.ErrCycle,
		},
		"unknown outlet": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "route", Type: "route"},
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("route", "other")},
				},
			}},
			want: workflow.ErrUnknownOutlet, field: "when",
		},
		"source without schema": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "route", Type: "plain"},
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("route", "yes")},
				},
			}},
			want: workflow.ErrInvalidGraph, field: "when",
			prepare: func(registry *workflow.Registry) {
				registry.MustRegisterLeaf("plain", addN())
			},
		},
		"source without outlets": {
			graph: workflow.Graph{Nodes: []workflow.NodeSpec{
				{ID: "route", Type: "plain"},
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("route", "yes")},
				},
			}},
			want: workflow.ErrInvalidGraph, field: "when",
			prepare: func(registry *workflow.Registry) {
				registry.
					MustRegisterLeaf("plain", addN()).
					MustRegisterSchema("plain", workflow.NodeSchema{Output: workflow.TypeNumber})
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			registry := baseRegistry()
			if test.prepare != nil {
				test.prepare(registry)
			}
			err := registry.ValidateGraph(test.graph)
			var graphErr *workflow.GraphError
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v; want %v", err, test.want)
			}
			if test.field != "" &&
				(!errors.As(err, &graphErr) || graphErr.Field != test.field) {
				t.Fatalf("error = %v; want GraphError field %q", err, test.field)
			}
		})
	}
}

func TestRegisterSchema_rejectsInvalidOutlets(t *testing.T) {
	tests := map[string]workflow.NodeSchema{
		"non-string output": {Output: workflow.TypeNumber, Outlets: []string{"yes"}},
		"empty outlet":      {Output: workflow.TypeString, Outlets: []string{""}},
		"duplicate outlet":  {Output: workflow.TypeString, Outlets: []string{"yes", "yes"}},
	}
	for name, schema := range tests {
		t.Run(name, func(t *testing.T) {
			err := workflow.NewRegistry().RegisterSchema("route", schema)
			if !errors.Is(err, workflow.ErrInvalidRegistration) {
				t.Fatalf("error = %v; want ErrInvalidRegistration", err)
			}
		})
	}
}

func TestNodeSchema_clonesOutlets(t *testing.T) {
	outlets := []string{"yes", "no"}
	registry := workflow.NewRegistry().
		MustRegisterSchema("route", routingSchema(outlets...))
	outlets[0] = "changed"
	schema, ok := registry.NodeSchema("route")
	if !ok || schema.Outlets[0] != "yes" {
		t.Fatalf("registered outlets = %v; want [yes no]", schema.Outlets)
	}
	schema.Outlets[0] = "mutated"
	again, _ := registry.NodeSchema("route")
	if again.Outlets[0] != "yes" {
		t.Fatalf("registry outlets changed through returned schema: %v", again.Outlets)
	}
}

func TestGraphConditionalJSON(t *testing.T) {
	data := []byte(`{
	  "nodes": [
	    {"id":"route","type":"route","input":{"nodeID":"start","path":"/output"}},
	    {
	      "id":"target",
	      "type":"target",
	      "input":{"nodeID":"start","path":"/output"},
	      "when":[{"nodeID":"route","outlet":"yes"}],
	      "trigger":"any"
	    }
	  ]
	}`)
	registry := workflow.NewRegistry().
		MustRegisterLeaf("route", routingFactory(func(int) string { return "yes" })).
		MustRegisterSchema("route", routingSchema("yes")).
		MustRegisterLeaf("target", addN())
	step, err := registry.CompileGraphJSON(data)
	if err != nil {
		t.Fatalf("CompileGraphJSON: %v", err)
	}
	out, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, ok := out.Lookup(workflow.Output("target")); !ok {
		t.Fatal("target output missing")
	}

	var graph workflow.Graph
	if err := json.Unmarshal(data, &graph); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if graph.Nodes[1].Trigger != workflow.TriggerAny ||
		graph.Nodes[1].When[0] != workflow.When("route", "yes") {
		t.Fatalf("decoded graph = %+v", graph)
	}
}

func hasEvent(events <-chan workflow.Event, kind workflow.EventKind, id string) bool {
	for event := range events {
		if event.Kind == kind && event.ID == id {
			return true
		}
	}
	return false
}
