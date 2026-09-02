package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func routingFactory(selectOutlet func(int) string) workflow.NodeFactory {
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

type singleEncodingRoute struct {
	calls *atomic.Int64
}

func (s singleEncodingRoute) MarshalJSON() ([]byte, error) {
	if call := s.calls.Add(1); call != 1 {
		return nil, errors.New("routing output encoded more than once")
	}
	return []byte(`"yes"`), nil
}

func TestCompileGraph_routesAndRemovesStaleBranchOutputs(t *testing.T) {
	var yesCalls atomic.Int64
	var noCalls atomic.Int64
	registry := workflow.NewRegistry().
		MustRegisterNode("route", routingFactory(func(input int) string {
			if input >= 0 {
				return "yes"
			}
			return "no"
		})).
		MustRegisterSchema("route", routingSchema("yes", "no")).
		MustRegisterNode("yes", workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
				yesCalls.Add(1)
				return input + 10, nil
			}), nil
		})).
		MustRegisterNode("no", workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
				noCalls.Add(1)
				return input - 10, nil
			}), nil
		}))

	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{
			ID: "yes", Type: "yes", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
			When: []workflow.Gate{workflow.When("route", "yes")},
		},
		{
			ID: "no", Type: "no", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
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
	if got, err := first.Get[int](workflow.Output("yes")); err != nil || got != 11 {
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
	if got, err := second.Get[int](workflow.Output("no")); err != nil || got != -11 {
		t.Fatalf("no output = %d, %v; want -11, nil", got, err)
	}
	if yesCalls.Load() != 1 || noCalls.Load() != 1 {
		t.Fatalf("calls = yes:%d no:%d; want 1,1", yesCalls.Load(), noCalls.Load())
	}
}

func TestCompileGraph_routesOnPersistentJSONString(t *testing.T) {
	var routeCalls atomic.Int64
	var targetCalls atomic.Int64
	registry := workflow.NewRegistry().
		MustRegisterNode("route", routingFactory(func(int) string {
			routeCalls.Add(1)
			return string([]byte{0xff, 0xfe})
		})).
		MustRegisterSchema("route", routingSchema("��")).
		MustRegisterNode("target", workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
				targetCalls.Add(1)
				return input + 1, nil
			}), nil
		}))
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID: "route", Type: "route",
			Inputs: workflow.OneInput(workflow.Output("start")),
		},
		{
			ID: "target", Type: "target",
			Inputs: workflow.OneInput(workflow.Output("start")),
			When:   []workflow.Gate{workflow.When("route", "��")},
		},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	journal := workflow.NewJournal()
	first, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", 1),
		workflow.RunConfig{Journal: journal},
	)
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if value, getErr := first.Get[int](workflow.Output("target")); getErr != nil || value != 2 {
		t.Fatalf("first target output = %d, %v; want 2, nil", value, getErr)
	}

	checkpoint, err := json.Marshal(journal)
	if err != nil {
		t.Fatalf("Marshal Journal: %v", err)
	}
	restored := workflow.NewJournal()
	if unmarshalErr := json.Unmarshal(checkpoint, restored); unmarshalErr != nil {
		t.Fatalf("Unmarshal Journal: %v", unmarshalErr)
	}
	second, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", 1),
		workflow.RunConfig{Journal: restored},
	)
	if err != nil {
		t.Fatalf("resumed Run: %v", err)
	}
	if value, getErr := second.Get[int](workflow.Output("target")); getErr != nil || value != 2 {
		t.Fatalf("resumed target output = %d, %v; want 2, nil", value, getErr)
	}
	if routeCalls.Load() != 1 || targetCalls.Load() != 1 {
		t.Fatalf(
			"calls = route:%d target:%d; want 1,1 after replay",
			routeCalls.Load(),
			targetCalls.Load(),
		)
	}
}

func TestCompileGraph_rejectsOpaqueFactoryStepBeforeGating(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("route", routingFactory(func(int) string { return "run" })).
		MustRegisterSchema("route", routingSchema("run")).
		MustRegisterNode("opaque", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return flow.NodeFunc[workflow.Store, workflow.Store](
				func(_ context.Context, store workflow.Store) (workflow.Store, error) {
					return store.WithOutput(spec.ID, 42), nil
				},
			), nil
		})
	_, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID:     "route",
			Type:   "route",
			Inputs: workflow.OneInput(workflow.Output("start")),
		},
		{
			ID:   "target",
			Type: "opaque",
			When: []workflow.Gate{workflow.When("route", "run")},
		},
	}})
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.As(err, &graphErr) || graphErr.NodeID != "target" ||
		!strings.Contains(err.Error(), "opaque Step") {
		t.Fatalf("CompileGraph error = %v; want target opaque-boundary error", err)
	}
}

func TestCompileGraph_triggerAnyAndFirstOfMerge(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("route", routingFactory(func(int) string { return "left" })).
		MustRegisterSchema("route", routingSchema("left", "right")).
		MustRegisterNode("copy", addN()).
		MustRegisterNode("merge", func(spec workflow.NodeSpec) (workflow.Step, error) {
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
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{
			ID: "left", Type: "copy", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
			When: []workflow.Gate{workflow.When("route", "left")},
		},
		{
			ID: "right", Type: "copy", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
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
	if got, err := out.Get[int](workflow.Output("merge")); err != nil || got != 42 {
		t.Fatalf("merge = %d, %v; want 42, nil", got, err)
	}
}

// Any means at least one, so it has to be able to say no. Every other trigger-any
// test gives it a gate that matches, which leaves the count it compares against
// unverified: a bound one step looser admits a node whose sources all chose
// something else.
func TestCompileGraph_triggerAnyBypassesWhenNoGateMatches(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("route", routingFactory(func(int) string { return "left" })).
		MustRegisterSchema("route", routingSchema("left", "right", "other")).
		MustRegisterNode("copy", addN())
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{
			ID: "target", Type: "copy",
			Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
			When: []workflow.Gate{
				workflow.When("route", "right"),
				workflow.When("route", "other"),
			},
			Trigger: workflow.TriggerAny,
		},
	}}

	step, err := registry.CompileGraph(graph)
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	out, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, ok := out.Lookup(workflow.Output("target")); ok {
		t.Fatalf("target = %v; want a bypassed node to produce nothing", value)
	}
}

func TestCompileGraph_triggerAnyReadsEachRoutingSourceOnce(t *testing.T) {
	var routeEncodes atomic.Int64
	registry := workflow.NewRegistry().
		MustRegisterNode("route", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, singleEncodingRoute](
					func(context.Context, struct{}) (singleEncodingRoute, error) {
						return singleEncodingRoute{calls: &routeEncodes}, nil
					},
				),
			), nil
		}).
		MustRegisterSchema("route", workflow.NodeSchema{
			Output:  workflow.TypeString,
			Outlets: []string{"yes", "no"},
		}).
		MustRegisterNode("target", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, int](func(context.Context, struct{}) (int, error) {
					return 1, nil
				}),
			), nil
		})
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route"},
		{
			ID:   "target",
			Type: "target",
			When: []workflow.Gate{
				workflow.When("route", "no"),
				workflow.When("route", "yes"),
			},
			Trigger: workflow.TriggerAny,
		},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	output, err := step.Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, getErr := output.Get[int](workflow.Output("target")); getErr != nil || value != 1 {
		t.Fatalf("target output = %d, %v; want 1, nil", value, getErr)
	}
	if encodes := routeEncodes.Load(); encodes != 1 {
		t.Fatalf("routing output encodes = %d; want 1", encodes)
	}
}

func TestCompileGraph_recomputesGatesAfterJournalReplay(t *testing.T) {
	var routeCalls atomic.Int64
	registry := workflow.NewRegistry().
		MustRegisterNode("route", routingFactory(func(int) string {
			routeCalls.Add(1)
			return "approve"
		})).
		MustRegisterSchema("route", routingSchema("approve", "reject")).
		MustRegisterNode("wait", workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
				return 0, workflow.Suspend(map[string]any{"input": input})
			}), nil
		})).
		MustRegisterNode("reject", addN())
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{
			ID: "wait", Type: "wait", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
			When: []workflow.Gate{workflow.When("route", "approve")},
		},
		{
			ID: "reject", Type: "reject", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
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
	if got, err := out.Get[int](workflow.Output("wait")); err != nil || got != 99 {
		t.Fatalf("wait output = %d, %v; want 99, nil", got, err)
	}
	if _, ok := out.Lookup(workflow.Output("reject")); ok {
		t.Fatal("rejected branch produced output after resume")
	}
}

func TestCompileGraph_suspendedRouterStopsBeforeGateEvaluation(t *testing.T) {
	var targetCalls atomic.Int64
	registry := workflow.NewRegistry().
		MustRegisterNode("route", workflow.Factory(
			func(struct{}) (flow.Node[int, string], error) {
				return flow.NodeFunc[int, string](
					func(_ context.Context, input int) (string, error) {
						return "", workflow.Suspend(map[string]any{"input": input})
					},
				), nil
			},
		)).
		MustRegisterSchema("route", routingSchema("yes")).
		MustRegisterNode("target", workflow.Factory(
			func(struct{}) (flow.Node[int, int], error) {
				return flow.NodeFunc[int, int](
					func(_ context.Context, input int) (int, error) {
						targetCalls.Add(1)
						return input, nil
					},
				), nil
			},
		))
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{
			ID: "target", Type: "target", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
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
		MustRegisterNode("route", routingFactory(func(int) string { return "no" })).
		MustRegisterSchema("route", routingSchema("yes", "no")).
		MustRegisterNode("copy", addN())
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{
			ID: "selected-only", Type: "copy", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
			When: []workflow.Gate{workflow.When("route", "yes")},
		},
		{
			ID: "ungated", Type: "copy",
			Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("selected-only")},
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
		MustRegisterNode("route", routingFactory(func(int) string { return "no" })).
		MustRegisterSchema("route", routingSchema("yes", "no")).
		MustRegisterNode("nested-route", routingFactory(func(int) string { return "next" })).
		MustRegisterSchema("nested-route", routingSchema("next")).
		MustRegisterNode("target", addN())
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{
			ID: "nested", Type: "nested-route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
			When: []workflow.Gate{workflow.When("route", "yes")},
		},
		{
			ID: "target", Type: "target", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
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

func TestCompileGraph_rejectsMismatchedFactoryIDBeforeGating(t *testing.T) {
	var calls atomic.Int64
	registry := workflow.NewRegistry().
		MustRegisterNode("route", func(workflow.NodeSpec) (workflow.Step, error) {
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
		MustRegisterNode("target", func(workflow.NodeSpec) (workflow.Step, error) {
			return workflow.LeafFunc(
				"duplicate",
				workflow.Output("start"),
				func(_ context.Context, input int) (int, error) {
					calls.Add(1)
					return input, nil
				},
			), nil
		})
	_, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{
			ID: "target", Type: "target", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
			When: []workflow.Gate{workflow.When("route", "yes")},
		},
	}})
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.As(err, &graphErr) || graphErr.Field != "type" ||
		!strings.Contains(err.Error(), `returned step ID "duplicate"; want "route"`) {
		t.Fatalf("CompileGraph error = %v; want mismatched route identity", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("node calls = %d; want validation before execution", calls.Load())
	}
}

func TestCompileGraph_rejectsRuntimeRoutingContractViolations(t *testing.T) {
	tests := map[string]workflow.NodeFactory{
		"unknown outlet": routingFactory(func(int) string { return "other" }),
		"wrong output type": func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				spec.Inputs[workflow.DefaultPort].Bind[int](),
				flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
					return input, nil
				}),
			), nil
		},
	}
	for name, factory := range tests {
		t.Run(name, func(t *testing.T) {
			registry := workflow.NewRegistry().
				MustRegisterNode("route", factory).
				MustRegisterSchema("route", routingSchema("yes")).
				MustRegisterNode("target", addN())
			graph := workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "route", Type: "route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
				{
					ID: "target", Type: "target", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
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
			// The event carries the same failure the run returned. A tracker watching
			// events is told why the step failed there and nowhere else, so an event
			// that reported only the kind would leave it guessing.
			failure := eventFailure(events, "target")
			if failure == nil || failure.Error() != err.Error() {
				t.Fatalf("EventFailed error = %v; want the failure Run reported, %v", failure, err)
			}
		})
	}
}

func TestCompileGraph_validatesEveryGateBeforeApplyingTrigger(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("good-route", routingFactory(func(int) string { return "yes" })).
		MustRegisterSchema("good-route", routingSchema("yes")).
		MustRegisterNode("bad-route", routingFactory(func(int) string { return "undeclared" })).
		MustRegisterSchema("bad-route", routingSchema("no")).
		MustRegisterNode("target", addN())
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "good", Type: "good-route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{ID: "bad", Type: "bad-route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")}},
		{
			ID: "target", Type: "target", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("start")},
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

func TestCompileGraph_triggerAnyWaitsForEveryRoutingSource(t *testing.T) {
	var targetCalls atomic.Int64
	registry := workflow.NewRegistry().
		MustRegisterNode("match", routingFactory(func(int) string { return "yes" })).
		MustRegisterSchema("match", routingSchema("yes")).
		MustRegisterNode("wait", workflow.Factory(
			func(struct{}) (flow.Node[int, string], error) {
				return flow.NodeFunc[int, string](
					func(_ context.Context, input int) (string, error) {
						return "", workflow.Suspend(input)
					},
				), nil
			},
		)).
		MustRegisterSchema("wait", routingSchema("yes")).
		MustRegisterNode("target", workflow.Factory(
			func(struct{}) (flow.Node[int, int], error) {
				return flow.NodeFunc[int, int](
					func(_ context.Context, input int) (int, error) {
						targetCalls.Add(1)
						return input, nil
					},
				), nil
			},
		))
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID:     "match",
			Type:   "match",
			Inputs: workflow.OneInput(workflow.Output("start")),
		},
		{
			ID:     "wait",
			Type:   "wait",
			Inputs: workflow.OneInput(workflow.Output("start")),
		},
		{
			ID:     "target",
			Type:   "target",
			Inputs: workflow.OneInput(workflow.Output("start")),
			When: []workflow.Gate{
				workflow.When("match", "yes"),
				workflow.When("wait", "yes"),
			},
			Trigger: workflow.TriggerAny,
		},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	_, err = workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", 1),
		workflow.RunConfig{Journal: workflow.NewJournal()},
	)
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("Run error = %v; want ErrSuspended", err)
	}
	if targetCalls.Load() != 0 {
		t.Fatalf("target calls = %d; want 0", targetCalls.Load())
	}
}

func TestValidateGraph_rejectsInvalidGates(t *testing.T) {
	baseRegistry := func() *workflow.Registry {
		return workflow.NewRegistry().
			MustRegisterNode("route", routingFactory(func(int) string { return "yes" })).
			MustRegisterSchema("route", workflow.NodeSchema{
				Output:  workflow.TypeString,
				Outlets: []string{"yes", "no"},
			}).
			MustRegisterNode("target", addN())
	}
	tests := map[string]struct {
		graph   workflow.Graph
		want    error
		field   string
		prepare func(*workflow.Registry)
	}{
		"unknown trigger": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "a", Type: "target", Trigger: "sometimes"},
			}},
			want: workflow.ErrInvalidGraph, field: "trigger",
		},
		"any without gates": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "a", Type: "target", Trigger: workflow.TriggerAny},
			}},
			want: workflow.ErrInvalidGraph, field: "trigger",
		},
		"empty source": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "a", Type: "target", When: []workflow.Gate{{Outlet: "yes"}}},
			}},
			want: workflow.ErrInvalidGraph, field: "when",
		},
		"empty outlet": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "a", Type: "target", When: []workflow.Gate{{NodeID: "route"}}},
			}},
			want: workflow.ErrInvalidGraph, field: "when",
		},
		"duplicate gate": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
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
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
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
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("missing", "yes")},
				},
			}},
			want: workflow.ErrUnknownNode, field: "when",
		},
		"self source": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("a", "yes")},
				},
			}},
			want: workflow.ErrCycle, field: "when",
		},
		"conditional cycle": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "route", Type: "route", Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("a")}},
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("route", "yes")},
				},
			}},
			want: workflow.ErrCycle,
		},
		"unknown outlet": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "route", Type: "route"},
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("route", "other")},
				},
			}},
			want: workflow.ErrUnknownOutlet, field: "when",
		},
		"source without schema": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "route", Type: "plain"},
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("route", "yes")},
				},
			}},
			want: workflow.ErrUnknownOutlet, field: "when",
			prepare: func(registry *workflow.Registry) {
				registry.MustRegisterNode("plain", addN())
			},
		},
		"source without outlets": {
			graph: workflow.Graph{Nodes: []workflow.GraphNode{
				{ID: "route", Type: "plain"},
				{
					ID: "a", Type: "target",
					When: []workflow.Gate{workflow.When("route", "yes")},
				},
			}},
			want: workflow.ErrUnknownOutlet, field: "when",
			prepare: func(registry *workflow.Registry) {
				registry.
					MustRegisterNode("plain", addN()).
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
			if test.field == "" {
				// A cycle spans the graph, so it belongs to no single node.
				return
			}
			// Every routing rule is checked at a node, so its failure names that
			// node both ways a caller can look for it: the pointer to where the
			// node is declared, and the ID the node declared for itself.
			wantPath := fmt.Sprintf("/nodes/%d", slices.IndexFunc(
				test.graph.Nodes,
				func(node workflow.GraphNode) bool { return node.ID == "a" },
			))
			if !errors.As(err, &graphErr) || graphErr.Field != test.field ||
				graphErr.Path != wantPath || graphErr.NodeID != "a" {
				t.Fatalf(
					"error = %v; want GraphError at %s on node %q field %q",
					err, wantPath, "a", test.field,
				)
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
	    {"id":"route","type":"route","inputs":{"in":{"nodeID":"start","path":"/output"}}},
	    {
	      "id":"target",
	      "type":"target",
	      "inputs":{"in":{"nodeID":"start","path":"/output"}},
	      "when":[{"nodeID":"route","outlet":"yes"}],
	      "trigger":"any"
	    }
	  ]
	}`)
	registry := workflow.NewRegistry().
		MustRegisterNode("route", routingFactory(func(int) string { return "yes" })).
		MustRegisterSchema("route", routingSchema("yes")).
		MustRegisterNode("target", addN())
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

// eventFailure returns the error reported with id's failure, or nil if no
// failure was reported for it.
func eventFailure(events <-chan workflow.Event, id string) error {
	for event := range events {
		if event.Kind == workflow.EventFailed && event.ID == id {
			return event.Err
		}
	}
	return nil
}

// TestCompiledGraph_bypassBelongsToOneScopedInvocation puts a chain of gates
// inside a loop body, which is where a bypass mark stops being a fact about a
// node and becomes a fact about one invocation of it. A gate whose source was
// bypassed is not satisfied, so the mark is read by the next node down; if it
// were kept without the scope it was made in, the second iteration would read
// the first one's mark and bypass a node whose gate is satisfied. Every gate
// test that runs a graph at the top level passes either way, because there the
// scope is empty.
func TestCompiledGraph_bypassBelongsToOneScopedInvocation(t *testing.T) {
	var branchCalls, targetCalls atomic.Int64
	iterationOutlet := func(ctx context.Context) string {
		scope := workflow.Scope(ctx)
		if scope[len(scope)-1].Index == 0 {
			return "no"
		}
		return "yes"
	}
	registry := workflow.NewRegistry().
		MustRegisterNode("route", workflow.Factory(
			func(struct{}) (flow.Node[int, string], error) {
				return flow.NodeFunc[int, string](
					func(ctx context.Context, _ int) (string, error) {
						return iterationOutlet(ctx), nil
					},
				), nil
			})).
		MustRegisterSchema("route", routingSchema("yes", "no")).
		MustRegisterNode("branch", workflow.Factory(
			func(struct{}) (flow.Node[int, string], error) {
				return flow.NodeFunc[int, string](
					func(context.Context, int) (string, error) {
						branchCalls.Add(1)
						return "go", nil
					},
				), nil
			})).
		MustRegisterSchema("branch", routingSchema("go")).
		MustRegisterNode("target", workflow.Factory(
			func(struct{}) (flow.Node[int, int], error) {
				return flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
					targetCalls.Add(1)
					return value, nil
				}), nil
			}))

	seed := workflow.Inputs{workflow.DefaultPort: workflow.Output("seed")}
	body, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route", Inputs: seed},
		{
			ID: "branch", Type: "branch", Inputs: seed,
			When: []workflow.Gate{workflow.When("route", "yes")},
		},
		{
			ID: "target", Type: "target", Inputs: seed,
			When: []workflow.Gate{workflow.When("branch", "go")},
		},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	iterations := 0
	loop := workflow.Loop(workflow.LoopConfig{
		ID:   "loop",
		Body: body,
		Condition: flow.NodeFunc[workflow.Store, bool](
			func(context.Context, workflow.Store) (bool, error) {
				iterations++
				return iterations >= 2, nil
			},
		),
	})

	bypassed := make(map[string]int)
	out, err := workflow.Run(
		t.Context(),
		loop,
		workflow.NewStore().WithOutput("seed", 7),
		workflow.RunConfig{Observer: workflow.ObserverFunc(
			func(_ context.Context, event workflow.Event) {
				if event.Kind == workflow.EventBypassed {
					bypassed[event.ID]++
				}
			},
		)},
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if branchCalls.Load() != 1 || targetCalls.Load() != 1 {
		t.Fatalf(
			"calls = branch:%d target:%d; want each to run in the iteration that selected it",
			branchCalls.Load(), targetCalls.Load(),
		)
	}
	if bypassed["branch"] != 1 || bypassed["target"] != 1 {
		t.Fatalf("bypassed = %v; want branch and target bypassed once each", bypassed)
	}
	if got, readErr := out.Get[int](workflow.Output("target")); readErr != nil || got != 7 {
		t.Fatalf("target output = %d, %v; want the value from the iteration that ran it", got, readErr)
	}
}
