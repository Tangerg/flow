package workflow_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// A workflow can be written three ways: built in Go, compiled from a Spec, or
// compiled from a flat Graph. Nothing in the package states that the three are
// interchangeable, yet every one of them is documented as producing a Step, and a
// caller choosing a serialized form has no reason to expect different behavior
// from it. These tests hold the three to the same observable outcome — the events
// a boundary reports, the Store it leaves, and the checkpoint it writes — so a
// change that only touches one construction route cannot quietly diverge.
//
// Only Describe is expected to differ: a compiled Graph is a graph, and reporting
// it as a sequence would misdescribe how its nodes are scheduled.
func equivalentForms(t *testing.T, wait bool) map[string]workflow.Step {
	t.Helper()
	doubler := func() flow.Node[int, int] {
		return flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value * 2, nil
		})
	}
	registry := workflow.NewRegistry().
		MustRegisterNode("double", workflow.Factory(
			func(struct{}) (flow.Node[int, int], error) { return doubler(), nil })).
		MustRegisterNode("wait", workflow.InterruptFactory())

	code := []workflow.Step{
		workflow.Leaf("a", workflow.From[int](workflow.Output("seed")), doubler()),
	}
	specs := []workflow.Spec{{
		Kind: workflow.KindLeaf, ID: "a", Type: "double",
		Inputs: workflow.OneInput(workflow.Output("seed")),
	}}
	nodes := []workflow.GraphNode{{
		ID: "a", Type: "double", Inputs: workflow.OneInput(workflow.Output("seed")),
	}}
	last := "a"
	if wait {
		code = append(code, workflow.Interrupt("w", "decide"))
		// InterruptFactory exposes the leaf's config as the suspension value, which
		// is what Interrupt's second argument does in Go, so all three carry it.
		ask := json.RawMessage(`"decide"`)
		specs = append(specs, workflow.Spec{
			Kind: workflow.KindLeaf, ID: "w", Type: "wait", Config: ask,
		})
		nodes = append(nodes, workflow.GraphNode{
			ID: "w", Type: "wait", Config: ask, DependsOn: []string{"a"},
		})
		last = "w"
	}
	code = append(code, workflow.Leaf("z", workflow.From[int](workflow.Output(last)), doubler()))
	specs = append(specs, workflow.Spec{
		Kind: workflow.KindLeaf, ID: "z", Type: "double",
		Inputs: workflow.OneInput(workflow.Output(last)),
	})
	nodes = append(nodes, workflow.GraphNode{
		ID: "z", Type: "double", Inputs: workflow.OneInput(workflow.Output(last)),
	})

	spec, err := registry.CompileSpec(workflow.Spec{Kind: workflow.KindSequence, Steps: specs})
	if err != nil {
		t.Fatalf("CompileSpec: %v", err)
	}
	graph, err := registry.CompileGraph(workflow.Graph{Nodes: nodes})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	return map[string]workflow.Step{
		"code":  workflow.Sequence(code...),
		"spec":  spec,
		"graph": graph,
	}
}

// observed is what a caller can see of one run.
type observed struct {
	events []string
	err    string
	values []int
	wire   string
}

func observe(t *testing.T, step workflow.Step, journal *workflow.Journal, ids []string) observed {
	t.Helper()
	result, _ := observeRun(t, step, journal, ids)
	return result
}

// observeRun also returns the live error, which a caller needs to read the wait
// it reports; observed keeps only the rendered text so two forms can be compared.
func observeRun(
	t *testing.T,
	step workflow.Step,
	journal *workflow.Journal,
	ids []string,
) (observed, error) {
	t.Helper()
	result := observed{}
	out, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("seed", 5),
		workflow.RunConfig{
			Journal: journal,
			Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
				result.events = append(result.events, string(event.Kind)+"/"+event.ID)
			}),
		},
	)
	if err != nil {
		result.err = err.Error()
	}
	for _, id := range ids {
		value, _ := workflow.Get[int](out, workflow.Output(id))
		result.values = append(result.values, value)
	}
	// Concurrent graph nodes report in completion order, which is not part of the
	// contract; the set of transitions is.
	slices.Sort(result.events)
	encoded, marshalErr := json.Marshal(journal)
	if marshalErr != nil {
		t.Fatalf("Marshal journal: %v", marshalErr)
	}
	result.wire = string(encoded)
	return result, err
}

func TestEveryConstructionFormRunsTheSameWorkflow(t *testing.T) {
	forms := equivalentForms(t, false)
	ids := []string{"a", "z"}
	want := observe(t, forms["code"], workflow.NewJournal(), ids)
	if want.err != "" || !slices.Equal(want.values, []int{10, 20}) {
		t.Fatalf("code form = %+v; want a clean run producing 10 and 20", want)
	}
	for _, name := range []string{"spec", "graph"} {
		got := observe(t, forms[name], workflow.NewJournal(), ids)
		if got.err != want.err || !slices.Equal(got.values, want.values) ||
			!slices.Equal(got.events, want.events) || got.wire != want.wire {
			t.Fatalf("%s form = %+v; want the code form's %+v", name, got, want)
		}
	}
}

func TestEveryConstructionFormSuspendsAndResumesAlike(t *testing.T) {
	forms := equivalentForms(t, true)
	ids := []string{"a", "w", "z"}

	resume := func(name string) (observed, observed) {
		journal := workflow.NewJournal()
		first, err := observeRun(t, forms[name], journal, ids)
		waits := workflow.Suspensions(err)
		if len(waits) != 1 {
			t.Fatalf("%s: got %d waits; want exactly one", name, len(waits))
		}
		if recordErr := journal.Record(waits[0].Key(), 7); recordErr != nil {
			t.Fatalf("%s: Record the response: %v", name, recordErr)
		}
		second, _ := observeRun(t, forms[name], journal, ids)
		return first, second
	}

	wantFirst, wantSecond := resume("code")
	if !slices.Contains(wantFirst.events, "suspended/w") {
		t.Fatalf("code form first run = %+v; want a suspension at w", wantFirst)
	}
	if wantSecond.err != "" || !slices.Equal(wantSecond.values, []int{10, 7, 14}) {
		t.Fatalf("code form resumed = %+v; want 10, 7 and 14", wantSecond)
	}
	for _, name := range []string{"spec", "graph"} {
		first, second := resume(name)
		if !slices.Equal(first.events, wantFirst.events) || first.err != wantFirst.err {
			t.Fatalf("%s form first run = %+v; want the code form's %+v", name, first, wantFirst)
		}
		if !slices.Equal(second.events, wantSecond.events) ||
			!slices.Equal(second.values, wantSecond.values) ||
			second.err != wantSecond.err || second.wire != wantSecond.wire {
			t.Fatalf("%s form resumed = %+v; want the code form's %+v", name, second, wantSecond)
		}
	}
}

// TestAProjectionDefectReadsTheSameWhicheverCheckFindsIt pins the division of
// labour these two checks document. A body whose node type declares a schema
// has a knowable output set, so validating the Spec rejects a bad projection.
// A body whose type declares none does not, so validation accepts and
// compilation rejects once the factory has returned a concrete step. The defect
// is the same either way and must read the same, or which check happened to run
// first would change what the author is told.
//
// Comparing CompileSpec against ValidateSpec on one Spec would prove nothing:
// compilation begins by validating. These are two independent judgements.
func TestAProjectionDefectReadsTheSameWhicheverCheckFindsIt(t *testing.T) {
	factory := func(spec workflow.NodeSpec) (workflow.Step, error) {
		return workflow.Interrupt(spec.ID, nil), nil
	}
	registry := workflow.NewRegistry().
		MustRegisterNode("declared", factory).
		MustRegisterSchema("declared", workflow.NodeSchema{Output: workflow.TypeAny}).
		MustRegisterNode("schemaless", factory)

	compose := map[string]func(body *workflow.Spec) workflow.Spec{
		"subgraph": func(body *workflow.Spec) workflow.Spec {
			return workflow.Spec{
				Kind: workflow.KindSubgraph, ID: "sg",
				Body: body, BodyOutput: workflow.Output("ghost"),
			}
		},
		"iteration": func(body *workflow.Spec) workflow.Spec {
			return workflow.Spec{
				Kind: workflow.KindIteration, ID: "each", Input: workflow.Output("seed"),
				Body: body, BodyOutput: workflow.Output("ghost"),
			}
		},
	}

	for kind, build := range compose {
		t.Run(kind, func(t *testing.T) {
			declared := workflow.Spec{Kind: workflow.KindLeaf, ID: "inner", Type: "declared"}
			schemaless := workflow.Spec{Kind: workflow.KindLeaf, ID: "inner", Type: "schemaless"}

			found := registry.ValidateSpec(build(&declared))
			if found == nil {
				t.Fatal("ValidateSpec accepted a projection its schema proves impossible")
			}
			if err := registry.ValidateSpec(build(&schemaless)); err != nil {
				t.Fatalf("ValidateSpec = %v; want it to defer to compilation without a schema", err)
			}
			_, deferred := registry.CompileSpec(build(&schemaless))
			if deferred == nil {
				t.Fatal("CompileSpec accepted a projection the built body contradicts")
			}
			if deferred.Error() != found.Error() {
				t.Fatalf("the same defect reads two ways:\n  validation:  %v\n  compilation: %v",
					found, deferred)
			}
		})
	}
}
