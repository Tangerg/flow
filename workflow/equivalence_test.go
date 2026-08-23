package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
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
		workflow.Leaf("a", workflow.Output("seed").Bind[int](), doubler()),
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
	code = append(code, workflow.Leaf("z", workflow.Output(last).Bind[int](), doubler()))
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
		value, _ := out.Get[int](workflow.Output(id))
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

// TestTheTwoValidatorsRefuseTheSameDefects pins the agreement between the two
// checks that judge structure: ValidateSpec walks a Spec before anything is
// built, and definition validation walks the built Steps. Neither can be derived
// from the other -- validating a Spec must not run factories, and a built Step
// exposes its children only through its own boundary -- so each states the same
// four rules over its own shape. Nothing but this holds the two statements to the
// same verdict, and a rule that drifted would make a workflow's legality depend
// on which form its author happened to write it in.
//
// The sentinel is the agreement; the message deliberately is not. A Spec locates
// a defect by wire path and a definition locates it by step identity, which
// TestAProjectionDefectReadsTheSameWhicheverCheckFindsIt covers for the one
// defect both can phrase the same way.
func TestTheTwoValidatorsRefuseTheSameDefects(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("leaf", workflow.InterruptFactory()).
		MustRegisterCondition("again", flow.NodeFunc[workflow.Store, bool](
			func(context.Context, workflow.Store) (bool, error) { return true, nil },
		))
	always := flow.NodeFunc[workflow.Store, bool](
		func(context.Context, workflow.Store) (bool, error) { return true, nil },
	)
	leafSpec := func(id string) workflow.Spec {
		return workflow.Spec{Kind: workflow.KindLeaf, ID: id, Type: "leaf"}
	}
	waitStep := func(id string) workflow.Step { return workflow.Interrupt(id, nil) }

	// Each defect is written twice: once as a Spec for the registry to validate,
	// once as the Steps that Spec compiles to, for definition validation.
	tests := map[string]struct {
		spec workflow.Spec
		step workflow.Step
		want error
	}{
		"a step ID taken twice among siblings": {
			spec: workflow.Spec{
				Kind:  workflow.KindSequence,
				Steps: []workflow.Spec{leafSpec("same"), leafSpec("same")},
			},
			step: workflow.Sequence(waitStep("same"), waitStep("same")),
			want: workflow.ErrDuplicateStep,
		},
		// Both validators hold one claim inline and allocate a set only for a
		// second distinct ID, so a repeat with another step between it and the
		// original is refused by different code than an immediate repeat.
		"a step ID taken twice with another between": {
			spec: workflow.Spec{
				Kind:  workflow.KindSequence,
				Steps: []workflow.Spec{leafSpec("same"), leafSpec("other"), leafSpec("same")},
			},
			step: workflow.Sequence(waitStep("same"), waitStep("other"), waitStep("same")),
			want: workflow.ErrDuplicateStep,
		},
		"a loop body taking the loop's own ID": {
			spec: workflow.Spec{
				Kind: workflow.KindLoop, ID: "same",
				Condition: "again",
				Body:      new(leafSpec("same")),
			},
			step: workflow.Loop(workflow.LoopConfig{
				ID: "same", Body: waitStep("same"), Condition: always,
			}),
			want: workflow.ErrDuplicateStep,
		},
		"nesting past the depth limit": {
			spec: nestedSpec(workflow.MaxNestingDepth + 1),
			step: nestedStep(workflow.MaxNestingDepth + 1),
			want: workflow.ErrMaxDepth,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			specErr := registry.ValidateSpec(test.spec)
			stepErr := flow.Validate(test.step)
			if !errors.Is(specErr, test.want) || !errors.Is(stepErr, test.want) {
				t.Fatalf(
					"the two checks disagree on %v:\n  ValidateSpec: %v\n  definition:   %v",
					test.want, specErr, stepErr,
				)
			}
		})
	}
}

// nestedSpec and nestedStep build the same sequence nested depth levels deep, so
// the two validators are asked about one shape rather than two.
func nestedSpec(depth int) workflow.Spec {
	spec := workflow.Spec{Kind: workflow.KindSequence}
	for range depth {
		spec = workflow.Spec{Kind: workflow.KindSequence, Steps: []workflow.Spec{spec}}
	}
	return spec
}

func nestedStep(depth int) workflow.Step {
	step := workflow.Sequence()
	for range depth {
		step = workflow.Sequence(step)
	}
	return step
}

// TestAReplayedDecisionMustCarryTheTypeItsCompositeRecorded holds Branch and Loop
// to one rule. Both journal a decision rather than an output, so both can meet a
// record of another type -- an edited checkpoint, or one written by a different
// workflow under the same ID -- and neither may guess what it decided last time.
// The wanted type is named by the type the composite replays, so a message cannot
// promise one thing while the assertion accepts another, and the refusal is
// located at the composite that made the decision.
func TestAReplayedDecisionMustCarryTheTypeItsCompositeRecorded(t *testing.T) {
	body := workflow.Leaf("body",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) { return value, nil }))

	for _, test := range []struct {
		name string
		id   string
		// scope is where the composite keys its decision: a branch decides once, a
		// loop once per iteration, so only the loop's key carries an indexed frame.
		scope    []workflow.ScopeFrame
		recorded any
		want     string
		step     workflow.Step
	}{
		{
			name:     "a branch replays the case name it chose",
			id:       "route",
			recorded: true,
			want:     "journaled branch decision has type bool; want string",
			step: workflow.Branch(workflow.BranchConfig{
				ID:       "route",
				Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) { return "ok", nil }),
				Cases:    map[string]workflow.Step{"ok": leafStep("ok")},
			}),
		},
		{
			name:     "a loop replays whether it stopped",
			id:       "repeat",
			scope:    []workflow.ScopeFrame{{ID: "repeat", Indexed: true}},
			recorded: "stop",
			want:     "journaled loop decision has type string; want bool",
			step: workflow.Loop(workflow.LoopConfig{
				ID:        "repeat",
				Body:      body,
				Condition: flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) { return true, nil }),
			}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			journal := workflow.NewJournal()
			key := workflow.JournalKey{ID: test.id, Scope: test.scope}
			if err := journal.Record(key, test.recorded); err != nil {
				t.Fatalf("Record: %v", err)
			}

			_, err := runJournal(test.step, workflow.NewStore().WithOutput("start", 1), journal)
			var stepErr *workflow.StepError
			if !errors.As(err, &stepErr) || stepErr.ID != test.id || stepErr.Op != workflow.OpRun {
				t.Fatalf("err = %v; want a StepError at %q under OpRun", err, test.id)
			}
			if !errors.Is(err, workflow.ErrTypeMismatch) {
				t.Fatalf("err = %v; want ErrTypeMismatch", err)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err = %v; want it to state %q", err, test.want)
			}
		})
	}
}
