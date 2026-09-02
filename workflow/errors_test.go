package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestErrNilStepIsANilNode(t *testing.T) {
	if !errors.Is(workflow.ErrNilStep, flow.ErrNilNode) {
		t.Fatal("ErrNilStep does not match flow.ErrNilNode")
	}
	if errors.Is(flow.ErrNilNode, workflow.ErrNilStep) {
		t.Fatal("ErrNilNode unexpectedly matches the narrower ErrNilStep")
	}
}

func TestStructuredErrorFormatting(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "step", err: &workflow.StepError{ID: "load", Op: workflow.OpRun, Err: errors.New("boom")}, want: `step "load" run`},
		{
			name: "scoped step",
			err: &workflow.StepError{
				ID:    "load",
				Scope: indexedScope("items", 2),
				Op:    workflow.OpRun,
				Err:   errors.New("boom"),
			},
			want: `step "load" in items[2] run`,
		},
		{name: "ref missing", err: &workflow.RefError{Ref: workflow.Output("load"), Want: "int", Err: workflow.ErrNotFound}, want: "value not found"},
		{name: "ref nil", err: &workflow.RefError{Ref: workflow.Output("load"), Want: "int", Err: workflow.ErrTypeMismatch}, want: "got <nil>, want int"},
		{name: "ref mismatch", err: &workflow.RefError{Ref: workflow.Output("load"), Want: "int", Got: "string", Err: workflow.ErrTypeMismatch}, want: "got string, want int"},
		{name: "registration unnamed", err: &workflow.RegistrationError{Kind: "leaf", Err: workflow.ErrInvalidRegistration}, want: "register leaf:"},
		{name: "registration named", err: &workflow.RegistrationError{Kind: "leaf", Name: "add", Err: workflow.ErrDuplicateRegistration}, want: `leaf "add"`},
		{name: "graph path node field", err: &workflow.GraphError{Path: "/nodes/1", NodeID: "a", Field: "type", Err: workflow.ErrUnknownNodeType}, want: `graph at "/nodes/1" node "a" field type`},
		{name: "graph node", err: &workflow.GraphError{NodeID: "a", Err: workflow.ErrInvalidGraph}, want: `node "a":`},
		{name: "graph field", err: &workflow.GraphError{Field: "nodes", Err: workflow.ErrInvalidGraph}, want: "field nodes:"},
		// The location and the condition each appear once: the wrapper
		// identifies this package, so the sentinel does not repeat it.
		{name: "whole graph", err: &workflow.GraphError{Err: workflow.ErrCycle}, want: "workflow: graph: graph cycle"},
		{name: "spec", err: &workflow.SpecError{Path: "/steps/0", Kind: workflow.KindLeaf, ID: "a", Field: "type", Err: workflow.ErrUnknownNodeType}, want: `spec at "/steps/0" leaf "a" field type`},
		{name: "spec nil cause", err: &workflow.SpecError{}, want: "spec: <nil>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); !strings.Contains(got, tt.want) {
				t.Fatalf("Error() = %q; want substring %q", got, tt.want)
			}
		})
	}
}

// RefError is exported structured data, so an application can assemble its
// cause without crossing a workflow depth boundary. Formatting must classify a
// deeply joined missing-value cause without recursive standard error matching.
func TestRefErrorFormatsDeepJoinedCauseWithoutStackPerWrapper(t *testing.T) {
	withBoundedStack(t, func() {
		cause := workflow.ErrNotFound
		for range 20_000 {
			cause = errorChildren{cause}
		}
		message := (&workflow.RefError{
			Ref:  workflow.Output("load"),
			Want: "int",
			Err:  cause,
		}).Error()
		if message != "workflow: ref load#/output: joined children" {
			t.Fatalf("RefError = %q; reinterpreted a caller-owned multi-error", message)
		}
	})
}

// Workflow's location errors are exported structured data, so an application
// can assemble a direct chain without crossing a definition-depth boundary.
// Rendering one must consume bounded stack just as matching and cloning it do.
func TestWorkflowErrorsFormatDeepOwnedChainIteratively(t *testing.T) {
	withBoundedStack(t, func() {
		var err error
		err = errors.New("boom")
		for index := range 20_000 {
			switch index % 5 {
			case 0:
				err = &workflow.StepError{ID: fmt.Sprintf("step-%d", index), Op: workflow.OpRun, Err: err}
			case 1:
				err = &workflow.RefError{Ref: workflow.Output(fmt.Sprintf("node-%d", index)), Want: "int", Got: "string", Err: err}
			case 2:
				err = &workflow.RegistrationError{Kind: "node", Name: fmt.Sprintf("node-%d", index), Err: err}
			case 3:
				err = &workflow.GraphError{Path: fmt.Sprintf("/nodes/%d", index), NodeID: fmt.Sprintf("node-%d", index), Field: "type", Err: err}
			case 4:
				err = &workflow.SpecError{Path: fmt.Sprintf("/steps/%d", index), Kind: workflow.KindLeaf, ID: fmt.Sprintf("step-%d", index), Field: "type", Err: err}
			}
		}
		message := err.Error()
		if !strings.HasPrefix(message, `workflow: spec at "/steps/19999" leaf "step-19999" field type: `) ||
			!strings.Contains(message, `step "step-0" run: boom: got string, want int`) ||
			strings.Count(message, "workflow:") != 1 {
			t.Fatalf("Error() did not preserve the complete wrapper order")
		}
	})
}

// Exported workflow errors are pointer-shaped structured data and can appear as
// typed nil causes. Formatting must terminate at that boundary instead of
// recursively asking fmt to invoke the same Error method again.
func TestWorkflowErrorsFormatTypedNilAsNil(t *testing.T) {
	var stepErr *workflow.StepError
	var refErr *workflow.RefError
	var registrationErr *workflow.RegistrationError
	var graphErr *workflow.GraphError
	var specErr *workflow.SpecError
	var indexErr *flow.IndexError
	var caseErr *flow.CaseError

	for name, err := range map[string]error{
		"step":          stepErr,
		"reference":     refErr,
		"registration":  registrationErr,
		"graph":         graphErr,
		"specification": specErr,
		"index":         indexErr,
		"case":          caseErr,
	} {
		t.Run(name, func(t *testing.T) {
			if got := err.Error(); got != "<nil>" {
				t.Fatalf("Error() = %q; want <nil>", got)
			}
			outer := &workflow.StepError{ID: "outer", Op: workflow.OpRun, Err: err}
			if got := outer.Error(); got != `workflow: step "outer" run: <nil>` {
				t.Fatalf("outer Error() = %q", got)
			}
			for _, category := range []error{
				workflow.ErrInvalidRegistration,
				workflow.ErrInvalidGraph,
				workflow.ErrInvalidSpec,
				workflow.ErrNotFound,
			} {
				if errors.Is(err, category) {
					t.Fatalf("typed nil error matched %v", category)
				}
			}
		})
	}
}

func TestRefError(t *testing.T) {
	tests := []struct {
		name  string
		store workflow.Store
		want  error
	}{
		{name: "missing", want: workflow.ErrNotFound},
		{name: "type mismatch", store: workflow.NewStore().WithOutput("n", "text"), want: workflow.ErrTypeMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.store.Get[int](workflow.Output("n"))
			var refErr *workflow.RefError
			if !errors.Is(err, tt.want) || !errors.As(err, &refErr) || refErr.Ref != workflow.Output("n") {
				t.Fatalf("err = %v; want RefError wrapping %v", err, tt.want)
			}
		})
	}
}

// TestRefErrorReportsItsMismatchOnceAcrossAJoinedCause pins where the got/want
// detail goes when a reference's cause is a multi-branch [errors.Join]. The
// mismatch describes the reference, not a branch, so it is stated once after the
// last branch rather than repeated per line or dropped because the branches
// interrupted the chain. A cause that reports absence still suppresses it: there
// is no value whose type could disagree.
func TestRefErrorReportsItsMismatchOnceAcrossAJoinedCause(t *testing.T) {
	ref := workflow.Output("n")
	mismatch := &workflow.RefError{
		Ref:  ref,
		Want: "int",
		Got:  "string",
		Err: errors.Join(
			workflow.ErrTypeMismatch,
			errors.New("convert: not a number"),
		),
	}
	want := "workflow: ref " + ref.String() + ": value type mismatch\n" +
		"convert: not a number: got string, want int"
	if got := mismatch.Error(); got != want {
		t.Fatalf("Error() = %q; want %q", got, want)
	}

	absent := &workflow.RefError{
		Ref:  ref,
		Want: "int",
		Err:  errors.Join(workflow.ErrNotFound, errors.New("no such node")),
	}
	if got := absent.Error(); strings.Contains(got, "want int") {
		t.Fatalf("Error() = %q; want no mismatch beside an absent value", got)
	}
}

// TestRegistrationError covers the three ways a Registry refuses an entry --
// an unusable name, a value that fails what its kind must prove, and a name
// already taken -- because each builds its own error. All three have to name the
// kind of registration and the name it was made under: that pair is what a
// caller has to search for to find the call to fix, and the message is all the
// panicking Must form leaves behind.
func TestRegistrationError(t *testing.T) {
	tests := map[string]struct {
		register func(*workflow.Registry) error
		name     string
		cause    error
	}{
		"unusable name": {
			register: func(reg *workflow.Registry) error {
				return reg.RegisterNode("bad\xff", addN())
			},
			name: "bad\xff",
		},
		"nil factory": {
			register: func(reg *workflow.Registry) error {
				return reg.RegisterNode("add", nil)
			},
			name:  "add",
			cause: flow.ErrNilFunc,
		},
		"name already taken": {
			register: func(reg *workflow.Registry) error {
				if err := reg.RegisterNode("add", addN()); err != nil {
					return err
				}
				return reg.RegisterNode("add", addN())
			},
			name:  "add",
			cause: workflow.ErrDuplicateRegistration,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := test.register(workflow.NewRegistry())
			var registrationErr *workflow.RegistrationError
			if !errors.Is(err, workflow.ErrInvalidRegistration) ||
				!errors.As(err, &registrationErr) ||
				registrationErr.Kind != "node" || registrationErr.Name != test.name {
				t.Fatalf("err = %v; want an invalid node registration named %q", err, test.name)
			}
			if test.cause != nil && !errors.Is(err, test.cause) {
				t.Fatalf("err = %v; want %v", err, test.cause)
			}
		})
	}
}

func TestGraphError(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("add", addN())
	graph := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "same", Type: "add"},
		{ID: "same", Type: "add"},
	}}
	err := reg.ValidateGraph(graph)
	var graphErr *workflow.GraphError
	if !errors.Is(err, workflow.ErrInvalidGraph) ||
		!errors.Is(err, workflow.ErrDuplicateNode) ||
		!errors.As(err, &graphErr) || graphErr.Path != "/nodes/1" ||
		graphErr.NodeID != "same" || graphErr.Field != "id" {
		t.Fatalf("err = %v; want invalid-graph duplicate node GraphError", err)
	}
}

func TestSpecError(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("add", addN())
	spec := workflow.Spec{Kind: workflow.KindParallel, Steps: []workflow.Spec{
		{Kind: workflow.KindLeaf, ID: "same", Type: "add"},
		{Kind: workflow.KindLeaf, ID: "same", Type: "add"},
	}}
	_, err := reg.CompileSpec(spec)
	var specErr *workflow.SpecError
	if !errors.Is(err, workflow.ErrInvalidSpec) ||
		!errors.Is(err, workflow.ErrDuplicateStep) ||
		!errors.As(err, &specErr) || specErr.ID != "same" || specErr.Field != "id" {
		t.Fatalf("err = %v; want invalid-spec duplicate step SpecError", err)
	}
}

// Every error this package surfaces names it exactly once. The structured error
// types and the top-level guards supply that prefix, so a sentinel must not
// carry one of its own; a cause from another package keeps its prefix, which is
// what marks a kernel failure inside a workflow error.
func TestSurfacedErrorsNamePackageExactlyOnce(t *testing.T) {
	registry := workflow.NewRegistry()
	registry.MustRegisterNode("n", workflow.Factory(
		func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](
				func(_ context.Context, value int) (int, error) { return value, nil },
			), nil
		},
	))
	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "a"}, 1); err != nil {
		t.Fatalf("Record: %v", err)
	}
	cyclic := workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "a", Type: "n", Inputs: workflow.OneInput(workflow.Output("b"))},
		{ID: "b", Type: "n", Inputs: workflow.OneInput(workflow.Output("a"))},
	}}
	buildFailure := workflow.NewRegistry().MustRegisterNode("broken", workflow.Factory(
		func(struct{}) (flow.Node[int, int], error) {
			return nil, errors.New("builder failed")
		},
	))
	_, directBuildFailure := workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
		return nil, errors.New("builder failed")
	})(workflow.NodeSpec{Inputs: workflow.OneInput(workflow.Output("seed"))})
	invalidSubgraphFactory := workflow.SubgraphFactory(nil, workflow.Output("body"))
	_, directSubgraphFailure := invalidSubgraphFactory(workflow.NodeSpec{ID: "subgraph"})
	invalidSubgraph := workflow.NewRegistry().MustRegisterNode("subgraph", invalidSubgraphFactory)

	errs := map[string]error{
		"journal conflict": journal.Record(workflow.JournalKey{ID: "a"}, 1),
		"missing value": func() error {
			_, err := workflow.NewStore().Get[int](workflow.Output("x"))
			return err
		}(),
		"type mismatch": func() error {
			_, err := workflow.NewStore().WithOutput("x", "s").Get[int](workflow.Output("x"))
			return err
		}(),
		"duplicate registration": registry.RegisterNode("n", func(workflow.NodeSpec) (workflow.Step, error) {
			return nil, nil
		}),
		"nil factory":       workflow.NewRegistry().RegisterNode("n", nil),
		"graph cycle":       registry.ValidateGraph(cyclic),
		"unknown node type": registry.ValidateGraph(workflow.Graph{Nodes: []workflow.GraphNode{{ID: "a", Type: "?"}}}),
		"unwired port": registry.ValidateGraph(workflow.Graph{Nodes: []workflow.GraphNode{
			{ID: "a", Type: "n", Inputs: workflow.OneInput(workflow.Ref{})},
		}}),
		"invalid spec": registry.ValidateSpec(workflow.Spec{Kind: workflow.KindSequence, Type: "x"}),
		"unknown spec node type": registry.ValidateSpec(workflow.Spec{
			Kind: workflow.KindLeaf, ID: "x", Type: "?",
		}),
		"node build failure": compileGraphError(buildFailure, workflow.Graph{Nodes: []workflow.GraphNode{{
			ID:     "broken",
			Type:   "broken",
			Inputs: workflow.OneInput(workflow.Output("seed")),
		}}}),
		"direct node build failure": directBuildFailure,
		"subgraph factory definition": compileGraphError(invalidSubgraph, workflow.Graph{Nodes: []workflow.GraphNode{{
			ID:   "subgraph",
			Type: "subgraph",
		}}}),
		"direct subgraph factory definition": directSubgraphFailure,
		"nested leaf validation":             nestedLeafValidationError(t),
		"nested switched leaf validation":    nestedSwitchedLeafValidationError(t),
		"nested mapped leaf failure":         nestedMappedLeafError(t),
		"subgraph input binding":             subgraphInputError(t),
		"subgraph body projection":           subgraphProjectionError(t),
		"iteration body failure":             iterationBodyError(t),
		"iteration body projection":          iterationProjectionError(t),
		"bad graph JSON":                     workflow.ValidateGraphJSON([]byte(`{"nodes":[`)),
		"bad journal wire":                   json.Unmarshal([]byte(`{"version":1,"records":[]}`), workflow.NewJournal()),
		"nil journal key":                    (*workflow.JournalKey)(nil).UnmarshalJSON([]byte(`{"id":"a"}`)),
		"nil scope frame":                    (*workflow.ScopeFrame)(nil).UnmarshalJSON([]byte(`{"id":"a"}`)),
		"nil step to Run":                    runError(t, nil),
		"nil loop body":                      runError(t, workflow.Loop(workflow.LoopConfig{ID: "l", Body: nil})),
		"duplicate step ID":                  runError(t, workflow.Sequence(passthrough("same"), passthrough("same"))),
		// The same conflict as "journal conflict", but reached during a run so a
		// StepError supplies the prefix instead of Record. The two paths format
		// separately, so both have to be checked.
		"journal conflict during a run": recordedDuringARun(t),
	}

	for name, err := range errs {
		t.Run(name, func(t *testing.T) {
			if err == nil {
				t.Fatal("expected an error")
			}
			message := err.Error()
			if got := strings.Count(message, "workflow:"); got != 1 {
				t.Fatalf("names %q %d times, want exactly 1: %s", "workflow:", got, message)
			}
		})
	}
}

func compileGraphError(registry *workflow.Registry, graph workflow.Graph) error {
	_, err := registry.CompileGraph(graph)
	return err
}

func nestedLeafValidationError(t *testing.T) error {
	t.Helper()
	bind := workflow.BinderFunc[workflow.Store](func(store workflow.Store) (workflow.Store, error) {
		return store, nil
	})
	inner := workflow.Leaf("inner", bind, flow.NodeFunc[workflow.Store, workflow.Store](nil))
	return runError(t, workflow.Leaf("outer", bind, inner))
}

func nestedSwitchedLeafValidationError(t *testing.T) error {
	t.Helper()
	bind := workflow.BinderFunc[workflow.Store](func(store workflow.Store) (workflow.Store, error) {
		return store, nil
	})
	first := workflow.Leaf("first", bind, flow.NodeFunc[workflow.Store, workflow.Store](nil))
	second := workflow.Leaf("second", bind, flow.NodeFunc[workflow.Store, workflow.Store](nil))
	resolve := flow.NodeFunc[workflow.Store, string](
		func(context.Context, workflow.Store) (string, error) { return "first", nil },
	)
	switched := flow.Switch(resolve, map[string]flow.Node[workflow.Store, workflow.Store]{
		"first":  first,
		"second": second,
	})
	return runError(t, workflow.Leaf("outer", bind, switched))
}

func nestedMappedLeafError(t *testing.T) error {
	t.Helper()
	inner := workflow.LeafFunc("inner", workflow.Output("missing"),
		func(_ context.Context, value int) (int, error) { return value, nil })
	mapped := flow.Map(inner, flow.MapConfig{})
	bind := workflow.BinderFunc[[]workflow.Store](func(store workflow.Store) ([]workflow.Store, error) {
		return []workflow.Store{store}, nil
	})
	return runError(t, workflow.Leaf("outer", bind, mapped))
}

func subgraphInputError(t *testing.T) error {
	t.Helper()
	body := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) { return store, nil },
	)
	return runError(t, workflow.Subgraph(workflow.SubgraphConfig{
		ID:         "subgraph",
		Inputs:     workflow.Inputs{"seed": workflow.Output("missing")},
		Body:       body,
		BodyOutput: workflow.Output("seed"),
	}))
}

func subgraphProjectionError(t *testing.T) error {
	t.Helper()
	body := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) { return store, nil },
	)
	return runError(t, workflow.Subgraph(workflow.SubgraphConfig{
		ID:         "subgraph",
		Body:       body,
		BodyOutput: workflow.Output("missing"),
	}))
}

func iterationProjectionError(t *testing.T) error {
	t.Helper()
	body := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) { return store, nil },
	)
	step := workflow.Iteration(workflow.IterationConfig{
		ID:         "iteration",
		Input:      workflow.Output("items"),
		Body:       body,
		BodyOutput: workflow.Output("missing"),
	})
	_, err := step.Run(t.Context(), workflow.NewStore().WithOutput("items", []int{1}))
	return err
}

func iterationBodyError(t *testing.T) error {
	t.Helper()
	body := workflow.LeafFunc("inner", workflow.Item("iteration"),
		func(_ context.Context, _ int) (int, error) {
			return 0, errors.New("body failed")
		})
	step := workflow.Iteration(workflow.IterationConfig{
		ID:         "iteration",
		Input:      workflow.Output("items"),
		Body:       body,
		BodyOutput: workflow.Output("inner"),
	})
	_, err := step.Run(t.Context(), workflow.NewStore().WithOutput("items", []int{1}))
	return err
}

func runError(t *testing.T, step workflow.Step) error {
	t.Helper()
	_, err := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{})
	return err
}

func passthrough(id string) workflow.Step {
	return workflow.LeafFunc(id, workflow.Output("seed"),
		func(_ context.Context, value int) (int, error) { return value, nil })
}

// recordedDuringARun reaches ErrJournalConflict the one way a single run can:
// a record created after the run began is deliberately not replayed, so the
// step it names still executes and then collides on its own identity. That is
// the hazard Journal documents for a host that admits two runs at once.
func recordedDuringARun(t *testing.T) error {
	t.Helper()
	journal := workflow.NewJournal()
	claimTarget := workflow.LeafFunc("claim", workflow.Output("seed"),
		func(_ context.Context, value int) (int, error) {
			if err := journal.Record(workflow.JournalKey{ID: "target"}, value); err != nil {
				t.Errorf("Record: %v", err)
			}
			return value, nil
		})
	_, err := workflow.Run(
		t.Context(),
		workflow.Sequence(claimTarget, passthrough("target")),
		workflow.NewStore().WithOutput("seed", 1),
		workflow.RunConfig{Journal: journal},
	)
	return err
}

// TestAJoinedSuspensionNamesThePackageOncePerWait extends the rule above to the
// one error this package builds out of several independent ones. Every wait
// names itself, so the envelope that counts them must not: a fan-out of three
// should read as three suspensions, not four mentions of this package.
func TestAJoinedSuspensionNamesThePackageOncePerWait(t *testing.T) {
	for count := 1; count <= 3; count++ {
		waits := make([]*workflow.Suspension, count)
		for index := range waits {
			waits[index] = &workflow.Suspension{ID: string(rune('a' + index))}
		}
		err := workflow.JoinSuspensions(waits...)
		if err == nil {
			t.Fatalf("%d waits joined to nothing", count)
		}
		if got := strings.Count(err.Error(), "workflow:"); got != count {
			t.Fatalf("%d waits name the package %d times: %v", count, got, err)
		}
	}
}

// TestUnknownNodeTypeIsMatchableOnEveryRouteThatReportsIt holds one category to
// the axiom that the construction routes agree: a caller who switched from a Spec
// to a Graph, or from validating to compiling, must still recognize the same
// failure. ErrUnknownNodeType is reported from three places, and only the Spec
// validator's was matched -- replacing the %w with a %v at the other two changed
// no message and failed no test, because a wrap is a promise only where something
// matches through it.
//
// errors_test.go's own table is why that reads as covered when it is not: it
// builds a GraphError around this sentinel by hand to check how a location prints,
// which says nothing about whether the path that reports it still carries the
// category.
func TestUnknownNodeTypeIsMatchableOnEveryRouteThatReportsIt(t *testing.T) {
	registry := workflow.NewRegistry()
	graph := workflow.Graph{Nodes: []workflow.GraphNode{{ID: "a", Type: "nope"}}}
	spec := workflow.Spec{Kind: workflow.KindLeaf, ID: "a", Type: "nope"}
	routes := map[string]func() error{
		"ValidateGraph": func() error { return registry.ValidateGraph(graph) },
		"CompileGraph":  func() error { _, err := registry.CompileGraph(graph); return err },
		"ValidateSpec":  func() error { return registry.ValidateSpec(spec) },
		"CompileSpec":   func() error { _, err := registry.CompileSpec(spec); return err },
	}
	for _, name := range slices.Sorted(maps.Keys(routes)) {
		err := routes[name]()
		if !errors.Is(err, workflow.ErrUnknownNodeType) {
			t.Errorf("%s error = %v; want it to match ErrUnknownNodeType", name, err)
		}
	}
}

// TestAStoredNilIsATypeErrorNotAnAbsence pins which half of RefError's documented
// Unwrap a stored untyped nil is. Both halves are one sentence -- "Unwrap returns
// [ErrNotFound] or [ErrTypeMismatch]" -- and replacing this branch's
// ErrTypeMismatch with an error carrying the same text failed no test, while doing
// that to the ErrNotFound beside it failed two examples immediately.
//
// The category is not decoration here. FirstOf skips a reference only when it is
// absent, so a cell holding nil read as absence would move the search on to a later
// reference and hand back a value the caller wired for a different case.
func TestAStoredNilIsATypeErrorNotAnAbsence(t *testing.T) {
	store := workflow.NewStore().
		WithOutput("empty", nil).
		WithOutput("later", 7)

	_, err := store.Get[int](workflow.Output("empty"))
	if !errors.Is(err, workflow.ErrTypeMismatch) {
		t.Fatalf("Get[int] of a nil cell = %v; want ErrTypeMismatch", err)
	}
	if errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("Get[int] of a nil cell = %v; want it not to read as absence", err)
	}

	value, err := workflow.FirstOf[int](
		workflow.Output("empty"),
		workflow.Output("later"),
	).Bind(store)
	if !errors.Is(err, workflow.ErrTypeMismatch) || value != 0 {
		t.Fatalf("FirstOf past a nil cell = %d, %v; want 0, ErrTypeMismatch", value, err)
	}
}
