package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestSequence_threadsStore(t *testing.T) {
	double := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil })
	inc := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })

	step1 := workflow.Leaf("double", workflow.Ref{NodeID: "start", Path: "/output"}.Bind[int](), double)
	step2 := workflow.Leaf("inc", workflow.Output("double").Bind[int](), inc)

	flow := workflow.Sequence(step1, step2)

	in := workflow.NewStore().WithOutput("start", 5)
	out, err := flow.Run(t.Context(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v, ok := out.Lookup(workflow.Output("inc")); !ok || v.(int) != 11 {
		t.Fatalf("final output = %v, %v; want 11", v, ok) // 5*2=10, +1=11
	}
	// Intermediate output is retained (snapshot semantics).
	if v, ok := out.Lookup(workflow.Output("double")); !ok || v.(int) != 10 {
		t.Fatalf("intermediate output = %v, %v; want 10", v, ok)
	}
}

func TestSequence_preservesAChildStoreOnErrorAcrossNesting(t *testing.T) {
	boom := errors.New("step failed")
	write := func(id string, err error) workflow.Step {
		return flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				return store.WithOutput(id, true), err
			},
		)
	}
	first := write("first", nil)
	failing := write("failing", boom)
	last := write("last", nil)

	steps := map[string]workflow.Step{
		"flat":   workflow.Sequence(first, failing, last),
		"nested": workflow.Sequence(workflow.Sequence(first, failing), last),
	}
	for name, step := range steps {
		t.Run(name, func(t *testing.T) {
			output, err := step.Run(t.Context(), workflow.NewStore())
			if !errors.Is(err, boom) {
				t.Fatalf("Run error = %v; want step failure", err)
			}
			for _, id := range []string{"first", "failing"} {
				value, getErr := output.Get[bool](workflow.Output(id))
				if getErr != nil || !value {
					t.Fatalf("output %q = %v, %v; want true, nil", id, value, getErr)
				}
			}
			if _, ok := output.Lookup(workflow.Output("last")); ok {
				t.Fatal("Sequence ran a child after an error")
			}
		})
	}
}

func TestLeaf_missingInput(t *testing.T) {
	leaf := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })
	step := workflow.Leaf("n", workflow.Ref{NodeID: "absent", Path: "/output"}.Bind[int](), leaf)

	if _, err := step.Run(t.Context(), workflow.NewStore()); err == nil {
		t.Fatal("expected error for missing input")
	}
}

func TestLeafFunc_andFirstOf(t *testing.T) {
	refs := []workflow.Ref{workflow.Output("missing"), workflow.Output("input")}
	step := workflow.LeafFunc(
		"double",
		refs[0],
		func(_ context.Context, value int) (int, error) {
			return value * 2, nil
		},
	)
	if _, err := step.Run(
		t.Context(),
		workflow.NewStore().WithOutput("input", 21),
	); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("LeafFunc missing input error = %v; want ErrNotFound", err)
	}

	bind := workflow.FirstOf[int](refs...)
	refs[1] = workflow.Output("changed")
	value, err := bind.Bind(workflow.NewStore().WithOutput("input", 21))
	if err != nil || value != 21 {
		t.Fatalf("FirstOf = %d, %v; want 21, nil", value, err)
	}

	out, err := workflow.Leaf(
		"double",
		bind,
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value * 2, nil
		}),
	).Run(t.Context(), workflow.NewStore().WithOutput("input", 21))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := out.Get[int](workflow.Output("double")); err != nil || got != 42 {
		t.Fatalf("output = %d, %v; want 42, nil", got, err)
	}
}

func TestSequence_ownsStepSliceStructure(t *testing.T) {
	steps := []workflow.Step{workflow.LeafFunc(
		"original",
		workflow.Output("input"),
		func(_ context.Context, value int) (int, error) { return value + 1, nil },
	)}
	sequence := workflow.Sequence(steps...)
	steps[0] = workflow.LeafFunc(
		"changed",
		workflow.Output("input"),
		func(_ context.Context, value int) (int, error) { return value + 100, nil },
	)

	out, err := sequence.Run(t.Context(), workflow.NewStore().WithOutput("input", 1))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, getErr := out.Get[int](workflow.Output("original")); getErr != nil || got != 2 {
		t.Fatalf("original output = %d, %v; want 2, nil", got, getErr)
	}
	if _, ok := out.Lookup(workflow.Output("changed")); ok {
		t.Fatal("source-slice mutation reconfigured Sequence")
	}
}

func TestFirstOf_reportsConversionAndMissingErrors(t *testing.T) {
	bind := workflow.FirstOf[int](
		workflow.Output("wrong"),
		workflow.Output("valid"),
	)
	store := workflow.NewStore().
		WithOutput("wrong", "not an integer").
		WithOutput("valid", 42)
	if _, err := bind.Bind(store); !errors.Is(err, workflow.ErrTypeMismatch) {
		t.Fatalf("conversion error = %v; want ErrTypeMismatch", err)
	}
	if _, err := workflow.FirstOf[int]().Bind(store); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatalf("empty FirstOf error = %v; want ErrNotFound", err)
	}
}

func TestLeaf_propagatesLeafError(t *testing.T) {
	boom := errors.New("boom")
	leaf := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	step := workflow.Leaf("n", workflow.Ref{NodeID: "start", Path: "/output"}.Bind[int](), leaf)

	_, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
}

func TestLeaf_errorIncludesStepAndOperation(t *testing.T) {
	boom := errors.New("boom")
	step := workflow.Leaf("load",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 0, boom }),
	)

	_, err := step.Run(t.Context(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "load" || stepErr.Op != workflow.OpRun || !errors.Is(err, boom) {
		t.Fatalf("err = %v; want load/run StepError", err)
	}
}

func TestSequence_empty(t *testing.T) {
	s := workflow.NewStore().WithOutput("x", 1)

	out, err := workflow.Sequence().Run(t.Context(), s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Lookup(workflow.At("x", "output")); !ok || v.(int) != 1 {
		t.Fatalf("empty sequence should pass the store through, got %v, %v", v, ok)
	}
}

func TestSequence_singleNilStep(t *testing.T) {
	_, err := workflow.Sequence(nil).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
}

func TestSequence_validatesEveryStepBeforeRunning(t *testing.T) {
	ran := false
	first := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		ran = true
		return store, nil
	})
	_, err := workflow.Sequence(first, nil).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 1 {
		t.Fatalf("err = %v; want IndexError at step 1", err)
	}
	if ran {
		t.Fatal("first step ran before the invalid sequence was rejected")
	}
}

func TestSequence_rejectsTypedNilFunctionStepBeforeRunning(t *testing.T) {
	ran := false
	first := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			ran = true
			return store, nil
		},
	)
	var invalid flow.NodeFunc[workflow.Store, workflow.Store]

	_, err := workflow.Sequence(first, invalid).
		Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 1 {
		t.Fatalf("err = %v; want IndexError at step 1", err)
	}
	if ran {
		t.Fatal("first step ran before the typed nil function step was rejected")
	}
}

type nilSafeStep struct{}

func (*nilSafeStep) Run(
	_ context.Context,
	store workflow.Store,
) (workflow.Store, error) {
	return store.WithOutput("nil-safe", true), nil
}

func TestSequence_acceptsNilSafePointerStep(t *testing.T) {
	var step *nilSafeStep
	output, err := workflow.Sequence(step).Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, getErr := output.Get[bool](workflow.Output("nil-safe")); getErr != nil || !value {
		t.Fatalf("nil-safe output = %v, %v; want true, nil", value, getErr)
	}
}

type nilSafeStepFunc func()

func (nilSafeStepFunc) Run(
	_ context.Context,
	store workflow.Store,
) (workflow.Store, error) {
	return store.WithOutput("nil-safe-func", true), nil
}

func TestSequence_acceptsNilSafeFunctionStep(t *testing.T) {
	var step nilSafeStepFunc
	output, err := workflow.Sequence(step).Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, getErr := output.Get[bool](workflow.Output("nil-safe-func")); getErr != nil || !value {
		t.Fatalf("nil-safe function output = %v, %v; want true, nil", value, getErr)
	}
}

type invalidValidatingStep struct {
	err   error
	calls *int
}

func (i invalidValidatingStep) Validate() error { return i.err }

func (i invalidValidatingStep) Run(
	_ context.Context,
	store workflow.Store,
) (workflow.Store, error) {
	(*i.calls)++
	return store, nil
}

func TestRun_honorsVisibleValidationBeforeCallingStep(t *testing.T) {
	invalid := errors.New("invalid definition")
	var calls int
	step := invalidValidatingStep{err: invalid, calls: &calls}

	_, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore(),
		workflow.RunConfig{},
	)
	if !errors.Is(err, invalid) {
		t.Fatalf("Run error = %v; want validation error", err)
	}
	if calls != 0 {
		t.Fatalf("step calls = %d; want 0", calls)
	}
}

func TestCompositesHonorCallerDefinedValidation(t *testing.T) {
	invalid := errors.New("invalid definition")
	resolver := resolverNode(func(context.Context, workflow.Store) (string, error) {
		return "case", nil
	})
	condition := flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) {
		return true, nil
	})
	tests := map[string]func(workflow.Step) workflow.Step{
		"sequence": func(body workflow.Step) workflow.Step {
			return workflow.Sequence(body)
		},
		"parallel": func(body workflow.Step) workflow.Step {
			return workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{body}})
		},
		"branch": func(body workflow.Step) workflow.Step {
			return workflow.Branch(workflow.BranchConfig{
				ID:       "branch",
				Resolver: resolver,
				Cases:    map[string]workflow.Step{"case": body},
			})
		},
		"loop": func(body workflow.Step) workflow.Step {
			return workflow.Loop(workflow.LoopConfig{ID: "loop", Body: body, Condition: condition})
		},
		"iteration": func(body workflow.Step) workflow.Step {
			return workflow.Iteration(workflow.IterationConfig{
				ID:         "iteration",
				Input:      workflow.Output("items"),
				Body:       body,
				BodyOutput: workflow.Output("result"),
			})
		},
		"subgraph": func(body workflow.Step) workflow.Step {
			return workflow.Subgraph(workflow.SubgraphConfig{
				ID:         "subgraph",
				Body:       body,
				BodyOutput: workflow.Output("result"),
			})
		},
	}

	for name, wrap := range tests {
		t.Run(name, func(t *testing.T) {
			calls := 0
			body := invalidValidatingStep{err: invalid, calls: &calls}
			_, err := wrap(body).Run(t.Context(), workflow.NewStore())
			if !errors.Is(err, invalid) {
				t.Fatalf("Run error = %v; want caller validation error", err)
			}
			if calls != 0 {
				t.Fatalf("body calls = %d; want 0", calls)
			}
		})
	}
}

func TestNestedRun_startsAtTheRootWorkflowScope(t *testing.T) {
	var nestedErr error
	outer := flow.NodeFunc[workflow.Store, workflow.Store](
		func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
			_, nestedErr = workflow.Run(
				workflow.WithScope(ctx, "outer"),
				workflow.Interrupt("wait", "question"),
				workflow.NewStore(),
				workflow.RunConfig{},
			)
			return store, nil
		},
	)
	_, err := workflow.Run(
		t.Context(),
		outer,
		workflow.NewStore(),
		workflow.RunConfig{},
	)
	if err != nil {
		t.Fatalf("outer Run: %v", err)
	}
	waits := workflow.Suspensions(nestedErr)
	if len(waits) != 1 || waits[0].Scope != nil {
		t.Fatalf("nested Run suspensions = %+v; want one root-scoped wait", waits)
	}
}

func TestNestedRun_rootSuspensionIdentitySurvivesOuterLeaf(t *testing.T) {
	journal := workflow.NewJournal()
	outer := workflow.Leaf(
		"outer",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(ctx context.Context, _ int) (int, error) {
			result, err := workflow.Run(
				ctx,
				workflow.Interrupt("inner", "question"),
				workflow.NewStore(),
				workflow.RunConfig{Journal: journal},
			)
			if err != nil {
				return 0, err
			}
			return result.Get[int](workflow.Output("inner"))
		}),
	)
	ctx := workflow.WithScope(t.Context(), "outer-scope")

	_, err := workflow.Run(ctx, outer, workflow.NewStore(), workflow.RunConfig{})
	waits := workflow.Suspensions(err)
	if len(waits) != 1 || waits[0].ID != "inner" || waits[0].Scope != nil {
		t.Fatalf("Suspensions = %+v; want inner at the nested Run root", waits)
	}
	if recordErr := journal.Record(waits[0].Key(), 42); recordErr != nil {
		t.Fatalf("Record nested result: %v", recordErr)
	}

	result, err := workflow.Run(ctx, outer, workflow.NewStore(), workflow.RunConfig{})
	if err != nil {
		t.Fatalf("resumed Run: %v", err)
	}
	if value, getErr := result.Get[int](workflow.Output("outer")); getErr != nil || value != 42 {
		t.Fatalf("outer output = %d, %v; want 42, nil", value, getErr)
	}
}

func TestDirectStepRun_preservesCallerCompositeScope(t *testing.T) {
	step := workflow.Interrupt("wait", "question")
	_, err := step.Run(
		workflow.WithScope(t.Context(), "outer"),
		workflow.NewStore(),
	)
	waits := workflow.Suspensions(err)
	if len(waits) != 1 || !slices.Equal(waits[0].Scope, []workflow.ScopeFrame{{ID: "outer"}}) {
		t.Fatalf("direct Run suspensions = %+v; want scope [outer]", waits)
	}
}

type nilSafeBinder struct{}

func (*nilSafeBinder) Bind(workflow.Store) (int, error) { return 21, nil }

func TestLeaf_acceptsNilSafePointerBinder(t *testing.T) {
	var bind *nilSafeBinder
	node := flow.NodeFunc[int, int](
		func(_ context.Context, value int) (int, error) { return value * 2, nil },
	)

	out, err := workflow.Leaf("double", bind, node).
		Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, getErr := out.Get[int](workflow.Output("double")); getErr != nil || got != 42 {
		t.Fatalf("output = %d, %v; want 42, nil", got, getErr)
	}
}

type nilSafeBinderFunc func()

func (nilSafeBinderFunc) Bind(workflow.Store) (int, error) { return 21, nil }

func TestLeaf_acceptsNilSafeFunctionBinder(t *testing.T) {
	var bind nilSafeBinderFunc
	step := workflow.Leaf(
		"double",
		bind,
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value * 2, nil
		}),
	)

	output, err := step.Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if value, getErr := output.Get[int](workflow.Output("double")); getErr != nil || value != 42 {
		t.Fatalf("output = %d, %v; want 42, nil", value, getErr)
	}
}

type invalidBinder struct {
	err   error
	calls *int
}

func (i invalidBinder) Validate() error { return i.err }

func (i invalidBinder) Bind(workflow.Store) (int, error) {
	(*i.calls)++
	return 0, nil
}

type invalidLeafNode struct {
	err   error
	calls *int
}

func (i invalidLeafNode) Validate() error { return i.err }

func (i invalidLeafNode) Run(context.Context, int) (int, error) {
	(*i.calls)++
	return 0, nil
}

func TestLeaf_definitionFailuresUseValidateOperation(t *testing.T) {
	invalid := errors.New("invalid definition")
	var nilBinderCalls, invalidBinderCalls, nilNodeCalls, invalidNodeCalls int
	for name, test := range map[string]struct {
		step      workflow.Step
		component string
		want      error
		calls     *int
	}{
		"nil binder": {
			step: workflow.Leaf(
				"leaf",
				workflow.BinderFunc[int](nil),
				flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
					nilBinderCalls++
					return 0, nil
				}),
			),
			component: "binder",
			want:      flow.ErrNilFunc,
			calls:     &nilBinderCalls,
		},
		"invalid binder": {
			step: workflow.Leaf(
				"leaf",
				invalidBinder{err: invalid, calls: &invalidBinderCalls},
				flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
					invalidBinderCalls++
					return 0, nil
				}),
			),
			component: "binder",
			want:      invalid,
			calls:     &invalidBinderCalls,
		},
		"nil node": {
			step: workflow.Leaf(
				"leaf",
				workflow.BinderFunc[int](func(workflow.Store) (int, error) {
					nilNodeCalls++
					return 0, nil
				}),
				flow.NodeFunc[int, int](nil),
			),
			component: "node",
			want:      flow.ErrNilNode,
			calls:     &nilNodeCalls,
		},
		"invalid node": {
			step: workflow.Leaf(
				"leaf",
				workflow.BinderFunc[int](func(workflow.Store) (int, error) {
					invalidNodeCalls++
					return 0, nil
				}),
				invalidLeafNode{err: invalid, calls: &invalidNodeCalls},
			),
			component: "node",
			want:      invalid,
			calls:     &invalidNodeCalls,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := test.step.Run(t.Context(), workflow.NewStore())
			var stepErr *workflow.StepError
			if !errors.Is(err, test.want) ||
				!errors.As(err, &stepErr) ||
				stepErr.Op != workflow.OpValidate ||
				!strings.Contains(stepErr.Err.Error(), test.component) {
				t.Fatalf(
					"Run error = %v; want %s OpValidate wrapping %v",
					err,
					test.component,
					test.want,
				)
			}
			if *test.calls != 0 {
				t.Fatalf("definition failure invoked application code %d times", *test.calls)
			}
		})
	}
}

func TestLeaf_preservesJoinedNodeValidationCauses(t *testing.T) {
	first := errors.New("first invalid case")
	second := errors.New("second invalid case")
	var firstCalls, secondCalls int
	node := flow.Switch(
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 0, nil }),
		map[int]flow.Node[int, int]{
			0: invalidLeafNode{err: first, calls: &firstCalls},
			1: invalidLeafNode{err: second, calls: &secondCalls},
		},
	)
	step := workflow.Leaf(
		"leaf",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		node,
	)

	_, err := step.Run(t.Context(), workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.Is(err, first) || !errors.Is(err, second) ||
		!errors.As(err, &stepErr) || stepErr.Op != workflow.OpValidate {
		t.Fatalf("Run error = %v; want OpValidate preserving both case errors", err)
	}
	if firstCalls != 0 || secondCalls != 0 {
		t.Fatalf("invalid cases ran %d and %d times; want neither", firstCalls, secondCalls)
	}
}

func TestLeaf_validatesCustomBinderBeforeJournalReplay(t *testing.T) {
	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "broken"}, 42); err != nil {
		t.Fatalf("Record: %v", err)
	}
	invalid := errors.New("invalid binder")
	var bindCalls, nodeCalls int
	step := workflow.Leaf(
		"broken",
		invalidBinder{err: invalid, calls: &bindCalls},
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
			nodeCalls++
			return 0, nil
		}),
	)

	_, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore(),
		workflow.RunConfig{Journal: journal},
	)
	if !errors.Is(err, invalid) || !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("Run error = %v; want binder error and ErrInvalidConfig", err)
	}
	if bindCalls != 0 || nodeCalls != 0 {
		t.Fatalf("calls = bind %d, node %d; want 0, 0", bindCalls, nodeCalls)
	}
}

func TestLeaf_rejectsEmptyIDAndNilBind(t *testing.T) {
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil })
	if _, err := workflow.Leaf("", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }), node).
		Run(t.Context(), workflow.NewStore()); !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("empty ID err = %v", err)
	}
	if _, err := workflow.Leaf("x", nil, node).
		Run(t.Context(), workflow.NewStore()); !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("nil binder err = %v", err)
	}
	var bind workflow.BinderFunc[int]
	if _, err := bind.Bind(workflow.NewStore()); !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("nil BinderFunc.Bind err = %v", err)
	}
	if _, err := workflow.Leaf("x", bind, node).
		Run(t.Context(), workflow.NewStore()); !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("nil BinderFunc err = %v", err)
	}
}

func TestSequence_rejectsNonUTF8StepIDBeforeWork(t *testing.T) {
	var calls int
	node := func(id string) workflow.Step {
		return workflow.Leaf(
			id,
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				calls++
				return 1, nil
			}),
		)
	}
	invalid := string([]byte{0xff})
	_, err := workflow.Sequence(node("valid"), node(invalid)).Run(
		t.Context(),
		workflow.NewStore(),
	)
	if !errors.Is(err, workflow.ErrInvalidStepID) ||
		!strings.Contains(err.Error(), "not valid UTF-8") {
		t.Fatalf("Run error = %v; want non-UTF-8 ErrInvalidStepID", err)
	}
	if calls != 0 {
		t.Fatalf("node calls = %d; want definition rejection before work", calls)
	}
}

func TestSequence_enforcesDefinitionNestingLimit(t *testing.T) {
	step := workflow.Leaf(
		"leaf",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value, nil
		}),
	)
	for range workflow.MaxNestingDepth {
		step = workflow.Sequence(step)
	}
	if _, err := step.Run(t.Context(), workflow.NewStore()); err != nil {
		t.Fatalf("Run at nesting limit: %v", err)
	}
	step = workflow.Sequence(step)

	if _, err := step.Run(
		t.Context(),
		workflow.NewStore(),
	); !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("err = %v; want ErrMaxDepth", err)
	}
}

// TestDefinitionNestingLimitCountsEveryCompositeOnce pins the level each composite
// kind adds to the nesting depth. Every kind counts its own in its own line, and a
// sequence was the only shape the limit had been measured on -- so one that counted
// twice would halve the limit for that shape alone, and nothing would say so. Each
// case here puts a single composite at the bottom of a stack of sequences, which
// measures that one kind's level without building a thousand of them.
func TestDefinitionNestingLimitCountsEveryCompositeOnce(t *testing.T) {
	body := leafStep("body")
	graph, err := workflow.NewRegistry().
		MustRegisterNode("noop", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Interrupt(spec.ID, nil), nil
		}).
		CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{ID: "node", Type: "noop"}}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	composites := map[string]workflow.Step{
		"branch": workflow.Branch(workflow.BranchConfig{
			ID: "branch",
			Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
				return "case", nil
			}),
			Cases: map[string]workflow.Step{"case": body},
		}),
		"loop": workflow.Loop(workflow.LoopConfig{
			ID:   "loop",
			Body: body,
			Condition: flow.NodeFunc[workflow.Store, bool](
				func(context.Context, workflow.Store) (bool, error) { return true, nil },
			),
		}),
		"iteration": workflow.Iteration(workflow.IterationConfig{
			ID:         "each",
			Input:      workflow.Output("items"),
			Body:       body,
			BodyOutput: workflow.Output("body"),
		}),
		"subgraph": workflow.Subgraph(workflow.SubgraphConfig{
			ID:         "sub",
			Inputs:     workflow.Inputs{"seed": workflow.Output("items")},
			Body:       body,
			BodyOutput: workflow.Output("body"),
		}),
		// A compiled graph's children are its nodes, which sit one level below it
		// like any other composite's.
		"graph": graph,
	}
	nest := func(step workflow.Step, levels int) workflow.Step {
		for range levels {
			step = workflow.Sequence(step)
		}
		return step
	}

	for name, composite := range composites {
		t.Run(name, func(t *testing.T) {
			// The composite's own child sits one level below it, so a stack this tall
			// puts that child exactly at the limit.
			if err := flow.Validate(nest(composite, workflow.MaxNestingDepth-1)); err != nil {
				t.Fatalf("Validate at the nesting limit: %v", err)
			}
			if err := flow.Validate(nest(composite, workflow.MaxNestingDepth)); !errors.Is(err, workflow.ErrMaxDepth) {
				t.Fatalf("Validate above the nesting limit = %v; want ErrMaxDepth", err)
			}
		})
	}
}

// TestValidate_rejectsAStepReusingAnIDABranchCaseIntroduced covers the half of the
// case-ID rule that reaches past the branch. Cases may reuse IDs with one another,
// because only one of them runs; what none of them may do is take an ID a step after
// the branch also uses, since the two would both run. Only a step after the branch
// can say that -- inside it, reuse is the point.
func TestValidate_rejectsAStepReusingAnIDABranchCaseIntroduced(t *testing.T) {
	branch := workflow.Branch(workflow.BranchConfig{
		ID: "route",
		Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
			return "first", nil
		}),
		Cases: map[string]workflow.Step{
			"first":  leafStep("inner"),
			"second": leafStep("inner"),
		},
	})

	// The cases alone are legal: each runs alone, so both may own "inner".
	if err := flow.Validate(branch); err != nil {
		t.Fatalf("Validate cases reusing an ID with each other: %v", err)
	}
	after := workflow.Sequence(branch, leafStep("inner"))
	if err := flow.Validate(after); !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("Validate error = %v; want ErrDuplicateStep", err)
	}
}

// TestValidate_rejectsACompositeReusingAnEarlierStepID covers the claim a Branch
// and a Loop make for their own ID against the steps around them. Running either
// reports the collision too — the run claims each ID as it reaches it — so, as with
// a case colliding with its branch, the definition check is observable only before
// execution, which is where a compiled workflow asks. See
// TestBranch_rejectsACaseIDCollisionBeforeRunning for the same rule one level in.
func TestValidate_rejectsACompositeReusingAnEarlierStepID(t *testing.T) {
	for name, composite := range map[string]workflow.Step{
		"branch": workflow.Branch(workflow.BranchConfig{
			ID: "taken",
			Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
				return "case", nil
			}),
			Cases: map[string]workflow.Step{"case": leafStep("case")},
		}),
		"loop": workflow.Loop(workflow.LoopConfig{
			ID:   "taken",
			Body: leafStep("body"),
			Condition: flow.NodeFunc[workflow.Store, bool](
				func(context.Context, workflow.Store) (bool, error) { return true, nil },
			),
		}),
	} {
		t.Run(name, func(t *testing.T) {
			step := workflow.Sequence(workflow.Interrupt("taken", nil), composite)
			if err := flow.Validate(step); !errors.Is(err, workflow.ErrDuplicateStep) {
				t.Fatalf("Validate error = %v; want ErrDuplicateStep", err)
			}
		})
	}
}

func TestWorkflowDefinitionsParticipateInFlowValidation(t *testing.T) {
	var calls int
	first := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			calls++
			return store, nil
		},
	)
	invalid := workflow.Sequence(workflow.Interrupt("", nil))
	composed := flow.Then(first, invalid)

	if err := flow.Validate(composed); !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("Validate error = %v; want ErrInvalidStepID", err)
	}
	if _, err := composed.Run(t.Context(), workflow.NewStore()); !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("Run error = %v; want ErrInvalidStepID", err)
	}
	if calls != 0 {
		t.Fatalf("first node calls = %d; want 0", calls)
	}
}

func TestLeaf_validatesDefinitionBeforeJournalReplay(t *testing.T) {
	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "broken"}, 42); err != nil {
		t.Fatalf("Record: %v", err)
	}
	bind := workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil })
	step := workflow.Leaf[int, int]("broken", bind, nil)

	if _, err := workflow.Run(t.Context(), step, workflow.NewStore(),
		workflow.RunConfig{Journal: journal}); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode instead of Journal replay", err)
	}
}

func TestLeaf_validatesNilNodeFuncBeforeJournalReplay(t *testing.T) {
	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "broken"}, 42); err != nil {
		t.Fatalf("Record: %v", err)
	}
	bind := workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil })
	var node flow.NodeFunc[int, int]
	step := workflow.Leaf("broken", bind, node)

	if _, err := workflow.Run(t.Context(), step, workflow.NewStore(),
		workflow.RunConfig{Journal: journal}); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode instead of Journal replay", err)
	}
}

func TestLeaf_validatesComposedNodeBeforeJournalReplay(t *testing.T) {
	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "broken"}, 42); err != nil {
		t.Fatalf("Record: %v", err)
	}
	var calls int
	first := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		calls++
		return 1, nil
	})
	var second flow.NodeFunc[int, int]
	step := workflow.Leaf(
		"broken",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.Then(first, second),
	)

	_, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore(),
		workflow.RunConfig{Journal: journal},
	)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("Run error = %v; want ErrNilNode before Journal replay", err)
	}
	if calls != 0 {
		t.Fatalf("first node calls = %d; want 0", calls)
	}
}

func TestLeaf_validatesBuiltinBinderBeforeJournalReplay(t *testing.T) {
	tests := []struct {
		name string
		bind workflow.Binder[int]
	}{
		{name: "Ref.Bind", bind: workflow.Ref{}.Bind[int]()},
		{name: "empty FirstOf", bind: workflow.FirstOf[int]()},
		{
			name: "FirstOf",
			bind: workflow.FirstOf[int](
				workflow.Output("valid"),
				workflow.Ref{NodeID: "invalid", Path: "not-a-pointer"},
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			journal := workflow.NewJournal()
			if err := journal.Record(workflow.JournalKey{ID: "broken"}, 42); err != nil {
				t.Fatalf("Record: %v", err)
			}
			var calls int
			step := workflow.Leaf(
				"broken",
				test.bind,
				flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
					calls++
					return 0, nil
				}),
			)

			_, err := workflow.Run(
				t.Context(),
				step,
				workflow.NewStore().WithOutput("valid", 1),
				workflow.RunConfig{Journal: journal},
			)
			var stepErr *workflow.StepError
			if !errors.As(err, &stepErr) || stepErr.Op != workflow.OpValidate ||
				!errors.Is(err, flow.ErrInvalidConfig) {
				t.Fatalf("Run error = %v; want validate ErrInvalidConfig", err)
			}
			if calls != 0 {
				t.Fatalf("node calls = %d; want definition rejection before replay", calls)
			}
		})
	}
}

func TestLeaf_acceptsCustomBindFunc(t *testing.T) {
	node := flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in * 2, nil })
	// A custom binder is just a BinderFunc; this one ignores the store.
	bind := workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 21, nil })
	out, err := workflow.Leaf("double", bind, node).Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got, err := out.Get[int](workflow.Output("double"))
	if err != nil || got != 42 {
		t.Fatalf("Get = %d, %v; want 42, nil", got, err)
	}
}

func TestLeaf_rejectsExcessiveExecutionScopeDepth(t *testing.T) {
	ctx := t.Context()
	for index := range workflow.MaxNestingDepth + 1 {
		ctx = workflow.WithScope(ctx, fmt.Sprintf("scope-%d", index))
	}
	step := workflow.Leaf(
		"leaf",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
			return 1, nil
		}),
	)
	_, err := workflow.Run(
		ctx,
		step,
		workflow.NewStore(),
		workflow.RunConfig{},
	)
	if !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("error = %v; want ErrMaxDepth", err)
	}
}

// TestScopedComposite_admitsTheDeepestScopeItCanPush pins where a composite's own
// depth boundary is. It counts the frame it is about to add, so an inherited scope
// one short of the limit is the deepest one whose body can still run. Only the
// accepting side says that: above the limit, rejection reads the same whether the
// composite counts one frame or two.
func TestScopedComposite_admitsTheDeepestScopeItCanPush(t *testing.T) {
	ctx := t.Context()
	for index := range workflow.MaxNestingDepth - 1 {
		ctx = workflow.WithScope(ctx, fmt.Sprintf("scope-%d", index))
	}
	step := workflow.Loop(workflow.LoopConfig{
		ID: "loop",
		Body: workflow.Leaf(
			"body",
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil }),
		),
		Condition: flow.NodeFunc[workflow.Store, bool](
			func(context.Context, workflow.Store) (bool, error) { return true, nil },
		),
	})

	out, err := workflow.Run(ctx, step, workflow.NewStore(), workflow.RunConfig{})
	if err != nil {
		t.Fatalf("Run at the deepest admissible scope: %v", err)
	}
	if got, err := out.Get[int](workflow.Output("body")); err != nil || got != 1 {
		t.Fatalf("body output = %d, %v; want 1", got, err)
	}
}

func TestGet_nilValue(t *testing.T) {
	store := workflow.NewStore().WithCell("n", "value", nil)
	ref := workflow.At("n", "value")

	if got, err := store.Get[any](ref); err != nil || got != nil {
		t.Fatalf("Get[any](nil) = %v, %v", got, err)
	}
	if got, err := store.Get[*int](ref); err != nil || got != nil {
		t.Fatalf("Get[*int](nil) = %v, %v", got, err)
	}
	if got, err := store.Get[chan int](ref); err != nil || got != nil {
		t.Fatalf("Get[chan int](nil) = %v, %v", got, err)
	}
	if got, err := store.Get[func()](ref); err != nil || got != nil {
		t.Fatalf("Get[func()](nil) returned non-nil function or error %v", err)
	}
	if got, err := store.Get[map[string]int](ref); err != nil || got != nil {
		t.Fatalf("Get[map[string]int](nil) = %v, %v", got, err)
	}
	if got, err := store.Get[[]int](ref); err != nil || got != nil {
		t.Fatalf("Get[[]int](nil) = %v, %v", got, err)
	}
	if got, err := store.Get[unsafe.Pointer](ref); err != nil || got != nil {
		t.Fatalf("Get[unsafe.Pointer](nil) = %v, %v", got, err)
	}
	if _, err := store.Get[int](ref); err == nil {
		t.Fatal("Get[int](nil) unexpectedly succeeded")
	}
}

// TestEveryCompositeRunDirectlyStillFormsOneRun states what a composite does
// before running anything: it installs a run when the caller has none. That run
// owns the set of identities claimed so far, and claiming against no run
// silently succeeds, so a composite invoked outside [workflow.Run] would let the
// same step ID run twice in one scope. Every other duplicate-ID test goes
// through Run, which installs the boundary itself and so would pass whether or
// not the composite did.
//
// Each body invokes one leaf twice through a flow combinator, for two reasons:
// a second workflow composite would install a run of its own and answer in place
// of the one under test, and two visible children with one ID are rejected by
// definition validation before anything runs, which is a different check.
func TestEveryCompositeRunDirectlyStillFormsOneRun(t *testing.T) {
	twice := func() workflow.Step {
		leaf := passthrough("same")
		return flow.Then(leaf, leaf)
	}
	stop := flow.NodeFunc[workflow.Store, bool](
		func(context.Context, workflow.Store) (bool, error) { return false, nil },
	)
	only := flow.NodeFunc[workflow.Store, string](
		func(context.Context, workflow.Store) (string, error) { return "only", nil },
	)

	composites := map[string]workflow.Step{
		"sequence": workflow.Sequence(twice()),
		"parallel": workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{twice()}}),
		"branch": workflow.Branch(workflow.BranchConfig{
			ID:       "branch",
			Resolver: only,
			Cases:    map[string]workflow.Step{"only": twice()},
		}),
		"loop": workflow.Loop(workflow.LoopConfig{
			ID:        "loop",
			Body:      twice(),
			Condition: stop,
		}),
		"iteration": workflow.Iteration(workflow.IterationConfig{
			ID:         "iteration",
			Input:      workflow.Output("items"),
			Body:       twice(),
			BodyOutput: workflow.Output("same"),
		}),
		// A subgraph body reads an isolated Store, so the leaf's input has to be
		// declared rather than inherited.
		"subgraph": workflow.Subgraph(workflow.SubgraphConfig{
			ID:         "subgraph",
			Inputs:     workflow.Inputs{"seed": workflow.Output("seed")},
			Body:       twice(),
			BodyOutput: workflow.Output("same"),
		}),
	}

	input := workflow.NewStore().WithOutput("seed", 1).WithOutput("items", []any{1})
	for name, composite := range composites {
		t.Run(name, func(t *testing.T) {
			if _, err := composite.Run(t.Context(), input); !errors.Is(err, workflow.ErrDuplicateStep) {
				t.Fatalf("Run outside a run = %v; want ErrDuplicateStep", err)
			}
		})
	}
}

// TestLeafRunDirectlyStillFormsOneRunForAComposedStep holds Leaf to the same
// identity boundary as every workflow composite. A Step is also a
// flow.Node[Store, Store], so it may be part of the typed Node a Leaf invokes;
// calling the Leaf directly must not make that nested identity disappear merely
// because package-level Run was omitted.
func TestLeafRunDirectlyStillFormsOneRunForAComposedStep(t *testing.T) {
	inner := passthrough("same")
	outer := workflow.Leaf(
		"same",
		workflow.BinderFunc[workflow.Store](func(store workflow.Store) (workflow.Store, error) {
			return store, nil
		}),
		inner,
	)

	_, err := outer.Run(
		t.Context(),
		workflow.NewStore().WithOutput("seed", 1),
	)
	if !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("direct Leaf Run = %v; want ErrDuplicateStep", err)
	}
}

// TestSequence_rejectsADuplicateOfAnyStepID covers the second half of how a
// definition tracks the IDs it has seen: it holds the first one alone and builds
// a set only once a second, different ID arrives. Two steps can therefore only
// ever collide with the first, so a definition of two says nothing about whether
// that set is ever built -- and without it, a later pair collides with nothing.
//
// That the duplicate is rejected is not enough to say so, because a run also
// claims each identity as it goes and would report the same failure once the
// first of the pair had run. Whether anything ran is what tells the two apart.
func TestSequence_rejectsADuplicateOfAnyStepID(t *testing.T) {
	ran := 0
	counted := func(id string) workflow.Step {
		return workflow.LeafFunc(id, workflow.Output("seed"),
			func(_ context.Context, value int) (int, error) {
				ran++
				return value, nil
			})
	}

	_, err := workflow.Run(
		t.Context(),
		workflow.Sequence(counted("first"), counted("same"), counted("same")),
		workflow.NewStore().WithOutput("seed", 1),
		workflow.RunConfig{},
	)
	if !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("Run = %v; want ErrDuplicateStep", err)
	}
	if ran != 0 {
		t.Fatalf("%d steps ran; want the definition rejected before any of them", ran)
	}
}

// TestEveryBuiltInStepReportsAnInvalidDefinitionThroughValidate asks each kind
// the question only Validate can answer. Every case in this repository reached a
// definition defect by running the step, and a run reports the same sentinel from
// a second boundary: an unusable ID is refused again when the run claims an
// identity, a nil child again when the composite invokes it. So a Validate that
// silently returned nil kept passing the whole suite while breaking the contract
// callers validate against — three kinds were in exactly that state.
func TestEveryBuiltInStepReportsAnInvalidDefinitionThroughValidate(t *testing.T) {
	body := workflow.Leaf(
		"body",
		workflow.Output("seed").Bind[int](),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value, nil
		}),
	)
	resolver := flow.NodeFunc[workflow.Store, string](
		func(context.Context, workflow.Store) (string, error) { return "only", nil },
	)
	stop := flow.NodeFunc[workflow.Store, bool](
		func(context.Context, workflow.Store) (bool, error) { return true, nil },
	)
	for name, testCase := range map[string]struct {
		step workflow.Step
		want error
	}{
		"leaf": {
			step: workflow.Leaf("", workflow.Output("seed").Bind[int](),
				flow.NodeFunc[int, int](func(_ context.Context, v int) (int, error) { return v, nil })),
			want: workflow.ErrInvalidStepID,
		},
		"await": {
			step: workflow.Await("", workflow.Output("approved")),
			want: workflow.ErrInvalidStepID,
		},
		"interrupt": {
			step: workflow.Interrupt("", "value"),
			want: workflow.ErrInvalidStepID,
		},
		"branch": {
			step: workflow.Branch(workflow.BranchConfig{
				Resolver: resolver,
				Cases:    map[string]workflow.Step{"only": body},
			}),
			want: workflow.ErrInvalidStepID,
		},
		"loop": {
			step: workflow.Loop(workflow.LoopConfig{Condition: stop, Body: body}),
			want: workflow.ErrInvalidStepID,
		},
		"iteration": {
			step: workflow.Iteration(workflow.IterationConfig{
				Input:      workflow.Output("items"),
				Body:       body,
				BodyOutput: workflow.Output("body"),
			}),
			want: workflow.ErrInvalidStepID,
		},
		"subgraph": {
			step: workflow.Subgraph(workflow.SubgraphConfig{
				Body:       body,
				BodyOutput: workflow.Output("body"),
			}),
			want: workflow.ErrInvalidStepID,
		},
		"sequence": {
			step: workflow.Sequence(nil),
			want: workflow.ErrNilStep,
		},
		"parallel": {
			step: workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{nil}}),
			want: workflow.ErrNilStep,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := flow.Validate(testCase.step); !errors.Is(err, testCase.want) {
				t.Fatalf("Validate = %v; want %v", err, testCase.want)
			}
		})
	}
}
