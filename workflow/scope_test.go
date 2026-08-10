package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func ordinaryScope(ids ...string) []workflow.ScopeFrame {
	scope := make([]workflow.ScopeFrame, len(ids))
	for index, id := range ids {
		scope[index] = workflow.ScopeFrame{ID: id}
	}
	return scope
}

func indexedScope(id string, index uint64) []workflow.ScopeFrame {
	return []workflow.ScopeFrame{{ID: id, Indexed: true, Index: index}}
}

func scopeText(scope []workflow.ScopeFrame) string {
	frames := make([]string, len(scope))
	for index, frame := range scope {
		frames[index] = frame.String()
	}
	return strings.Join(frames, "/")
}

func TestScopeFrame_Validate(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})
	tests := map[string]struct {
		frame workflow.ScopeFrame
		valid bool
	}{
		"ordinary":              {frame: workflow.ScopeFrame{ID: "subgraph"}, valid: true},
		"indexed zero":          {frame: workflow.ScopeFrame{ID: "loop", Indexed: true}, valid: true},
		"indexed nonzero":       {frame: workflow.ScopeFrame{ID: "loop", Indexed: true, Index: 2}, valid: true},
		"empty ID":              {frame: workflow.ScopeFrame{}},
		"non-UTF-8 ID":          {frame: workflow.ScopeFrame{ID: invalidUTF8}},
		"index without Indexed": {frame: workflow.ScopeFrame{ID: "loop", Index: 2}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := test.frame.Validate()
			if (err == nil) != test.valid {
				t.Fatalf("Validate error = %v; valid = %v", err, test.valid)
			}
		})
	}
}

func TestTopLevelScopeDiagnosticsNameTheRoot(t *testing.T) {
	journal := workflow.NewJournal()
	key := workflow.JournalKey{ID: "step"}
	if err := journal.Record(key, 1); err != nil {
		t.Fatalf("first Record: %v", err)
	}
	if err := journal.Record(key, 2); err == nil ||
		!strings.Contains(err.Error(), `at "<root>"`) {
		t.Fatalf("duplicate Record error = %v; want named root scope", err)
	}

	addOne := func(_ context.Context, value int) (int, error) {
		return value + 1, nil
	}
	first := workflow.LeafFunc("step", workflow.Output("in"), addOne)
	second := workflow.LeafFunc("step", workflow.Output("in"), addOne)
	step := flow.NodeFunc[workflow.Store, workflow.Store](
		func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
			store, err := first.Run(ctx, store)
			if err != nil {
				return store, err
			}
			return second.Run(ctx, store)
		},
	)
	_, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("in", 1),
		workflow.RunConfig{},
	)
	if err == nil || !strings.Contains(err.Error(), `scope "<root>"`) {
		t.Fatalf("duplicate Step error = %v; want named root scope", err)
	}
}

func TestScopeFrame_distinguishesIndexedInvocationFromLiteralID(t *testing.T) {
	var runs atomic.Int32
	body := func() workflow.Step {
		return workflow.Leaf(
			"work",
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				runs.Add(1)
				return 1, nil
			}),
		)
	}

	literal := workflow.Subgraph(workflow.SubgraphConfig{
		ID:         "repeat[0]",
		Body:       body(),
		BodyOutput: workflow.Output("work"),
	})
	repeated := workflow.Loop(
		"repeat",
		body(),
		func(context.Context, int, workflow.Store) (bool, error) { return true, nil },
		workflow.LoopConfig{},
	)
	pipeline := workflow.Parallel(
		[]workflow.Step{literal, repeated},
		workflow.ParallelConfig{},
	)
	journal := workflow.NewJournal()
	_, err := workflow.Run(
		t.Context(),
		pipeline,
		workflow.NewStore(),
		workflow.RunConfig{Journal: journal},
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("body runs = %d; want 2", got)
	}

	want := []workflow.JournalKey{
		{ID: "repeat", Scope: indexedScope("repeat", 0)},
		{ID: "work", Scope: indexedScope("repeat", 0)},
		{ID: "work", Scope: ordinaryScope("repeat[0]")},
	}
	if got := journal.Keys(); !slices.EqualFunc(got, want, func(left, right workflow.JournalKey) bool {
		return left.ID == right.ID && slices.Equal(left.Scope, right.Scope)
	}) {
		t.Fatalf("Journal keys = %+v; want %+v", got, want)
	}

	encoded, err := json.Marshal(journal)
	if err != nil {
		t.Fatalf("Marshal Journal: %v", err)
	}
	var restored workflow.Journal
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatalf("Unmarshal Journal: %v", err)
	}
	if _, err := workflow.Run(
		t.Context(),
		pipeline,
		workflow.NewStore(),
		workflow.RunConfig{Journal: &restored},
	); err != nil {
		t.Fatalf("resumed Run: %v", err)
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("body runs after resume = %d; want both structured identities replayed", got)
	}
}

func TestWithScope_invalidIdentityFailsWithoutRunConfig(t *testing.T) {
	var calls atomic.Int32
	step := workflow.Leaf(
		"work",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
			calls.Add(1)
			return 1, nil
		}),
	)

	_, err := step.Run(
		workflow.WithScope(t.Context(), ""),
		workflow.NewStore(),
	)
	if err == nil {
		t.Fatal("Run accepted an empty execution scope ID")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("node calls = %d; want validation before work", got)
	}
}

func TestWithScopeIndex_rejectsAnEmptyIDBeforeWork(t *testing.T) {
	var calls atomic.Int32
	step := workflow.Leaf(
		"work",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
			calls.Add(1)
			return 1, nil
		}),
	)
	ctx := workflow.WithScopeIndex(t.Context(), "", 1)

	_, err := step.Run(ctx, workflow.NewStore())
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.Op != workflow.OpValidate {
		t.Fatalf("Run error = %v; want validation StepError", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("node calls = %d; want validation before work", got)
	}
}

func TestScopedComposites_rejectAnOverDepthChildScopeBeforeWork(t *testing.T) {
	ctx := scopeContext(t.Context(), workflow.MaxNestingDepth)
	var calls atomic.Int32
	body := workflow.Leaf(
		"work",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
			calls.Add(1)
			return 1, nil
		}),
	)
	tests := []struct {
		name  string
		step  workflow.Step
		store workflow.Store
	}{
		{
			name: "loop",
			step: workflow.Loop(
				"loop",
				body,
				func(context.Context, int, workflow.Store) (bool, error) { return true, nil },
				workflow.LoopConfig{},
			),
		},
		{
			name: "iteration",
			step: workflow.Iteration(workflow.IterationConfig{
				ID:         "iteration",
				Input:      workflow.Output("items"),
				Body:       body,
				BodyOutput: workflow.Output("work"),
			}),
			store: workflow.NewStore().WithOutput("items", []any{}),
		},
		{
			name: "subgraph",
			step: workflow.Subgraph(workflow.SubgraphConfig{
				ID:         "subgraph",
				Body:       body,
				BodyOutput: workflow.Output("work"),
			}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.step.Run(ctx, test.store)
			var stepErr *workflow.StepError
			if !errors.Is(err, workflow.ErrMaxDepth) ||
				!errors.As(err, &stepErr) || stepErr.Op != workflow.OpValidate {
				t.Fatalf("Run error = %v; want OpValidate ErrMaxDepth", err)
			}
		})
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("body calls = %d; want no work before scope validation", got)
	}
}

func TestWithScopeIndex_supportsCallerDefinedRepetition(t *testing.T) {
	var calls atomic.Int32
	body := workflow.Leaf(
		"work",
		workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) { return struct{}{}, nil }),
		flow.NodeFunc[struct{}, int](func(context.Context, struct{}) (int, error) {
			return int(calls.Add(1)), nil
		}),
	)
	repeated := flow.NodeFunc[workflow.Store, workflow.Store](
		func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
			current := store
			for index := range 2 {
				var err error
				current, err = body.Run(
					workflow.WithScopeIndex(ctx, "custom", uint64(index)),
					current,
				)
				if err != nil {
					return current, err
				}
			}
			return current, nil
		},
	)
	journal := workflow.NewJournal()
	config := workflow.RunConfig{Journal: journal}

	for range 2 {
		if _, err := workflow.Run(
			t.Context(),
			repeated,
			workflow.NewStore(),
			config,
		); err != nil {
			t.Fatalf("Run: %v", err)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("body calls = %d; want two first-run indexed invocations", got)
	}
	want := []workflow.JournalKey{
		{ID: "work", Scope: indexedScope("custom", 0)},
		{ID: "work", Scope: indexedScope("custom", 1)},
	}
	if keys := journal.Keys(); !equalJournalKeys(keys, want) {
		t.Fatalf("Journal keys = %+v; want %+v", keys, want)
	}
}

func scopeContext(ctx context.Context, depth int) context.Context {
	if depth == 0 {
		return ctx
	}
	return scopeContext(
		workflow.WithScope(ctx, strconv.Itoa(depth)),
		depth-1,
	)
}
