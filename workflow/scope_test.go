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

// TestScopeFrame_StringIsDecimal covers the base an indexed frame displays in.
// Every scope text asserted elsewhere comes from a run of three iterations, where
// one digit reads the same in any base; a two-digit index is the smallest one that
// says decimal. This text reaches a caller inside a StepError, so it is the form a
// reader matches a scope against.
func TestScopeFrame_StringIsDecimal(t *testing.T) {
	for _, test := range []struct {
		frame workflow.ScopeFrame
		want  string
	}{
		{frame: workflow.ScopeFrame{ID: "loop", Indexed: true}, want: "loop[0]"},
		{frame: workflow.ScopeFrame{ID: "loop", Indexed: true, Index: 11}, want: "loop[11]"},
		{frame: workflow.ScopeFrame{ID: "subgraph"}, want: "subgraph"},
	} {
		t.Run(test.want, func(t *testing.T) {
			if got := test.frame.String(); got != test.want {
				t.Fatalf("String = %q; want %q", got, test.want)
			}
		})
	}
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

func TestScopeFrame_JSONIsCanonicalAndLossless(t *testing.T) {
	tests := map[string]struct {
		frame workflow.ScopeFrame
		wire  string
	}{
		"ordinary": {
			frame: workflow.ScopeFrame{ID: "subgraph"},
			wire:  `{"id":"subgraph"}`,
		},
		"indexed zero": {
			frame: workflow.ScopeFrame{ID: "loop", Indexed: true},
			wire:  `{"id":"loop","index":0}`,
		},
		"indexed maximum": {
			frame: workflow.ScopeFrame{ID: "loop", Indexed: true, Index: ^uint64(0)},
			wire:  `{"id":"loop","index":18446744073709551615}`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(test.frame)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if got := string(encoded); got != test.wire {
				t.Fatalf("Marshal = %s; want %s", got, test.wire)
			}

			var restored workflow.ScopeFrame
			if err := json.Unmarshal(encoded, &restored); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if restored != test.frame {
				t.Fatalf("round trip = %+v; want %+v", restored, test.frame)
			}
		})
	}

	var exponent workflow.ScopeFrame
	if err := json.Unmarshal([]byte(`{"id":"loop","index":1e0}`), &exponent); err != nil {
		t.Fatalf("Unmarshal mathematical integer: %v", err)
	}
	if want := (workflow.ScopeFrame{ID: "loop", Indexed: true, Index: 1}); exponent != want {
		t.Fatalf("mathematical integer frame = %+v; want %+v", exponent, want)
	}
}

func TestScopeFrame_JSONBoundaryIsStrictAndAtomic(t *testing.T) {
	invalid := map[string][]byte{
		"null":                []byte(`null`),
		"array":               []byte(`[]`),
		"missing ID":          []byte(`{"index":0}`),
		"non-string ID":       []byte(`{"id":1}`),
		"empty ID":            []byte(`{"id":""}`),
		"invalid UTF-8":       {'{', '"', 'i', 'd', '"', ':', '"', 0xff, '"', '}'},
		"unpaired surrogate":  []byte(`{"id":"\ud800"}`),
		"unknown field":       []byte(`{"id":"loop","extra":true}`),
		"duplicate ID":        []byte(`{"id":"first","id":"second"}`),
		"duplicate index":     []byte(`{"id":"loop","index":0,"index":1}`),
		"legacy indexed flag": []byte(`{"id":"loop","indexed":true}`),
		"string index":        []byte(`{"id":"loop","index":"0"}`),
		"fractional index":    []byte(`{"id":"loop","index":0.5}`),
		"negative index":      []byte(`{"id":"loop","index":-1}`),
		"index out of range":  []byte(`{"id":"loop","index":18446744073709551616}`),
	}
	for name, data := range invalid {
		t.Run(name, func(t *testing.T) {
			frame := workflow.ScopeFrame{ID: "kept", Indexed: true, Index: 7}
			if err := json.Unmarshal(data, &frame); err == nil {
				t.Fatal("Unmarshal unexpectedly succeeded")
			}
			if want := (workflow.ScopeFrame{ID: "kept", Indexed: true, Index: 7}); frame != want {
				t.Fatalf("failed Unmarshal changed receiver to %+v; want %+v", frame, want)
			}
		})
	}

	for name, frame := range map[string]workflow.ScopeFrame{
		"invalid ID":            {ID: string([]byte{0xff})},
		"index without Indexed": {ID: "loop", Index: 1},
	} {
		t.Run("marshal "+name, func(t *testing.T) {
			if _, err := json.Marshal(frame); err == nil {
				t.Fatal("Marshal unexpectedly succeeded")
			}
		})
	}

	var frame *workflow.ScopeFrame
	if err := frame.UnmarshalJSON([]byte(`{"id":"loop"}`)); err == nil {
		t.Fatal("nil receiver UnmarshalJSON unexpectedly succeeded")
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
	repeated := workflow.Loop(workflow.LoopConfig{
		ID:        "repeat",
		Body:      body(),
		Condition: flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) { return true, nil }),
	})

	pipeline := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{literal, repeated}})

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
			step: workflow.Loop(workflow.LoopConfig{
				ID:        "loop",
				Body:      body,
				Condition: flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) { return true, nil }),
			}),
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
