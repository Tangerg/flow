package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestJournal_recordsOnlyCompletedSteps(t *testing.T) {
	boom := errors.New("boom")
	ok := workflow.Leaf("ok", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	bad := workflow.Leaf("bad", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 0, boom }))

	journal := workflow.NewJournal()
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Journal: journal})
	if _, err := workflow.Sequence(ok, bad).Run(ctx, workflow.NewStore()); !errors.Is(err, boom) {
		t.Fatalf("err = %v; want boom", err)
	}
	// A failed step is not recorded, so a later run retries it.
	if keys := journal.Keys(); !slices.Equal(keys, []string{"ok"}) {
		t.Fatalf("keys = %v; want only the completed step", keys)
	}
}

func TestJournal_forgetAndReset(t *testing.T) {
	var runs int
	step := workflow.Leaf("a", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { runs++; return x, nil }))

	journal := workflow.NewJournal()
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Journal: journal})

	for range 3 {
		if _, err := step.Run(ctx, workflow.NewStore()); err != nil {
			t.Fatalf("run: %v", err)
		}
	}
	if runs != 1 {
		t.Fatalf("ran %d times; want 1", runs)
	}

	journal.Forget(nil, "a")
	if _, err := step.Run(ctx, workflow.NewStore()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if runs != 2 {
		t.Fatalf("ran %d times after Forget; want 2", runs)
	}

	journal.Reset()
	if journal.Len() != 0 {
		t.Fatalf("Len after Reset = %d; want 0", journal.Len())
	}
	if _, err := step.Run(ctx, workflow.NewStore()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if runs != 3 {
		t.Fatalf("ran %d times after Reset; want 3", runs)
	}
}

func TestJournal_jsonRoundTrip(t *testing.T) {
	type payload struct {
		N int `json:"n"`
	}
	journal := workflow.NewJournal()
	step := func(id string, value any) workflow.Step {
		return workflow.Leaf(id, workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
			flow.NodeFunc[int, any](func(context.Context, int) (any, error) { return value, nil }))
	}
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Journal: journal})
	pipeline := workflow.Sequence(
		step("i", 42),
		step("s", "text"),
		step("b", true),
		step("struct", payload{N: 7}),
	)
	if _, err := pipeline.Run(ctx, workflow.NewStore()); err != nil {
		t.Fatalf("run: %v", err)
	}

	data, err := json.Marshal(journal)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	restored := workflow.NewJournal()
	if err := json.Unmarshal(data, restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !slices.Equal(restored.Keys(), journal.Keys()) {
		t.Fatalf("keys = %v; want %v", restored.Keys(), journal.Keys())
	}

	// A restored record has to read back as the type the reading step asks for.
	out, err := pipeline.Run(workflow.WithConfig(context.Background(), workflow.RunConfig{Journal: restored}), workflow.NewStore())
	if err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if got, err := workflow.Get[int](out, workflow.Output("i")); err != nil || got != 42 {
		t.Fatalf("int = %v, %v", got, err)
	}
	if got, err := workflow.Get[string](out, workflow.Output("s")); err != nil || got != "text" {
		t.Fatalf("string = %v, %v", got, err)
	}
	if got, err := workflow.Get[bool](out, workflow.Output("b")); err != nil || !got {
		t.Fatalf("bool = %v, %v", got, err)
	}
	if got, err := workflow.Get[payload](out, workflow.Output("struct")); err != nil || got.N != 7 {
		t.Fatalf("struct = %+v, %v", got, err)
	}
}

func TestJournal_marshalReportsTheOffendingRecord(t *testing.T) {
	journal := workflow.NewJournal()
	step := workflow.Leaf("bad", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, any](func(context.Context, int) (any, error) { return func() {}, nil }))
	if _, err := step.Run(workflow.WithConfig(context.Background(), workflow.RunConfig{Journal: journal}), workflow.NewStore()); err != nil {
		t.Fatalf("run: %v", err)
	}

	_, err := json.Marshal(journal)
	if err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("Marshal err = %v; want it to name the record", err)
	}
}

func TestJournal_unmarshalIsAtomic(t *testing.T) {
	journal := workflow.NewJournal()
	if err := json.Unmarshal([]byte(`{"a":1}`), journal); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"b":`), journal); err == nil {
		t.Fatal("expected a decode error")
	}
	if keys := journal.Keys(); !slices.Equal(keys, []string{"a"}) {
		t.Fatalf("keys = %v; want the journal unchanged after a failed decode", keys)
	}
}

func TestJournal_nilIsSafe(t *testing.T) {
	var journal *workflow.Journal
	if journal.Len() != 0 || journal.Keys() != nil {
		t.Fatal("nil Journal did not read as empty")
	}
	journal.Reset()
	journal.Forget(nil, "a")

	// A nil Journal disables resumption rather than being attached.
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Journal: nil})
	step := workflow.Leaf("a", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	if _, err := step.Run(ctx, workflow.NewStore()); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestJournal_concurrentBranchesRecordSafely(t *testing.T) {
	const branches = 32
	steps := make([]workflow.Step, branches)
	for i := range steps {
		id := "b" + strconv.Itoa(i)
		steps[i] = workflow.Leaf(id, workflow.BindFunc[int](func(workflow.Store) (int, error) { return i, nil }),
			flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	}

	journal := workflow.NewJournal()
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Journal: journal})
	if _, err := workflow.Parallel(steps).Run(ctx, workflow.NewStore()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if journal.Len() != branches {
		t.Fatalf("journal recorded %d of %d branches", journal.Len(), branches)
	}

	// Reading and writing at once must also be safe.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _ = journal.Keys() })
		wg.Go(func() { journal.Forget(nil, "b0") })
	}
	wg.Wait()
}

func TestAwaitFactory(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterLeaf("addN", addN()).
		MustRegisterLeaf("await", workflow.AwaitFactory())

	spec := []byte(`{"kind":"sequence","steps":[
	  {"id":"approval","type":"await","kind":"leaf","input":{"nodeID":"inbox","path":"decision"}},
	  {"id":"act","type":"addN","kind":"leaf","input":{"nodeID":"start","path":"output"},"config":{"n":1}}
	]}`)
	step, err := reg.CompileSpecJSON(spec)
	if err != nil {
		t.Fatalf("CompileSpecJSON: %v", err)
	}

	in := workflow.NewStore().WithOutput("start", 1)
	if _, err := step.Run(context.Background(), in); !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
	out, err := step.Run(context.Background(), in.With("inbox", "decision", "yes"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got, err := workflow.Get[int](out, workflow.Output("act")); err != nil || got != 2 {
		t.Fatalf("act = %v, %v; want 2", got, err)
	}
}

func TestAwaitFactory_requiresAWiredPort(t *testing.T) {
	if _, err := workflow.AwaitFactory()(workflow.LeafSpec{ID: "a"}); !errors.Is(err, workflow.ErrMissingPort) {
		t.Fatalf("err = %v; want ErrMissingPort", err)
	}
}
