package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestParallel_mergesBranches(t *testing.T) {
	from := workflow.From[int](workflow.Ref{NodeID: "start", Path: "output"})
	a := workflow.Leaf("a", from, flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil }))
	b := workflow.Leaf("b", from, flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil }))

	p := workflow.Parallel([]workflow.Step{a, b}, workflow.ParallelConfig{Concurrency: 2})

	out, err := p.Run(context.Background(), workflow.NewStore().WithOutput("start", 5))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("a")); !ok || v.(int) != 10 {
		t.Fatalf("branch a = %v, %v; want 10", v, ok)
	}
	if v, ok := out.Lookup(workflow.Output("b")); !ok || v.(int) != 6 {
		t.Fatalf("branch b = %v, %v; want 6", v, ok)
	}
}

func TestParallel_failFast(t *testing.T) {
	boom := errors.New("boom")
	from := workflow.From[int](workflow.Ref{NodeID: "start", Path: "output"})
	ok := workflow.Leaf("ok", from, flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	bad := workflow.Leaf("bad", from, flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }))

	_, err := workflow.Parallel([]workflow.Step{ok, bad}).Run(context.Background(), workflow.NewStore().WithOutput("start", 1))
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
}

func TestParallel_singleBranchPreservesIndexError(t *testing.T) {
	boom := errors.New("boom")
	branch := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		return store, boom
	})

	_, err := workflow.Parallel([]workflow.Step{branch}).Run(context.Background(), workflow.NewStore())
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 0 || !errors.Is(err, boom) {
		t.Fatalf("err = %v; want IndexError(0, boom)", err)
	}
}

func TestParallel_emptyAndSingleRespectCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	identity := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		return store, nil
	})

	for _, step := range []workflow.Step{workflow.Parallel(nil), workflow.Parallel([]workflow.Step{identity})} {
		if _, err := step.Run(ctx, workflow.NewStore()); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v; want context.Canceled", err)
		}
	}
}

func TestParallel_mergesOnlyBranchWrites(t *testing.T) {
	writeExisting := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s.With("existing", "value", 1), nil
	})
	writeOther := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s.With("other", "value", 2), nil
	})
	base := workflow.NewStore().With("existing", "value", 0)

	out, err := workflow.Parallel([]workflow.Step{writeExisting, writeOther}).Run(context.Background(), base)
	if err != nil {
		t.Fatalf("Parallel: %v", err)
	}
	if got, _ := out.Lookup(workflow.At("existing", "value")); got != 1 {
		t.Fatalf("existing value = %v; stale base snapshot overwrote branch write", got)
	}
	if got, _ := out.Lookup(workflow.At("other", "value")); got != 2 {
		t.Fatalf("other value = %v; want 2", got)
	}
}

func TestParallel_laterBranchWinsCellConflict(t *testing.T) {
	write := func(value int) workflow.Step {
		return flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
			return s.With("shared", "value", value), nil
		})
	}

	out, err := workflow.Parallel([]workflow.Step{write(1), write(2)}).Run(context.Background(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Parallel: %v", err)
	}
	if got, _ := out.Lookup(workflow.At("shared", "value")); got != 2 {
		t.Fatalf("shared value = %v; want later branch value 2", got)
	}
}

func TestParallel_compactedBranchMergesOnlyWrites(t *testing.T) {
	writeShared := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		return store.WithOutput("shared", 1), nil
	})
	writeMany := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		for i := range 40 {
			store = store.WithOutput(fmt.Sprintf("node-%d", i), i)
		}
		return store, nil
	})
	base := workflow.NewStore().WithOutput("shared", 0)

	out, err := workflow.Parallel([]workflow.Step{writeShared, writeMany}).Run(context.Background(), base)
	if err != nil {
		t.Fatalf("Parallel: %v", err)
	}
	if got, _ := out.Lookup(workflow.Output("shared")); got != 1 {
		t.Fatalf("shared = %v; inherited base value from compacted branch won", got)
	}
	for i := range 40 {
		if got, _ := out.Lookup(workflow.Output(fmt.Sprintf("node-%d", i))); got != i {
			t.Fatalf("node-%d = %v; want %d", i, got, i)
		}
	}
}

func TestParallel_mergesUnrelatedStore(t *testing.T) {
	replace := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, _ workflow.Store) (workflow.Store, error) {
		return workflow.NewStore().WithOutput("other", 2), nil
	})
	base := workflow.NewStore().WithOutput("base", 1)

	out, err := workflow.Parallel([]workflow.Step{replace}).Run(context.Background(), base)
	if err != nil {
		t.Fatalf("Parallel: %v", err)
	}
	if got, _ := out.Lookup(workflow.Output("base")); got != 1 {
		t.Fatalf("base = %v; want 1", got)
	}
	if got, _ := out.Lookup(workflow.Output("other")); got != 2 {
		t.Fatalf("other = %v; want 2", got)
	}
}
