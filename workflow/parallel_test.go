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
	from := workflow.From[int](workflow.Ref{NodeID: "start", Path: "/output"})
	a := workflow.Leaf("a", from, flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil }))
	b := workflow.Leaf("b", from, flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil }))

	p := workflow.Parallel([]workflow.Step{a, b}, workflow.ParallelConfig{Concurrency: 2})

	out, err := p.Run(t.Context(), workflow.NewStore().WithOutput("start", 5))
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
	from := workflow.From[int](workflow.Ref{NodeID: "start", Path: "/output"})
	ok := workflow.Leaf("ok", from, flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	bad := workflow.Leaf("bad", from, flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }))

	_, err := workflow.Parallel([]workflow.Step{ok, bad}, workflow.ParallelConfig{}).Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
}

func TestParallel_singleBranchPreservesIndexError(t *testing.T) {
	boom := errors.New("boom")
	branch := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		return store, boom
	})

	_, err := workflow.Parallel([]workflow.Step{branch}, workflow.ParallelConfig{}).Run(t.Context(), workflow.NewStore())
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 0 || !errors.Is(err, boom) {
		t.Fatalf("err = %v; want IndexError(0, boom)", err)
	}
}

func TestParallel_emptyAndSingleRespectCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	identity := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		return store, nil
	})

	for _, step := range []workflow.Step{workflow.Parallel(nil, workflow.ParallelConfig{}), workflow.Parallel([]workflow.Step{identity}, workflow.ParallelConfig{})} {
		if _, err := step.Run(ctx, workflow.NewStore()); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v; want context.Canceled", err)
		}
	}
}

func TestParallel_emptyPassesThrough(t *testing.T) {
	input := workflow.NewStore().WithOutput("start", 1)
	output, err := workflow.Parallel(nil, workflow.ParallelConfig{}).
		Run(t.Context(), input)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := workflow.Get[int](output, workflow.Output("start")); err != nil || got != 1 {
		t.Fatalf("start = %v, %v; want 1", got, err)
	}
}

func TestParallel_rejectsDuplicateStaticIDs(t *testing.T) {
	step := leafStep("same")
	_, err := workflow.Parallel(
		[]workflow.Step{step, step},
		workflow.ParallelConfig{},
	).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("error = %v; want ErrDuplicateStep", err)
	}
}

func TestParallel_singleBranchCancellationTakesPrecedence(t *testing.T) {
	boom := errors.New("boom")
	tests := map[string]error{
		"branch error":   boom,
		"branch success": nil,
	}
	for name, branchErr := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			branch := flow.NodeFunc[workflow.Store, workflow.Store](
				func(context.Context, workflow.Store) (workflow.Store, error) {
					cancel()
					return workflow.NewStore(), branchErr
				},
			)
			_, err := workflow.Parallel(
				[]workflow.Step{branch},
				workflow.ParallelConfig{},
			).Run(ctx, workflow.NewStore())
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v; want context.Canceled", err)
			}
		})
	}
}

func TestParallel_singleSuspensionIsPreserved(t *testing.T) {
	output, err := workflow.Parallel(
		[]workflow.Step{workflow.Await("wait", workflow.Output("ready"))},
		workflow.ParallelConfig{},
	).Run(t.Context(), workflow.NewStore())
	if !workflow.SuspendedOnly(err) {
		t.Fatalf("error = %v; want pure suspension", err)
	}
	if _, ok := output.Lookup(workflow.Output("ready")); ok {
		t.Fatal("suspended single branch created its awaited value")
	}
}

func TestParallel_validatesEveryBranchBeforeRunning(t *testing.T) {
	ran := false
	first := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		ran = true
		return store, nil
	})
	_, err := workflow.Parallel([]workflow.Step{first, nil}, workflow.ParallelConfig{}).
		Run(t.Context(), workflow.NewStore())
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 1 || !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want IndexError(1, ErrNilStep)", err)
	}
	if ran {
		t.Fatal("a branch ran before the invalid parallel composition was rejected")
	}
}

func TestParallel_rejectsTypedNilFunctionBeforeRunning(t *testing.T) {
	ran := false
	first := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			ran = true
			return store, nil
		},
	)
	var invalid flow.NodeFunc[workflow.Store, workflow.Store]

	_, err := workflow.Parallel(
		[]workflow.Step{first, invalid},
		workflow.ParallelConfig{},
	).Run(t.Context(), workflow.NewStore())
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 1 ||
		!errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want IndexError(1, ErrNilStep)", err)
	}
	if ran {
		t.Fatal("a branch ran before the typed nil function was rejected")
	}
}

func TestParallel_rejectsNegativeConcurrencyEvenWhenEmpty(t *testing.T) {
	_, err := workflow.Parallel(nil, workflow.ParallelConfig{Concurrency: -1}).
		Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("err = %v; want ErrInvalidConfig", err)
	}
}

func TestParallel_mergesOnlyBranchWrites(t *testing.T) {
	writeExisting := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s.WithCell("existing", "value", 1), nil
	})
	writeOther := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s.WithCell("other", "value", 2), nil
	})
	base := workflow.NewStore().WithCell("existing", "value", 0)

	out, err := workflow.Parallel([]workflow.Step{writeExisting, writeOther}, workflow.ParallelConfig{}).Run(t.Context(), base)
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
			return s.WithCell("shared", "value", value), nil
		})
	}

	out, err := workflow.Parallel([]workflow.Step{write(1), write(2)}, workflow.ParallelConfig{}).Run(t.Context(), workflow.NewStore())
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

	out, err := workflow.Parallel([]workflow.Step{writeShared, writeMany}, workflow.ParallelConfig{}).Run(t.Context(), base)
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

	out, err := workflow.Parallel([]workflow.Step{replace}, workflow.ParallelConfig{}).Run(t.Context(), base)
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

func TestParallel_compactsDeepBranchInput(t *testing.T) {
	base := workflow.NewStore()
	for index := range 64 {
		base = base.WithOutput(fmt.Sprintf("base-%d", index), index)
	}
	write := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store.WithOutput("written", true), nil
		},
	)
	pass := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store, nil
		},
	)
	output, err := workflow.Parallel(
		[]workflow.Step{write, pass},
		workflow.ParallelConfig{},
	).Run(t.Context(), base)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := workflow.Get[bool](output, workflow.Output("written")); err != nil || !got {
		t.Fatalf("written = %v, %v; want true", got, err)
	}
	if got, err := workflow.Get[int](output, workflow.Output("base-0")); err != nil || got != 0 {
		t.Fatalf("base-0 = %v, %v; want 0", got, err)
	}
}

func TestParallel_compactsLargeMergedResult(t *testing.T) {
	branches := make([]workflow.Step, 130)
	for index := range branches {
		branches[index] = flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				return store.WithOutput(fmt.Sprintf("branch-%d", index), index), nil
			},
		)
	}
	output, err := workflow.Parallel(branches, workflow.ParallelConfig{Concurrency: 4}).
		Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for index := range branches {
		if got, ok := output.Lookup(workflow.Output(fmt.Sprintf("branch-%d", index))); !ok || got != index {
			t.Fatalf("branch-%d = %v, %v; want %d", index, got, ok, index)
		}
	}
}
