package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/workflow"
)

func TestParallel_mergesBranches(t *testing.T) {
	from := workflow.FromRef[int](workflow.Ref{NodeID: "start", Path: "output"})
	a := workflow.Adapt("a", from, core.Func[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil }))
	b := workflow.Adapt("b", from, core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil }))

	p := workflow.Parallel([]workflow.Step{a, b})

	out, err := p.Run(context.Background(), workflow.NewStore().With("start", "output", 5))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Get("a", workflow.OutputKey); !ok || v.(int) != 10 {
		t.Fatalf("branch a = %v, %v; want 10", v, ok)
	}
	if v, ok := out.Get("b", workflow.OutputKey); !ok || v.(int) != 6 {
		t.Fatalf("branch b = %v, %v; want 6", v, ok)
	}
}

func TestParallel_failFast(t *testing.T) {
	boom := errors.New("boom")
	from := workflow.FromRef[int](workflow.Ref{NodeID: "start", Path: "output"})
	ok := workflow.Adapt("ok", from, core.Func[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	bad := workflow.Adapt("bad", from, core.Func[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }))

	_, err := workflow.Parallel([]workflow.Step{ok, bad}).Run(context.Background(), workflow.NewStore().With("start", "output", 1))
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
}

func TestParallel_mergesOnlyBranchWrites(t *testing.T) {
	writeExisting := core.Func[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s.With("existing", "value", 1), nil
	})
	writeOther := core.Func[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s.With("other", "value", 2), nil
	})
	base := workflow.NewStore().With("existing", "value", 0)

	out, err := workflow.Parallel([]workflow.Step{writeExisting, writeOther}).Run(context.Background(), base)
	if err != nil {
		t.Fatalf("Parallel: %v", err)
	}
	if got, _ := out.Get("existing", "value"); got != 1 {
		t.Fatalf("existing value = %v; stale base snapshot overwrote branch write", got)
	}
	if got, _ := out.Get("other", "value"); got != 2 {
		t.Fatalf("other value = %v; want 2", got)
	}
}

func TestParallel_laterBranchWinsCellConflict(t *testing.T) {
	write := func(value int) workflow.Step {
		return core.Func[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
			return s.With("shared", "value", value), nil
		})
	}

	out, err := workflow.Parallel([]workflow.Step{write(1), write(2)}).Run(context.Background(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Parallel: %v", err)
	}
	if got, _ := out.Get("shared", "value"); got != 2 {
		t.Fatalf("shared value = %v; want later branch value 2", got)
	}
}
