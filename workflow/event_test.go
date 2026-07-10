package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/workflow"
)

func TestEvents_emittedForSequence(t *testing.T) {
	from := func(id string) func(workflow.Store) (int, error) {
		return workflow.FromRef[int](workflow.Ref{NodeID: id, Path: "output"})
	}
	a := workflow.Adapt("a", from("start"), core.Func[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	b := workflow.Adapt("b", from("a"), core.Func[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	var col workflow.Collector
	ctx := workflow.WithSink(context.Background(), col.Sink())

	_, err := workflow.Sequence(a, b).Run(ctx, workflow.NewStore().With("start", "output", 1))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	events := col.Events()
	// Expect: started a, completed a, started b, completed b.
	if len(events) != 4 {
		t.Fatalf("got %d events, want 4: %#v", len(events), events)
	}
	if _, ok := events[0].(workflow.NodeStarted); !ok {
		t.Fatalf("event 0 = %T, want NodeStarted", events[0])
	}
	if c, ok := events[1].(workflow.NodeCompleted); !ok || c.ID != "a" {
		t.Fatalf("event 1 = %#v, want NodeCompleted{a}", events[1])
	}
}

func TestEvents_failure(t *testing.T) {
	boom := errors.New("boom")
	bad := workflow.Adapt("bad",
		workflow.FromRef[int](workflow.Ref{NodeID: "start", Path: "output"}),
		core.Func[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }),
	)

	var col workflow.Collector
	ctx := workflow.WithSink(context.Background(), col.Sink())

	_, _ = bad.Run(ctx, workflow.NewStore().With("start", "output", 1))

	events := col.Events()
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	f, ok := events[1].(workflow.NodeFailed)
	if !ok || f.ID != "bad" || !errors.Is(f.Err, boom) {
		t.Fatalf("event 1 = %#v, want NodeFailed{bad, boom}", events[1])
	}
}

func TestEvents_noSinkIsFine(t *testing.T) {
	a := workflow.Adapt("a",
		workflow.FromRef[int](workflow.Ref{NodeID: "start", Path: "output"}),
		core.Func[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	// No sink in context: emit must be a no-op, not panic.
	if _, err := a.Run(context.Background(), workflow.NewStore().With("start", "output", 1)); err != nil {
		t.Fatalf("run: %v", err)
	}
}
