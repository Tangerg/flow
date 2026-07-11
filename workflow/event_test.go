package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestEvents_emittedForSequence(t *testing.T) {
	from := func(id string) workflow.Binder[int] {
		return workflow.From[int](workflow.Output(id))
	}
	a := workflow.Leaf("a", from("start"), flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	b := workflow.Leaf("b", from("a"), flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	var events []workflow.Event
	ctx := workflow.WithObserver(context.Background(), workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		events = append(events, event)
	}))

	_, err := workflow.Sequence(a, b).Run(ctx, workflow.NewStore().WithOutput("start", 1))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// Expect: started a, completed a, started b, completed b.
	if len(events) != 4 {
		t.Fatalf("got %d events, want 4: %#v", len(events), events)
	}
	if events[0].Kind != workflow.EventStarted || events[0].ID != "a" {
		t.Fatalf("event 0 = %#v, want started a", events[0])
	}
	if events[1].Kind != workflow.EventCompleted || events[1].ID != "a" {
		t.Fatalf("event 1 = %#v, want completed a", events[1])
	}
}

func TestEvents_failure(t *testing.T) {
	boom := errors.New("boom")
	bad := workflow.Leaf("bad",
		workflow.From[int](workflow.Ref{NodeID: "start", Path: "output"}),
		flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }),
	)

	var events []workflow.Event
	ctx := workflow.WithObserver(context.Background(), workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		events = append(events, event)
	}))

	_, _ = bad.Run(ctx, workflow.NewStore().WithOutput("start", 1))

	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	f := events[1]
	if f.Kind != workflow.EventFailed || f.ID != "bad" || !errors.Is(f.Err, boom) {
		t.Fatalf("event 1 = %#v, want failed bad with boom", events[1])
	}
}

func TestEvents_noObserverIsFine(t *testing.T) {
	a := workflow.Leaf("a",
		workflow.From[int](workflow.Ref{NodeID: "start", Path: "output"}),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	// No observer in context: emit must be a no-op, not panic.
	if _, err := a.Run(context.Background(), workflow.NewStore().WithOutput("start", 1)); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestObserverFunc(t *testing.T) {
	var got workflow.Event
	observer := workflow.ObserverFunc(func(_ context.Context, event workflow.Event) { got = event })
	observer.Observe(context.Background(), workflow.Event{Kind: workflow.EventStarted, ID: "a"})
	if got.Kind != workflow.EventStarted || got.ID != "a" {
		t.Fatalf("event = %#v", got)
	}
}
