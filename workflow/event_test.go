package workflow_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestEvents_emittedForSequence(t *testing.T) {
	from := func(id string) workflow.BindFunc[int] {
		return workflow.From[int](workflow.Output(id))
	}
	a := workflow.Leaf("a", from("start"), flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	b := workflow.Leaf("b", from("a"), flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	var events []workflow.Event
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		events = append(events, event)
	})})

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
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		events = append(events, event)
	})})

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

func TestEvents_carrySequenceElapsedAndStore(t *testing.T) {
	a := workflow.Leaf("a",
		workflow.From[int](workflow.Output("start")),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil }),
	)

	var events []workflow.Event
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		events = append(events, event)
	})})

	in := workflow.NewStore().WithOutput("start", 21)
	if _, err := a.Run(ctx, in); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	started, completed := events[0], events[1]
	if started.Seq != 1 || completed.Seq != 2 {
		t.Fatalf("Seq = %d, %d; want 1, 2", started.Seq, completed.Seq)
	}
	if started.Elapsed != 0 {
		t.Fatalf("started Elapsed = %v; want 0", started.Elapsed)
	}
	// A completed event carries the Store the step produced, which is what an
	// external tracker or persister records.
	if v, err := workflow.Get[int](completed.Store, workflow.Output("a")); err != nil || v != 42 {
		t.Fatalf("completed Store a = %v, %v; want 42", v, err)
	}
	if changes := completed.Store.Changes(in); len(changes) != 1 || changes[0].Ref() != workflow.Output("a") {
		t.Fatalf("Changes = %+v; want one write to a.output", changes)
	}
}

func TestEvents_failedCarriesNoStore(t *testing.T) {
	boom := errors.New("boom")
	bad := workflow.Leaf("bad",
		workflow.From[int](workflow.Output("start")),
		flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }),
	)

	var failed workflow.Event
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		if event.Kind == workflow.EventFailed {
			failed = event
		}
	})})
	_, _ = bad.Run(ctx, workflow.NewStore().WithOutput("start", 1))

	if _, ok := failed.Store.Lookup(workflow.Output("bad")); ok {
		t.Fatal("failed event Store holds an output; a failed step produces none")
	}
	if failed.Seq != 2 {
		t.Fatalf("Seq = %d; want 2", failed.Seq)
	}
}

func TestEvents_scopePathDistinguishesIterationElements(t *testing.T) {
	body := workflow.Leaf("el",
		workflow.From[int](workflow.Item("iter")),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	step := workflow.Iteration(workflow.IterationConfig{
		ID:          "iter",
		Input:       workflow.Output("start"),
		Body:        body,
		BodyOutput:  workflow.Output("el"),
		Concurrency: 1, // deterministic order for the assertion
	})

	var paths []string
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		if event.Kind == workflow.EventCompleted {
			paths = append(paths, strings.Join(event.Path, "/"))
		}
	})})

	if _, err := step.Run(ctx, workflow.NewStore().WithOutput("start", []any{1, 2, 3})); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{"iter[0]", "iter[1]", "iter[2]"}
	if !slices.Equal(paths, want) {
		t.Fatalf("paths = %v; want %v", paths, want)
	}
}

func TestEvents_scopePathDistinguishesLoopIterations(t *testing.T) {
	count := 0
	body := workflow.Leaf("tick",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { count++; return count, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	done := func(_ context.Context, iter int, _ workflow.Store) (bool, error) { return iter >= 2, nil }

	var paths []string
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		if event.Kind == workflow.EventCompleted {
			paths = append(paths, strings.Join(event.Path, "/"))
		}
	})})

	if _, err := workflow.Loop("loop", body, done).Run(ctx, workflow.NewStore()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if want := []string{"[0]", "[1]", "[2]"}; !slices.Equal(paths, want) {
		t.Fatalf("paths = %v; want %v", paths, want)
	}
}

func TestWithScope_isMaintainedWithoutAnObserver(t *testing.T) {
	// A scope identifies a step rather than labelling it: a Journal keys its
	// records by the scope, so it has to exist whether or not anything is
	// watching. Tying it to the observer let a journaled Loop skip every
	// iteration after the first.
	bare := workflow.WithScope(context.Background(), "kept")
	if scope := workflow.Scope(bare); !slices.Equal(scope, []string{"kept"}) {
		t.Fatalf("Scope = %v; want [kept] even with no observer", scope)
	}
	if scope := workflow.Scope(context.Background()); scope != nil {
		t.Fatalf("Scope = %v; want nil at the top level", scope)
	}
}

func TestWithScope_nests(t *testing.T) {
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Observer: workflow.ObserverFunc(func(context.Context, workflow.Event) {})})
	outer := workflow.WithScope(ctx, "a")
	inner := workflow.WithScope(outer, "b")
	if !slices.Equal(workflow.Scope(inner), []string{"a", "b"}) {
		t.Fatalf("inner scope = %v; want [a b]", workflow.Scope(inner))
	}
	// Deriving a sibling must not disturb the outer scope.
	if !slices.Equal(workflow.Scope(outer), []string{"a"}) {
		t.Fatalf("outer scope = %v; want [a]", workflow.Scope(outer))
	}
}

func TestWithObserver_nilObserverIsInert(t *testing.T) {
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Observer: nil})
	if scope := workflow.Scope(ctx); scope != nil {
		t.Fatalf("Scope = %v; want nil", scope)
	}
	a := workflow.Leaf("a",
		workflow.From[int](workflow.Output("start")),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	if _, err := a.Run(ctx, workflow.NewStore().WithOutput("start", 1)); err != nil {
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

func TestRunConfig_readBackAndZeroValue(t *testing.T) {
	journal := workflow.NewJournal()
	observe := workflow.ObserverFunc(func(context.Context, workflow.Event) {})
	ctx := workflow.WithConfig(context.Background(), workflow.RunConfig{Observer: observe, Journal: journal})

	// A node can read the run's configuration back to hand it to a nested run.
	cfg := workflow.Config(ctx)
	if cfg.Journal != journal {
		t.Fatalf("Config().Journal = %p; want %p", cfg.Journal, journal)
	}
	if cfg.Observer == nil {
		t.Fatal("Config().Observer is nil")
	}

	// Outside a configured run, and for a zero config, everything is inert.
	if got := workflow.Config(context.Background()); got.Observer != nil || got.Journal != nil {
		t.Fatalf("Config(bare) = %+v; want the zero RunConfig", got)
	}
	bare := workflow.WithConfig(context.Background(), workflow.RunConfig{})
	if got := workflow.Config(bare); got.Observer != nil || got.Journal != nil {
		t.Fatalf("Config(zero) = %+v; want the zero RunConfig", got)
	}
	step := workflow.Leaf("a", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	if _, err := step.Run(bare, workflow.NewStore()); err != nil {
		t.Fatalf("run under a zero config: %v", err)
	}
}

// Observation and resumption are independent: either alone must work.
func TestRunConfig_eitherHalfAlone(t *testing.T) {
	step := workflow.Leaf("a", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	var seen int
	onlyObserver := workflow.WithConfig(context.Background(), workflow.RunConfig{
		Observer: workflow.ObserverFunc(func(context.Context, workflow.Event) { seen++ }),
	})
	if _, err := step.Run(onlyObserver, workflow.NewStore()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if seen != 2 {
		t.Fatalf("events = %d; want 2 with no journal attached", seen)
	}

	journal := workflow.NewJournal()
	onlyJournal := workflow.WithConfig(context.Background(), workflow.RunConfig{Journal: journal})
	if _, err := step.Run(onlyJournal, workflow.NewStore()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if journal.Len() != 1 {
		t.Fatalf("journal recorded %d steps with no observer attached; want 1", journal.Len())
	}
}
