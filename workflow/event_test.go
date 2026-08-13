package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	cfg := workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		events = append(events, event)
	})}

	_, err := workflow.Run(t.Context(), workflow.Sequence(a, b),
		workflow.NewStore().WithOutput("start", 1), cfg)
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
		workflow.From[int](workflow.Ref{NodeID: "start", Path: "/output"}),
		flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }),
	)

	var events []workflow.Event
	cfg := workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		events = append(events, event)
	})}

	_, _ = workflow.Run(t.Context(), bad, workflow.NewStore().WithOutput("start", 1), cfg)

	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	f := events[1]
	if f.Kind != workflow.EventFailed || f.ID != "bad" || !errors.Is(f.Err, boom) {
		t.Fatalf("event 1 = %#v, want failed bad with boom", events[1])
	}
}

func TestEvents_distinguishValidationReplayAndAdmission(t *testing.T) {
	record := func(events *[]workflow.Event) workflow.ObserverFunc {
		return func(_ context.Context, event workflow.Event) {
			*events = append(*events, event)
		}
	}

	t.Run("validation failure has no start", func(t *testing.T) {
		var events []workflow.Event
		_, err := workflow.Run(
			t.Context(),
			workflow.Leaf[int, int]("", workflow.From[int](workflow.Output("seed")), flow.NodeFunc[int, int](nil)),
			workflow.NewStore(),
			workflow.RunConfig{Observer: record(&events)},
		)
		if !errors.Is(err, workflow.ErrInvalidStepID) ||
			len(events) != 1 ||
			events[0].Kind != workflow.EventFailed {
			t.Fatalf("error, events = %v, %+v; want one validation failure", err, events)
		}
	})

	t.Run("replay has only skipped", func(t *testing.T) {
		journal := workflow.NewJournal()
		if err := journal.Record(workflow.JournalKey{ID: "leaf"}, 7); err != nil {
			t.Fatalf("Record: %v", err)
		}
		var events []workflow.Event
		step := workflow.Leaf(
			"leaf",
			workflow.From[int](workflow.Output("seed")),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				t.Fatal("replayed node ran")
				return 0, nil
			}),
		)
		_, err := workflow.Run(
			t.Context(),
			step,
			workflow.NewStore(),
			workflow.RunConfig{Journal: journal, Observer: record(&events)},
		)
		if err != nil || len(events) != 1 || events[0].Kind != workflow.EventSkipped {
			t.Fatalf("error, events = %v, %+v; want one skipped event", err, events)
		}
	})

	t.Run("rejected admission reports a failure without a start", func(t *testing.T) {
		leaf := workflow.Leaf(
			"leaf",
			workflow.From[int](workflow.Output("seed")),
			flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) { return in, nil }),
		)
		// The definition is valid, so nothing before admission rejects it. Reaching
		// the same boundary twice claims one identity twice, which is the other way
		// admission fails -- and the only one that fails a boundary that already ran.
		twice := flow.NodeFunc[workflow.Store, workflow.Store](
			func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
				next, err := leaf.Run(ctx, store)
				if err != nil {
					return next, err
				}
				return leaf.Run(ctx, next)
			},
		)
		var events []workflow.Event
		_, err := workflow.Run(
			t.Context(),
			twice,
			workflow.NewStore().WithOutput("seed", 1),
			workflow.RunConfig{Observer: record(&events)},
		)
		if !errors.Is(err, workflow.ErrDuplicateStep) {
			t.Fatalf("error = %v; want ErrDuplicateStep", err)
		}
		kinds := make([]workflow.EventKind, len(events))
		for index, event := range events {
			kinds[index] = event.Kind
		}
		want := []workflow.EventKind{
			workflow.EventStarted,
			workflow.EventCompleted,
			workflow.EventFailed,
		}
		if !slices.Equal(kinds, want) {
			t.Fatalf("events = %v; want %v", kinds, want)
		}
		if !errors.Is(events[2].Err, workflow.ErrDuplicateStep) {
			t.Fatalf("failure event error = %v; want ErrDuplicateStep", events[2].Err)
		}
	})

	t.Run("cancellation before admission has no event", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		cause := errors.New("stop before admission")
		cancel(cause)
		var events []workflow.Event
		step := workflow.Leaf(
			"leaf",
			workflow.From[int](workflow.Output("seed")),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				t.Fatal("cancelled node ran")
				return 0, nil
			}),
		)
		_, err := workflow.Run(
			ctx,
			step,
			workflow.NewStore().WithOutput("seed", 1),
			workflow.RunConfig{Observer: record(&events)},
		)
		if !errors.Is(err, cause) || len(events) != 0 {
			t.Fatalf("error, events = %v, %+v; want cancellation and no event", err, events)
		}
	})
}

func TestEvents_noObserverIsFine(t *testing.T) {
	a := workflow.Leaf("a",
		workflow.From[int](workflow.Ref{NodeID: "start", Path: "/output"}),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	// No observer in context: emit must be a no-op, not panic.
	if _, err := a.Run(t.Context(), workflow.NewStore().WithOutput("start", 1)); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestEvents_carrySequenceElapsedAndStore(t *testing.T) {
	a := workflow.Leaf("a",
		workflow.From[int](workflow.Output("start")),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil }),
	)

	var events []workflow.Event
	cfg := workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		events = append(events, event)
	})}

	in := workflow.NewStore().WithOutput("start", 21)
	if _, err := workflow.Run(t.Context(), a, in, cfg); err != nil {
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
	cfg := workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		if event.Kind == workflow.EventFailed {
			failed = event
		}
	})}
	_, _ = workflow.Run(t.Context(), bad, workflow.NewStore().WithOutput("start", 1), cfg)

	if _, ok := failed.Store.Lookup(workflow.Output("bad")); ok {
		t.Fatal("failed event Store holds an output; a failed step produces none")
	}
	if failed.Seq != 2 {
		t.Fatalf("Seq = %d; want 2", failed.Seq)
	}
}

func TestEvents_scopeDistinguishesIterationElements(t *testing.T) {
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

	var scopes []string
	cfg := workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		if event.Kind == workflow.EventCompleted {
			scopes = append(scopes, scopeText(event.Scope))
		}
	})}

	if _, err := workflow.Run(t.Context(), step,
		workflow.NewStore().WithOutput("start", []any{1, 2, 3}), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{"iter[0]", "iter[1]", "iter[2]"}
	if !slices.Equal(scopes, want) {
		t.Fatalf("scopes = %v; want %v", scopes, want)
	}
}

func TestEvents_scopeDistinguishesLoopIterations(t *testing.T) {
	count := 0
	body := workflow.Leaf("tick",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { count++; return count, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	// A condition runs inside the iteration's indexed scope, which is where a
	// decision that depends on the iteration index reads it from.
	done := flow.NodeFunc[workflow.Store, bool](
		func(ctx context.Context, _ workflow.Store) (bool, error) {
			frames := workflow.Scope(ctx)
			return frames[len(frames)-1].Index >= 2, nil
		},
	)

	var scopes []string
	cfg := workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
		if event.Kind == workflow.EventCompleted {
			scopes = append(scopes, scopeText(event.Scope))
		}
	})}

	if _, err := workflow.Run(t.Context(),
		workflow.Loop(workflow.LoopConfig{ID: "loop", Body: body, Condition: done}),
		workflow.NewStore(), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}
	if want := []string{"loop[0]", "loop[1]", "loop[2]"}; !slices.Equal(scopes, want) {
		t.Fatalf("scopes = %v; want %v", scopes, want)
	}
}

func TestWithScope_isMaintainedWithoutAnObserver(t *testing.T) {
	// A scope identifies a step rather than labelling it: a Journal keys its
	// records by the scope, so it has to exist whether or not anything is
	// watching. Tying it to the observer let a journaled Loop skip every
	// iteration after the first.
	bare := workflow.WithScope(t.Context(), "kept")
	if scope := workflow.Scope(bare); !slices.Equal(scope, ordinaryScope("kept")) {
		t.Fatalf("Scope = %v; want [kept] even with no observer", scope)
	}
	if scope := workflow.Scope(t.Context()); scope != nil {
		t.Fatalf("Scope = %v; want nil at the top level", scope)
	}
}

func TestWithScope_nests(t *testing.T) {
	outer := workflow.WithScope(t.Context(), "a")
	inner := workflow.WithScope(outer, "b")
	if !slices.Equal(workflow.Scope(inner), ordinaryScope("a", "b")) {
		t.Fatalf("inner scope = %v; want [a b]", workflow.Scope(inner))
	}
	// Deriving a sibling must not disturb the outer scope.
	if !slices.Equal(workflow.Scope(outer), ordinaryScope("a")) {
		t.Fatalf("outer scope = %v; want [a]", workflow.Scope(outer))
	}
}

func TestScope_returnsACopy(t *testing.T) {
	ctx := workflow.WithScope(t.Context(), "original")
	scope := workflow.Scope(ctx)
	scope[0].ID = "changed"
	if got := workflow.Scope(ctx); !slices.Equal(got, ordinaryScope("original")) {
		t.Fatalf("Scope leaked context-owned storage: %v", got)
	}
}

func TestWithObserver_nilObserverIsInert(t *testing.T) {
	a := workflow.Leaf("a",
		workflow.From[int](workflow.Output("start")),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	if _, err := workflow.Run(t.Context(), a,
		workflow.NewStore().WithOutput("start", 1), workflow.RunConfig{}); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestObserverFunc(t *testing.T) {
	var got workflow.Event
	observer := workflow.ObserverFunc(func(_ context.Context, event workflow.Event) { got = event })
	observer.Observe(t.Context(), workflow.Event{Kind: workflow.EventStarted, ID: "a"})
	if got.Kind != workflow.EventStarted || got.ID != "a" {
		t.Fatalf("event = %#v", got)
	}
}

func TestObserverContextCannotJoinObservedRun(t *testing.T) {
	type contextKey struct{}

	callback := workflow.Leaf(
		"callback",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(ctx context.Context, input int) (int, error) {
			if got := ctx.Value(contextKey{}); got != "kept" {
				return 0, fmt.Errorf("callback context value = %v; want kept", got)
			}
			if scope := workflow.Scope(ctx); scope != nil {
				return 0, fmt.Errorf("callback workflow scope = %v; want nil", scope)
			}
			return input, nil
		}),
	)
	outer := workflow.Leaf(
		"outer",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 2, nil }),
	)

	journal := workflow.NewJournal()
	var (
		events      []workflow.Event
		callbackErr error
	)
	ctx := context.WithValue(t.Context(), contextKey{}, "kept")
	ctx = workflow.WithScope(ctx, "root")
	_, err := workflow.Run(ctx, outer, workflow.NewStore(), workflow.RunConfig{
		Journal: journal,
		Observer: workflow.ObserverFunc(func(ctx context.Context, event workflow.Event) {
			events = append(events, event)
			if event.Kind == workflow.EventStarted && event.ID == "outer" {
				_, callbackErr = callback.Run(ctx, workflow.NewStore())
			}
		}),
	})
	if err != nil || callbackErr != nil {
		t.Fatalf("Run, callback = %v, %v; want nil, nil", err, callbackErr)
	}
	if len(events) != 2 || events[0].ID != "outer" || events[1].ID != "outer" {
		t.Fatalf("events = %+v; callback joined the observed run", events)
	}
	wantKey := workflow.JournalKey{ID: "outer", Scope: []workflow.ScopeFrame{{ID: "root"}}}
	if keys := journal.Keys(); !equalJournalKeys(keys, []workflow.JournalKey{wantKey}) {
		t.Fatalf("Journal keys = %+v; callback wrote into the observed run", keys)
	}
}

func TestRun_startsEachEventSequenceAtOne(t *testing.T) {
	step := workflow.Leaf("a", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	for run := range 2 {
		var sequence []uint64
		cfg := workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			sequence = append(sequence, event.Seq)
		})}
		if _, err := workflow.Run(t.Context(), step, workflow.NewStore(), cfg); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if !slices.Equal(sequence, []uint64{1, 2}) {
			t.Fatalf("run %d sequence = %v; want [1 2]", run, sequence)
		}
	}
}

func TestRun_rejectsNilStep(t *testing.T) {
	in := workflow.NewStore().WithOutput("start", 1)
	out, err := workflow.Run(t.Context(), nil, in, workflow.RunConfig{})
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want ErrNilStep", err)
	}
	if got, _ := workflow.Get[int](out, workflow.Output("start")); got != 1 {
		t.Fatalf("Run changed its input Store: %d", got)
	}

	var invalid flow.NodeFunc[workflow.Store, workflow.Store]
	out, err = workflow.Run(t.Context(), invalid, in, workflow.RunConfig{})
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("typed nil err = %v; want ErrNilStep", err)
	}
	if got, _ := workflow.Get[int](out, workflow.Output("start")); got != 1 {
		t.Fatalf("typed nil Run changed its input Store: %d", got)
	}
}

// Observation and resumption are independent: either alone must work.
func TestRunConfig_eitherHalfAlone(t *testing.T) {
	step := workflow.Leaf("a", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	var seen int
	onlyObserver := workflow.RunConfig{
		Observer: workflow.ObserverFunc(func(context.Context, workflow.Event) { seen++ }),
	}
	if _, err := workflow.Run(t.Context(), step, workflow.NewStore(), onlyObserver); err != nil {
		t.Fatalf("run: %v", err)
	}
	if seen != 2 {
		t.Fatalf("events = %d; want 2 with no journal attached", seen)
	}

	journal := workflow.NewJournal()
	onlyJournal := workflow.RunConfig{Journal: journal}
	if _, err := workflow.Run(t.Context(), step, workflow.NewStore(), onlyJournal); err != nil {
		t.Fatalf("run: %v", err)
	}
	if journal.Len() != 1 {
		t.Fatalf("journal recorded %d steps with no observer attached; want 1", journal.Len())
	}
}
