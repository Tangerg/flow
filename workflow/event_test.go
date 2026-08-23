package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// record collects every event a run reports, which is what a test asserting the
// sequence of outcomes needs. A test that wants only some of them filters in its
// own closure, because the filter is part of what it is asserting.
func record(events *[]workflow.Event) workflow.ObserverFunc {
	return func(_ context.Context, event workflow.Event) {
		*events = append(*events, event)
	}
}

func TestEvents_emittedForSequence(t *testing.T) {
	from := func(id string) workflow.Binder[int] {
		return workflow.Output(id).Bind[int]()
	}
	a := workflow.Leaf("a", from("start"), flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	b := workflow.Leaf("b", from("a"), flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	var events []workflow.Event
	cfg := workflow.RunConfig{Observer: record(&events)}

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
		workflow.Ref{NodeID: "start", Path: "/output"}.Bind[int](),
		flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }),
	)

	var events []workflow.Event
	cfg := workflow.RunConfig{Observer: record(&events)}

	before := time.Now()
	_, _ = workflow.Run(t.Context(), bad, workflow.NewStore().WithOutput("start", 1), cfg)
	within := time.Since(before)

	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	f := events[1]
	if f.Kind != workflow.EventFailed || f.ID != "bad" || !errors.Is(f.Err, boom) {
		t.Fatalf("event 1 = %#v, want failed bad with boom", events[1])
	}
	// An attempt that failed was still an attempt, and it is timed like any other:
	// the work it did before failing is what a tracker is watching for.
	if f.Elapsed <= 0 || f.Elapsed > within {
		t.Fatalf("failed Elapsed = %v; want the attempt's own duration, at most %v", f.Elapsed, within)
	}
}

func TestEvents_ownMutableWorkflowErrors(t *testing.T) {
	boom := errors.New("boom")
	failing := workflow.Leaf(
		"failed",
		workflow.Output("seed").Bind[int](),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
			return 0, boom
		}),
	)
	failureCtx := workflow.WithScope(t.Context(), "outer")

	_, failure := workflow.Run(
		failureCtx,
		failing,
		workflow.NewStore().WithOutput("seed", 1),
		workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			if event.Kind != workflow.EventFailed {
				return
			}
			var stepErr *workflow.StepError
			if !errors.As(event.Err, &stepErr) {
				t.Fatalf("event error = %T; want *workflow.StepError", event.Err)
			}
			stepErr.ID = "observer-mutated"
			stepErr.Scope = append(stepErr.Scope, workflow.ScopeFrame{ID: "observer-mutated"})
			stepErr.Err = nil
			if len(event.Scope) != 1 {
				t.Fatalf("event Scope = %+v; want outer scope", event.Scope)
			}
			event.Scope[0].ID = "observer-mutated"
		})},
	)
	var stepErr *workflow.StepError
	if !errors.As(failure, &stepErr) ||
		stepErr.ID != "failed" ||
		!slices.Equal(stepErr.Scope, ordinaryScope("outer")) ||
		!errors.Is(failure, boom) {
		t.Fatalf("Run error = %#v; Observer mutation escaped its Event", failure)
	}
	if scope := workflow.Scope(failureCtx); !slices.Equal(scope, ordinaryScope("outer")) {
		t.Fatalf("context Scope = %+v; Observer mutation escaped its Event", scope)
	}

	missing := workflow.Output("missing")
	_, bindFailure := workflow.Run(
		t.Context(),
		workflow.Leaf(
			"bind",
			missing.Bind[int](),
			flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
				return value, nil
			}),
		),
		workflow.NewStore(),
		workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			if event.Kind != workflow.EventFailed {
				return
			}
			var refErr *workflow.RefError
			if !errors.As(event.Err, &refErr) {
				t.Fatalf("event error = %T; want a *workflow.RefError cause", event.Err)
			}
			refErr.Ref = workflow.Output("observer-mutated")
			refErr.Err = nil
		})},
	)
	var refErr *workflow.RefError
	if !errors.As(bindFailure, &refErr) || refErr.Ref != missing || !errors.Is(bindFailure, workflow.ErrNotFound) {
		t.Fatalf("Run bind error = %#v; nested Observer mutation escaped its Event", bindFailure)
	}

	waiting := workflow.Interrupt("approval", "approve?")
	_, suspensionErr := workflow.Run(
		t.Context(),
		waiting,
		workflow.NewStore(),
		workflow.RunConfig{Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			if event.Kind != workflow.EventSuspended {
				return
			}
			var wait *workflow.Suspension
			if !errors.As(event.Err, &wait) {
				t.Fatalf("event error = %T; want *workflow.Suspension", event.Err)
			}
			wait.ID = "observer-mutated"
			wait.Scope = append(wait.Scope, workflow.ScopeFrame{ID: "observer-mutated"})
			wait.Value = "observer-mutated"
		})},
	)
	waits := workflow.Suspensions(suspensionErr)
	if len(waits) != 1 || waits[0].ID != "approval" || waits[0].Value != "approve?" {
		t.Fatalf("Run suspensions = %+v; Observer mutation escaped its Event", waits)
	}
}

// TestEventsOwnMutableErrorsAcrossInternalLocations extends the Event ownership
// boundary through the location wrappers introduced by built-in composition.
// A detail or collection index is immutable presentation state, but it must not
// hide a mutable workflow error beneath it from the snapshot walk.
func TestEventsOwnMutableErrorsAcrossInternalLocations(t *testing.T) {
	t.Run("detail", testEventOwnsDetailLocation)
	t.Run("index", testEventOwnsIndexLocation)
	t.Run("joined indexes", testEventOwnsJoinedLocations)
	t.Run("joined cases", testEventOwnsCaseLocations)
	t.Run("structured locations", testEventOwnsStructuredLocations)
	t.Run("factory location", testEventOwnsFactoryLocation)
}

func testEventOwnsDetailLocation(t *testing.T) {
	storeBinder := workflow.BinderFunc[workflow.Store](
		func(store workflow.Store) (workflow.Store, error) { return store, nil },
	)
	missing := workflow.Output("missing")
	body := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) { return store, nil },
	)
	step := workflow.Leaf(
		"outer",
		storeBinder,
		workflow.Subgraph(workflow.SubgraphConfig{
			ID:         "subgraph",
			Body:       body,
			BodyOutput: missing,
		}),
	)

	_, failure := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{
		Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			if event.Kind != workflow.EventFailed || event.ID != "outer" {
				return
			}
			var refErr *workflow.RefError
			if !errors.As(event.Err, &refErr) {
				t.Fatalf("event error = %T; want *workflow.RefError", event.Err)
			}
			refErr.Ref = workflow.Output("observer-mutated")
			refErr.Err = nil
		}),
	})

	var refErr *workflow.RefError
	if !errors.As(failure, &refErr) || refErr.Ref != missing || !errors.Is(failure, workflow.ErrNotFound) {
		t.Fatalf("Run error = %#v; Observer mutation crossed a detail location", failure)
	}
}

func testEventOwnsIndexLocation(t *testing.T) {
	inner := workflow.LeafFunc(
		"inner",
		workflow.Output("missing"),
		func(_ context.Context, value int) (int, error) { return value, nil },
	)
	mapped := flow.Map(inner, flow.MapConfig{})
	bind := workflow.BinderFunc[[]workflow.Store](
		func(store workflow.Store) ([]workflow.Store, error) { return []workflow.Store{store}, nil },
	)
	step := workflow.Leaf("outer", bind, mapped)

	_, failure := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{
		Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			if event.Kind != workflow.EventFailed || event.ID != "outer" {
				return
			}
			var innerErr *workflow.StepError
			if !errors.As(event.Err, &innerErr) || innerErr.ID != "outer" {
				t.Fatalf("event error = %#v; want outer StepError", event.Err)
			}
			if !errors.As(innerErr.Err, &innerErr) || innerErr.ID != "inner" {
				t.Fatalf("event nested error = %#v; want inner StepError", event.Err)
			}
			var indexErr *flow.IndexError
			if !errors.As(event.Err, &indexErr) || indexErr.Index != 0 {
				t.Fatalf("event error = %#v; want element index 0", event.Err)
			}
			indexErr.Index = 99
			innerErr.ID = "observer-mutated"
			innerErr.Err = nil
		}),
	})

	var outerErr *workflow.StepError
	if !errors.As(failure, &outerErr) {
		t.Fatalf("Run error = %#v; want outer StepError", failure)
	}
	var innerErr *workflow.StepError
	if !errors.As(outerErr.Err, &innerErr) || innerErr.ID != "inner" ||
		!errors.Is(failure, workflow.ErrNotFound) {
		t.Fatalf("Run error = %#v; Observer mutation crossed an index location", failure)
	}
	var indexErr *flow.IndexError
	if !errors.As(failure, &indexErr) || indexErr.Index != 0 {
		t.Fatalf("Run error = %#v; Observer rewrote its collection index", failure)
	}
}

func testEventOwnsJoinedLocations(t *testing.T) {
	storeBinder := workflow.BinderFunc[workflow.Store](
		func(store workflow.Store) (workflow.Store, error) { return store, nil },
	)
	first := workflow.LeafFunc(
		"first",
		workflow.Output("missing-first"),
		func(_ context.Context, value int) (int, error) { return value, nil },
	)
	second := workflow.LeafFunc(
		"second",
		workflow.Output("missing-second"),
		func(_ context.Context, value int) (int, error) { return value, nil },
	)
	step := workflow.Leaf("outer", storeBinder, flow.Race(first, second))

	_, failure := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{
		Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			if event.Kind != workflow.EventFailed || event.ID != "outer" {
				return
			}
			var indexErr *flow.IndexError
			var stepErr *workflow.StepError
			if !errors.As(event.Err, &indexErr) || indexErr.Index != 0 ||
				!errors.As(indexErr.Err, &stepErr) || stepErr.ID != "first" {
				t.Fatalf("event error = %#v; want first joined branch", event.Err)
			}
			indexErr.Index = 99
			stepErr.ID = "observer-mutated"
			stepErr.Err = nil
		}),
	})

	var indexErr *flow.IndexError
	var stepErr *workflow.StepError
	if !errors.As(failure, &indexErr) || indexErr.Index != 0 ||
		!errors.As(indexErr.Err, &stepErr) || stepErr.ID != "first" ||
		!errors.Is(failure, workflow.ErrNotFound) {
		t.Fatalf("Run error = %#v; Observer mutation crossed a joined branch", failure)
	}
}

func testEventOwnsCaseLocations(t *testing.T) {
	storeBinder := workflow.BinderFunc[workflow.Store](
		func(store workflow.Store) (workflow.Store, error) { return store, nil },
	)
	first := workflow.Leaf("first", storeBinder, flow.NodeFunc[workflow.Store, workflow.Store](nil))
	second := workflow.Leaf("second", storeBinder, flow.NodeFunc[workflow.Store, workflow.Store](nil))
	resolve := flow.NodeFunc[workflow.Store, string](
		func(context.Context, workflow.Store) (string, error) { return "first", nil },
	)
	step := workflow.Leaf("outer", storeBinder, flow.Switch(resolve,
		map[string]flow.Node[workflow.Store, workflow.Store]{
			"first":  first,
			"second": second,
		},
	))

	_, failure := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{
		Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			if event.Kind != workflow.EventFailed || event.ID != "outer" {
				return
			}
			var caseErr *flow.CaseError
			var stepErr *workflow.StepError
			if !errors.As(event.Err, &caseErr) || caseErr.Key != "first" ||
				!errors.As(caseErr.Err, &stepErr) || stepErr.ID != "first" {
				t.Fatalf("event error = %#v; want first joined case", event.Err)
			}
			caseErr.Key = "observer-mutated"
			stepErr.ID = "observer-mutated"
			stepErr.Err = nil
		}),
	})

	var caseErr *flow.CaseError
	var stepErr *workflow.StepError
	if !errors.As(failure, &caseErr) || caseErr.Key != "first" ||
		!errors.As(caseErr.Err, &stepErr) || stepErr.ID != "first" ||
		!errors.Is(failure, flow.ErrNilNode) {
		t.Fatalf("Run error = %#v; Observer mutation crossed a joined case", failure)
	}
}

func testEventOwnsStructuredLocations(t *testing.T) {
	missing := workflow.Output("missing")
	cause := &workflow.GraphError{
		Path: "/nodes/0",
		Err: &workflow.SpecError{
			Path: "/body",
			Err: &workflow.RegistrationError{
				Kind: "node",
				Name: "broken",
				Err: &workflow.RefError{
					Ref:  missing,
					Want: "int",
					Err:  workflow.ErrNotFound,
				},
			},
		},
	}
	step := workflow.Leaf(
		"outer",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 0, cause }),
	)

	_, failure := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{
		Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			if event.Kind != workflow.EventFailed {
				return
			}
			var graphErr *workflow.GraphError
			var specErr *workflow.SpecError
			var registrationErr *workflow.RegistrationError
			var refErr *workflow.RefError
			if !errors.As(event.Err, &graphErr) ||
				!errors.As(event.Err, &specErr) ||
				!errors.As(event.Err, &registrationErr) ||
				!errors.As(event.Err, &refErr) {
				t.Fatalf("event error = %#v; want every structured location", event.Err)
			}
			graphErr.Path = "/observer-mutated"
			specErr.Path = "/observer-mutated"
			registrationErr.Name = "observer-mutated"
			refErr.Ref = workflow.Output("observer-mutated")
		}),
	})

	var graphErr *workflow.GraphError
	var specErr *workflow.SpecError
	var registrationErr *workflow.RegistrationError
	var refErr *workflow.RefError
	if !errors.As(failure, &graphErr) || graphErr.Path != "/nodes/0" ||
		!errors.As(failure, &specErr) || specErr.Path != "/body" ||
		!errors.As(failure, &registrationErr) || registrationErr.Name != "broken" ||
		!errors.As(failure, &refErr) || refErr.Ref != missing ||
		!errors.Is(failure, workflow.ErrNotFound) {
		t.Fatalf("Run error = %#v; Observer mutation crossed a structured location", failure)
	}
}

func testEventOwnsFactoryLocation(t *testing.T) {
	missing := workflow.Output("missing")
	factory := workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
		return nil, &workflow.RefError{
			Ref:  missing,
			Want: "int",
			Err:  workflow.ErrNotFound,
		}
	})
	_, cause := factory(workflow.NodeSpec{
		ID:     "unused",
		Inputs: workflow.OneInput(workflow.Output("seed")),
	})
	if cause == nil {
		t.Fatal("Factory returned no error")
	}
	step := workflow.Leaf(
		"outer",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 0, cause }),
	)

	_, failure := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{
		Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			if event.Kind != workflow.EventFailed {
				return
			}
			var refErr *workflow.RefError
			if !errors.As(event.Err, &refErr) {
				t.Fatalf("event error = %#v; want factory RefError", event.Err)
			}
			refErr.Ref = workflow.Output("observer-mutated")
		}),
	})

	var refErr *workflow.RefError
	if !errors.As(failure, &refErr) || refErr.Ref != missing ||
		!errors.Is(failure, workflow.ErrNotFound) {
		t.Fatalf("Run error = %#v; Observer mutation crossed a factory location", failure)
	}
}

// TestEventsAcceptTypedNilStructuredCauses keeps observation from turning a
// malformed-but-valid error interface into a panic. Its typed nil remains the
// terminal cause; it does not acquire the category of a non-nil wrapper.
func TestEventsAcceptTypedNilStructuredCauses(t *testing.T) {
	var stepErr *workflow.StepError
	var refErr *workflow.RefError
	var registrationErr *workflow.RegistrationError
	var graphErr *workflow.GraphError
	var specErr *workflow.SpecError
	var indexErr *flow.IndexError
	var caseErr *flow.CaseError

	for name, cause := range map[string]error{
		"step":          stepErr,
		"reference":     refErr,
		"registration":  registrationErr,
		"graph":         graphErr,
		"specification": specErr,
		"index":         indexErr,
		"case":          caseErr,
	} {
		t.Run(name, func(t *testing.T) {
			step := workflow.Leaf(
				"outer",
				workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
				flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
					return 0, cause
				}),
			)

			var observed error
			_, failure := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{
				Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
					if event.Kind == workflow.EventFailed {
						observed = event.Err
					}
				}),
			})
			if observed == nil || failure == nil {
				t.Fatalf("observed, Run error = %v, %v; want two non-nil error interfaces", observed, failure)
			}
			if got := failure.Error(); got != `workflow: step "outer" run: <nil>` {
				t.Fatalf("Run error = %q; want typed nil as the terminal cause", got)
			}
		})
	}
}

// Package-owned error wrappers have exported mutable location fields, so an
// Observer receives a structural copy even when a caller assembled the tree.
// That ownership walk must use heap for an arbitrary error chain rather than
// consume one call frame per wrapper.
func TestEventsCopyDeepOwnedErrorChainWithoutStackPerWrapper(t *testing.T) {
	withBoundedStack(t, func() {
		boom := errors.New("boom")
		nodeErr := boom
		for index := range 20_000 {
			nodeErr = &workflow.StepError{
				ID:    fmt.Sprintf("inner-%05d", index),
				Scope: []workflow.ScopeFrame{{ID: "scope"}},
				Op:    workflow.OpRun,
				Err:   nodeErr,
			}
		}

		step := workflow.Leaf(
			"outer",
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				return 0, nodeErr
			}),
		)
		_, err := workflow.Run(context.Background(), step, workflow.NewStore(), workflow.RunConfig{
			Observer: workflow.ObserverFunc(func(context.Context, workflow.Event) {}),
		})
		if !errors.Is(err, boom) {
			t.Fatalf("Run error = %v; want boom in the copied chain", err)
		}
	})
}

func TestEventsCopyDeepJoinedOwnedErrorTreeWithoutStackPerBranch(t *testing.T) {
	withBoundedStack(t, func() {
		var nodeErr error
		nodeErr = errors.New("boom")
		for index := range 20_000 {
			nodeErr = errors.Join(&workflow.StepError{
				ID:  fmt.Sprintf("inner-%05d", index),
				Op:  workflow.OpRun,
				Err: nodeErr,
			})
		}

		step := workflow.Leaf(
			"outer",
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				return 0, nodeErr
			}),
		)
		_, failure := workflow.Run(context.Background(), step, workflow.NewStore(), workflow.RunConfig{
			Observer: workflow.ObserverFunc(func(context.Context, workflow.Event) {}),
		})
		message := failure.Error()
		if strings.Count(message, "workflow:") != 1 ||
			!strings.Contains(message, `step "inner-19999" run: step "inner-19998"`) ||
			!strings.HasSuffix(message, `step "inner-00000" run: boom`) {
			t.Fatal("joined error tree lost its owned locations")
		}
	})
}

func TestEvents_distinguishValidationReplayAndAdmission(t *testing.T) {
	// A rejected definition is reported under the ID it declared. The two defects
	// differ in whether there is an ID to report: an invalid one is the single
	// case where the failure cannot name its step, so a valid ID with a defect
	// behind it is what shows that the event names the step at all.
	for name, invalid := range map[string]struct {
		id   string
		want error
	}{
		"invalid id": {id: "", want: workflow.ErrInvalidStepID},
		"nil node":   {id: "leaf", want: flow.ErrNilNode},
	} {
		t.Run("validation failure has no start: "+name, func(t *testing.T) {
			var events []workflow.Event
			_, err := workflow.Run(
				t.Context(),
				workflow.Leaf[int, int](
					invalid.id,
					workflow.Output("seed").Bind[int](),
					flow.NodeFunc[int, int](nil),
				),
				workflow.NewStore(),
				workflow.RunConfig{Observer: record(&events)},
			)
			if !errors.Is(err, invalid.want) ||
				len(events) != 1 ||
				events[0].Kind != workflow.EventFailed ||
				events[0].ID != invalid.id ||
				!errors.Is(events[0].Err, invalid.want) {
				t.Fatalf("error, events = %v, %+v; want one validation failure naming %q", err, events, invalid.id)
			}
		})
	}

	t.Run("replay has only skipped", func(t *testing.T) {
		journal := workflow.NewJournal()
		if err := journal.Record(workflow.JournalKey{ID: "leaf"}, 7); err != nil {
			t.Fatalf("Record: %v", err)
		}
		var events []workflow.Event
		step := workflow.Leaf(
			"leaf",
			workflow.Output("seed").Bind[int](),
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
		// A skipped step produced its output by replay rather than by running, and
		// the event carries that Store the same way a completed one does. It is the
		// only report a tracker gets for a replayed boundary.
		if value, getErr := events[0].Store.Get[int](workflow.Output("leaf")); getErr != nil || value != 7 {
			t.Fatalf("skipped Store leaf = %v, %v; want the replayed 7", value, getErr)
		}
	})

	t.Run("rejected admission reports a failure without a start", func(t *testing.T) {
		leaf := workflow.Leaf(
			"leaf",
			workflow.Output("seed").Bind[int](),
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
		if !errors.Is(events[2].Err, workflow.ErrDuplicateStep) || events[2].ID != "leaf" {
			t.Fatalf("failure event = %+v; want the duplicate claim named on leaf", events[2])
		}
	})

	t.Run("cancellation before admission has no event", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		cause := errors.New("stop before admission")
		cancel(cause)
		var events []workflow.Event
		step := workflow.Leaf(
			"leaf",
			workflow.Output("seed").Bind[int](),
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
		workflow.Ref{NodeID: "start", Path: "/output"}.Bind[int](),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	// No observer in context: emit must be a no-op, not panic.
	if _, err := a.Run(t.Context(), workflow.NewStore().WithOutput("start", 1)); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestEvents_carrySequenceElapsedAndStore(t *testing.T) {
	a := workflow.Leaf("a",
		workflow.Output("start").Bind[int](),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil }),
	)

	var events []workflow.Event
	cfg := workflow.RunConfig{Observer: record(&events)}

	in := workflow.NewStore().WithOutput("start", 21)
	before := time.Now()
	if _, err := workflow.Run(t.Context(), a, in, cfg); err != nil {
		t.Fatalf("run: %v", err)
	}
	within := time.Since(before)

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
	// The completed event times the attempt, so it is a duration the run itself
	// contained: an unstamped start reports the age of the zero time, and no
	// measurement at all reports nothing happened.
	if completed.Elapsed <= 0 || completed.Elapsed > within {
		t.Fatalf("completed Elapsed = %v; want the attempt's own duration, at most %v", completed.Elapsed, within)
	}
	// A completed event carries the Store the step produced, which is what an
	// external tracker or persister records.
	if v, err := completed.Store.Get[int](workflow.Output("a")); err != nil || v != 42 {
		t.Fatalf("completed Store a = %v, %v; want 42", v, err)
	}
	if changes := completed.Store.Changes(in); len(changes) != 1 || changes[0].Ref != workflow.Output("a") {
		t.Fatalf("Changes = %+v; want one write to a.output", changes)
	}
}

func TestEvents_failedCarriesNoStore(t *testing.T) {
	boom := errors.New("boom")
	bad := workflow.Leaf("bad",
		workflow.Output("start").Bind[int](),
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
		workflow.Item("iter").Bind[int](),
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
		workflow.Output("start").Bind[int](),
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
	if got, _ := out.Get[int](workflow.Output("start")); got != 1 {
		t.Fatalf("Run changed its input Store: %d", got)
	}

	var invalid flow.NodeFunc[workflow.Store, workflow.Store]
	out, err = workflow.Run(t.Context(), invalid, in, workflow.RunConfig{})
	if !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("typed nil err = %v; want ErrNilStep", err)
	}
	if got, _ := out.Get[int](workflow.Output("start")); got != 1 {
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
