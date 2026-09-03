package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

type cancelingJSON struct {
	cancel  context.CancelCauseFunc
	cause   error
	encoded string
	err     error
}

func (c cancelingJSON) MarshalJSON() ([]byte, error) {
	c.cancel(c.cause)
	if c.err != nil {
		return nil, c.err
	}
	return []byte(c.encoded), nil
}

// TestEveryBoundaryClosesTheContextItDerived holds all three derived-context
// boundaries in this package to one rule: a boundary that hands its children a
// context of its own ends that context before it returns, so a child goroutine
// left behind stops and the parent stops accumulating children it will never
// cancel. Each boundary is asked where only its own cancel can answer -- the
// graph runs on its own, and the leaf's emission context is checked from the
// following step, because by the time Run has returned its own cancel would have
// closed everything below it. [flow.Race] derives one too and is held to the
// same rule by TestRace_closesTheContextItDerived.
func TestEveryBoundaryClosesTheContextItDerived(t *testing.T) {
	assertClosed := func(t *testing.T, ctx context.Context) {
		t.Helper()
		select {
		case <-ctx.Done():
			if !errors.Is(context.Cause(ctx), context.Canceled) {
				t.Fatalf("derived context cause = %v; want context.Canceled", context.Cause(ctx))
			}
		default:
			t.Fatal("derived context remains live after its boundary ended")
		}
	}

	t.Run("run", func(t *testing.T) {
		executionContexts := make(chan context.Context, 1)
		step := flow.NodeFunc[workflow.Store, workflow.Store](
			func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
				executionContexts <- ctx
				return store, nil
			},
		)
		if _, err := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{}); err != nil {
			t.Fatalf("Run: %v", err)
		}
		assertClosed(t, <-executionContexts)
	})

	t.Run("run panic", func(t *testing.T) {
		const panicValue = "run panic"
		executionContexts := make(chan context.Context, 1)
		step := flow.NodeFunc[workflow.Store, workflow.Store](
			func(ctx context.Context, _ workflow.Store) (workflow.Store, error) {
				executionContexts <- ctx
				panic(panicValue)
			},
		)

		func() {
			defer func() {
				if recovered := recover(); recovered != panicValue {
					t.Fatalf("recovered = %v; want %q", recovered, panicValue)
				}
			}()
			_, _ = workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{})
		}()
		assertClosed(t, <-executionContexts)
	})

	// The graph step runs directly on the test's context, so nothing above it can
	// close what its nodes ran under.
	t.Run("graph", func(t *testing.T) {
		nodeContexts := make(chan context.Context, 1)
		registry := workflow.NewRegistry().MustRegisterNode(
			"capture",
			workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
				return flow.NodeFunc[int, int](func(ctx context.Context, input int) (int, error) {
					nodeContexts <- ctx
					return input, nil
				}), nil
			}),
		)
		step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
			ID:     "owned",
			Type:   "capture",
			Inputs: workflow.OneInput(workflow.Output("seed")),
		}}})
		if err != nil {
			t.Fatalf("CompileGraph: %v", err)
		}

		if _, err := step.Run(t.Context(), workflow.NewStore().WithOutput("seed", 1)); err != nil {
			t.Fatalf("Run: %v", err)
		}
		assertClosed(t, <-nodeContexts)
	})

	// A leaf derives an emission context only when the run has an Emitter, and it
	// is that context the node runs under. The next step asks while the run is
	// still going, which is the only place the leaf's own cancel is visible.
	t.Run("leaf emission", func(t *testing.T) {
		// The following step is the one that has to look: by the time Run has
		// returned, its own cancel has closed everything under it whatever the leaf
		// did. So that step reports only what it saw, and the assertions run below.
		emissionContexts := make(chan context.Context, 1)
		closedInTime := make(chan bool, 1)
		step := workflow.Sequence(
			workflow.LeafFunc(
				"first",
				workflow.Output("seed"),
				func(ctx context.Context, input int) (int, error) {
					emissionContexts <- ctx
					return input, nil
				},
			),
			workflow.LeafFunc(
				"second",
				workflow.Output("first"),
				func(_ context.Context, input int) (int, error) {
					emissionCtx := <-emissionContexts
					select {
					case <-emissionCtx.Done():
						closedInTime <- true
					default:
						closedInTime <- false
					}
					emissionContexts <- emissionCtx // Put it back for the assertions below.
					return input, nil
				},
			),
		)

		if _, err := workflow.Run(
			t.Context(),
			step,
			workflow.NewStore().WithOutput("seed", 1),
			workflow.RunConfig{Emitter: workflow.EmitterFunc(
				func(context.Context, workflow.Chunk) error { return nil },
			)},
		); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !<-closedInTime {
			t.Fatal("the leaf's emission context was still live in the following step")
		}
		assertClosed(t, <-emissionContexts)
	})
}

func TestSequence_parentCancellationStopsAndRollsBackAChild(t *testing.T) {
	cause := errors.New("stop sequence")

	t.Run("without children", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(cause)
		input := workflow.NewStore().WithOutput("seed", 1)
		output, err := workflow.Sequence().Run(ctx, input)
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
		if value, getErr := output.Get[int](workflow.Output("seed")); getErr != nil || value != 1 {
			t.Fatalf("seed = %v, %v; want unchanged input", value, getErr)
		}
	})

	t.Run("before child", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(cause)
		called := false
		step := workflow.Sequence(flow.NodeFunc[workflow.Store, workflow.Store](
			func(context.Context, workflow.Store) (workflow.Store, error) {
				called = true
				return workflow.NewStore(), nil
			},
		))

		input := workflow.NewStore().WithOutput("seed", 1)
		output, err := step.Run(ctx, input)
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
		if called {
			t.Fatal("child ran under an already-cancelled context")
		}
		if value, getErr := output.Get[int](workflow.Output("seed")); getErr != nil || value != 1 {
			t.Fatalf("seed = %v, %v; want unchanged input", value, getErr)
		}
	})

	t.Run("during child", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		step := workflow.Sequence(flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				cancel(cause)
				return store.WithOutput("partial", 1), nil
			},
		))

		input := workflow.NewStore().WithOutput("seed", 1)
		output, err := step.Run(ctx, input)
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
		if _, ok := output.Lookup(workflow.Output("partial")); ok {
			t.Fatal("Sequence committed the cancelled child's write")
		}
	})
}

func TestLeaf_parentCancellationStopsEveryPreNodeCallback(t *testing.T) {
	cause := errors.New("stop leaf")

	t.Run("observer", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		called := false
		step := workflow.Leaf(
			"leaf",
			workflow.Output("seed").Bind[int](),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				called = true
				return 2, nil
			}),
		)
		journal := workflow.NewJournal()
		_, err := workflow.Run(ctx, step, workflow.NewStore().WithOutput("seed", 1), workflow.RunConfig{
			Journal: journal,
			Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
				if event.Kind == workflow.EventStarted {
					cancel(cause)
				}
			}),
		})
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
		if called || journal.Len() != 0 {
			t.Fatalf("node called = %t, Journal.Len = %d; want false, 0", called, journal.Len())
		}
	})

	t.Run("binder", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		called := false
		binderErr := errors.New("binder failed")
		step := workflow.Leaf(
			"leaf",
			workflow.BinderFunc[int](func(workflow.Store) (int, error) {
				cancel(cause)
				return 0, binderErr
			}),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				called = true
				return 2, nil
			}),
		)

		_, err := step.Run(ctx, workflow.NewStore())
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
		if called {
			t.Fatal("node ran after its binder cancelled the parent")
		}
	})
}

func TestWaitingSteps_preferParentCancellation(t *testing.T) {
	cause := errors.New("stop wait")
	for name, step := range map[string]workflow.Step{
		"await":     workflow.Await("wait", workflow.Output("missing")),
		"interrupt": workflow.Interrupt("wait", "request"),
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			cancel(cause)
			_, err := step.Run(ctx, workflow.NewStore())
			if !errors.Is(err, cause) || workflow.SuspendedOnly(err) {
				t.Fatalf("Run error = %v; want cancellation cause, not suspension", err)
			}
		})
	}
}

func TestTerminalObserverCancellationWins(t *testing.T) {
	cause := errors.New("cancel during terminal event")

	leaf := func(output int, err error) workflow.Step {
		return workflow.Leaf(
			"leaf",
			workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) { return struct{}{}, nil }),
			flow.NodeFunc[struct{}, int](func(context.Context, struct{}) (int, error) {
				return output, err
			}),
		)
	}

	tests := []struct {
		name    string
		step    workflow.Step
		input   workflow.Store
		journal *workflow.Journal
		event   workflow.EventKind
	}{
		{
			name:  "completed leaf",
			step:  leaf(1, nil),
			input: workflow.NewStore(),
			event: workflow.EventCompleted,
		},
		{
			name:    "replayed leaf",
			step:    leaf(0, errors.New("replayed leaf ran")),
			input:   workflow.NewStore(),
			journal: journalWith(t, workflow.JournalKey{ID: "leaf"}, 1),
			event:   workflow.EventSkipped,
		},
		{
			name:  "suspended leaf",
			step:  leaf(0, workflow.Suspend("wait")),
			input: workflow.NewStore(),
			event: workflow.EventSuspended,
		},
		{
			name:  "completed await",
			step:  workflow.Await("wait", workflow.Output("ready")),
			input: workflow.NewStore().WithOutput("ready", true),
			event: workflow.EventCompleted,
		},
		{
			name:  "suspended await",
			step:  workflow.Await("wait", workflow.Output("ready")),
			input: workflow.NewStore(),
			event: workflow.EventSuspended,
		},
		{
			name:    "replayed interrupt",
			step:    workflow.Interrupt("wait", "request"),
			input:   workflow.NewStore(),
			journal: journalWith(t, workflow.JournalKey{ID: "wait"}, "response"),
			event:   workflow.EventSkipped,
		},
		{
			name:  "suspended interrupt",
			step:  workflow.Interrupt("wait", "request"),
			input: workflow.NewStore(),
			event: workflow.EventSuspended,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			seen := false
			var observed []workflow.EventKind
			output, err := workflow.Run(ctx, test.step, test.input, workflow.RunConfig{
				Journal: test.journal,
				Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
					observed = append(observed, event.Kind)
					if event.Kind == test.event {
						seen = true
						cancel(cause)
					}
				}),
			})
			if !seen || !errors.Is(err, cause) || workflow.SuspendedOnly(err) {
				t.Fatalf("Run error = %v, event seen = %t; want cancellation cause", err, seen)
			}
			// Observer promises the cancellation is sampled before the boundary
			// returns. Returning the cause says it was sampled; emitting nothing
			// further is what says it was sampled before returning -- a boundary that
			// went on to announce a start would reach the same error the next time it
			// looked.
			if last := observed[len(observed)-1]; last != test.event {
				t.Fatalf("events = %v; want none after the %s that cancelled", observed, test.event)
			}
			if _, published := output.Lookup(workflow.Output("leaf")); published {
				t.Fatal("cancelled leaf terminal event published its output")
			}
			if _, published := output.Lookup(workflow.Output("wait")); published {
				t.Fatal("cancelled wait terminal event published its output")
			}
		})
	}
}

func journalWith(t *testing.T, key workflow.JournalKey, value any) *workflow.Journal {
	t.Helper()
	journal := workflow.NewJournal()
	if err := journal.Record(key, value); err != nil {
		t.Fatalf("Record: %v", err)
	}
	return journal
}

func TestGraph_cancellationBeforeAdmissionPreservesInput(t *testing.T) {
	cause := errors.New("stop graph")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)

	var called bool
	registry := workflow.NewRegistry().MustRegisterNode(
		"copy",
		workflow.Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				called = true
				return 0, nil
			}), nil
		}),
	)
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{{
		ID:     "owned",
		Type:   "copy",
		Inputs: workflow.OneInput(workflow.Output("seed")),
	}}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	input := workflow.NewStore().WithOutput("seed", 1).WithOutput("owned", "stale")
	output, err := step.Run(ctx, input)
	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v; want cancellation cause", err)
	}
	if called {
		t.Fatal("graph node ran after cancellation")
	}
	if value, ok := output.Lookup(workflow.Output("owned")); !ok || value != "stale" {
		t.Fatalf("owned output = %v, %t; want unchanged stale value", value, ok)
	}
	// Preserving the values is not the whole of it: a node that never ran wrote
	// nothing, so the Store it hands back must report nothing either. Restamped
	// cells carry the same values and would read as untouched here while telling a
	// caller that persists Changes to write every one of them again.
	if changes := output.Changes(input); len(changes) != 0 {
		t.Fatalf("Changes after a graph that never admitted a node = %+v; want none", changes)
	}
}

func TestGraph_cancellationAfterAdmissionCommitsAcceptedNodesAndClearsStaleCells(t *testing.T) {
	cause := errors.New("stop admitted graph")
	blocked := make(chan struct{})
	registry := workflow.NewRegistry().
		MustRegisterNode("complete", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, string](func(context.Context, struct{}) (string, error) {
					return "fresh", nil
				}),
			), nil
		}).
		MustRegisterNode("block", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, string](func(ctx context.Context, _ struct{}) (string, error) {
					close(blocked)
					<-ctx.Done()
					return "", context.Cause(ctx)
				}),
			), nil
		})
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "completed", Type: "complete"},
		{ID: "blocked", Type: "block", DependsOn: []string{"completed"}},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	ctx, cancel := context.WithCancelCause(t.Context())
	type runResult struct {
		store workflow.Store
		err   error
	}
	result := make(chan runResult, 1)
	go func() {
		store, runErr := step.Run(
			ctx,
			workflow.NewStore().
				WithOutput("completed", "stale").
				WithOutput("blocked", "stale"),
		)
		result <- runResult{store: store, err: runErr}
	}()
	<-blocked
	cancel(cause)
	outcome := <-result

	if !errors.Is(outcome.err, cause) {
		t.Fatalf("Run error = %v; want cancellation cause", outcome.err)
	}
	if value, getErr := outcome.store.Get[string](workflow.Output("completed")); getErr != nil || value != "fresh" {
		t.Fatalf("completed output = %q, %v; want fresh, nil", value, getErr)
	}
	if _, present := outcome.store.Lookup(workflow.Output("blocked")); present {
		t.Fatal("cancelled graph retained the blocked node's stale output")
	}
}

func TestAwait_cancellationDuringLookupWins(t *testing.T) {
	cause := errors.New("stop lookup")
	ctx, cancel := context.WithCancelCause(t.Context())
	input := workflow.NewStore().WithOutput("payload", cancelingJSON{
		cancel: cancel, cause: cause, encoded: `{"ready":true}`,
	})

	_, err := workflow.Await("wait", workflow.Output("payload").Child("ready")).Run(ctx, input)
	if !errors.Is(err, cause) || workflow.SuspendedOnly(err) {
		t.Fatalf("Run error = %v; want cancellation cause", err)
	}
}

func TestBranch_parentCancellationWinsAtEveryCallBoundary(t *testing.T) {
	cause := errors.New("stop branch")

	t.Run("before resolver", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(cause)
		called := false
		step := workflow.Branch(workflow.BranchConfig{ID: "branch", Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
			called = true
			return "case", nil
		}), Cases: map[string]workflow.Step{"case": workflow.Sequence()}})

		_, err := step.Run(ctx, workflow.NewStore())
		if !errors.Is(err, cause) || called {
			t.Fatalf("Run error = %v, resolver called = %t; want cause, false", err, called)
		}
	})

	t.Run("during resolver", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		caseCalled := false
		journal := workflow.NewJournal()
		step := workflow.Branch(workflow.BranchConfig{ID: "branch", Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
			cancel(cause)
			return "", errors.New("resolver failed")
		}), Cases: map[string]workflow.Step{
			"case": flow.NodeFunc[workflow.Store, workflow.Store](func(context.Context, workflow.Store) (workflow.Store, error) {
				caseCalled = true
				return workflow.NewStore(), nil
			}),
		}})

		_, err := workflow.Run(ctx, step, workflow.NewStore(), workflow.RunConfig{Journal: journal})
		if !errors.Is(err, cause) || caseCalled || journal.Len() != 0 {
			t.Fatalf("Run error = %v, case called = %t, Journal.Len = %d; want cause, false, 0", err, caseCalled, journal.Len())
		}
	})

	t.Run("during case", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		input := workflow.NewStore().WithOutput("seed", 1)
		step := workflow.Branch(workflow.BranchConfig{ID: "branch", Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) {
			return "case", nil
		}), Cases: map[string]workflow.Step{
			"case": flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				cancel(cause)
				return store.WithOutput("partial", 1), nil
			}),
		}})

		output, err := step.Run(ctx, input)
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
		if _, ok := output.Lookup(workflow.Output("partial")); ok {
			t.Fatal("Branch committed the cancelled case's write")
		}
	})
}

func TestSubgraph_parentCancellationWinsAtEveryBoundary(t *testing.T) {
	cause := errors.New("stop subgraph")
	identity := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store.WithOutput("result", true), nil
		},
	)

	t.Run("before binding", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(cause)
		step := workflow.Subgraph(workflow.SubgraphConfig{
			ID: "sub", Body: identity, BodyOutput: workflow.Output("result"),
		})
		_, err := step.Run(ctx, workflow.NewStore())
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
	})

	t.Run("during binding", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		input := workflow.NewStore().WithOutput("payload", cancelingJSON{
			cancel: cancel, cause: cause, err: errors.New("marshal failed"),
		})
		step := workflow.Subgraph(workflow.SubgraphConfig{
			ID:         "sub",
			Inputs:     workflow.Inputs{"seed": workflow.Output("payload").Child("value")},
			Body:       identity,
			BodyOutput: workflow.Output("result"),
		})
		_, err := step.Run(ctx, input)
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
	})

	t.Run("during body", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		body := flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				cancel(cause)
				return store.WithOutput("result", true), nil
			},
		)
		step := workflow.Subgraph(workflow.SubgraphConfig{
			ID: "sub", Body: body, BodyOutput: workflow.Output("result"),
		})
		output, err := step.Run(ctx, workflow.NewStore())
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
		if _, ok := output.Lookup(workflow.Output("sub")); ok {
			t.Fatal("Subgraph published output from a cancelled body")
		}
	})

	t.Run("during projection", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		body := flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				return store.WithOutput("result", cancelingJSON{
					cancel: cancel, cause: cause, encoded: `{"value":true}`,
				}), nil
			},
		)
		step := workflow.Subgraph(workflow.SubgraphConfig{
			ID: "sub", Body: body, BodyOutput: workflow.Output("result").Child("value"),
		})
		output, err := step.Run(ctx, workflow.NewStore())
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
		if _, ok := output.Lookup(workflow.Output("sub")); ok {
			t.Fatal("Subgraph published output after projection cancelled the parent")
		}
	})

	t.Run("during failed projection", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		body := flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				return store.WithOutput("result", cancelingJSON{
					cancel: cancel,
					cause:  cause,
					err:    errors.New("marshal failed"),
				}), nil
			},
		)
		step := workflow.Subgraph(workflow.SubgraphConfig{
			ID: "sub", Body: body, BodyOutput: workflow.Output("result").Child("value"),
		})
		output, err := step.Run(ctx, workflow.NewStore())
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
		if _, ok := output.Lookup(workflow.Output("sub")); ok {
			t.Fatal("Subgraph published output after failed projection cancelled the parent")
		}
	})
}

func TestSubgraph_cancellationDuringBindingStopsLaterInputs(t *testing.T) {
	cause := errors.New("stop subgraph binding")
	ctx, cancel := context.WithCancelCause(t.Context())
	laterRead := false
	input := workflow.NewStore().
		WithOutput("first", cancelingJSON{
			cancel: cancel,
			cause:  cause,
			encoded: `{
				"value": true
			}`,
		}).
		WithOutput("later", projectionProbe{called: &laterRead})
	bodyCalled := false
	step := workflow.Subgraph(workflow.SubgraphConfig{
		ID: "sub",
		Inputs: workflow.Inputs{
			"a": workflow.Output("first").Child("value"),
			"b": workflow.Output("later").Child("value"),
		},
		Body: flow.NodeFunc[workflow.Store, workflow.Store](
			func(context.Context, workflow.Store) (workflow.Store, error) {
				bodyCalled = true
				return workflow.NewStore(), nil
			},
		),
		BodyOutput: workflow.Output("result"),
	})

	_, err := step.Run(ctx, input)
	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v; want cancellation cause", err)
	}
	if laterRead || bodyCalled {
		t.Fatalf("later input read = %t, body called = %t; want false, false", laterRead, bodyCalled)
	}
}

func TestIteration_parentCancellationWinsAtEveryCallBoundary(t *testing.T) {
	cause := errors.New("stop iteration")
	newStep := func(body workflow.Step) workflow.Step {
		return workflow.Iteration(workflow.IterationConfig{
			ID:         "items",
			Input:      workflow.Output("input"),
			Body:       body,
			BodyOutput: workflow.Output("result"),
		})
	}
	identity := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store.WithOutput("result", true), nil
		},
	)

	t.Run("before input", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(cause)
		_, err := newStep(identity).Run(
			ctx,
			workflow.NewStore().WithOutput("input", []any{1}),
		)
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
	})

	t.Run("during input", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		input := workflow.NewStore().WithOutput("input", cancelingJSON{
			cancel: cancel, cause: cause, err: errors.New("marshal failed"),
		})
		_, err := newStep(identity).Run(ctx, input)
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
	})

	t.Run("during element", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		body := flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				cancel(cause)
				return store.WithOutput("result", true), errors.New("body failed")
			},
		)
		output, err := newStep(body).Run(
			ctx,
			workflow.NewStore().WithOutput("input", []any{1}),
		)
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
		if _, ok := output.Lookup(workflow.Output("items")); ok {
			t.Fatal("Iteration published output after cancellation")
		}
	})

	t.Run("before output projection", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		marshaled := false
		body := flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				cancel(cause)
				return store.WithOutput("result", projectionProbe{called: &marshaled}), nil
			},
		)
		step := workflow.Iteration(workflow.IterationConfig{
			ID:         "items",
			Input:      workflow.Output("input"),
			Body:       body,
			BodyOutput: workflow.Output("result").Child("value"),
		})

		_, err := step.Run(
			ctx,
			workflow.NewStore().WithOutput("input", []any{1}),
		)
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
		if marshaled {
			t.Fatal("Iteration projected body output after cancellation")
		}
	})
}

type projectionProbe struct{ called *bool }

func (p projectionProbe) MarshalJSON() ([]byte, error) {
	*p.called = true
	return []byte(`{"value":true}`), nil
}

func TestCompileGraph_gateCancellationPreventsTargetAndPreservesDescription(t *testing.T) {
	cause := errors.New("stop gate")
	ctx, cancel := context.WithCancelCause(t.Context())
	targetCalled := false
	registry := workflow.NewRegistry().
		MustRegisterNode("route", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) { return struct{}{}, nil }),
				flow.NodeFunc[struct{}, cancelingJSON](func(context.Context, struct{}) (cancelingJSON, error) {
					return cancelingJSON{
						cancel: cancel,
						cause:  cause,
						err:    errors.New("route output is not JSON"),
					}, nil
				}),
			), nil
		}).
		MustRegisterSchema("route", workflow.NodeSchema{
			Output: workflow.TypeString, Outlets: []string{"go"},
		}).
		MustRegisterNode("target", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) { return struct{}{}, nil }),
				flow.NodeFunc[struct{}, int](func(context.Context, struct{}) (int, error) {
					targetCalled = true
					return 1, nil
				}),
			), nil
		})
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route"},
		{ID: "target", Type: "target", When: []workflow.Gate{workflow.When("route", "go")}},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	description := workflow.Describe(step)
	if len(description.Children) != 2 || description.Children[1].ID != "target" {
		t.Fatalf("Describe = %+v; want transparent gated target", description)
	}

	_, err = step.Run(ctx, workflow.NewStore())
	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v; want cancellation cause", err)
	}
	if targetCalled {
		t.Fatal("target ran after gate evaluation cancelled the parent")
	}
}

func TestCompileGraph_gateCancellationStopsLaterGates(t *testing.T) {
	cause := errors.New("stop gate evaluation")
	ctx, cancel := context.WithCancelCause(t.Context())
	laterGateRead := false
	targetCalled := false
	registry := workflow.NewRegistry().
		MustRegisterNode("first-route", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, cancelingJSON](func(context.Context, struct{}) (cancelingJSON, error) {
					return cancelingJSON{cancel: cancel, cause: cause, encoded: `"go"`}, nil
				}),
			), nil
		}).
		MustRegisterSchema("first-route", workflow.NodeSchema{
			Output: workflow.TypeString, Outlets: []string{"go"},
		}).
		MustRegisterNode("later-route", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, projectionProbe](func(context.Context, struct{}) (projectionProbe, error) {
					return projectionProbe{called: &laterGateRead}, nil
				}),
			), nil
		}).
		MustRegisterSchema("later-route", workflow.NodeSchema{
			Output: workflow.TypeString, Outlets: []string{"go"},
		}).
		MustRegisterNode("target", func(spec workflow.NodeSpec) (workflow.Step, error) {
			return workflow.Leaf(
				spec.ID,
				workflow.BinderFunc[struct{}](func(workflow.Store) (struct{}, error) {
					return struct{}{}, nil
				}),
				flow.NodeFunc[struct{}, int](func(context.Context, struct{}) (int, error) {
					targetCalled = true
					return 1, nil
				}),
			), nil
		})
	step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "first", Type: "first-route"},
		{ID: "later", Type: "later-route"},
		{
			ID:   "target",
			Type: "target",
			When: []workflow.Gate{
				workflow.When("first", "go"),
				workflow.When("later", "go"),
			},
		},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	_, err = step.Run(ctx, workflow.NewStore())
	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v; want cancellation cause", err)
	}
	if laterGateRead || targetCalled {
		t.Fatalf("later gate read = %t, target called = %t; want false, false", laterGateRead, targetCalled)
	}
}

// TestConcurrentCompositesLeaveNoBranchRunning pins for this layer what
// TestConcurrentCombinatorsLeaveNoChildRunning pins for flow: a composite waits
// for every branch it admitted, so none is still executing when Run returns.
// Each composite says so in prose and nothing asked. The shape is the same
// everywhere — one sibling fails, the rest hold their input until they observe
// the cancellation that failure causes, so every one of them is in flight at the
// moment the outcome is decided — and the counter is read at the moment of
// return, never after a wait, so a branch that outlived its composite cannot be
// mistaken for one that finished.
func TestConcurrentCompositesLeaveNoBranchRunning(t *testing.T) {
	boom := errors.New("boom")
	var live atomic.Int64
	hold := func(ctx context.Context) (int, error) {
		live.Add(1)
		defer live.Add(-1)
		<-ctx.Done()
		return 0, context.Cause(ctx)
	}
	seed := workflow.NewStore().WithOutput("seed", 1)

	t.Run("parallel", func(t *testing.T) {
		waiting := func(id string) workflow.Step {
			return workflow.LeafFunc(id, workflow.Output("seed"),
				func(ctx context.Context, _ int) (int, error) { return hold(ctx) })
		}
		step := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{
			workflow.LeafFunc("boom", workflow.Output("seed"),
				func(context.Context, int) (int, error) { return 0, boom }),
			waiting("first"),
			waiting("second"),
		}})
		if _, err := workflow.Run(t.Context(), step, seed, workflow.RunConfig{}); !errors.Is(err, boom) {
			t.Fatalf("Run error = %v; want the branch failure", err)
		}
		if running := live.Load(); running != 0 {
			t.Fatalf("%d branches were still running when Parallel returned", running)
		}
	})

	t.Run("iteration", func(t *testing.T) {
		step := workflow.Iteration(workflow.IterationConfig{
			ID:    "each",
			Input: workflow.Output("items"),
			Body: workflow.LeafFunc("body", workflow.Item("each"),
				func(ctx context.Context, value int) (int, error) {
					if value == 0 {
						return 0, boom
					}
					return hold(ctx)
				}),
			BodyOutput: workflow.Output("body"),
		})
		input := workflow.NewStore().WithOutput("items", []any{0, 1, 2})
		if _, err := workflow.Run(t.Context(), step, input, workflow.RunConfig{}); !errors.Is(err, boom) {
			t.Fatalf("Run error = %v; want the element failure", err)
		}
		if running := live.Load(); running != 0 {
			t.Fatalf("%d elements were still running when Iteration returned", running)
		}
	})

	t.Run("graph", func(t *testing.T) {
		registry := workflow.NewRegistry().
			MustRegisterNode("boom", workflow.Factory(
				func(struct{}) (flow.Node[int, int], error) {
					return flow.NodeFunc[int, int](
						func(context.Context, int) (int, error) { return 0, boom },
					), nil
				})).
			MustRegisterNode("wait", workflow.Factory(
				func(struct{}) (flow.Node[int, int], error) {
					return flow.NodeFunc[int, int](
						func(ctx context.Context, _ int) (int, error) { return hold(ctx) },
					), nil
				}))
		wired := workflow.Inputs{workflow.DefaultPort: workflow.Output("seed")}
		step, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
			{ID: "boom", Type: "boom", Inputs: wired},
			{ID: "first", Type: "wait", Inputs: wired},
			{ID: "second", Type: "wait", Inputs: wired},
		}})
		if err != nil {
			t.Fatalf("CompileGraph: %v", err)
		}
		if _, err := workflow.Run(t.Context(), step, seed, workflow.RunConfig{}); !errors.Is(err, boom) {
			t.Fatalf("Run error = %v; want the node failure", err)
		}
		if running := live.Load(); running != 0 {
			t.Fatalf("%d nodes were still running when the graph returned", running)
		}
	})
}
