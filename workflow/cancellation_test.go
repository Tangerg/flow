package workflow_test

import (
	"context"
	"errors"
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

func TestRunClosesItsExecutionContext(t *testing.T) {
	assertClosed := func(t *testing.T, ctx context.Context) {
		t.Helper()
		select {
		case <-ctx.Done():
			if !errors.Is(context.Cause(ctx), context.Canceled) {
				t.Fatalf("execution context cause = %v; want context.Canceled", context.Cause(ctx))
			}
		default:
			t.Fatal("execution context remains live after Run ended")
		}
	}

	t.Run("return", func(t *testing.T) {
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

	t.Run("panic", func(t *testing.T) {
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
		if value, getErr := workflow.Get[int](output, workflow.Output("seed")); getErr != nil || value != 1 {
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
		if value, getErr := workflow.Get[int](output, workflow.Output("seed")); getErr != nil || value != 1 {
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
			workflow.From[int](workflow.Output("seed")),
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
			output, err := workflow.Run(ctx, test.step, test.input, workflow.RunConfig{
				Journal: test.journal,
				Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
					if event.Kind == test.event {
						seen = true
						cancel(cause)
					}
				}),
			})
			if !seen || !errors.Is(err, cause) || workflow.SuspendedOnly(err) {
				t.Fatalf("Run error = %v, event seen = %t; want cancellation cause", err, seen)
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
		Inputs: workflow.DefaultInput(workflow.Output("seed")),
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
	if value, getErr := workflow.Get[string](outcome.store, workflow.Output("completed")); getErr != nil || value != "fresh" {
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
		step := workflow.Branch(workflow.BranchConfig{ID: "branch", Resolve: resolverNode(func(context.Context, workflow.Store) (string, error) {
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
		step := workflow.Branch(workflow.BranchConfig{ID: "branch", Resolve: resolverNode(func(context.Context, workflow.Store) (string, error) {
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
		step := workflow.Branch(workflow.BranchConfig{ID: "branch", Resolve: resolverNode(func(context.Context, workflow.Store) (string, error) {
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
