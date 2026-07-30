package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestRoute_publishesAndReplaysResolverDecision(t *testing.T) {
	var calls atomic.Int64
	route := workflow.Route(
		"route",
		func(_ context.Context, store workflow.Store) (string, error) {
			calls.Add(1)
			value, err := workflow.Get[int](store, workflow.Output("input"))
			if err != nil {
				return "", err
			}
			if value >= 0 {
				return "yes", nil
			}
			return "no", nil
		},
	)
	journal := workflow.NewJournal()
	input := workflow.NewStore().WithOutput("input", 1)

	first, runErr := workflow.Run(
		t.Context(),
		route,
		input,
		workflow.RunConfig{Journal: journal},
	)
	if runErr != nil {
		t.Fatalf("first run: %v", runErr)
	}
	if got, err := workflow.Get[string](first, workflow.Output("route")); err != nil || got != "yes" {
		t.Fatalf("route output = %q, %v; want yes, nil", got, err)
	}

	replayed, runErr := workflow.Run(
		t.Context(),
		route,
		input.WithOutput("input", -1),
		workflow.RunConfig{Journal: journal},
	)
	if runErr != nil {
		t.Fatalf("replay: %v", runErr)
	}
	if got, err := workflow.Get[string](replayed, workflow.Output("route")); err != nil || got != "yes" {
		t.Fatalf("replayed output = %q, %v; want yes, nil", got, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("resolver calls = %d; want 1", calls.Load())
	}
}

func TestRoute_reportsNilResolver(t *testing.T) {
	var resolve workflow.Resolver
	_, err := workflow.Route("route", resolve).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("error = %v; want flow.ErrNilNode", err)
	}
}
