package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestBranch_routes(t *testing.T) {
	label := func(text string) workflow.Step {
		return workflow.Leaf(text,
			workflow.From[int](workflow.Ref{NodeID: "start", Path: "output"}),
			flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return text, nil }),
		)
	}
	cases := map[string]workflow.Step{"pos": label("pos"), "neg": label("neg")}

	resolve := func(_ context.Context, s workflow.Store) (string, error) {
		v, _ := s.Lookup(workflow.At("start", "output"))
		if v.(int) >= 0 {
			return "pos", nil
		}
		return "neg", nil
	}
	b := workflow.Branch(resolve, cases)

	out, err := b.Run(context.Background(), workflow.NewStore().WithOutput("start", 5))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("pos")); !ok || v.(string) != "pos" {
		t.Fatalf("expected pos branch to run, got %v, %v", v, ok)
	}
	if _, ok := out.Lookup(workflow.Output("neg")); ok {
		t.Fatal("neg branch should not have run")
	}
}

func TestBranch_noCase(t *testing.T) {
	resolve := func(_ context.Context, _ workflow.Store) (string, error) { return "missing", nil }

	_, err := workflow.Branch(resolve, map[string]workflow.Step{}).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, flow.ErrNoCase) {
		t.Fatalf("error = %v, want flow.ErrNoCase", err)
	}
}

func TestBranch_nilResolver(t *testing.T) {
	_, err := workflow.Branch(nil, map[string]workflow.Step{"x": leafStep("x")}).
		Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("err = %v; want ErrNilFunc", err)
	}
}
