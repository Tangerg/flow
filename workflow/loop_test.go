package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestLoop_untilDone(t *testing.T) {
	// Increment a counter until it reaches 3, threading through the Store.
	bind := workflow.BindFunc[int](func(s workflow.Store) (int, error) {
		if v, ok := s.Lookup(workflow.Output("step")); ok {
			return v.(int), nil
		}
		v, _ := s.Lookup(workflow.At("start", "output"))
		return v.(int), nil
	})
	body := workflow.Leaf("step", bind, flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil }))

	done := func(_ context.Context, _ int, s workflow.Store) (bool, error) {
		v, _ := s.Lookup(workflow.Output("step"))
		return v.(int) >= 3, nil
	}

	loop := workflow.LoopLimit(10, body, done)

	out, err := loop.Run(context.Background(), workflow.NewStore().WithOutput("start", 0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("step")); !ok || v.(int) != 3 {
		t.Fatalf("final counter = %v, %v; want 3", v, ok) // 0 -> 1 -> 2 -> 3
	}
}

func TestLoop_nilCondition(t *testing.T) {
	body := workflow.Leaf("x",
		workflow.From[int](workflow.Ref{NodeID: "start", Path: "output"}),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)

	_, err := workflow.Loop(body, nil).Run(context.Background(), workflow.NewStore().WithOutput("start", 1))
	if !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("err = %v; want ErrNilFunc", err)
	}
}

func TestLoop_maxIterations(t *testing.T) {
	body := workflow.Leaf("x",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	done := func(context.Context, int, workflow.Store) (bool, error) { return false, nil } // never done

	_, err := workflow.LoopLimit(3, body, done).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, flow.ErrMaxIterations) {
		t.Fatalf("err = %v; want ErrMaxIterations", err)
	}
}

func TestLoop_conditionError(t *testing.T) {
	boom := errors.New("condition failed")
	body := workflow.Leaf("x",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	done := func(context.Context, int, workflow.Store) (bool, error) { return false, boom }

	_, err := workflow.Loop(body, done).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v; want condition error", err)
	}
}
