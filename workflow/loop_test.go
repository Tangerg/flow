package workflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow/core"
	"github.com/Tangerg/flow/workflow"
)

func TestLoop_untilDone(t *testing.T) {
	// Increment a counter until it reaches 3, threading through the Store.
	bind := func(s workflow.Store) (int, error) {
		if v, ok := s.Get("step", workflow.OutputKey); ok {
			return v.(int), nil
		}
		v, _ := s.Get("start", "output")
		return v.(int), nil
	}
	body := workflow.Adapt("step", bind, core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil }))

	done := func(_ context.Context, _ int, s workflow.Store) (bool, error) {
		v, _ := s.Get("step", workflow.OutputKey)
		return v.(int) >= 3, nil
	}

	loop := workflow.Loop(body, done, core.WithMaxIterations(10))

	out, err := loop.Run(context.Background(), workflow.NewStore().With("start", "output", 0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Get("step", workflow.OutputKey); !ok || v.(int) != 3 {
		t.Fatalf("final counter = %v, %v; want 3", v, ok) // 0 -> 1 -> 2 -> 3
	}
}

func TestLoop_nilCondition(t *testing.T) {
	body := workflow.Adapt("x",
		workflow.FromRef[int](workflow.Ref{NodeID: "start", Path: "output"}),
		core.Func[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)

	_, err := workflow.Loop(body, nil).Run(context.Background(), workflow.NewStore().With("start", "output", 1))
	if !errors.Is(err, core.ErrNilFunc) {
		t.Fatalf("err = %v; want ErrNilFunc", err)
	}
}

func TestLoop_maxIterations(t *testing.T) {
	body := workflow.Adapt("x",
		func(workflow.Store) (int, error) { return 0, nil },
		core.Func[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	done := func(context.Context, int, workflow.Store) (bool, error) { return false, nil } // never done

	_, err := workflow.Loop(body, done, core.WithMaxIterations(3)).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, core.ErrMaxIterations) {
		t.Fatalf("err = %v; want ErrMaxIterations", err)
	}
}

func TestLoop_conditionError(t *testing.T) {
	boom := errors.New("condition failed")
	body := workflow.Adapt("x",
		func(workflow.Store) (int, error) { return 0, nil },
		core.Func[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	)
	done := func(context.Context, int, workflow.Store) (bool, error) { return false, boom }

	_, err := workflow.Loop(body, done).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v; want condition error", err)
	}
}
