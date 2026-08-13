package flowx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/flowx"
)

func TestFallback(t *testing.T) {
	boom := errors.New("boom")
	primary := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	alt := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })

	got, err := flowx.Fallback(primary, alt).Run(t.Context(), 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 11 {
		t.Fatalf("got %d, want 11", got)
	}
}

// TestFallback_reportsTheAlternateFailureWhenBothFail covers the outcome the
// other tests leave out: an alternate that fails too. Nothing else observes it,
// and the failure mode is silent — a Fallback that dropped the alternate's error
// would return the zero output as a success, which is the one result a caller
// cannot detect.
func TestFallback_reportsTheAlternateFailureWhenBothFail(t *testing.T) {
	primaryErr := errors.New("primary unavailable")
	alternateErr := errors.New("alternate unavailable")
	primary := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
		return 0, primaryErr
	})
	alternate := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
		return 0, alternateErr
	})

	got, err := flowx.Fallback(primary, alternate).Run(t.Context(), 1)
	if !errors.Is(err, alternateErr) {
		t.Fatalf("err = %v; want the alternate's error", err)
	}
	if errors.Is(err, primaryErr) {
		t.Fatalf("err = %v; want primary's error discarded once the alternate answered it", err)
	}
	if got != 0 {
		t.Fatalf("output = %d; want the failed alternate's output", got)
	}
}

func TestFallback_primarySuccessSkipsAlternate(t *testing.T) {
	altRan := false
	primary := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })
	alt := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { altRan = true; return 0, nil })

	got, err := flowx.Fallback(primary, alt).Run(t.Context(), 7)
	if err != nil || got != 7 || altRan {
		t.Fatalf("got %d, err %v, altRan %v; want 7, nil, false", got, err, altRan)
	}
}

func TestFallback_rejectsNilNodes(t *testing.T) {
	ok := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })
	if _, err := flowx.Fallback[int, int](nil, ok).Run(t.Context(), 1); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil primary err = %v, want ErrNilNode", err)
	}
	if _, err := flowx.Fallback[int, int](ok, nil).Run(t.Context(), 1); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil alternate err = %v, want ErrNilNode", err)
	}
}

func TestFallback_rejectsTypedNilFunctionBeforeRunningPrimary(t *testing.T) {
	ran := false
	primary := flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
		ran = true
		return value, nil
	})
	var alternate flow.NodeFunc[int, int]

	_, err := flowx.Fallback(primary, alternate).Run(t.Context(), 1)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
	if ran {
		t.Fatal("primary ran before the typed nil alternate was rejected")
	}
}

type nilSafeNode struct{}

func (*nilSafeNode) Run(_ context.Context, value int) (int, error) {
	return value + 1, nil
}

func TestFallback_acceptsNilSafePointerNode(t *testing.T) {
	var primary *nilSafeNode
	alternate := flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
		return value + 100, nil
	})

	got, err := flowx.Fallback[int, int](primary, alternate).Run(t.Context(), 1)
	if err != nil || got != 2 {
		t.Fatalf("Run = %d, %v; want 2, nil", got, err)
	}
}

func TestFallback_prefersParentCancellation(t *testing.T) {
	boom := errors.New("boom")
	primary := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom })
	altRan := false
	alt := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { altRan = true; return 0, nil })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := flowx.Fallback(primary, alt).Run(ctx, 0)
	if !errors.Is(err, context.Canceled) || altRan {
		t.Fatalf("err = %v, altRan = %v; want context.Canceled, false", err, altRan)
	}
}

func TestFallback_cancellationDuringSuccessfulNodeWins(t *testing.T) {
	for name, primary := range map[string]bool{
		"primary":   true,
		"alternate": false,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			boom := errors.New("boom")
			first := flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
				if primary {
					cancel()
					return input + 1, nil
				}
				return 0, boom
			})
			second := flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
				cancel()
				return input + 2, nil
			})

			output, err := flowx.Fallback(first, second).Run(ctx, 1)
			if !errors.Is(err, context.Canceled) || output != 0 {
				t.Fatalf("Run = %d, %v; want 0, context.Canceled", output, err)
			}
		})
	}
}

func TestFallback_preCancelledContextDoesNotRunPrimary(t *testing.T) {
	cause := errors.New("stop fallback")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)
	called := false
	node := flowx.Fallback(
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
			called = true
			return 1, nil
		}),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 2, nil }),
	)

	if _, err := node.Run(ctx, 0); !errors.Is(err, cause) {
		t.Fatalf("Run error = %v; want cancellation cause", err)
	}
	if called {
		t.Fatal("primary ran with an already-cancelled context")
	}
}
