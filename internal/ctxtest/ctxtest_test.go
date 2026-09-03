package ctxtest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow/internal/ctxtest"
)

func TestCancelAtCheck(t *testing.T) {
	cause := errors.New("cancelled at check")
	ctx := ctxtest.CancelAtCheck(context.Background(), 3, cause)

	// Live until the configured check, and Done must stay open while Err is nil:
	// a value that reported a closed Done with a nil Err would not be a valid
	// Context, and the boundary tests that use this rely on the pairing.
	for check := 1; check < 3; check++ {
		if err := ctx.Err(); err != nil {
			t.Fatalf("Err at check %d = %v; want nil", check, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Done closed at check %d, before cancellation", check)
		default:
		}
	}

	// Done closes before this Err call returns.
	if err := ctx.Err(); !errors.Is(err, cause) {
		t.Fatalf("Err at the cancelling check = %v; want the cause", err)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("Done did not close when Err reported the cause")
	}

	// Cancellation is terminal and closing Done happens only once.
	for range 2 {
		if err := ctx.Err(); !errors.Is(err, cause) {
			t.Fatalf("Err after cancellation = %v; want the cause", err)
		}
	}

	// context.Cause reads through to the same value, which is how callers that
	// classify failures observe it.
	if err := context.Cause(ctx); !errors.Is(err, cause) {
		t.Fatalf("context.Cause = %v; want the cause", err)
	}
}

// TestCancelAtCheckIsShadowedByAParentCause pins the one limit of this value,
// because a test that hit it would not fail — it would observe a cancellation at
// the wrong moment with the wrong cause and still pass its assertion about
// "cancelled". [context.Cause] consults a parent's cause before falling back to
// Err, so the parent has to be live for the check count to decide anything.
func TestCancelAtCheckIsShadowedByAParentCause(t *testing.T) {
	parentCause := errors.New("parent cause")
	parent, cancel := context.WithCancelCause(context.Background())
	cancel(parentCause)

	own := errors.New("own cause")
	ctx := ctxtest.CancelAtCheck(parent, 1, own)
	if err := ctx.Err(); !errors.Is(err, own) {
		t.Fatalf("Err = %v; want this value's own cause", err)
	}
	if err := context.Cause(ctx); !errors.Is(err, parentCause) {
		t.Fatalf("context.Cause = %v; want the parent's cause, which shadows this one", err)
	}
}

func TestCancelAtCheckDelegatesToItsParent(t *testing.T) {
	type key struct{}
	parent := context.WithValue(context.Background(), key{}, "carried")
	ctx := ctxtest.CancelAtCheck(parent, 1, errors.New("cause"))

	if got := ctx.Value(key{}); got != "carried" {
		t.Fatalf("Value = %v; want the parent's value", got)
	}
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("Deadline reported one the parent does not have")
	}
}
