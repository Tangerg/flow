package flow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Tangerg/flow"
)

func TestFunc_Run(t *testing.T) {
	double := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		return x * 2, nil
	})

	got, err := double.Run(t.Context(), 21)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 {
		t.Fatalf("Run(21) = %d, want 42", got)
	}
}

func TestFunc_Run_propagatesError(t *testing.T) {
	sentinel := errors.New("boom")
	fail := flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) {
		return 0, sentinel
	})

	_, err := fail.Run(t.Context(), 1)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Run error = %v, want %v", err, sentinel)
	}
}

func TestFunc_Run_nil(t *testing.T) {
	var f flow.NodeFunc[int, int]

	_, err := f.Run(t.Context(), 1)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("Run error = %v, want ErrNilNode", err)
	}
}

func TestFunc_Run_passesContext(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(t.Context(), ctxKey{}, "v")

	read := flow.NodeFunc[struct{}, string](func(ctx context.Context, _ struct{}) (string, error) {
		s, _ := ctx.Value(ctxKey{}).(string)
		return s, nil
	})

	got, err := read.Run(ctx, struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "v" {
		t.Fatalf("context value = %q, want %q", got, "v")
	}
}

type validatingNode struct {
	err error
}

func (v validatingNode) Run(_ context.Context, value int) (int, error) {
	return value, nil
}

func (v validatingNode) Validate() error { return v.err }

type opaqueNode struct{}

func (opaqueNode) Run(_ context.Context, value int) (int, error) {
	return value, nil
}

// nilSafeFuncNode proves that a caller-defined function type owns its nil
// behavior just like a caller-defined pointer type. Only flow.NodeFunc is the
// library's adapter and therefore subject to flow's nil-adapter rule.
type nilSafeFuncNode func()

func (nilSafeFuncNode) Run(_ context.Context, value int) (int, error) {
	return value * 2, nil
}

// TestRunChild_appliesTheContractACompositeOwesItsChildren checks the rule from
// where a caller-defined composite stands, which is the reason it is exported:
// the alternative is every composite author reimplementing three decisions from
// prose, and getting the third one wrong silently, because a child that returned
// a good value under a cancelled context looks like a successful child.
func TestRunChild_appliesTheContractACompositeOwesItsChildren(t *testing.T) {
	started := 0
	child := flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
		started++
		return value * 2, nil
	})

	if output, err := flow.RunChild(t.Context(), child, 21); err != nil || output != 42 {
		t.Fatalf("RunChild = %d, %v; want 42, nil", output, err)
	}

	if output, err := flow.RunChild[int, int](t.Context(), nil, 21); !errors.Is(err, flow.ErrNilNode) ||
		output != 0 {
		t.Fatalf("RunChild of a nil node = %d, %v; want 0, ErrNilNode", output, err)
	}

	cause := errors.New("caller stopped the composite")
	cancelled, cancel := context.WithCancelCause(t.Context())
	cancel(cause)
	before := started
	if output, err := flow.RunChild(cancelled, child, 21); !errors.Is(err, cause) || output != 0 {
		t.Fatalf("RunChild under a cancelled context = %d, %v; want 0, %v", output, err, cause)
	}
	if started != before {
		t.Fatal("RunChild started a child after the context was cancelled")
	}

	// A child that succeeds while the composite is being cancelled has produced a
	// result nothing may commit. Returning it would let a cancelled operation
	// report an output, which is the decision this rule exists to make.
	racing, cancelRacing := context.WithCancelCause(t.Context())
	late := flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
		cancelRacing(cause)
		return value * 2, nil
	})
	if output, err := flow.RunChild(racing, late, 21); !errors.Is(err, cause) || output != 0 {
		t.Fatalf("RunChild whose child raced cancellation = %d, %v; want 0, %v", output, err, cause)
	}
}

func TestValidate_checksTheCompleteVisibleDefinition(t *testing.T) {
	invalid := errors.New("invalid definition")
	valid := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		return 0, nil
	})
	tests := map[string]struct {
		node flow.Node[int, int]
		want error
	}{
		"nil interface":       {node: nil, want: flow.ErrNilNode},
		"typed nil function":  {node: flow.NodeFunc[int, int](nil), want: flow.ErrNilNode},
		"opaque node":         {node: opaqueNode{}},
		"opt-in validator":    {node: validatingNode{err: invalid}, want: invalid},
		"nested opt-in node":  {node: flow.Then(valid, validatingNode{err: invalid}), want: invalid},
		"nested nil function": {node: flow.Then(valid, flow.NodeFunc[int, int](nil)), want: flow.ErrNilNode},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if err := flow.Validate(test.node); !errors.Is(err, test.want) {
				t.Fatalf("Validate error = %v; want %v", err, test.want)
			}
		})
	}

	var nilSafe *nilSafeNode
	if err := flow.Validate[int, int](nilSafe); err != nil {
		t.Fatalf("Validate nil-safe pointer: %v", err)
	}
	var nilSafeFunc nilSafeFuncNode
	if err := flow.Validate[int, int](nilSafeFunc); err != nil {
		t.Fatalf("Validate nil-safe function type: %v", err)
	}
	if output, err := flow.Then[int, int, int](
		nilSafeFunc,
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value + 1, nil
		}),
	).Run(t.Context(), 21); err != nil || output != 43 {
		t.Fatalf("Then nil-safe function type = %d, %v; want 43, nil", output, err)
	}
}
