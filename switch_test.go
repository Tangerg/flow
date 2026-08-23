package flow_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Tangerg/flow"
)

func TestSwitch_routes(t *testing.T) {
	cases := map[string]flow.Node[int, string]{
		"even": flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "even", nil }),
		"odd":  flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "odd", nil }),
	}
	resolve := flow.NodeFunc[int, string](func(_ context.Context, n int) (string, error) {
		if n%2 == 0 {
			return "even", nil
		}
		return "odd", nil
	})

	node := flow.Switch(resolve, cases)

	for in, want := range map[int]string{2: "even", 3: "odd"} {
		got, err := node.Run(t.Context(), in)
		if err != nil {
			t.Fatalf("Run(%d) error: %v", in, err)
		}
		if got != want {
			t.Fatalf("Run(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestSwitch_ownsCaseMapStructure(t *testing.T) {
	cases := map[string]flow.Node[int, int]{
		"selected": flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
			return in + 1, nil
		}),
	}
	node := flow.Switch(
		flow.NodeFunc[int, string](func(context.Context, int) (string, error) {
			return "selected", nil
		}),
		cases,
	)
	cases["selected"] = flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
		return in + 100, nil
	})

	got, err := node.Run(t.Context(), 1)
	if err != nil || got != 2 {
		t.Fatalf("Run after source-map mutation = %d, %v; want 2, nil", got, err)
	}
}

func TestSwitch_noCase(t *testing.T) {
	cases := map[string]flow.Node[int, int]{
		"a": flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }),
	}
	resolve := flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "missing", nil })

	_, err := flow.Switch(resolve, cases).Run(t.Context(), 1)
	if !errors.Is(err, flow.ErrNoCase) {
		t.Fatalf("error = %v, want ErrNoCase", err)
	}
	if got, want := err.Error(), `flow: no matching case: key "missing"`; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func TestSwitch_resolveError(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]flow.Node[int, int]{
		"ok": flow.NodeFunc[int, int](func(_ context.Context, input int) (int, error) {
			return input, nil
		}),
	}
	resolve := flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "", boom })

	_, err := flow.Switch(resolve, cases).Run(t.Context(), 1)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
}

func TestSwitch_rejectsEmptyCasesBeforeRunningResolver(t *testing.T) {
	ran := false
	resolve := flow.NodeFunc[int, string](func(context.Context, int) (string, error) {
		ran = true
		return "missing", nil
	})

	_, err := flow.Switch(resolve, map[string]flow.Node[int, int]{}).Run(t.Context(), 1)
	if !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("error = %v; want ErrInvalidConfig", err)
	}
	if ran {
		t.Fatal("resolver ran before the empty case set was rejected")
	}
}

func TestSwitch_nilResolver(t *testing.T) {
	_, err := flow.Switch[string, int, int](nil, nil).Run(t.Context(), 1)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("error = %v, want ErrNilNode", err)
	}
}

func TestSwitch_validatesCasesBeforeRunningResolver(t *testing.T) {
	ran := false
	resolve := flow.NodeFunc[int, string](func(context.Context, int) (string, error) {
		ran = true
		return "ok", nil
	})
	_, err := flow.Switch(resolve, map[string]flow.Node[int, int]{
		"invalid-a": nil,
		"invalid-b": nil,
	}).Run(t.Context(), 1)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
	for _, key := range []string{"invalid-a", "invalid-b"} {
		if !strings.Contains(err.Error(), key) {
			t.Fatalf("err = %q; want case %q", err, key)
		}
	}
	if ran {
		t.Fatal("resolver ran before the invalid cases were rejected")
	}
}

type invalidSwitchCase struct{ err error }

func (i invalidSwitchCase) Run(context.Context, int) (int, error) { return 0, i.err }

func (i invalidSwitchCase) Validate() error { return i.err }

func TestSwitch_preservesEveryInvalidCaseCause(t *testing.T) {
	first := errors.New("same diagnostic")
	second := errors.New("same diagnostic")
	node := flow.Switch(
		flow.NodeFunc[int, string](func(context.Context, int) (string, error) { return "", nil }),
		map[string]flow.Node[int, int]{
			"first":  invalidSwitchCase{err: first},
			"second": invalidSwitchCase{err: second},
		},
	)

	err := flow.Validate(node)
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("Validate error = %v; want both indistinguishable case causes", err)
	}
	var located *flow.CaseError
	if !errors.As(err, &located) || located.Key != "first" {
		t.Fatalf("Validate error = %#v; want first sorted CaseError", err)
	}
}

func TestSwitch_reportsNestedCaseValidationDeterministically(t *testing.T) {
	resolve := flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) {
		return "ok", nil
	})
	cases := map[string]flow.Node[int, int]{
		"nil": flow.Then(
			flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
				return value, nil
			}),
			flow.Node[int, int](nil),
		),
		"config": flow.Loop(
			func(_ context.Context, _ int, value int) (int, bool, error) {
				return value, true, nil
			},
			flow.LoopConfig{MaxIterations: -1},
		),
	}

	var first string
	for range 100 {
		_, err := flow.Switch(resolve, cases).Run(t.Context(), 1)
		if err == nil {
			t.Fatal("Run succeeded with invalid cases")
		}
		if first == "" {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("validation changed from %q to %q", first, err)
		}
	}
}

func TestSwitch_rejectsTypedNilFunctionBeforeRunningResolver(t *testing.T) {
	ran := false
	resolve := flow.NodeFunc[int, string](func(context.Context, int) (string, error) {
		ran = true
		return "ok", nil
	})
	var invalid flow.NodeFunc[int, int]

	_, err := flow.Switch(resolve, map[string]flow.Node[int, int]{
		"ok": invalid,
	}).Run(t.Context(), 1)
	if !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("err = %v; want ErrNilNode", err)
	}
	if ran {
		t.Fatal("resolver ran before the typed nil function node was rejected")
	}

	var nilResolve flow.NodeFunc[int, string]
	if _, err := flow.Switch(nilResolve, map[string]flow.Node[int, int]{"ok": invalid}).
		Run(t.Context(), 1); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("typed nil resolver err = %v; want ErrNilNode", err)
	}
}

func TestSwitch_composedResolver(t *testing.T) {
	// The router itself is a composed node: double, then bucket by size.
	router := flow.Then(
		flow.NodeFunc[int, int](func(_ context.Context, n int) (int, error) { return n * 2, nil }),
		flow.NodeFunc[int, string](func(_ context.Context, n int) (string, error) {
			if n >= 10 {
				return "big", nil
			}
			return "small", nil
		}),
	)
	cases := map[string]flow.Node[int, string]{
		"big":   flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "BIG", nil }),
		"small": flow.NodeFunc[int, string](func(_ context.Context, _ int) (string, error) { return "small", nil }),
	}

	got, err := flow.Switch(router, cases).Run(t.Context(), 6) // 6*2=12 >= 10 -> "big"
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "BIG" {
		t.Fatalf("Run(6) = %q, want %q", got, "BIG")
	}
}

func TestSwitch_parentCancellationStopsBeforeSelectedCase(t *testing.T) {
	cause := errors.New("stop switch")
	ctx, cancel := context.WithCancelCause(t.Context())
	caseCalled := false
	node := flow.Switch(
		flow.NodeFunc[int, string](func(context.Context, int) (string, error) {
			cancel(cause)
			return "selected", nil
		}),
		map[string]flow.Node[int, int]{
			"selected": flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				caseCalled = true
				return 1, nil
			}),
		},
	)

	output, err := node.Run(ctx, 0)
	if !errors.Is(err, cause) || output != 0 {
		t.Fatalf("Run = %d, %v; want 0, cancellation cause", output, err)
	}
	if caseCalled {
		t.Fatal("selected case ran after parent cancellation")
	}
}
