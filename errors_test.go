package flow_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Tangerg/flow"
)

func TestIndexError(t *testing.T) {
	boom := errors.New("boom")
	err := &flow.IndexError{Index: 2, Err: boom}
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "index 2") {
		t.Fatalf("err = %v; want index and wrapped cause", err)
	}
}

func TestNilIndexErrorFormatsAsNil(t *testing.T) {
	var indexed *flow.IndexError
	if got := indexed.Error(); got != "<nil>" {
		t.Fatalf("Error() = %q; want <nil>", got)
	}
	outer := &flow.IndexError{Index: 2, Err: indexed}
	if got := outer.Error(); got != "index 2: <nil>" {
		t.Fatalf("nested Error() = %q", got)
	}
	if errors.Is(indexed, flow.ErrNilNode) {
		t.Fatal("typed nil IndexError unexpectedly matched a cause")
	}
}

func TestCaseError(t *testing.T) {
	boom := errors.New("boom")
	err := &flow.CaseError{Key: "large", Err: boom}
	if !errors.Is(err, boom) || err.Error() != `switch case "large": boom` {
		t.Fatalf("err = %v; want structured case and wrapped cause", err)
	}

	var nilCase *flow.CaseError
	if got := nilCase.Error(); got != "<nil>" {
		t.Fatalf("nil Error() = %q; want <nil>", got)
	}
	if errors.Is(nilCase, flow.ErrNilNode) {
		t.Fatal("typed nil CaseError unexpectedly matched a cause")
	}
}

// Map and Race are ordinary nodes, so callers can nest them without crossing a
// definition-depth boundary. Rendering their locations must not spend one call
// frame per composite.
func TestIndexErrorFormatsDeepChainIteratively(t *testing.T) {
	withBoundedStack(t, func() {
		// Declared as the interface because the loop assigns wrappers to it.
		var err error
		err = errors.New("boom")
		for index := range 20_000 {
			err = &flow.IndexError{Index: index, Err: err}
		}
		message := err.Error()
		if !strings.HasPrefix(message, "index 19999: ") ||
			!strings.HasSuffix(message, "index 0: boom") {
			t.Fatal("Error() did not preserve the complete wrapper order")
		}
	})
}

func TestLocationErrorsFormatDeepMixedChainIteratively(t *testing.T) {
	withBoundedStack(t, func() {
		// Declared as the interface because the loop assigns wrappers to it.
		var err error
		err = errors.New("boom")
		for index := range 20_000 {
			if index%2 == 0 {
				err = errors.Join(&flow.IndexError{Index: index, Err: err})
			} else {
				err = errors.Join(&flow.CaseError{Key: index, Err: err})
			}
		}
		message := err.Error()
		if !strings.HasPrefix(message, "switch case 19999: index 19998: ") ||
			!strings.HasSuffix(message, "switch case 1: index 0: boom") {
			t.Fatal("Error() did not preserve the complete mixed wrapper order")
		}
	})
}

type failingNode struct{ err error }

func (f failingNode) Run(context.Context, int) (int, error) { return 0, f.err }
func (f failingNode) Validate() error                       { return f.err }

type callerMultiError []error

func (callerMultiError) Error() string     { return "caller-owned tree" }
func (c callerMultiError) Unwrap() []error { return c }

// TestLocationErrorFormatsEveryJoinedBranch pins what a location does with a
// join that has more than one branch: [errors.Join] separates branches with a
// newline, and a location above one states where exactly once, before the first
// branch, instead of once per line. Every other test here joins a single branch,
// which is the degenerate arity: it cannot show the separator, the order, or
// that the prefix is not repeated.
func TestLocationErrorFormatsEveryJoinedBranch(t *testing.T) {
	err := &flow.IndexError{
		Index: 2,
		Err: errors.Join(
			&flow.CaseError{Key: "left", Err: flow.ErrNilNode},
			errors.New("middle"),
			&flow.IndexError{Index: 7, Err: flow.ErrNoCase},
		),
	}
	want := "index 2: switch case \"left\": flow: nil node\n" +
		"middle\n" +
		"index 7: flow: no matching case"
	if got := err.Error(); got != want {
		t.Fatalf("Error() = %q; want %q", got, want)
	}
}

func TestLocationFormattingLeavesCallerMultiErrorsOpaque(t *testing.T) {
	err := &flow.IndexError{
		Index: 3,
		Err: callerMultiError{
			&flow.CaseError{Key: "hidden", Err: flow.ErrNilNode},
		},
	}
	if got, want := err.Error(), "index 3: caller-owned tree"; got != want {
		t.Fatalf("Error() = %q; want %q", got, want)
	}
}

// TestErrorsNameThePackageAtMostOnce pins how this package splits an error
// between origin and location. Its sentinels carry the package name because
// most of them reach a caller with nothing wrapping them; every location this
// package adds -- an index, a switch case -- therefore states only where, and
// nesting locations cannot multiply the name. A location over a caller's error
// names no package at all, which is correct: the caller owns that error.
func TestErrorsNameThePackageAtMostOnce(t *testing.T) {
	ok := flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) { return value, nil })
	resolve := flow.NodeFunc[int, string](func(context.Context, int) (string, error) { return "a", nil })
	caseSet := func(cases map[string]flow.Node[int, int]) flow.Node[int, int] {
		return flow.Switch(resolve, cases)
	}
	caller := errors.New("connection refused")
	var missing flow.Node[int, int]

	failures := map[string]error{
		"nil node":           flow.Validate(missing),
		"race case":          flow.Validate(flow.Race(ok, missing)),
		"race caller case":   flow.Validate(flow.Race[int, int](ok, failingNode{caller})),
		"map element":        runIndexed(t, flow.Map[int, int](failingNode{caller}, flow.MapConfig{}), 2),
		"map node":           flow.Validate(flow.Map(missing, flow.MapConfig{})),
		"switch case":        flow.Validate(caseSet(map[string]flow.Node[int, int]{"a": nil})),
		"switch nested case": flow.Validate(caseSet(map[string]flow.Node[int, int]{"a": caseSet(map[string]flow.Node[int, int]{"z": nil})})),
		"switch caller case": flow.Validate(caseSet(map[string]flow.Node[int, int]{"a": failingNode{caller}})),
		"switch no case":     runOne(t, caseSet(map[string]flow.Node[int, int]{"b": ok})),
		"switch empty":       flow.Validate(caseSet(nil)),
		"then nested":        flow.Validate(flow.Then[int, int, int](ok, flow.Then[int, int, int](ok, missing))),
		// Then validates two children in two statements, and every case here used
		// to name the second one. Run reaches the same sentinel through RunChild,
		// so only Validate can tell the positions apart.
		"then first":          flow.Validate(flow.Then[int, int, int](missing, ok)),
		"loop config":         flow.Validate(flow.Loop(doneAtOnce, flow.LoopConfig{MaxIterations: -1})),
		"map config":          flow.Validate(flow.Map(ok, flow.MapConfig{Concurrency: -1})),
		"race without nodes":  flow.Validate(flow.Race[int, int]()),
		"index over a nested": flow.Validate(flow.Race[int, int](caseSet(map[string]flow.Node[int, int]{"a": nil}))),
	}

	for name, err := range failures {
		t.Run(name, func(t *testing.T) {
			if err == nil {
				t.Fatal("want an error")
			}
			if got := strings.Count(err.Error(), "flow: "); got > 1 {
				t.Fatalf("names the package %d times: %v", got, err)
			}
		})
	}
}

func doneAtOnce(_ context.Context, _, value int) (int, bool, error) { return value, true, nil }

func runOne(t *testing.T, node flow.Node[int, int]) error {
	t.Helper()
	_, err := node.Run(t.Context(), 1)
	return err
}

func runIndexed(t *testing.T, node flow.Node[[]int, []int], count int) error {
	t.Helper()
	_, err := node.Run(t.Context(), make([]int, count))
	return err
}
