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

type failingNode struct{ err error }

func (f failingNode) Run(context.Context, int) (int, error) { return 0, f.err }
func (f failingNode) Validate() error                       { return f.err }

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
		"nil node":            flow.Validate(missing),
		"race case":           flow.Validate(flow.Race(ok, missing)),
		"race caller case":    flow.Validate(flow.Race[int, int](ok, failingNode{caller})),
		"map element":         runIndexed(t, flow.Map[int, int](failingNode{caller}, flow.MapConfig{}), 2),
		"map node":            flow.Validate(flow.Map(missing, flow.MapConfig{})),
		"switch case":         flow.Validate(caseSet(map[string]flow.Node[int, int]{"a": nil})),
		"switch nested case":  flow.Validate(caseSet(map[string]flow.Node[int, int]{"a": caseSet(map[string]flow.Node[int, int]{"z": nil})})),
		"switch caller case":  flow.Validate(caseSet(map[string]flow.Node[int, int]{"a": failingNode{caller}})),
		"switch no case":      runOne(t, caseSet(map[string]flow.Node[int, int]{"b": ok})),
		"switch empty":        flow.Validate(caseSet(nil)),
		"then nested":         flow.Validate(flow.Then[int, int, int](ok, flow.Then[int, int, int](ok, missing))),
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
