package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/internal/ctxtest"
	jschema "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

type schemaValidatorFunc func(any) error

func (s schemaValidatorFunc) Validate(value any) error {
	return s(value)
}

type opaqueTestStepFunc func(context.Context, Store) (Store, error)

func (o opaqueTestStepFunc) Run(ctx context.Context, store Store) (Store, error) {
	return o(ctx, store)
}

type definitionFixture struct{ shape stepDefinition }

func (definitionFixture) Run(context.Context, Store) (Store, error) { return Store{}, nil }
func (definitionFixture) validate() error                           { return nil }
func (definitionFixture) Describe() Description                     { return Description{} }
func (d definitionFixture) definition() stepDefinition              { return d.shape }

func TestErrorTreeMatchesNilByIdentity(t *testing.T) {
	if !(errorTree{}).matches(nil) {
		t.Fatal("zero errorTree does not match nil")
	}
	if (errorTree{root: errors.New("failure")}).matches(nil) {
		t.Fatal("non-empty errorTree matches nil")
	}
}

// TestCloningAnOwnedErrorStopsAtEveryTypedNil states the rule once for all ten
// wrappers this package copies. Each is pointer-shaped structured data, so any
// of them can appear as a typed nil cause, and copying one would dereference it.
// A typed nil is therefore kept as it arrived -- it is still the error the caller
// built -- while a wrapper above it is copied normally, because stopping the copy
// of one node must not stop the walk. Two of these locations are private and one
// is a Suspension the public routes classify before an Observer could ever see
// it copied, so no external test reaches them.
func TestCloningAnOwnedErrorStopsAtEveryTypedNil(t *testing.T) {
	for name, typedNil := range map[string]error{
		"step":          (*StepError)(nil),
		"reference":     (*RefError)(nil),
		"registration":  (*RegistrationError)(nil),
		"graph":         (*GraphError)(nil),
		"specification": (*SpecError)(nil),
		"detail":        (*detailError)(nil),
		"factory build": (*factoryBuildError)(nil),
		"suspension":    (*Suspension)(nil),
		"index":         (*flow.IndexError)(nil),
		"case":          (*flow.CaseError)(nil),
	} {
		t.Run(name, func(t *testing.T) {
			if clone := (ownedError{root: typedNil}).clone(); clone != typedNil {
				t.Fatalf("clone() = %#v; want the value it was given", clone)
			}

			outer := &StepError{ID: "outer", Op: OpRun, Err: typedNil}
			clone := (ownedError{root: outer}).clone()
			cloned, ok := clone.(*StepError)
			switch {
			case !ok || cloned == outer:
				t.Fatalf("clone() = %#v; want an independent StepError", clone)
			case cloned.Err != typedNil:
				t.Fatalf("clone().Err = %#v; want the typed nil it wrapped", cloned.Err)
			}
		})
	}
}

// TestDerivedScopeLeavesNoRoomForASiblingToWriteInto pins the property that lets
// every key, event, and chunk borrow the scope it was built under instead of
// copying it: a derived scope is exactly as long as its capacity, so nothing can
// append into it. Writing the derivation as append would look equivalent -- and
// is, until the growth doubles, after which two boundaries derived from the same
// parent share one array and the second one's frame overwrites the first's. That
// needs four nested boundaries with siblings under the deepest to be observed
// through behavior, which is why it is asked here directly.
func TestDerivedScopeLeavesNoRoomForASiblingToWriteInto(t *testing.T) {
	ctx := context.Background()
	for depth := 1; depth <= 8; depth++ {
		ctx = withScopeFrame(ctx, ScopeFrame{ID: "frame", Indexed: true, Index: uint64(depth)})
		derived := scope(ctx)
		if len(derived) != depth {
			t.Fatalf("depth %d scope has %d frames", depth, len(derived))
		}
		if cap(derived) != len(derived) {
			t.Fatalf(
				"depth %d scope has capacity %d for %d frames; a sibling could append into it",
				depth, cap(derived), len(derived),
			)
		}

		left := scope(withScopeFrame(ctx, ScopeFrame{ID: "left"}))
		right := scope(withScopeFrame(ctx, ScopeFrame{ID: "right"}))
		if left[len(left)-1].ID != "left" || right[len(right)-1].ID != "right" {
			t.Fatalf(
				"siblings at depth %d read as %q and %q; they share one array",
				depth+1, left[len(left)-1].ID, right[len(right)-1].ID,
			)
		}
	}
}

func TestReplayBoundaries_resampleCancellationAfterLookup(t *testing.T) {
	cause := errors.New("cancel during replay lookup")

	t.Run("leaf", func(t *testing.T) {
		called := false
		step := Leaf(
			"leaf",
			BinderFunc[Store](func(store Store) (Store, error) { return store, nil }),
			opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
				called = true
				return Store{}, nil
			}),
		)
		ctx := ctxtest.CancelAtCheck(t.Context(), 2, cause)
		output, err := step.Run(ctx, NewStore())
		_, published := output.Lookup(Output("leaf"))
		if !errors.Is(err, cause) || called || published {
			t.Fatalf("Run = %v, called %t, output %+v; want cancellation before node", err, called, output)
		}
	})

	t.Run("interrupt", func(t *testing.T) {
		ctx := ctxtest.CancelAtCheck(t.Context(), 2, cause)
		_, err := Interrupt("wait", "request").Run(ctx, NewStore())
		if !errors.Is(err, cause) || SuspendedOnly(err) {
			t.Fatalf("Run error = %v; want cancellation, not suspension", err)
		}
	})

	t.Run("branch decision", func(t *testing.T) {
		ctx := withConfig(ctxtest.CancelAtCheck(t.Context(), 1, cause), RunConfig{})
		execution := branchExecution{
			branch: branchStep{id: "branch"},
			key:    JournalKey{ID: "branch"},
			input:  NewStore(),
			run:    runFrom(ctx),
		}
		_, _, err := execution.decide(ctx)
		if !errors.Is(err, cause) {
			t.Fatalf("decide error = %v; want cancellation", err)
		}
	})

	t.Run("loop decision", func(t *testing.T) {
		ctx := withConfig(ctxtest.CancelAtCheck(t.Context(), 1, cause), RunConfig{})
		execution := loopExecution{loop: loopStep{config: LoopConfig{ID: "loop"}}, run: runFrom(ctx)}
		_, err := execution.stop(ctx, NewStore())
		if !errors.Is(err, cause) {
			t.Fatalf("stop error = %v; want cancellation", err)
		}
	})
}

func TestLoopStop_resamplesCancellationAfterJournalRecord(t *testing.T) {
	cause := errors.New("cancel during loop checkpoint")
	tests := []struct {
		name     string
		conflict bool
	}{
		{name: "successful record"},
		{name: "journal conflict", conflict: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			journal := NewJournal()
			cancelCtx := ctxtest.CancelAtCheck(t.Context(), 3, cause)
			ctx := withConfig(cancelCtx, RunConfig{Journal: journal})
			if test.conflict {
				// Add the conflicting record after the run snapshot so replay does
				// not consume it as historical work.
				if err := journal.Record(JournalKey{ID: "loop"}, false); err != nil {
					t.Fatal(err)
				}
			}

			execution := loopExecution{
				loop: loopStep{config: LoopConfig{
					ID: "loop",
					Condition: flow.NodeFunc[Store, bool](func(context.Context, Store) (bool, error) {
						return true, nil
					}),
				}},
				run: runFrom(ctx),
			}
			stop, err := execution.stop(ctx, NewStore())
			if !errors.Is(err, cause) || stop {
				t.Fatalf("stop = %t, error = %v; want cancellation", stop, err)
			}
			if journal.Len() != 1 {
				t.Fatalf("Journal.Len = %d; want completed checkpoint write", journal.Len())
			}
		})
	}
}

// TestLoopRejectsCancellationBeforeFirstBodyAdmission pins the checkpoint
// between admitting the loop boundary and admitting its first body. The body may
// be an opaque caller-defined Step with no cancellation check of its own, so the
// loop must not enter it after cancellation becomes observable.
func TestLoopRejectsCancellationBeforeFirstBodyAdmission(t *testing.T) {
	cause := errors.New("cancel before first loop body")
	bodyCalled := false
	step := Loop(LoopConfig{
		ID: "loop",
		Body: opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
			bodyCalled = true
			return NewStore(), nil
		}),
		Condition: flow.NodeFunc[Store, bool](func(context.Context, Store) (bool, error) {
			return true, nil
		}),
	})
	input := NewStore().WithOutput("seed", 1)
	ctx := ctxtest.CancelAtCheck(t.Context(), 2, cause)

	output, err := step.Run(ctx, input)
	if !errors.Is(err, cause) || bodyCalled || len(output.Changes(input)) != 0 {
		t.Fatalf("Run = output %+v, error %v, body called %t; want unchanged input and cancellation", output, err, bodyCalled)
	}
}

// TestLoopStop_cancellationOutranksTheConditionsOwnError pins the order of the
// two checks a loop makes after its condition runs: the cancellation sampled at
// that boundary is reported, not the error the condition returned while racing it.
// A condition that honours its context fails with the cause anyway, which is what
// hides the order; one that fails for its own reason is where they differ.
func TestLoopStop_cancellationOutranksTheConditionsOwnError(t *testing.T) {
	cause := errors.New("cancel while the condition ran")
	conditionErr := errors.New("condition failed for its own reason")
	cancelCtx := ctxtest.CancelAtCheck(t.Context(), 2, cause)
	ctx := withConfig(cancelCtx, RunConfig{Journal: NewJournal()})

	execution := loopExecution{
		loop: loopStep{config: LoopConfig{
			ID: "loop",
			Condition: flow.NodeFunc[Store, bool](func(context.Context, Store) (bool, error) {
				return true, conditionErr
			}),
		}},
		run: runFrom(ctx),
	}
	stop, err := execution.stop(ctx, NewStore())
	if !errors.Is(err, cause) || errors.Is(err, conditionErr) || stop {
		t.Fatalf("stop = %t, error = %v; want the cancellation cause alone", stop, err)
	}
}

func TestBranch_resamplesCancellationBeforeCaseAdmission(t *testing.T) {
	cause := errors.New("cancel after decision")
	for _, cancelAt := range []int{4, 5} {
		caseCalled := false
		step := Branch(BranchConfig{ID: "branch", Resolver: flow.NodeFunc[Store, string](func(context.Context, Store) (string, error) { return "selected", nil }), Cases: map[string]Step{
			"selected": opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
				caseCalled = true
				return Store{}, nil
			}),
		}})

		ctx := ctxtest.CancelAtCheck(t.Context(), cancelAt, cause)
		_, err := step.Run(ctx, NewStore())
		if !errors.Is(err, cause) || caseCalled {
			t.Fatalf("check %d: Run error = %v, case called = %t; want cancellation before case", cancelAt, err, caseCalled)
		}
	}
}

func TestGatedStep_resamplesCancellationAroundBypassCommit(t *testing.T) {
	cause := errors.New("cancel bypass")
	innerCalled := false
	inner := Leaf(
		"target",
		BinderFunc[Store](func(store Store) (Store, error) { return store, nil }),
		opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
			innerCalled = true
			return Store{}, nil
		}),
	).(definedStep)
	step := gated(
		[]compiledGate{{Gate: When("route", "yes"), outlets: []string{"yes", "no"}}},
		TriggerAll,
		inner,
	)
	store := NewStore().WithOutput("route", "no")

	for _, cancelAt := range []int{1, 2, 3, 4, 5} {
		ctx := ctxtest.CancelAtCheck(t.Context(), cancelAt, cause)
		if _, err := step.Run(ctx, store); !errors.Is(err, cause) {
			t.Fatalf("cancel check %d error = %v; want cancellation", cancelAt, err)
		}
	}
	if innerCalled {
		t.Fatal("bypassed inner step ran")
	}
}

// TestSubgraphBind_rejectsCancellationAroundEveryInputRead pins both checks a
// subgraph makes around one input read: before it, and after it, where the read's
// own failure competes. The input is missing either way, so the second check is
// what decides whether a cancelled subgraph reports the cancellation or blames an
// input it was never going to finish reading.
func TestSubgraphBind_rejectsCancellationAroundEveryInputRead(t *testing.T) {
	cause := errors.New("cancel around a subgraph input")
	for _, cancelAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("check %d", cancelAt), func(t *testing.T) {
			ctx := ctxtest.CancelAtCheck(t.Context(), cancelAt, cause)
			execution := subgraphExecution{
				subgraph: subgraphStep{inputs: Inputs{"seed": Output("missing")}},
				outer:    NewStore(),
			}
			_, err := execution.bind(ctx)
			if !errors.Is(err, cause) || errors.Is(err, ErrNotFound) {
				t.Fatalf("bind error = %v; want the cancellation cause alone", err)
			}
		})
	}
}

func TestEmissionSession_retainsFirstEmitterError(t *testing.T) {
	boom := errors.New("emitter failed")
	ctx, cancel := context.WithCancelCause(t.Context())
	session := emissionSession{
		run:     new(runState),
		cancel:  cancel,
		emitter: EmitterFunc(func(context.Context, Chunk) error { return boom }),
		key:     JournalKey{ID: "stream"},
	}
	first := session.emit(ctx, 1)
	second := session.emit(ctx, 2)
	if !errors.Is(first, boom) || second != first {
		t.Fatalf("emit errors = %v, %v; want the same first emitter error", first, second)
	}
}

// TestEmissionLease_closeReportsWhyTheStreamStopped pins what StreamFunc.Run's
// documented precedence rests on: once yield reports false, close reports the
// reason, so a producer cannot return success over a stream that stopped. Inside
// a leaf an Emitter failure also reaches the run through the session above, which
// masks this layer; a stream stopped by cancellation with no Emitter to fail has
// only this one.
func TestEmissionLease_closeReportsWhyTheStreamStopped(t *testing.T) {
	stopped := errors.New("cancelled while streaming")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(stopped)
	// A nil session is a StreamFunc with no Emitter to publish through, so the
	// only reason its yield can fail is the invocation's own context.
	lease := emissionLease{}
	if lease.yield(ctx, 1) {
		t.Fatal("yield published a value through a stopped stream")
	}
	if err := lease.close(); !errors.Is(err, stopped) {
		t.Fatalf("close() = %v; want the reason yield reported false", err)
	}
}

func TestGatedStep_resamplesCancellationAfterBypassEvent(t *testing.T) {
	cause := errors.New("cancel during bypass event")
	ctx, cancel := context.WithCancelCause(t.Context())
	ctx = withConfig(ctx, RunConfig{
		Observer: ObserverFunc(func(context.Context, Event) { cancel(cause) }),
	})
	inner := Leaf(
		"target",
		BinderFunc[Store](func(store Store) (Store, error) { return store, nil }),
		opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
			t.Fatal("bypassed inner step ran")
			return Store{}, nil
		}),
	).(definedStep)
	step := gated(
		[]compiledGate{{Gate: When("route", "yes"), outlets: []string{"yes", "no"}}},
		TriggerAll,
		inner,
	)

	_, err := step.Run(ctx, NewStore().WithOutput("route", "no"))
	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v; want cancellation cause", err)
	}
}

func TestLeaf_resamplesCancellationAfterJournalCommit(t *testing.T) {
	cause := errors.New("cancel after journal commit")
	ctx := withConfig(ctxtest.CancelAtCheck(t.Context(), 1, cause), RunConfig{
		Journal: NewJournal(),
	})
	execution := leafExecution[struct{}, int]{
		leaf:  leafStep[struct{}, int]{id: "leaf"},
		key:   JournalKey{ID: "leaf"},
		store: NewStore(),
		run:   runFrom(ctx),
	}

	output, err := execution.complete(ctx, 42)
	if !errors.Is(err, cause) {
		t.Fatalf("complete error = %v; want cancellation cause", err)
	}
	if _, published := output.Lookup(Output("leaf")); published {
		t.Fatal("complete published output after cancellation")
	}
	if keys := execution.run.journal().Keys(); len(keys) != 1 || keys[0].ID != "leaf" {
		t.Fatalf("Journal keys = %+v; want committed leaf checkpoint", keys)
	}
}

func TestJournalCommitErrorsDoNotHideParentCancellation(t *testing.T) {
	cause := errors.New("cancel during journal commit")

	t.Run("leaf", func(t *testing.T) {
		journal := NewJournal()
		if err := journal.Record(JournalKey{ID: "leaf"}, 1); err != nil {
			t.Fatalf("Record: %v", err)
		}
		ctx := withConfig(ctxtest.CancelAtCheck(t.Context(), 1, cause), RunConfig{
			Journal: journal,
		})
		execution := leafExecution[struct{}, int]{
			leaf:  leafStep[struct{}, int]{id: "leaf"},
			key:   JournalKey{ID: "leaf"},
			store: NewStore(),
			run:   runFrom(ctx),
		}

		if _, err := execution.complete(ctx, 2); !errors.Is(err, cause) {
			t.Fatalf("complete error = %v; want cancellation cause", err)
		}
	})

	t.Run("branch", func(t *testing.T) {
		journal := NewJournal()
		ctx := withConfig(ctxtest.CancelAtCheck(t.Context(), 4, cause), RunConfig{
			Journal: journal,
		})
		step := Branch(BranchConfig{ID: "route", Resolver: flow.NodeFunc[Store, string](func(context.Context, Store) (string, error) {
			if err := journal.Record(JournalKey{ID: "route"}, "case"); err != nil {
				return "", err
			}
			return "case", nil
		}), Cases: map[string]Step{"case": Interrupt("result", nil)}})

		if _, err := step.Run(ctx, NewStore()); !errors.Is(err, cause) {
			t.Fatalf("Run error = %v; want cancellation cause", err)
		}
	})
}

func TestParallelBranches_resamplesParentCancellation(t *testing.T) {
	cause := errors.New("cancel during parallel merge")
	input := NewStore().WithOutput("seed", 1)
	branch := opaqueTestStepFunc(func(_ context.Context, store Store) (Store, error) {
		return store.WithOutput("result", 2), nil
	})

	// One branch runs the same path as many, so both arities are asked at every
	// check that path makes: one per collected outcome, then one after the merge.
	for _, arity := range []int{1, 2} {
		branches := make(stepList, arity)
		for index := range branches {
			branches[index] = branch
		}
		for cancelAt := 2; cancelAt <= arity+2; cancelAt++ {
			t.Run(fmt.Sprintf("%d branches, check %d", arity, cancelAt), func(t *testing.T) {
				ctx := ctxtest.CancelAtCheck(t.Context(), cancelAt, cause)
				output, err := (parallelStep{branches: branches}).runBranches(ctx, input)
				if !errors.Is(err, cause) {
					t.Fatalf("runBranches error = %v; want cancellation cause", err)
				}
				if _, published := output.Lookup(Output("result")); published {
					t.Fatal("runBranches published merged output after cancellation")
				}
			})
		}
	}
}

func TestIterationCollect_resamplesParentCancellation(t *testing.T) {
	cause := errors.New("cancel during iteration collection")
	input := NewStore().WithOutput("seed", 1)
	outcomes := []elementOutcome{{value: 1}}

	for _, cancelAt := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("check %d", cancelAt), func(t *testing.T) {
			ctx := ctxtest.CancelAtCheck(t.Context(), cancelAt, cause)
			execution := iterationExecution{
				iteration: iterationStep{id: "items"},
				input:     input,
			}
			output, err := execution.collect(ctx, outcomes)
			if !errors.Is(err, cause) {
				t.Fatalf("collect error = %v; want cancellation cause", err)
			}
			if _, published := output.Lookup(Output("items")); published {
				t.Fatal("collect published output after cancellation")
			}
		})
	}
}

// TestIterationElement_cancellationOutranksASuspension pins which of two true
// answers an element gives: the cancellation sampled after its body ran, not the
// suspension the body returned. A suspension deliberately travels as a value so
// Map keeps the other elements running, so swallowing the cancellation here does
// not surface as a failure — it reports the element as waiting, and a cancelled
// run then reads as one that can be resumed. Every step's own boundary would have
// converted the cancellation first, which is what leaves this check untested.
func TestIterationElement_cancellationOutranksASuspension(t *testing.T) {
	cause := errors.New("cancel while the element suspended")
	ctx, cancel := context.WithCancelCause(t.Context())
	execution := iterationExecution{
		iteration: iterationStep{
			id: "items",
			body: opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
				cancel(cause)
				return Store{}, Suspend("wait")
			}),
		},
		input: NewStore(),
		items: []any{1},
	}

	outcome, err := execution.Run(ctx, 0)
	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v; want the cancellation cause", err)
	}
	if outcome.suspensions != nil {
		t.Fatalf("cancelled element reported %v; want no suspension", outcome.suspensions)
	}
}

func TestGuaranteedOutputsModelsEveryBuiltInShape(t *testing.T) {
	produces := interruptStep{id: "value"}
	opaque := opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
		return Store{}, nil
	})
	tests := []struct {
		name  string
		step  Step
		want  []string
		known bool
	}{
		{name: "named output", step: produces, want: []string{"value"}, known: true},
		{name: "named pass through", step: awaitStep{id: "wait"}, known: true},
		{
			name: "step list",
			step: sequenceStep{steps: stepList{awaitStep{id: "wait"}, produces}},
			want: []string{"value"}, known: true,
		},
		{
			name: "opaque step list",
			step: sequenceStep{steps: stepList{produces, opaque}},
		},
		{
			name: "branch intersection",
			step: branchStep{cases: map[string]Step{
				"a": sequenceStep{steps: stepList{produces}},
				"b": sequenceStep{steps: stepList{produces, awaitStep{id: "wait"}}},
			}},
			want: []string{"value"}, known: true,
		},
		{name: "empty branch", step: branchStep{}, known: true},
		{
			name: "opaque branch",
			step: branchStep{cases: map[string]Step{"a": produces, "b": opaque}},
		},
		{
			name: "loop body",
			step: loopStep{config: LoopConfig{Body: produces}},
			want: []string{"value"}, known: true,
		},
		{
			name: "iteration projection",
			step: iterationStep{id: "each"},
			want: []string{"each"}, known: true,
		},
		{
			name: "subgraph projection",
			step: subgraphStep{id: "sub"},
			want: []string{"sub"}, known: true,
		},
		{name: "graph", step: graphStep{}},
		{
			name: "invalid internal kind",
			step: definitionFixture{shape: stepDefinition{kind: definitionKind(255)}},
		},
		{name: "opaque", step: opaque},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outputs := guaranteedOutputs(test.step)
			if outputs.known != test.known || !slices.Equal(slices.Sorted(maps.Keys(outputs.nodes)), test.want) {
				t.Fatalf("guaranteedOutputs = %v, %t; want %v, %t", outputs.nodes, outputs.known, test.want, test.known)
			}
		})
	}
}

func TestSpecGuaranteedOutputsModelsEverySerializableShape(t *testing.T) {
	validator := specValidator{registry: registrySnapshot{
		schemas: map[string]registeredNodeSchema{
			"output": {schema: NodeSchema{Output: TypeAny}},
			"pass":   {schema: NodeSchema{}},
		},
	}}
	output := Spec{Kind: KindLeaf, ID: "value", Type: "output"}
	pass := Spec{Kind: KindLeaf, ID: "wait", Type: "pass"}
	unknown := Spec{Kind: KindLeaf, ID: "unknown", Type: "unknown"}
	tests := []struct {
		name  string
		spec  Spec
		want  []string
		known bool
	}{
		{name: "leaf output", spec: output, want: []string{"value"}, known: true},
		{name: "leaf pass through", spec: pass, known: true},
		{name: "leaf without schema", spec: unknown},
		{
			name: "sequence",
			spec: Spec{Kind: KindSequence, Steps: []Spec{pass, output}},
			want: []string{"value"}, known: true,
		},
		{
			name: "parallel",
			spec: Spec{Kind: KindParallel, Steps: []Spec{output}},
			want: []string{"value"}, known: true,
		},
		{
			name: "step list with unknown leaf",
			spec: Spec{Kind: KindSequence, Steps: []Spec{output, unknown}},
		},
		{
			name: "branch intersection",
			spec: Spec{Kind: KindBranch, Cases: map[string]Spec{
				"a": output,
				"b": {Kind: KindSequence, Steps: []Spec{output, pass}},
			}},
			want: []string{"value"}, known: true,
		},
		{name: "empty branch", spec: Spec{Kind: KindBranch}, known: true},
		{
			name: "branch removes non-common output",
			spec: Spec{Kind: KindBranch, Cases: map[string]Spec{
				"a": output,
				"b": pass,
			}},
			known: true,
		},
		{
			name: "branch with unknown leaf",
			spec: Spec{Kind: KindBranch, Cases: map[string]Spec{
				"a": output,
				"b": unknown,
			}},
		},
		{
			name: "loop body",
			spec: Spec{Kind: KindLoop, Body: &output},
			want: []string{"value"}, known: true,
		},
		{name: "loop without body", spec: Spec{Kind: KindLoop}},
		{
			name: "iteration projection",
			spec: Spec{Kind: KindIteration, ID: "each"},
			want: []string{"each"}, known: true,
		},
		{
			name: "subgraph projection",
			spec: Spec{Kind: KindSubgraph, ID: "sub"},
			want: []string{"sub"}, known: true,
		},
		{name: "invalid kind", spec: Spec{Kind: "invalid"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outputs := validator.guaranteedOutputs(test.spec)
			if outputs.known != test.known || !slices.Equal(slices.Sorted(maps.Keys(outputs.nodes)), test.want) {
				t.Fatalf("guaranteedOutputs = %v, %t; want %v, %t", outputs.nodes, outputs.known, test.want, test.known)
			}
		})
	}
}

func TestJSONDocument_reportsMalformedStructure(t *testing.T) {
	var target any
	if err := jsonDocument(`{`).decode(&target); err == nil {
		t.Fatal("decode unexpectedly accepted a truncated object")
	}
	invalidUTF8 := jsonDocument([]byte{'{', '"', 0xff, '"', ':', '1', '}'})
	if _, err := invalidUTF8.value(); err == nil ||
		!strings.Contains(err.Error(), "invalid UTF-8 at byte 3") {
		t.Fatalf("invalid UTF-8 error = %v; want byte offset", err)
	}
	if _, err := jsonDocument(`1 x`).value(); err == nil {
		t.Fatal("value unexpectedly accepted malformed trailing data")
	}
}

func TestJSONDocument_rejectsUnpairedUnicodeSurrogates(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{name: "plain", data: []byte(`"plain"`)},
		{name: "ordinary escape", data: []byte(`"\u0041"`)},
		{name: "paired surrogates", data: []byte(`"\ud800\udc00"`)},
		{name: "escaped backslash", data: []byte(`"\\ud800"`)},
		{name: "escaped quote", data: []byte(`"\"value"`)},
		{name: "high surrogate", data: []byte(`"\ud800"`), wantErr: "unpaired UTF-16 surrogate"},
		{name: "low surrogate", data: []byte(`"\udc00"`), wantErr: "unpaired UTF-16 surrogate"},
		{name: "high then scalar", data: []byte(`"\ud800\u0041"`), wantErr: "unpaired UTF-16 surrogate"},
		{name: "high then malformed", data: []byte(`"\ud800\uXXXX"`), wantErr: "unpaired UTF-16 surrogate"},
		{name: "short unicode escape", data: []byte(`"\u12"`), wantErr: "escape"},
		{name: "unknown escape", data: []byte(`"\q"`), wantErr: "escape"},
		{name: "trailing escape", data: []byte{'"', '\\'}, wantErr: "unexpected EOF"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := jsonDocument(test.data).value()
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("value error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("value error = %v; want %q", err, test.wantErr)
			}
		})
	}
}

func TestSchemaInfrastructure_reportsEveryFailureBoundary(t *testing.T) {
	// Each boundary is named, not just detected. The stages run in order and a
	// later one fails on the same input, so a stage that dropped its own error
	// would still return one -- from the stage after it, about something else.
	if _, err := (schemaSource{
		url:      "https://example.com/schema.json",
		document: jsonDocument(`{`),
	}).compile(); err == nil || !strings.Contains(err.Error(), "decode JSON Schema") {
		t.Fatalf("malformed schema JSON = %v; want the decode boundary", err)
	}
	if _, err := (schemaSource{
		url:      "%",
		document: jsonDocument(`{}`),
	}).compile(); err == nil ||
		!strings.HasPrefix(err.Error(), "add JSON Schema resource: ") ||
		!strings.Contains(err.Error(), "invalid URL escape") {
		t.Fatalf("invalid resource URL = %v; want the resource boundary and what it said", err)
	}
	// A document that decodes and registers can still be no schema at all, which
	// is the one boundary the backend rather than this package detects.
	if _, err := (schemaSource{
		url:      "https://example.com/schema.json",
		document: jsonDocument(`{"type": 5}`),
	}).compile(); err == nil ||
		!strings.HasPrefix(err.Error(), "compile JSON Schema: ") ||
		!strings.Contains(err.Error(), "not valid against metaschema") {
		t.Fatalf("unusable schema = %v; want the compile boundary and what it said", err)
	}

	loadErr := errors.New("load schema")
	load := schemaLoader(func() (compiledSchema, error) {
		return compiledSchema{}, loadErr
	})
	var decoded map[string]any
	if err := load.decode(jsonDocument(`{}`), &decoded); !errors.Is(err, loadErr) {
		t.Fatalf("decode error = %v; want load error", err)
	}

	// The backend's words are kept and its identity is not, so the sentinel here
	// must share no text with the boundary that reports it -- otherwise the
	// boundary's own name would satisfy the check that the words survived.
	validateErr := errors.New("refused by the backend")
	schema := compiledSchema{validator: schemaValidatorFunc(func(any) error {
		return validateErr
	})}
	if err := schema.validate(nil); errors.Is(err, validateErr) ||
		!strings.Contains(err.Error(), "validate JSON Schema: "+validateErr.Error()) {
		t.Fatalf("validate error = %v; want the named boundary carrying the backend's words", err)
	}
}

func TestJSONSchemaError_ordersDeduplicatesAndHidesBackend(t *testing.T) {
	first := &jschema.ValidationError{
		SchemaURL:        "https://example.com/schema.json",
		InstanceLocation: []string{"a"},
		ErrorKind:        &kind.FalseSchema{},
	}
	last := &jschema.ValidationError{
		SchemaURL:        "https://example.com/schema.json",
		InstanceLocation: []string{"z"},
		ErrorKind:        &kind.FalseSchema{},
	}
	makeError := func(causes ...*jschema.ValidationError) *jsonSchemaError {
		return &jsonSchemaError{err: &jschema.ValidationError{
			SchemaURL: "https://example.com/schema.json",
			ErrorKind: &kind.FalseSchema{},
			Causes:    causes,
		}}
	}
	// The repeat sits in the middle: a leaf already collected is skipped, not taken
	// for the end of the list, and only a leaf behind the repeat says which.
	err := makeError(last, last, first)
	message := err.Error()
	if message != makeError(first, last).Error() ||
		strings.Count(message, "at '/a'") != 1 ||
		strings.Count(message, "at '/z'") != 1 ||
		strings.Index(message, "at '/a'") > strings.Index(message, "at '/z'") {
		t.Fatalf("Error = %q; want stable, deduplicated path order", message)
	}
	// Every leaf is a diagnostic, so the joined text has no empty entry. Comparing
	// two of these messages to each other cannot see one, because both would carry
	// it.
	if strings.HasPrefix(message, "; ") || strings.Contains(message, "; ; ") {
		t.Fatalf("Error = %q; want no empty diagnostic among the leaves", message)
	}
	if _, ok := errors.AsType[*jschema.ValidationError](err); ok {
		t.Fatal("JSON Schema backend escaped through the public error chain")
	}
}

func TestSpecCompiler_defendsItsValidatedInputContract(t *testing.T) {
	buildErr := errors.New("build")
	registry := NewRegistry().
		MustRegisterNode("broken", func(NodeSpec) (Step, error) {
			return nil, buildErr
		}).
		// A single-input factory reports the wiring it did not get, which is the
		// one factory failure a caller repairs in inputs rather than in config.
		MustRegisterNode("wired", Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
				return value, nil
			}), nil
		})).
		MustRegisterResolver("resolver", flow.NodeFunc[Store, string](func(context.Context, Store) (string, error) {
			return "", nil
		})).
		MustRegisterCondition("condition", flow.NodeFunc[Store, bool](func(context.Context, Store) (bool, error) {
			return false, nil
		}))
	compiler := specCompiler{registry: registry.snapshot()}
	broken := Spec{Kind: KindLeaf, ID: "broken", Type: "broken"}

	tests := map[string]struct {
		spec Spec
		want string
	}{
		"sequence child": {
			spec: Spec{Kind: KindSequence, Steps: []Spec{broken}},
			want: `at "/steps/0" leaf "broken" field config: build`,
		},
		"parallel child": {
			spec: Spec{Kind: KindParallel, Steps: []Spec{broken}},
			want: `at "/steps/0" leaf "broken" field config: build`,
		},
		"unknown kind": {
			spec: Spec{Kind: "unknown"},
			want: `field kind: unknown kind "unknown"`,
		},
		"unknown leaf": {
			spec: Spec{Kind: KindLeaf, ID: "leaf", Type: "missing"},
			want: `leaf "leaf" field type: unknown node type "missing"`,
		},
		"leaf input field": {
			spec: Spec{
				Kind: KindLeaf, ID: "leaf", Type: "broken",
				Input: Output("a"),
			},
			want: `leaf "leaf" field config: build`,
		},
		"unwired leaf": {
			spec: Spec{Kind: KindLeaf, ID: "leaf", Type: "wired"},
			want: `leaf "leaf" field inputs: `,
		},
		"unknown resolver": {
			spec: Spec{
				Kind: KindBranch, ID: "branch", Resolver: "missing",
				Cases: map[string]Spec{"case": {Kind: KindSequence}},
			},
			want: `branch "branch" field resolver: unknown resolver "missing"`,
		},
		"branch child": {
			spec: Spec{
				Kind: KindBranch, ID: "branch", Resolver: "resolver",
				Cases: map[string]Spec{"case": broken},
			},
			want: `at "/cases/case" leaf "broken" field config: build`,
		},
		"missing loop body": {
			spec: Spec{Kind: KindLoop, ID: "loop", Condition: "condition"},
			want: `loop "loop" field body: required`,
		},
		"unknown condition": {
			spec: Spec{
				Kind: KindLoop, ID: "loop", Body: &Spec{Kind: KindSequence},
				Condition: "missing",
			},
			want: `loop "loop" field condition: unknown condition "missing"`,
		},
		"loop body": {
			spec: Spec{
				Kind: KindLoop, ID: "loop", Body: &broken,
				Condition: "condition",
			},
			want: `at "/body" leaf "broken" field config: build`,
		},
		"missing iteration input": {
			spec: Spec{Kind: KindIteration, ID: "each"},
			want: `iteration "each" field input: required`,
		},
		"missing iteration body": {
			spec: Spec{Kind: KindIteration, ID: "each", Input: Output("items")},
			want: `iteration "each" field body: required`,
		},
		"missing iteration output": {
			spec: Spec{
				Kind: KindIteration, ID: "each", Input: Output("items"),
				Body: &Spec{Kind: KindSequence},
			},
			want: `iteration "each" field bodyOutput: required`,
		},
		"iteration body": {
			spec: Spec{
				Kind: KindIteration, ID: "each", Input: Output("items"),
				Body: &broken, BodyOutput: Output("value"),
			},
			want: `at "/body" leaf "broken" field config: build`,
		},
		"missing subgraph body": {
			spec: Spec{Kind: KindSubgraph, ID: "sub", BodyOutput: Output("value")},
			want: `subgraph "sub" field body: required`,
		},
		"missing subgraph output": {
			spec: Spec{
				Kind: KindSubgraph, ID: "sub",
				Body: &Spec{Kind: KindSequence},
			},
			want: `subgraph "sub" field bodyOutput: required`,
		},
		"subgraph body": {
			spec: Spec{
				Kind: KindSubgraph, ID: "sub",
				Body: &broken, BodyOutput: Output("value"),
			},
			want: `at "/body" leaf "broken" field config: build`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Each case asserts the defense its name describes. Checking only that
			// an error occurred would let a reordered check pass a case for a
			// reason that has nothing to do with what it is named for.
			_, err := compiler.compile(test.spec)
			if err == nil {
				t.Fatal("compile unexpectedly succeeded")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("compile error = %v; want a message containing %q", err, test.want)
			}
		})
	}
}

// TestJournalUnmarshal_stampsDecodedRecordsAsNewerThanEverySnapshot pins what
// keeps a run from replaying work it never did: a decoded record is stamped with a
// revision above every snapshot taken before the decode, so a run already under way
// cannot see records loaded into its Journal afterwards. The decoder assigns no
// revision of its own, so without the stamp every decoded record would read as
// older than any snapshot and become visible to all of them.
func TestJournalUnmarshal_stampsDecodedRecordsAsNewerThanEverySnapshot(t *testing.T) {
	journal := NewJournal()
	if err := journal.Record(JournalKey{ID: "before"}, 1); err != nil {
		t.Fatalf("Record: %v", err)
	}
	snapshot := journal.snapshotRevision()

	document := []byte(`{"version":4,"records":[{"id":"loaded","value":2}]}`)
	if err := json.Unmarshal(document, journal); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if value, ok := journal.lookupAt(JournalKey{ID: "loaded"}, snapshot); ok {
		t.Fatalf("a snapshot taken before the decode saw %v", value)
	}
	if value, ok := journal.lookupAt(JournalKey{ID: "loaded"}, journal.snapshotRevision()); !ok || value != json.Number("2") {
		t.Fatalf("lookupAt after the decode = %v, %v; want the loaded record", value, ok)
	}
}

// TestLocateSpecError_passesThroughAnErrorItCannotLocate pins the branch that
// keeps a prefixer from deciding an error's fate. Recursive Spec compilation
// returns its boundary error directly, so every reachable caller hands this a
// *SpecError or nothing at all — which is why returning the error unchanged and
// returning nothing look the same from outside, and why the branch has to be
// stated here to mean anything. It cannot be removed: what follows it dereferences
// the assertion.
func TestLocateSpecError_passesThroughAnErrorItCannotLocate(t *testing.T) {
	foreign := errors.New("not a spec error")
	if got := locateSpecError(foreign, "steps", "0"); !errors.Is(got, foreign) {
		t.Fatalf("locateSpecError = %v; want the error it was given", got)
	}
	if got := locateSpecError(nil, "steps", "0"); got != nil {
		t.Fatalf("locateSpecError(nil) = %v; want nil", got)
	}
}

// storeDerivers is the fan-out width the sharing test uses: enough goroutines to
// interleave, few enough to stay an ordinary unit test.
const storeDerivers = 8

// deriveConcurrently hands one Store to every deriver at once. Each reads the
// shared base before writing, so the race detector sees the reads and the writes
// overlap, and returns what it derived.
func deriveConcurrently(t *testing.T, base Store) []Store {
	t.Helper()
	derived := make([]Store, storeDerivers)
	reads := make([]int, storeDerivers)
	var group sync.WaitGroup
	for index := range storeDerivers {
		group.Go(func() {
			value, err := base.Get[int](At("seed", "key-00"))
			if err != nil {
				t.Errorf("deriver %d read the shared base: %v", index, err)
			}
			reads[index] = value
			derived[index] = base.WithOutput(fmt.Sprintf("node-%d", index), index)
		})
	}
	group.Wait()
	for index, read := range reads {
		if read != 0 {
			t.Fatalf("deriver %d read %d from the shared base; want 0", index, read)
		}
	}
	return derived
}

// assertDerivationIsPrivate checks that a derivation holds its own write and no
// other deriver's: that is what makes handing one Store to all of them safe.
func assertDerivationIsPrivate(t *testing.T, derived []Store) {
	t.Helper()
	for index, store := range derived {
		if got, err := store.Get[int](Output(fmt.Sprintf("node-%d", index))); err != nil || got != index {
			t.Fatalf("deriver %d = %d, %v; want %d", index, got, err, index)
		}
		for other := range storeDerivers {
			if other == index {
				continue
			}
			if _, present := store.Lookup(Output(fmt.Sprintf("node-%d", other))); present {
				t.Fatalf("deriver %d saw deriver %d's write", index, other)
			}
		}
	}
}

// assertBaseSurvives checks that every derivation still reads the whole base, from
// the snapshot behind the overlay as well as the overlay itself.
func assertBaseSurvives(t *testing.T, derived []Store, base Store) {
	t.Helper()
	last := fmt.Sprintf("key-%02d", storeOverlayLimit-1)
	for index, store := range derived {
		if got, err := store.Get[int](At("seed", last)); err != nil || got != storeOverlayLimit-1 {
			t.Fatalf("deriver %d lost the snapshot: %d, %v", index, got, err)
		}
		if got, err := store.Get[int](At("overlay", last)); err != nil || got != storeOverlayLimit-1 {
			t.Fatalf("deriver %d lost the overlay: %d, %v", index, got, err)
		}
	}
	if _, present := base.Lookup(Output("node-0")); present {
		t.Fatal("a derivation wrote into the shared base")
	}
}

// TestStore_sharesOneBaseAcrossConcurrentDerivers pins what the whole fan-out
// design rests on: a derivation is a new value, so one Store can be handed to every
// branch at once. Nothing tested it directly. Concurrent runs reach it through a
// composite, where a race would surface as a failure somewhere else entirely, and
// the benchmarks that watch for the flattening cliff measure cost rather than
// correctness.
//
// Both shapes a fan-out can hand out are covered. A base at the overlay limit makes
// every deriver flatten the same snapshot and overlay for itself, which is the
// heaviest concurrent read of one Store there is; sharedBase flattens once and hands
// out a snapshot nobody has to walk. A flattening that decided to remember its
// result would fail the first shape: the race detector reports it writing into the
// snapshot every deriver is reading.
func TestStore_sharesOneBaseAcrossConcurrentDerivers(t *testing.T) {
	atLimit := NewStore()
	for index := range storeOverlayLimit {
		atLimit = atLimit.WithCell("seed", fmt.Sprintf("key-%02d", index), index)
	}
	// Compact once, then fill the overlay again. A Store in a running workflow has a
	// snapshot behind its overlay, and that snapshot is what a flattening reads
	// through.
	atLimit = atLimit.sharedBase()
	for index := range storeOverlayLimit {
		atLimit = atLimit.WithCell("overlay", fmt.Sprintf("key-%02d", index), index)
	}

	for name, base := range map[string]Store{
		"each deriver flattens": atLimit,
		"flattened once":        atLimit.sharedBase(),
	} {
		t.Run(name, func(t *testing.T) {
			derived := deriveConcurrently(t, base)
			assertDerivationIsPrivate(t, derived)
			assertBaseSurvives(t, derived, base)
		})
	}
}

func TestStoreInternals_reportStableFallbacks(t *testing.T) {
	changes := make([]storeChange, 0, storeOverlayLimit*2+1)
	base := NewStore()
	for index := range storeOverlayLimit*2 + 1 {
		next := base.WithOutput(fmt.Sprintf("node-%d", index), index)
		changes = append(changes, next.changesSince(base)...)
	}
	merged := base.withChanges(changes)
	if merged.depth != 0 {
		t.Fatalf("withChanges depth = %d; want compacted snapshot", merged.depth)
	}
}

func TestStoreChanges_orderSurvivesCompaction(t *testing.T) {
	base := NewStore()
	first := base.WithOutput("first", 1)
	second := base.WithOutput("second", 2)

	// Merge in the opposite order from the writes themselves. Revision is the
	// Store's write order; overlay linkage is only a representation detail.
	merged := base.merge(second, first)
	want := []Ref{Output("first"), Output("second")}
	for name, store := range map[string]Store{
		"overlay":   merged,
		"compacted": merged.compact(),
	} {
		t.Run(name, func(t *testing.T) {
			changes := store.Changes(base)
			got := make([]Ref, len(changes))
			for index, change := range changes {
				got[index] = change.Ref
			}
			if !slices.Equal(got, want) {
				t.Fatalf("Changes = %v; want write order %v", got, want)
			}
		})
	}
}

// withoutNodes is the graph decorator's Store boundary: a compiled Graph clears
// its own node namespace from the input so a rerun cannot read a previous
// attempt's outputs. The three tests below separate what that has to hold -- it
// changes nothing it does not own, it removes the namespace from every Store
// representation, and the removal keeps behaving like a write.
func TestStoreWithoutNodes_changesNothingItDoesNotOwn(t *testing.T) {
	store := NewStore().WithOutput("external", 1)
	if got := store.withoutNodes(nil); got != store {
		t.Fatal("empty node set changed the Store")
	}
	nodes := nodeSet{"node": {}}
	if got := (Store{}).withoutNodes(nodes); got != (Store{}) {
		t.Fatalf("empty Store changed to %+v", got)
	}
	if got := store.withoutNodes(nodes); got != store {
		t.Fatal("unrelated node set changed the Store")
	}
	externalSnapshot := store.compact()
	if got := externalSnapshot.withoutNodes(nodes); got != externalSnapshot {
		t.Fatal("unrelated node set changed a snapshotted Store")
	}
}

// A Store is either an overlay over a base or a flattened snapshot, and removal
// has to reach the namespace through both. Whichever it is, the removal must not
// read as a change to the caller: it is the decorator's own bookkeeping.
func TestStoreWithoutNodes_removesTheNamespaceFromEveryRepresentation(t *testing.T) {
	nodes := nodeSet{"node": {}}
	for name, owned := range map[string]Store{
		"overlay":  NewStore().WithOutput("node", 1),
		"snapshot": NewStore().WithOutput("node", 1).compact(),
	} {
		t.Run(name, func(t *testing.T) {
			cleared := owned.withoutNodes(nodes)
			if _, present := cleared.Lookup(Output("node")); present {
				t.Fatal("cleared Store retained the internal node")
			}
			if _, present := owned.merge(cleared).Lookup(Output("node")); present {
				t.Fatal("namespace removal did not survive Store merging")
			}
			if changes := cleared.Changes(owned); len(changes) != 0 {
				t.Fatalf("Changes exposed private namespace removals: %v", changes)
			}
			encoded, err := cleared.MarshalJSON()
			if err != nil || string(encoded) != `{}` {
				t.Fatalf("MarshalJSON = %s, %v; want {}, nil", encoded, err)
			}
		})
	}
}

// A removal is a write, so it carries a revision and a lineage and merges by the
// same rules: it survives compaction, applies only to the lineage it removed, and
// loses to a later write while beating an earlier one.
func TestStoreWithoutNodes_removalMergesLikeTheWriteItIs(t *testing.T) {
	nodes := nodeSet{"node": {}}
	base := NewStore().WithOutput("node", 1)
	cleared := base.
		WithOutput("node", 2).
		WithOutput("node", 3).
		withoutNodes(nodes).
		compact()
	if _, present := base.merge(cleared).Lookup(Output("node")); present {
		t.Fatal("compaction lost a namespace removal's Store lineage")
	}

	unrelated := NewStore().WithOutput("node", 4)
	merged := unrelated.merge(cleared)
	if value, err := merged.Get[int](Output("node")); err != nil || value != 4 {
		t.Fatalf("unrelated Store value = %d, %v; private removal leaked across lineages", value, err)
	}

	removed := base.withoutNodes(nodes)
	written := base.WithOutput("node", 5)
	if value, err := base.merge(removed, written).Get[int](Output("node")); err != nil || value != 5 {
		t.Fatalf("later write = %d, %v; want 5, nil", value, err)
	}
	if value, present := base.merge(written, removed).Lookup(Output("node")); present {
		t.Fatalf("later namespace cleanup left value %v", value)
	}

	// Past the overlay limit the removal is written into a compacted snapshot
	// instead of onto an overlay, which is the other half of the same rule.
	large := NewStore()
	for index := range storeOverlayLimit*2 + 1 {
		large = large.WithCell("node", fmt.Sprintf("key-%03d", index), index)
	}
	cleared = large.withoutNodes(nodes)
	if cleared.depth != 0 {
		t.Fatalf("large namespace cleanup depth = %d; want compacted snapshot", cleared.depth)
	}
	if encoded, err := cleared.MarshalJSON(); err != nil || string(encoded) != `{}` {
		t.Fatalf("large cleanup MarshalJSON = %s, %v; want {}, nil", encoded, err)
	}
}

// A gate wraps a step without hiding it: the definition it decorates is still
// reached by validation, so a body too deep to run is rejected through the gate.
func TestGatedStep_validatesTheDefinitionItWraps(t *testing.T) {
	var step definedStep = interruptStep{id: "inner"}
	for range MaxNestingDepth + 1 {
		step = Sequence(step).(definedStep)
	}
	step = gated(
		[]compiledGate{{
			Gate:    When("route", "yes"),
			outlets: []string{"yes"},
		}},
		TriggerAll,
		step,
	)
	validator := definitionValidator{}
	if err := validator.validate(step); !errors.Is(err, ErrMaxDepth) {
		t.Fatalf("error = %v; want ErrMaxDepth", err)
	}
}

// A bypassed step still consumed its identity: it reached its boundary and decided
// not to run, which is an outcome, not an absence. Reaching it twice is the
// duplicate the run rejects.
func TestGatedStep_bypassReservesExecutionIdentity(t *testing.T) {
	step := gated(
		[]compiledGate{{
			Gate:    When("route", "yes"),
			outlets: []string{"yes", "no"},
		}},
		TriggerAll,
		interruptStep{id: "target"},
	)
	ctx := withConfig(t.Context(), RunConfig{})
	store := NewStore().WithOutput("route", "no")
	if _, err := step.Run(ctx, store); err != nil {
		t.Fatalf("first bypass: %v", err)
	}
	if _, err := step.Run(ctx, store); !errors.Is(err, ErrDuplicateStep) {
		t.Fatalf("second bypass error = %v; want ErrDuplicateStep", err)
	}
}

func TestGraphCall_doesNotEnterStepAfterCancellation(t *testing.T) {
	calls := 0
	call := graphCall{
		index: 3,
		input: NewStore().WithOutput("seed", 1),
		step: opaqueTestStepFunc(func(context.Context, Store) (Store, error) {
			calls++
			return Store{}, nil
		}),
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	outcome := call.run(ctx)
	if calls != 0 {
		t.Fatalf("step calls = %d; want 0", calls)
	}
	if outcome.index != 3 || !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("outcome = %+v; want index 3 with context cancellation", outcome)
	}
	if value, err := outcome.store.Get[int](Output("seed")); err != nil || value != 1 {
		t.Fatalf("preserved input = %d, %v; want 1, nil", value, err)
	}
}

func TestGraphExecution_doesNotStartReadyNodeAfterCancellation(t *testing.T) {
	calls := 0
	execution := graphExecution{
		graph: graphStep{steps: stepList{opaqueTestStepFunc(
			func(context.Context, Store) (Store, error) {
				calls++
				return Store{}, nil
			},
		)}},
		input: NewStore(),
		ready: []int{0},
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	outcomes := make(chan graphOutcome, 1)

	execution.startReady(ctx, outcomes, 1)

	if calls != 0 || execution.active != 0 || execution.head != 0 {
		t.Fatalf(
			"calls, active, head = %d, %d, %d; want 0, 0, 0",
			calls,
			execution.active,
			execution.head,
		)
	}
	if len(outcomes) != 0 || execution.changes != nil {
		t.Fatalf(
			"outcomes, changes = %d, %v; want 0, nil",
			len(outcomes),
			execution.changes,
		)
	}
}

func TestCompareBool_ordersFalseBeforeTrue(t *testing.T) {
	if got := compareBool(false, true); got >= 0 {
		t.Fatalf("compareBool(false, true) = %d; want negative", got)
	}
}

func TestValueType_acceptsOnlyPossibleCellPaths(t *testing.T) {
	tests := []struct {
		valueType ValueType
		ref       Ref
		want      bool
	}{
		{valueType: TypeString, ref: Output("source"), want: true},
		{valueType: TypeAny, ref: Output("source").Child("member"), want: true},
		{valueType: TypeObject, ref: Output("source").Child("member"), want: true},
		{valueType: TypeArray, ref: Output("source").Child("0"), want: true},
		{valueType: TypeArray, ref: Output("source").Child("member")},
		{valueType: TypeBool, ref: Output("source").Child("member")},
		{valueType: TypeArray, ref: Ref{NodeID: "source", Path: "output/0"}},
		{valueType: TypeArray, ref: Ref{NodeID: "source", Path: "/other/0"}},
		{valueType: TypeArray, ref: Ref{NodeID: "source", Path: "/output~2/0"}},
		{valueType: TypeArray, ref: Ref{NodeID: "source", Path: "/output/~2"}},
		{valueType: TypeArray, ref: Output("other")},
	}
	for _, test := range tests {
		if got := test.valueType.acceptsCellPath(
			test.ref,
			"source",
			outputKey,
		); got != test.want {
			t.Fatalf("%q accepts %q = %v; want %v", test.valueType, test.ref.Path, got, test.want)
		}
	}
}

// populatedSpec returns a Spec with every field except Kind set to a non-zero,
// JSON-valid value. A field whose type has no sample here fails the test rather
// than quietly escaping the matrix checks below, which is the point: a new Spec
// field must be accounted for deliberately.
func populatedSpec(t *testing.T) Spec {
	t.Helper()
	inner := Spec{Kind: KindLeaf, ID: "inner", Type: "registered"}
	samples := map[reflect.Type]any{
		reflect.TypeFor[string]():          "value",
		reflect.TypeFor[int]():             1,
		reflect.TypeFor[json.RawMessage](): json.RawMessage(`{}`),
		reflect.TypeFor[Ref]():             Output("producer"),
		reflect.TypeFor[Inputs]():          OneInput(Output("producer")),
		reflect.TypeFor[[]Spec]():          []Spec{inner},
		reflect.TypeFor[map[string]Spec](): map[string]Spec{"case": inner},
		reflect.TypeFor[*Spec]():           &inner,
	}

	spec := Spec{Kind: KindLeaf}
	value := reflect.ValueOf(&spec).Elem()
	for index := range value.NumField() {
		field := value.Type().Field(index)
		if field.Name == "Kind" {
			continue
		}
		sample, ok := samples[field.Type]
		if !ok {
			t.Fatalf(
				"Spec field %s has type %s with no sample value; add one so the field matrices are checked against it",
				field.Name,
				field.Type,
			)
		}
		value.Field(index).Set(reflect.ValueOf(sample))
	}
	return spec
}

// specMemberNames returns the JSON member name of every Spec field except kind,
// read from the struct tags. It is the one source of truth the checks below
// compare against.
func specMemberNames(t *testing.T) []string {
	t.Helper()
	specType := reflect.TypeFor[Spec]()
	names := make([]string, 0, specType.NumField())
	for field := range specType.Fields() {
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "" {
			t.Fatalf("Spec field %s has no JSON member name", field.Name)
		}
		if name == fieldKind {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// TestJournalForgetKeepsNothingThatHoldsNothing pins what forget promises and
// nothing outside observes: it leaves behind neither a scope node nor a record
// map with nothing in it. Keys and the wire format report records, and an
// emptied scope holds none, so either one outliving its last record leaks
// silently — one per repeated boundary a long-lived Journal forgets.
func TestJournalForgetKeepsNothingThatHoldsNothing(t *testing.T) {
	journal := NewJournal()
	scope := []ScopeFrame{{ID: "loop", Indexed: true}, {ID: "body"}}
	kept := JournalKey{ID: "kept", Scope: scope}
	forgotten := JournalKey{ID: "forgotten", Scope: scope}
	for _, key := range []JournalKey{kept, forgotten} {
		if err := journal.Record(key, 1); err != nil {
			t.Fatalf("Record %v: %v", key, err)
		}
	}
	// A record directly on the outer scope, which therefore has a child. Emptying
	// it cannot be answered by pruning the node, because the child still holds
	// something -- so the map it emptied is what would be left behind instead.
	outer := JournalKey{ID: "outer", Scope: scope[:1]}
	if err := journal.Record(outer, 1); err != nil {
		t.Fatalf("Record %v: %v", outer, err)
	}

	if err := journal.Forget(forgotten); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if journal.root.children == nil {
		t.Fatal("forget dropped a scope that still holds a record")
	}
	if err := journal.Forget(outer); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	outerNode := journal.root.children[scope[0]]
	if outerNode == nil {
		t.Fatal("forget dropped a scope whose child still holds a record")
	}
	if outerNode.records != nil {
		t.Fatalf("forget left a record map holding nothing at %q", scope[0].ID)
	}

	if err := journal.Forget(kept); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if journal.root.children != nil {
		t.Fatalf("forget left %d scope nodes holding nothing", len(journal.root.children))
	}
}

// TestSpecFieldMatricesAgreeWithTheSpecStruct pins the three places that must
// know every Spec field to the struct itself: populatedFields, which decides
// what "populated" means; specKindFields, which decides what each kind may
// carry; and the embedded JSON Schema, which decides the same thing on the wire.
// Nothing else enforces that agreement, and its absence is not inert — a field
// added to Spec but missing from a matrix is accepted by every kind, and one
// missing from the schema makes Spec marshal a document it cannot unmarshal.
func TestSpecFieldMatricesAgreeWithTheSpecStruct(t *testing.T) {
	want := specMemberNames(t)

	t.Run("populatedFields", func(t *testing.T) {
		got := populatedSpec(t).populatedFields()
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("populatedFields() = %v; want every Spec field %v", got, want)
		}
	})

	t.Run("specKindFields union", func(t *testing.T) {
		union := make(map[string]struct{}, len(want))
		for _, fields := range specKindFields {
			for _, field := range fields {
				union[field] = struct{}{}
			}
		}
		if got := slices.Sorted(maps.Keys(union)); !slices.Equal(got, want) {
			t.Fatalf("specKindFields covers %v; want every Spec field %v", got, want)
		}
	})

	t.Run("wire schema per kind", func(t *testing.T) {
		for kind, members := range specSchemaKindMembers(t) {
			got := slices.Sorted(maps.Keys(members))
			expected := slices.Sorted(slices.Values(specKindFields[kind]))
			if !slices.Equal(got, expected) {
				t.Fatalf(
					"schema allows %v for kind %q; specKindFields allows %v",
					got,
					kind,
					expected,
				)
			}
		}
	})
}

// TestGraphDiagnosticFieldsNameTheGraphsOwnMembers is the Graph half of
// TestSpecFieldMatricesAgreeWithTheSpecStruct. A Spec's members reach the
// diagnostic vocabulary through specKindFields, which that test pins to the
// struct. A Graph's reach it only through the fieldError calls that locate them,
// so nothing keeps the two spellings the same word: a renamed tag leaves
// GraphError.Field naming a member no reader can find in the document, and a new
// member arrives with no name to be located by at all.
func TestGraphDiagnosticFieldsNameTheGraphsOwnMembers(t *testing.T) {
	located := []string{
		fieldConcurrency,
		fieldConfig,
		fieldDependsOn,
		fieldID,
		fieldInputs,
		fieldNodes,
		fieldTrigger,
		fieldType,
		fieldWhen,
	}
	slices.Sort(located)

	var members []string
	for _, ownerType := range []reflect.Type{
		reflect.TypeFor[Graph](),
		reflect.TypeFor[GraphNode](),
	} {
		for field := range ownerType.Fields() {
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "" {
				t.Fatalf("%s field %s has no JSON member name", ownerType.Name(), field.Name)
			}
			members = append(members, name)
		}
	}
	slices.Sort(members)

	if !slices.Equal(members, located) {
		t.Fatalf("graph members %v; the vocabulary locates %v", members, located)
	}
}

// specSchemaKindMembers reads the per-kind member sets out of the embedded Spec
// JSON Schema. Each kind's definition is the object whose kind property is
// pinned to a const, so the set is discovered rather than restated here.
func specSchemaKindMembers(t *testing.T) map[Kind]map[string]any {
	t.Helper()
	var document struct {
		Defs map[string]struct {
			Properties map[string]json.RawMessage `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(specSchemaJSON, &document); err != nil {
		t.Fatalf("decode embedded spec schema: %v", err)
	}

	byKind := make(map[Kind]map[string]any, len(specKindFields))
	for _, definition := range document.Defs {
		raw, ok := definition.Properties[fieldKind]
		if !ok {
			continue
		}
		var pinned struct {
			Const string `json:"const"`
		}
		if err := json.Unmarshal(raw, &pinned); err != nil || pinned.Const == "" {
			continue
		}
		members := make(map[string]any, len(definition.Properties))
		for member := range definition.Properties {
			if member != fieldKind {
				members[member] = nil
			}
		}
		byKind[Kind(pinned.Const)] = members
	}
	if len(byKind) != len(specKindFields) {
		t.Fatalf("schema defines %d kinds; specKindFields has %d", len(byKind), len(specKindFields))
	}
	return byKind
}

// TestStoreCells_reportsEveryLiveRecordAndHonorsAnEarlyStop covers the iterator
// contract that withoutNodes, its only caller, does not exercise: a caller may
// stop in either half of the traversal, and stopping must not run the rest.
func TestStoreCells_reportsEveryLiveRecordAndHonorsAnEarlyStop(t *testing.T) {
	const writes = storeOverlayLimit + 3

	// Writing past the limit leaves a snapshot behind, and the writes after that
	// form the overlay, so the fixture has both halves.
	store := NewStore()
	for index := range writes {
		store = store.WithOutput(fmt.Sprintf("node-%02d", index), index)
	}
	// Rewriting one cell makes the overlay shadow a snapshot record.
	store = store.WithOutput("node-00", "rewritten")
	if store.snapshot == nil || store.delta == nil {
		t.Fatalf(
			"fixture has snapshot %t, overlay %t; want both halves",
			store.snapshot != nil,
			store.delta != nil,
		)
	}

	seen := make(map[storeKey]cell, writes)
	for key, record := range store.cells() {
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("cell %v reported twice", key)
		}
		seen[key] = record
	}
	if len(seen) != writes {
		t.Fatalf("cells reported %d records; want %d", len(seen), writes)
	}
	if got := seen[storeKey{nodeID: "node-00", key: outputKey}].value; got != "rewritten" {
		t.Fatalf("shadowed cell = %v; want the overlay value", got)
	}

	for _, test := range []struct {
		name  string
		store Store
		after int
	}{
		{name: "in the overlay", store: store, after: 1},
		{name: "in the snapshot", store: store, after: store.depth + 1},
		{name: "with no overlay", store: store.compact(), after: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			count := 0
			for range test.store.cells() {
				count++
				if count == test.after {
					break
				}
			}
			if count != test.after {
				t.Fatalf("stopped after %d records; want %d", count, test.after)
			}
		})
	}

	for range (Store{}).cells() {
		t.Fatal("the zero Store reported a cell")
	}
}

// uniqueDependencyEdges reads the dependency lists as a set of edges, which is
// only a set if no node depends on itself and no edge is recorded twice -- either
// would make the scheduler's countdown reach zero at the wrong time.
func uniqueDependencyEdges(t *testing.T, plan graphPlan) map[[2]int]struct{} {
	t.Helper()
	edges := make(map[[2]int]struct{})
	for node, dependencies := range plan.dependencyNodeIndexes {
		if slices.Contains(dependencies, node) {
			t.Fatalf("node %d depends on itself", node)
		}
		for _, dependency := range dependencies {
			edge := [2]int{dependency, node}
			if _, duplicate := edges[edge]; duplicate {
				t.Fatalf("edge %v recorded twice", edge)
			}
			edges[edge] = struct{}{}
		}
	}
	return edges
}

func assertOneListEntryPerNode(t *testing.T, plan graphPlan, nodes int) {
	t.Helper()
	if len(plan.dependencyNodeIndexes) != nodes || len(plan.dependentNodeIndexes) != nodes {
		t.Fatalf(
			"lists sized %d and %d; want %d",
			len(plan.dependencyNodeIndexes),
			len(plan.dependentNodeIndexes),
			nodes,
		)
	}
}

// assertEachListIsTheOther consumes the dependency edges from the dependent side.
// Every dependent edge must find one, and none may be left over: that is what
// makes the two lists the same edges read from opposite ends.
func assertEachListIsTheOther(t *testing.T, plan graphPlan, edges map[[2]int]struct{}) {
	t.Helper()
	for dependency, dependents := range plan.dependentNodeIndexes {
		for _, node := range dependents {
			edge := [2]int{dependency, node}
			if _, present := edges[edge]; !present {
				t.Fatalf("dependent edge %v has no matching dependency", edge)
			}
			delete(edges, edge)
		}
	}
	if len(edges) != 0 {
		t.Fatalf("%d dependency edges have no matching dependent", len(edges))
	}
}

func assertInDegreesCountDependencies(t *testing.T, plan graphPlan) {
	t.Helper()
	counts := inDegrees(plan.dependencyNodeIndexes)
	for node, dependencies := range plan.dependencyNodeIndexes {
		if counts[node] != len(dependencies) {
			t.Fatalf("in-degree of %d = %d; want %d", node, counts[node], len(dependencies))
		}
	}
}

// TestGraphPlanAdjacencyListsAreTransposes pins what connectDependency maintains
// by hand: it appends to a node's dependency list and to the dependency's
// dependent list in the same breath, so the two must describe the same edges from
// opposite ends. The scheduler reads one to decide when a node is ready and the
// other to decide whom completing it releases, so a one-sided edge would either
// deadlock a run or start a node early. In-degrees are no longer stored, but the
// derivation is checked here too, since the scheduler counts down from it.
func TestGraphPlanAdjacencyListsAreTransposes(t *testing.T) {
	shapes := map[string]Graph{
		"empty":  {},
		"single": {Nodes: []GraphNode{{ID: "a", Type: "n"}}},
		"chain": {Nodes: []GraphNode{
			{ID: "a", Type: "n"},
			{ID: "b", Type: "n", Inputs: OneInput(Output("a"))},
			{ID: "c", Type: "n", Inputs: OneInput(Output("b"))},
		}},
		"diamond": {Nodes: []GraphNode{
			{ID: "a", Type: "n"},
			{ID: "b", Type: "n", Inputs: OneInput(Output("a"))},
			{ID: "c", Type: "n", DependsOn: []string{"a"}},
			{ID: "d", Type: "n", Inputs: OneInput(Output("b")), DependsOn: []string{"c"}},
		}},
		"gate and input from one node": {Nodes: []GraphNode{
			{ID: "route", Type: "n"},
			{ID: "b", Type: "n", Inputs: OneInput(Output("route")), When: []Gate{When("route", "yes")}},
		}},
		"external seed": {Nodes: []GraphNode{
			{ID: "a", Type: "n", Inputs: OneInput(Output("outside"))},
		}},
	}

	for name, graph := range shapes {
		t.Run(name, func(t *testing.T) {
			plan, err := graph.plan()
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			assertOneListEntryPerNode(t, plan, len(graph.Nodes))
			assertEachListIsTheOther(t, plan, uniqueDependencyEdges(t, plan))
			assertInDegreesCountDependencies(t, plan)
		})
	}
}

// depth caches the length of the overlay chain so a write does not have to walk
// it. Only withDelta and compact set it — one adds a link and increments, the
// other drops the chain and returns to zero — but everything that bounds an
// overlay reads it, so a drift would either let a Store grow past
// storeOverlayLimit unnoticed or flatten one that had not reached it.
func TestStoreDepthMatchesItsOverlay(t *testing.T) {
	links := func(s Store) int {
		count := 0
		for delta := s.delta; delta != nil; delta = delta.parent {
			count++
		}
		return count
	}
	check := func(t *testing.T, label string, s Store) Store {
		t.Helper()
		if s.depth != links(s) {
			t.Fatalf("%s: depth = %d; overlay has %d links", label, s.depth, links(s))
		}
		if s.depth > storeOverlayLimit {
			t.Fatalf("%s: depth %d exceeds the limit %d", label, s.depth, storeOverlayLimit)
		}
		return s
	}

	check(t, "zero value", Store{})
	check(t, "NewStore", NewStore())

	// Enough writes to cross the compaction boundary several times.
	store := NewStore()
	for index := range storeOverlayLimit*3 + 7 {
		store = check(t, fmt.Sprintf("write %d", index), store.WithOutput(fmt.Sprintf("n%03d", index), index))
	}

	base := check(t, "compact", store.compact())
	check(t, "sharedBase", store.sharedBase())
	check(t, "bounded", store.bounded())

	// Composition paths: each ends by bounding an overlay it extended.
	branches := make([]Store, 0, 4)
	for index := range 4 {
		branches = append(branches, base.WithOutput(fmt.Sprintf("branch%d", index), index))
	}
	check(t, "merge", base.merge(branches...))

	changes := make([]storeChange, 0, len(branches))
	for _, branch := range branches {
		changes = append(changes, branch.changesSince(base)...)
	}
	check(t, "withChanges", base.withChanges(changes))

	owned := nodeSet{}
	for index := range 8 {
		owned[fmt.Sprintf("n%03d", index)] = struct{}{}
	}
	check(t, "withoutNodes", base.withoutNodes(owned))
	check(t, "withoutNodes on an overlay", store.withoutNodes(owned))

	encoded, err := json.Marshal(store)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded Store
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	check(t, "decoded", decoded)
	check(t, "write after decoding", decoded.WithOutput("later", 1))
}

// TestGuaranteedOutputsAgreeAcrossRepresentations pins the one statement this
// package makes twice. The projection rules a composite applies to its body
// output are shared, but the walk that discovers what a body guarantees is
// written once for a built Step and once for a Spec, because each reads a
// different representation. A kind handled in one and not the other would let
// ValidateSpec accept a projection Validate rejects, or the reverse.
func TestGuaranteedOutputsAgreeAcrossRepresentations(t *testing.T) {
	registry := NewRegistry().
		MustRegisterNode("produce", func(spec NodeSpec) (Step, error) {
			return Interrupt(spec.ID, nil), nil
		}).
		MustRegisterSchema("produce", NodeSchema{Output: TypeAny})
	snapshot := registry.snapshot()

	leafStep := func(id string) Step { return Interrupt(id, nil) }
	leafSpec := func(id string) Spec { return Spec{Kind: KindLeaf, ID: id, Type: "produce"} }
	body := func(spec Spec) *Spec { return &spec }

	pairs := map[string]struct {
		step Step
		spec Spec
	}{
		"leaf": {leafStep("a"), leafSpec("a")},
		"sequence": {
			Sequence(leafStep("a"), leafStep("b")),
			Spec{Kind: KindSequence, Steps: []Spec{leafSpec("a"), leafSpec("b")}},
		},
		"parallel": {
			Parallel(ParallelConfig{Steps: []Step{leafStep("a"), leafStep("b")}}),
			Spec{Kind: KindParallel, Steps: []Spec{leafSpec("a"), leafSpec("b")}},
		},
		"branch": {
			Branch(BranchConfig{
				ID:       "pick",
				Resolver: flow.NodeFunc[Store, string](func(context.Context, Store) (string, error) { return "x", nil }),
				Cases:    map[string]Step{"x": leafStep("a"), "y": leafStep("b")},
			}),
			Spec{
				Kind: KindBranch, ID: "pick", Resolver: "r",
				Cases: map[string]Spec{"x": leafSpec("a"), "y": leafSpec("b")},
			},
		},
		"branch with a shared output": {
			Branch(BranchConfig{
				ID:       "pick",
				Resolver: flow.NodeFunc[Store, string](func(context.Context, Store) (string, error) { return "x", nil }),
				Cases:    map[string]Step{"x": leafStep("a"), "y": leafStep("a")},
			}),
			Spec{
				Kind: KindBranch, ID: "pick", Resolver: "r",
				Cases: map[string]Spec{"x": leafSpec("a"), "y": leafSpec("a")},
			},
		},
		"loop": {
			Loop(LoopConfig{ID: "l", Body: leafStep("a"), MaxIterations: 1}),
			Spec{Kind: KindLoop, ID: "l", Condition: "c", Body: body(leafSpec("a")), MaxIterations: 1},
		},
		"iteration": {
			Iteration(IterationConfig{ID: "each", Input: Output("seed"), Body: leafStep("a"), BodyOutput: Output("a")}),
			Spec{Kind: KindIteration, ID: "each", Input: Output("seed"), Body: body(leafSpec("a")), BodyOutput: Output("a")},
		},
		"subgraph": {
			Subgraph(SubgraphConfig{ID: "sg", Body: leafStep("a"), BodyOutput: Output("a")}),
			Spec{Kind: KindSubgraph, ID: "sg", Body: body(leafSpec("a")), BodyOutput: Output("a")},
		},
		"nested": {
			Sequence(
				leafStep("a"),
				Subgraph(SubgraphConfig{ID: "sg", Body: leafStep("b"), BodyOutput: Output("b")}),
			),
			Spec{Kind: KindSequence, Steps: []Spec{
				leafSpec("a"),
				{Kind: KindSubgraph, ID: "sg", Body: body(leafSpec("b")), BodyOutput: Output("b")},
			}},
		},
		"unknowable extension point": {
			Sequence(leafStep("a"), opaqueTestStepFunc(func(_ context.Context, s Store) (Store, error) {
				return s, nil
			})),
			Spec{Kind: KindSequence, Steps: []Spec{
				leafSpec("a"), {Kind: KindLeaf, ID: "opaque", Type: "unregistered"},
			}},
		},
	}

	for name, pair := range pairs {
		t.Run(name, func(t *testing.T) {
			fromStep := guaranteedOutputs(pair.step)
			fromSpec := (&specValidator{registry: snapshot}).guaranteedOutputs(pair.spec)
			if fromStep.known != fromSpec.known {
				t.Fatalf("known = %t from the step and %t from the spec", fromStep.known, fromSpec.known)
			}
			if !maps.Equal(fromStep.nodes, fromSpec.nodes) {
				t.Fatalf("outputs = %v from the step and %v from the spec",
					slices.Sorted(maps.Keys(fromStep.nodes)), slices.Sorted(maps.Keys(fromSpec.nodes)))
			}
		})
	}
}

// storeAtDepth returns a Store carrying exactly depth overlay writes. depth must
// not exceed storeOverlayLimit, or the writes would flatten on the way.
func storeAtDepth(t *testing.T, depth int) Store {
	t.Helper()
	if depth > storeOverlayLimit {
		t.Fatalf("depth %d exceeds the overlay limit", depth)
	}
	store := NewStore()
	for write := range depth {
		store = store.WithOutput(fmt.Sprintf("n%d", write), write)
	}
	if store.depth != depth {
		t.Fatalf("built a Store at depth %d; want %d", store.depth, depth)
	}
	return store
}

// TestStoreOverlayBoundsAreExact pins the two places this package decides when
// to flatten. Neither changes a value a caller can read, so nothing else
// notices if either moves by one write: bounded lets an overlay reach the limit
// and flattens on the write after it, while sharedBase flattens one write
// earlier so a concurrent deriver starts with the whole budget available.
func TestStoreOverlayBoundsAreExact(t *testing.T) {
	store := NewStore()
	depths := make([]int, 0, storeOverlayLimit+2)
	for write := range storeOverlayLimit + 2 {
		store = store.WithOutput(fmt.Sprintf("n%d", write), write)
		depths = append(depths, store.depth)
	}
	if got := depths[storeOverlayLimit-1]; got != storeOverlayLimit {
		t.Fatalf("depth after %d writes = %d; want the full limit %d",
			storeOverlayLimit, got, storeOverlayLimit)
	}
	if got := depths[storeOverlayLimit]; got != 0 {
		t.Fatalf("depth after %d writes = %d; want a flattened overlay",
			storeOverlayLimit+1, got)
	}
	if got := depths[storeOverlayLimit+1]; got != 1 {
		t.Fatalf("depth after %d writes = %d; want one write over a fresh snapshot",
			storeOverlayLimit+2, got)
	}

	for _, depth := range []int{0, 1, storeOverlayLimit - 1} {
		if got := storeAtDepth(t, depth).sharedBase().depth; got != depth {
			t.Fatalf("sharedBase at depth %d = %d; want it left alone", depth, got)
		}
	}
	if got := storeAtDepth(t, storeOverlayLimit).sharedBase().depth; got != 0 {
		t.Fatalf("sharedBase at the limit = %d; want a base with the whole budget", got)
	}
}

// TestStore_removalDoesNotHideTheChangesAroundIt uses withoutNodes because a
// removal has no public spelling: a graph clears the cells its nodes own, and
// what a caller then sees is a Store whose chain holds a removal underneath the
// writes that replaced it. Both walks that skip a removal are asked here, and
// each needs a different base to reach: a descendant base takes the delta walk,
// an unrelated one the cell-by-cell comparison. A walk that stopped at a removal
// instead of passing over it would report a Store as unchanged in one and
// half-written in the other.
func TestStore_removalDoesNotHideTheChangesAroundIt(t *testing.T) {
	base := NewStore().WithCell("owned", "extra", 1).WithOutput("seed", 2)
	next := base.withoutNodes(newNodeSet("owned")).WithOutput("owned", 3)

	changes := next.Changes(base)
	if len(changes) != 1 ||
		changes[0].Ref != Output("owned") || changes[0].Value.(int) != 3 {
		t.Fatalf("Changes against the store it derives from = %+v; want the one write past the removal", changes)
	}

	// An unrelated base has never held the removed cell, so the removal says
	// nothing and is skipped -- but the cells the Store did write remain. The base
	// has to hold something: every Store descends from an empty one, which would
	// take the delta walk again instead.
	fresh := changesRefs(next.Changes(NewStore().WithOutput("elsewhere", 0)))
	if !slices.Equal(fresh, []Ref{Output("seed"), Output("owned")}) {
		t.Fatalf("Changes against an unrelated store = %v; want both writes in write order", fresh)
	}
}

func changesRefs(changes []Write) []Ref {
	refs := make([]Ref, 0, len(changes))
	for _, change := range changes {
		refs = append(refs, change.Ref)
	}
	return refs
}

// TestGraphExecution_admitsExactlyTheConcurrencyLimit pins the bound a graph's
// Concurrency setting places on its scheduler. Nothing observable changes when
// the limit is off by one -- the same nodes run and produce the same Store -- so
// only the admission count states it.
func TestGraphExecution_admitsExactlyTheConcurrencyLimit(t *testing.T) {
	tests := []struct{ ready, limit, want int }{
		{ready: 5, limit: 1, want: 1},
		{ready: 5, limit: 2, want: 2},
		{ready: 3, limit: 3, want: 3},
		{ready: 2, limit: 5, want: 2},
	}
	for _, test := range tests {
		steps := make(stepList, test.ready)
		ready := make([]int, test.ready)
		for index := range steps {
			steps[index] = opaqueTestStepFunc(func(_ context.Context, s Store) (Store, error) {
				return s, nil
			})
			ready[index] = index
		}
		execution := graphExecution{
			graph: graphStep{steps: steps, dependencyNodeIndexes: make([][]int, test.ready)},
			input: NewStore(),
			ready: ready,
		}
		outcomes := make(chan graphOutcome, test.ready)
		execution.startReady(t.Context(), outcomes, test.limit)
		if execution.active != test.want || execution.head != test.want {
			t.Fatalf("%d ready under a limit of %d admitted active=%d head=%d; want %d",
				test.ready, test.limit, execution.active, execution.head, test.want)
		}
		for range test.want {
			<-outcomes
		}
	}
}

// TestGraphExecution_dropsAnOutcomeFinishingAfterAFailure pins what the returned
// Store means once a node has failed. A sibling admitted before the failure may
// still finish successfully, and its writes are deliberately not merged: the
// Journal keeps that completed boundary for the next run, while the Store handed
// back with an error reports only what was known good before it.
func TestGraphExecution_dropsAnOutcomeFinishingAfterAFailure(t *testing.T) {
	input := NewStore().WithOutput("seed", 1)
	execution := graphExecution{
		graph: graphStep{
			steps:                make(stepList, 2),
			dependentNodeIndexes: make([][]int, 2),
		},
		input:   input,
		counts:  []int{0, 0},
		changes: make([][]storeChange, 2),
		active:  2,
	}

	boom := errors.New("node failed")
	if !execution.accept(graphOutcome{index: 0, input: input, store: input, err: boom}, false) {
		t.Fatal("accept did not report the first failure")
	}
	late := input.WithOutput("late", 2)
	if execution.accept(graphOutcome{index: 1, input: input, store: late}, false) {
		t.Fatal("accept reported a second failure for a successful outcome")
	}
	if execution.changes[1] != nil {
		t.Fatalf("a post-failure outcome recorded %v", execution.changes[1])
	}
	if _, found := execution.completedStore().Lookup(Output("late")); found {
		t.Fatal("the returned Store carries a write made after the failure")
	}
}

// TestStore_neverHoldsACellWithTheZeroIdentity pins what lets a reader treat the
// cell a Store lacks as the zero cell: revision and lineage 0 belong to that zero
// alone. Every cell a Store holds is therefore stamped from the counter, and the
// two whose identity is assembled rather than inherited from an earlier version
// are the ones that could forget -- a removal marker, and a cell arriving from
// the wire with no history behind it.
func TestStore_neverHoldsACellWithTheZeroIdentity(t *testing.T) {
	live := NewStore().WithCell("owned", "extra", 1).WithOutput("kept", 2)
	removed := live.withoutNodes(newNodeSet("owned"))

	encoded, err := json.Marshal(removed)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded Store
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for name, store := range map[string]Store{"removed": removed, "decoded": decoded} {
		for key, record := range store.cells() {
			if record.revision == 0 || record.lineage == 0 {
				t.Fatalf("%s cell %+v = %+v; want an identity taken from the counter", name, key, record)
			}
		}
	}
}

// TestSpecCompile_carriesEverySchedulingSettingIntoTheStep pins the settings a
// Spec carries that change only how work is scheduled. A step built without them
// computes the same outputs from the same inputs, so no behavioural test of a
// compiled spec can tell whether the value arrived -- a parallel spec asking for
// two at a time and one asking for all of them agree on every answer. The step's
// own limit is where the value either is or is not.
func TestSpecCompile_carriesEverySchedulingSettingIntoTheStep(t *testing.T) {
	registry := NewRegistry().
		MustRegisterNode("leaf", InterruptFactory()).
		MustRegisterCondition("again", flow.NodeFunc[Store, bool](
			func(context.Context, Store) (bool, error) { return true, nil },
		))
	leaf := Spec{Kind: KindLeaf, ID: "inner", Type: "leaf"}

	tests := map[string]struct {
		spec Spec
		want func(Step) (int, bool)
	}{
		"parallel concurrency": {
			spec: Spec{Kind: KindParallel, Concurrency: 3, Steps: []Spec{leaf}},
			want: func(step Step) (int, bool) {
				parallel, ok := step.(parallelStep)
				return parallel.limit, ok
			},
		},
		"iteration concurrency": {
			spec: Spec{
				Kind:        KindIteration,
				ID:          "each",
				Input:       Output("items"),
				Body:        &leaf,
				BodyOutput:  Output("inner"),
				Concurrency: 3,
			},
			want: func(step Step) (int, bool) {
				iteration, ok := step.(iterationStep)
				return iteration.limit, ok
			},
		},
		"loop iterations": {
			spec: Spec{
				Kind:          KindLoop,
				ID:            "repeat",
				Body:          &leaf,
				Condition:     "again",
				MaxIterations: 3,
			},
			want: func(step Step) (int, bool) {
				loop, ok := step.(loopStep)
				return loop.config.MaxIterations, ok
			},
		},
	}
	t.Run("graph concurrency", func(t *testing.T) {
		step, err := registry.CompileGraph(Graph{
			Concurrency: 3,
			Nodes:       []GraphNode{{ID: "only", Type: "leaf"}},
		})
		if err != nil {
			t.Fatalf("CompileGraph: %v", err)
		}
		graph, ok := step.(graphStep)
		if !ok || graph.limit != 3 {
			t.Fatalf("graph step = %#v; want the 3 the graph asked for", step)
		}
	})

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			step, err := registry.CompileSpec(test.spec)
			if err != nil {
				t.Fatalf("CompileSpec: %v", err)
			}
			got, ok := test.want(step)
			if !ok {
				t.Fatalf("CompileSpec built %T; want the step the kind names", step)
			}
			if got != 3 {
				t.Fatalf("setting reached the step as %d; want the 3 the spec asked for", got)
			}
		})
	}
}

// TestParallelRunBranches_boundsThemWithTheLimitItHolds continues where
// TestSpecCompile_carriesEverySchedulingSettingIntoTheStep stops: the limit is on
// the step, and this is the step handing it to the mapper that schedules the
// branches. Concurrency changes no answer, so the proof that it arrived is a
// value flow.MapConfig refuses -- a mapper built without the limit runs the
// branches unbounded and reports nothing at all.
func TestParallelRunBranches_boundsThemWithTheLimitItHolds(t *testing.T) {
	step := parallelStep{branches: stepList{Sequence(), Sequence()}, limit: -1}

	input := NewStore().WithOutput("start", 1)
	out, err := step.runBranches(t.Context(), input)
	if !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("runBranches error = %v; want the mapper to reject the limit it was given", err)
	}
	if len(out.Changes(input)) != 0 {
		t.Fatalf("runBranches changed %v; want the untouched input", out.Changes(input))
	}
}

// TestInputsValidation_namesTheBindingItChecked pins the vocabulary rule
// bindingVocabulary exists to state: a port and a seed each describe themselves
// with one word, and the same word, whichever of the four checks found the
// problem. The name of the binding is the only part of the message that says
// which of two Inputs maps the caller wrote wrong, and dropping it leaves the
// reference to explain itself.
func TestInputsValidation_namesTheBindingItChecked(t *testing.T) {
	// A reference no rule set accepts, so every check reaches the same report.
	invalid := Inputs{"port": {NodeID: "bad\xff", Path: outputPath}}
	tests := map[string]struct {
		check func(Inputs) error
		want  string
	}{
		"definition port": {check: Inputs.validatePorts, want: `input port "port"`},
		"definition seed": {check: Inputs.validateSeeds, want: `subgraph seed "port"`},
		"json text port":  {check: Inputs.validatePortJSONText, want: `input port "port"`},
		"json text seed":  {check: Inputs.validateSeedJSONText, want: `subgraph seed "port"`},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := test.check(invalid)
			if err == nil || !strings.HasPrefix(err.Error(), test.want+": ") {
				t.Fatalf("error = %v; want it to open with %s", err, test.want)
			}
		})
	}
}

// TestEmbeddedSchemas_compileUnderTheIdentityTheyDeclare pins the one agreement
// between a schema constant and the schema file it names: the URL this package
// compiles a document under is the $id that document publishes. The backend takes
// its identity from the $id, so a disagreement breaks nothing at run time -- it
// leaves the package naming a schema by a URL the schema itself does not claim,
// and the two copies are in two languages and edited by hand. configSchemaURL has
// no counterpart here: it is the base URI given to an application's config schema,
// which publishes no identity of its own.
func TestEmbeddedSchemas_compileUnderTheIdentityTheyDeclare(t *testing.T) {
	for url, document := range map[string][]byte{
		specSchemaURL:  specSchemaJSON,
		graphSchemaURL: graphSchemaJSON,
	} {
		var declared struct {
			ID string `json:"$id"`
		}
		if err := json.Unmarshal(document, &declared); err != nil {
			t.Fatalf("decode %s: %v", url, err)
		}
		if declared.ID != url {
			t.Fatalf("schema declares $id %q; the package compiles it under %q", declared.ID, url)
		}
	}
}

// TestADefinitionWithSeveralDefectsIsRefusedForTheSameOne pins the determinism
// three validators buy by walking a sorted map: a branch checking its case list, a
// branch descending into those cases, and a NodeSchema looking for the ports it
// declared. Each returns the first defect it meets, so the sort is the only reason
// a definition broken in three places is refused for the same one on every pass.
// Nothing in the sorted spelling says that, and dropping it breaks no accepted
// definition -- it only makes a rejected one report a different reason each time an
// editor asks. TestInputsWalkInNameOrderSoTheFirstOffenderIsAlwaysTheSame holds the
// wiring side of the same rule.
func TestADefinitionWithSeveralDefectsIsRefusedForTheSameOne(t *testing.T) {
	resolve := flow.NodeFunc[Store, string](
		func(context.Context, Store) (string, error) { return "alpha", nil },
	)
	unreadable := func(id string) Step {
		return LeafFunc(id, Ref{}, func(_ context.Context, x int) (int, error) { return x, nil })
	}
	registry := NewRegistry().
		MustRegisterNode("declared", Factory(func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }), nil
		})).
		MustRegisterSchema("declared", NodeSchema{
			Inputs: Ports{"alpha": TypeAny, "mike": TypeAny, "zulu": TypeAny},
			Output: TypeAny,
		})

	// Every definition below is broken at alpha, mike, and zulu at once, so the one
	// named is decided by the walk rather than by the definition.
	boundaries := map[string]func() error{
		"a branch case list": func() error {
			return flow.Validate(Branch(BranchConfig{
				ID:       "b",
				Resolver: resolve,
				Cases:    map[string]Step{"alpha": nil, "mike": nil, "zulu": nil},
			}))
		},
		"the steps inside those cases": func() error {
			return flow.Validate(Branch(BranchConfig{
				ID:       "b",
				Resolver: resolve,
				Cases: map[string]Step{
					"alpha": unreadable("alpha"),
					"mike":  unreadable("mike"),
					"zulu":  unreadable("zulu"),
				},
			}))
		},
		"the ports a schema declared": func() error {
			return registry.ValidateGraph(Graph{
				Nodes: []GraphNode{{ID: "n", Type: "declared"}},
			})
		},
	}
	for _, name := range slices.Sorted(maps.Keys(boundaries)) {
		err := boundaries[name]()
		if err == nil {
			t.Fatalf("%s: accepted a definition broken in three places", name)
		}
		if !strings.Contains(err.Error(), `"alpha"`) {
			t.Errorf("%s: error = %v; want it to name the first defect, alpha", name, err)
		}
		for _, later := range []string{"mike", "zulu"} {
			if strings.Contains(err.Error(), `"`+later+`"`) {
				t.Errorf("%s: error = %v; want it to stop at alpha, not reach %s", name, err, later)
			}
		}
	}
}

// TestOutputGuaranteeCombinatorsLeaveTheirOperandsAlone states the value semantics
// of the two set combinators, which is the whole reason union clones before
// copying into the result. Both are methods on a struct passed by value, so a
// reader assumes a.union(b) reports what the two guarantee together and leaves a
// alone -- and the map inside is what makes that an assumption rather than a fact.
// Nothing observes it today: unionOutputs folds the result straight back over the
// accumulator, so an in-place union computes the same answer, and its clone is a
// defense no caller can currently trip. It is stated here rather than left to the
// next caller to discover.
func TestOutputGuaranteeCombinatorsLeaveTheirOperandsAlone(t *testing.T) {
	left := knownOutputs("a")
	right := knownOutputs("b")

	if both := left.union(right); !both.contains("a") || !both.contains("b") {
		t.Fatalf("union of a and b contains a=%v b=%v; want both", both.contains("a"), both.contains("b"))
	}
	if left.contains("b") || right.contains("a") {
		t.Fatalf("union mutated an operand: left has b=%v, right has a=%v", left.contains("b"), right.contains("a"))
	}

	if common := left.intersection(right); common.contains("a") || common.contains("b") {
		t.Fatal("intersection of disjoint guarantees is not empty")
	}
	if !left.contains("a") || !right.contains("b") {
		t.Fatal("intersection mutated an operand")
	}
}

// publishedVocabularies spells out every enumerated value this package publishes
// as text that leaves it. A Kind names a step in a Spec document and a node in a
// Description, an EventKind and a StepOp reach an application that traces or
// persists a run, a ValueType is a member of the NodeSchema an editor encodes, a
// Trigger is a Graph member, and the registration kinds are the Kind field of a
// RegistrationError.
//
// A caller compares against the constant, and so does almost every test here,
// which makes both invariant to the spelling: renaming twenty-two of these values
// failed nothing. The ones that did fail were incidental -- a test that happened to
// write the word out -- which is why TypeString and TypeNumber were held and the
// four beside them were not.
//
// The name kinds beside StepOp in errors.go are deliberately absent. Each is a
// fragment of a sentence, and this package's stated contract is the sentinel and
// the structured location rather than the words around them.
var publishedVocabularies = map[string]string{
	"KindLeaf":      "leaf",
	"KindSequence":  "sequence",
	"KindParallel":  "parallel",
	"KindBranch":    "branch",
	"KindLoop":      "loop",
	"KindIteration": "iteration",
	"KindSubgraph":  "subgraph",
	"KindAwait":     "await",
	"KindGraph":     "graph",
	"KindInterrupt": "interrupt",
	"KindOpaque":    "opaque",

	"EventStarted":   "started",
	"EventCompleted": "completed",
	"EventFailed":    "failed",
	"EventSuspended": "suspended",
	"EventSkipped":   "skipped",
	"EventBypassed":  "bypassed",

	"TypeAny":    "any",
	"TypeString": "string",
	"TypeNumber": "number",
	"TypeBool":   "bool",
	"TypeArray":  "array",
	"TypeObject": "object",

	"TriggerAll": "",
	"TriggerAny": "any",

	"OpValidate": "validate",
	"OpBind":     "bind",
	"OpRun":      "run",

	"registrationCondition": "condition",
	"registrationNode":      "node",
	"registrationResolver":  "resolver",
	"registrationSchema":    "schema",
}

// vocabularyTypes are the declared types whose every constant the table above must
// spell. The registration kinds have no type of their own -- they are the string a
// RegistrationError carries -- so they are recognized by name instead.
var vocabularyTypes = map[string]bool{
	"Kind":      true,
	"EventKind": true,
	"ValueType": true,
	"Trigger":   true,
	"StepOp":    true,
}

// TestEveryPublishedVocabularySpellsItselfOut reads the constants out of the
// package source rather than the table's own keys, so a new member of a published
// vocabulary has to be spelled before it can ship, and a spelling left behind by a
// removed one fails instead of reading as coverage.
func TestEveryPublishedVocabularySpellsItselfOut(t *testing.T) {
	found := publishedConstants(t)
	if len(found) == 0 {
		t.Fatal("no vocabulary constant found; the scan stopped seeing the package")
	}
	for _, constant := range slices.Sorted(maps.Keys(found)) {
		want, listed := publishedVocabularies[constant]
		switch {
		case !listed:
			t.Errorf("%s publishes %q, which is spelled nowhere", constant, found[constant])
		case found[constant] != want:
			t.Errorf("%s = %q; the value this package publishes is %q", constant, found[constant], want)
		}
	}
	for _, constant := range slices.Sorted(maps.Keys(publishedVocabularies)) {
		if _, ok := found[constant]; !ok {
			t.Errorf("%s is spelled out but the package declares no such constant", constant)
		}
	}
}

// publishedConstants returns the declared value of every constant belonging to a
// published vocabulary, read from this package's own source.
func publishedConstants(t *testing.T) map[string]string {
	t.Helper()
	fileSet := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	found := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, declaration := range file.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok || general.Tok != token.CONST {
				continue
			}
			collectVocabulary(t, general, found)
		}
	}
	return found
}

func collectVocabulary(t *testing.T, declaration *ast.GenDecl, found map[string]string) {
	t.Helper()
	for _, spec := range declaration.Specs {
		value, ok := spec.(*ast.ValueSpec)
		if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
			continue
		}
		constant := value.Names[0].Name
		if !publishesText(constant, value.Type) {
			continue
		}
		literal, ok := value.Values[0].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			t.Fatalf("%s belongs to a published vocabulary but is not a string literal", constant)
		}
		text, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatalf("%s: %v", constant, err)
		}
		found[constant] = text
	}
}

func publishesText(constant string, declared ast.Expr) bool {
	if strings.HasPrefix(constant, "registration") {
		return true
	}
	name, ok := declared.(*ast.Ident)
	return ok && vocabularyTypes[name.Name]
}

// TestValidatingOneBoundaryAllocatesNoIdentitySet guards the one representation
// choice in definitionValidator that measurement rather than taste decided. The
// common definition names a single boundary -- a leaf validates itself on every
// execution -- so the first claimed ID stays in a field and no set is built to hold
// it.
//
// Collapsing that into "always use the set" is the simpler model, and it passes
// every other test in this repository, which is why the cost is written down here:
// two allocations per validated boundary. BenchmarkSequenceRunScaling/512 goes from
// 1612 to 2636 allocations per run, and BenchmarkGraphRunScaling/chain/128 from 803
// to 1059.
//
// The budget below is what one boundary costs today, not a target. The second
// boundary is what may build the set, and does, which is what makes the budget a
// property of the representation instead of validation in general.
func TestValidatingOneBoundaryAllocatesNoIdentitySet(t *testing.T) {
	const oneBoundary = 1
	passThrough := flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
		return value, nil
	})
	single := Leaf("only", Output("seed").Bind[int](), passThrough)
	pair := Sequence(single, Leaf("second", Output("seed").Bind[int](), passThrough))

	if allocs := testing.AllocsPerRun(200, func() { _ = flow.Validate(single) }); allocs > oneBoundary {
		t.Errorf(
			"validating one boundary allocated %.0f times; want at most %d, because the "+
				"first claimed ID is held inline instead of in a set",
			allocs, oneBoundary)
	}
	if allocs := testing.AllocsPerRun(200, func() { _ = flow.Validate(pair) }); allocs <= oneBoundary {
		t.Errorf(
			"validating two boundaries allocated %.0f times; want more than %d, since the "+
				"second claim is what builds the set the first one avoids",
			allocs, oneBoundary)
	}
}

// TestEveryFanOutSharesOneFlattenedBase pins the rule at the three sites that
// apply it, not just in the helper that implements it. A composite handing one
// Store to concurrent derivers flattens it first, so the fan-out pays one
// snapshot copy instead of one per deriver;
// TestStore_sharesOneBaseAcrossConcurrentDerivers proves sharedBase does that,
// and the base-scaling benchmarks measure what it saves. Nothing checked that
// the composites still call it: removing the call from Parallel, Iteration, or
// the graph scheduler passed the whole suite.
//
// The observable form of the rule is the snapshot every deriver reads through.
// A Store at the overlay limit carries no snapshot yet, so a child that received
// one unflattened would either hold none or flatten its own.
func TestEveryFanOutSharesOneFlattenedBase(t *testing.T) {
	// Exactly at the limit: one more write would flatten it here instead, which
	// would leave the check below proving nothing.
	fill := func(store Store) Store {
		for index := range storeOverlayLimit - store.depth {
			store = store.WithCell("filler", fmt.Sprintf("key-%02d", index), index)
		}
		if store.depth != storeOverlayLimit || store.snapshot != nil {
			t.Fatalf("input has depth %d and snapshot %v; want the limit and none",
				store.depth, store.snapshot)
		}
		return store
	}

	const derivers = 2
	captured := make(chan *storeSnapshot, derivers)
	capture := BinderFunc[int](func(store Store) (int, error) {
		captured <- store.snapshot
		return 0, nil
	})
	capturingStep := Leaf(
		"captured",
		capture,
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) { return value, nil }),
	)

	registry := NewRegistry()
	registry.MustRegisterNode("capture", BindFactory(
		func(struct{}, Inputs) (Binder[int], error) { return capture, nil },
		func(struct{}) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
				return value, nil
			}), nil
		},
	))
	graph, err := registry.CompileGraph(Graph{Nodes: []GraphNode{
		{ID: "one", Type: "capture"},
		{ID: "two", Type: "capture"},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	for name, step := range map[string]Step{
		"parallel": Parallel(ParallelConfig{Steps: []Step{
			capturingStep,
			Leaf("other", capture,
				flow.NodeFunc[int, int](func(_ context.Context, v int) (int, error) { return v, nil })),
		}}),
		"iteration": Iteration(IterationConfig{
			ID:         "each",
			Input:      At("seed", "items"),
			Body:       capturingStep,
			BodyOutput: Output("captured"),
		}),
		"graph": graph,
	} {
		t.Run(name, func(t *testing.T) {
			input := fill(NewStore().WithCell("seed", "items", []any{1, 2}))
			if _, err := Run(t.Context(), step, input, RunConfig{}); err != nil {
				t.Fatalf("Run: %v", err)
			}
			first, second := <-captured, <-captured
			if first == nil {
				t.Fatal("a deriver read through no snapshot; its input was never flattened")
			}
			if first != second {
				t.Fatal("two derivers read through different snapshots; each flattened its own")
			}
		})
	}
}
