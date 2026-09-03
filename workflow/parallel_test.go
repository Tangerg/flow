package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestParallel_mergesBranches(t *testing.T) {
	from := workflow.Ref{NodeID: "start", Path: "/output"}.Bind[int]()
	a := workflow.Leaf("a", from, flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil }))
	b := workflow.Leaf("b", from, flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil }))

	p := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{a, b}, Concurrency: 2})

	out, err := p.Run(t.Context(), workflow.NewStore().WithOutput("start", 5))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Lookup(workflow.Output("a")); !ok || v.(int) != 10 {
		t.Fatalf("branch a = %v, %v; want 10", v, ok)
	}
	if v, ok := out.Lookup(workflow.Output("b")); !ok || v.(int) != 6 {
		t.Fatalf("branch b = %v, %v; want 6", v, ok)
	}
}

func TestParallel_ownsBranchSliceStructure(t *testing.T) {
	branches := []workflow.Step{leafStep("original")}
	parallel := workflow.Parallel(workflow.ParallelConfig{Steps: branches})
	branches[0] = leafStep("changed")

	out, err := parallel.Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, getErr := out.Get[int](workflow.Output("original")); getErr != nil || got != 1 {
		t.Fatalf("original output = %d, %v; want 1, nil", got, getErr)
	}
	if _, ok := out.Lookup(workflow.Output("changed")); ok {
		t.Fatal("source-slice mutation reconfigured Parallel")
	}
}

func TestParallel_failFast(t *testing.T) {
	boom := errors.New("boom")
	from := workflow.Ref{NodeID: "start", Path: "/output"}.Bind[int]()
	ok := workflow.Leaf("ok", from, flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	bad := workflow.Leaf("bad", from, flow.NodeFunc[int, int](func(_ context.Context, _ int) (int, error) { return 0, boom }))

	_, err := workflow.Parallel(workflow.ParallelConfig{
		Steps: []workflow.Step{ok, bad},
	}).Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
}

func TestParallel_failureReturnsTheInputStore(t *testing.T) {
	boom := errors.New("boom")
	completed := make(chan struct{})
	success := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			next := store.WithOutput("finished", 1)
			close(completed)
			return next, nil
		},
	)
	failure := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			<-completed
			return store, boom
		},
	)
	input := workflow.NewStore().WithOutput("seed", 1)

	output, err := workflow.Parallel(workflow.ParallelConfig{
		Steps: []workflow.Step{success, failure},
	}).
		Run(t.Context(), input)
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v; want boom", err)
	}
	if _, present := output.Lookup(workflow.Output("finished")); present {
		t.Fatal("ordinary failure returned a successful sibling's write")
	}
	if value, getErr := output.Get[int](workflow.Output("seed")); getErr != nil || value != 1 {
		t.Fatalf("seed = %d, %v; want 1, nil", value, getErr)
	}
}

func TestParallel_singleBranchPreservesIndexError(t *testing.T) {
	boom := errors.New("boom")
	branch := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		return store, boom
	})

	_, err := workflow.Parallel(workflow.ParallelConfig{
		Steps: []workflow.Step{branch},
	}).Run(t.Context(), workflow.NewStore())
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 0 || !errors.Is(err, boom) {
		t.Fatalf("err = %v; want IndexError(0, boom)", err)
	}
}

func TestParallel_emptyAndSingleRespectCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	identity := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		return store, nil
	})

	for _, step := range []workflow.Step{workflow.Parallel(workflow.ParallelConfig{
		Steps: nil,
	}), workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{identity}})} {
		if _, err := step.Run(ctx, workflow.NewStore()); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v; want context.Canceled", err)
		}
	}
}

func TestParallel_emptyPassesThrough(t *testing.T) {
	input := workflow.NewStore().WithOutput("start", 1)
	output, err := workflow.Parallel(workflow.ParallelConfig{Steps: nil}).
		Run(t.Context(), input)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := output.Get[int](workflow.Output("start")); err != nil || got != 1 {
		t.Fatalf("start = %v, %v; want 1", got, err)
	}
}

func TestParallel_rejectsDuplicateStaticIDs(t *testing.T) {
	step := leafStep("same")
	_, err := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{step, step}}).
		Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("error = %v; want ErrDuplicateStep", err)
	}
}

func TestParallel_singleBranchCancellationTakesPrecedence(t *testing.T) {
	boom := errors.New("boom")
	tests := map[string]error{
		"branch error":   boom,
		"branch success": nil,
	}
	for name, branchErr := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			branch := flow.NodeFunc[workflow.Store, workflow.Store](
				func(context.Context, workflow.Store) (workflow.Store, error) {
					cancel()
					return workflow.NewStore(), branchErr
				},
			)
			_, err := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{branch}}).
				Run(ctx, workflow.NewStore())
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v; want context.Canceled", err)
			}
		})
	}
}

func TestParallel_singleSuspensionIsPreserved(t *testing.T) {
	output, err := workflow.Parallel(workflow.ParallelConfig{
		Steps: []workflow.Step{workflow.Await("wait", workflow.Output("ready"))},
	}).
		Run(t.Context(), workflow.NewStore())
	if !workflow.SuspendedOnly(err) {
		t.Fatalf("error = %v; want pure suspension", err)
	}
	if _, ok := output.Lookup(workflow.Output("ready")); ok {
		t.Fatal("suspended single branch created its awaited value")
	}
	// A wait is not a branch failure. Wrapping it in an IndexError would keep it
	// classifiable as a suspension while telling a caller that branch 0 failed,
	// which is the vocabulary reserved for a real error (see
	// TestParallel_singleBranchPreservesIndexError).
	var indexErr *flow.IndexError
	waits := workflow.Suspensions(err)
	if errors.As(err, &indexErr) || len(waits) != 1 || waits[0].ID != "wait" {
		t.Fatalf("error = %v; want the branch's own wait, unlabelled by index", err)
	}
}

func TestParallel_validatesEveryBranchBeforeRunning(t *testing.T) {
	ran := false
	first := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		ran = true
		return store, nil
	})
	_, err := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{first, nil}}).
		Run(t.Context(), workflow.NewStore())
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 1 || !errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want IndexError(1, ErrNilStep)", err)
	}
	if ran {
		t.Fatal("a branch ran before the invalid parallel composition was rejected")
	}
}

func TestParallel_rejectsTypedNilFunctionBeforeRunning(t *testing.T) {
	ran := false
	first := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			ran = true
			return store, nil
		},
	)
	var invalid flow.NodeFunc[workflow.Store, workflow.Store]

	_, err := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{first, invalid}}).
		Run(t.Context(), workflow.NewStore())
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 1 ||
		!errors.Is(err, workflow.ErrNilStep) {
		t.Fatalf("err = %v; want IndexError(1, ErrNilStep)", err)
	}
	if ran {
		t.Fatal("a branch ran before the typed nil function was rejected")
	}
}

func TestParallel_rejectsNegativeConcurrencyEvenWhenEmpty(t *testing.T) {
	_, err := workflow.Parallel(workflow.ParallelConfig{Steps: nil, Concurrency: -1}).
		Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("err = %v; want ErrInvalidConfig", err)
	}
}

func TestParallel_mergesOnlyBranchWrites(t *testing.T) {
	writeExisting := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s.WithCell("existing", "value", 1), nil
	})
	writeOther := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s.WithCell("other", "value", 2), nil
	})
	base := workflow.NewStore().WithCell("existing", "value", 0)

	out, err := workflow.Parallel(workflow.ParallelConfig{
		Steps: []workflow.Step{writeExisting, writeOther},
	}).Run(t.Context(), base)
	if err != nil {
		t.Fatalf("Parallel: %v", err)
	}
	if got, _ := out.Lookup(workflow.At("existing", "value")); got != 1 {
		t.Fatalf("existing value = %v; stale base snapshot overwrote branch write", got)
	}
	if got, _ := out.Lookup(workflow.At("other", "value")); got != 2 {
		t.Fatalf("other value = %v; want 2", got)
	}
}

func TestParallel_preservesACompiledGraphsCellOwnership(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("route", routingFactory(func(input int) string {
			if input > 0 {
				return "selected"
			}
			return "bypassed"
		})).
		MustRegisterSchema("route", routingSchema("selected", "bypassed")).
		MustRegisterNode("target", addN())
	graph, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{
			ID: "route", Type: "route",
			Inputs: workflow.OneInput(workflow.Output("start")),
		},
		{
			ID: "target", Type: "target",
			Inputs: workflow.OneInput(workflow.Output("start")),
			When:   []workflow.Gate{workflow.When("route", "selected")},
		},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}

	stale, err := graph.Run(t.Context(), workflow.NewStore().WithOutput("start", 1))
	if err != nil {
		t.Fatalf("first Graph Run: %v", err)
	}
	if _, present := stale.Lookup(workflow.Output("target")); !present {
		t.Fatal("first Graph Run did not create the selected target output")
	}

	identity := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store, nil
		},
	)
	output, err := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{graph, identity}}).
		Run(t.Context(), stale.WithOutput("start", 0))
	if err != nil {
		t.Fatalf("Parallel Run: %v", err)
	}
	if value, present := output.Lookup(workflow.Output("target")); present {
		t.Fatalf("bypassed target output = %v; want the Graph-owned stale cell removed", value)
	}
}

func TestParallel_graphOwnershipSurvivesPersistedSuspension(t *testing.T) {
	registry := workflow.NewRegistry().
		MustRegisterNode("route", workflow.InterruptFactory()).
		MustRegisterSchema("route", workflow.NodeSchema{
			Output:  workflow.TypeString,
			Outlets: []string{"selected", "bypassed"},
		}).
		MustRegisterNode("target", addN())
	graph, err := registry.CompileGraph(workflow.Graph{Nodes: []workflow.GraphNode{
		{ID: "route", Type: "route"},
		{
			ID: "target", Type: "target",
			Inputs: workflow.OneInput(workflow.Output("start")),
			When:   []workflow.Gate{workflow.When("route", "selected")},
		},
	}})
	if err != nil {
		t.Fatalf("CompileGraph: %v", err)
	}
	identity := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store, nil
		},
	)
	parallel := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{graph, identity}})

	journal := workflow.NewJournal()
	input := workflow.NewStore().
		WithOutput("start", 1).
		WithOutput("route", "selected").
		WithOutput("target", 99)

	paused, err := workflow.Run(
		t.Context(),
		parallel,
		input,
		workflow.RunConfig{Journal: journal},
	)
	waits := workflow.Suspensions(err)
	if len(waits) != 1 || waits[0].ID != "route" {
		t.Fatalf("first Run error = %v; want route suspension", err)
	}
	if _, present := paused.Lookup(workflow.Output("route")); present {
		t.Fatal("paused Store retained the stale routing output")
	}
	if _, present := paused.Lookup(workflow.Output("target")); present {
		t.Fatal("paused Store retained the stale target output")
	}

	storeCheckpoint, err := json.Marshal(paused)
	if err != nil {
		t.Fatalf("Marshal Store: %v", err)
	}
	journalCheckpoint, err := json.Marshal(journal)
	if err != nil {
		t.Fatalf("Marshal Journal: %v", err)
	}
	var restoredStore workflow.Store
	if decodeErr := json.Unmarshal(storeCheckpoint, &restoredStore); decodeErr != nil {
		t.Fatalf("Unmarshal Store: %v", decodeErr)
	}
	restoredJournal := workflow.NewJournal()
	if decodeErr := json.Unmarshal(journalCheckpoint, restoredJournal); decodeErr != nil {
		t.Fatalf("Unmarshal Journal: %v", decodeErr)
	}
	if recordErr := restoredJournal.Record(waits[0].Key(), "bypassed"); recordErr != nil {
		t.Fatalf("Record response: %v", recordErr)
	}

	resumed, err := workflow.Run(
		t.Context(),
		parallel,
		restoredStore,
		workflow.RunConfig{Journal: restoredJournal},
	)
	if err != nil {
		t.Fatalf("resumed Run: %v", err)
	}
	if value, getErr := resumed.Get[string](workflow.Output("route")); getErr != nil || value != "bypassed" {
		t.Fatalf("route output = %q, %v; want bypassed, nil", value, getErr)
	}
	if value, present := resumed.Lookup(workflow.Output("target")); present {
		t.Fatalf("resumed Store resurrected stale target output %v", value)
	}
}

func TestParallel_laterBranchWinsCellConflict(t *testing.T) {
	write := func(value int) workflow.Step {
		return flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
			return s.WithCell("shared", "value", value), nil
		})
	}

	out, err := workflow.Parallel(workflow.ParallelConfig{
		Steps: []workflow.Step{write(1), write(2)},
	}).Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Parallel: %v", err)
	}
	if got, _ := out.Lookup(workflow.At("shared", "value")); got != 2 {
		t.Fatalf("shared value = %v; want later branch value 2", got)
	}
}

func TestParallel_compactedBranchMergesOnlyWrites(t *testing.T) {
	writeShared := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		return store.WithOutput("shared", 1), nil
	})
	writeMany := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, store workflow.Store) (workflow.Store, error) {
		for i := range 40 {
			store = store.WithOutput(fmt.Sprintf("node-%d", i), i)
		}
		return store, nil
	})
	base := workflow.NewStore().WithOutput("shared", 0)

	out, err := workflow.Parallel(workflow.ParallelConfig{
		Steps: []workflow.Step{writeShared, writeMany},
	}).Run(t.Context(), base)
	if err != nil {
		t.Fatalf("Parallel: %v", err)
	}
	if got, _ := out.Lookup(workflow.Output("shared")); got != 1 {
		t.Fatalf("shared = %v; inherited base value from compacted branch won", got)
	}
	for i := range 40 {
		if got, _ := out.Lookup(workflow.Output(fmt.Sprintf("node-%d", i))); got != i {
			t.Fatalf("node-%d = %v; want %d", i, got, i)
		}
	}
}

func TestParallel_mergesUnrelatedStore(t *testing.T) {
	replace := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, _ workflow.Store) (workflow.Store, error) {
		return workflow.NewStore().WithOutput("other", 2), nil
	})
	base := workflow.NewStore().WithOutput("base", 1)

	out, err := workflow.Parallel(workflow.ParallelConfig{
		Steps: []workflow.Step{replace},
	}).Run(t.Context(), base)
	if err != nil {
		t.Fatalf("Parallel: %v", err)
	}
	if got, _ := out.Lookup(workflow.Output("base")); got != 1 {
		t.Fatalf("base = %v; want 1", got)
	}
	if got, _ := out.Lookup(workflow.Output("other")); got != 2 {
		t.Fatalf("other = %v; want 2", got)
	}
}

func TestParallel_compactsDeepBranchInput(t *testing.T) {
	base := workflow.NewStore()
	for index := range 64 {
		base = base.WithOutput(fmt.Sprintf("base-%d", index), index)
	}
	write := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store.WithOutput("written", true), nil
		},
	)
	pass := flow.NodeFunc[workflow.Store, workflow.Store](
		func(_ context.Context, store workflow.Store) (workflow.Store, error) {
			return store, nil
		},
	)
	output, err := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{write, pass}}).
		Run(t.Context(), base)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := output.Get[bool](workflow.Output("written")); err != nil || !got {
		t.Fatalf("written = %v, %v; want true", got, err)
	}
	if got, err := output.Get[int](workflow.Output("base-0")); err != nil || got != 0 {
		t.Fatalf("base-0 = %v, %v; want 0", got, err)
	}
}

func TestParallel_compactsLargeMergedResult(t *testing.T) {
	branches := make([]workflow.Step, 130)
	for index := range branches {
		branches[index] = flow.NodeFunc[workflow.Store, workflow.Store](
			func(_ context.Context, store workflow.Store) (workflow.Store, error) {
				return store.WithOutput(fmt.Sprintf("branch-%d", index), index), nil
			},
		)
	}
	output, err := workflow.Parallel(workflow.ParallelConfig{Steps: branches, Concurrency: 4}).
		Run(t.Context(), workflow.NewStore())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for index := range branches {
		if got, ok := output.Lookup(workflow.Output(fmt.Sprintf("branch-%d", index))); !ok || got != index {
			t.Fatalf("branch-%d = %v, %v; want %d", index, got, ok, index)
		}
	}
}

// gatedWriter is a caller-defined branch that writes a shared cell and can be
// held until another branch has finished, which is how a test chooses the
// completion order instead of observing whichever one the scheduler produced.
type gatedWriter struct {
	value  string
	wait   <-chan struct{}
	signal chan<- struct{}
}

func (g gatedWriter) Run(_ context.Context, store workflow.Store) (workflow.Store, error) {
	if g.wait != nil {
		<-g.wait
	}
	next := store.WithOutput("shared", g.value)
	if g.signal != nil {
		close(g.signal)
	}
	return next, nil
}

// TestParallel_mergesInDeclarationOrderNotCompletionOrder pins which order the
// conflict rule means, and therefore whether a Parallel is reproducible.
// TestParallel_laterBranchWinsCellConflict shows that a later branch wins, but
// its branches both finish at once, so a merge that followed completion order
// would pass it whenever the scheduler happened to finish them in declaration
// order. Here the first branch is held until the second has finished, which is
// the only arrangement that tells the two rules apart — and if the rule were
// temporal, one definition and one input would merge differently from run to
// run. Only a caller-defined branch can write another's cell, so that is what
// both tests use.
func TestParallel_mergesInDeclarationOrderNotCompletionOrder(t *testing.T) {
	secondDone := make(chan struct{})
	step := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{
		gatedWriter{value: "first", wait: secondDone},
		gatedWriter{value: "second", signal: secondDone},
	}})

	out, err := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got, getErr := out.Get[string](workflow.Output("shared"))
	if getErr != nil || got != "second" {
		t.Fatalf("shared = %q, %v; want the later-declared branch's value", got, getErr)
	}
}
