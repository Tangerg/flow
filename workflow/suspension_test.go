package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// counting returns a leaf that adds n to its input and counts how often it ran,
// so a test can tell replayed work from repeated work.
func counting(runs *atomic.Int64, id, from string, n int) workflow.Step {
	return workflow.Leaf(id, workflow.From[int](workflow.Output(from)),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
			runs.Add(1)
			return x + n, nil
		}))
}

func runJournal(step workflow.Step, in workflow.Store, journal *workflow.Journal) (workflow.Store, error) {
	return workflow.Run(context.Background(), step, in, workflow.RunConfig{Journal: journal})
}

func TestSuspend_sequenceResumesWithoutRepeatingWork(t *testing.T) {
	var aRuns, bRuns atomic.Int64
	pipeline := workflow.Sequence(
		counting(&aRuns, "a", "start", 1),
		workflow.Await("gate", workflow.Output("approval")),
		counting(&bRuns, "b", "a", 10),
	)

	journal := workflow.NewJournal()

	// First run stops at the gate.
	paused, err := runJournal(pipeline, workflow.NewStore().WithOutput("start", 1), journal)
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("first run err = %v; want ErrSuspended", err)
	}
	suspensions := workflow.Suspensions(err)
	if len(suspensions) != 1 || suspensions[0].ID != "gate" || suspensions[0].Await != workflow.Output("approval") {
		t.Fatalf("suspensions = %+v; want one gate awaiting approval.output", suspensions)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 0 {
		t.Fatalf("runs = a:%d b:%d; want a:1 b:0", aRuns.Load(), bRuns.Load())
	}

	// Second run supplies what the gate waited for.
	final, err := runJournal(pipeline, paused.WithOutput("approval", true), journal)
	if err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if got, err := workflow.Get[int](final, workflow.Output("b")); err != nil || got != 12 {
		t.Fatalf("b = %v, %v; want 12", got, err)
	}
	if aRuns.Load() != 1 {
		t.Fatalf("step a ran %d times across both runs; want 1", aRuns.Load())
	}
	if bRuns.Load() != 1 {
		t.Fatalf("step b ran %d times; want 1", bRuns.Load())
	}
}

func TestSuspend_structuredLeafValueCanBeResolved(t *testing.T) {
	type approvalRequest struct {
		Document string   `json:"document"`
		Actions  []string `json:"actions"`
	}
	approval := workflow.Leaf("approval", workflow.From[string](workflow.Output("document")),
		flow.NodeFunc[string, bool](func(_ context.Context, document string) (bool, error) {
			return false, workflow.Suspend(approvalRequest{
				Document: document,
				Actions:  []string{"approve", "reject"},
			})
		}))

	journal := workflow.NewJournal()
	in := workflow.NewStore().WithOutput("document", "draft-42")
	_, err := runJournal(approval, in, journal)
	waits := workflow.Suspensions(err)
	if len(waits) != 1 {
		t.Fatalf("Suspensions = %+v; want one", waits)
	}
	request, ok := waits[0].Value.(approvalRequest)
	if !ok || request.Document != "draft-42" ||
		!slices.Equal(request.Actions, []string{"approve", "reject"}) {
		t.Fatalf("Value = %#v; want structured request", waits[0].Value)
	}

	if err := journal.Record(waits[0].Key(), true); err != nil {
		t.Fatalf("Record: %v", err)
	}
	out, err := runJournal(approval, in, journal)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if approved, err := workflow.Get[bool](out, workflow.Output("approval")); err != nil || !approved {
		t.Fatalf("approval = %v, %v; want true", approved, err)
	}
}

func TestInterrupt_resumesAsARecordedStepOutput(t *testing.T) {
	type approvalRequest struct {
		Question string   `json:"question"`
		Actions  []string `json:"actions"`
	}

	var beforeRuns, afterRuns atomic.Int64
	before := counting(&beforeRuns, "before", "start", 1)
	approval := workflow.Interrupt("approval", approvalRequest{
		Question: "publish?",
		Actions:  []string{"approve", "reject"},
	})
	after := workflow.Leaf("after", workflow.From[bool](workflow.Output("approval")),
		flow.NodeFunc[bool, string](func(_ context.Context, approved bool) (string, error) {
			afterRuns.Add(1)
			return strconv.FormatBool(approved), nil
		}))
	pipeline := workflow.Sequence(before, approval, after)

	journal := workflow.NewJournal()
	paused, err := runJournal(pipeline, workflow.NewStore().WithOutput("start", 1), journal)
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("first run err = %v; want ErrSuspended", err)
	}
	waits := workflow.Suspensions(err)
	if len(waits) != 1 || waits[0].ID != "approval" {
		t.Fatalf("Suspensions = %+v; want approval", waits)
	}
	request, ok := waits[0].Value.(approvalRequest)
	if !ok || request.Question != "publish?" || !slices.Equal(request.Actions, []string{"approve", "reject"}) {
		t.Fatalf("Value = %#v; want structured approval request", waits[0].Value)
	}
	if got := waits[0].Key(); got.ID != "approval" || len(got.Path) != 0 {
		t.Fatalf("Key = %+v; want approval at root", got)
	}
	if _, ok := paused.Lookup(workflow.Output("approval")); ok {
		t.Fatal("an unresolved Interrupt wrote an output")
	}

	if err := journal.Record(waits[0].Key(), true); err != nil {
		t.Fatalf("Record response: %v", err)
	}
	if err := journal.Record(waits[0].Key(), false); !errors.Is(err, workflow.ErrJournalConflict) {
		t.Fatalf("duplicate Record error = %v; want ErrJournalConflict", err)
	}

	journalJSON, err := json.Marshal(journal)
	if err != nil {
		t.Fatalf("Marshal Journal: %v", err)
	}
	resumedJournal := workflow.NewJournal()
	if err := json.Unmarshal(journalJSON, resumedJournal); err != nil {
		t.Fatalf("Unmarshal Journal: %v", err)
	}
	out, err := runJournal(pipeline, paused, resumedJournal)
	if err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if approved, err := workflow.Get[bool](out, workflow.Output("approval")); err != nil || !approved {
		t.Fatalf("approval = %v, %v; want true", approved, err)
	}
	if result, err := workflow.Get[string](out, workflow.Output("after")); err != nil || result != "true" {
		t.Fatalf("after = %q, %v; want true", result, err)
	}
	if beforeRuns.Load() != 1 || afterRuns.Load() != 1 {
		t.Fatalf("runs = before:%d after:%d; want 1 and 1", beforeRuns.Load(), afterRuns.Load())
	}
}

func TestInterrupt_resolvesRepeatedScopesIndependently(t *testing.T) {
	step := workflow.Iteration(workflow.IterationConfig{
		ID:          "items",
		Input:       workflow.Output("start"),
		Body:        workflow.Interrupt("approval", "approve item?"),
		BodyOutput:  workflow.Output("approval"),
		Concurrency: 1,
	})
	journal := workflow.NewJournal()
	in := workflow.NewStore().WithOutput("start", []any{"a", "b", "c"})

	_, err := runJournal(step, in, journal)
	waits := workflow.Suspensions(err)
	if len(waits) != 3 {
		t.Fatalf("first Suspensions = %+v; want three", waits)
	}
	if !slices.Equal(waits[0].Path, []string{"items[0]"}) ||
		!slices.Equal(waits[1].Path, []string{"items[1]"}) ||
		!slices.Equal(waits[2].Path, []string{"items[2]"}) {
		t.Fatalf("paths = %v, %v, %v; want one per item", waits[0].Path, waits[1].Path, waits[2].Path)
	}

	if err := journal.Record(waits[1].Key(), false); err != nil {
		t.Fatalf("resolve item 1: %v", err)
	}
	_, err = runJournal(step, in, journal)
	remaining := workflow.Suspensions(err)
	if len(remaining) != 2 ||
		!slices.Equal(remaining[0].Path, []string{"items[0]"}) ||
		!slices.Equal(remaining[1].Path, []string{"items[2]"}) {
		t.Fatalf("remaining = %+v; want only items 0 and 2", remaining)
	}

	if err := journal.Record(remaining[0].Key(), true); err != nil {
		t.Fatalf("resolve item 0: %v", err)
	}
	if err := journal.Record(remaining[1].Key(), true); err != nil {
		t.Fatalf("resolve item 2: %v", err)
	}
	out, err := runJournal(step, in, journal)
	if err != nil {
		t.Fatalf("final run: %v", err)
	}
	got, err := workflow.Get[[]bool](out, workflow.Output("items"))
	if err != nil || !slices.Equal(got, []bool{true, false, true}) {
		t.Fatalf("items = %v, %v; want [true false true]", got, err)
	}
}

// The Store a suspended run hands back is not needed to resume: the Journal
// carries the completed work on its own, which is what makes a durable resume
// possible and what saves Parallel's finished branches.
func TestSuspend_journalAloneIsEnoughToResume(t *testing.T) {
	var aRuns, bRuns atomic.Int64
	pipeline := workflow.Sequence(
		counting(&aRuns, "a", "start", 1),
		workflow.Await("gate", workflow.Output("approval")),
		counting(&bRuns, "b", "a", 10),
	)

	journal := workflow.NewJournal()
	if _, err := runJournal(pipeline, workflow.NewStore().WithOutput("start", 1), journal); !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("first run err = %v; want ErrSuspended", err)
	}

	// Discard the returned Store entirely and rebuild the input from scratch.
	fresh := workflow.NewStore().WithOutput("start", 1).WithOutput("approval", true)
	final, err := runJournal(pipeline, fresh, journal)
	if err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if got, err := workflow.Get[int](final, workflow.Output("b")); err != nil || got != 12 {
		t.Fatalf("b = %v, %v; want 12", got, err)
	}
	if aRuns.Load() != 1 {
		t.Fatalf("step a ran %d times; want 1 — the Journal did not replay it", aRuns.Load())
	}
}

func TestSuspend_durableResumeAcrossSerialization(t *testing.T) {
	type report struct {
		Title string `json:"title"`
		Score int    `json:"score"`
	}

	var draftRuns, publishRuns atomic.Int64
	draft := workflow.Leaf("draft", workflow.From[int](workflow.Output("start")),
		flow.NodeFunc[int, report](func(_ context.Context, seed int) (report, error) {
			draftRuns.Add(1)
			return report{Title: "draft", Score: seed * 2}, nil
		}))
	// A typed read of a struct is exactly what a naive Store round trip breaks.
	publish := workflow.Leaf("publish", workflow.From[report](workflow.Output("draft")),
		flow.NodeFunc[report, string](func(_ context.Context, r report) (string, error) {
			publishRuns.Add(1)
			return r.Title + ":" + strconv.Itoa(r.Score), nil
		}))
	pipeline := workflow.Sequence(draft, workflow.Await("gate", workflow.Output("approval")), publish)

	// Run one: suspend, then persist both the Store and the Journal.
	first := workflow.NewJournal()
	paused, err := runJournal(pipeline, workflow.NewStore().WithOutput("start", 21), first)
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("first run err = %v; want ErrSuspended", err)
	}
	storeJSON, err := json.Marshal(paused)
	if err != nil {
		t.Fatalf("marshal store: %v", err)
	}
	journalJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal journal: %v", err)
	}

	// Run two: a different process would start from nothing but these bytes.
	var restoredStore workflow.Store
	if err := json.Unmarshal(storeJSON, &restoredStore); err != nil {
		t.Fatalf("unmarshal store: %v", err)
	}
	restoredJournal := workflow.NewJournal()
	if err := json.Unmarshal(journalJSON, restoredJournal); err != nil {
		t.Fatalf("unmarshal journal: %v", err)
	}

	final, err := runJournal(pipeline, restoredStore.WithOutput("approval", true), restoredJournal)
	if err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if got, err := workflow.Get[string](final, workflow.Output("publish")); err != nil || got != "draft:42" {
		t.Fatalf("publish = %q, %v; want draft:42", got, err)
	}
	if draftRuns.Load() != 1 {
		t.Fatalf("draft ran %d times across processes; want 1", draftRuns.Load())
	}
	if publishRuns.Load() != 1 {
		t.Fatalf("publish ran %d times; want 1", publishRuns.Load())
	}
}

func TestSuspend_parallelLetsSiblingsFinish(t *testing.T) {
	var slowRuns atomic.Int64
	slow := counting(&slowRuns, "slow", "start", 1)
	waiting := workflow.Await("waiting", workflow.Output("approval"))

	journal := workflow.NewJournal()
	p := workflow.Parallel([]workflow.Step{slow, waiting}, workflow.ParallelConfig{})

	out, err := runJournal(p, workflow.NewStore().WithOutput("start", 1), journal)
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
	// The sibling was neither cancelled nor discarded.
	if slowRuns.Load() != 1 {
		t.Fatalf("sibling ran %d times; want 1 — it was cancelled", slowRuns.Load())
	}
	if got, err := workflow.Get[int](out, workflow.Output("slow")); err != nil || got != 2 {
		t.Fatalf("completed branch missing from the merged Store: %v, %v", got, err)
	}

	// Resuming does not repeat the sibling's work.
	final, err := runJournal(p, out.WithOutput("approval", true), journal)
	if err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if slowRuns.Load() != 1 {
		t.Fatalf("sibling ran %d times in total; want 1", slowRuns.Load())
	}
	if _, err := workflow.Get[int](final, workflow.Output("slow")); err != nil {
		t.Fatalf("resumed Store lost the sibling's output: %v", err)
	}
}

func TestSuspend_parallelReportsEverySuspension(t *testing.T) {
	p := workflow.Parallel([]workflow.Step{
		workflow.Await("first", workflow.Output("a")),
		workflow.Await("second", workflow.Output("b")),
		workflow.Await("third", workflow.Output("c")),
	}, workflow.ParallelConfig{})

	_, err := p.Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
	suspensions := workflow.Suspensions(err)
	ids := make([]string, 0, len(suspensions))
	for _, s := range suspensions {
		ids = append(ids, s.ID)
	}
	if !slices.Equal(ids, []string{"first", "second", "third"}) {
		t.Fatalf("suspended IDs = %v; want all three", ids)
	}
	// The message has to name every wait, since a caller must satisfy all of them.
	for _, want := range []string{"a#/output", "b#/output", "c#/output"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %q; want it to mention %s", err, want)
		}
	}
}

func TestSuspend_nestedParallelPreservesSuspensionsAndCompletedWork(t *testing.T) {
	completed := workflow.Leaf("done",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) { return value, nil }),
	)
	inner := workflow.Parallel([]workflow.Step{
		completed,
		workflow.Await("a", workflow.Output("input-a")),
		workflow.Await("b", workflow.Output("input-b")),
	}, workflow.ParallelConfig{})
	outer := workflow.Parallel([]workflow.Step{
		inner,
		workflow.Await("c", workflow.Output("input-c")),
	}, workflow.ParallelConfig{})

	out, err := outer.Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
	suspensions := workflow.Suspensions(err)
	ids := make([]string, len(suspensions))
	for i, suspension := range suspensions {
		ids[i] = suspension.ID
	}
	if !slices.Equal(ids, []string{"a", "b", "c"}) {
		t.Fatalf("suspended IDs = %v; want every nested wait", ids)
	}
	if got, getErr := workflow.Get[int](out, workflow.Output("done")); getErr != nil || got != 1 {
		t.Fatalf("completed nested branch = %v, %v; want 1", got, getErr)
	}
}

func TestSuspend_iterationPreservesNestedSuspensions(t *testing.T) {
	body := workflow.Parallel([]workflow.Step{
		workflow.Await("a", workflow.Output("input-a")),
		workflow.Await("b", workflow.Output("input-b")),
	}, workflow.ParallelConfig{})
	iteration := workflow.Iteration(workflow.IterationConfig{
		ID:         "iter",
		Input:      workflow.Output("items"),
		Body:       body,
		BodyOutput: workflow.Output("unused"),
	})

	_, err := iteration.Run(context.Background(),
		workflow.NewStore().WithOutput("items", []any{1}))
	suspensions := workflow.Suspensions(err)
	if len(suspensions) != 2 ||
		suspensions[0].ID != "a" || suspensions[1].ID != "b" ||
		!slices.Equal(suspensions[0].Path, []string{"iter[0]"}) ||
		!slices.Equal(suspensions[1].Path, []string{"iter[0]"}) {
		t.Fatalf("suspensions = %+v; want a and b in iter[0]", suspensions)
	}
}

// A real failure must still fail fast. Suspension changed the "not yet" path, not
// the error path.
func TestSuspend_parallelStillFailsFastOnRealErrors(t *testing.T) {
	boom := errors.New("boom")
	bad := workflow.Leaf("bad", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 0, boom }))

	_, err := workflow.Parallel([]workflow.Step{
		workflow.Await("waiting", workflow.Output("approval")),
		bad,
	}, workflow.ParallelConfig{}).
		Run(context.Background(), workflow.NewStore())

	if !errors.Is(err, boom) {
		t.Fatalf("err = %v; want boom to dominate", err)
	}
	if errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; a failure must not be reported as a suspension", err)
	}
}

func TestSuspend_joinedFailureIsNotClassifiedAsPureSuspension(t *testing.T) {
	boom := errors.New("boom")
	mixed := flow.NodeFunc[workflow.Store, workflow.Store](func(_ context.Context, s workflow.Store) (workflow.Store, error) {
		return s, errors.Join(workflow.Suspend("waiting"), boom)
	})

	_, err := workflow.Parallel([]workflow.Step{mixed}, workflow.ParallelConfig{}).
		Run(context.Background(), workflow.NewStore())
	var indexErr *flow.IndexError
	if !errors.As(err, &indexErr) || indexErr.Index != 0 || !errors.Is(err, boom) {
		t.Fatalf("err = %v; want branch failure at index 0", err)
	}
	if suspensions := workflow.Suspensions(err); len(suspensions) != 1 {
		t.Fatalf("Suspensions = %v; joined wait detail should remain inspectable", suspensions)
	}
}

func TestSuspend_typedNilRemainsASuspension(t *testing.T) {
	var wait *workflow.Suspension
	node := flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
		return 0, wait
	})
	step := workflow.Leaf(
		"approval",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		node,
	)

	_, err := step.Run(context.Background(), workflow.NewStore())
	suspensions := workflow.Suspensions(err)
	if !errors.Is(err, workflow.ErrSuspended) || len(suspensions) != 1 ||
		suspensions[0].ID != "approval" {
		t.Fatalf("err = %v, suspensions = %+v; want identified suspension", err, suspensions)
	}
}

func TestSuspend_iterationResumesElementByElement(t *testing.T) {
	// Every element doubles its item, but element 1 waits for approval.
	var runs atomic.Int64
	body := workflow.Sequence(
		workflow.Leaf("double", workflow.From[int](workflow.Item("iter")),
			flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
				runs.Add(1)
				return x * 2, nil
			})),
		workflow.Leaf("check", workflow.BindFunc[int](func(s workflow.Store) (int, error) {
			index, err := workflow.Get[int](s, workflow.Index("iter"))
			if err != nil {
				return 0, err
			}
			if index == 1 {
				if _, ok := s.Lookup(workflow.Output("approval")); !ok {
					return 0, workflow.Suspend("element 1 needs approval")
				}
			}
			return workflow.Get[int](s, workflow.Output("double"))
		}), flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })),
	)
	step := workflow.Iteration(workflow.IterationConfig{
		ID: "iter", Input: workflow.Output("start"), Body: body,
		BodyOutput: workflow.Output("check"), Concurrency: 1,
	})

	journal := workflow.NewJournal()
	in := workflow.NewStore().WithOutput("start", []any{1, 2, 3})

	_, err := runJournal(step, in, journal)
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
	// Elements 0 and 2 finished and were not cancelled.
	if runs.Load() != 3 {
		t.Fatalf("body ran %d times; want 3 — a sibling element was cancelled", runs.Load())
	}
	// Each element's record is scoped, so they are distinct.
	if keys := journal.Keys(); !equalJournalKeys(keys, []workflow.JournalKey{
		{Path: []string{"iter[0]"}, ID: "check"},
		{Path: []string{"iter[0]"}, ID: "double"},
		{Path: []string{"iter[1]"}, ID: "double"},
		{Path: []string{"iter[2]"}, ID: "check"},
		{Path: []string{"iter[2]"}, ID: "double"},
	}) {
		t.Fatalf("journal keys = %v", keys)
	}

	final, err := runJournal(step, in.WithOutput("approval", true), journal)
	if err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	// Only element 1's remaining step re-ran; the doubling did not.
	if runs.Load() != 3 {
		t.Fatalf("body ran %d times in total; want 3", runs.Load())
	}
	got, err := workflow.Get[[]any](final, workflow.Output("iter"))
	if err != nil {
		t.Fatalf("collected: %v", err)
	}
	want := []any{2, 4, 6}
	for i := range want {
		if v, _ := workflow.Get[int](final, workflow.At("iter", "output", strconv.Itoa(i))); v != want[i] {
			t.Fatalf("collected = %v; want %v", got, want)
		}
	}
}

func TestSuspend_iterationWritesNoPartialCollection(t *testing.T) {
	body := workflow.Leaf("el", workflow.BindFunc[int](func(s workflow.Store) (int, error) {
		index, err := workflow.Get[int](s, workflow.Index("iter"))
		if err != nil {
			return 0, err
		}
		if index == 1 {
			return 0, workflow.Suspend("waiting")
		}
		return index, nil
	}), flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))

	step := workflow.Iteration(workflow.IterationConfig{
		ID: "iter", Input: workflow.Output("start"), Body: body,
		BodyOutput: workflow.Output("el"), Concurrency: 1,
	})

	out, err := step.Run(context.Background(), workflow.NewStore().WithOutput("start", []any{1, 2, 3}))
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
	// A slice with a hole in it would read as a finished result.
	if _, ok := out.Lookup(workflow.Output("iter")); ok {
		t.Fatal("a suspended iteration wrote a partial collection")
	}
}

func TestSuspend_loopResumesAtTheWaitingIteration(t *testing.T) {
	var runs atomic.Int64
	body := workflow.Leaf("tick", workflow.BindFunc[int](func(s workflow.Store) (int, error) {
		current, err := workflow.Get[int](s, workflow.Output("tick"))
		if err != nil {
			current = 0
		}
		if current == 2 {
			if _, ok := s.Lookup(workflow.Output("approval")); !ok {
				return 0, workflow.Suspend("pausing at 2")
			}
		}
		return current, nil
	}), flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
		runs.Add(1)
		return x + 1, nil
	}))
	done := func(_ context.Context, _ int, s workflow.Store) (bool, error) {
		v, err := workflow.Get[int](s, workflow.Output("tick"))
		return err == nil && v >= 4, nil
	}

	journal := workflow.NewJournal()
	loop := workflow.Loop("loop", body, done, workflow.LoopConfig{})

	if _, err := runJournal(loop, workflow.NewStore(), journal); !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
	firstPass := runs.Load()
	// Each completed iteration records both the body's output and the loop's own
	// stop decision, so a resumed loop cannot stop somewhere else.
	if keys := journal.Keys(); !equalJournalKeys(keys, []workflow.JournalKey{
		{Path: []string{"loop[0]"}, ID: "loop"},
		{Path: []string{"loop[0]"}, ID: "tick"},
		{Path: []string{"loop[1]"}, ID: "loop"},
		{Path: []string{"loop[1]"}, ID: "tick"},
	}) {
		t.Fatalf("journal keys = %v; want a body and a decision record per iteration", keys)
	}

	final, err := runJournal(loop, workflow.NewStore().WithOutput("approval", true), journal)
	if err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if got, err := workflow.Get[int](final, workflow.Output("tick")); err != nil || got != 4 {
		t.Fatalf("tick = %v, %v; want 4", got, err)
	}
	// The completed iterations replayed from the journal instead of re-running.
	if runs.Load() != firstPass+2 {
		t.Fatalf("body ran %d times in total, first pass %d; completed iterations re-ran",
			runs.Load(), firstPass)
	}
}

func TestSuspend_awaitPassesThroughOnceSatisfied(t *testing.T) {
	gate := workflow.Await("gate", workflow.At("inbox", "decision"))
	in := workflow.NewStore().With("inbox", "decision", "approve")

	out, err := gate.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Await: %v", err)
	}
	if got, err := workflow.Get[string](out, workflow.At("inbox", "decision")); err != nil || got != "approve" {
		t.Fatalf("Await altered the Store: %v, %v", got, err)
	}
	if description := workflow.Describe(gate); description.Kind != "await" || description.ID != "gate" {
		t.Fatalf("Describe = %+v", description)
	}
}

// An Await is never skipped by a Journal: it writes nothing, so there is nothing
// to replay, and skipping it would mean never waiting again.
func TestSuspend_awaitIsNotJournaled(t *testing.T) {
	gate := workflow.Await("gate", workflow.Output("approval"))
	journal := workflow.NewJournal()

	satisfied := workflow.NewStore().WithOutput("approval", true)
	if _, err := runJournal(gate, satisfied, journal); err != nil {
		t.Fatalf("Await: %v", err)
	}
	if journal.Len() != 0 {
		t.Fatalf("journal recorded %d steps; want none", journal.Len())
	}
	// Without the value it suspends again rather than replaying as done.
	if _, err := runJournal(gate, workflow.NewStore(), journal); !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
}

func TestSuspend_emptyStepIDIsAnError(t *testing.T) {
	_, err := workflow.Await("", workflow.Output("x")).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("err = %v; want ErrInvalidStepID", err)
	}
}

func TestAwait_rejectsAnInvalidReference(t *testing.T) {
	_, err := workflow.Await("approval", workflow.Ref{}).Run(context.Background(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrInvalidSpec) || errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrInvalidSpec only", err)
	}
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "approval" || stepErr.Op != workflow.OpValidate {
		t.Fatalf("err = %v; want approval validation StepError", err)
	}
}

func TestSuspension_errorMessage(t *testing.T) {
	tests := map[string]*workflow.Suspension{
		`workflow: step "a" suspended: waiting on a person`: {ID: "a", Value: "waiting on a person"},
		`workflow: step "a" suspended: awaiting x#/output`:  {ID: "a", Await: workflow.Output("x")},
		`workflow: step "a" in iter[2] suspended`:           {ID: "a", Path: []string{"iter[2]"}},
		`workflow: step "a" suspended`:                      {ID: "a", Value: map[string]any{"private": "payload"}},
		`workflow: suspended`:                               nil,
	}
	for want, suspension := range tests {
		if got := suspension.Error(); got != want {
			t.Fatalf("Error = %q; want %q", got, want)
		}
		if !errors.Is(suspension, workflow.ErrSuspended) {
			t.Fatalf("%v does not match ErrSuspended", suspension)
		}
	}
}

func TestSuspension_keyAndJSONOwnTheirStructure(t *testing.T) {
	suspension := &workflow.Suspension{
		ID:    `approve"item`,
		Path:  []string{"items/0"},
		Value: map[string]any{"question": "approve?", "actions": []string{"yes", "no"}},
	}
	key := suspension.Key()
	key.Path[0] = "changed"
	if suspension.Path[0] != "items/0" {
		t.Fatalf("Key leaked Suspension.Path: %+v", suspension)
	}
	if got := suspension.Error(); got != `workflow: step "approve\"item" in items/0 suspended` {
		t.Fatalf("Error = %q", got)
	}

	data, err := json.Marshal(suspension)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded["id"] != `approve"item` {
		t.Fatalf("JSON = %s; missing structured ID", data)
	}
	value, ok := decoded["value"].(map[string]any)
	if !ok || value["question"] != "approve?" {
		t.Fatalf("JSON value = %#v; want structured payload", decoded["value"])
	}
}

func TestSuspensions_ofOtherErrors(t *testing.T) {
	if got := workflow.Suspensions(nil); got != nil {
		t.Fatalf("Suspensions(nil) = %v; want nil", got)
	}
	if got := workflow.Suspensions(errors.New("boom")); got != nil {
		t.Fatalf("Suspensions(plain error) = %v; want nil", got)
	}
	// A caller may wrap the sentinel without the richer value.
	wrapped := fmt.Errorf("outer: %w", workflow.ErrSuspended)
	if got := workflow.Suspensions(wrapped); len(got) != 1 {
		t.Fatalf("Suspensions(wrapped sentinel) = %v; want one", got)
	}

	joined := errors.Join(
		&workflow.Suspension{ID: "b", Path: []string{"outer"}},
		fmt.Errorf("wrapped: %w", &workflow.Suspension{ID: "a", Path: []string{"inner"}}),
		errors.New("ordinary failure"),
	)
	got := workflow.Suspensions(joined)
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("Suspensions(joined error tree) = %+v; want a and b", got)
	}
	got[0].Path[0] = "changed"
	if again := workflow.Suspensions(joined); again[0].Path[0] != "inner" {
		t.Fatalf("Suspensions leaked its internal path: %+v", again)
	}
}

func TestSuspend_eventsReportTheThirdOutcome(t *testing.T) {
	pipeline := workflow.Sequence(
		workflow.Leaf("a", workflow.From[int](workflow.Output("start")),
			flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil })),
		workflow.Await("gate", workflow.Output("approval")),
	)

	var kinds []string
	journal := workflow.NewJournal()
	observe := workflow.ObserverFunc(func(_ context.Context, e workflow.Event) {
		kinds = append(kinds, string(e.Kind)+":"+e.ID)
	})

	cfg := workflow.RunConfig{Observer: observe, Journal: journal}
	in := workflow.NewStore().WithOutput("start", 1)
	if _, err := workflow.Run(context.Background(), pipeline, in, cfg); !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
	if want := []string{"started:a", "completed:a", "suspended:gate"}; !slices.Equal(kinds, want) {
		t.Fatalf("events = %v; want %v", kinds, want)
	}

	// On resume, the replayed step reports that it was skipped rather than started.
	kinds = nil
	if _, err := workflow.Run(context.Background(), pipeline, in.WithOutput("approval", true), cfg); err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if want := []string{"skipped:a", "completed:gate"}; !slices.Equal(kinds, want) {
		t.Fatalf("resumed events = %v; want %v", kinds, want)
	}
}

func TestInterrupt_eventsReportSuspensionThenReplay(t *testing.T) {
	journal := workflow.NewJournal()
	var events []workflow.Event
	cfg := workflow.RunConfig{
		Journal: journal,
		Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			events = append(events, event)
		}),
	}
	step := workflow.Interrupt("approval", map[string]any{"question": "approve?"})

	_, err := workflow.Run(context.Background(), step, workflow.NewStore(), cfg)
	waits := workflow.Suspensions(err)
	if len(events) != 1 || events[0].Kind != workflow.EventSuspended ||
		len(waits) != 1 || events[0].Err == nil {
		t.Fatalf("first events = %+v, waits = %+v", events, waits)
	}
	if err := journal.Record(waits[0].Key(), true); err != nil {
		t.Fatalf("Record: %v", err)
	}

	events = nil
	out, err := workflow.Run(context.Background(), step, workflow.NewStore(), cfg)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(events) != 1 || events[0].Kind != workflow.EventSkipped ||
		events[0].Err != nil {
		t.Fatalf("resumed events = %+v; want one skipped event", events)
	}
	if approved, err := workflow.Get[bool](out, workflow.Output("approval")); err != nil || !approved {
		t.Fatalf("approval = %v, %v; want true", approved, err)
	}
}

// A resolver need not be a pure function of the Store — a classifier may answer
// differently the second time. The decision is journaled so a resumed run cannot
// take the other branch and leave outputs from both in the Store.
func TestSuspend_branchDecisionIsJournaled(t *testing.T) {
	var calls atomic.Int64
	// Answers "first" once, then "second" forever.
	flaky := func(context.Context, workflow.Store) (string, error) {
		if calls.Add(1) == 1 {
			return "first", nil
		}
		return "second", nil
	}
	label := func(text string) workflow.Step {
		return workflow.Leaf("out", workflow.BindFunc[string](func(workflow.Store) (string, error) { return text, nil }),
			flow.NodeFunc[string, string](func(_ context.Context, x string) (string, error) { return x, nil }))
	}
	pipeline := workflow.Sequence(
		workflow.Branch("route", flaky, map[string]workflow.Step{
			"first":  label("first"),
			"second": label("second"),
		}),
		workflow.Await("gate", workflow.Output("approval")),
	)

	journal := workflow.NewJournal()

	paused, err := runJournal(pipeline, workflow.NewStore(), journal)
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("first run err = %v; want ErrSuspended", err)
	}
	if got, err := workflow.Get[string](paused, workflow.Output("out")); err != nil || got != "first" {
		t.Fatalf("first run took %q, %v; want first", got, err)
	}

	final, err := runJournal(pipeline, paused.WithOutput("approval", true), journal)
	if err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if got, err := workflow.Get[string](final, workflow.Output("out")); err != nil || got != "first" {
		t.Fatalf("resumed run took %q; want the journaled branch first", got)
	}
	// The resolver was not consulted again, which also saves the second call.
	if calls.Load() != 1 {
		t.Fatalf("resolver ran %d times; want 1", calls.Load())
	}
}

// The same guarantee for a loop's stop condition.
func TestSuspend_loopDecisionIsJournaled(t *testing.T) {
	var checks atomic.Int64
	body := workflow.Leaf("tick", workflow.BindFunc[int](func(s workflow.Store) (int, error) {
		v, err := workflow.Get[int](s, workflow.Output("tick"))
		if err != nil {
			return 0, nil
		}
		if v == 1 {
			if _, ok := s.Lookup(workflow.Output("approval")); !ok {
				return 0, workflow.Suspend("pausing")
			}
		}
		return v, nil
	}), flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil }))
	// Says "keep going" once, then "stop" — a condition that is not a function of
	// the Store at all.
	flaky := func(context.Context, int, workflow.Store) (bool, error) {
		return checks.Add(1) > 1, nil
	}

	journal := workflow.NewJournal()
	loop := workflow.Loop("loop", body, flaky, workflow.LoopConfig{})

	if _, err := runJournal(loop, workflow.NewStore(), journal); !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("first run err = %v; want ErrSuspended", err)
	}
	firstChecks := checks.Load()

	if _, err := runJournal(loop, workflow.NewStore().WithOutput("approval", true), journal); err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	// Iteration 0's recorded "keep going" was reused rather than re-asked, so the
	// loop did not stop early on the run that resumed.
	if replayed := checks.Load() - firstChecks; replayed > 1 {
		t.Fatalf("the condition was re-asked %d times for replayed iterations; want 0", replayed-1)
	}
}

func TestSuspend_journaledDecisionOfTheWrongTypeIsReported(t *testing.T) {
	journal := workflow.NewJournal()
	// A journal from a different definition could hold anything under these keys.
	// A loop records one decision per iteration, so its key carries the scope.
	if err := json.Unmarshal([]byte(`{"version":1,"records":[
		{"id":"route","value":"not-a-case"},
		{"path":["repeat[0]"],"id":"repeat","value":"not-a-bool"}
	]}`), journal); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// A recorded string that names no case is a plain no-case error.
	branch := workflow.Branch("route",
		func(context.Context, workflow.Store) (string, error) { return "a", nil },
		map[string]workflow.Step{"a": leafStep("a")})
	_, err := runJournal(branch, workflow.NewStore(), journal)
	if !errors.Is(err, flow.ErrNoCase) {
		t.Fatalf("branch err = %v; want ErrNoCase", err)
	}

	// A recorded value of the wrong type is reported rather than ignored.
	body := workflow.Leaf("b", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	loop := workflow.Loop("repeat", body,
		func(context.Context, int, workflow.Store) (bool, error) { return true, nil },
		workflow.LoopConfig{})
	_, err = runJournal(loop, workflow.NewStore(), journal)
	if !errors.Is(err, workflow.ErrTypeMismatch) {
		t.Fatalf("loop err = %v; want ErrTypeMismatch", err)
	}
}
