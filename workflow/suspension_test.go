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
	"time"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

// counting returns a leaf that adds n to its input and counts how often it ran,
// so a test can tell replayed work from repeated work.
func counting(runs *atomic.Int64, id, from string, n int) workflow.Step {
	return workflow.Leaf(id, workflow.Output(from).Bind[int](),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
			runs.Add(1)
			return x + n, nil
		}))
}

func runJournal(step workflow.Step, in workflow.Store, journal *workflow.Journal) (workflow.Store, error) {
	return workflow.Run(context.Background(), step, in, workflow.RunConfig{Journal: journal})
}

type suspendedByIdentity struct{}

func (suspendedByIdentity) Error() string { return "custom suspension" }

func (suspendedByIdentity) Is(target error) bool {
	return target == workflow.ErrSuspended
}

type errorChildren []error

func (e errorChildren) Error() string   { return "joined children" }
func (e errorChildren) Unwrap() []error { return e }

type markedSuspensionWrapper struct{ child error }

func (m markedSuspensionWrapper) Error() string { return "custom suspension wrapper" }

func (m markedSuspensionWrapper) Is(target error) bool {
	return target == workflow.ErrSuspended
}

func (m markedSuspensionWrapper) Unwrap() error { return m.child }

type emptySuspensionChildren struct{}

func (emptySuspensionChildren) Error() string { return "empty custom suspension children" }

func (emptySuspensionChildren) Is(target error) bool {
	return target == workflow.ErrSuspended
}

func (emptySuspensionChildren) Unwrap() []error { return []error{nil} }

type linearError struct{ child error }

func (linearError) Error() string   { return "linear wrapper" }
func (e linearError) Unwrap() error { return e.child }

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
	if got, err := final.Get[int](workflow.Output("b")); err != nil || got != 12 {
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
	approval := workflow.Leaf("approval", workflow.Output("document").Bind[string](),
		flow.NodeFunc[string, bool](func(_ context.Context, document string) (bool, error) {
			return false, workflow.Suspend(approvalRequest{
				Document: document,
				Actions:  []string{"approve", "reject"},
			})
		}))

	journal := workflow.NewJournal()
	in := workflow.NewStore().WithOutput("document", "draft-42")
	_, runErr := runJournal(approval, in, journal)
	waits := workflow.Suspensions(runErr)
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
	out, runErr := runJournal(approval, in, journal)
	if runErr != nil {
		t.Fatalf("resume: %v", runErr)
	}
	if approved, err := out.Get[bool](workflow.Output("approval")); err != nil || !approved {
		t.Fatalf("approval = %v, %v; want true", approved, err)
	}
}

func TestSuspend_leafObserverReceivesSuspension(t *testing.T) {
	step := workflow.Leaf(
		"wait",
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
			return 0, workflow.Suspend("not ready")
		}),
	)
	var event workflow.Event
	before := time.Now()
	_, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore(),
		workflow.RunConfig{Observer: workflow.ObserverFunc(
			func(_ context.Context, candidate workflow.Event) {
				if candidate.Kind == workflow.EventSuspended {
					event = candidate
				}
			},
		)},
	)
	within := time.Since(before)
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("Run error = %v; want ErrSuspended", err)
	}
	if event.Kind != workflow.EventSuspended || event.ID != "wait" ||
		!errors.Is(event.Err, workflow.ErrSuspended) {
		t.Fatalf("suspension event = %+v", event)
	}
	// A suspended attempt ran until it decided it could not finish, and that is
	// timed like a completion or a failure. Await and Interrupt are the boundaries
	// that report no duration, because they do no work of their own.
	if event.Elapsed <= 0 || event.Elapsed > within {
		t.Fatalf("suspended Elapsed = %v; want the attempt's own duration, at most %v", event.Elapsed, within)
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
	after := workflow.Leaf("after", workflow.Output("approval").Bind[bool](),
		flow.NodeFunc[bool, string](func(_ context.Context, approved bool) (string, error) {
			afterRuns.Add(1)
			return strconv.FormatBool(approved), nil
		}))
	pipeline := workflow.Sequence(before, approval, after)

	journal := workflow.NewJournal()
	paused, runErr := runJournal(pipeline, workflow.NewStore().WithOutput("start", 1), journal)
	if !errors.Is(runErr, workflow.ErrSuspended) {
		t.Fatalf("first run err = %v; want ErrSuspended", runErr)
	}
	waits := workflow.Suspensions(runErr)
	if len(waits) != 1 || waits[0].ID != "approval" {
		t.Fatalf("Suspensions = %+v; want approval", waits)
	}
	request, ok := waits[0].Value.(approvalRequest)
	if !ok || request.Question != "publish?" || !slices.Equal(request.Actions, []string{"approve", "reject"}) {
		t.Fatalf("Value = %#v; want structured approval request", waits[0].Value)
	}
	if got := waits[0].Key(); got.ID != "approval" || len(got.Scope) != 0 {
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

	journalJSON, runErr := json.Marshal(journal)
	if runErr != nil {
		t.Fatalf("Marshal Journal: %v", runErr)
	}
	resumedJournal := workflow.NewJournal()
	if err := json.Unmarshal(journalJSON, resumedJournal); err != nil {
		t.Fatalf("Unmarshal Journal: %v", err)
	}
	out, runErr := runJournal(pipeline, paused, resumedJournal)
	if runErr != nil {
		t.Fatalf("resumed run: %v", runErr)
	}
	if approved, err := out.Get[bool](workflow.Output("approval")); err != nil || !approved {
		t.Fatalf("approval = %v, %v; want true", approved, err)
	}
	if result, err := out.Get[string](workflow.Output("after")); err != nil || result != "true" {
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

	_, runErr := runJournal(step, in, journal)
	waits := workflow.Suspensions(runErr)
	if len(waits) != 3 {
		t.Fatalf("first Suspensions = %+v; want three", waits)
	}
	if !slices.Equal(waits[0].Scope, indexedScope("items", 0)) ||
		!slices.Equal(waits[1].Scope, indexedScope("items", 1)) ||
		!slices.Equal(waits[2].Scope, indexedScope("items", 2)) {
		t.Fatalf("scopes = %v, %v, %v; want one per item", waits[0].Scope, waits[1].Scope, waits[2].Scope)
	}

	if err := journal.Record(waits[1].Key(), false); err != nil {
		t.Fatalf("resolve item 1: %v", err)
	}
	_, runErr = runJournal(step, in, journal)
	remaining := workflow.Suspensions(runErr)
	if len(remaining) != 2 ||
		!slices.Equal(remaining[0].Scope, indexedScope("items", 0)) ||
		!slices.Equal(remaining[1].Scope, indexedScope("items", 2)) {
		t.Fatalf("remaining = %+v; want only items 0 and 2", remaining)
	}

	if err := journal.Record(remaining[0].Key(), true); err != nil {
		t.Fatalf("resolve item 0: %v", err)
	}
	if err := journal.Record(remaining[1].Key(), true); err != nil {
		t.Fatalf("resolve item 2: %v", err)
	}
	out, runErr := runJournal(step, in, journal)
	if runErr != nil {
		t.Fatalf("final run: %v", runErr)
	}
	got, runErr := out.Get[[]bool](workflow.Output("items"))
	if runErr != nil || !slices.Equal(got, []bool{true, false, true}) {
		t.Fatalf("items = %v, %v; want [true false true]", got, runErr)
	}
}

func TestAwaitAndInterrupt_rejectDuplicateOpaqueInvocation(t *testing.T) {
	await := workflow.Await("gate", workflow.Output("ready"))
	awaitTwice := flow.NodeFunc[workflow.Store, workflow.Store](
		func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
			next, err := await.Run(ctx, store)
			if err != nil {
				return next, err
			}
			return await.Run(ctx, next)
		},
	)
	_, runErr := workflow.Run(
		t.Context(),
		awaitTwice,
		workflow.NewStore().WithOutput("ready", true),
		workflow.RunConfig{},
	)
	if !errors.Is(runErr, workflow.ErrDuplicateStep) {
		t.Fatalf("duplicate Await error = %v; want ErrDuplicateStep", runErr)
	}

	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "question"}, "answer"); err != nil {
		t.Fatalf("Record: %v", err)
	}
	interrupt := workflow.Interrupt("question", "continue?")
	interruptTwice := flow.NodeFunc[workflow.Store, workflow.Store](
		func(ctx context.Context, store workflow.Store) (workflow.Store, error) {
			next, err := interrupt.Run(ctx, store)
			if err != nil {
				return next, err
			}
			return interrupt.Run(ctx, next)
		},
	)
	_, runErr = workflow.Run(
		t.Context(),
		interruptTwice,
		workflow.NewStore(),
		workflow.RunConfig{Journal: journal},
	)
	if !errors.Is(runErr, workflow.ErrDuplicateStep) {
		t.Fatalf("duplicate Interrupt error = %v; want ErrDuplicateStep", runErr)
	}
}

func TestInterrupt_rejectsEmptyID(t *testing.T) {
	if _, err := workflow.Interrupt("", "continue?").Run(
		t.Context(),
		workflow.NewStore(),
	); !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("error = %v; want ErrInvalidStepID", err)
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
	if got, err := final.Get[int](workflow.Output("b")); err != nil || got != 12 {
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
	draft := workflow.Leaf("draft", workflow.Output("start").Bind[int](),
		flow.NodeFunc[int, report](func(_ context.Context, seed int) (report, error) {
			draftRuns.Add(1)
			return report{Title: "draft", Score: seed * 2}, nil
		}))
	// A typed read of a struct is exactly what a naive Store round trip breaks.
	publish := workflow.Leaf("publish", workflow.Output("draft").Bind[report](),
		flow.NodeFunc[report, string](func(_ context.Context, r report) (string, error) {
			publishRuns.Add(1)
			return r.Title + ":" + strconv.Itoa(r.Score), nil
		}))
	pipeline := workflow.Sequence(draft, workflow.Await("gate", workflow.Output("approval")), publish)

	// Run one: suspend, then persist both the Store and the Journal.
	first := workflow.NewJournal()
	paused, runErr := runJournal(pipeline, workflow.NewStore().WithOutput("start", 21), first)
	if !errors.Is(runErr, workflow.ErrSuspended) {
		t.Fatalf("first run err = %v; want ErrSuspended", runErr)
	}
	storeJSON, runErr := json.Marshal(paused)
	if runErr != nil {
		t.Fatalf("marshal store: %v", runErr)
	}
	journalJSON, runErr := json.Marshal(first)
	if runErr != nil {
		t.Fatalf("marshal journal: %v", runErr)
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

	final, runErr := runJournal(pipeline, restoredStore.WithOutput("approval", true), restoredJournal)
	if runErr != nil {
		t.Fatalf("resumed run: %v", runErr)
	}
	if got, err := final.Get[string](workflow.Output("publish")); err != nil || got != "draft:42" {
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
	p := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{slow, waiting}})

	out, runErr := runJournal(p, workflow.NewStore().WithOutput("start", 1), journal)
	if !errors.Is(runErr, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", runErr)
	}
	// The sibling was neither cancelled nor discarded.
	if slowRuns.Load() != 1 {
		t.Fatalf("sibling ran %d times; want 1 — it was cancelled", slowRuns.Load())
	}
	if got, err := out.Get[int](workflow.Output("slow")); err != nil || got != 2 {
		t.Fatalf("completed branch missing from the merged Store: %v, %v", got, err)
	}

	// Resuming does not repeat the sibling's work.
	final, runErr := runJournal(p, out.WithOutput("approval", true), journal)
	if runErr != nil {
		t.Fatalf("resumed run: %v", runErr)
	}
	if slowRuns.Load() != 1 {
		t.Fatalf("sibling ran %d times in total; want 1", slowRuns.Load())
	}
	if _, err := final.Get[int](workflow.Output("slow")); err != nil {
		t.Fatalf("resumed Store lost the sibling's output: %v", err)
	}
}

func TestSuspend_parallelReportsEverySuspension(t *testing.T) {
	p := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{
		workflow.Await("first", workflow.Output("a")),
		workflow.Await("second", workflow.Output("b")),
		workflow.Await("third", workflow.Output("c")),
	}})

	_, err := p.Run(t.Context(), workflow.NewStore())
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
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) { return value, nil }),
	)
	inner := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{
		completed,
		workflow.Await("a", workflow.Output("input-a")),
		workflow.Await("b", workflow.Output("input-b")),
	}})

	outer := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{
		inner,
		workflow.Await("c", workflow.Output("input-c")),
	}})

	out, err := outer.Run(t.Context(), workflow.NewStore())
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
	if got, getErr := out.Get[int](workflow.Output("done")); getErr != nil || got != 1 {
		t.Fatalf("completed nested branch = %v, %v; want 1", got, getErr)
	}
}

func TestSuspend_iterationPreservesNestedSuspensions(t *testing.T) {
	body := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{
		workflow.Await("a", workflow.Output("input-a")),
		workflow.Await("b", workflow.Output("input-b")),
	}})

	iteration := workflow.Iteration(workflow.IterationConfig{
		ID:         "iter",
		Input:      workflow.Output("items"),
		Body:       body,
		BodyOutput: workflow.Item("iter"),
	})

	_, err := iteration.Run(t.Context(),
		workflow.NewStore().WithOutput("items", []any{1}))
	suspensions := workflow.Suspensions(err)
	if len(suspensions) != 2 ||
		suspensions[0].ID != "a" || suspensions[1].ID != "b" ||
		!slices.Equal(suspensions[0].Scope, indexedScope("iter", 0)) ||
		!slices.Equal(suspensions[1].Scope, indexedScope("iter", 0)) {
		t.Fatalf("suspensions = %+v; want a and b in iter[0]", suspensions)
	}
}

// A real failure must still fail fast. Suspension changed the "not yet" path, not
// the error path.
func TestSuspend_parallelStillFailsFastOnRealErrors(t *testing.T) {
	boom := errors.New("boom")
	bad := workflow.Leaf("bad", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 0, boom }))

	_, err := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{
		workflow.Await("waiting", workflow.Output("approval")),
		bad,
	}}).
		Run(t.Context(), workflow.NewStore())

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

	_, err := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{mixed}}).
		Run(t.Context(), workflow.NewStore())
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
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		node,
	)

	_, err := step.Run(t.Context(), workflow.NewStore())
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
		workflow.Leaf("double", workflow.Item("iter").Bind[int](),
			flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) {
				runs.Add(1)
				return x * 2, nil
			})),
		workflow.Leaf("check", workflow.BinderFunc[int](func(s workflow.Store) (int, error) {
			index, err := s.Get[int](workflow.ItemIndex("iter"))
			if err != nil {
				return 0, err
			}
			if index == 1 {
				if _, ok := s.Lookup(workflow.Output("approval")); !ok {
					return 0, workflow.Suspend("element 1 needs approval")
				}
			}
			return s.Get[int](workflow.Output("double"))
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
		{Scope: indexedScope("iter", 0), ID: "check"},
		{Scope: indexedScope("iter", 0), ID: "double"},
		{Scope: indexedScope("iter", 1), ID: "double"},
		{Scope: indexedScope("iter", 2), ID: "check"},
		{Scope: indexedScope("iter", 2), ID: "double"},
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
	got, err := final.Get[[]any](workflow.Output("iter"))
	if err != nil {
		t.Fatalf("collected: %v", err)
	}
	want := []any{2, 4, 6}
	for i := range want {
		if v, _ := final.Get[int](workflow.At("iter", "output", strconv.Itoa(i))); v != want[i] {
			t.Fatalf("collected = %v; want %v", got, want)
		}
	}
}

func TestSuspend_iterationWritesNoPartialCollection(t *testing.T) {
	body := workflow.Leaf("el", workflow.BinderFunc[int](func(s workflow.Store) (int, error) {
		index, err := s.Get[int](workflow.ItemIndex("iter"))
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

	out, err := step.Run(t.Context(), workflow.NewStore().WithOutput("start", []any{1, 2, 3}))
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
	body := workflow.Leaf("tick", workflow.BinderFunc[int](func(s workflow.Store) (int, error) {
		current, err := s.Get[int](workflow.Output("tick"))
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
	done := flow.NodeFunc[workflow.Store, bool](func(_ context.Context, s workflow.Store) (bool, error) {
		v, err := s.Get[int](workflow.Output("tick"))
		return err == nil && v >= 4, nil
	})

	journal := workflow.NewJournal()
	loop := workflow.Loop(workflow.LoopConfig{ID: "loop", Body: body, Condition: done})

	if _, err := runJournal(loop, workflow.NewStore(), journal); !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
	firstPass := runs.Load()
	// Each completed iteration records both the body's output and the loop's own
	// stop decision, so a resumed loop cannot stop somewhere else.
	if keys := journal.Keys(); !equalJournalKeys(keys, []workflow.JournalKey{
		{Scope: indexedScope("loop", 0), ID: "loop"},
		{Scope: indexedScope("loop", 0), ID: "tick"},
		{Scope: indexedScope("loop", 1), ID: "loop"},
		{Scope: indexedScope("loop", 1), ID: "tick"},
	}) {
		t.Fatalf("journal keys = %v; want a body and a decision record per iteration", keys)
	}

	final, err := runJournal(loop, workflow.NewStore().WithOutput("approval", true), journal)
	if err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if got, err := final.Get[int](workflow.Output("tick")); err != nil || got != 4 {
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
	in := workflow.NewStore().WithCell("inbox", "decision", "approve")

	out, err := gate.Run(t.Context(), in)
	if err != nil {
		t.Fatalf("Await: %v", err)
	}
	if got, err := out.Get[string](workflow.At("inbox", "decision")); err != nil || got != "approve" {
		t.Fatalf("Await altered the Store: %v, %v", got, err)
	}
	if description := workflow.Describe(gate); description.Kind != workflow.KindAwait || description.ID != "gate" {
		t.Fatalf("Describe = %+v", description)
	}
}

// TestAwait_reportsTheWaitItRaisedAndTheStoreItLetThrough pins everything an
// Await tells anyone, which is all there is: it records nothing, so the wait it
// raises and the one event it emits are its whole account of a run. The wait has
// to name the step and the reference it is waiting on, the suspended event has to
// carry that same wait, and the completed event has to carry the Store the step
// let through -- for a step that writes nothing, an event with an empty Store
// would report a completion an observer cannot see the effect of.
func TestAwait_reportsTheWaitItRaisedAndTheStoreItLetThrough(t *testing.T) {
	gate := workflow.Await("gate", workflow.Output("approval"))
	var events []workflow.Event
	cfg := workflow.RunConfig{Observer: record(&events)}

	_, err := workflow.Run(t.Context(), gate, workflow.NewStore(), cfg)
	waits := workflow.Suspensions(err)
	if len(waits) != 1 || waits[0].ID != "gate" || waits[0].Await != workflow.Output("approval") {
		t.Fatalf("suspensions = %+v; want one wait named gate on the approval output", waits)
	}
	raised := workflow.Suspensions(events[0].Err)
	if len(events) != 1 || events[0].Kind != workflow.EventSuspended ||
		len(raised) != 1 || raised[0].ID != waits[0].ID || raised[0].Await != waits[0].Await {
		t.Fatalf("events = %+v; want one suspended event carrying that same wait", events)
	}

	events = nil
	if _, err := workflow.Run(
		t.Context(),
		gate,
		workflow.NewStore().WithOutput("approval", true),
		cfg,
	); err != nil {
		t.Fatalf("satisfied Await: %v", err)
	}
	if len(events) != 1 || events[0].Kind != workflow.EventCompleted || events[0].ID != "gate" {
		t.Fatalf("events = %+v; want one completed event named gate", events)
	}
	approved, getErr := events[0].Store.Get[bool](workflow.Output("approval"))
	if getErr != nil || !approved {
		t.Fatalf("completed event Store = %v, %v; want the Store the wait let through", approved, getErr)
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
	_, err := workflow.Await("", workflow.Output("x")).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("err = %v; want ErrInvalidStepID", err)
	}
}

func TestAwait_rejectsAnInvalidReference(t *testing.T) {
	_, err := workflow.Await("approval", workflow.Ref{}).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, flow.ErrInvalidConfig) || errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrInvalidConfig only", err)
	}
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "approval" || stepErr.Op != workflow.OpValidate {
		t.Fatalf("err = %v; want approval validation StepError", err)
	}
}

func TestAwait_reportsAnUnresolvableNestedValueAsFailure(t *testing.T) {
	var events []workflow.Event
	ref := workflow.Output("input").Child("ready")
	_, err := workflow.Run(
		t.Context(),
		workflow.Await("approval", ref),
		workflow.NewStore().WithOutput("input", brokenJSON{}),
		workflow.RunConfig{Observer: workflow.ObserverFunc(
			func(_ context.Context, event workflow.Event) {
				events = append(events, event)
			},
		)},
	)
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.Op != workflow.OpBind ||
		!errors.Is(err, workflow.ErrTypeMismatch) ||
		!errors.Is(err, errBrokenJSON) ||
		errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("Run error = %v; want bind-time JSON resolution failure", err)
	}
	// An Observer attributes a failure by ID, and this is the only event an
	// Await's bind failure publishes, so the identity is the half of it that
	// nothing else can supply.
	if len(events) != 1 || events[0].Kind != workflow.EventFailed ||
		events[0].ID != "approval" ||
		!errors.Is(events[0].Err, errBrokenJSON) {
		t.Fatalf("events = %+v; want one failed event naming approval with the cause", events)
	}
}

func TestSuspension_errorMessage(t *testing.T) {
	type reason string
	tests := map[string]*workflow.Suspension{
		`workflow: step "a" suspended: waiting on a person`: {ID: "a", Value: "waiting on a person"},
		`workflow: step "a" suspended: typed reason`:        {ID: "a", Value: reason("typed reason")},
		`workflow: step "a" suspended: awaiting x#/output`:  {ID: "a", Await: workflow.Output("x")},
		`workflow: step "a" in iter[2] suspended`:           {ID: "a", Scope: indexedScope("iter", 2)},
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

// TestSuspension_fanOutMessageNamesEveryWaitOnce pins the envelope a fan-out
// reports. Suspensions() gives a caller the structured waits, but the error's own
// text is what reaches a log, and nothing had asked for it -- so an empty reason
// joined in front of the real ones would read as a wait that gave none.
func TestSuspension_fanOutMessageNamesEveryWaitOnce(t *testing.T) {
	wait := func(id string) workflow.Step {
		return workflow.Leaf(
			id,
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				return 0, workflow.Suspend("waiting on " + id)
			}),
		)
	}

	_, err := workflow.Run(
		t.Context(),
		workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{wait("a"), wait("b")}}),
		workflow.NewStore(),
		workflow.RunConfig{},
	)
	want := `2 steps suspended: workflow: step "a" suspended: waiting on a; ` +
		`workflow: step "b" suspended: waiting on b`
	if err == nil || err.Error() != want {
		t.Fatalf("Run error = %v; want %q", err, want)
	}
}

func TestSuspension_keyAndJSONOwnTheirStructure(t *testing.T) {
	suspension := &workflow.Suspension{
		ID:    `approve"item`,
		Scope: ordinaryScope("items/0"),
		Value: map[string]any{"question": "approve?", "actions": []string{"yes", "no"}},
	}
	key := suspension.Key()
	key.Scope[0].ID = "changed"
	if suspension.Scope[0].ID != "items/0" {
		t.Fatalf("Key leaked Suspension.Scope: %+v", suspension)
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

func TestSuspension_JSONBoundaryIsStrictAndAtomic(t *testing.T) {
	invalid := map[string][]byte{
		"unknown field":       []byte(`{"id":"wait","unknown":true}`),
		"duplicate identity":  []byte(`{"id":"first","id":"second"}`),
		"invalid UTF-8":       {'{', '"', 'i', 'd', '"', ':', '"', 0xff, '"', '}'},
		"unpaired surrogate":  []byte(`{"id":"\ud800"}`),
		"scope without ID":    []byte(`{"scope":[{"id":"loop"}]}`),
		"invalid scope":       []byte(`{"id":"wait","scope":[{"id":"loop","index":-1}]}`),
		"invalid await":       []byte(`{"id":"wait","await":{"nodeID":"source","path":"output"}}`),
		"unknown scope field": []byte(`{"id":"wait","scope":[{"id":"loop","extra":1}]}`),
		// A persisted wait must not depend on encoding/json's case folding. Each
		// alternate spelling below would otherwise satisfy a real field, and the
		// colliding pairs would let member order decide which value survives.
		"folded identity":       []byte(`{"ID":"wait"}`),
		"folded scope":          []byte(`{"id":"wait","SCOPE":[{"id":"loop"}]}`),
		"folded await":          []byte(`{"id":"wait","AWAIT":{"nodeID":"s","path":"/output"}}`),
		"folded value":          []byte(`{"id":"wait","VALUE":1}`),
		"colliding identity":    []byte(`{"id":"first","ID":"second"}`),
		"colliding value":       []byte(`{"id":"wait","value":1,"VALUE":2}`),
		"folded await nodeID":   []byte(`{"id":"wait","await":{"NODEID":"s","path":"/output"}}`),
		"folded await path":     []byte(`{"id":"wait","await":{"nodeID":"s","PATH":"/output"}}`),
		"unknown await field":   []byte(`{"id":"wait","await":{"nodeID":"s","path":"/output","x":1}}`),
		"await without nodeID":  []byte(`{"id":"wait","await":{"path":"/output"}}`),
		"await without path":    []byte(`{"id":"wait","await":{"nodeID":"s"}}`),
		"await nodeID not text": []byte(`{"id":"wait","await":{"nodeID":1,"path":"/output"}}`),
		"await path not text":   []byte(`{"id":"wait","await":{"nodeID":"s","path":1}}`),
		"await not an object":   []byte(`{"id":"wait","await":[]}`),
	}
	for name, data := range invalid {
		t.Run(name, func(t *testing.T) {
			target := workflow.Suspension{
				ID:    "kept",
				Scope: ordinaryScope("outer"),
				Value: "kept value",
			}
			if err := json.Unmarshal(data, &target); err == nil {
				t.Fatal("Unmarshal unexpectedly succeeded")
			}
			if target.ID != "kept" || !slices.Equal(target.Scope, ordinaryScope("outer")) ||
				target.Value != "kept value" {
				t.Fatalf("failed Unmarshal changed receiver: %#v", target)
			}
		})
	}
	for name, data := range map[string][]byte{
		"null":  []byte(`null`),
		"array": []byte(`[]`),
	} {
		t.Run(name, func(t *testing.T) {
			if err := json.Unmarshal(data, new(workflow.Suspension)); err == nil {
				t.Fatal("Unmarshal accepted a non-object suspension")
			}
		})
	}
	var nilSuspension *workflow.Suspension
	if err := nilSuspension.UnmarshalJSON([]byte(`{}`)); err == nil {
		t.Fatal("nil receiver UnmarshalJSON unexpectedly succeeded")
	}

	tooDeep := []byte(`{"id":"wait","value":` +
		strings.Repeat(`[`, workflow.MaxNestingDepth) + `0` +
		strings.Repeat(`]`, workflow.MaxNestingDepth) + `}`)
	if err := json.Unmarshal(tooDeep, new(workflow.Suspension)); !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("depth error = %v; want ErrMaxDepth", err)
	}

	// A scope that long is breadth, not nesting: the document is three levels
	// deep whatever its length, so the decoder's own limit never sees it and the
	// only thing standing between a persisted wait and an unrecordable identity
	// is the scope check. Every other invalid scope here is refused one layer
	// lower, by the frame's own decoder, which left that check unasked.
	frames := strings.Repeat(`{"id":"f"},`, workflow.MaxNestingDepth+1)
	tooMany := []byte(`{"id":"wait","scope":[` + strings.TrimSuffix(frames, ",") + `]}`)
	if err := json.Unmarshal(tooMany, new(workflow.Suspension)); !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("scope length error = %v; want ErrMaxDepth", err)
	}
}

// Suspend returns an anonymous wait: no ID, and therefore no scope, since a
// scope requires an identified step. Every other test crosses JSON with an
// identified one, so the rule that rejects a scope without an ID was free to
// reject the absence of both.
func TestSuspension_anonymousWaitCrossesJSON(t *testing.T) {
	data, err := json.Marshal(workflow.Suspension{Value: "pending"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var restored workflow.Suspension
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if restored.ID != "" || len(restored.Scope) != 0 || restored.Value != "pending" {
		t.Fatalf("restored = %#v; want an anonymous wait still carrying its value", restored)
	}
}

func TestSuspension_JSONEncodingPreservesIdentityAndReadableStructure(t *testing.T) {
	bad := string([]byte{0xff})
	for name, suspension := range map[string]workflow.Suspension{
		"ID":               {ID: bad},
		"scope without ID": {Scope: ordinaryScope("outer")},
		"scope":            {ID: "wait", Scope: ordinaryScope(bad)},
		"await":            {ID: "wait", Await: workflow.Ref{NodeID: bad, Path: "/output"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := json.Marshal(suspension); err == nil {
				t.Fatal("Marshal accepted invalid identity text")
			}
		})
	}
	for name, value := range map[string]any{
		"duplicate member":   duplicateObjectJSON{},
		"unpaired surrogate": unpairedSurrogateJSON{},
		"excessive depth":    nestedArrays(workflow.MaxNestingDepth),
		"encoding failure":   brokenJSON{},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := json.Marshal(workflow.Suspension{ID: "wait", Value: value}); err == nil {
				t.Fatal("Marshal produced a document the strict decoder cannot read")
			}
		})
	}

	data, err := json.Marshal(workflow.Suspension{
		ID:    "wait",
		Scope: indexedScope("items", 2),
		Await: workflow.Output("source"),
		Value: map[string]any{"large": json.Number("9007199254740993")},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var restored workflow.Suspension
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	value, ok := restored.Value.(map[string]any)
	if !ok || value["large"] != json.Number("9007199254740993") {
		t.Fatalf("Value = %#v; want exact json.Number", restored.Value)
	}
}

func TestSuspensions_ofOtherErrors(t *testing.T) {
	if got := workflow.Suspensions(nil); got != nil {
		t.Fatalf("Suspensions(nil) = %v; want nil", got)
	}
	if got := workflow.Suspensions(errors.New("boom")); got != nil {
		t.Fatalf("Suspensions(plain error) = %v; want nil", got)
	}
	// A caller may wrap the sentinel without the richer value. The wrapper's own
	// text is then all the reason there is, so the wait carries it as its value --
	// otherwise wrapping the sentinel to say what is being waited for would report
	// a wait that gives no reason at all.
	wrapped := fmt.Errorf("outer: %w", workflow.ErrSuspended)
	if got := workflow.Suspensions(wrapped); len(got) != 1 ||
		got[0].Value != wrapped.Error() {
		t.Fatalf("Suspensions(wrapped sentinel) = %+v; want one carrying %q", got, wrapped)
	}

	joined := errors.Join(
		&workflow.Suspension{ID: "b", Scope: ordinaryScope("outer")},
		fmt.Errorf("wrapped: %w", &workflow.Suspension{ID: "a", Scope: ordinaryScope("inner")}),
		errors.New("ordinary failure"),
	)
	got := workflow.Suspensions(joined)
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("Suspensions(joined error tree) = %+v; want a and b", got)
	}
	got[0].Scope[0].ID = "changed"
	if again := workflow.Suspensions(joined); again[0].Scope[0].ID != "inner" {
		t.Fatalf("Suspensions leaked its internal scope: %+v", again)
	}
}

func TestSuspensions_supportsSentinelCustomIdentityAndNilChildren(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		value any
	}{
		{name: "sentinel", err: workflow.ErrSuspended},
		{name: "custom identity", err: suspendedByIdentity{}, value: "custom suspension"},
		{
			name:  "custom identity with nil child",
			err:   markedSuspensionWrapper{},
			value: "custom suspension wrapper",
		},
		{
			name:  "custom identity with nil children",
			err:   emptySuspensionChildren{},
			value: "empty custom suspension children",
		},
		{
			name: "nil joined child",
			err: errorChildren{
				nil,
				&workflow.Suspension{ID: "wait", Value: "ready"},
			},
			value: "ready",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			waits := workflow.Suspensions(test.err)
			if len(waits) != 1 || waits[0].Value != test.value {
				t.Fatalf("Suspensions = %+v; want one with value %#v", waits, test.value)
			}
			if !workflow.SuspendedOnly(test.err) {
				t.Fatalf("SuspendedOnly(%v) = false", test.err)
			}
		})
	}
}

func TestSuspensions_walksDeepLinearWrappingWithoutRecursiveStackGrowth(t *testing.T) {
	const depth = 100_000

	var err error = &workflow.Suspension{ID: "wait", Value: "ready"}
	for range depth {
		err = linearError{child: err}
	}

	waits := workflow.Suspensions(err)
	if len(waits) != 1 || waits[0].ID != "wait" || waits[0].Value != "ready" {
		t.Fatalf("Suspensions = %+v; want the suspension below %d wrappers", waits, depth)
	}
	if !workflow.SuspendedOnly(err) {
		t.Fatalf("SuspendedOnly returned false below %d wrappers", depth)
	}
}

// Unwrap() []error is as much a standard error shape as linear wrapping. A
// caller can produce a deep join tree without crossing any workflow nesting
// boundary, so classifying it must not spend one call frame per branch level.
func TestSuspensions_walksDeepBranchedWrappingWithoutRecursiveStackGrowth(t *testing.T) {
	withBoundedStack(t, func() {
		const depth = 20_000
		var err error = &workflow.Suspension{ID: "wait", Value: "ready"}
		for range depth {
			err = errorChildren{err}
		}

		waits := workflow.Suspensions(err)
		if len(waits) != 1 || waits[0].ID != "wait" || waits[0].Value != "ready" {
			t.Fatalf("Suspensions = %+v; want the suspension below %d branches", waits, depth)
		}
		if !workflow.SuspendedOnly(err) {
			t.Fatalf("SuspendedOnly returned false below %d branches", depth)
		}
	})
}

func TestSuspendedOnly_classifiesTheWholeErrorTree(t *testing.T) {
	first := &workflow.Suspension{ID: "a", Value: "first"}
	second := &workflow.Suspension{ID: "b", Value: "second"}

	for name, test := range map[string]struct {
		err  error
		want bool
	}{
		"nil":        {err: nil, want: false},
		"failure":    {err: errors.New("boom"), want: false},
		"suspension": {err: fmt.Errorf("wrapped: %w", first), want: true},
		"only joined suspensions": {
			err:  errors.Join(first, fmt.Errorf("nested: %w", second)),
			want: true,
		},
		"mixed join": {
			err:  errors.Join(first, errors.New("boom")),
			want: false,
		},
		"custom marker with failure child": {
			err:  markedSuspensionWrapper{child: errors.New("boom")},
			want: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := workflow.SuspendedOnly(test.err); got != test.want {
				t.Fatalf("SuspendedOnly(%v) = %v; want %v", test.err, got, test.want)
			}
		})
	}
}

func TestValidationErrorsCannotBecomeSuspensions(t *testing.T) {
	assertInvalid := func(t *testing.T, err error, calls int) {
		t.Helper()
		if !errors.Is(err, flow.ErrInvalidConfig) ||
			errors.Is(err, workflow.ErrSuspended) ||
			workflow.SuspendedOnly(err) ||
			len(workflow.Suspensions(err)) != 0 {
			t.Fatalf("error = %v; want a non-suspending invalid-config error", err)
		}
		if calls != 0 {
			t.Fatalf("invalid node ran %d times; want 0", calls)
		}
	}

	for name, run := range map[string]func(workflow.Step) error{
		"top-level caller-defined step": func(step workflow.Step) error {
			_, err := workflow.Run(t.Context(), step, workflow.NewStore(), workflow.RunConfig{})
			return err
		},
		"nested caller-defined step": func(step workflow.Step) error {
			_, err := workflow.Parallel(workflow.ParallelConfig{Steps: []workflow.Step{step}}).
				Run(t.Context(), workflow.NewStore())
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			err := run(invalidValidatingStep{
				err:   workflow.Suspend("validation cannot wait"),
				calls: &calls,
			})
			assertInvalid(t, err, calls)
		})
	}

	t.Run("wrapper identity", func(t *testing.T) {
		calls := 0
		_, err := workflow.Run(
			t.Context(),
			invalidValidatingStep{
				err: markedSuspensionWrapper{
					child: errors.New("ordinary child does not erase wrapper identity"),
				},
				calls: &calls,
			},
			workflow.NewStore(),
			workflow.RunConfig{},
		)
		assertInvalid(t, err, calls)
	})

	t.Run("leaf binder", func(t *testing.T) {
		calls := 0
		step := workflow.Leaf(
			"leaf",
			invalidBinder{
				err:   workflow.Suspend("binder validation cannot wait"),
				calls: &calls,
			},
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				calls++
				return 0, nil
			}),
		)
		_, err := step.Run(t.Context(), workflow.NewStore())
		assertInvalid(t, err, calls)
		var stepErr *workflow.StepError
		if !errors.As(err, &stepErr) || stepErr.ID != "leaf" || stepErr.Op != workflow.OpValidate {
			t.Fatalf("error = %v; want leaf validation StepError", err)
		}
		if count := strings.Count(err.Error(), "workflow:"); count != 1 {
			t.Fatalf("error names workflow %d times: %v", count, err)
		}
	})

	t.Run("registered resolver", func(t *testing.T) {
		calls := 0
		invalid := invalidValidatingStep{
			err:   workflow.Suspend("resolver validation cannot wait"),
			calls: &calls,
		}
		resolver := flow.Then(
			invalid,
			flow.NodeFunc[workflow.Store, string](
				func(context.Context, workflow.Store) (string, error) {
					return "ready", nil
				},
			),
		)
		err := workflow.NewRegistry().RegisterResolver("resolver", resolver)
		assertInvalid(t, err, calls)
	})

	t.Run("factory-built node", func(t *testing.T) {
		calls := 0
		factory := workflow.Factory(func(struct{}) (flow.Node[workflow.Store, workflow.Store], error) {
			return invalidValidatingStep{
				err:   workflow.Suspend("node validation cannot wait"),
				calls: &calls,
			}, nil
		})
		_, err := factory(workflow.NodeSpec{
			ID:     "node",
			Inputs: workflow.OneInput(workflow.Output("seed")),
		})
		assertInvalid(t, err, calls)
	})
}

// Definition errors come from application validators and therefore need not
// obey workflow definition depth. Detecting a suspension in such an error tree
// uses an iterative matcher with [errors.Is] semantics rather than its recursive
// multi-error walk.
func TestValidationClassifiesDeepBranchedSuspensionWithoutStackPerWrapper(t *testing.T) {
	withBoundedStack(t, func() {
		validationErr := workflow.Suspend("validation cannot wait")
		for range 20_000 {
			validationErr = errorChildren{validationErr}
		}

		calls := 0
		_, err := workflow.Run(
			context.Background(),
			invalidValidatingStep{err: validationErr, calls: &calls},
			workflow.NewStore(),
			workflow.RunConfig{},
		)
		if !errors.Is(err, flow.ErrInvalidConfig) || errors.Is(err, workflow.ErrSuspended) {
			t.Fatalf("Run error = %v; want non-suspending invalid config", err)
		}
		if calls != 0 {
			t.Fatalf("invalid step ran %d times; want 0", calls)
		}
	})
}

// TestValidationKeepsAnApplicationLocationAndOwnsItsScope pins both halves of
// what the definition boundary does with a suspension reported through this
// package's own location type. The location survives, because discarding it
// would lose the ID and Op a caller repairs the definition by. The result also
// owns its Scope: the validator that reported it is application code that still
// holds the slice it passed, and the tests above only ever reach this path with
// an error the package built itself, whose Scope is empty.
func TestValidationKeepsAnApplicationLocationAndOwnsItsScope(t *testing.T) {
	retained := indexedScope("items", 1)
	calls := 0
	reported := &workflow.StepError{
		ID:    "inner",
		Scope: retained,
		Op:    workflow.OpValidate,
		Err:   fmt.Errorf("gate: %w", workflow.Suspend("cannot wait")),
	}

	_, err := workflow.Run(
		t.Context(),
		invalidValidatingStep{err: reported, calls: &calls},
		workflow.NewStore(),
		workflow.RunConfig{},
	)
	if !errors.Is(err, flow.ErrInvalidConfig) || errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrInvalidConfig only", err)
	}
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "inner" || stepErr.Op != workflow.OpValidate {
		t.Fatalf("err = %v; want the location the validator reported", err)
	}
	if stepErr == reported {
		t.Fatal("the returned error is the one the validator still holds")
	}
	if !slices.Equal(stepErr.Scope, retained) {
		t.Fatalf("Scope = %+v; want %+v", stepErr.Scope, retained)
	}
	stepErr.Scope[0].ID = "rewritten"
	if retained[0].ID == "rewritten" {
		t.Fatal("the returned error shares the Scope its validator still holds")
	}
	if calls != 0 {
		t.Fatalf("invalid step ran %d times; want 0", calls)
	}
}

func TestJoinSuspensions_normalizesAndCopies(t *testing.T) {
	scope := indexedScope("items", 1)
	second := &workflow.Suspension{ID: "b", Scope: scope, Value: "second"}
	err := workflow.JoinSuspensions(
		second,
		nil,
		&workflow.Suspension{ID: "a", Value: "first"},
	)
	if !errors.Is(err, workflow.ErrSuspended) || !workflow.SuspendedOnly(err) {
		t.Fatalf("JoinSuspensions error = %v; want pure suspension", err)
	}
	got := workflow.Suspensions(err)
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("Suspensions = %+v; want a, b", got)
	}

	scope[0].ID = "changed"
	second.ID = "changed"
	if got = workflow.Suspensions(err); got[1].ID != "b" ||
		!slices.Equal(got[1].Scope, indexedScope("items", 1)) {
		t.Fatalf("joined suspension changed with its input: %+v", got[1])
	}
	var exposed *workflow.Suspension
	if !errors.As(err, &exposed) {
		t.Fatal("errors.As did not find a joined suspension")
	}
	exposed.ID = "changed through errors.As"
	if got = workflow.Suspensions(err); got[0].ID != "a" {
		t.Fatalf("joined suspension changed through Unwrap: %+v", got[0])
	}
	if err := workflow.JoinSuspensions(nil, nil); err != nil {
		t.Fatalf("JoinSuspensions(nil, nil) = %v; want nil", err)
	}
}

// TestJoinSuspensions_ordersByIDThenScope pins the two links before the one the
// test below covers, and it has to supply them out of order to do it. Every other
// ordering test feeds suspensions that arrive sorted already -- an iteration
// collects its elements by index -- and a stable sort leaves those alone whether the
// scope is compared or not.
func TestJoinSuspensions_ordersByIDThenScope(t *testing.T) {
	scoped := func(id string, index uint64) *workflow.Suspension {
		return &workflow.Suspension{ID: id, Scope: indexedScope("items", index)}
	}

	// The ID decides before the scope: read the other way round, the deeper index
	// would put "b" first.
	identity := workflow.Suspensions(workflow.JoinSuspensions(scoped("b", 0), scoped("a", 1)))
	if len(identity) != 2 || identity[0].ID != "a" || identity[1].ID != "b" {
		t.Fatalf("Suspensions = %+v; want a before b", identity)
	}

	// Same ID, reversed scopes: only comparing the scope can put these back in order.
	scopes := workflow.Suspensions(workflow.JoinSuspensions(scoped("wait", 2), scoped("wait", 1), scoped("wait", 0)))
	if len(scopes) != 3 {
		t.Fatalf("Suspensions = %+v; want three", scopes)
	}
	for index, wait := range scopes {
		if !slices.Equal(wait.Scope, indexedScope("items", uint64(index))) {
			t.Fatalf("scopes = %v; want items[0], items[1], items[2]", scopeText(wait.Scope))
		}
	}
}

func TestJoinSuspensions_ordersByAwaitAfterIdentity(t *testing.T) {
	err := workflow.JoinSuspensions(
		&workflow.Suspension{ID: "wait", Await: workflow.Output("z")},
		&workflow.Suspension{ID: "wait", Await: workflow.Output("a")},
	)
	waits := workflow.Suspensions(err)
	if len(waits) != 2 ||
		waits[0].Await != workflow.Output("a") ||
		waits[1].Await != workflow.Output("z") {
		t.Fatalf("Suspensions = %+v; want await a before z", waits)
	}
}

func TestSuspension_nilKey(t *testing.T) {
	var suspension *workflow.Suspension
	if key := suspension.Key(); key.ID != "" || key.Scope != nil {
		t.Fatalf("nil Suspension Key = %+v; want zero", key)
	}
}

func TestSuspend_eventsReportTheThirdOutcome(t *testing.T) {
	pipeline := workflow.Sequence(
		workflow.Leaf("a", workflow.Output("start").Bind[int](),
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
	if _, err := workflow.Run(t.Context(), pipeline, in, cfg); !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
	if want := []string{"started:a", "completed:a", "suspended:gate"}; !slices.Equal(kinds, want) {
		t.Fatalf("events = %v; want %v", kinds, want)
	}

	// On resume, the replayed step reports that it was skipped rather than started.
	kinds = nil
	if _, err := workflow.Run(t.Context(), pipeline, in.WithOutput("approval", true), cfg); err != nil {
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
		Journal:  journal,
		Observer: record(&events),
	}
	step := workflow.Interrupt("approval", map[string]any{"question": "approve?"})

	_, runErr := workflow.Run(t.Context(), step, workflow.NewStore(), cfg)
	waits := workflow.Suspensions(runErr)
	if len(events) != 1 || events[0].Kind != workflow.EventSuspended ||
		len(waits) != 1 || events[0].Err == nil {
		t.Fatalf("first events = %+v, waits = %+v", events, waits)
	}
	if err := journal.Record(waits[0].Key(), true); err != nil {
		t.Fatalf("Record: %v", err)
	}

	events = nil
	out, runErr := workflow.Run(t.Context(), step, workflow.NewStore(), cfg)
	if runErr != nil {
		t.Fatalf("resume: %v", runErr)
	}
	if len(events) != 1 || events[0].Kind != workflow.EventSkipped ||
		events[0].Err != nil || events[0].ID != "approval" {
		t.Fatalf("resumed events = %+v; want one skipped event named approval", events)
	}
	if approved, err := out.Get[bool](workflow.Output("approval")); err != nil || !approved {
		t.Fatalf("approval = %v, %v; want true", approved, err)
	}
	// A replayed wait produces its recorded answer without running anything, so
	// the event's Store is the only place an observer can see the value the
	// resumed run continued with.
	if replayed, err := events[0].Store.Get[bool](workflow.Output("approval")); err != nil || !replayed {
		t.Fatalf("skipped event Store = %v, %v; want the replayed answer", replayed, err)
	}
}

// A resolver need not be a pure function of the Store — a classifier may answer
// differently the second time. The decision is journaled so a resumed run cannot
// take the other branch and leave outputs from both in the Store.
func TestSuspend_branchDecisionIsJournaled(t *testing.T) {
	var calls atomic.Int64
	// Answers "first" once, then "second" forever.
	flaky := resolverNode(func(context.Context, workflow.Store) (string, error) {
		if calls.Add(1) == 1 {
			return "first", nil
		}
		return "second", nil
	})
	label := func(text string) workflow.Step {
		return workflow.Leaf("out", workflow.BinderFunc[string](func(workflow.Store) (string, error) { return text, nil }),
			flow.NodeFunc[string, string](func(_ context.Context, x string) (string, error) { return x, nil }))
	}
	pipeline := workflow.Sequence(
		workflow.Branch(workflow.BranchConfig{ID: "route", Resolver: flaky, Cases: map[string]workflow.Step{
			"first":  label("first"),
			"second": label("second"),
		}}),

		workflow.Await("gate", workflow.Output("approval")),
	)

	journal := workflow.NewJournal()

	paused, runErr := runJournal(pipeline, workflow.NewStore(), journal)
	if !errors.Is(runErr, workflow.ErrSuspended) {
		t.Fatalf("first run err = %v; want ErrSuspended", runErr)
	}
	if got, err := paused.Get[string](workflow.Output("out")); err != nil || got != "first" {
		t.Fatalf("first run took %q, %v; want first", got, err)
	}

	final, runErr := runJournal(pipeline, paused.WithOutput("approval", true), journal)
	if runErr != nil {
		t.Fatalf("resumed run: %v", runErr)
	}
	if got, err := final.Get[string](workflow.Output("out")); err != nil || got != "first" {
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
	body := workflow.Leaf("tick", workflow.BinderFunc[int](func(s workflow.Store) (int, error) {
		v, err := s.Get[int](workflow.Output("tick"))
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
	flaky := flow.NodeFunc[workflow.Store, bool](func(context.Context, workflow.Store) (bool, error) {
		return checks.Add(1) > 1, nil
	})

	journal := workflow.NewJournal()
	loop := workflow.Loop(workflow.LoopConfig{ID: "loop", Body: body, Condition: flaky})

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

// TestSuspend_journaledDecisionThatNamesNoCaseIsReported covers the decision a
// resumed run can replay faithfully and still not honor: the recorded name has
// the type a branch records, so nothing about the record is malformed -- the
// definition it was written for simply had a case this one does not.
// TestAReplayedDecisionMustCarryTheTypeItsCompositeRecorded covers the other
// half, where the record cannot be read as a decision at all.
func TestSuspend_journaledDecisionThatNamesNoCaseIsReported(t *testing.T) {
	journal := workflow.NewJournal()
	// A journal from a different definition could hold anything under this key.
	if err := json.Unmarshal([]byte(`{"version":4,"records":[
		{"id":"route","value":"not-a-case"}
	]}`), journal); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	branch := workflow.Branch(workflow.BranchConfig{
		ID:       "route",
		Resolver: resolverNode(func(context.Context, workflow.Store) (string, error) { return "a", nil }),
		Cases:    map[string]workflow.Step{"a": leafStep("a")},
	})

	if _, err := runJournal(branch, workflow.NewStore(), journal); !errors.Is(err, flow.ErrNoCase) {
		t.Fatalf("branch err = %v; want ErrNoCase", err)
	}
}

// TestSuspend_anonymousWaitTakesTheScopeItWasRaisedIn covers the other half of
// what a boundary fills in for a wait that arrives with no identity. Every wait
// elsewhere carries its own ID -- Await and Interrupt name themselves -- so only
// one raised by application code reaches the boundary anonymous, and every test
// that does raise one raises it at the root, where the scope it should take and
// the one it would keep are the same empty slice.
func TestSuspend_anonymousWaitTakesTheScopeItWasRaisedIn(t *testing.T) {
	iteration := workflow.Iteration(workflow.IterationConfig{
		ID:    "iter",
		Input: workflow.Output("items"),
		Body: workflow.Leaf(
			"ask",
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				return 0, workflow.Suspend("approve?")
			}),
		),
		BodyOutput: workflow.Output("ask"),
	})

	_, err := iteration.Run(
		t.Context(),
		workflow.NewStore().WithOutput("items", []any{1}),
	)
	suspensions := workflow.Suspensions(err)
	if len(suspensions) != 1 || suspensions[0].ID != "ask" ||
		!slices.Equal(suspensions[0].Scope, indexedScope("iter", 0)) {
		t.Fatalf("suspensions = %+v; want ask waiting in iter[0]", suspensions)
	}
}

// TestAFirstSuccessCombinatorHidesTheSuspensionItBeat pins the warning this
// package's documentation gives about composing Steps with generic combinators:
// they know nothing about a Store, so a first-success combinator may hide a
// suspension. Nothing held that sentence to the behavior, and it is the kind of
// claim that goes stale silently -- a combinator taught to recognize the third
// outcome would leave the doc telling callers to avoid a trap that no longer
// exists, and the reverse drift loses an approval nobody is waiting for.
//
// The contrast is the other half, and TestSuspend_parallelLetsSiblingsFinish
// already owns it: [workflow.Parallel] is this package's peer combinator, and
// the same wait survives there with its sibling's output committed. What both
// agree on is that a suspension itself commits nothing.
func TestAFirstSuccessCombinatorHidesTheSuspensionItBeat(t *testing.T) {
	waiting := workflow.Await("approval", workflow.Output("approved"))
	winner := workflow.Leaf(
		"fast",
		workflow.Output("seed").Bind[int](),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value * 2, nil
		}),
	)
	seed := workflow.NewStore().WithOutput("seed", 21)

	t.Run("race", func(t *testing.T) {
		out, err := workflow.Run(
			t.Context(),
			flow.Race[workflow.Store, workflow.Store](waiting, winner),
			seed,
			workflow.RunConfig{},
		)
		if err != nil {
			t.Fatalf("Run: %v; want the winner's success", err)
		}
		if got, getErr := out.Get[int](workflow.Output("fast")); getErr != nil || got != 42 {
			t.Fatalf("winner output = %d, %v; want 42, nil", got, getErr)
		}
		if _, held := out.Lookup(workflow.Output("approval")); held {
			t.Fatal("the beaten wait committed a cell")
		}
	})

	t.Run("race joins them when none wins", func(t *testing.T) {
		failing := workflow.Leaf(
			"slow",
			workflow.Output("seed").Bind[int](),
			flow.NodeFunc[int, int](func(context.Context, int) (int, error) {
				return 0, errors.New("boom")
			}),
		)
		_, err := workflow.Run(
			t.Context(),
			flow.Race[workflow.Store, workflow.Store](waiting, failing),
			seed,
			workflow.RunConfig{},
		)
		waits := workflow.Suspensions(err)
		if len(waits) != 1 || waits[0].ID != "approval" || workflow.SuspendedOnly(err) {
			t.Fatalf("Run error = %v; want the wait joined with the failure", err)
		}
	})
}
