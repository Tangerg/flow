package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestJournal_recordsOnlyCompletedSteps(t *testing.T) {
	boom := errors.New("boom")
	ok := workflow.Leaf("ok", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	bad := workflow.Leaf("bad", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(context.Context, int) (int, error) { return 0, boom }))

	journal := workflow.NewJournal()
	cfg := workflow.RunConfig{Journal: journal}
	if _, err := workflow.Run(t.Context(), workflow.Sequence(ok, bad),
		workflow.NewStore(), cfg); !errors.Is(err, boom) {
		t.Fatalf("err = %v; want boom", err)
	}
	// A failed step is not recorded, so a later run retries it.
	if keys := journal.Keys(); !equalJournalKeys(keys, []workflow.JournalKey{{ID: "ok"}}) {
		t.Fatalf("keys = %v; want only the completed step", keys)
	}
}

func TestSequence_rejectsDuplicateIDsBeforeRunning(t *testing.T) {
	var runs int
	step := func() workflow.Step {
		return workflow.Leaf(
			"same",
			workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
				runs++
				return value, nil
			}),
		)
	}

	_, err := workflow.Sequence(step(), step()).
		Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("err = %v; want ErrDuplicateStep", err)
	}
	if runs != 0 {
		t.Fatalf("%d duplicate steps ran; want validation before side effects", runs)
	}
}

func TestRun_rejectsDuplicateIDsHiddenByOpaqueStepsOnFreshAndReplay(t *testing.T) {
	var firstRuns, secondRuns int
	hidden := func(runs *int) workflow.Step {
		leaf := workflow.Leaf(
			"same",
			workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
				(*runs)++
				return value, nil
			}),
		)
		return flow.NodeFunc[workflow.Store, workflow.Store](leaf.Run)
	}
	pipeline := workflow.Sequence(hidden(&firstRuns), hidden(&secondRuns))
	journal := workflow.NewJournal()
	cfg := workflow.RunConfig{Journal: journal}

	for attempt := range 2 {
		_, err := workflow.Run(
			t.Context(),
			pipeline,
			workflow.NewStore(),
			cfg,
		)
		if !errors.Is(err, workflow.ErrDuplicateStep) {
			t.Fatalf("run %d err = %v; want ErrDuplicateStep", attempt+1, err)
		}
	}
	if firstRuns != 1 || secondRuns != 0 {
		t.Fatalf("runs = %d,%d; duplicate execution was not stopped", firstRuns, secondRuns)
	}
	if journal.Len() != 1 {
		t.Fatalf("journal Len = %d; want the one unambiguous completion", journal.Len())
	}
}

func TestJournal_internalConflictIsReturnedByRun(t *testing.T) {
	journal := workflow.NewJournal()
	step := workflow.Leaf(
		"same",
		workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
			if err := journal.Record(workflow.JournalKey{ID: "same"}, "external"); err != nil {
				return 0, err
			}
			return value, nil
		}),
	)

	_, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore(),
		workflow.RunConfig{Journal: journal},
	)
	if !errors.Is(err, workflow.ErrJournalConflict) {
		t.Fatalf("err = %v; want ErrJournalConflict", err)
	}
}

func TestNilJournalJSONMethods(t *testing.T) {
	var journal *workflow.Journal
	data, err := journal.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var restored workflow.Journal
	if err := restored.UnmarshalJSON(data); err != nil {
		t.Fatalf("UnmarshalJSON encoded nil Journal: %v", err)
	}
	if restored.Len() != 0 {
		t.Fatalf("restored Len = %d; want 0", restored.Len())
	}
	if err := journal.UnmarshalJSON(data); err == nil ||
		!strings.Contains(err.Error(), "nil journal") {
		t.Fatalf("nil receiver UnmarshalJSON err = %v; want nil journal", err)
	}
}

func equalJournalKeys(a, b []workflow.JournalKey) bool {
	return slices.EqualFunc(a, b, func(a, b workflow.JournalKey) bool {
		return a.ID == b.ID && slices.Equal(a.Scope, b.Scope)
	})
}

func TestJournal_forgetAndReset(t *testing.T) {
	var runs int
	step := workflow.Leaf("a", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { runs++; return x, nil }))

	journal := workflow.NewJournal()
	cfg := workflow.RunConfig{Journal: journal}

	for range 3 {
		if _, err := workflow.Run(t.Context(), step, workflow.NewStore(), cfg); err != nil {
			t.Fatalf("run: %v", err)
		}
	}
	if runs != 1 {
		t.Fatalf("ran %d times; want 1", runs)
	}

	journal.Forget(workflow.JournalKey{ID: "a"})
	if _, err := workflow.Run(t.Context(), step, workflow.NewStore(), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}
	if runs != 2 {
		t.Fatalf("ran %d times after Forget; want 2", runs)
	}

	journal.Reset()
	if journal.Len() != 0 {
		t.Fatalf("Len after Reset = %d; want 0", journal.Len())
	}
	if _, err := workflow.Run(t.Context(), step, workflow.NewStore(), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}
	if runs != 3 {
		t.Fatalf("ran %d times after Reset; want 3", runs)
	}
}

func TestJournal_recordExternalCompletion(t *testing.T) {
	var journal workflow.Journal
	key := workflow.JournalKey{ID: "approval", Scope: []string{"items[0]"}}
	if err := journal.Record(key, map[string]any{"approved": true}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	key.Scope[0] = "changed"
	if keys := journal.Keys(); !equalJournalKeys(keys, []workflow.JournalKey{
		{ID: "approval", Scope: []string{"items[0]"}},
	}) {
		t.Fatalf("Keys = %+v; Record retained the caller's path slice", keys)
	}
	if err := journal.Record(workflow.JournalKey{
		ID: "approval", Scope: []string{"items[0]"},
	}, false); !errors.Is(err, workflow.ErrJournalConflict) {
		t.Fatalf("duplicate Record error = %v; want ErrJournalConflict", err)
	}

	journal.Forget(workflow.JournalKey{ID: "approval", Scope: []string{"items[0]"}})
	if err := journal.Record(workflow.JournalKey{
		ID: "approval", Scope: []string{"items[0]"},
	}, false); err != nil {
		t.Fatalf("Record after Forget: %v", err)
	}
	journal.Forget(workflow.JournalKey{ID: "approval", Scope: []string{"missing"}})
	if journal.Len() != 1 {
		t.Fatalf("Len after forgetting a missing scope = %d; want 1", journal.Len())
	}
	if err := journal.Record(workflow.JournalKey{}, true); !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("empty key error = %v; want ErrInvalidStepID", err)
	}

	var nilJournal *workflow.Journal
	if err := nilJournal.Record(workflow.JournalKey{ID: "approval"}, true); err == nil {
		t.Fatal("nil Journal Record unexpectedly succeeded")
	}
}

func TestJournal_enforcesOneSharedDepthLimit(t *testing.T) {
	deepPath := make([]string, workflow.MaxNestingDepth)
	for index := range deepPath {
		deepPath[index] = strconv.Itoa(index)
	}

	journal := workflow.NewJournal()
	if err := journal.Record(
		workflow.JournalKey{ID: "deep", Scope: deepPath},
		true,
	); err != nil {
		t.Fatalf("Record at limit: %v", err)
	}
	keys := journal.Keys()
	if len(keys) != 1 || !slices.Equal(keys[0].Scope, deepPath) {
		t.Fatalf("Keys = %v; want one key with path depth %d", keys, len(deepPath))
	}
	encoded, marshalErr := json.Marshal(journal)
	if marshalErr != nil {
		t.Fatalf("Marshal deep Journal: %v", marshalErr)
	}
	var restored workflow.Journal
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatalf("Unmarshal deep Journal: %v", err)
	}

	tooDeep := append(slices.Clone(deepPath), "too-deep")
	if err := journal.Record(
		workflow.JournalKey{ID: "rejected", Scope: tooDeep},
		true,
	); !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("Record error = %v; want ErrMaxDepth", err)
	}

	document, marshalErr := json.Marshal(map[string]any{
		"version": 2,
		"records": []any{map[string]any{
			"id": "rejected", "scope": tooDeep, "value": true,
		}},
	})
	if marshalErr != nil {
		t.Fatalf("Marshal fixture: %v", marshalErr)
	}
	if err := json.Unmarshal(document, &restored); !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("Unmarshal error = %v; want ErrMaxDepth", err)
	}
}

func TestJournal_jsonRoundTrip(t *testing.T) {
	type payload struct {
		N int `json:"n"`
	}
	journal := workflow.NewJournal()
	step := func(id string, value any) workflow.Step {
		return workflow.Leaf(id, workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
			flow.NodeFunc[int, any](func(context.Context, int) (any, error) { return value, nil }))
	}
	cfg := workflow.RunConfig{Journal: journal}
	pipeline := workflow.Sequence(
		step("i", 42),
		step("s", "text"),
		step("b", true),
		step("struct", payload{N: 7}),
	)
	if _, err := workflow.Run(t.Context(), pipeline, workflow.NewStore(), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}

	data, marshalErr := json.Marshal(journal)
	if marshalErr != nil {
		t.Fatalf("Marshal: %v", marshalErr)
	}
	restored := workflow.NewJournal()
	if err := json.Unmarshal(data, restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !equalJournalKeys(restored.Keys(), journal.Keys()) {
		t.Fatalf("keys = %v; want %v", restored.Keys(), journal.Keys())
	}

	// A restored record has to read back as the type the reading step asks for.
	out, marshalErr := workflow.Run(t.Context(), pipeline, workflow.NewStore(),
		workflow.RunConfig{Journal: restored})
	if marshalErr != nil {
		t.Fatalf("resumed run: %v", marshalErr)
	}
	if got, err := workflow.Get[int](out, workflow.Output("i")); err != nil || got != 42 {
		t.Fatalf("int = %v, %v", got, err)
	}
	if got, err := workflow.Get[string](out, workflow.Output("s")); err != nil || got != "text" {
		t.Fatalf("string = %v, %v", got, err)
	}
	if got, err := workflow.Get[bool](out, workflow.Output("b")); err != nil || !got {
		t.Fatalf("bool = %v, %v", got, err)
	}
	if got, err := workflow.Get[payload](out, workflow.Output("struct")); err != nil || got.N != 7 {
		t.Fatalf("struct = %+v, %v", got, err)
	}
}

func TestJournal_keysAreStructuredAndCollisionFree(t *testing.T) {
	var firstRuns, secondRuns int
	step := func(id string, runs *int) workflow.Step {
		return workflow.Leaf(id,
			workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
			flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
				(*runs)++
				return value, nil
			}),
		)
	}
	first := step("c", &firstRuns)
	second := step("b/c", &secondRuns)

	journal := workflow.NewJournal()
	cfg := workflow.RunConfig{Journal: journal}
	run := func() {
		t.Helper()
		if _, err := workflow.Run(workflow.WithScope(t.Context(), "a/b"),
			first, workflow.NewStore(), cfg); err != nil {
			t.Fatalf("first: %v", err)
		}
		if _, err := workflow.Run(workflow.WithScope(t.Context(), "a"),
			second, workflow.NewStore(), cfg); err != nil {
			t.Fatalf("second: %v", err)
		}
	}
	run()
	run()
	if firstRuns != 1 || secondRuns != 1 || journal.Len() != 2 {
		t.Fatalf("runs = %d,%d records = %d; distinct structured keys collided",
			firstRuns, secondRuns, journal.Len())
	}

	want := []workflow.JournalKey{
		{Scope: []string{"a"}, ID: "b/c"},
		{Scope: []string{"a/b"}, ID: "c"},
	}
	if got := journal.Keys(); !equalJournalKeys(got, want) {
		t.Fatalf("Keys = %v; want %v", got, want)
	}
	keys := journal.Keys()
	keys[0].Scope[0] = "changed"
	if got := journal.Keys(); !equalJournalKeys(got, want) {
		t.Fatalf("Keys leaked its path storage: %v", got)
	}

	journal.Forget(want[1])
	run()
	if firstRuns != 2 || secondRuns != 1 {
		t.Fatalf("runs after Forget = %d,%d; want 2,1", firstRuns, secondRuns)
	}
}

func TestJournal_jsonFormatIsVersionedAndRejectsDuplicateKeys(t *testing.T) {
	empty, err := json.Marshal(workflow.NewJournal())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if got, want := string(empty), `{"version":2,"records":[]}`; got != want {
		t.Fatalf("empty Journal JSON = %s; want %s", got, want)
	}

	journal := workflow.NewJournal()
	duplicate := []byte(`{"version":2,"records":[
		{"scope":["a/b"],"id":"c","value":1},
		{"scope":["a/b"],"id":"c","value":2}
	]}`)
	if err := json.Unmarshal(duplicate, journal); err == nil {
		t.Fatal("duplicate structured key unexpectedly decoded")
	}
	if journal.Len() != 0 {
		t.Fatalf("failed decode changed Journal: %d records", journal.Len())
	}
	for _, data := range [][]byte{
		[]byte(`{"version":3,"records":[]}`),
		[]byte(`{"version":2,"records":[],"extra":true}`),
		[]byte(`{"version":2,"records":[{"id":"","value":1}]}`),
		[]byte(`{"version":2,"records":[{"id":"a"}]}`),
	} {
		if err := json.Unmarshal(data, journal); err == nil {
			t.Fatalf("invalid Journal JSON decoded: %s", data)
		}
	}
}

// encoding/json matches member names case-insensitively, so "reCords" fills the
// same field as "records". A decode that also consulted a second, case-sensitive
// view of the same bytes would disagree with itself on such a document.
func TestJournal_unmarshalToleratesMemberNameCase(t *testing.T) {
	journal := workflow.NewJournal()
	mixedCase := []byte(`{"version":2,"reCords":[{"id":"0","vAlue":0}]}`)
	if err := json.Unmarshal(mixedCase, journal); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if journal.Len() != 1 {
		t.Fatalf("records = %d; want 1", journal.Len())
	}
	if keys := journal.Keys(); len(keys) != 1 || keys[0].ID != "0" {
		t.Fatalf("keys = %+v; want one record named 0", keys)
	}
}

// A recorded nil is a completed step, while an omitted value is a malformed
// record. Both encode as an absent Go value, so only the member's presence tells
// them apart.
func TestJournal_unmarshalSeparatesRecordedNilFromAbsentValue(t *testing.T) {
	journal := workflow.NewJournal()
	if err := json.Unmarshal(
		[]byte(`{"version":2,"records":[{"id":"a","value":null}]}`),
		journal,
	); err != nil {
		t.Fatalf("Unmarshal explicit null: %v", err)
	}
	encoded, err := json.Marshal(journal)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if got, want := string(encoded), `{"version":2,"records":[{"id":"a","value":null}]}`; got != want {
		t.Fatalf("round trip = %s; want %s", got, want)
	}
	if err := json.Unmarshal(
		[]byte(`{"version":2,"records":[{"id":"a"}]}`),
		workflow.NewJournal(),
	); err == nil {
		t.Fatal("record without a value unexpectedly decoded")
	}
}

func TestJournal_marshalReportsTheOffendingRecord(t *testing.T) {
	journal := workflow.NewJournal()
	step := workflow.Leaf("bad", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
		flow.NodeFunc[int, any](func(context.Context, int) (any, error) { return func() {}, nil }))
	if _, err := workflow.Run(t.Context(), step, workflow.NewStore(),
		workflow.RunConfig{Journal: journal}); err != nil {
		t.Fatalf("run: %v", err)
	}

	_, err := json.Marshal(journal)
	if err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("Marshal err = %v; want it to name the record", err)
	}
}

func TestJournal_unmarshalIsAtomic(t *testing.T) {
	journal := workflow.NewJournal()
	if err := json.Unmarshal([]byte(`{"version":2,"records":[{"id":"a","value":1}]}`), journal); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"b":`), journal); err == nil {
		t.Fatal("expected a decode error")
	}
	if keys := journal.Keys(); !equalJournalKeys(keys, []workflow.JournalKey{{ID: "a"}}) {
		t.Fatalf("keys = %v; want the journal unchanged after a failed decode", keys)
	}
}

func TestJournal_nilIsSafe(t *testing.T) {
	var journal *workflow.Journal
	if journal.Len() != 0 || journal.Keys() != nil {
		t.Fatal("nil Journal did not read as empty")
	}
	journal.Reset()
	journal.Forget(workflow.JournalKey{ID: "a"})

	// A nil Journal disables resumption rather than being attached.
	step := workflow.Leaf("a", workflow.BindFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	if _, err := workflow.Run(t.Context(), step, workflow.NewStore(),
		workflow.RunConfig{Journal: nil}); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestJournal_concurrentBranchesRecordSafely(t *testing.T) {
	const branches = 32
	steps := make([]workflow.Step, branches)
	for i := range steps {
		id := "b" + strconv.Itoa(i)
		steps[i] = workflow.Leaf(id, workflow.BindFunc[int](func(workflow.Store) (int, error) { return i, nil }),
			flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	}

	journal := workflow.NewJournal()
	cfg := workflow.RunConfig{Journal: journal}
	if _, err := workflow.Run(t.Context(),
		workflow.Parallel(steps, workflow.ParallelConfig{}), workflow.NewStore(), cfg); err != nil {
		t.Fatalf("run: %v", err)
	}
	if journal.Len() != branches {
		t.Fatalf("journal recorded %d of %d branches", journal.Len(), branches)
	}

	// Reading and writing at once must also be safe.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _ = journal.Keys() })
		wg.Go(func() { journal.Forget(workflow.JournalKey{ID: "b0"}) })
	}
	wg.Wait()
}

func TestAwaitFactory(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterNode("await", workflow.AwaitFactory())

	spec := []byte(`{"kind":"sequence","steps":[
	  {"id":"approval","type":"await","kind":"leaf","input":{"nodeID":"inbox","path":"/decision"}},
	  {"id":"act","type":"addN","kind":"leaf","input":{"nodeID":"start","path":"/output"},"config":{"n":1}}
	]}`)
	step, compileErr := reg.CompileSpecJSON(spec)
	if compileErr != nil {
		t.Fatalf("CompileSpecJSON: %v", compileErr)
	}

	in := workflow.NewStore().WithOutput("start", 1)
	if _, err := step.Run(t.Context(), in); !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("err = %v; want ErrSuspended", err)
	}
	out, compileErr := step.Run(t.Context(), in.WithCell("inbox", "decision", "yes"))
	if compileErr != nil {
		t.Fatalf("run: %v", compileErr)
	}
	if got, err := workflow.Get[int](out, workflow.Output("act")); err != nil || got != 2 {
		t.Fatalf("act = %v, %v; want 2", got, err)
	}
}

func TestAwaitFactory_requiresAWiredPort(t *testing.T) {
	if _, err := workflow.AwaitFactory()(workflow.NodeSpec{ID: "a"}); !errors.Is(err, workflow.ErrMissingPort) {
		t.Fatalf("err = %v; want ErrMissingPort", err)
	}
	if _, err := workflow.AwaitFactory()(workflow.NodeSpec{
		ID: "a",
		Inputs: workflow.Inputs{
			workflow.DefaultPort: workflow.Output("x"),
			"extra":              workflow.Output("y"),
		},
	}); !errors.Is(err, workflow.ErrUnknownPort) {
		t.Fatalf("extra port error = %v; want ErrUnknownPort", err)
	}
	if _, err := workflow.AwaitFactory()(workflow.NodeSpec{
		ID:     "a",
		Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("x")},
		Config: json.RawMessage(`{}`),
	}); !errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("config error = %v; want ErrInvalidSpec", err)
	}
}

func TestInterruptFactory_roundTripsStructuredRequestAndResponse(t *testing.T) {
	reg := workflow.NewRegistry().MustRegisterNode("interrupt", workflow.InterruptFactory())
	step, compileErr := reg.CompileSpecJSON([]byte(`{
		"kind":"leaf",
		"id":"approval",
		"type":"interrupt",
		"config":{"question":"publish?","actions":["approve","reject"]}
	}`))
	if compileErr != nil {
		t.Fatalf("CompileSpecJSON: %v", compileErr)
	}

	journal := workflow.NewJournal()
	cfg := workflow.RunConfig{Journal: journal}
	_, compileErr = workflow.Run(t.Context(), step, workflow.NewStore(), cfg)
	waits := workflow.Suspensions(compileErr)
	if len(waits) != 1 {
		t.Fatalf("Suspensions = %+v; want one", waits)
	}
	request, ok := waits[0].Value.(map[string]any)
	if !ok || request["question"] != "publish?" {
		t.Fatalf("Value = %#v; want decoded config", waits[0].Value)
	}

	if err := journal.Record(waits[0].Key(), map[string]any{"approved": true}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	out, compileErr := workflow.Run(t.Context(), step, workflow.NewStore(), cfg)
	if compileErr != nil {
		t.Fatalf("resume: %v", compileErr)
	}
	response, compileErr := workflow.Get[struct {
		Approved bool `json:"approved"`
	}](out, workflow.Output("approval"))
	if compileErr != nil || !response.Approved {
		t.Fatalf("response = %+v, %v; want approved", response, compileErr)
	}
}

func TestInterruptFactory_rejectsInputsAndInvalidConfig(t *testing.T) {
	factory := workflow.InterruptFactory()
	if _, err := factory(workflow.NodeSpec{
		ID:     "approval",
		Inputs: workflow.Inputs{workflow.DefaultPort: workflow.Output("x")},
	}); !errors.Is(err, workflow.ErrUnknownPort) {
		t.Fatalf("input error = %v; want ErrUnknownPort", err)
	}
	if _, err := factory(workflow.NodeSpec{
		ID:     "approval",
		Config: json.RawMessage(`{`),
	}); !errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("config error = %v; want ErrInvalidSpec", err)
	}
	if _, err := factory(workflow.NodeSpec{
		ID:     "approval",
		Config: json.RawMessage(`{"question":"first","question":"second"}`),
	}); !errors.Is(err, workflow.ErrInvalidSpec) {
		t.Fatalf("duplicate config error = %v; want ErrInvalidSpec", err)
	}
}
