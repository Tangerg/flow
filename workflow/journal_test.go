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

type journalRecordingMarshaler struct {
	journal *workflow.Journal
}

func (j journalRecordingMarshaler) MarshalJSON() ([]byte, error) {
	if err := j.journal.Record(workflow.JournalKey{ID: "recorded-during-marshal"}, 2); err != nil {
		return nil, err
	}
	return []byte(`1`), nil
}

func TestJournalMarshalJSONInvokesApplicationCodeOutsideTheSnapshotLock(t *testing.T) {
	journal := workflow.NewJournal()
	if err := journal.Record(
		workflow.JournalKey{ID: "original"},
		journalRecordingMarshaler{journal: journal},
	); err != nil {
		t.Fatalf("Record: %v", err)
	}

	data, err := json.Marshal(journal)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if keys := journal.Keys(); !equalJournalKeys(keys, []workflow.JournalKey{
		{ID: "original"},
		{ID: "recorded-during-marshal"},
	}) {
		t.Fatalf("Journal keys = %+v; want the reentrant record", keys)
	}

	restored := workflow.NewJournal()
	if err := json.Unmarshal(data, restored); err != nil {
		t.Fatalf("Unmarshal snapshot: %v", err)
	}
	if keys := restored.Keys(); !equalJournalKeys(keys, []workflow.JournalKey{{ID: "original"}}) {
		t.Fatalf("snapshot keys = %+v; want only the pre-marshal record", keys)
	}
}

func TestJournal_recordsOnlyCompletedSteps(t *testing.T) {
	boom := errors.New("boom")
	ok := workflow.Leaf("ok", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
		flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x, nil }))
	bad := workflow.Leaf("bad", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
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

func TestJournalKey_JSONBoundaryIsStrictLosslessAndAtomic(t *testing.T) {
	invalid := map[string][]byte{
		"unknown field":       []byte(`{"id":"wait","unknown":true}`),
		"duplicate identity":  []byte(`{"id":"first","id":"second"}`),
		"invalid UTF-8":       {'{', '"', 'i', 'd', '"', ':', '"', 0xff, '"', '}'},
		"unpaired surrogate":  []byte(`{"id":"\ud800"}`),
		"invalid scope index": []byte(`{"id":"wait","scope":[{"id":"loop","index":-1}]}`),
		"unknown scope field": []byte(`{"id":"wait","scope":[{"id":"loop","extra":1}]}`),
		"null":                []byte(`null`),
		"array":               []byte(`[]`),
		// A persisted key names its members exactly, so encoding/json's case
		// folding cannot satisfy id or scope with another spelling, and a
		// colliding pair cannot let member order pick the surviving value.
		"folded identity":    []byte(`{"ID":"wait"}`),
		"folded scope":       []byte(`{"id":"wait","SCOPE":[{"id":"loop"}]}`),
		"colliding identity": []byte(`{"id":"first","ID":"second"}`),
		// Canonical members can still describe an unusable key, so identity is
		// validated after decoding rather than inferred from member presence.
		"empty identity":         []byte(`{"id":""}`),
		"scope without identity": []byte(`{"scope":[{"id":"loop"}]}`),
	}
	for name, data := range invalid {
		t.Run(name, func(t *testing.T) {
			target := workflow.JournalKey{ID: "kept", Scope: ordinaryScope("outer")}
			if err := json.Unmarshal(data, &target); err == nil {
				t.Fatal("Unmarshal unexpectedly succeeded")
			}
			if target.ID != "kept" || !equalJournalKeys(
				[]workflow.JournalKey{target},
				[]workflow.JournalKey{{ID: "kept", Scope: ordinaryScope("outer")}},
			) {
				t.Fatalf("failed Unmarshal changed receiver: %#v", target)
			}
		})
	}
	var nilKey *workflow.JournalKey
	if err := nilKey.UnmarshalJSON([]byte(`{"id":"wait"}`)); err == nil {
		t.Fatal("nil receiver UnmarshalJSON unexpectedly succeeded")
	}

	tooDeep := workflow.JournalKey{ID: "wait", Scope: make([]workflow.ScopeFrame, workflow.MaxNestingDepth+1)}
	for index := range tooDeep.Scope {
		tooDeep.Scope[index] = workflow.ScopeFrame{ID: strconv.Itoa(index)}
	}
	if _, err := json.Marshal(tooDeep); !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("Marshal depth error = %v; want ErrMaxDepth", err)
	}

	bad := string([]byte{0xff})
	for name, key := range map[string]workflow.JournalKey{
		"ID":    {ID: bad},
		"scope": {ID: "wait", Scope: ordinaryScope(bad)},
	} {
		t.Run("marshal "+name, func(t *testing.T) {
			if _, err := json.Marshal(key); err == nil {
				t.Fatal("Marshal accepted invalid identity text")
			}
		})
	}

	original := workflow.JournalKey{ID: "wait", Scope: indexedScope("items", 1<<63)}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var restored workflow.JournalKey
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !equalJournalKeys([]workflow.JournalKey{restored}, []workflow.JournalKey{original}) {
		t.Fatalf("round trip = %#v; want %#v", restored, original)
	}
}

func TestSequence_rejectsDuplicateIDsBeforeRunning(t *testing.T) {
	var runs int
	step := func() workflow.Step {
		return workflow.Leaf(
			"same",
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
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

func TestSequence_validatesBuiltInCompositeBehindOpaqueBoundary(t *testing.T) {
	var runs int
	step := func() workflow.Step {
		return workflow.Leaf(
			"same",
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
			flow.NodeFunc[int, int](func(_ context.Context, value int) (int, error) {
				runs++
				return value, nil
			}),
		)
	}
	inner := workflow.Sequence(step(), step())
	opaque := flow.NodeFunc[workflow.Store, workflow.Store](inner.Run)

	_, err := workflow.Sequence(opaque).Run(t.Context(), workflow.NewStore())
	if !errors.Is(err, workflow.ErrDuplicateStep) {
		t.Fatalf("err = %v; want ErrDuplicateStep", err)
	}
	if runs != 0 {
		t.Fatalf("steps ran %d times; want inner validation before side effects", runs)
	}
}

func TestSequence_validatesNestedConstructionBeforeRunning(t *testing.T) {
	var runs int
	valid := workflow.LeafFunc(
		"valid",
		workflow.Output("start"),
		func(_ context.Context, input int) (int, error) {
			runs++
			return input, nil
		},
	)
	invalid := workflow.Leaf[int, int]("invalid", nil, flow.NodeFunc[int, int](
		func(_ context.Context, input int) (int, error) { return input, nil },
	))

	_, err := workflow.Sequence(valid, invalid).Run(
		t.Context(),
		workflow.NewStore().WithOutput("start", 1),
	)
	if !errors.Is(err, flow.ErrNilFunc) {
		t.Fatalf("err = %v; want flow.ErrNilFunc", err)
	}
	if runs != 0 {
		t.Fatalf("valid step ran %d times; want complete validation first", runs)
	}
}

func TestRun_rejectsDuplicateIDsHiddenByOpaqueStepsOnFreshAndReplay(t *testing.T) {
	var firstRuns, secondRuns int
	hidden := func(runs *int) workflow.Step {
		leaf := workflow.Leaf(
			"same",
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
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
		workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
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

func TestNilJournalJSONRepresentsAbsence(t *testing.T) {
	var journal *workflow.Journal
	data, err := journal.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if string(data) != "null" {
		t.Fatalf("MarshalJSON = %s; want null", data)
	}
	encoded, err := json.Marshal(journal)
	if err != nil || string(encoded) != "null" {
		t.Fatalf("json.Marshal = %s, %v; want null, nil", encoded, err)
	}

	var restored *workflow.Journal
	if err := json.Unmarshal(data, &restored); err != nil || restored != nil {
		t.Fatalf("json.Unmarshal = %#v, %v; want nil Journal", restored, err)
	}
	var concrete workflow.Journal
	if err := json.Unmarshal(data, &concrete); err == nil {
		t.Fatal("concrete Journal accepted null checkpoint")
	}
	if err := journal.UnmarshalJSON(data); err == nil ||
		!strings.Contains(err.Error(), "nil journal") {
		t.Fatalf("nil receiver UnmarshalJSON err = %v; want nil journal", err)
	}
}

func TestJournal_MarshalEnforcesRoundTripDepth(t *testing.T) {
	atLimit := workflow.NewJournal()
	if err := atLimit.Record(
		workflow.JournalKey{ID: "deep"},
		nestedArrays(workflow.MaxNestingDepth-3),
	); err != nil {
		t.Fatalf("Record at depth limit: %v", err)
	}
	data, err := json.Marshal(atLimit)
	if err != nil {
		t.Fatalf("Marshal at depth limit: %v", err)
	}
	var restored workflow.Journal
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal value produced at depth limit: %v", err)
	}

	tooDeep := workflow.NewJournal()
	if err := tooDeep.Record(
		workflow.JournalKey{ID: "deep"},
		nestedArrays(workflow.MaxNestingDepth-2),
	); err != nil {
		t.Fatalf("Record beyond depth limit: %v", err)
	}
	if _, err := json.Marshal(tooDeep); !errors.Is(err, workflow.ErrMaxDepth) ||
		!strings.Contains(err.Error(), `record "deep"`) {
		t.Fatalf("Marshal beyond depth limit error = %v; want record-scoped ErrMaxDepth", err)
	}
}

func equalJournalKeys(a, b []workflow.JournalKey) bool {
	return slices.EqualFunc(a, b, func(a, b workflow.JournalKey) bool {
		return a.ID == b.ID && slices.Equal(a.Scope, b.Scope)
	})
}

func TestJournal_forgetAndReset(t *testing.T) {
	var runs int
	step := workflow.Leaf("a", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
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

	if err := journal.Forget(workflow.JournalKey{ID: "a"}); err != nil {
		t.Fatalf("Forget: %v", err)
	}
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
	key := workflow.JournalKey{ID: "approval", Scope: indexedScope("items", 0)}
	if err := journal.Record(key, map[string]any{"approved": true}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	key.Scope[0].ID = "changed"
	if keys := journal.Keys(); !equalJournalKeys(keys, []workflow.JournalKey{
		{ID: "approval", Scope: indexedScope("items", 0)},
	}) {
		t.Fatalf("Keys = %+v; Record retained the caller's scope slice", keys)
	}
	if err := journal.Record(workflow.JournalKey{
		ID: "approval", Scope: indexedScope("items", 0),
	}, false); !errors.Is(err, workflow.ErrJournalConflict) {
		t.Fatalf("duplicate Record error = %v; want ErrJournalConflict", err)
	}

	if err := journal.Forget(workflow.JournalKey{
		ID: "approval", Scope: indexedScope("items", 0),
	}); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if err := journal.Record(workflow.JournalKey{
		ID: "approval", Scope: indexedScope("items", 0),
	}, false); err != nil {
		t.Fatalf("Record after Forget: %v", err)
	}
	if err := journal.Forget(workflow.JournalKey{
		ID: "approval", Scope: ordinaryScope("missing"),
	}); err != nil {
		t.Fatalf("Forget missing record: %v", err)
	}
	if journal.Len() != 1 {
		t.Fatalf("Len after forgetting a missing scope = %d; want 1", journal.Len())
	}
	if err := journal.Record(workflow.JournalKey{}, true); !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("empty key error = %v; want ErrInvalidStepID", err)
	}
	if err := journal.Forget(workflow.JournalKey{}); !errors.Is(err, workflow.ErrInvalidStepID) {
		t.Fatalf("empty Forget key error = %v; want ErrInvalidStepID", err)
	}

	var nilJournal *workflow.Journal
	if err := nilJournal.Record(workflow.JournalKey{ID: "approval"}, true); err == nil {
		t.Fatal("nil Journal Record unexpectedly succeeded")
	}
	if err := nilJournal.Forget(workflow.JournalKey{ID: "approval"}); err == nil {
		t.Fatal("nil Journal Forget unexpectedly succeeded")
	}
}

func TestJournal_rejectsInvalidScopeFrames(t *testing.T) {
	tests := []workflow.ScopeFrame{
		{},
		{ID: "items", Index: 1},
	}
	for _, frame := range tests {
		journal := workflow.NewJournal()
		if err := journal.Record(workflow.JournalKey{
			ID:    "step",
			Scope: []workflow.ScopeFrame{frame},
		}, true); err == nil {
			t.Errorf("Record accepted invalid scope frame %+v", frame)
		}
		if journal.Len() != 0 {
			t.Errorf("invalid scope frame %+v changed Journal", frame)
		}
	}
}

func TestJournal_scopeIndexesArePortableAcrossWordSizes(t *testing.T) {
	const beyondUint32 = uint64(1) << 32
	document := []byte(`{"version":4,"records":[{"scope":[{"id":"items","index":4294967296}],"id":"step","value":true}]}`)
	journal := workflow.NewJournal()
	if err := json.Unmarshal(document, journal); err != nil {
		t.Fatalf("Unmarshal 64-bit scope index: %v", err)
	}
	keys := journal.Keys()
	if len(keys) != 1 || len(keys[0].Scope) != 1 || keys[0].Scope[0].Index != beyondUint32 {
		t.Fatalf("Keys = %+v; want portable index %d", keys, beyondUint32)
	}
	encoded, err := json.Marshal(journal)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(encoded), `"index":4294967296`) {
		t.Fatalf("encoded Journal = %s; want portable index", encoded)
	}
}

func TestJournal_enforcesOneSharedDepthLimit(t *testing.T) {
	deepScope := make([]workflow.ScopeFrame, workflow.MaxNestingDepth)
	for index := range deepScope {
		deepScope[index] = workflow.ScopeFrame{ID: strconv.Itoa(index)}
	}

	journal := workflow.NewJournal()
	if err := journal.Record(
		workflow.JournalKey{ID: "deep", Scope: deepScope},
		true,
	); err != nil {
		t.Fatalf("Record at limit: %v", err)
	}
	keys := journal.Keys()
	if len(keys) != 1 || !slices.Equal(keys[0].Scope, deepScope) {
		t.Fatalf("Keys = %v; want one key with scope depth %d", keys, len(deepScope))
	}
	encoded, marshalErr := json.Marshal(journal)
	if marshalErr != nil {
		t.Fatalf("Marshal deep Journal: %v", marshalErr)
	}
	var restored workflow.Journal
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatalf("Unmarshal deep Journal: %v", err)
	}

	tooDeep := append(slices.Clone(deepScope), workflow.ScopeFrame{ID: "too-deep"})
	if err := journal.Record(
		workflow.JournalKey{ID: "rejected", Scope: tooDeep},
		true,
	); !errors.Is(err, workflow.ErrMaxDepth) {
		t.Fatalf("Record error = %v; want ErrMaxDepth", err)
	}

	document, marshalErr := json.Marshal(map[string]any{
		"version": 4,
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
		return workflow.Leaf(id, workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
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
			workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
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
		{Scope: ordinaryScope("a"), ID: "b/c"},
		{Scope: ordinaryScope("a/b"), ID: "c"},
	}
	if got := journal.Keys(); !equalJournalKeys(got, want) {
		t.Fatalf("Keys = %v; want %v", got, want)
	}
	keys := journal.Keys()
	keys[0].Scope[0].ID = "changed"
	if got := journal.Keys(); !equalJournalKeys(got, want) {
		t.Fatalf("Keys leaked its scope storage: %v", got)
	}

	if err := journal.Forget(want[1]); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	run()
	if firstRuns != 2 || secondRuns != 1 {
		t.Fatalf("runs after Forget = %d,%d; want 2,1", firstRuns, secondRuns)
	}
}

func TestJournal_jsonFormatIsVersionedAndRejectsDuplicateKeys(t *testing.T) {
	empty, marshalErr := json.Marshal(workflow.NewJournal())
	if marshalErr != nil {
		t.Fatalf("Marshal: %v", marshalErr)
	}
	if got, want := string(empty), `{"version":4,"records":[]}`; got != want {
		t.Fatalf("empty Journal JSON = %s; want %s", got, want)
	}

	journal := workflow.NewJournal()
	duplicate := []byte(`{"version":4,"records":[
		{"scope":[{"id":"a/b"}],"id":"c","value":1},
		{"scope":[{"id":"a/b"}],"id":"c","value":2}
	]}`)
	if err := json.Unmarshal(duplicate, journal); err == nil {
		t.Fatal("duplicate structured key unexpectedly decoded")
	}
	if journal.Len() != 0 {
		t.Fatalf("failed decode changed Journal: %d records", journal.Len())
	}
	if err := journal.Record(workflow.JournalKey{
		ID:    "ordinary",
		Scope: ordinaryScope("same"),
	}, 1); err != nil {
		t.Fatalf("Record ordinary frame: %v", err)
	}
	if err := journal.Record(workflow.JournalKey{
		ID:    "indexed",
		Scope: indexedScope("same", 0),
	}, 2); err != nil {
		t.Fatalf("Record indexed frame: %v", err)
	}
	if got := journal.Keys(); len(got) != 2 || got[0].ID != "ordinary" || got[1].ID != "indexed" {
		t.Fatalf("Keys = %+v; want ordinary frame before indexed frame", got)
	}
	encoded, marshalErr := json.Marshal(journal)
	if marshalErr != nil {
		t.Fatalf("Marshal structured scopes: %v", marshalErr)
	}
	if got, want := string(encoded), `{"version":4,"records":[{"scope":[{"id":"same"}],"id":"ordinary","value":1},{"scope":[{"id":"same","index":0}],"id":"indexed","value":2}]}`; got != want {
		t.Fatalf("structured Journal JSON = %s; want %s", got, want)
	}
	journal.Reset()
	for _, data := range [][]byte{
		[]byte(`[]`),
		[]byte(`{"version":"4","records":[]}`),
		[]byte(`{"version":4.5,"records":[]}`),
		[]byte(`{"version":4,"version":4,"records":[]}`),
		[]byte(`{"version":5,"records":[]}`),
		[]byte(`{"version":4}`),
		[]byte(`{"version":4,"records":null}`),
		[]byte(`{"version":4,"records":[],"extra":true}`),
		[]byte(`{"version":4,"records":[null]}`),
		[]byte(`{"version":4,"records":[{"value":1}]}`),
		[]byte(`{"version":4,"records":[{"id":1,"value":1}]}`),
		[]byte(`{"version":4,"records":[{"id":"","value":1}]}`),
		[]byte(`{"version":4,"records":[{"id":"a"}]}`),
		[]byte(`{"version":4,"records":[{"scope":null,"id":"a","value":1}]}`),
		[]byte(`{"version":4,"records":[{"scope":[1],"id":"a","value":1}]}`),
		[]byte(`{"version":4,"records":[{"scope":[{}],"id":"a","value":1}]}`),
		[]byte(`{"version":4,"records":[{"scope":[{"id":1}],"id":"a","value":1}]}`),
		[]byte(`{"version":4,"records":[{"scope":[{"id":""}],"id":"a","value":1}]}`),
		[]byte(`{"version":4,"records":[{"scope":[{"id":"s","extra":1}],"id":"a","value":1}]}`),
		[]byte(`{"version":4,"records":[{"scope":[{"id":"s","indexed":true}],"id":"a","value":1}]}`),
		[]byte(`{"version":4,"records":[{"scope":[{"id":"s","index":"0"}],"id":"a","value":1}]}`),
		[]byte(`{"version":4,"records":[{"scope":[{"id":"s","index":0.5}],"id":"a","value":1}]}`),
		[]byte(`{"version":4,"records":[{"scope":[{"id":"s","index":999999999999999999999999999}],"id":"a","value":1}]}`),
		[]byte(`{"version":4,"records":[{"scope":[{"id":"s","index":-1}],"id":"a","value":1}]}`),
		[]byte(`{"version":4,"records":[{"id":"\ud800","value":1}]}`),
	} {
		if err := json.Unmarshal(data, journal); err == nil {
			t.Fatalf("invalid Journal JSON decoded: %s", data)
		}
	}
}

func TestJournal_rejectsEveryPreviousWireVersion(t *testing.T) {
	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "kept"}, true); err != nil {
		t.Fatalf("Record: %v", err)
	}
	for version := 1; version < 4; version++ {
		document := []byte(`{"version":` + strconv.Itoa(version) + `,"records":[]}`)
		if err := json.Unmarshal(document, journal); err == nil {
			t.Errorf("Journal wire version %d unexpectedly decoded", version)
		}
		if keys := journal.Keys(); !equalJournalKeys(keys, []workflow.JournalKey{{ID: "kept"}}) {
			t.Fatalf("failed version %d decode changed Journal: %+v", version, keys)
		}
	}
}

func TestJournal_unmarshalAcceptsMathematicalIntegerSpellings(t *testing.T) {
	tests := map[string]struct {
		document string
		want     uint64
	}{
		"decimal version and exponent index": {
			document: `{"version":4.0,"records":[{"scope":[{"id":"s","index":1e0}],"id":"a","value":true}]}`,
			want:     1,
		},
		"exponent version and decimal max uint64": {
			document: `{"version":4e0,"records":[{"scope":[{"id":"s","index":18446744073709551615.0}],"id":"a","value":true}]}`,
			want:     ^uint64(0),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			journal := workflow.NewJournal()
			if err := json.Unmarshal([]byte(test.document), journal); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			keys := journal.Keys()
			if len(keys) != 1 || len(keys[0].Scope) != 1 || keys[0].Scope[0].Index != test.want {
				t.Fatalf("Keys = %+v; want index %d", keys, test.want)
			}
		})
	}
}

func TestJournal_versionDiagnosticsAreMachineIndependent(t *testing.T) {
	const document = `{"version":2147483648,"records":[]}`
	err := json.Unmarshal([]byte(document), workflow.NewJournal())
	if err == nil || !strings.Contains(err.Error(), "unsupported version 2147483648") {
		t.Fatalf("Unmarshal error = %v; want fixed-width unsupported-version diagnostic", err)
	}
}

// A versioned checkpoint has one canonical spelling for every member.
// encoding/json folds struct field names by case, which would let a second
// spelling silently replace records, scope, or a value if Journal delegated its
// wire contract to ordinary struct decoding.
func TestJournal_unmarshalRejectsNoncanonicalAndCaseCollidingMembers(t *testing.T) {
	tests := []string{
		`{"version":4,"reCords":[]}`,
		`{"version":4,"records":[],"Records":[{"id":"a","value":1}]}`,
		`{"version":4,"records":[{"id":"a","vAlue":1}]}`,
		`{"version":4,"records":[{"id":"a","value":1,"Value":2}]}`,
		`{"version":4,"records":[{"scope":[],"Scope":[{"id":"changed"}],"id":"a","value":1}]}`,
	}
	for _, data := range tests {
		journal := workflow.NewJournal()
		if err := json.Unmarshal([]byte(data), journal); err == nil {
			t.Errorf("noncanonical Journal JSON decoded: %s", data)
		}
		if journal.Len() != 0 {
			t.Errorf("failed decode changed Journal: %d records", journal.Len())
		}
	}
}

// A recorded nil is a completed step, while an omitted value is a malformed
// record. Both encode as an absent Go value, so only the member's presence tells
// them apart.
func TestJournal_unmarshalSeparatesRecordedNilFromAbsentValue(t *testing.T) {
	journal := workflow.NewJournal()
	if err := json.Unmarshal(
		[]byte(`{"version":4,"records":[{"id":"a","value":null}]}`),
		journal,
	); err != nil {
		t.Fatalf("Unmarshal explicit null: %v", err)
	}
	encoded, err := json.Marshal(journal)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if got, want := string(encoded), `{"version":4,"records":[{"id":"a","value":null}]}`; got != want {
		t.Fatalf("round trip = %s; want %s", got, want)
	}
	if err := json.Unmarshal(
		[]byte(`{"version":4,"records":[{"id":"a"}]}`),
		workflow.NewJournal(),
	); err == nil {
		t.Fatal("record without a value unexpectedly decoded")
	}
}

func TestJournal_marshalReportsTheOffendingRecord(t *testing.T) {
	journal := workflow.NewJournal()
	step := workflow.Leaf("bad", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 0, nil }),
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

func TestJournal_marshalRejectsDuplicateMembersFromApplicationMarshaler(t *testing.T) {
	journal := workflow.NewJournal()
	if err := journal.Record(
		workflow.JournalKey{ID: "duplicate"},
		duplicateObjectJSON{},
	); err != nil {
		t.Fatalf("Record: %v", err)
	}

	_, err := json.Marshal(journal)
	if err == nil ||
		!strings.Contains(err.Error(), `record "duplicate"`) ||
		!strings.Contains(err.Error(), `duplicate object member "same"`) {
		t.Fatalf("Marshal error = %v; want record-scoped duplicate member", err)
	}
}

func TestJournal_marshalRejectsUnpairedSurrogateFromApplicationMarshaler(t *testing.T) {
	journal := workflow.NewJournal()
	if err := journal.Record(
		workflow.JournalKey{ID: "surrogate"},
		unpairedSurrogateJSON{},
	); err != nil {
		t.Fatalf("Record: %v", err)
	}

	_, err := json.Marshal(journal)
	if err == nil ||
		!strings.Contains(err.Error(), `record "surrogate"`) ||
		!strings.Contains(err.Error(), "unpaired UTF-16 surrogate") {
		t.Fatalf("Marshal error = %v; want record-scoped Unicode error", err)
	}
}

func TestJournal_recordRejectsNonUTF8Identities(t *testing.T) {
	invalid := string([]byte{0xff})
	tests := map[string]struct {
		key  workflow.JournalKey
		want string
	}{
		"step ID": {
			key:  workflow.JournalKey{ID: invalid},
			want: "not valid UTF-8",
		},
		"scope ID": {
			key: workflow.JournalKey{
				ID:    "step",
				Scope: []workflow.ScopeFrame{{ID: invalid}},
			},
			want: "scope frame 0: scope frame ID is not valid UTF-8",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			journal := workflow.NewJournal()
			err := journal.Record(test.key, 1)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Record error = %v; want %q", err, test.want)
			}
			if journal.Len() != 0 {
				t.Fatal("invalid identity changed Journal")
			}
		})
	}
}

func TestJournal_unmarshalIsAtomic(t *testing.T) {
	journal := workflow.NewJournal()
	if err := json.Unmarshal([]byte(`{"version":4,"records":[{"id":"a","value":1}]}`), journal); err != nil {
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
	if err := journal.Forget(workflow.JournalKey{ID: "a"}); err == nil {
		t.Fatal("nil Journal Forget unexpectedly succeeded")
	}

	// A nil Journal disables resumption rather than being attached.
	step := workflow.Leaf("a", workflow.BinderFunc[int](func(workflow.Store) (int, error) { return 1, nil }),
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
		steps[i] = workflow.Leaf(id, workflow.BinderFunc[int](func(workflow.Store) (int, error) { return i, nil }),
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
		wg.Go(func() {
			if err := journal.Forget(workflow.JournalKey{ID: "b0"}); err != nil {
				t.Errorf("Forget: %v", err)
			}
		})
	}
	wg.Wait()
}

func TestAwaitFactory(t *testing.T) {
	reg := workflow.NewRegistry().
		MustRegisterNode("addN", addN()).
		MustRegisterNode("await", workflow.AwaitFactory())

	spec := []byte(`{"kind":"sequence","steps":[
	  {"id":"approval","type":"await","kind":"leaf","inputs":{"in":{"nodeID":"inbox","path":"/decision"}}},
	  {"id":"act","type":"addN","kind":"leaf","inputs":{"in":{"nodeID":"start","path":"/output"}},"config":{"n":1}}
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
	}); !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("config error = %v; want ErrInvalidConfig", err)
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
	}); !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("config error = %v; want ErrInvalidConfig", err)
	}
	if _, err := factory(workflow.NodeSpec{
		ID:     "approval",
		Config: json.RawMessage(`{"question":"first","question":"second"}`),
	}); !errors.Is(err, flow.ErrInvalidConfig) {
		t.Fatalf("duplicate config error = %v; want ErrInvalidConfig", err)
	}
}
