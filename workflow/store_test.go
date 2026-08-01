package workflow_test

import (
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Tangerg/flow/workflow"
)

type failOnceJSON struct {
	calls int
}

func (f *failOnceJSON) MarshalJSON() ([]byte, error) {
	f.calls++
	if f.calls == 1 {
		return nil, errors.New("first marshal failed")
	}
	return []byte(`true`), nil
}

type nestedValue struct {
	Next *nestedValue `json:"next,omitempty"`
}

func TestRef_helpers(t *testing.T) {
	ref := workflow.Output("step").Child("items", "0")
	if ref != workflow.At("step", "output", "items", "0") || ref.String() != "step#/output/items/0" {
		t.Fatalf("ref = %#v (%s)", ref, ref)
	}
	if got := ref.Child(); got != ref {
		t.Fatalf("Child() = %#v, want %#v", got, ref)
	}
	if got := workflow.Output("step").Child(""); got.Path != "/output/" {
		t.Fatalf("Child(empty segment) = %#v; want an addressable empty JSON key", got)
	}
	if workflow.Item("each") != workflow.At("each", "item") {
		t.Fatal("Item returned the wrong reference")
	}
	if workflow.ItemIndex("each") != workflow.At("each", "index") {
		t.Fatal("Index returned the wrong reference")
	}

	escaped := workflow.At("step", "output", "a/b", "c~d", "")
	if escaped.Path != "/output/a~1b/c~0d/" {
		t.Fatalf("escaped Path = %q", escaped.Path)
	}
	store := workflow.NewStore().WithOutput("step", map[string]any{
		"a/b": map[string]any{"c~d": map[string]any{"": "found"}},
	})
	if got, ok := store.Lookup(escaped); !ok || got != "found" {
		t.Fatalf("Lookup(%s) = %v, %v; want found, true", escaped, got, ok)
	}
}

func TestStore_LookupRejectsMalformedJSONPointers(t *testing.T) {
	store := workflow.NewStore().WithOutput("step", 1)
	for _, path := range []string{"", "output", "/output/~", "/output/~2"} {
		if value, ok := store.Lookup(workflow.Ref{NodeID: "step", Path: path}); ok {
			t.Fatalf("Lookup path %q = %v, true; want unresolved", path, value)
		}
	}
}

func TestStore_Changes(t *testing.T) {
	base := workflow.NewStore().WithOutput("a", 1)
	next := base.WithOutput("b", 2).WithCell("c", "key", 3)

	changes := next.Changes(base)
	if len(changes) != 2 {
		t.Fatalf("Changes = %+v; want 2 writes", changes)
	}
	if changes[0].Ref() != workflow.Output("b") || changes[0].Value.(int) != 2 {
		t.Fatalf("changes[0] = %+v; want b.output = 2", changes[0])
	}
	if changes[1].Ref() != workflow.At("c", "key") || changes[1].Value.(int) != 3 {
		t.Fatalf("changes[1] = %+v; want c.key = 3", changes[1])
	}
	if got := base.Changes(base); len(got) != 0 {
		t.Fatalf("Changes against self = %+v; want none", got)
	}
}

func TestStore_ChangesKeepsOnlyTheFinalWritePerCell(t *testing.T) {
	base := workflow.NewStore()
	next := base.WithOutput("a", 1).WithOutput("a", 2).WithOutput("a", 3)

	changes := next.Changes(base)
	if len(changes) != 1 || changes[0].Value.(int) != 3 {
		t.Fatalf("Changes = %+v; want one write of 3", changes)
	}
}

func TestStore_ChangesAcrossCompaction(t *testing.T) {
	// Past the overlay limit a Store materializes, so Changes falls back to
	// comparing write identities. Order must still be the order of writing.
	base := workflow.NewStore().WithOutput("base", 0)
	next := base
	const writes = 200
	for i := range writes {
		next = next.WithOutput("n"+strconv.Itoa(i), i)
	}

	changes := next.Changes(base)
	if len(changes) != writes {
		t.Fatalf("Changes = %d writes; want %d", len(changes), writes)
	}
	for i, change := range changes {
		if change.Ref() != workflow.Output("n"+strconv.Itoa(i)) || change.Value.(int) != i {
			t.Fatalf("changes[%d] = %+v; want n%d = %d", i, change, i, i)
		}
	}
}

func TestStore_ChangesAgainstUnrelatedStore(t *testing.T) {
	// An unrelated base shares no snapshot, so every cell counts as a change.
	unrelated := workflow.NewStore().WithOutput("x", 1)
	s := workflow.NewStore().WithOutput("y", 2)

	changes := s.Changes(unrelated)
	if len(changes) != 1 || changes[0].Ref() != workflow.Output("y") {
		t.Fatalf("Changes = %+v; want one write to y.output", changes)
	}
	// A Store has no delete, so a cell missing from s is not reported.
	if got := workflow.NewStore().Changes(unrelated); len(got) != 0 {
		t.Fatalf("Changes = %+v; want none", got)
	}
}

func TestStore_WithAndGet(t *testing.T) {
	s := workflow.NewStore().WithOutput("n1", 42)

	v, ok := s.Lookup(workflow.At("n1", "output"))
	if !ok || v.(int) != 42 {
		t.Fatalf("Get = %v, %v; want 42, true", v, ok)
	}
}

func TestStore_immutable(t *testing.T) {
	s1 := workflow.NewStore().WithOutput("n", 1)
	s2 := s1.WithOutput("n", 2)

	if v, _ := s1.Lookup(workflow.At("n", "output")); v.(int) != 1 {
		t.Fatalf("original store mutated: got %v, want 1", v)
	}
	if v, _ := s2.Lookup(workflow.At("n", "output")); v.(int) != 2 {
		t.Fatalf("new store wrong: got %v, want 2", v)
	}
}

func TestStore_sharesUntouchedNodes(t *testing.T) {
	s1 := workflow.NewStore().WithOutput("a", 1)
	s2 := s1.WithOutput("b", 2)

	// Writing b must not disturb a.
	if v, ok := s2.Lookup(workflow.At("a", "output")); !ok || v.(int) != 1 {
		t.Fatalf("Get(a) after writing b = %v, %v; want 1, true", v, ok)
	}
}

func TestStore_overlayCompactionPreservesSnapshots(t *testing.T) {
	snapshots := make(map[int]workflow.Store)
	store := workflow.NewStore()
	for i := range 100 {
		store = store.WithOutput("node-"+strconv.Itoa(i), i)
		if i == 31 || i == 32 || i == 64 || i == 99 {
			snapshots[i] = store
		}
	}

	for last, snapshot := range snapshots {
		t.Run(strconv.Itoa(last+1)+" writes", func(t *testing.T) {
			for i := 0; i <= last; i++ {
				got, ok := snapshot.Lookup(workflow.Output("node-" + strconv.Itoa(i)))
				if !ok || got != i {
					t.Fatalf("node-%d = %v, %v; want %d, true", i, got, ok, i)
				}
			}
			if _, ok := snapshot.Lookup(workflow.Output("node-" + strconv.Itoa(last+1))); ok {
				t.Fatalf("snapshot unexpectedly contains node-%d", last+1)
			}
		})
	}
}

func TestStore_overwriteAcrossCompaction(t *testing.T) {
	original := workflow.NewStore().WithOutput("shared", "old")
	store := original
	for i := range 40 {
		store = store.WithOutput("node-"+strconv.Itoa(i), i)
	}
	updated := store.WithOutput("shared", "new")

	if got, _ := original.Lookup(workflow.Output("shared")); got != "old" {
		t.Fatalf("original shared = %v; want old", got)
	}
	if got, _ := store.Lookup(workflow.Output("shared")); got != "old" {
		t.Fatalf("compacted shared = %v; want old", got)
	}
	if got, _ := updated.Lookup(workflow.Output("shared")); got != "new" {
		t.Fatalf("updated shared = %v; want new", got)
	}
}

func TestStore_path(t *testing.T) {
	nested := map[string]any{
		"items": []any{
			map[string]any{"name": "a"},
			map[string]any{"name": "b"},
		},
	}
	s := workflow.NewStore().WithOutput("n", nested)

	v, ok := s.Lookup(workflow.At("n", "output", "items", "1", "name"))
	if !ok || v.(string) != "b" {
		t.Fatalf("path Get = %v, %v; want b, true", v, ok)
	}
}

func TestStore_arrayPathsUseCanonicalJSONPointerIndexes(t *testing.T) {
	store := workflow.NewStore().
		WithOutput("array", []any{"zero", "one"}).
		WithOutput("object", map[string]any{"01": "object key"})

	if value, ok := store.Lookup(workflow.Output("array").Child("1")); !ok || value != "one" {
		t.Fatalf("canonical index = %v, %v; want one, true", value, ok)
	}
	for _, token := range []string{"01", "+1", "-1", "-"} {
		if value, ok := store.Lookup(workflow.Output("array").Child(token)); ok {
			t.Fatalf("array token %q resolved to %v; want miss", token, value)
		}
	}
	if value, ok := store.Lookup(workflow.Output("object").Child("01")); !ok || value != "object key" {
		t.Fatalf("object key = %v, %v; want object key, true", value, ok)
	}
}

func TestStore_pathHasTheSameJSONSemanticsBeforeAndAfterSerialization(t *testing.T) {
	type item struct {
		Name string `json:"name"`
	}
	type payload struct {
		Count  int               `json:"count"`
		Items  []item            `json:"items"`
		Labels map[string]string `json:"labels"`
		Hidden string            `json:"-"`
	}

	original := workflow.NewStore().WithOutput("typed", payload{
		Count:  2,
		Items:  []item{{Name: "first"}, {Name: "second"}},
		Labels: map[string]string{"kind": "example"},
		Hidden: "not addressable",
	})
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var restored workflow.Store
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for _, store := range []workflow.Store{original, restored} {
		if got, err := workflow.Get[int](store, workflow.At("typed", "output", "count")); err != nil || got != 2 {
			t.Fatalf("count = %v, %v; want 2", got, err)
		}
		if got, err := workflow.Get[string](store, workflow.At("typed", "output", "items", "1", "name")); err != nil || got != "second" {
			t.Fatalf("item name = %q, %v; want second", got, err)
		}
		if got, err := workflow.Get[string](store, workflow.At("typed", "output", "labels", "kind")); err != nil || got != "example" {
			t.Fatalf("label = %q, %v; want example", got, err)
		}
		if _, ok := store.Lookup(workflow.At("typed", "output", "Hidden")); ok {
			t.Fatal("json-excluded field unexpectedly resolved")
		}
	}
}

func TestStore_missing(t *testing.T) {
	s := workflow.NewStore().WithOutput("n", 1)

	if _, ok := s.Lookup(workflow.At("n", "nope")); ok {
		t.Fatal("expected miss on unknown key")
	}
	if _, ok := s.Lookup(workflow.At("other", "output")); ok {
		t.Fatal("expected miss on unknown node")
	}
	if _, ok := s.Lookup(workflow.At("n", "output", "deep")); ok {
		t.Fatal("expected miss walking into a non-container")
	}
}

func TestStore_rejectsInvalidPointerAndUnrepresentableNestedValues(t *testing.T) {
	store := workflow.NewStore().
		WithOutput("channel", make(chan int)).
		WithOutput("plain", 1)

	if _, ok := store.Lookup(workflow.Ref{NodeID: "plain", Path: "/~2"}); ok {
		t.Fatal("invalid JSON Pointer unexpectedly resolved")
	}
	if _, ok := store.Lookup(workflow.Output("channel").Child("field")); ok {
		t.Fatal("channel unexpectedly resolved through JSON")
	}

	var deep *nestedValue
	for range workflow.MaxNestingDepth + 1 {
		deep = &nestedValue{Next: deep}
	}
	store = store.WithOutput("deep", deep)
	if _, ok := store.Lookup(workflow.Output("deep").Child("next")); ok {
		t.Fatal("excessively nested value unexpectedly resolved")
	}
}

func TestGet_reportsValuesThatCannotBeMarshaled(t *testing.T) {
	store := workflow.NewStore().WithOutput("channel", make(chan int))
	if _, err := workflow.Get[int](store, workflow.Output("channel")); !errors.Is(err, workflow.ErrTypeMismatch) {
		t.Fatalf("error = %v; want ErrTypeMismatch", err)
	}
}

func TestStore_JSONRoundTrip(t *testing.T) {
	original := workflow.NewStore().
		WithCell("a", "output", map[string]any{"items": []any{"x", true}}).
		WithCell("b", "output", 42)
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded workflow.Store
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got, ok := decoded.Lookup(workflow.At("a", "output", "items", "1")); !ok || got != true {
		t.Fatalf("nested value = %v, %v", got, ok)
	}
	// A decoded Store holds JSON-domain values: a number is a json.Number, so
	// nothing has been rounded yet.
	if got, ok := decoded.Lookup(workflow.At("b", "output")); !ok || got != json.Number("42") {
		t.Fatalf("number = %T(%v), %v; want json.Number(42)", got, got, ok)
	}
	// Get hides the difference, which is what lets a typed step resume.
	if got, err := workflow.Get[int](decoded, workflow.Output("b")); err != nil || got != 42 {
		t.Fatalf("Get[int] = %v, %v; want 42", got, err)
	}
}

// A round trip must preserve what a typed step reads, at any depth and for any
// JSON-representable shape. This is the property a resumed workflow depends on.
func TestStore_JSONRoundTripPreservesTypedReads(t *testing.T) {
	type payload struct {
		N     int               `json:"n"`
		Items []string          `json:"items"`
		Meta  map[string]string `json:"meta"`
	}
	original := workflow.NewStore().
		WithOutput("i", 42).
		WithOutput("i64", int64(math.MaxInt64)). // beyond float64's exact range
		WithOutput("u", uint32(7)).
		WithOutput("f", 1.5).
		WithOutput("s", "text").
		WithOutput("b", true).
		WithOutput("slice", []int{1, 2, 3}).
		WithOutput("struct", payload{N: 1, Items: []string{"a"}, Meta: map[string]string{"k": "v"}}).
		WithOutput("nested", map[string]any{"deep": []any{map[string]any{"n": 9}}})

	data, marshalErr := json.Marshal(original)
	if marshalErr != nil {
		t.Fatalf("Marshal: %v", marshalErr)
	}
	var decoded workflow.Store
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got, err := workflow.Get[int](decoded, workflow.Output("i")); err != nil || got != 42 {
		t.Fatalf("int = %v, %v", got, err)
	}
	if got, err := workflow.Get[int64](decoded, workflow.Output("i64")); err != nil || got != math.MaxInt64 {
		t.Fatalf("int64 = %v, %v; want %d — precision was lost", got, err, int64(math.MaxInt64))
	}
	if got, err := workflow.Get[uint32](decoded, workflow.Output("u")); err != nil || got != 7 {
		t.Fatalf("uint32 = %v, %v", got, err)
	}
	if got, err := workflow.Get[float64](decoded, workflow.Output("f")); err != nil || got != 1.5 {
		t.Fatalf("float64 = %v, %v", got, err)
	}
	if got, err := workflow.Get[string](decoded, workflow.Output("s")); err != nil || got != "text" {
		t.Fatalf("string = %v, %v", got, err)
	}
	if got, err := workflow.Get[bool](decoded, workflow.Output("b")); err != nil || !got {
		t.Fatalf("bool = %v, %v", got, err)
	}
	if got, err := workflow.Get[[]int](decoded, workflow.Output("slice")); err != nil || !slices.Equal(got, []int{1, 2, 3}) {
		t.Fatalf("[]int = %v, %v", got, err)
	}
	got, marshalErr := workflow.Get[payload](decoded, workflow.Output("struct"))
	if marshalErr != nil || got.N != 1 || !slices.Equal(got.Items, []string{"a"}) || got.Meta["k"] != "v" {
		t.Fatalf("struct = %+v, %v", got, marshalErr)
	}
	// Conversion applies at any path depth, not just to whole cells.
	if n, err := workflow.Get[int](decoded, workflow.At("nested", "output", "deep", "0", "n")); err != nil || n != 9 {
		t.Fatalf("nested int = %v, %v", n, err)
	}
}

func TestGet_convertsWithoutReinterpreting(t *testing.T) {
	var decoded workflow.Store
	if err := json.Unmarshal([]byte(`{"n":{"frac":42.5,"whole":42,"text":"42","huge":1e10000}}`), &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// A number that is not an integer must not be truncated into one.
	if _, err := workflow.Get[int](decoded, workflow.At("n", "frac")); !errors.Is(err, workflow.ErrTypeMismatch) {
		t.Fatalf("Get[int] of 42.5 err = %v; want ErrTypeMismatch", err)
	}
	// A number is not a string and a string is not a number.
	if _, err := workflow.Get[string](decoded, workflow.At("n", "whole")); !errors.Is(err, workflow.ErrTypeMismatch) {
		t.Fatalf("Get[string] of a number err = %v; want ErrTypeMismatch", err)
	}
	if _, err := workflow.Get[int](decoded, workflow.At("n", "text")); !errors.Is(err, workflow.ErrTypeMismatch) {
		t.Fatalf("Get[int] of a string err = %v; want ErrTypeMismatch", err)
	}
	// A value outside float64's range is preserved on the way in and reported on
	// the way out, so decoding never silently drops a cell.
	if _, ok := decoded.Lookup(workflow.At("n", "huge")); !ok {
		t.Fatal("an unrepresentable number was dropped during decode")
	}
	if _, err := workflow.Get[float64](decoded, workflow.At("n", "huge")); !errors.Is(err, workflow.ErrTypeMismatch) {
		t.Fatalf("Get[float64] of 1e10000 err = %v; want ErrTypeMismatch", err)
	}
}

func TestStore_UnmarshalIsAtomic(t *testing.T) {
	for name, data := range map[string]string{
		"truncated":      `{"new":{"output":1}`,
		"wrong shape":    `{"new":"not an object"}`,
		"null store":     `null`,
		"null node":      `{"new":null}`,
		"duplicate node": `{"new":{"output":1},"new":{"output":2}}`,
		"duplicate cell": `{"new":{"output":1,"output":2}}`,
		"trailing":       `{"new":{"output":1}} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := workflow.NewStore().WithOutput("old", 1)
			if err := json.Unmarshal([]byte(data), &store); err == nil {
				t.Fatal("expected a decode error")
			}
			if got, ok := store.Lookup(workflow.At("old", "output")); !ok || got != 1 {
				t.Fatalf("store changed after a failed decode: %v, %v", got, ok)
			}
		})
	}
}

func TestStore_UnmarshalReportsEveryJSONKind(t *testing.T) {
	for _, data := range []string{"true", "1", `"text"`, "[]"} {
		store := workflow.NewStore().WithOutput("old", 1)
		if err := json.Unmarshal([]byte(data), &store); err == nil {
			t.Fatalf("Unmarshal(%s) unexpectedly succeeded", data)
		}
		if got, ok := store.Lookup(workflow.Output("old")); !ok || got != 1 {
			t.Fatalf("store changed after Unmarshal(%s)", data)
		}
	}
}

func TestStore_UnmarshalReportsFirstInvalidNodeDeterministically(t *testing.T) {
	const document = `{"z":null,"a":false}`
	const want = `workflow: unmarshal store node "a": expected object, got boolean`

	for range 100 {
		var store workflow.Store
		err := json.Unmarshal([]byte(document), &store)
		if err == nil || err.Error() != want {
			t.Fatalf("Unmarshal error = %v; want %q", err, want)
		}
	}
}

func TestStore_UnmarshalRejectsNilReceiver(t *testing.T) {
	var store *workflow.Store
	if err := store.UnmarshalJSON([]byte(`{}`)); err == nil ||
		!strings.Contains(err.Error(), "nil store") {
		t.Fatalf("UnmarshalJSON err = %v; want nil store", err)
	}
}

func TestStore_UnmarshalEmptyReplacesStore(t *testing.T) {
	store := workflow.NewStore().WithOutput("old", 1)
	if err := json.Unmarshal([]byte(`{}`), &store); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if _, ok := store.Lookup(workflow.Output("old")); ok {
		t.Fatal("empty JSON did not replace the previous Store")
	}
}

func TestStore_UnmarshalGivesRestoredWritesDeterministicOrder(t *testing.T) {
	var store workflow.Store
	if err := json.Unmarshal([]byte(`{
		"z":{"output":3},
		"a":{"z":2,"a":1}
	}`), &store); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	changes := store.Changes(workflow.NewStore())
	got := make([]workflow.Ref, len(changes))
	for i, change := range changes {
		got[i] = change.Ref()
	}
	want := []workflow.Ref{
		workflow.At("a", "a"),
		workflow.At("a", "z"),
		workflow.Output("z"),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Changes refs = %v; want %v", got, want)
	}
}

func TestStore_MarshalReportsCell(t *testing.T) {
	store := workflow.NewStore().WithOutput("bad", func() {})
	_, err := json.Marshal(store)
	if err == nil || !strings.Contains(err.Error(), `node "bad" key "output"`) {
		t.Fatalf("err = %v; want cell path", err)
	}
}

func TestStore_MarshalReportsTheFirstBadCellDeterministically(t *testing.T) {
	store := workflow.NewStore().
		WithOutput("z", func() {}).
		WithCell("a", "z", func() {}).
		WithCell("a", "a", func() {})

	_, err := json.Marshal(store)
	if err == nil || !strings.Contains(err.Error(), `node "a" key "a"`) {
		t.Fatalf("err = %v; want first bad cell a.a", err)
	}
}

func TestStore_MarshalPreservesOriginalErrorWhenRetrySucceeds(t *testing.T) {
	value := new(failOnceJSON)
	_, err := json.Marshal(workflow.NewStore().WithOutput("flaky", value))
	if err == nil || !strings.Contains(err.Error(), "marshal store") {
		t.Fatalf("error = %v; want Store marshal error", err)
	}
	if value.calls != 2 {
		t.Fatalf("MarshalJSON calls = %d; want 2", value.calls)
	}
}
