package workflow_test

import (
	"encoding/json"
	"testing"

	"github.com/Tangerg/flow/workflow"
)

func FuzzStoreLookupPath(f *testing.F) {
	f.Add("output.items.0.name")
	f.Add("")
	f.Add("output.-1")

	value := map[string]any{
		"items": []any{map[string]any{"name": "first"}},
	}
	store := workflow.NewStore().WithOutput("node", value)
	f.Fuzz(func(_ *testing.T, path string) {
		_, _ = store.Lookup(workflow.At("node", path))
	})
}

func FuzzCompileGraphJSON(f *testing.F) {
	f.Add([]byte(`{"nodes":[]}`))
	f.Add([]byte(`{"nodes":[{"id":"a","type":"addN"}]}`))
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = reg.CompileGraphJSON(data)
	})
}

func FuzzCompileSpecJSON(f *testing.F) {
	f.Add([]byte(`{"kind":"sequence","steps":[]}`))
	f.Add([]byte(`{"kind":"leaf","id":"a","type":"addN"}`))
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = reg.CompileSpecJSON(data)
	})
}

// FuzzStoreJSON checks that decoding arbitrary JSON never panics and that a
// decoded Store round-trips: whatever it holds must marshal again and decode to
// the same thing. Get must also return rather than panic for every target type.
func FuzzStoreJSON(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"a":{"output":1}}`))
	f.Add([]byte(`{"a":{"output":1e10000}}`))
	f.Add([]byte(`{"a":{"output":9223372036854775807}}`))
	f.Add([]byte(`{"a":{"output":-0.0}}`))
	f.Add([]byte(`{"a":{"output":{"n":[1,2,{"deep":true}]}}}`))
	f.Add([]byte(`{"a":{"output":null}}`))
	f.Add([]byte(`{"a":{"output":"text"}}`))
	f.Add([]byte(`null`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var store workflow.Store
		if err := json.Unmarshal(data, &store); err != nil {
			return
		}

		again, err := json.Marshal(store)
		if err != nil {
			t.Fatalf("a decoded Store failed to marshal: %v", err)
		}
		var second workflow.Store
		if err := json.Unmarshal(again, &second); err != nil {
			t.Fatalf("re-decoding a marshalled Store failed: %v", err)
		}
		third, err := json.Marshal(second)
		if err != nil {
			t.Fatalf("marshal is not stable: %v", err)
		}
		if string(again) != string(third) {
			t.Fatalf("round trip is not idempotent:\n%s\n%s", again, third)
		}

		// Every typed read must come back as a value or an error.
		ref := workflow.Output("a")
		_, _ = workflow.Get[int](store, ref)
		_, _ = workflow.Get[int64](store, ref)
		_, _ = workflow.Get[uint](store, ref)
		_, _ = workflow.Get[float64](store, ref)
		_, _ = workflow.Get[string](store, ref)
		_, _ = workflow.Get[bool](store, ref)
		_, _ = workflow.Get[[]any](store, ref)
		_, _ = workflow.Get[[]int](store, ref)
		_, _ = workflow.Get[map[string]any](store, ref)
		_, _ = workflow.Get[struct{ N int }](store, ref)
		_, _ = workflow.Get[any](store, ref)
	})
}

// FuzzJournalJSON checks the same properties for a Journal, which carries a run's
// recorded results across a restart.
func FuzzJournalJSON(f *testing.F) {
	f.Add([]byte(`{"version":1,"records":[]}`))
	f.Add([]byte(`{"version":1,"records":[{"id":"a","value":1}]}`))
	f.Add([]byte(`{"version":1,"records":[{"path":["iter[0]"],"id":"el","value":{"n":1}}]}`))
	f.Add([]byte(`{"version":1,"records":[{"path":["[0]"],"id":"loop","value":true},{"id":"route","value":"case"}]}`))
	f.Add([]byte(`{"version":1,"records":[{"path":["a/b"],"id":"c","value":1},{"path":["a"],"id":"b/c","value":2}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		journal := workflow.NewJournal()
		if err := json.Unmarshal(data, journal); err != nil {
			return
		}
		again, err := json.Marshal(journal)
		if err != nil {
			t.Fatalf("a decoded Journal failed to marshal: %v", err)
		}
		second := workflow.NewJournal()
		if err := json.Unmarshal(again, second); err != nil {
			t.Fatalf("re-decoding a marshalled Journal failed: %v", err)
		}
		if second.Len() != journal.Len() {
			t.Fatalf("round trip changed the record count: %d then %d", journal.Len(), second.Len())
		}
	})
}
