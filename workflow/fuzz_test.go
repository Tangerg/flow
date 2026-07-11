package workflow_test

import (
	"encoding/json"
	"testing"

	"github.com/Tangerg/flow/workflow"
)

func FuzzStoreGetPath(f *testing.F) {
	f.Add("output.items.0.name")
	f.Add("")
	f.Add("output.-1")

	value := map[string]any{
		"items": []any{map[string]any{"name": "first"}},
	}
	store := workflow.NewStore().WithOutput("node", value)
	f.Fuzz(func(t *testing.T, path string) {
		_, _ = store.Lookup(workflow.At("node", path))
	})
}

func FuzzCompileJSON(f *testing.F) {
	f.Add([]byte(`{"nodes":[]}`))
	f.Add([]byte(`{"nodes":[{"id":"a","type":"addN"}]}`))
	reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN())

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = reg.CompileGraphJSON(data)
	})
}

func FuzzStoreJSON(f *testing.F) {
	f.Add([]byte(`{"node":{"output":1}}`))
	f.Add([]byte(`null`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var store workflow.Store
		if err := json.Unmarshal(data, &store); err != nil {
			return
		}
		encoded, err := json.Marshal(store)
		if err != nil {
			t.Fatalf("Marshal after successful Unmarshal: %v", err)
		}
		var roundTrip workflow.Store
		if err := json.Unmarshal(encoded, &roundTrip); err != nil {
			t.Fatalf("round-trip Unmarshal: %v", err)
		}
	})
}
