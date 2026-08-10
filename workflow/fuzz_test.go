package workflow_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Tangerg/flow/workflow"
)

func FuzzStoreLookupPath(f *testing.F) {
	f.Add("/output/items/0/name")
	f.Add("")
	f.Add("/output/~")
	f.Add("/output/~2")

	value := map[string]any{
		"items": []any{map[string]any{"name": "first"}},
	}
	store := workflow.NewStore().WithOutput("node", value)
	f.Fuzz(func(_ *testing.T, path string) {
		_, _ = store.Lookup(workflow.Ref{NodeID: "node", Path: path})
	})
}

func FuzzCompileGraphJSON(f *testing.F) {
	f.Add([]byte(`{"nodes":[]}`))
	f.Add([]byte(`{"nodes":[],"concurrency":1e0}`))
	f.Add([]byte(`{"nodes":[{"id":"a","type":"addN"}]}`))
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())

	f.Fuzz(func(t *testing.T, data []byte) {
		_, documentErr := reg.CompileGraphJSON(data)
		graph := workflow.Graph{
			Nodes:       []workflow.GraphNode{{ID: "sentinel", Type: "sentinel"}},
			Concurrency: 7,
		}
		before := graph
		decodeErr := json.Unmarshal(data, &graph)
		if decodeErr != nil {
			if documentErr == nil {
				t.Fatal("direct Graph decode failed but CompileGraphJSON succeeded")
			}
			if !reflect.DeepEqual(graph, before) {
				t.Fatal("failed direct Graph decode changed its destination")
			}
			return
		}
		encoded, marshalErr := json.Marshal(graph)
		if marshalErr != nil {
			t.Fatalf("a strictly decoded Graph failed to marshal: %v", marshalErr)
		}
		var roundTripped workflow.Graph
		if err := json.Unmarshal(encoded, &roundTripped); err != nil {
			t.Fatalf("a marshalled Graph failed to decode: %v", err)
		}
		again, marshalErr := json.Marshal(roundTripped)
		if marshalErr != nil || string(encoded) != string(again) {
			t.Fatalf("Graph encoding is not idempotent: %s, %s, %v", encoded, again, marshalErr)
		}
		_, typedErr := reg.CompileGraph(graph)
		if (documentErr == nil) != (typedErr == nil) {
			t.Fatalf(
				"Graph compile success differs after direct decode: bytes=%v, typed=%v",
				documentErr,
				typedErr,
			)
		}
	})
}

func FuzzCompileSpecJSON(f *testing.F) {
	f.Add([]byte(`{"kind":"sequence","steps":[]}`))
	f.Add([]byte(`{"kind":"parallel","steps":[],"concurrency":1.0}`))
	f.Add([]byte(`{"kind":"leaf","id":"a","type":"addN"}`))
	reg := workflow.NewRegistry().MustRegisterNode("addN", addN())

	f.Fuzz(func(t *testing.T, data []byte) {
		_, documentErr := reg.CompileSpecJSON(data)
		spec := workflow.Spec{Kind: workflow.KindSequence, Steps: []workflow.Spec{{Kind: workflow.KindSequence}}}
		before := spec
		decodeErr := json.Unmarshal(data, &spec)
		if decodeErr != nil {
			if documentErr == nil {
				t.Fatal("direct Spec decode failed but CompileSpecJSON succeeded")
			}
			if !reflect.DeepEqual(spec, before) {
				t.Fatal("failed direct Spec decode changed its destination")
			}
			return
		}
		encoded, marshalErr := json.Marshal(spec)
		if marshalErr != nil {
			t.Fatalf("a strictly decoded Spec failed to marshal: %v", marshalErr)
		}
		var roundTripped workflow.Spec
		if err := json.Unmarshal(encoded, &roundTripped); err != nil {
			t.Fatalf("a marshalled Spec failed to decode: %v", err)
		}
		again, marshalErr := json.Marshal(roundTripped)
		if marshalErr != nil || string(encoded) != string(again) {
			t.Fatalf("Spec encoding is not idempotent: %s, %s, %v", encoded, again, marshalErr)
		}
		_, typedErr := reg.CompileSpec(spec)
		if (documentErr == nil) != (typedErr == nil) {
			t.Fatalf(
				"Spec compile success differs after direct decode: bytes=%v, typed=%v",
				documentErr,
				typedErr,
			)
		}
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

		again, marshalErr := json.Marshal(store)
		if marshalErr != nil {
			t.Fatalf("a decoded Store failed to marshal: %v", marshalErr)
		}
		var second workflow.Store
		if err := json.Unmarshal(again, &second); err != nil {
			t.Fatalf("re-decoding a marshalled Store failed: %v", err)
		}
		third, marshalErr := json.Marshal(second)
		if marshalErr != nil {
			t.Fatalf("marshal is not stable: %v", marshalErr)
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
	f.Add([]byte(`{"version":3,"records":[]}`))
	f.Add([]byte(`{"version":3,"records":[{"id":"a","value":1}]}`))
	f.Add([]byte(`{"version":3,"records":[{"scope":[{"id":"iter","indexed":true}],"id":"el","value":{"n":1}}]}`))
	f.Add([]byte(`{"version":3,"records":[{"scope":[{"id":"loop","indexed":true}],"id":"loop","value":true},{"id":"route","value":"case"}]}`))
	f.Add([]byte(`{"version":3,"records":[{"scope":[{"id":"a/b"}],"id":"c","value":1},{"scope":[{"id":"a"}],"id":"b/c","value":2}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		journal := workflow.NewJournal()
		if err := json.Unmarshal(data, journal); err != nil {
			return
		}
		again, marshalErr := json.Marshal(journal)
		if marshalErr != nil {
			t.Fatalf("a decoded Journal failed to marshal: %v", marshalErr)
		}
		second := workflow.NewJournal()
		if unmarshalErr := json.Unmarshal(again, second); unmarshalErr != nil {
			t.Fatalf("re-decoding a marshalled Journal failed: %v", unmarshalErr)
		}
		if second.Len() != journal.Len() {
			t.Fatalf("round trip changed the record count: %d then %d", journal.Len(), second.Len())
		}
		third, stableErr := json.Marshal(second)
		if stableErr != nil {
			t.Fatalf("marshal is not stable: %v", stableErr)
		}
		if string(again) != string(third) {
			t.Fatalf("round trip is not idempotent:\n%s\n%s", again, third)
		}
	})
}

// FuzzResumeIdentityJSON keeps externally persisted waits and callback keys on
// the same strict, atomic, idempotent identity boundary as Journal replay.
func FuzzResumeIdentityJSON(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte(`{"id":"wait"}`),
		[]byte(`{"id":"wait","scope":[{"id":"items","indexed":true,"index":2}],"value":{"n":9007199254740993}}`),
		[]byte(`{"id":"wait","await":{"nodeID":"approval","path":"/output"}}`),
		[]byte(`{"id":"first","id":"second"}`),
		[]byte(`{"unknown":true}`),
		{'{', '"', 'i', 'd', '"', ':', '"', 0xff, '"', '}'},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		suspension := workflow.Suspension{ID: "sentinel", Value: "kept"}
		beforeSuspension := suspension
		if err := json.Unmarshal(data, &suspension); err != nil {
			if !reflect.DeepEqual(suspension, beforeSuspension) {
				t.Fatalf("failed Suspension decode changed destination: %#v", suspension)
			}
		} else {
			encoded, err := json.Marshal(suspension)
			if err != nil {
				t.Fatalf("accepted Suspension cannot be marshaled: %v", err)
			}
			var restored workflow.Suspension
			if decodeErr := json.Unmarshal(encoded, &restored); decodeErr != nil {
				t.Fatalf("encoded Suspension cannot be decoded: %v", decodeErr)
			}
			reencoded, err := json.Marshal(restored)
			if err != nil || !bytes.Equal(reencoded, encoded) {
				t.Fatalf("Suspension encoding is not idempotent: %s, %s, %v", encoded, reencoded, err)
			}
		}

		key := workflow.JournalKey{ID: "sentinel", Scope: []workflow.ScopeFrame{{ID: "outer"}}}
		beforeKey := key
		if err := json.Unmarshal(data, &key); err != nil {
			if !reflect.DeepEqual(key, beforeKey) {
				t.Fatalf("failed JournalKey decode changed destination: %#v", key)
			}
			return
		}
		encoded, err := json.Marshal(key)
		if err != nil {
			t.Fatalf("accepted JournalKey cannot be marshaled: %v", err)
		}
		var restored workflow.JournalKey
		if decodeErr := json.Unmarshal(encoded, &restored); decodeErr != nil {
			t.Fatalf("encoded JournalKey cannot be decoded: %v", decodeErr)
		}
		reencoded, err := json.Marshal(restored)
		if err != nil || !bytes.Equal(reencoded, encoded) {
			t.Fatalf("JournalKey encoding is not idempotent: %s, %s, %v", encoded, reencoded, err)
		}
	})
}
