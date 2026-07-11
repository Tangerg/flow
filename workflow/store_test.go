package workflow_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Tangerg/flow/workflow"
)

func TestRef_helpers(t *testing.T) {
	ref := workflow.Output("step").Child("items.0")
	if ref != workflow.At("step", "output.items.0") || ref.String() != "step.output.items.0" {
		t.Fatalf("ref = %#v (%s)", ref, ref)
	}
	if got := ref.Child(""); got != ref {
		t.Fatalf("Child(empty) = %#v, want %#v", got, ref)
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

func TestStore_path(t *testing.T) {
	nested := map[string]any{
		"items": []any{
			map[string]any{"name": "a"},
			map[string]any{"name": "b"},
		},
	}
	s := workflow.NewStore().WithOutput("n", nested)

	v, ok := s.Lookup(workflow.At("n", "output.items.1.name"))
	if !ok || v.(string) != "b" {
		t.Fatalf("path Get = %v, %v; want b, true", v, ok)
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
	if _, ok := s.Lookup(workflow.At("n", "output.deep")); ok {
		t.Fatal("expected miss walking into a non-container")
	}
}

func TestStore_JSONRoundTrip(t *testing.T) {
	original := workflow.NewStore().
		With("a", "output", map[string]any{"items": []any{"x", true}}).
		With("b", "output", 42)
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded workflow.Store
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got, ok := decoded.Lookup(workflow.At("a", "output.items.1")); !ok || got != true {
		t.Fatalf("nested value = %v, %v", got, ok)
	}
	if got, ok := decoded.Lookup(workflow.At("b", "output")); !ok || got != float64(42) {
		t.Fatalf("number = %T(%v), %v; want float64(42)", got, got, ok)
	}
}

func TestStore_UnmarshalIsAtomic(t *testing.T) {
	store := workflow.NewStore().WithOutput("old", 1)
	if err := json.Unmarshal([]byte(`{"new":{"output":1e10000}}`), &store); err == nil {
		t.Fatal("expected value decode error")
	}
	if got, ok := store.Lookup(workflow.At("old", "output")); !ok || got != 1 {
		t.Fatalf("store changed after failed decode: %v, %v", got, ok)
	}
}

func TestStore_MarshalReportsCell(t *testing.T) {
	store := workflow.NewStore().WithOutput("bad", func() {})
	_, err := json.Marshal(store)
	if err == nil || !strings.Contains(err.Error(), "bad.output") {
		t.Fatalf("err = %v; want cell path", err)
	}
}
