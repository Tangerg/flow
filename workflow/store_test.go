package workflow_test

import (
	"testing"

	"github.com/Tangerg/flow/workflow"
)

func TestStore_WithAndGet(t *testing.T) {
	s := workflow.NewStore().With("n1", "output", 42)

	v, ok := s.Get("n1", "output")
	if !ok || v.(int) != 42 {
		t.Fatalf("Get = %v, %v; want 42, true", v, ok)
	}
}

func TestStore_immutable(t *testing.T) {
	s1 := workflow.NewStore().With("n", "output", 1)
	s2 := s1.With("n", "output", 2)

	if v, _ := s1.Get("n", "output"); v.(int) != 1 {
		t.Fatalf("original store mutated: got %v, want 1", v)
	}
	if v, _ := s2.Get("n", "output"); v.(int) != 2 {
		t.Fatalf("new store wrong: got %v, want 2", v)
	}
}

func TestStore_sharesUntouchedNodes(t *testing.T) {
	s1 := workflow.NewStore().With("a", "output", 1)
	s2 := s1.With("b", "output", 2)

	// Writing b must not disturb a.
	if v, ok := s2.Get("a", "output"); !ok || v.(int) != 1 {
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
	s := workflow.NewStore().With("n", "output", nested)

	v, ok := s.Get("n", "output.items.1.name")
	if !ok || v.(string) != "b" {
		t.Fatalf("path Get = %v, %v; want b, true", v, ok)
	}
}

func TestStore_missing(t *testing.T) {
	s := workflow.NewStore().With("n", "output", 1)

	if _, ok := s.Get("n", "nope"); ok {
		t.Fatal("expected miss on unknown key")
	}
	if _, ok := s.Get("other", "output"); ok {
		t.Fatal("expected miss on unknown node")
	}
	if _, ok := s.Get("n", "output.deep"); ok {
		t.Fatal("expected miss walking into a non-container")
	}
}
