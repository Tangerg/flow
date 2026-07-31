package workflow_test

import (
	"testing"

	"github.com/Tangerg/flow/workflow"
)

func TestDefaultInput(t *testing.T) {
	want := workflow.Output("source")
	inputs := workflow.DefaultInput(want)

	got, ok := inputs.Default()
	if !ok || got != want {
		t.Fatalf("Default() = %v, %v; want %v, true", got, ok, want)
	}
	if len(inputs) != 1 {
		t.Fatalf("len(DefaultInput) = %d; want 1", len(inputs))
	}
}
