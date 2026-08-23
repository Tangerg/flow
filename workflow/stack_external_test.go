package workflow_test

import (
	"os"
	"os/exec"
	"regexp"
	"runtime/debug"
	"testing"
)

const boundedStackChild = "FLOW_BOUNDED_STACK_TEST"

// withBoundedStack runs test in a subprocess whose deliberately small stack
// turns an accidental per-item recursive walk into a deterministic failure.
// The production contract is stack growth independent of an application tree's
// size; the limit is only the test instrument that proves it.
func withBoundedStack(t *testing.T, test func()) {
	t.Helper()
	if os.Getenv(boundedStackChild) == t.Name() {
		debug.SetMaxStack(256 << 10)
		test()
		return
	}

	//nolint:gosec // Re-executes this test binary with a quoted testing-owned name.
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
	command.Env = append(os.Environ(), boundedStackChild+"="+t.Name())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("operation exhausted a bounded stack: %v\n%s", err, output)
	}
}
