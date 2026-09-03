package flow_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"runtime/debug"
	"testing"
)

const boundedStackChild = "FLOW_BOUNDED_STACK_TEST"

// boundedStackRan is what the child prints after the body returns, and the
// parent requires it. Without it a passing run cannot be told from a child that
// selected no test at all: `go test -run` exits 0 when its pattern matches
// nothing, so renaming one of these tests would quietly turn its guard into a
// subprocess that proves the operation never ran.
const boundedStackRan = "bounded stack body completed"

// withBoundedStack runs test in a subprocess whose deliberately small stack
// turns an accidental per-wrapper recursive walk into a deterministic failure.
// The contract is stack growth independent of how deep a caller's error tree
// runs; the limit is only the instrument that proves it.
//
// The workflow package keeps its own copy of this helper. They cannot be one:
// a test file is not importable, and moving it to a package like internal/ctxtest
// would put the child branch -- which runs only inside the subprocess, whose
// coverage counters are discarded -- into a measured package, where the gate
// requires every statement to be executed.
func withBoundedStack(t *testing.T, test func()) {
	t.Helper()
	if os.Getenv(boundedStackChild) == t.Name() {
		debug.SetMaxStack(256 << 10)
		test()
		fmt.Println(boundedStackRan)
		return
	}

	//nolint:gosec // Re-executes this test binary with a quoted testing-owned name.
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
	command.Env = append(os.Environ(), boundedStackChild+"="+t.Name())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("formatting exhausted a bounded stack: %v\n%s", err, output)
	}
	if !bytes.Contains(output, []byte(boundedStackRan)) {
		t.Fatalf("the subprocess ran no bounded-stack body:\n%s", output)
	}
}
