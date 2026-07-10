package workflow

import (
	"errors"
	"fmt"
)

// ErrNilStep is returned when a nil [Step] is run inside a composite (Loop,
// Parallel, Iteration). Test for it with [errors.Is].
var ErrNilStep = errors.New("workflow: nil step")

// ErrInvalidStepID is returned when a step that writes to the Store has an
// empty ID.
var ErrInvalidStepID = errors.New("workflow: empty step ID")

// StepError identifies the workflow step and operation that failed. Use
// [errors.As] to inspect ID and Op and [errors.Is] to match Err.
type StepError struct {
	ID  string
	Op  string
	Err error
}

func (e *StepError) Error() string {
	return fmt.Sprintf("workflow: step %q %s: %v", e.ID, e.Op, e.Err)
}

// Unwrap returns the underlying bind or run error.
func (e *StepError) Unwrap() error { return e.Err }
