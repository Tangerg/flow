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

// Stable sentinel errors returned by Store lookup, registration, and graph
// validation. Use [errors.Is] rather than matching their text.
var (
	ErrNotFound              = errors.New("workflow: value not found")
	ErrTypeMismatch          = errors.New("workflow: value type mismatch")
	ErrInvalidRegistration   = errors.New("workflow: invalid registration")
	ErrDuplicateRegistration = errors.New("workflow: duplicate registration")
	ErrInvalidGraph          = errors.New("workflow: invalid graph")
	ErrDuplicateNode         = errors.New("workflow: duplicate graph node")
	ErrCycle                 = errors.New("workflow: graph cycle")
	ErrUnknownNode           = errors.New("workflow: unknown graph node")
	ErrUnknownNodeType       = errors.New("workflow: unknown node type")
	ErrIncompatibleType      = errors.New("workflow: incompatible value type")
	ErrInvalidSpec           = errors.New("workflow: invalid spec")
	ErrDuplicateStep         = errors.New("workflow: duplicate step")
)

// StepOp identifies the phase of a [Leaf] that failed.
type StepOp string

// Leaf execution phases reported by [StepError].
const (
	OpValidate StepOp = "validate"
	OpBind     StepOp = "bind"
	OpRun      StepOp = "run"
)

// StepError identifies the workflow step and operation that failed. Use
// [errors.As] to inspect ID and Op and [errors.Is] to match Err.
type StepError struct {
	ID  string
	Op  StepOp
	Err error
}

func (e *StepError) Error() string {
	return fmt.Sprintf("workflow: step %q %s: %v", e.ID, e.Op, e.Err)
}

// Unwrap returns the underlying bind or run error.
func (e *StepError) Unwrap() error { return e.Err }

// RefError reports a failed typed lookup in a [Store]. Want is the requested
// type; Got is empty when the reference is missing or contains an untyped nil.
type RefError struct {
	Ref  Ref
	Want string
	Got  string
	Err  error
}

func (e *RefError) Error() string {
	switch {
	case errors.Is(e.Err, ErrNotFound):
		return fmt.Sprintf("workflow: ref %s: %v", e.Ref, e.Err)
	case e.Got == "":
		return fmt.Sprintf("workflow: ref %s: %v: got <nil>, want %s", e.Ref, e.Err, e.Want)
	default:
		return fmt.Sprintf("workflow: ref %s: %v: got %s, want %s", e.Ref, e.Err, e.Got, e.Want)
	}
}

// Unwrap returns [ErrNotFound] or [ErrTypeMismatch].
func (e *RefError) Unwrap() error { return e.Err }

// RegistrationError reports an invalid or duplicate [Registry] entry.
type RegistrationError struct {
	Kind string
	Name string
	Err  error
}

func (e *RegistrationError) Error() string {
	if e.Name == "" {
		return fmt.Sprintf("workflow: register %s: %v", e.Kind, e.Err)
	}
	return fmt.Sprintf("workflow: register %s %q: %v", e.Kind, e.Name, e.Err)
}

// Unwrap returns [ErrInvalidRegistration] or [ErrDuplicateRegistration].
func (e *RegistrationError) Unwrap() error { return e.Err }

// GraphError identifies the graph node and field associated with a validation
// or compilation error. NodeID and Field may be empty for whole-graph errors.
type GraphError struct {
	NodeID string
	Field  string
	Err    error
}

func (e *GraphError) Error() string {
	switch {
	case e.NodeID != "" && e.Field != "":
		return fmt.Sprintf("workflow: graph node %q field %s: %v", e.NodeID, e.Field, e.Err)
	case e.NodeID != "":
		return fmt.Sprintf("workflow: graph node %q: %v", e.NodeID, e.Err)
	case e.Field != "":
		return fmt.Sprintf("workflow: graph field %s: %v", e.Field, e.Err)
	default:
		return fmt.Sprintf("workflow: graph: %v", e.Err)
	}
}

// Unwrap returns the underlying graph error.
func (e *GraphError) Unwrap() error { return e.Err }

// SpecError identifies the nested specification and field associated with a
// validation or compilation error.
type SpecError struct {
	Kind  SpecKind
	ID    string
	Field string
	Err   error
}

func (e *SpecError) Error() string {
	prefix := "workflow: spec"
	if e.Kind != "" {
		prefix += " " + string(e.Kind)
	}
	if e.ID != "" {
		prefix += fmt.Sprintf(" %q", e.ID)
	}
	if e.Field != "" {
		prefix += " field " + e.Field
	}
	return prefix + ": " + e.Err.Error()
}

// Unwrap returns the underlying specification error.
func (e *SpecError) Unwrap() error { return e.Err }
