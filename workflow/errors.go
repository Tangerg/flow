package workflow

import (
	"errors"
	"fmt"
)

// ErrNilStep is returned when [Run] or a composite is given a nil [Step]. Test
// for it with [errors.Is].
var ErrNilStep = errors.New("workflow: nil step")

// ErrInvalidStepID is returned when a named workflow step has an empty ID.
var ErrInvalidStepID = errors.New("workflow: empty step ID")

// Stable sentinel errors returned by Store lookup, recursive input boundaries,
// Journal mutation, registration, and graph validation. Use [errors.Is] rather
// than matching their text.
var (
	ErrNotFound              = errors.New("workflow: value not found")
	ErrTypeMismatch          = errors.New("workflow: value type mismatch")
	ErrMaxDepth              = errors.New("workflow: maximum nesting depth exceeded")
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
	ErrJournalConflict       = errors.New("workflow: journal record already exists")
	ErrMissingPort           = errors.New("workflow: unwired input port")
	ErrUnknownPort           = errors.New("workflow: unknown input port")
	ErrDuplicatePort         = errors.New("workflow: duplicate input port")
	ErrUnknownOutlet         = errors.New("workflow: unknown outlet")
)

// MaxNestingDepth is the maximum nesting accepted at recursive workflow
// boundaries, including JSON values and Journal scope paths. Keeping one limit
// prevents a document from passing one boundary only to exhaust the stack in
// the next one.
const MaxNestingDepth = 1024

// StepOp identifies the phase of a workflow step that failed.
type StepOp string

// Workflow step phases reported by [StepError].
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

func (s *StepError) Error() string {
	return fmt.Sprintf("workflow: step %q %s: %v", s.ID, s.Op, s.Err)
}

// Unwrap returns the underlying validation, bind, or run error.
func (s *StepError) Unwrap() error { return s.Err }

// RefError reports a failed typed lookup in a [Store]. Want is the requested
// type; Got is empty when the reference is missing or contains an untyped nil.
type RefError struct {
	Ref  Ref
	Want string
	Got  string
	Err  error
}

func (r *RefError) Error() string {
	switch {
	case errors.Is(r.Err, ErrNotFound):
		return fmt.Sprintf("workflow: ref %s: %v", r.Ref, r.Err)
	case r.Got == "":
		return fmt.Sprintf("workflow: ref %s: %v: got <nil>, want %s", r.Ref, r.Err, r.Want)
	default:
		return fmt.Sprintf("workflow: ref %s: %v: got %s, want %s", r.Ref, r.Err, r.Got, r.Want)
	}
}

// Unwrap returns [ErrNotFound] or [ErrTypeMismatch].
func (r *RefError) Unwrap() error { return r.Err }

// RegistrationError reports an invalid or duplicate [Registry] entry.
type RegistrationError struct {
	Kind string
	Name string
	Err  error
}

func (r *RegistrationError) Error() string {
	if r.Name == "" {
		return fmt.Sprintf("workflow: register %s: %v", r.Kind, r.Err)
	}
	return fmt.Sprintf("workflow: register %s %q: %v", r.Kind, r.Name, r.Err)
}

// Unwrap returns [ErrInvalidRegistration] or [ErrDuplicateRegistration].
func (r *RegistrationError) Unwrap() error { return r.Err }

// GraphError identifies the graph node and field associated with a validation
// or compilation error. NodeID and Field may be empty for whole-graph errors.
type GraphError struct {
	NodeID string
	Field  string
	Err    error
}

func (g *GraphError) Error() string {
	switch {
	case g.NodeID != "" && g.Field != "":
		return fmt.Sprintf("workflow: graph node %q field %s: %v", g.NodeID, g.Field, g.Err)
	case g.NodeID != "":
		return fmt.Sprintf("workflow: graph node %q: %v", g.NodeID, g.Err)
	case g.Field != "":
		return fmt.Sprintf("workflow: graph field %s: %v", g.Field, g.Err)
	default:
		return fmt.Sprintf("workflow: graph: %v", g.Err)
	}
}

// Unwrap returns the underlying graph error.
func (g *GraphError) Unwrap() error { return g.Err }

// SpecError identifies the nested specification and field associated with a
// validation or compilation error.
type SpecError struct {
	Kind  SpecKind
	ID    string
	Field string
	Err   error
}

func (s *SpecError) Error() string {
	prefix := "workflow: spec"
	if s.Kind != "" {
		prefix += " " + string(s.Kind)
	}
	if s.ID != "" {
		prefix += fmt.Sprintf(" %q", s.ID)
	}
	if s.Field != "" {
		prefix += " field " + s.Field
	}
	return prefix + ": " + s.Err.Error()
}

// Unwrap returns the underlying specification error.
func (s *SpecError) Unwrap() error { return s.Err }
