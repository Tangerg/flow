package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/Tangerg/flow"
)

// ErrNilStep is returned when [Run] or a composite is given a nil Step or a nil
// [flow.NodeFunc] specialized to Store. It also matches [flow.ErrNilNode],
// because a Step is a Node specialized to Store. Test for it with [errors.Is].
var ErrNilStep error = nilStepError{}

type nilStepError struct{}

func (nilStepError) Error() string { return "nil step" }

func (nilStepError) Is(target error) bool { return target == flow.ErrNilNode }

// ErrInvalidStepID is returned when a named workflow step has an empty ID or
// one that cannot be represented as UTF-8 across workflow boundaries.
var ErrInvalidStepID = errors.New("invalid step ID")

// Stable sentinel errors returned by Store lookup, Journal mutation,
// registration, and definition validation. Use [errors.Is] rather than matching
// their text.
//
// Each text names only the condition. Every path that surfaces one wraps it in
// an error that already identifies this package and the failing location, so a
// prefix here would repeat that. A cause from another package keeps its own
// prefix, which is what distinguishes a kernel failure inside a workflow error.
var (
	ErrNotFound              = errors.New("value not found")
	ErrTypeMismatch          = errors.New("value type mismatch")
	ErrMaxDepth              = errors.New("maximum nesting depth exceeded")
	ErrInvalidRegistration   = errors.New("invalid registration")
	ErrDuplicateRegistration = errors.New("duplicate registration")
	ErrInvalidGraph          = errors.New("invalid graph")
	ErrDuplicateNode         = errors.New("duplicate graph node")
	ErrCycle                 = errors.New("graph cycle")
	ErrUnknownNode           = errors.New("unknown graph node")
	ErrUnknownNodeType       = errors.New("unknown node type")
	ErrIncompatibleType      = errors.New("incompatible value type")
	ErrInvalidSpec           = errors.New("invalid spec")
	ErrDuplicateStep         = errors.New("duplicate step")
	ErrJournalConflict       = errors.New("journal record already exists")
	ErrMissingPort           = errors.New("unwired input port")
	ErrUnknownPort           = errors.New("unknown input port")
	ErrUnknownOutlet         = errors.New("unknown outlet")
)

// MaxNestingDepth is the maximum nesting accepted at recursive workflow
// boundaries, including programmatic definitions, JSON values, Journal scopes,
// and expressions compiled by package expr. Keeping one limit prevents a value
// from passing one boundary only to exhaust the stack in the next one.
//
// Each boundary counts its own unit of recursion, so a value crossing several is
// bounded by whichever it reaches first. A nested [Spec] spends two JSON
// containers per level — the step object and its steps array — so one that will
// be persisted nests about half as deep as one held only in memory. Both are
// rejected cleanly rather than by exhausting the stack, which is what the shared
// limit is for.
const MaxNestingDepth = 1024

// Definition diagnostic fields are shared by strict validation, compilation,
// and error reporting. Most name serialized members; fieldJSON names the
// document boundary before an individual member can be identified. Keeping one
// vocabulary prevents those paths from describing the same location
// differently.
const (
	fieldBody          = "body"
	fieldBodyOutput    = "bodyOutput"
	fieldCases         = "cases"
	fieldConcurrency   = "concurrency"
	fieldCondition     = "condition"
	fieldConfig        = "config"
	fieldDependsOn     = "dependsOn"
	fieldID            = "id"
	fieldInput         = "input"
	fieldInputs        = "inputs"
	fieldJSON          = "json"
	fieldKind          = "kind"
	fieldMaxIterations = "maxIterations"
	fieldNodes         = "nodes"
	fieldResolver      = "resolver"
	fieldSteps         = "steps"
	fieldTrigger       = "trigger"
	fieldType          = "type"
	fieldWhen          = "when"
)

// StepOp identifies the operation that owns a workflow step error.
type StepOp string

// Workflow step operations reported by [StepError]. OpValidate covers
// definition and identity checks. OpBind covers an input-preparation error.
// OpRun covers the execution boundary, including cancellation, application
// execution, streaming, and output commit.
const (
	OpValidate StepOp = "validate"
	OpBind     StepOp = "bind"
	OpRun      StepOp = "run"
)

// StepError identifies the workflow step invocation and operation that failed.
// Scope is empty for definition errors, which have no execution instance, and
// contains the enclosing execution scopes for failures observed while running.
// Use [errors.As] to inspect ID, Scope, and Op and [errors.Is] to match Err.
type StepError struct {
	ID    string
	Scope []ScopeFrame
	Op    StepOp
	Err   error
}

func (s *StepError) Error() string {
	prefix := fmt.Sprintf("workflow: step %q", s.ID)
	if len(s.Scope) > 0 {
		prefix += " in " + formatScope(s.Scope)
	}
	return fmt.Sprintf("%s %s: %v", prefix, s.Op, s.Err)
}

// Unwrap returns the underlying validation, bind, or run error.
func (s *StepError) Unwrap() error { return s.Err }

// newStepError captures the structured identity of a failure observed during
// execution. Use [newValidationError] for an invalid definition, which has no
// invocation and therefore no execution scope.
func newStepError(ctx context.Context, id string, op StepOp, err error) *StepError {
	return &StepError{
		ID:    id,
		Scope: Scope(ctx),
		Op:    op,
		Err:   err,
	}
}

// newValidationError reports an invalid step definition. A definition is wrong
// everywhere it would run rather than at one execution scope, so the error
// carries no [Scope] even when a run discovers it. Naming that once keeps every
// step from restating the decision by omitting a field.
func newValidationError(id string, err error) *StepError {
	return &StepError{ID: id, Op: OpValidate, Err: err}
}

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

// RegistrationError reports an invalid or duplicate [Registry] entry. Every
// RegistrationError matches [ErrInvalidRegistration]; Err remains available
// through [errors.Is] for a more specific cause such as
// [ErrDuplicateRegistration].
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

// Is reports the stable category shared by all registration errors.
func (r *RegistrationError) Is(target error) bool { return target == ErrInvalidRegistration }

// Unwrap returns the specific registration failure. The broader
// [ErrInvalidRegistration] category is provided by Is.
func (r *RegistrationError) Unwrap() error { return r.Err }

// GraphError identifies the graph node and field associated with a validation
// or compilation error. Path is an RFC 6901 JSON Pointer to the containing
// GraphNode and is empty for a whole-graph error. NodeID may be empty when the
// invalid node has no ID; Path still identifies its declaration. Every
// GraphError matches [ErrInvalidGraph]; Err remains available through
// [errors.Is] and [errors.As] for the specific cause.
type GraphError struct {
	Path   string
	NodeID string
	Field  string
	Err    error
}

func (g *GraphError) Error() string {
	prefix := "workflow: graph"
	if g.Path != "" {
		prefix += fmt.Sprintf(" at %q", g.Path)
	}
	if g.NodeID != "" {
		prefix += fmt.Sprintf(" node %q", g.NodeID)
	}
	if g.Field != "" {
		prefix += " field " + g.Field
	}
	return fmt.Sprintf("%s: %v", prefix, g.Err)
}

// Is reports the stable category shared by all graph validation and compilation
// errors. Specific causes are matched through Unwrap.
func (g *GraphError) Is(target error) bool { return target == ErrInvalidGraph }

// Unwrap returns the underlying graph error.
func (g *GraphError) Unwrap() error { return g.Err }

// SpecError identifies the nested specification and field associated with a
// validation or compilation error. Path is an RFC 6901 JSON Pointer from the
// root to the containing Spec; it is empty for the root. Field names the member
// within that Spec. Every SpecError matches [ErrInvalidSpec]; Err remains
// available through [errors.Is] and [errors.As] for the specific cause.
type SpecError struct {
	Path  string
	Kind  Kind
	ID    string
	Field string
	Err   error
}

func (s *SpecError) Error() string {
	prefix := "workflow: spec"
	if s.Path != "" {
		prefix += fmt.Sprintf(" at %q", s.Path)
	}
	if s.Kind != "" {
		prefix += " " + string(s.Kind)
	}
	if s.ID != "" {
		prefix += fmt.Sprintf(" %q", s.ID)
	}
	if s.Field != "" {
		prefix += " field " + s.Field
	}
	return fmt.Sprintf("%s: %v", prefix, s.Err)
}

// Is reports the stable category shared by all specification validation and
// compilation errors. Specific causes are matched through Unwrap.
func (s *SpecError) Is(target error) bool { return target == ErrInvalidSpec }

// Unwrap returns the underlying specification error.
func (s *SpecError) Unwrap() error { return s.Err }

// locateSpecError prefixes a recursive child location without mutating an
// error that may already have escaped from another compile operation. Encoded
// JSON Pointers compose by concatenation because each non-root pointer begins
// with '/'.
func locateSpecError(err error, segments ...string) error {
	// Recursive Spec validation and compilation return their boundary error
	// directly. Looking through an arbitrary wrapper would discard that wrapper
	// when the located copy is returned, so only the direct contract is valid.
	//
	//nolint:errorlint // errors.As would accept a shape this prefixer cannot preserve.
	specErr, ok := err.(*SpecError)
	if !ok {
		return err
	}
	located := *specErr
	located.Path = pointerPath(segments).encode() + located.Path
	return &located
}
