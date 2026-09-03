package workflow

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"

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
	ErrJournalConflict       = errors.New("duplicate journal record")
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

// Diagnostic name kinds are the human-readable half of the same vocabulary. A
// concept is checked in more than one place -- a definition, the JSON text
// carrying it, and sometimes the compiled step -- and must describe itself the
// same way in each, so each kind is written once here rather than at every
// check.
const (
	nameBranchCase   = "branch case name"
	nameCondition    = "condition name"
	nameDependency   = "dependency ID"
	nameGateOutlet   = "gate outlet"
	nameGateSource   = "gate source node ID"
	nameInputPort    = "input port name"
	nameKind         = "kind"
	nameNodeID       = "node ID"
	nameNodeType     = "node type"
	nameOutlet       = "outlet name"
	namePath         = "path"
	nameResolver     = "resolver name"
	nameScopeFrameID = "scope frame ID"
	nameStepID       = "step ID"
	nameSubgraphSeed = "subgraph seed ID"
	nameTrigger      = "trigger"
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
	return (ownedError{root: s}).format()
}

func (s *StepError) appendErrorPrefix(message *strings.Builder) (bool, error) {
	if s == nil {
		return false, nil
	}
	beginWorkflowError(message)
	message.WriteString("step ")
	message.WriteString(strconv.Quote(s.ID))
	if len(s.Scope) > 0 {
		message.WriteString(" in ")
		message.WriteString(formatScope(s.Scope))
	}
	message.WriteByte(' ')
	message.WriteString(string(s.Op))
	message.WriteString(": ")
	return true, s.Err
}

func (s *StepError) Unwrap() error {
	if s == nil {
		return nil
	}
	return s.Err
}

// withCause keeps this location and replaces what it wraps. Its caller has
// classified a concrete StepError, so the receiver is non-nil, and it is
// restating why a step failed rather than copying a tree: cloning the old cause
// here would compute a copy the next statement discards, and [ownedError.clone]
// already owns copying a StepError frame for the callers who do need it. Scope
// is still copied, because the result escapes to a caller who may mutate it.
func (s *StepError) withCause(err error) *StepError {
	clone := *s
	clone.Scope = slices.Clone(s.Scope)
	clone.Err = err
	return &clone
}

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
	return (ownedError{root: r}).format()
}

func (r *RefError) appendErrorPrefix(message *strings.Builder) (bool, error) {
	if r == nil {
		return false, nil
	}
	beginWorkflowError(message)
	message.WriteString("ref ")
	message.WriteString(r.Ref.String())
	message.WriteString(": ")
	return true, r.Err
}

func (r *RefError) appendMismatch(message *strings.Builder) {
	message.WriteString(": got ")
	if r.Got == "" {
		message.WriteString("<nil>")
	} else {
		message.WriteString(r.Got)
	}
	message.WriteString(", want ")
	message.WriteString(r.Want)
}

// Unwrap returns [ErrNotFound] or [ErrTypeMismatch].
func (r *RefError) Unwrap() error {
	if r == nil {
		return nil
	}
	return r.Err
}

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
	return (ownedError{root: r}).format()
}

func (r *RegistrationError) appendErrorPrefix(message *strings.Builder) (bool, error) {
	if r == nil {
		return false, nil
	}
	beginWorkflowError(message)
	message.WriteString("register ")
	message.WriteString(r.Kind)
	if r.Name != "" {
		message.WriteByte(' ')
		message.WriteString(strconv.Quote(r.Name))
	}
	message.WriteString(": ")
	return true, r.Err
}

// Is reports the stable category shared by all registration errors.
func (r *RegistrationError) Is(target error) bool {
	return r != nil && target == ErrInvalidRegistration
}

// Unwrap returns the specific registration failure. The broader
// [ErrInvalidRegistration] category is provided by Is.
func (r *RegistrationError) Unwrap() error {
	if r == nil {
		return nil
	}
	return r.Err
}

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
	return (ownedError{root: g}).format()
}

func (g *GraphError) appendErrorPrefix(message *strings.Builder) (bool, error) {
	if g == nil {
		return false, nil
	}
	beginWorkflowError(message)
	message.WriteString("graph")
	if g.Path != "" {
		message.WriteString(" at ")
		message.WriteString(strconv.Quote(g.Path))
	}
	if g.NodeID != "" {
		message.WriteString(" node ")
		message.WriteString(strconv.Quote(g.NodeID))
	}
	if g.Field != "" {
		message.WriteString(" field ")
		message.WriteString(g.Field)
	}
	message.WriteString(": ")
	return true, g.Err
}

// Is reports the stable category shared by all graph validation and compilation
// errors. Specific causes are matched through Unwrap.
func (g *GraphError) Is(target error) bool {
	return g != nil && target == ErrInvalidGraph
}

func (g *GraphError) Unwrap() error {
	if g == nil {
		return nil
	}
	return g.Err
}

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
	return (ownedError{root: s}).format()
}

func (s *SpecError) appendErrorPrefix(message *strings.Builder) (bool, error) {
	if s == nil {
		return false, nil
	}
	beginWorkflowError(message)
	message.WriteString("spec")
	if s.Path != "" {
		message.WriteString(" at ")
		message.WriteString(strconv.Quote(s.Path))
	}
	if s.Kind != "" {
		message.WriteByte(' ')
		message.WriteString(string(s.Kind))
	}
	if s.ID != "" {
		message.WriteByte(' ')
		message.WriteString(strconv.Quote(s.ID))
	}
	if s.Field != "" {
		message.WriteString(" field ")
		message.WriteString(s.Field)
	}
	message.WriteString(": ")
	return true, s.Err
}

// Is reports the stable category shared by all specification validation and
// compilation errors. Specific causes are matched through Unwrap.
func (s *SpecError) Is(target error) bool {
	return s != nil && target == ErrInvalidSpec
}

func (s *SpecError) Unwrap() error {
	if s == nil {
		return nil
	}
	return s.Err
}

// errorPrefixAppender is the private capability shared by the location errors
// this package owns. Its unexported method prevents an application error from
// becoming part of the formatting protocol by accident.
type errorPrefixAppender interface {
	appendErrorPrefix(message *strings.Builder) (valid bool, next error)
}

// detailError is a package-owned location fragment inside a structured error,
// not an independently surfaced boundary. It deliberately does not add the
// workflow qualifier: the StepError, GraphError, SpecError, RegistrationError,
// or RefError that owns the operation supplies it. Keeping the fragment in the
// same private formatting protocol preserves one qualifier even when a lower-
// layer location such as flow.IndexError lies between the two workflow errors.
type detailError struct {
	detail string
	err    error
}

func (d *detailError) Error() string { return (ownedError{root: d}).format() }

func (d *detailError) Unwrap() error { return d.err }

func (d *detailError) appendErrorPrefix(message *strings.Builder) (bool, error) {
	message.WriteString(d.detail)
	message.WriteString(": ")
	return true, d.err
}

// factoryBuildError locates a typed Factory's application construction error.
// It remains private because callers branch on the cause, not on this prose
// location; participating in the package-owned formatter keeps a direct factory
// call and a Graph- or Spec-wrapped call on the same one-prefix path.
type factoryBuildError struct {
	err error
}

func (f *factoryBuildError) Error() string { return (ownedError{root: f}).format() }

func (f *factoryBuildError) Unwrap() error { return f.err }

func (f *factoryBuildError) appendErrorPrefix(message *strings.Builder) (bool, error) {
	beginWorkflowError(message)
	message.WriteString("build node: ")
	return true, f.err
}

// beginWorkflowError names this package at the outermost exact wrapper. Inner
// locations remain structured in the error chain but do not repeat the package
// qualifier in its flattened text.
func beginWorkflowError(message *strings.Builder) {
	if message.Len() == 0 {
		message.WriteString("workflow: ")
	}
}

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
