package workflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

// errorTree is the standard Go error-tree shape with an iterative matcher.
// Application validators and factories may return arbitrarily deep joined
// errors without crossing a workflow nesting boundary, while [errors.Is] walks
// Unwrap() []error branches recursively. Keeping this representation private
// avoids changing the public error protocol merely to make engine boundaries
// independent of application-tree depth.
type errorTree struct {
	root error
}

var standardJoinErrorTypes = [...]reflect.Type{
	standardJoinErrorType(errors.Join(ErrInvalidSpec)),
	standardJoinErrorType(errors.Join(ErrInvalidSpec, ErrInvalidGraph)),
}

func standardJoinErrorType(err error) reflect.Type {
	if _, ok := err.(interface{ Unwrap() []error }); !ok {
		return nil
	}
	return reflect.TypeOf(err)
}

// standardJoinChildren recognizes only the concrete multi-error produced by
// [errors.Join]. A caller-defined Unwrap() []error remains an opaque application
// error: interpreting its branches could change presentation or ownership rules
// chosen by that type. Deriving the type from the constructor avoids depending
// on an unexported standard-library name.
func standardJoinChildren(err error) ([]error, bool) {
	typeOf := reflect.TypeOf(err)
	if typeOf == nil ||
		(typeOf != standardJoinErrorTypes[0] && typeOf != standardJoinErrorTypes[1]) {
		return nil, false
	}
	// The type table contains only values that passed this exact assertion.
	joined := err.(interface{ Unwrap() []error }) //nolint:forcetypeassert // Proven by the type table.
	return joined.Unwrap(), true
}

// matches has the ordering and matching rules of [errors.Is]: visit each node
// before its children, follow depth first from left to right, compare a
// comparable target directly, then honor a node's shallow Is method. The only
// difference is the explicit branch worklist.
func (e errorTree) matches(target error) bool {
	if e.root == nil || target == nil {
		//nolint:errorlint // Mirrors errors.Is's nil identity before tree traversal.
		return e.root == target
	}
	targetComparable := reflect.TypeOf(target).Comparable()
	pending := []error{e.root}
	for len(pending) > 0 {
		last := len(pending) - 1
		current := pending[last]
		pending = pending[:last]
		for current != nil {
			//nolint:errorlint // This is one node of the explicit errors.Is traversal.
			if targetComparable && current == target {
				return true
			}
			matcher, ok := current.(interface{ Is(target error) bool })
			if ok && matcher.Is(target) {
				return true
			}
			//nolint:errorlint // The explicit worklist makes multi-error traversal stack-safe.
			switch wrapped := current.(type) {
			case interface{ Unwrap() error }:
				current = wrapped.Unwrap()
			case interface{ Unwrap() []error }:
				for _, child := range slices.Backward(wrapped.Unwrap()) {
					if child != nil {
						pending = append(pending, child)
					}
				}
				current = nil
			default:
				current = nil
			}
		}
	}
	return false
}

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

// Unwrap returns the underlying validation, bind, or run error.
func (s *StepError) Unwrap() error {
	if s == nil {
		return nil
	}
	return s.Err
}

// clone returns an independently mutable location wrapper. Callers have already
// classified a concrete StepError, so the receiver is non-nil. A direct workflow
// wrapper in the cause is cloned too; an application-defined cause remains
// borrowed and follows Go's immutable-error convention.
func (s *StepError) clone() *StepError {
	clone := *s
	clone.Scope = slices.Clone(s.Scope)
	clone.Err = (ownedError{root: s.Err}).clone()
	return &clone
}

// ownedError is the engine-owned view of one error tree. It knows how to copy
// and render only the mutable workflow locations, root flow locations, and the
// exact branching shape produced by [errors.Join]. Every other wrapper is an
// opaque application value governed by Go's immutable-error convention.
type ownedError struct {
	root error
}

type errorCloneFrame struct {
	wrapper error
}

type errorCloneFrames []errorCloneFrame

type errorCloneNode struct {
	frames   errorCloneFrames
	children []int
	result   error
}

type errorCloner struct {
	nodes  []errorCloneNode
	cursor int
}

type errorCloneResult struct {
	frame    errorCloneFrame
	next     error
	owned    bool
	terminal bool
}

// clone copies the complete owned structure without looking through an
// application wrapper. Its worklists keep both linear and joined depth off the
// call stack; exported location errors need not originate in a bounded Spec.
func (e ownedError) clone() error {
	cloner := errorCloner{nodes: []errorCloneNode{{result: e.root}}}
	return cloner.clone()
}

func (c *errorCloner) clone() error {
	for c.expandNext() {
	}
	for index := range slices.Backward(c.nodes) {
		c.rebuild(index)
	}
	return c.nodes[0].result
}

func (c *errorCloner) expandNext() bool {
	if c.cursor >= len(c.nodes) {
		return false
	}
	c.expand(c.cursor)
	c.cursor++
	return true
}

func (c *errorCloner) expand(index int) {
	frames, terminal := c.peel(c.nodes[index].result)
	c.nodes[index].frames = frames
	children, joined := standardJoinChildren(terminal)
	if !joined {
		c.nodes[index].result = frames.attach(terminal)
		return
	}

	c.nodes[index].result = nil
	c.nodes[index].children = make([]int, len(children))
	for childIndex, child := range children {
		c.nodes[index].children[childIndex] = len(c.nodes)
		c.nodes = append(c.nodes, errorCloneNode{result: child})
	}
}

func (c *errorCloner) rebuild(index int) {
	node := &c.nodes[index]
	if len(node.children) == 0 {
		return
	}
	children := make([]error, len(node.children))
	for childIndex, nodeIndex := range node.children {
		children[childIndex] = c.nodes[nodeIndex].result
	}
	node.result = node.frames.attach(errors.Join(children...))
}

// peel copies one exact linear chain and returns the first error outside it.
// [errors.As] would cross an application wrapper and is therefore deliberately
// not used here.
func (c *errorCloner) peel(err error) (errorCloneFrames, error) {
	var frames errorCloneFrames
	for {
		result := c.cloneWorkflowFrame(err)
		if !result.owned {
			result = c.cloneCompositionFrame(err)
		}
		if !result.owned || result.terminal {
			return frames, result.next
		}
		frames = append(frames, result.frame)
		err = result.next
	}
}

//nolint:errorlint // Exact wrapper identity determines ownership.
func (*errorCloner) cloneWorkflowFrame(err error) errorCloneResult {
	switch current := err.(type) {
	case *StepError:
		if current == nil {
			return errorCloneResult{next: err, owned: true, terminal: true}
		}
		copied := *current
		copied.Scope = slices.Clone(current.Scope)
		copied.Err = nil
		return errorCloneResult{frame: errorCloneFrame{wrapper: &copied}, next: current.Err, owned: true}
	case *RefError:
		if current == nil {
			return errorCloneResult{next: err, owned: true, terminal: true}
		}
		copied := *current
		copied.Err = nil
		return errorCloneResult{frame: errorCloneFrame{wrapper: &copied}, next: current.Err, owned: true}
	case *RegistrationError:
		if current == nil {
			return errorCloneResult{next: err, owned: true, terminal: true}
		}
		copied := *current
		copied.Err = nil
		return errorCloneResult{frame: errorCloneFrame{wrapper: &copied}, next: current.Err, owned: true}
	case *GraphError:
		if current == nil {
			return errorCloneResult{next: err, owned: true, terminal: true}
		}
		copied := *current
		copied.Err = nil
		return errorCloneResult{frame: errorCloneFrame{wrapper: &copied}, next: current.Err, owned: true}
	case *SpecError:
		if current == nil {
			return errorCloneResult{next: err, owned: true, terminal: true}
		}
		copied := *current
		copied.Err = nil
		return errorCloneResult{frame: errorCloneFrame{wrapper: &copied}, next: current.Err, owned: true}
	default:
		return errorCloneResult{next: err}
	}
}

//nolint:errorlint // Exact wrapper identity determines ownership.
func (*errorCloner) cloneCompositionFrame(err error) errorCloneResult {
	switch current := err.(type) {
	case *flow.IndexError:
		if current == nil {
			return errorCloneResult{next: err, owned: true, terminal: true}
		}
		copied := *current
		copied.Err = nil
		return errorCloneResult{frame: errorCloneFrame{wrapper: &copied}, next: current.Err, owned: true}
	case *flow.CaseError:
		if current == nil {
			return errorCloneResult{next: err, owned: true, terminal: true}
		}
		copied := *current
		copied.Err = nil
		return errorCloneResult{frame: errorCloneFrame{wrapper: &copied}, next: current.Err, owned: true}
	case *detailError:
		if current == nil {
			return errorCloneResult{next: err, owned: true, terminal: true}
		}
		copied := *current
		copied.err = nil
		return errorCloneResult{frame: errorCloneFrame{wrapper: &copied}, next: current.err, owned: true}
	case *factoryBuildError:
		if current == nil {
			return errorCloneResult{next: err, owned: true, terminal: true}
		}
		copied := *current
		copied.err = nil
		return errorCloneResult{frame: errorCloneFrame{wrapper: &copied}, next: current.err, owned: true}
	case *Suspension:
		if current == nil {
			return errorCloneResult{next: err, owned: true, terminal: true}
		}
		return errorCloneResult{next: current.clone(), owned: true, terminal: true}
	default:
		return errorCloneResult{next: err}
	}
}

func (f errorCloneFrames) attach(cause error) error {
	for _, frame := range slices.Backward(f) {
		cause = frame.wrap(cause)
	}
	return cause
}

func (f errorCloneFrame) wrap(cause error) error {
	//nolint:errorlint // The exact clone determines which field carries the cause.
	switch wrapper := f.wrapper.(type) {
	case *StepError:
		wrapper.Err = cause
	case *RefError:
		wrapper.Err = cause
	case *RegistrationError:
		wrapper.Err = cause
	case *GraphError:
		wrapper.Err = cause
	case *SpecError:
		wrapper.Err = cause
	case *flow.IndexError:
		wrapper.Err = cause
	case *flow.CaseError:
		wrapper.Err = cause
	case *detailError:
		wrapper.err = cause
	case *factoryBuildError:
		wrapper.err = cause
	default:
		panic("workflow: invalid owned error clone frame")
	}
	return f.wrapper
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

// Unwrap returns the underlying graph error.
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

// Unwrap returns the underlying specification error.
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

// errorFormatter renders the owned part of an error tree. Applications may
// construct its exported locations directly, so neither linear depth nor
// [errors.Join] depth may consume the call stack. A caller-defined wrapper or
// multi-error remains one opaque cause.
type errorFormatter struct {
	message strings.Builder
	tasks   []errorFormatTask
}

type errorFormatTask struct {
	err        error
	newline    bool
	references []*RefError
	cause      error
	suffix     bool
}

type errorFormatFrame struct {
	next      error
	reference *RefError
	owned     bool
	terminal  bool
}

func (e ownedError) format() string {
	formatter := errorFormatter{tasks: []errorFormatTask{{err: e.root}}}
	return formatter.format()
}

func (f *errorFormatter) format() string {
	for len(f.tasks) > 0 {
		last := len(f.tasks) - 1
		task := f.tasks[last]
		f.tasks = f.tasks[:last]
		f.render(task)
	}
	return f.message.String()
}

func (f *errorFormatter) render(task errorFormatTask) {
	if task.suffix {
		f.appendMismatches(task.references, task.cause)
		return
	}
	if task.newline {
		f.message.WriteByte('\n')
	}

	err := task.err
	var references []*RefError
	for {
		if children, joined := standardJoinChildren(err); joined {
			f.scheduleJoin(children, references, err)
			return
		}
		frame := f.appendFrame(err)
		if !frame.owned || frame.terminal {
			f.finish(references, frame.next)
			return
		}
		if frame.reference != nil {
			references = append(references, frame.reference)
		}
		err = frame.next
	}
}

func (f *errorFormatter) scheduleJoin(children []error, references []*RefError, cause error) {
	if len(references) > 0 {
		f.tasks = append(f.tasks, errorFormatTask{
			references: references,
			cause:      cause,
			suffix:     true,
		})
	}
	for index, child := range slices.Backward(children) {
		f.tasks = append(f.tasks, errorFormatTask{
			err:     child,
			newline: index > 0,
		})
	}
}

// appendFrame consumes one exact engine-owned location. A type assertion via
// [errors.As] would cross an application wrapper and change its presentation.
//
//nolint:errorlint // Exact wrapper identity determines formatting ownership.
func (f *errorFormatter) appendFrame(err error) errorFormatFrame {
	switch frame := err.(type) {
	case errorPrefixAppender:
		valid, next := frame.appendErrorPrefix(&f.message)
		reference, _ := frame.(*RefError)
		return errorFormatFrame{
			next:      next,
			reference: reference,
			owned:     true,
			terminal:  !valid,
		}
	case *flow.IndexError:
		if frame == nil {
			return errorFormatFrame{owned: true, terminal: true}
		}
		fmt.Fprintf(&f.message, "index %d: ", frame.Index)
		return errorFormatFrame{next: frame.Err, owned: true}
	case *flow.CaseError:
		if frame == nil {
			return errorFormatFrame{owned: true, terminal: true}
		}
		fmt.Fprintf(&f.message, "switch case %#v: ", frame.Key)
		return errorFormatFrame{next: frame.Err, owned: true}
	default:
		return errorFormatFrame{next: err}
	}
}

func (f *errorFormatter) finish(references []*RefError, cause error) {
	fmt.Fprint(&f.message, cause)
	f.appendMismatches(references, cause)
}

func (f *errorFormatter) appendMismatches(references []*RefError, cause error) {
	if (errorTree{root: cause}).matches(ErrNotFound) {
		return
	}
	for _, reference := range slices.Backward(references) {
		reference.appendMismatch(&f.message)
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
