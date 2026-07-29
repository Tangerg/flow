package workflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ErrSuspended marks a run that stopped because a step is waiting for something
// the workflow cannot produce on its own — a human decision, an external job, a
// retry window. Match it with [errors.Is] and read the details with [errors.As]
// on [*Suspension].
//
// A suspension is a third outcome alongside success and failure, not a kind of
// failure. Composites treat it accordingly: [Parallel] and [Iteration] let their
// remaining work finish instead of cancelling it, because "not yet" says nothing
// about whether the rest should proceed.
var ErrSuspended = errors.New("workflow: suspended")

// Suspension reports why a run stopped and what would let it continue.
//
// Await names a value that must appear in the Store. It is the zero Ref for an
// [Interrupt] or for a node that called [Suspend]. Value is the information
// exposed to the caller: commonly a string, but it may be any application value
// such as an approval request, form, or external-job descriptor.
type Suspension struct {
	// ID is the step that suspended.
	ID string `json:"id,omitempty"`
	// Path is the step's enclosing repeated scopes, as on [Event.Path].
	Path []string `json:"path,omitempty"`
	// Await is the reference whose absence caused the suspension, if any.
	Await Ref `json:"await,omitzero"`
	// Value is application-owned and must be treated as immutable. A caller that
	// persists a Suspension is responsible for using a codec that can represent
	// the concrete value.
	Value any `json:"value,omitempty"`
}

func (e *Suspension) Error() string {
	if e == nil {
		return ErrSuspended.Error()
	}
	var b strings.Builder
	b.WriteString("workflow:")
	if e.ID != "" {
		b.WriteString(" step ")
		b.WriteString(strconv.Quote(e.ID))
	}
	if len(e.Path) > 0 {
		b.WriteString(" in " + strings.Join(e.Path, "/"))
	}
	b.WriteString(" suspended")
	message, _ := e.Value.(string)
	switch {
	case message != "":
		b.WriteString(": " + message)
	case e.Await != (Ref{}):
		b.WriteString(": awaiting " + e.Await.String())
	}
	return b.String()
}

// Unwrap returns [ErrSuspended].
func (e *Suspension) Unwrap() error { return ErrSuspended }

// Suspend returns an error that stops the run at the calling step. Use it inside
// a node when the work cannot proceed yet; the caller resumes by supplying what
// is missing and running the same workflow again with the run's [Journal].
//
// The step ID and scope are filled in by the step that returns it, so a node need
// only provide the value to expose. Mutable values must not be modified after
// this call. A caller may either supply the missing external state and let the
// step run again, or record the step's expected result under [Suspension.Key] so
// a journaled boundary replays it as completed. [Interrupt] packages the latter
// pattern as a Step.
func Suspend(value any) error {
	return &Suspension{Value: value}
}

// Key returns the structured identity of the suspended step. The returned Path
// is a copy. It is the key to pass to [Journal.Record] when supplying the result
// of an [Interrupt] or another journaled boundary that called [Suspend].
func (e *Suspension) Key() JournalKey {
	if e == nil {
		return JournalKey{}
	}
	return JournalKey{ID: e.ID, Path: slices.Clone(e.Path)}
}

// Await returns a [Step] that passes the Store through once ref resolves and
// suspends until then. It is the common shape of a wait: a human approval, a
// callback, a value another system will write.
//
//	approval := workflow.Await("approval", workflow.At("inbox", "decision"))
//	out, err := workflow.Sequence(draft, approval, publish).Run(ctx, in)
//	if errors.Is(err, workflow.ErrSuspended) {
//		// persist the Journal, wait for the decision, then run again
//	}
//
// The step writes nothing of its own, so it re-evaluates on every run rather than
// being skipped as completed — which is what makes the wait meaningful.
func Await(id string, ref Ref) Step { return awaitStep{id: id, ref: ref} }

// awaitStep is the [Step] produced by [Await].
type awaitStep struct {
	id  string
	ref Ref
}

func (a awaitStep) Run(ctx context.Context, s Store) (Store, error) {
	run := runFrom(ctx)
	if a.id == "" {
		err := &StepError{ID: a.id, Op: OpValidate, Err: ErrInvalidStepID}
		run.emit(ctx, Event{Kind: EventFailed, ID: a.id, Err: err})
		return s, err
	}
	if _, ok := s.Lookup(a.ref); ok {
		run.emit(ctx, Event{Kind: EventCompleted, ID: a.id, Store: s})
		return s, nil
	}
	suspension := &Suspension{ID: a.id, Path: Scope(ctx), Await: a.ref}
	run.emit(ctx, Event{Kind: EventSuspended, ID: a.id, Err: suspension})
	return s, suspension
}

func (a awaitStep) Describe() Description {
	return Description{ID: a.id, Kind: "await", Label: a.ref.String()}
}

// AwaitFactory is the [LeafFactory] form of [Await]: a node type a serialized
// workflow can name to place a wait in a graph. It waits on whatever its
// [DefaultPort] is wired to and accepts no config. Use [InterruptFactory] when
// the wait must expose a structured request.
//
//	reg.MustRegisterLeaf("await", workflow.AwaitFactory())
//	// {"id":"approval","type":"await","input":{"nodeID":"inbox","path":"decision"}}
func AwaitFactory() LeafFactory {
	return func(spec LeafSpec) (Step, error) {
		for _, port := range spec.Inputs.PortNames() {
			if port != DefaultPort {
				return nil, fmt.Errorf("%w %q", ErrUnknownPort, port)
			}
		}
		if len(bytes.TrimSpace(spec.Config)) > 0 {
			return nil, fmt.Errorf("%w: await config must be omitted", ErrInvalidSpec)
		}
		ref, ok := spec.Inputs.Default()
		if !ok {
			return nil, fmt.Errorf("%w %q", ErrMissingPort, DefaultPort)
		}
		return Await(spec.ID, ref), nil
	}
}

// Interrupt returns a value-producing [Step] that exposes value in a
// [Suspension] and stops until its response is recorded in the run's [Journal].
// On the next run the response is restored under [Output](id), exactly like a
// completed leaf result. A run without a Journal cannot hold a response, so the
// Step suspends each time. Value is held as-is; mutable values must not be
// modified after this call.
//
// Interrupt is the graph-native form of a resumable request: the Step is the
// explicit replay boundary, so there is no hidden call-order matching inside a
// node. Resolve one with:
//
//	wait := workflow.Suspensions(err)[0]
//	if err := journal.Record(wait.Key(), response); err != nil { ... }
//	out, err := pipeline.Run(ctx, paused)
func Interrupt(id string, value any) Step {
	return interruptStep{id: id, value: value}
}

type interruptStep struct {
	id    string
	value any
}

func (i interruptStep) Run(ctx context.Context, s Store) (Store, error) {
	run := runFrom(ctx)
	if i.id == "" {
		err := &StepError{ID: i.id, Op: OpValidate, Err: ErrInvalidStepID}
		run.emit(ctx, Event{Kind: EventFailed, ID: i.id, Err: err})
		return s, err
	}
	if journal := run.journal(); journal != nil {
		if response, ok := journal.lookup(scope(ctx), i.id); ok {
			next := s.WithOutput(i.id, response)
			run.emit(ctx, Event{Kind: EventSkipped, ID: i.id, Store: next})
			return next, nil
		}
	}

	suspension := &Suspension{
		ID:    i.id,
		Path:  Scope(ctx),
		Value: i.value,
	}
	run.emit(ctx, Event{Kind: EventSuspended, ID: i.id, Err: suspension})
	return s, suspension
}

func (i interruptStep) Describe() Description {
	return Description{ID: i.id, Kind: "interrupt"}
}

// InterruptFactory is the [LeafFactory] form of [Interrupt]. The leaf's JSON
// config becomes the value exposed by the suspension; an omitted config becomes
// nil. Interrupt leaves accept no input ports.
//
//	reg.MustRegisterLeaf("interrupt", workflow.InterruptFactory())
//	// {"id":"approval","type":"interrupt","config":{"question":"approve?"}}
func InterruptFactory() LeafFactory {
	return func(spec LeafSpec) (Step, error) {
		if ports := spec.Inputs.PortNames(); len(ports) > 0 {
			return nil, fmt.Errorf("%w %q", ErrUnknownPort, ports[0])
		}

		var value any
		if config := bytes.TrimSpace(spec.Config); len(config) > 0 {
			decoded, err := decodeValue(config)
			if err != nil {
				return nil, fmt.Errorf("%w: decode config: %w", ErrInvalidSpec, err)
			}
			value = decoded
		}
		return Interrupt(spec.ID, value), nil
	}
}

// asSuspensions reports the suspension leaves in err and whether every leaf in
// its error tree is a suspension. Composites use the second result to keep a
// joined failure from being mistaken for "not yet".
func asSuspensions(err error) ([]*Suspension, bool) {
	suspensions, only := collectSuspensions(err)
	if len(suspensions) == 0 {
		return nil, false
	}
	return normalizeSuspensions(suspensions), only
}

// collectSuspensions walks both forms supported by the standard error tree:
// Unwrap() error and Unwrap() []error. It returns copies so identifying a wait
// at a workflow boundary never mutates an error owned by its caller.
func collectSuspensions(err error) ([]*Suspension, bool) {
	if err == nil {
		return nil, false
	}
	if suspension, ok := err.(*Suspension); ok {
		return []*Suspension{cloneSuspension(suspension)}, true
	}
	if err == ErrSuspended {
		return []*Suspension{{}}, true
	}

	if many, ok := err.(interface{ Unwrap() []error }); ok {
		var suspensions []*Suspension
		only := true
		children := 0
		for _, child := range many.Unwrap() {
			if child == nil {
				continue
			}
			children++
			found, childOnly := collectSuspensions(child)
			suspensions = append(suspensions, found...)
			only = only && childOnly
		}
		return suspensions, children > 0 && only
	}
	if one, ok := err.(interface{ Unwrap() error }); ok {
		child := one.Unwrap()
		if child == ErrSuspended {
			return []*Suspension{{Value: err.Error()}}, true
		}
		return collectSuspensions(child)
	}

	// A custom error may participate in errors.Is without exposing an unwrap.
	if errors.Is(err, ErrSuspended) {
		return []*Suspension{{Value: err.Error()}}, true
	}
	return nil, false
}

// identifySuspensions fills in the workflow boundary that owns an otherwise
// anonymous suspension. Already-identified nested waits keep their identity.
func identifySuspensions(suspensions []*Suspension, id string, path []string) []*Suspension {
	for _, suspension := range suspensions {
		if suspension.ID == "" {
			suspension.ID = id
		}
		if suspension.Path == nil {
			suspension.Path = slices.Clone(path)
		}
	}
	return suspensions
}

// joinSuspensions reports the suspensions of a fan-out as one error. Several
// branches may be waiting at once, and a caller needs every reason to know what
// to supply before resuming.
func joinSuspensions(suspensions []*Suspension) error {
	suspensions = normalizeSuspensions(suspensions)
	switch len(suspensions) {
	case 0:
		return nil
	case 1:
		return suspensions[0]
	}
	reasons := make([]string, 0, len(suspensions))
	for _, s := range suspensions {
		reasons = append(reasons, s.Error())
	}
	return &multiSuspension{
		suspensions: suspensions,
		message:     fmt.Sprintf("workflow: %d steps suspended: %s", len(suspensions), strings.Join(reasons, "; ")),
	}
}

func normalizeSuspensions(suspensions []*Suspension) []*Suspension {
	normalized := make([]*Suspension, 0, len(suspensions))
	for _, suspension := range suspensions {
		if suspension != nil {
			normalized = append(normalized, cloneSuspension(suspension))
		}
	}
	slices.SortStableFunc(normalized, compareSuspensions)
	return normalized
}

func compareSuspensions(a, b *Suspension) int {
	if c := strings.Compare(a.ID, b.ID); c != 0 {
		return c
	}
	if c := slices.Compare(a.Path, b.Path); c != 0 {
		return c
	}
	if c := strings.Compare(a.Await.NodeID, b.Await.NodeID); c != 0 {
		return c
	}
	if c := strings.Compare(a.Await.Path, b.Await.Path); c != 0 {
		return c
	}
	return 0
}

func cloneSuspension(suspension *Suspension) *Suspension {
	if suspension == nil {
		return nil
	}
	clone := *suspension
	clone.Path = slices.Clone(suspension.Path)
	return &clone
}

// multiSuspension carries every suspension of one fan-out.
type multiSuspension struct {
	suspensions []*Suspension
	message     string
}

func (e *multiSuspension) Error() string { return e.message }

// Unwrap returns each suspension so errors.As finds the first and errors.Is
// matches [ErrSuspended].
func (e *multiSuspension) Unwrap() []error {
	errs := make([]error, len(e.suspensions))
	for i, s := range e.suspensions {
		errs[i] = s
	}
	return errs
}

// Suspensions returns every suspension in err's error tree, ordered by step ID
// and then scope. A run that stopped in one place yields one; nested fan-out may
// yield several. The returned Suspension values and their paths are copies;
// application-owned mutable Values remain borrowed and must not be modified.
func Suspensions(err error) []*Suspension {
	suspensions, _ := asSuspensions(err)
	return suspensions
}
