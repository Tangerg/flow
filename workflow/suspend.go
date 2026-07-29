package workflow

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

// Suspension reports why a run stopped and what would let it continue. Await
// names the reference the step is waiting for; it is the zero Ref when a step
// suspends for a reason that is not a missing value.
type Suspension struct {
	// ID is the step that suspended.
	ID string
	// Path is the step's enclosing repeated scopes, as on [Event.Path].
	Path []string
	// Await is the reference whose absence caused the suspension, if any.
	Await Ref
	// Reason describes the wait in a form a caller can show a person.
	Reason string
}

func (e *Suspension) Error() string {
	var b strings.Builder
	b.WriteString("workflow: step ")
	b.WriteString(`"` + e.ID + `"`)
	if len(e.Path) > 0 {
		b.WriteString(" in " + strings.Join(e.Path, "/"))
	}
	b.WriteString(" suspended")
	switch {
	case e.Reason != "":
		b.WriteString(": " + e.Reason)
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
// only say why.
func Suspend(reason string) error {
	return &Suspension{Reason: reason}
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
// [DefaultPort] is wired to.
//
//	reg.MustRegisterLeaf("await", workflow.AwaitFactory())
//	// {"id":"approval","type":"await","input":{"nodeID":"inbox","path":"decision"}}
func AwaitFactory() LeafFactory {
	return func(spec LeafSpec) (Step, error) {
		ref, ok := spec.Inputs.Default()
		if !ok {
			return nil, fmt.Errorf("%w %q", ErrMissingPort, DefaultPort)
		}
		return Await(spec.ID, ref), nil
	}
}

// suspensionOf returns the [*Suspension] err carries, or nil when err is not a
// suspension. Composites use it to tell "not yet" from a failure.
func suspensionOf(err error) *Suspension {
	var suspension *Suspension
	if errors.As(err, &suspension) {
		return suspension
	}
	if errors.Is(err, ErrSuspended) {
		// A caller may wrap ErrSuspended without the richer value.
		return &Suspension{Reason: err.Error()}
	}
	return nil
}

// joinSuspensions reports the suspensions of a fan-out as one error. Several
// branches may be waiting at once, and a caller needs every reason to know what
// to supply before resuming.
func joinSuspensions(suspensions []*Suspension) error {
	switch len(suspensions) {
	case 0:
		return nil
	case 1:
		return suspensions[0]
	}
	slices.SortFunc(suspensions, func(a, b *Suspension) int {
		return strings.Compare(a.ID, b.ID)
	})
	reasons := make([]string, 0, len(suspensions))
	for _, s := range suspensions {
		reasons = append(reasons, s.Error())
	}
	return &multiSuspension{
		suspensions: suspensions,
		message:     fmt.Sprintf("workflow: %d steps suspended: %s", len(suspensions), strings.Join(reasons, "; ")),
	}
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

// Suspensions returns every suspension err reports, in step ID order. A run that
// stopped in one place yields one; a fan-out may yield several.
func Suspensions(err error) []*Suspension {
	var multi *multiSuspension
	if errors.As(err, &multi) {
		return slices.Clone(multi.suspensions)
	}
	if suspension := suspensionOf(err); suspension != nil {
		return []*Suspension{suspension}
	}
	return nil
}
