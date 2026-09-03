package workflow

import (
	"context"
	"slices"
	"time"
)

// EventKind identifies an observable workflow-boundary event.
type EventKind string

// Observable workflow-boundary event kinds. An admitted, non-replayed [Leaf]
// reports [EventStarted] followed by exactly one of [EventCompleted],
// [EventFailed], or [EventSuspended]. A Leaf rejected during validation reports
// EventFailed without a start; one cancelled before admission reports nothing.
// An [Await] or [Interrupt] never reports a start: validation failures report
// EventFailed, and an admitted call reports its terminal transition. A boundary
// skipped because a [Journal] already holds its result reports [EventSkipped],
// while a Graph node whose gate is not satisfied reports [EventBypassed].
// Composite steps ([Sequence], [Parallel], [Branch], [Loop], [Iteration],
// [Subgraph], and a compiled [Graph]) are transparent and do not add lifecycle
// events of their own. Their leaf and wait boundaries remain observable. A
// graph gate may report a bypass or a gate-evaluation failure before entering
// its wrapped boundary.
const (
	EventStarted   EventKind = "started"
	EventCompleted EventKind = "completed"
	EventFailed    EventKind = "failed"
	EventSuspended EventKind = "suspended"
	EventSkipped   EventKind = "skipped"
	EventBypassed  EventKind = "bypassed"
)

// Event describes one observable workflow-boundary transition.
//
// Together the fields carry enough to trace execution and persist a boundary's
// produced Store snapshot outside this package: Seq orders the externally
// visible signals of one run, Scope distinguishes repeated executions of the
// same step, and Store is serializable. Resumption also requires the run's
// Journal, persisted at an application-chosen boundary. Keeping those concerns
// out of the package is deliberate — see the package documentation.
//
// [EventCompleted] and [EventSkipped] arrive after that boundary's checkpoint is
// recorded, so an Observer persisting the pair sees a Journal that already holds
// the step it is observing. A failure or suspension records nothing.
type Event struct {
	// Kind is the transition this event reports.
	Kind EventKind
	ID   string

	// Scope is the chain of enclosing execution scopes, outermost first. [Loop]
	// and [Iteration] distinguish repeated invocations; [Subgraph] isolates an
	// inner namespace. Inspect [ScopeFrame.Indexed] rather than parsing its
	// display text. It is empty outside a scoped composite. Each event owns its
	// slice.
	Scope []ScopeFrame

	// Seq numbers events and [Chunk] values within one run, starting at 1. It is
	// assigned immediately before delivery, so sorting both signal types by Seq
	// gives one run timeline; callbacks from concurrent invocations may arrive
	// out of order. A step's completion always outnumbers its start. Either
	// consumer may see gaps occupied by the other signal type.
	Seq uint64

	// Elapsed is the wall time of a Leaf attempt after validation and replay.
	// It is zero on [EventStarted], on validation failure, and for boundaries
	// that do not time work, such as Await and Interrupt.
	Elapsed time.Duration

	// Store is the Store the boundary produced. It is set on [EventCompleted] and
	// [EventSkipped]; a failed, suspended, or bypassed step has no output. Pair
	// it with [Store.Changes] to record just the step's writes.
	Store Store

	// Err is the failure on [EventFailed], or an error matching [ErrSuspended] on
	// [EventSuspended]. Use [Suspensions] to read every wait. The event owns the
	// exact mutable flow and workflow location wrappers in its error chain,
	// including across standard-library joined branches. An application-defined
	// wrapper ends that snapshot boundary, and it and its causes remain borrowed
	// under Go's immutable-error convention. Application values stored in a
	// location remain borrowed too. Err is nil otherwise.
	Err error
}

// owned returns the event value an Observer may retain or modify. Store is
// already an immutable value, and scalar fields copy with the struct. Scope and
// the exported fields on module-owned location errors need their own storage so
// a callback cannot rewrite the outcome the running boundary returns afterward.
func (e Event) owned() Event {
	e.Scope = slices.Clone(e.Scope)
	e.Err = (ownedError{root: e.Err}).clone()
	return e
}

// Observer receives low-volume workflow-boundary events synchronously. Pass one
// to [Run] through [RunConfig]. Observe may be called from multiple goroutines
// and should return promptly. A slow Observer delays the boundary emitting the
// event. An Observer must not wait for a later event from the same boundary
// invocation: synchronous delivery would deadlock that invocation. Cancellation
// that occurs while a non-failure terminal event is being observed is sampled
// before that boundary returns; definition errors and an already-classified
// failure retain their own error. The context preserves the run's cancellation,
// deadline, and application values, but is detached from its workflow identity:
// calling a Step directly from Observe does not join the observed run. Call
// [Run] to start an independent execution. Use [Emitter] for intermediate
// application values.
type Observer interface {
	Observe(ctx context.Context, event Event)
}

// ObserverFunc adapts a function into an [Observer].
type ObserverFunc func(context.Context, Event)

var _ Observer = ObserverFunc(nil)

// Observe calls f. A nil ObserverFunc discards the event.
func (o ObserverFunc) Observe(ctx context.Context, event Event) {
	if o != nil {
		o(ctx, event)
	}
}
