package workflow

import (
	"context"
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
type Event struct {
	// Kind is the transition this event reports.
	Kind EventKind

	// ID is the step's ID.
	ID string

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
	// [EventSuspended]. Use [Suspensions] to read every wait. It is nil otherwise.
	Err error
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

// ObserverFunc satisfies Observer.
var _ Observer = ObserverFunc(nil)

// Observe calls f. A nil ObserverFunc discards the event.
func (o ObserverFunc) Observe(ctx context.Context, event Event) {
	if o != nil {
		o(ctx, event)
	}
}
