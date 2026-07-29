package workflow

import (
	"context"
	"strconv"
	"time"
)

// EventKind identifies a step lifecycle event.
type EventKind string

// Step lifecycle event kinds. A step that ran reports exactly one of
// [EventCompleted], [EventFailed], or [EventSuspended]; a step skipped because a
// [Journal] already holds its result reports [EventSkipped] instead of starting.
const (
	EventStarted   EventKind = "started"
	EventCompleted EventKind = "completed"
	EventFailed    EventKind = "failed"
	EventSuspended EventKind = "suspended"
	EventSkipped   EventKind = "skipped"
)

// Event describes one step lifecycle transition.
//
// Together the fields carry enough to build tracing and durability outside this
// package: Seq orders the events of one run, Path distinguishes repeated
// executions of the same step, and Store is the snapshot a step produced, which
// is serializable. Keeping those concerns out of the package is deliberate — see
// the package documentation.
type Event struct {
	// Kind is the transition this event reports.
	Kind EventKind

	// ID is the step's ID.
	ID string

	// Path is the chain of enclosing repeated scopes, outermost first: a [Loop]
	// iteration or an [Iteration] element. It is empty for a step that runs at
	// most once per run. Each event owns its slice.
	Path []string

	// Seq numbers events within one run, starting at 1. Events are numbered in
	// the order they are emitted, so a step's completion always outnumbers its
	// start even when steps run concurrently.
	Seq uint64

	// Elapsed is how long the step took. It is zero on [EventStarted].
	Elapsed time.Duration

	// Store is the Store the step produced. It is set on [EventCompleted] and
	// [EventSkipped]; a failed or suspended step has no output. Pair it with
	// [Store.Changes] to record just the step's writes.
	Store Store

	// Err is the failure on [EventFailed], or an error matching [ErrSuspended] on
	// [EventSuspended]. Use [Suspensions] to read every wait. It is nil otherwise.
	Err error
}

// Observer receives workflow events synchronously. Attach one with a
// [RunConfig]. Observe may be called from multiple goroutines and should return
// promptly. A slow Observer delays the step emitting the event.
type Observer interface {
	Observe(context.Context, Event)
}

// ObserverFunc adapts a function into an [Observer].
type ObserverFunc func(context.Context, Event)

// Observe calls f. A nil ObserverFunc discards the event.
func (f ObserverFunc) Observe(ctx context.Context, event Event) {
	if f != nil {
		f(ctx, event)
	}
}

// indexScope names one repetition of a scope. An empty id yields a bare index,
// which is what a [Loop] iteration reports.
func indexScope(id string, index int) string {
	return id + "[" + strconv.Itoa(index) + "]"
}
