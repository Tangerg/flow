package workflow

import (
	"context"
	"slices"
	"sync"
)

// EventKind identifies a step lifecycle event.
type EventKind string

// Step lifecycle event kinds.
const (
	EventStarted   EventKind = "started"
	EventCompleted EventKind = "completed"
	EventFailed    EventKind = "failed"
)

// Event describes one step lifecycle transition. Err is non-nil only for an
// [EventFailed] event.
type Event struct {
	Kind EventKind
	ID   string
	Err  error
}

// Observer receives workflow events synchronously. Observe may be called from
// multiple goroutines and should return promptly. A slow Observer delays the
// step emitting the event.
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

type observerKey struct{}

// WithObserver returns a context carrying observer. Steps built with [Leaf]
// report their lifecycle events to it while running under that context.
func WithObserver(ctx context.Context, observer Observer) context.Context {
	return context.WithValue(ctx, observerKey{}, observer)
}

func emit(ctx context.Context, event Event) {
	if observer, ok := ctx.Value(observerKey{}).(Observer); ok && observer != nil {
		observer.Observe(ctx, event)
	}
}

// Collector is a concurrency-safe [Observer] that records events in arrival
// order. Its zero value is ready to use.
type Collector struct {
	mu     sync.Mutex
	events []Event
}

var _ Observer = (*Collector)(nil)

// Observe records event.
func (c *Collector) Observe(_ context.Context, event Event) {
	c.mu.Lock()
	c.events = append(c.events, event)
	c.mu.Unlock()
}

// Events returns a copy of the events collected so far.
func (c *Collector) Events() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.events)
}
