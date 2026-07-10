package workflow

import (
	"context"
	"slices"
	"sync"
)

// Event is emitted as steps run, for observability. The concrete events are
// [NodeStarted], [NodeCompleted], and [NodeFailed].
type Event interface{ isEvent() }

// NodeStarted is emitted just before a step's leaf runs.
type NodeStarted struct{ ID string }

// NodeCompleted is emitted after a step's leaf succeeds.
type NodeCompleted struct{ ID string }

// NodeFailed is emitted when a step's bind or leaf fails.
type NodeFailed struct {
	ID  string
	Err error
}

func (NodeStarted) isEvent()   {}
func (NodeCompleted) isEvent() {}
func (NodeFailed) isEvent()    {}

// Sink receives events synchronously. It may be called from several goroutines
// at once, since steps run concurrently in [Parallel] and [Iteration], so it
// must be concurrency-safe, return promptly, and not panic. A slow Sink delays
// the node emitting the event.
type Sink func(Event)

type sinkKey struct{}

// WithSink returns a context carrying sink. Steps built with [Adapt] emit their
// lifecycle events to it while running under that context.
func WithSink(ctx context.Context, sink Sink) context.Context {
	return context.WithValue(ctx, sinkKey{}, sink)
}

func emit(ctx context.Context, e Event) {
	if sink, ok := ctx.Value(sinkKey{}).(Sink); ok && sink != nil {
		sink(e)
	}
}

// Collector is a concurrency-safe [Sink] that records events in the order they
// arrive.
type Collector struct {
	mu     sync.Mutex
	events []Event
}

// Sink returns the collector's sink function.
func (c *Collector) Sink() Sink {
	return func(e Event) {
		c.mu.Lock()
		c.events = append(c.events, e)
		c.mu.Unlock()
	}
}

// Events returns a copy of the events collected so far.
func (c *Collector) Events() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.events)
}
