// Package ctxtest provides Context implementations for tests that need
// cancellation to happen at an exact point in a boundary check.
package ctxtest

import "context"

// CancelAtCheck returns a Context that reports cause the checks'th time its Err
// method is called, and nil before that. Done closes before that Err call
// returns, so the value is a valid Context at the moment it cancels rather than
// one that reports an error while still appearing live.
//
// This makes an otherwise microscopic boundary deterministic — the window
// between finishing work and admitting its result — without sleeps or
// assumptions about goroutine scheduling. Counting is not synchronized, so use
// one of these values from a single goroutine's check sequence.
func CancelAtCheck(parent context.Context, checks int, cause error) context.Context {
	return &cancelAtCheck{Context: parent, done: make(chan struct{}), cause: cause, at: checks}
}

// cancelAtCheck retains its parent because a Context implementation must
// delegate Deadline and Value.
//
//nolint:containedctx // A custom Context must retain its parent.
type cancelAtCheck struct {
	context.Context
	done   chan struct{}
	cause  error
	at     int
	checks int
}

func (c *cancelAtCheck) Done() <-chan struct{} { return c.done }

func (c *cancelAtCheck) Err() error {
	c.checks++
	if c.checks < c.at {
		return nil
	}
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	return c.cause
}
