package workflow

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/Tangerg/flow"
)

// StreamFunc adapts a typed streaming function into a [flow.Node]. The function
// computes one final O exactly like any other Node and may additionally publish
// intermediate C values through yield. A false result means the consumer
// stopped: the function must stop yielding and return promptly.
//
// Streaming is a run-scoped side channel, not a second execution protocol.
// StreamFunc therefore composes directly with flow helpers such as [flow.Then],
// [flow.Map], and [flow.Race]. When the resulting Node runs inside a [Leaf],
// yielded values go to that leaf invocation's [Emitter]. Without an Emitter they
// are discarded without constructing a [Chunk] or consuming a sequence number.
// Outside a Leaf they are also discarded because no workflow identity exists,
// even if an enclosing [Run] has an Emitter.
//
// C is the producer-side contract for this function. An Emitter intentionally
// receives it as [Chunk.Value] of type any because one leaf may compose
// StreamFunc values with different C types. Use an application-defined tagged
// value, or separate leaves, when the consumer must distinguish those streams.
//
// Yielded values are attempts, not durable records. A resumed incomplete leaf
// runs again from the beginning and may repeat a prefix. A leaf replayed as
// complete from a [Journal] does not run and yields nothing.
//
// The function may call yield from multiple goroutines; Emitter delivery is
// serialized for the enclosing leaf invocation. StreamFunc does not return
// until every in-flight yield returns. A retained yield called after Run exits
// reports false without reaching the Emitter, including when the producer
// panics and an outer boundary recovers. Implementations must otherwise be safe
// for concurrent use and keep invocation state local.
type StreamFunc[I, O, C any] func(
	context.Context,
	I,
	func(C) bool,
) (O, error)

// StreamFunc satisfies flow.Node.
var _ flow.Node[any, any] = StreamFunc[any, any, any](nil)

// Validate rejects a nil StreamFunc before a composite performs work. The
// adapter owns this check rather than asking flow to classify every
// caller-defined function type as an invalid nil function.
func (s StreamFunc[I, O, C]) Validate() error {
	if s == nil {
		return flow.ErrNilNode
	}
	return nil
}

// Run calls f with the current leaf invocation's typed yield function. A nil
// StreamFunc returns [flow.ErrNilNode]. Once yield reports false, the reason the
// stream stopped takes precedence over the function's return: a producer cannot
// hide a failed consumer by returning success or an unrelated error afterward.
func (s StreamFunc[I, O, C]) Run(ctx context.Context, input I) (output O, err error) {
	if s == nil {
		var zero O
		return zero, flow.ErrNilNode
	}

	emission := emissionLease{session: emissionFrom(ctx)}
	// Close from a defer so the lexical yield capability cannot outlive this
	// invocation even when the producer panics and an outer boundary recovers.
	// The defer also waits for every admitted concurrent yield before Run exits.
	defer func() {
		if emissionErr := emission.close(); emissionErr != nil {
			var zero O
			output = zero
			err = emissionErr
		}
	}()
	return s(ctx, input, func(value C) bool {
		return emission.yield(ctx, value)
	})
}

// Chunk is one intermediate value from a streaming [flow.Node].
//
// ID and Scope identify the enclosing leaf invocation; each Chunk owns its
// Scope slice. Inspect [ScopeFrame.Indexed] rather than parsing display text.
// Index starts at zero for each invocation. Seq shares a run-wide ordering with
// [Event]; callbacks from concurrent leaf invocations may arrive out of order.
// Value is application-owned and must be treated as immutable.
type Chunk struct {
	ID    string
	Scope []ScopeFrame
	Seq   uint64
	Index uint64
	Value any
}

// Emitter receives chunks synchronously. Pass one to [Run] through [RunConfig].
// Calls are serialized within one leaf invocation but may arrive concurrently
// from different invocations, so implementations must be safe for concurrent
// use. An Emitter must not wait for another chunk from the same leaf invocation:
// serialized delivery would deadlock that invocation. A slow Emitter applies
// backpressure to the producing Node. Returning an error stops that leaf's
// stream and returns the error through its normal failure or suspension
// classification. The context preserves the run's cancellation, deadline, and
// application values, but is detached from its workflow identity: calling a
// Step directly from Emit does not join the producing run. Call [Run] to start
// an independent execution.
type Emitter interface {
	Emit(ctx context.Context, chunk Chunk) error
}

// EmitterFunc adapts a function into an [Emitter].
type EmitterFunc func(context.Context, Chunk) error

// EmitterFunc satisfies Emitter.
var _ Emitter = EmitterFunc(nil)

// Emit calls f. A nil EmitterFunc discards the chunk.
func (e EmitterFunc) Emit(ctx context.Context, chunk Chunk) error {
	if e == nil {
		return nil
	}
	return e(ctx, chunk)
}

type emissionKey struct{}

// emissionSession owns the application-output boundary of one leaf invocation.
// It is deliberately non-generic because one composed Node may contain several
// StreamFunc values with different C types; Chunk is their heterogeneous run
// boundary.
type emissionSession struct {
	// mu is held across the Emitter call on purpose: that is what serializes
	// delivery for the invocation and keeps Index monotonic, which the Emitter
	// contract promises and warns about. Unlike Journal's lock, it guards no state
	// another step needs, so spanning application code costs nothing but the
	// documented backpressure.
	mu      sync.Mutex
	run     *runState
	cancel  context.CancelCauseFunc
	emitter Emitter
	id      string
	scope   []ScopeFrame
	index   uint64
	closed  bool
	err     error
}

func withEmission(
	ctx context.Context,
	run *runState,
	id string,
	scope []ScopeFrame,
	emitter Emitter,
) (context.Context, *emissionSession) {
	ctx, cancel := context.WithCancelCause(ctx)
	session := &emissionSession{
		run:     run,
		cancel:  cancel,
		emitter: emitter,
		id:      id,
		scope:   slices.Clone(scope),
	}
	return context.WithValue(ctx, emissionKey{}, session), session
}

// emissionFrom returns only a session owned by the current workflow run. A
// nested Run installs a new runState; comparing the owner prevents it from
// accidentally publishing through an enclosing run when its own Emitter is nil.
func emissionFrom(ctx context.Context) *emissionSession {
	session, _ := ctx.Value(emissionKey{}).(*emissionSession)
	if session == nil || session.run != runFrom(ctx) {
		return nil
	}
	return session
}

func (e *emissionSession) emit(ctx context.Context, value any) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Both of these are also refused by the emission context, which is why no test
	// can tell them apart from it: recording a failure below cancels that context
	// with the same error, and the invocation that opened the session defers the
	// cancel next to the close. They stay because the session owns its own state --
	// and because those two deferred statements are not one moment, so a yield that
	// leaked out of the invocation can arrive after the close and before the cancel.
	switch {
	case e.err != nil:
		return e.err
	case e.closed:
		return context.Canceled
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}

	index := e.index
	chunk := Chunk{
		ID:    e.id,
		Scope: slices.Clone(e.scope),
		Seq:   e.run.nextSeq(),
		Index: index,
		Value: value,
	}
	if err := e.emitter.Emit(callbackContext(ctx), chunk); err != nil {
		e.err = fmt.Errorf("emit chunk %d: %w", index, err)
		e.cancel(e.err)
		return e.err
	}
	e.index++
	return context.Cause(ctx)
}

// close refuses further chunks and reports the failure that stopped the stream.
// Cancelling the emission context is not part of that: the invocation that opened
// the session defers it, which is the one statement of it that also holds when the
// node panics. cancel remains the session's own for [emissionSession.emit], which
// cancels with the Emitter's failure as the cause.
//
// The lock is what makes this safe against a leaked yield, and only that: every
// yield an invocation admitted has already returned by the time the leaf closes,
// because the lease waits for them. So no test can put a delivery and this call in
// flight together on purpose -- the only way in is a goroutine that outlived the
// invocation that gave it the yield.
func (e *emissionSession) close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	return e.err
}

// emissionLease restricts a yield callback to one StreamFunc invocation. The
// mutex is the linearization point between admitting a yield and closing the
// invocation; close then waits for every admitted call before Run returns.
type emissionLease struct {
	mu      sync.Mutex
	active  sync.WaitGroup
	session *emissionSession
	closed  bool
	err     error
}

func (e *emissionLease) yield(ctx context.Context, value any) bool {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return false
	}
	e.active.Add(1)
	e.mu.Unlock()
	defer e.active.Done()

	var err error
	if e.session == nil {
		err = context.Cause(ctx)
	} else {
		err = e.session.emit(ctx, value)
	}
	if err == nil {
		return true
	}

	e.mu.Lock()
	if e.err == nil {
		e.err = err
	}
	e.closed = true
	e.mu.Unlock()
	return false
}

// close refuses new yields, waits for the admitted ones, and reports why the
// stream stopped. The lock is released before the wait on purpose: an in-flight
// yield takes it to record its own failure, so holding it across Wait would
// deadlock the invocation this is trying to end.
func (e *emissionLease) close() error {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	e.active.Wait()

	e.mu.Lock()
	err := e.err
	e.mu.Unlock()
	return err
}
