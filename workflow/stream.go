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
// until every in-flight yield returns, and a retained yield called after the
// function returns reports false without reaching the Emitter. Implementations
// must otherwise be safe for concurrent use and keep invocation state local.
type StreamFunc[I, O, C any] func(
	context.Context,
	I,
	func(C) bool,
) (O, error)

// StreamFunc satisfies flow.Node.
var _ flow.Node[any, any] = StreamFunc[any, any, any](nil)

// Run calls f with the current leaf invocation's typed yield function. A nil
// StreamFunc returns [flow.ErrNilNode]. If the function ignores a false yield
// and returns success, Run returns the reason the stream stopped instead.
func (s StreamFunc[I, O, C]) Run(ctx context.Context, input I) (O, error) {
	if s == nil {
		var zero O
		return zero, flow.ErrNilNode
	}

	emission := emissionLease{session: emissionFrom(ctx)}
	output, err := s(ctx, input, func(value C) bool {
		return emission.yield(ctx, value)
	})
	if emissionErr := emission.close(); emissionErr != nil && err == nil {
		var zero O
		return zero, emissionErr
	}
	return output, err
}

// Chunk is one intermediate value from a streaming [flow.Node].
//
// ID and Scope identify the enclosing leaf invocation; each Chunk owns its
// Scope slice. Index starts at zero for each invocation. Seq shares a run-wide
// ordering with [Event]; callbacks from concurrent leaf invocations may arrive
// out of order. Value is application-owned and must be treated as immutable.
type Chunk struct {
	ID    string
	Scope []string
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
// classification.
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
	mu      sync.Mutex
	run     *runState
	cancel  context.CancelCauseFunc
	emitter Emitter
	id      string
	scope   []string
	index   uint64
	closed  bool
	err     error
}

func withEmission(
	ctx context.Context,
	run *runState,
	id string,
	scope []string,
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

	switch {
	case e.err != nil:
		return e.err
	case e.closed:
		return context.Canceled
	case context.Cause(ctx) != nil:
		return context.Cause(ctx)
	}

	index := e.index
	chunk := Chunk{
		ID:    e.id,
		Scope: slices.Clone(e.scope),
		Seq:   e.run.nextSeq(),
		Index: index,
		Value: value,
	}
	// An Emitter consumes output; it is not part of the producing node. Mask
	// the session so reusing the callback context cannot emit recursively under
	// this leaf's identity (and deadlock on the serialization lock).
	emitterCtx := context.WithValue(ctx, emissionKey{}, (*emissionSession)(nil))
	if err := e.emitter.Emit(emitterCtx, chunk); err != nil {
		e.err = fmt.Errorf("emit chunk %d: %w", index, err)
		e.cancel(e.err)
		return e.err
	}
	e.index++
	return context.Cause(ctx)
}

func (e *emissionSession) close() error {
	e.mu.Lock()
	e.closed = true
	err := e.err
	e.mu.Unlock()
	e.cancel(nil)
	return err
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
