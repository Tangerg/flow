package workflow

import (
	"context"
	"fmt"
	"slices"

	"github.com/Tangerg/flow"
)

// StreamNode is a typed node that can publish intermediate values while
// computing its final result. Within each RunStream call it must call yield
// synchronously and never from more than one goroutine at a time. A false result
// means the consumer stopped: the node must stop yielding and return promptly.
// Implementations must be safe for concurrent use; keep invocation state local.
//
// Yielded values are attempts, not durable records. A resumed incomplete step
// runs again from the beginning and may repeat a prefix. A step replayed as
// complete from a [Journal] does not run and yields nothing.
type StreamNode[I, O, C any] interface {
	RunStream(context.Context, I, func(C) bool) (O, error)
}

// StreamNodeFunc adapts a function into a [StreamNode].
type StreamNodeFunc[I, O, C any] func(context.Context, I, func(C) bool) (O, error)

// StreamNodeFunc satisfies StreamNode.
var _ StreamNode[any, any, any] = StreamNodeFunc[any, any, any](nil)

// RunStream calls f. A nil StreamNodeFunc returns [flow.ErrNilNode].
func (s StreamNodeFunc[I, O, C]) RunStream(
	ctx context.Context,
	input I,
	yield func(C) bool,
) (O, error) {
	if s == nil {
		var zero O
		return zero, flow.ErrNilNode
	}
	return s(ctx, input, yield)
}

// Chunk is one intermediate value from a [StreamLeaf].
//
// ID and Path identify the leaf invocation; each Chunk owns its Path slice.
// Index starts at zero for each invocation. Seq shares a run-wide ordering with
// [Event]; callbacks from concurrent invocations may arrive out of order. Value
// is application-owned and must be treated as immutable.
type Chunk struct {
	ID    string
	Path  []string
	Seq   uint64
	Index uint64
	Value any
}

// Emitter receives chunks synchronously. Pass one to [Run] through
// [RunConfig]. Emit may be called concurrently by different leaf invocations
// and must be safe for concurrent use. A slow Emitter applies backpressure to
// the producing node. Returning an error stops that stream and returns the
// error through the leaf's normal failure or suspension classification.
type Emitter interface {
	Emit(context.Context, Chunk) error
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

// StreamLeaf turns a [StreamNode] into a [Step]. It has the same binding,
// replay, event, suspension, journal, and final-output semantics as [Leaf].
// Intermediate values go to the run's [Emitter]; a nil Emitter discards them
// before a Chunk is constructed or a sequence number is consumed.
//
// id follows the same identity rule as [Leaf]: it must be non-empty and may run
// only once in an execution scope. Apply retry or hedging to node before lifting
// it if those policies can invoke the computation more than once.
func StreamLeaf[I, O, C any](
	id string,
	bind BindFunc[I],
	node StreamNode[I, O, C],
) Step {
	return leafStep[I, O]{
		id:     id,
		bind:   bind,
		runner: streamRunner[I, O, C]{node: node},
	}
}

// StreamLeafFunc lifts an ordinary streaming function into a [Step] that reads
// its input from ref. It is the concise form of combining [StreamLeaf], [From],
// and [StreamNodeFunc].
func StreamLeafFunc[I, O, C any](
	id string,
	ref Ref,
	fn func(context.Context, I, func(C) bool) (O, error),
) Step {
	return StreamLeaf(id, From[I](ref), StreamNodeFunc[I, O, C](fn))
}

type streamRunner[I, O, C any] struct {
	node StreamNode[I, O, C]
}

func (s streamRunner[I, O, C]) validate() error {
	if s.node == nil {
		return flow.ErrNilNode
	}
	if function, ok := s.node.(StreamNodeFunc[I, O, C]); ok && function == nil {
		return flow.ErrNilNode
	}
	return nil
}

func (s streamRunner[I, O, C]) run(
	ctx context.Context,
	input I,
	invocation leafInvocation,
) (O, error) {
	emitter := invocation.run.emitter()
	if emitter == nil {
		stopped := false
		output, err := s.node.RunStream(ctx, input, func(C) bool {
			if ctx.Err() == nil {
				return true
			}
			stopped = true
			return false
		})
		if stopped && err == nil {
			var zero O
			return zero, context.Cause(ctx)
		}
		return output, err
	}

	streamCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stream := chunkStream[C]{
		ctx:     streamCtx,
		cancel:  cancel,
		run:     invocation.run,
		emitter: emitter,
		id:      invocation.id,
		path:    invocation.path,
	}
	output, err := s.node.RunStream(streamCtx, input, stream.yield)
	if stream.err != nil {
		var zero O
		return zero, stream.err
	}
	if stream.stopped && err == nil {
		var zero O
		return zero, context.Cause(streamCtx)
	}
	return output, err
}

// chunkStream owns the emission state of one StreamLeaf invocation.
type chunkStream[C any] struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	run     *runState
	emitter Emitter
	id      string
	path    []string
	index   uint64
	stopped bool
	err     error
}

func (c *chunkStream[C]) yield(value C) bool {
	if c.stopped || c.ctx.Err() != nil {
		c.stopped = true
		return false
	}

	index := c.index
	chunk := Chunk{
		ID:    c.id,
		Path:  slices.Clone(c.path),
		Seq:   c.run.nextSeq(),
		Index: index,
		Value: value,
	}
	if err := c.emitter.Emit(c.ctx, chunk); err != nil {
		c.err = fmt.Errorf("emit chunk %d: %w", index, err)
		c.stopped = true
		c.cancel(c.err)
		return false
	}
	c.index++
	if c.ctx.Err() != nil {
		c.stopped = true
		return false
	}
	return true
}
