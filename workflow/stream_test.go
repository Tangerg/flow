package workflow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Tangerg/flow"
	"github.com/Tangerg/flow/workflow"
)

func TestStreamLeaf_emitsChunksAndPublishesFinalOutput(t *testing.T) {
	node := workflow.StreamNodeFunc[int, int, string](
		func(_ context.Context, input int, yield func(string) bool) (int, error) {
			for index := range input {
				if !yield(fmt.Sprintf("chunk-%d", index)) {
					return 0, errors.New("unexpected stopped stream")
				}
			}
			return input * 10, nil
		},
	)
	step := workflow.StreamLeaf(
		"stream",
		workflow.From[int](workflow.Output("start")),
		node,
	)

	type signal struct {
		kind  workflow.EventKind
		seq   uint64
		chunk *workflow.Chunk
	}
	var signals []signal
	cfg := workflow.RunConfig{
		Observer: workflow.ObserverFunc(func(_ context.Context, event workflow.Event) {
			signals = append(signals, signal{kind: event.Kind, seq: event.Seq})
		}),
		Emitter: workflow.EmitterFunc(func(_ context.Context, chunk workflow.Chunk) error {
			captured := chunk
			signals = append(signals, signal{seq: chunk.Seq, chunk: &captured})
			return nil
		}),
	}

	ctx := workflow.WithScope(t.Context(), "outer")
	out, err := workflow.Run(
		ctx,
		step,
		workflow.NewStore().WithOutput("start", 3),
		cfg,
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := workflow.Get[int](out, workflow.Output("stream")); err != nil || got != 30 {
		t.Fatalf("final output = %v, %v; want 30", got, err)
	}

	if len(signals) != 5 {
		t.Fatalf("signals = %#v; want start, three chunks, complete", signals)
	}
	var want uint64
	for index, signal := range signals {
		want++
		if signal.seq != want {
			t.Fatalf("signal %d Seq = %d; want %d", index, signal.seq, want)
		}
	}
	if signals[0].kind != workflow.EventStarted ||
		signals[4].kind != workflow.EventCompleted {
		t.Fatalf("boundary events = %q, %q; want started, completed", signals[0].kind, signals[4].kind)
	}
	var wantIndex uint64
	for index, signal := range signals[1:4] {
		chunk := signal.chunk
		if chunk == nil {
			t.Fatalf("signal %d is not a chunk", index+1)
		}
		if chunk.ID != "stream" ||
			!slices.Equal(chunk.Scope, []string{"outer"}) ||
			chunk.Index != wantIndex ||
			chunk.Value != fmt.Sprintf("chunk-%d", index) {
			t.Fatalf("chunk %d = %+v; want identified, scoped chunk", index, chunk)
		}
		wantIndex++
	}

	// Each Chunk owns its Scope.
	signals[1].chunk.Scope[0] = "changed"
	if signals[2].chunk.Scope[0] != "outer" {
		t.Fatalf("mutating one Scope changed another: %+v", signals[2].chunk.Scope)
	}
}

func TestStreamLeafFunc(t *testing.T) {
	step := workflow.StreamLeafFunc(
		"stream",
		workflow.Output("start"),
		func(_ context.Context, input int, yield func(string) bool) (int, error) {
			yield(fmt.Sprintf("value=%d", input))
			return input * 2, nil
		},
	)
	var chunks []workflow.Chunk
	out, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", 21),
		workflow.RunConfig{Emitter: workflow.EmitterFunc(
			func(_ context.Context, chunk workflow.Chunk) error {
				chunks = append(chunks, chunk)
				return nil
			},
		)},
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(chunks) != 1 || chunks[0].Value != "value=21" {
		t.Fatalf("chunks = %+v; want value=21", chunks)
	}
	if got, err := workflow.Get[int](out, workflow.Output("stream")); err != nil || got != 42 {
		t.Fatalf("output = %d, %v; want 42, nil", got, err)
	}
}

func TestStreamLeaf_emitterErrorStopsAndFailsTheLeaf(t *testing.T) {
	emitErr := errors.New("sink unavailable")
	var attempted atomic.Int64
	node := workflow.StreamNodeFunc[int, int, int](
		func(ctx context.Context, _ int, yield func(int) bool) (int, error) {
			for value := range 4 {
				attempted.Add(1)
				if !yield(value) {
					if !errors.Is(context.Cause(ctx), emitErr) {
						t.Fatalf("context cause = %v; want emitter error", context.Cause(ctx))
					}
					// A producer that accidentally tries again remains stopped
					// and cannot invoke the Emitter after its first error.
					if yield(99) {
						t.Fatal("yield after an emitter error returned true")
					}
					return 0, ctx.Err()
				}
			}
			return 4, nil
		},
	)
	step := workflow.StreamLeaf(
		"stream",
		workflow.From[int](workflow.Output("start")),
		node,
	)

	var emitted []uint64
	journal := workflow.NewJournal()
	cfg := workflow.RunConfig{
		Emitter: workflow.EmitterFunc(
			func(_ context.Context, chunk workflow.Chunk) error {
				emitted = append(emitted, chunk.Index)
				if chunk.Index == 1 {
					return emitErr
				}
				return nil
			},
		),
		Journal: journal,
	}
	in := workflow.NewStore().WithOutput("start", 1)
	out, err := workflow.Run(t.Context(), step, in, cfg)
	if !errors.Is(err, emitErr) {
		t.Fatalf("Run error = %v; want emitter error", err)
	}
	var stepErr *workflow.StepError
	if !errors.As(err, &stepErr) || stepErr.ID != "stream" || stepErr.Op != workflow.OpRun {
		t.Fatalf("Run error = %#v; want stream OpRun StepError", err)
	}
	if got := attempted.Load(); got != 2 {
		t.Fatalf("producer attempts = %d; want 2", got)
	}
	if !slices.Equal(emitted, []uint64{0, 1}) {
		t.Fatalf("emitted indexes = %v; want [0 1]", emitted)
	}
	if _, ok := out.Lookup(workflow.Output("stream")); ok {
		t.Fatal("failed stream published a final output")
	}
	if journal.Len() != 0 {
		t.Fatalf("failed stream recorded %d Journal entries; want none", journal.Len())
	}
}

func TestStreamLeaf_emitterSuspensionRemainsAThirdOutcome(t *testing.T) {
	node := workflow.StreamNodeFunc[int, int, int](
		func(ctx context.Context, input int, yield func(int) bool) (int, error) {
			if !yield(input) {
				return 0, context.Cause(ctx)
			}
			return input, nil
		},
	)
	step := workflow.StreamLeaf(
		"stream",
		workflow.From[int](workflow.Output("start")),
		node,
	)
	out, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", 1),
		workflow.RunConfig{Emitter: workflow.EmitterFunc(
			func(context.Context, workflow.Chunk) error {
				return workflow.Suspend("output destination is paused")
			},
		)},
	)
	if !workflow.SuspendedOnly(err) {
		t.Fatalf("Run error = %v; want a pure suspension", err)
	}
	waits := workflow.Suspensions(err)
	if len(waits) != 1 || waits[0].ID != "stream" ||
		waits[0].Value != "output destination is paused" {
		t.Fatalf("Suspensions = %+v; want identified emitter suspension", waits)
	}
	if _, ok := out.Lookup(workflow.Output("stream")); ok {
		t.Fatal("suspended stream published a final output")
	}
}

func TestStreamLeaf_completedReplayDoesNotReemit(t *testing.T) {
	var runs atomic.Int64
	node := workflow.StreamNodeFunc[int, int, int](
		func(_ context.Context, input int, yield func(int) bool) (int, error) {
			runs.Add(1)
			yield(input)
			yield(input + 1)
			return input + 2, nil
		},
	)
	step := workflow.StreamLeaf(
		"stream",
		workflow.From[int](workflow.Output("start")),
		node,
	)
	journal := workflow.NewJournal()
	in := workflow.NewStore().WithOutput("start", 10)

	var first []workflow.Chunk
	out, err := workflow.Run(t.Context(), step, in, workflow.RunConfig{
		Journal: journal,
		Emitter: workflow.EmitterFunc(func(_ context.Context, chunk workflow.Chunk) error {
			first = append(first, chunk)
			return nil
		}),
	})
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("first chunks = %d; want 2", len(first))
	}
	if first[0].Seq != 1 || first[1].Seq != 2 {
		t.Fatalf("chunk Seq = %d, %d; want 1, 2 without an Observer", first[0].Seq, first[1].Seq)
	}

	var replayed []workflow.Chunk
	out, err = workflow.Run(t.Context(), step, out, workflow.RunConfig{
		Journal: journal,
		Emitter: workflow.EmitterFunc(func(_ context.Context, chunk workflow.Chunk) error {
			replayed = append(replayed, chunk)
			return nil
		}),
	})
	if err != nil {
		t.Fatalf("replayed Run: %v", err)
	}
	if len(replayed) != 0 {
		t.Fatalf("replayed chunks = %+v; want none", replayed)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("node runs = %d; want 1", got)
	}
	if got, err := workflow.Get[int](out, workflow.Output("stream")); err != nil || got != 12 {
		t.Fatalf("replayed output = %v, %v; want 12", got, err)
	}
}

func TestStreamLeaf_incompleteReplayRepeatsTheAttemptPrefix(t *testing.T) {
	var runs atomic.Int64
	node := workflow.StreamNodeFunc[int, int, int](
		func(_ context.Context, input int, yield func(int) bool) (int, error) {
			run := runs.Add(1)
			yield(input)
			yield(input + 1)
			if run == 1 {
				return 0, workflow.Suspend("wait for the source")
			}
			yield(input + 2)
			return input + 3, nil
		},
	)
	step := workflow.StreamLeaf(
		"stream",
		workflow.From[int](workflow.Output("start")),
		node,
	)
	journal := workflow.NewJournal()
	in := workflow.NewStore().WithOutput("start", 10)

	var first []workflow.Chunk
	_, err := workflow.Run(t.Context(), step, in, workflow.RunConfig{
		Journal: journal,
		Emitter: workflow.EmitterFunc(func(_ context.Context, chunk workflow.Chunk) error {
			first = append(first, chunk)
			return nil
		}),
	})
	if !errors.Is(err, workflow.ErrSuspended) {
		t.Fatalf("first Run error = %v; want ErrSuspended", err)
	}
	waits := workflow.Suspensions(err)
	if len(waits) != 1 || waits[0].ID != "stream" || len(waits[0].Scope) != 0 {
		t.Fatalf("Suspensions = %+v; want root stream", waits)
	}
	if got := chunkValues(first); !slices.Equal(got, []int{10, 11}) {
		t.Fatalf("first chunks = %v; want [10 11]", got)
	}

	var second []workflow.Chunk
	out, err := workflow.Run(t.Context(), step, in, workflow.RunConfig{
		Journal: journal,
		Emitter: workflow.EmitterFunc(func(_ context.Context, chunk workflow.Chunk) error {
			second = append(second, chunk)
			return nil
		}),
	})
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if got := chunkValues(second); !slices.Equal(got, []int{10, 11, 12}) {
		t.Fatalf("second chunks = %v; want [10 11 12]", got)
	}
	var wantIndex uint64
	for index, chunk := range second {
		if chunk.Index != wantIndex {
			t.Fatalf("second chunk %d Index = %d; want reset to %d", index, chunk.Index, wantIndex)
		}
		wantIndex++
	}
	if got, err := workflow.Get[int](out, workflow.Output("stream")); err != nil || got != 13 {
		t.Fatalf("final output = %v, %v; want 13", got, err)
	}
}

func TestStreamLeaf_iterationEmitsConcurrentScopedStreams(t *testing.T) {
	body := workflow.StreamLeaf(
		"double",
		workflow.From[int](workflow.Item("items")),
		workflow.StreamNodeFunc[int, int, int](
			func(_ context.Context, input int, yield func(int) bool) (int, error) {
				yield(input)
				yield(input * 2)
				return input * 2, nil
			},
		),
	)
	step := workflow.Iteration(workflow.IterationConfig{
		ID:          "items",
		Input:       workflow.Output("start"),
		Body:        body,
		BodyOutput:  workflow.Output("double"),
		Concurrency: 4,
	})

	var mu sync.Mutex
	var chunks []workflow.Chunk
	cfg := workflow.RunConfig{Emitter: workflow.EmitterFunc(
		func(_ context.Context, chunk workflow.Chunk) error {
			mu.Lock()
			chunks = append(chunks, chunk)
			mu.Unlock()
			return nil
		},
	)}
	out, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", []any{1, 2, 3, 4}),
		cfg,
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := workflow.Get[[]int](out, workflow.Output("items")); err != nil ||
		!slices.Equal(got, []int{2, 4, 6, 8}) {
		t.Fatalf("outputs = %v, %v; want [2 4 6 8]", got, err)
	}

	if len(chunks) != 8 {
		t.Fatalf("chunks = %d; want 8", len(chunks))
	}
	byScope := make(map[string][]workflow.Chunk)
	seqs := make(map[uint64]struct{})
	for _, chunk := range chunks {
		if chunk.ID != "double" || len(chunk.Scope) != 1 {
			t.Fatalf("chunk identity = %+v; want scoped double", chunk)
		}
		byScope[chunk.Scope[0]] = append(byScope[chunk.Scope[0]], chunk)
		if chunk.Seq == 0 {
			t.Fatal("chunk has zero Seq")
		}
		if _, exists := seqs[chunk.Seq]; exists {
			t.Fatalf("duplicate Seq %d", chunk.Seq)
		}
		seqs[chunk.Seq] = struct{}{}
	}
	for index := range 4 {
		scope := fmt.Sprintf("items[%d]", index)
		stream := byScope[scope]
		if len(stream) != 2 || stream[0].Index != 0 || stream[1].Index != 1 {
			t.Fatalf("%s chunks = %+v; want indexes 0, 1", scope, stream)
		}
	}
}

func TestStreamLeaf_parallelStreamsShareRunSequence(t *testing.T) {
	makeStep := func(id string) workflow.Step {
		return workflow.StreamLeaf(
			id,
			workflow.From[int](workflow.Output("start")),
			workflow.StreamNodeFunc[int, int, int](
				func(_ context.Context, input int, yield func(int) bool) (int, error) {
					yield(input)
					yield(input + 1)
					return input + 2, nil
				},
			),
		)
	}
	step := workflow.Parallel(
		[]workflow.Step{makeStep("left"), makeStep("right")},
		workflow.ParallelConfig{},
	)

	var mu sync.Mutex
	seqs := make(map[uint64]struct{})
	cfg := workflow.RunConfig{Emitter: workflow.EmitterFunc(
		func(_ context.Context, chunk workflow.Chunk) error {
			mu.Lock()
			defer mu.Unlock()
			if _, exists := seqs[chunk.Seq]; exists {
				t.Errorf("duplicate Seq %d", chunk.Seq)
			}
			seqs[chunk.Seq] = struct{}{}
			return nil
		},
	)}
	_, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", 1),
		cfg,
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(seqs) != 4 {
		t.Fatalf("stream sequence count = %d; want 4", len(seqs))
	}
}

func TestStreamLeaf_nilEmitterDiscardsWithoutChangingEventSequence(t *testing.T) {
	node := workflow.StreamNodeFunc[int, int, int](
		func(_ context.Context, input int, yield func(int) bool) (int, error) {
			if !yield(input) || !yield(input+1) {
				return 0, errors.New("nil emitter stopped the stream")
			}
			return input + 2, nil
		},
	)
	step := workflow.StreamLeaf(
		"stream",
		workflow.From[int](workflow.Output("start")),
		node,
	)
	var events []workflow.Event
	cfg := workflow.RunConfig{Observer: workflow.ObserverFunc(
		func(_ context.Context, event workflow.Event) {
			events = append(events, event)
		},
	)}
	_, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", 1),
		cfg,
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(events) != 2 || events[0].Seq != 1 || events[1].Seq != 2 {
		t.Fatalf("events = %+v; want dense Seq 1, 2 with nil Emitter", events)
	}

	var nilEmitter workflow.EmitterFunc
	events = nil
	cfg = workflow.RunConfig{
		Observer: cfg.Observer,
		Emitter:  nilEmitter,
	}
	if _, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", 1),
		cfg,
	); err != nil {
		t.Fatalf("Run with nil EmitterFunc: %v", err)
	}
	if len(events) != 2 || events[0].Seq != 1 || events[1].Seq != 2 {
		t.Fatalf("events = %+v; want dense Seq 1, 2 with nil EmitterFunc", events)
	}

	var nilObserver workflow.ObserverFunc
	var chunks []workflow.Chunk
	cfg = workflow.RunConfig{
		Observer: nilObserver,
		Emitter: workflow.EmitterFunc(func(_ context.Context, chunk workflow.Chunk) error {
			chunks = append(chunks, chunk)
			return nil
		}),
	}
	if _, err := workflow.Run(
		t.Context(),
		step,
		workflow.NewStore().WithOutput("start", 1),
		cfg,
	); err != nil {
		t.Fatalf("Run with nil ObserverFunc: %v", err)
	}
	if len(chunks) != 2 || chunks[0].Seq != 1 || chunks[1].Seq != 2 {
		t.Fatalf("chunks = %+v; want dense Seq 1, 2 with nil ObserverFunc", chunks)
	}
}

func TestStreamLeaf_cancellationCannotPublishAPartialFinalOutput(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(errors.New("caller stopped"))
	node := workflow.StreamNodeFunc[int, int, int](
		func(_ context.Context, input int, yield func(int) bool) (int, error) {
			if yield(input) {
				t.Fatal("yield under a cancelled context returned true")
			}
			// Returning nil after observing a stopped consumer must not turn a
			// partial computation into a completed Journal record.
			return input, nil
		},
	)
	step := workflow.StreamLeaf(
		"stream",
		workflow.From[int](workflow.Output("start")),
		node,
	)
	out, err := step.Run(ctx, workflow.NewStore().WithOutput("start", 1))
	if context.Cause(ctx) == nil || !errors.Is(err, context.Cause(ctx)) {
		t.Fatalf("Run error = %v; want cancellation cause %v", err, context.Cause(ctx))
	}
	if _, ok := out.Lookup(workflow.Output("stream")); ok {
		t.Fatal("cancelled stream published a partial final output")
	}
}

func TestStreamLeaf_cancellationDuringEmissionStopsCompletion(t *testing.T) {
	stopErr := errors.New("caller stopped")
	ctx, cancel := context.WithCancelCause(t.Context())
	node := workflow.StreamNodeFunc[int, int, int](
		func(_ context.Context, input int, yield func(int) bool) (int, error) {
			if yield(input) {
				t.Fatal("yield returned true after the Emitter cancelled the run")
			}
			return input, nil
		},
	)
	step := workflow.StreamLeaf(
		"stream",
		workflow.From[int](workflow.Output("start")),
		node,
	)
	emits := 0
	out, err := workflow.Run(
		ctx,
		step,
		workflow.NewStore().WithOutput("start", 1),
		workflow.RunConfig{Emitter: workflow.EmitterFunc(
			func(context.Context, workflow.Chunk) error {
				emits++
				cancel(stopErr)
				return nil
			},
		)},
	)
	if !errors.Is(err, stopErr) {
		t.Fatalf("Run error = %v; want cancellation cause", err)
	}
	if emits != 1 {
		t.Fatalf("Emitter calls = %d; want 1", emits)
	}
	if _, ok := out.Lookup(workflow.Output("stream")); ok {
		t.Fatal("cancelled stream published a partial final output")
	}
}

func TestStreamLeaf_validatesNode(t *testing.T) {
	in := workflow.NewStore().WithOutput("start", 1)
	var nilNode workflow.StreamNode[int, int, int]
	step := workflow.StreamLeaf(
		"stream",
		workflow.From[int](workflow.Output("start")),
		nilNode,
	)
	if _, err := step.Run(t.Context(), in); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil StreamNode error = %v; want ErrNilNode", err)
	}

	var nilFunc workflow.StreamNodeFunc[int, int, int]
	step = workflow.StreamLeaf(
		"stream",
		workflow.From[int](workflow.Output("start")),
		nilFunc,
	)
	if _, err := step.Run(t.Context(), in); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil StreamNodeFunc error = %v; want ErrNilNode", err)
	}
	if _, err := nilFunc.RunStream(
		t.Context(),
		1,
		func(int) bool { return true },
	); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("nil StreamNodeFunc.RunStream error = %v; want ErrNilNode", err)
	}

	journal := workflow.NewJournal()
	if err := journal.Record(workflow.JournalKey{ID: "stream"}, 42); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if _, err := workflow.Run(
		t.Context(),
		step,
		in,
		workflow.RunConfig{Journal: journal},
	); !errors.Is(err, flow.ErrNilNode) {
		t.Fatalf("replayed nil StreamNodeFunc error = %v; want ErrNilNode", err)
	}
}

func TestEmitterFunc_nilDiscards(t *testing.T) {
	var emitter workflow.EmitterFunc
	if err := emitter.Emit(t.Context(), workflow.Chunk{}); err != nil {
		t.Fatalf("nil EmitterFunc: %v", err)
	}
}

func chunkValues(chunks []workflow.Chunk) []int {
	values := make([]int, len(chunks))
	for index, chunk := range chunks {
		values[index] = chunk.Value.(int)
	}
	return values
}
