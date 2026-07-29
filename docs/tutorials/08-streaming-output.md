# Level 8: Streaming output

Most nodes have one result: they return it from `Run`. Some work also produces
useful intermediate values — model tokens, progress updates, decoded rows, or
search matches — before the final result is ready. A workflow should expose
those values without storing an iterator in the `Store` or turning lifecycle
events into a high-volume data channel.

This tutorial adds streaming to one named workflow leaf. Its runnable
counterpart is
[`example/stream_test.go`](../../example/stream_test.go).

## 1. Define the typed producer

`StreamNode` is the streaming counterpart to `flow.Node`:

```go
type StreamNode[I, O, C any] interface {
	RunStream(
		context.Context,
		I,
		func(C) bool,
	) (O, error)
}
```

The three types are the input, final output, and chunk value. Adapt an ordinary
function with `StreamNodeFunc`:

```go
generate := workflow.StreamNodeFunc[string, string, string](
	func(ctx context.Context, prompt string, yield func(string) bool) (string, error) {
		var answer strings.Builder
		for _, token := range tokenize(prompt) {
			if !yield(token) {
				return "", context.Cause(ctx)
			}
			answer.WriteString(token)
		}
		return answer.String(), nil
	},
)
```

Call `yield` synchronously, from the goroutine running `RunStream`. When it
returns `false`, stop yielding and return promptly. This is the same cooperative
stop shape used by Go iterators: the producer owns enumeration, and the
consumer can end it.

## 2. Lift it into a named workflow boundary

`StreamLeaf` binds the typed input and gives the node all ordinary leaf
semantics:

```go
step := workflow.StreamLeaf(
	"generate",
	workflow.From[string](workflow.Output("prompt")),
	generate,
)
```

The final `string` is written under `workflow.Output("generate")`. Binding
errors, node errors, lifecycle events, suspension, Journal replay, and final
output publication follow the same path as `Leaf`; streaming is not a second
execution engine.

Like every named step, this step may run only once in a scope during one run.
Apply retry and hedging inside the typed node before calling `StreamLeaf`.

## 3. Attach an output destination to the run

An `Emitter` is configured per run:

```go
cfg := workflow.RunConfig{
	Emitter: workflow.EmitterFunc(
		func(ctx context.Context, chunk workflow.Chunk) error {
			return websocket.Send(ctx, chunk.Value)
		},
	),
}

out, err := workflow.Run(ctx, step, in, cfg)
```

`Emit` is synchronous. If the destination is slow, the producer waits: this is
backpressure, not an unbounded internal queue. If `Emit` returns an ordinary
error, the stream context is cancelled, `yield` returns `false`, and the leaf
fails with a `StepError` that preserves the emitter error for `errors.Is` and
`errors.As`. As at any leaf boundary, an error consisting only of workflow
suspensions remains the third outcome rather than becoming a failure.

Different leaves and iteration elements may emit concurrently. An Emitter that
mutates state must protect that state:

```go
var mu sync.Mutex
var chunks []workflow.Chunk

emitter := workflow.EmitterFunc(
	func(_ context.Context, chunk workflow.Chunk) error {
		mu.Lock()
		chunks = append(chunks, chunk)
		mu.Unlock()
		return nil
	},
)
```

A nil `RunConfig.Emitter` discards intermediate values before constructing a
`Chunk` or consuming a run sequence number. The producer still computes them;
the workflow simply has no output destination.

## 4. Use chunk identity correctly

Each Chunk carries:

- `ID`: the `StreamLeaf` ID.
- `Path`: enclosing loop or iteration scopes.
- `Index`: a zero-based counter for this leaf invocation.
- `Seq`: a run-wide number shared with lifecycle `Event` values.
- `Value`: the typed chunk stored as `any`.

`ID`, `Path`, and `Index` distinguish concurrent streams inside one run.
`Seq` lets a caller combine event and chunk logs after concurrent callbacks;
either receiver can see gaps occupied by the other signal type. Treat `Value`
and `Path` as immutable.

These fields do not identify a business run globally. A durable sink should
also record an application run ID and workflow-definition version.

## 5. Understand replay

The Journal checkpoints final leaf results, not chunks:

- A completed leaf is replayed without running its StreamNode and emits no
  chunks.
- A failed or suspended leaf is incomplete. The next run starts it again at
  index zero and may repeat the prefix emitted by the earlier attempt.
- An emitter error leaves no final output or Journal record.

This is intentional checkpoint-and-restart behavior. Do not describe chunk
delivery as exactly once. If repeated attempt output matters, make the external
sink idempotent using application identity plus `ID`, `Path`, and `Index`.

## 6. Keep streaming separate from observation

`Observer` reports a small number of lifecycle transitions: started, completed,
failed, suspended, or skipped. `Emitter` carries potentially high-volume
application data and can fail the step when its destination fails. Combining
them would either make observation unexpectedly affect correctness or make
streaming failures invisible.

Use the final Store output for downstream workflow steps, the Emitter for live
incremental delivery, and the Observer for tracing and audit metadata.

## Exercise

Change the example Emitter so it returns a sentinel error at chunk index two.
Verify all three properties:

1. `errors.Is(err, sentinel)` is true.
2. No final output exists at `workflow.Output("generate")`.
3. The producer receives `false` from `yield` and observes the sentinel through
   `context.Cause(ctx)`.

[Previous: Suspension and resumption](./07-suspension-and-resumption.md) ·
[Tutorial index](./README.md)
