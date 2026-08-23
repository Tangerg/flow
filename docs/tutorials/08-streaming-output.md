# Level 8: Streaming output

Most nodes have one result: they return it from `Run`. Some work also produces
useful intermediate values such as model tokens, progress updates, decoded rows,
or search matches before the final result is ready. A workflow should expose
those values without storing an iterator in the `Store` or turning lifecycle
events into a high-volume data channel.

This tutorial adds streaming to one named workflow leaf. Its runnable
counterpart is
[`example/stream_test.go`](../../example/stream_test.go).

## 1. Define the typed producer

Streaming does not introduce a second node protocol. `StreamFunc` adapts a
function with a typed yield callback directly into `flow.Node[I, O]`:

```go
generate := workflow.StreamFunc[string, string, string](
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

The three types are the input, final output, and chunk value. The final output
remains the Node result; yielded values are a run-scoped side channel that does
not feed downstream nodes or enter the Store.

When yield returns `false`, stop yielding and return promptly. This is the same
cooperative stop shape used by Go iterators: the producer owns enumeration, and
the consumer can end it. A function may call yield from multiple goroutines;
Emitter delivery is serialized for the enclosing leaf, and `Run` waits for
every in-flight yield before returning. A retained yield called after the
function returns always reports false.

`StreamFunc` is also directly callable as a function, so its production logic
can be unit-tested with a typed test callback without constructing a workflow.

## 2. Lift it into a named workflow boundary

`StreamFunc` already implements `flow.Node`, so it composes normally:

```go
parsed := flow.Then(generate, parseAnswer)
```

`Leaf` then binds the typed input and supplies the one named workflow boundary:

```go
step := workflow.Leaf(
	"generate",
	workflow.Output("prompt").Bind[string](),
	parsed,
)
```

The final result of `parsed` is written under
`workflow.Output("generate")`. Binding errors, node errors, lifecycle events,
suspension, Journal replay, and final output publication all follow the ordinary
Leaf path.

Chunks are owned by the enclosing Leaf, not by opaque nodes inside the typed
composition. If `generate` and `parseAnswer` need separate workflow identities,
Journal checkpoints, or lifecycle events, lift them as two separate leaves.
Composed StreamFunc values may also use different chunk types; their producer
callbacks stay typed, while the shared Emitter receives each value as `any`.
Use an application-defined tagged value or separate leaves when the sink must
distinguish them.

A StreamFunc run outside a Leaf has no workflow identity, so its yielded values
are discarded even if an enclosing `workflow.Run` has an Emitter.

Like every named step, this step may run only once in a scope during one run.
Apply retry and hedging inside the typed Node before calling `Leaf`.

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
`errors.As`. That emitter error remains the cause even if a faulty producer
ignores the stopped stream and returns another error afterward. As at any leaf
boundary, an error consisting only of workflow
suspensions remains the third outcome rather than becoming a failure.

Calls from one leaf invocation are serialized and receive increasing `Index`
values in delivery order. An Emitter must not wait for a later chunk from that
same invocation: serialized delivery means the producer cannot deliver it until
the current call returns. Different leaves and iteration elements may emit
concurrently, so an Emitter that mutates shared state must still protect it:

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
the workflow has no output destination in that call.

## 4. Use chunk identity correctly

Each Chunk carries:

- `ID`: the enclosing `Leaf` ID.
- `Scope`: enclosing `ScopeFrame` values. `Indexed` and `Index` distinguish a
  repeated invocation from an ordinary namespace.
- `Index`: a zero-based counter for this leaf invocation.
- `Seq`: a run-wide number shared with lifecycle `Event` values.
- `Value`: the typed chunk stored as `any`.

`ID`, `Scope`, and `Index` distinguish concurrent streams inside one run.
`Seq` lets a caller combine event and chunk logs after concurrent callbacks;
either receiver can see gaps occupied by the other signal type. Treat `Value`
and `Scope` as immutable. Do not parse `ScopeFrame.String()` for identity; use
the structured fields.

These fields do not identify a business run globally. A durable sink should
also record an application run ID and workflow-definition version.

## 5. Understand replay

The Journal checkpoints final leaf results, not chunks:

- A completed leaf is replayed without running its Node and emits no chunks.
- A failed or suspended leaf is incomplete. The next run starts it again at
  index zero and may repeat the prefix emitted by the earlier attempt.
- An emitter error leaves no final output or Journal record.

This is intentional checkpoint-and-restart behavior. Do not describe chunk
delivery as exactly once. If repeated attempt output matters, make the external
sink idempotent using application identity plus `ID`, `Scope`, and `Index`.

## 6. Keep streaming separate from observation

`Observer` reports a small number of observable boundary transitions: started,
completed, failed, suspended, skipped, or bypassed. Leaves and wait boundaries
are observable; structural composites such as sequences and parallels remain
transparent. `Emitter` carries potentially high-volume application data and can
fail the step when its destination fails. Combining them would either make
observation unexpectedly affect correctness or make streaming failures
invisible.

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
[Next: Conditional graphs and diagrams](./09-conditional-graphs-and-diagrams.md)
