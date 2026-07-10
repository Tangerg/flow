# flow

A type-safe, composable, in-process control-flow toolkit for Go — with an
optional dynamic layer for building workflows from config or a visual editor.

`flow` is deliberately two layers:

| Package | What it is | Types |
| --- | --- | --- |
| [`core`](./core) | The minimal, atomic composition primitives. Compile-time typed, zero third-party dependencies. | `Node[I, O]` |
| [`flowx`](./flowx) | Derived combinators (`FanOut`, `Race`, …) and decorators (`Retry`, `Timeout`, `Trace`, …) built on `core`. | `Node[I, O]` |
| [`workflow`](./workflow) | The dynamic layer: a variable pool (`Store`) threaded through nodes addressed by ID, plus config-driven construction. | `Node[Store, Store]` |

## Install

```sh
go get github.com/Tangerg/flow
```

## core — typed composition

The whole package is six irreducible primitives. Everything else is derivable
and lives elsewhere.

```go
type Node[I, O any] interface {
    Run(ctx context.Context, in I) (O, error)
}
```

| Primitive | Role |
| --- | --- |
| `Func` | adapt a plain function into a `Node` |
| `Then` | sequence: run one node, feed its output to the next |
| `Switch` | selection: route to a node chosen at runtime |
| `Loop` | iteration: repeat until done |
| `Map` | concurrency: apply a node to every element of a slice |

```go
double := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil })
addOne := core.Func[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })

pipe := core.Then(double, addOne)
out, _ := pipe.Run(ctx, 10) // 21
```

These form a category: `Then` is associative and closed over `Node`, so any
composition is itself a `Node` you can `Run`. Convenience shapes (fan-out,
collect-all, heterogeneous fan-in, race, retry, timeout) are derivable from these
and live in `flowx`, not the core.

## flowx — derived combinators and decorators

Everything deliberately kept out of `core` lives here, built on top of it:
`FanOut`, `FanOutAll`, `MapAll`, `Combine2` (heterogeneous fan-in), `Race` (first
success wins), `Identity`, and `Chain`.

Decorators are type-preserving and compose fluently — the last applied is the
outermost at run time:

```go
node := flowx.Wrap(callAPI).
    Retry(flowx.WithAttempts(3), flowx.WithBackoff(flowx.ExponentialBackoff(50*time.Millisecond))).
    Timeout(2 * time.Second).
    Fallback(serveFromCache).
    Node()
```

## workflow — the dynamic layer

When a graph must be assembled at runtime (from config, or a drag-and-drop
editor), `workflow` threads an immutable variable pool through nodes addressed by
ID.

```go
reg := workflow.NewRegistry().RegisterLeaf("addN", addNFactory)

graph := `{"nodes":[
  {"id":"a","type":"addN","input":{"nodeID":"start","path":"output"},"config":{"n":10}},
  {"id":"b","type":"addN","input":{"nodeID":"a","path":"output"},"config":{"n":5}}
]}`

step, _ := reg.CompileJSON([]byte(graph))
out, _ := step.Run(ctx, workflow.NewStore().With("start", "output", 1))
v, _ := out.Get("b", workflow.OutputKey) // 16
```

Highlights:

- **Immutable `Store`.** Every write returns a new `Store`, so concurrent
  branches are safe by construction and every intermediate state is a snapshot.
- **Composites on core.** `Sequence`/`Branch`/`Loop`/`Parallel`/`Iteration` are
  built from the core primitives; `Parallel` merges branch stores, `Iteration`
  scopes each element.
- **Config-driven.** A nested `Spec` or a flat, arbitrarily wired `Graph`
  (topologically layered, cycle-checked) compiles to a runnable `Step`.
- **Validation.** `Registry.Validate` checks a `Graph` — unique IDs, known types,
  no cycles, and type-compatible edges via registered `Schema`s — without running
  it, for a visual editor's live feedback.
- **Observability.** Attach a `Sink` with `WithSink` to receive per-node
  start/complete/fail events.

## Design principles

- **Minimal core.** Only primitives that cannot be expressed in terms of the
  others. If it is derivable, it belongs in a higher layer.
- **Type-safe.** Composition is checked at compile time; no reflection in `core`.
- **Zero dependencies in `core`.** Bounded concurrency uses only the standard
  library.
- **Immutable state in `workflow`.** Correctness by construction over locking.

## Non-goals

Durability (surviving restarts / resuming from a checkpoint), distribution
(running one flow across machines), and deterministic replay are out of scope.
For those, use a workflow engine such as [Temporal](https://temporal.io). Keeping
them out is what lets `flow` stay small, fast, and easy to reason about.
