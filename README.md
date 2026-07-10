# flow

A type-safe, composable, in-process control-flow toolkit for Go — with an
optional dynamic layer for building workflows from config or a visual editor.

`flow` is deliberately split into three layers:

| Package | What it is | Types |
| --- | --- | --- |
| [`core`](./core) | The minimal, atomic composition primitives. Compile-time typed, zero third-party dependencies. | `Node[I, O]` |
| [`flowx`](./flowx) | Derived combinators (`FanOut`, `Race`, …) and decorators (`Retry`, `Timeout`, `Trace`, …) built on `core`. | `Node[I, O]` |
| [`workflow`](./workflow) | The dynamic layer: a variable pool (`Store`) threaded through nodes addressed by ID, plus config-driven construction. | `Node[Store, Store]` |

## Install

```sh
go get github.com/Tangerg/flow
```

The current implementation requires Go 1.25 or newer.

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
editor), `workflow` threads a persistent variable pool through nodes addressed
by ID.

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

- **Persistent `Store`.** Every write returns a new structural snapshot. Values
  are shared as-is and must be treated as immutable after insertion.
- **Serializable state.** `Store` implements `json.Marshaler` and
  `json.Unmarshaler`; decoding is atomic and uses encoding/json's standard value
  representation.
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
- **Introspection.** Every composite describes its own structure via `Describe`;
  `Mermaid` renders a compiled workflow tree and `MermaidGraph` renders the
  original DAG edges.

## Architecture

Dependencies point inward, toward the stable kernel — `core` imports nothing from
the outer packages:

```
workflow ─┐
          ├─► core   (zero dependencies)
flowx ────┘
```

- `core` is the domain kernel: minimal, and already rich — behavior lives on
  concrete types (`then`, `mapNode`, …) behind the `Node` interface.
- `flowx` adds derived combinators and cross-cutting decorators (interceptors);
  it is a utility layer, not a set of domain entities, so it stays functional.
- `workflow` is the dynamic domain layer: a persistent `Store` value object,
  composite domain types (`Sequence`, `Branch`, `Loop`, `Parallel`, `Iteration`)
  that own their behavior and describe themselves, and a `Registry` that compiles
  serialized graphs into runnable steps.

## Design principles

- **Minimal core.** Only primitives that cannot be expressed in terms of the
  others. If it is derivable, it belongs in a higher layer.
- **Type-safe.** Composition is checked at compile time; no reflection in `core`.
- **Zero dependencies in `core`.** Bounded concurrency uses only the standard
  library.
- **Persistent state in `workflow`.** Store structure is copy-on-write; inserted
  values follow an explicit caller-owned immutability contract.

## Execution model

`workflow` compiles dynamic definitions into ordinary node composition before
execution. It has no central runtime scheduler:

```
Spec / Graph -> validate -> compile -> Node[Store, Store] -> Run
```

A flat Graph is compiled into topological barriers using
`Sequence(Parallel(layer)...)`. Nodes in a layer run concurrently; the next
layer starts after the whole current layer finishes. This favors a small,
deterministic in-process runtime over maximally eager DAG scheduling.

## Errors

Errors wrap their causes and are intended for `errors.Is` and `errors.As`, not
string matching. In particular:

- `core.IndexError` identifies the failing item in `Map`, `Race`, and collected
  result errors.
- `workflow.StepError` identifies the step ID and operation (`bind`, `run`, or
  `validate`).
- Sentinel errors such as `core.ErrNilNode`, `core.ErrNoCase`, and
  `core.ErrMaxIterations` remain discoverable through wrapping.

## Compatibility

The project follows semantic versioning. Before a v1 release, minor versions may
refine public APIs; release notes should call out migrations such as renamed
fields or callback signatures. After v1, exported behavior and error contracts
are compatibility commitments.

Current rewrite migrations:

- `flowx.Result.Error` is now `Result.Err`, following Go's conventional error
  field naming.
- `workflow.Condition` returns `(bool, error)` so condition evaluation failures
  are not mistaken for “keep looping”.

## Non-goals

Durability (surviving restarts / resuming from a checkpoint), distribution
(running one flow across machines), and deterministic replay are out of scope.
For those, use a workflow engine such as [Temporal](https://temporal.io). Keeping
them out is what lets `flow` stay small, fast, and easy to reason about.
