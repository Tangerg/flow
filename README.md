# flow

A type-safe, composable, in-process control-flow toolkit for Go — with an
optional dynamic layer for building workflows from config or a visual editor.

`flow` is deliberately split into three layers:

| Package | What it is | Types |
| --- | --- | --- |
| [`flow`](.) | The minimal, atomic composition primitives. Compile-time typed, zero third-party dependencies. | `Node[I, O]` |
| [`flowx`](./flowx) | Derived control-flow sugar (`FanOut`, `Combine2`, `Chain`, `Fallback`) built on `flow`. | `Node[I, O]` |
| [`workflow`](./workflow) | The dynamic layer: a variable pool (`Store`) threaded through nodes addressed by ID, plus config-driven construction. | `Node[Store, Store]` |

## Install

```sh
go get github.com/Tangerg/flow
```

The current implementation requires Go 1.25 or newer.

## flow — typed composition

The whole package is six irreducible primitives. Everything else is derivable
and lives elsewhere.

```go
type Node[I, O any] interface {
    Run(ctx context.Context, in I) (O, error)
}
```

| Primitive | Role |
| --- | --- |
| `NodeFunc` | adapt a plain function into a `Node` |
| `Then` | sequence: run one node, feed its output to the next |
| `Switch` | selection: route to a node chosen at runtime |
| `Loop` | iteration: repeat until done, with an optional `LoopConfig` limit |
| `Map` | concurrency (AND): apply a node to every element and wait for all |
| `Race` | concurrency (OR): run nodes on one input, first success wins |

```go
double := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x * 2, nil })
addOne := flow.NodeFunc[int, int](func(_ context.Context, x int) (int, error) { return x + 1, nil })

pipe := flow.Then(double, addOne)
out, _ := pipe.Run(ctx, 10) // 21
```

These form a category: `Then` is associative and closed over `Node`, so any
composition is itself a `Node` you can `Run`. `Map` and `Race` are the two
concurrency atoms — wait-for-all (AND) and first-success (OR) — and neither is
expressible in terms of the other, so both live in the core. Convenience shapes
(fan-out, heterogeneous fan-in, variadic sequence, fallback) are derivable and
live in `flowx`, not the core.

## flowx — derived control-flow sugar

Everything derivable from the core primitives lives here, with exactly one
implementation per control-flow shape:

- `Chain` — variadic same-type sequence (sugar over `Then`).
- `FanOut` — run several nodes on the same input concurrently.
- `Combine2` — heterogeneous fan-in: merge two differently typed nodes.
- `Fallback` — run a primary node, then an alternate if it fails.

```go
// Serve a cached value when the primary node fails.
node := flowx.Fallback(callAPI, serveFromCache)
```

Resilience (retry, timeout, circuit breaking) and observability are left out on
purpose: a decorator is just a `flow.Node[I, O] -> flow.Node[I, O]`, so wrap a
node yourself or drop in a dedicated library.

## workflow — the dynamic layer

When a graph must be assembled at runtime (from config, or a drag-and-drop
editor), `workflow` threads a persistent variable pool through nodes addressed
by ID.

```go
type addConfig struct {
    N int `json:"n"`
}

addN := workflow.Factory(func(cfg addConfig) (flow.Node[int, int], error) {
    return flow.NodeFunc[int, int](func(_ context.Context, in int) (int, error) {
        return in + cfg.N, nil
    }), nil
})

reg := workflow.NewRegistry().MustRegisterLeaf("addN", addN)

graph := `{"nodes":[
  {"id":"a","type":"addN","input":{"nodeID":"start","path":"output"},"config":{"n":10}},
  {"id":"b","type":"addN","input":{"nodeID":"a","path":"output"},"config":{"n":5}}
]}`

step, _ := reg.CompileGraphJSON([]byte(graph))
out, _ := step.Run(ctx, workflow.NewStore().WithOutput("start", 1))
v, _ := workflow.Get[int](out, workflow.Output("b")) // 16
```

### JSON DSL and Schema

`Spec` is the nested control-flow form; `Graph` is the flat DAG form. Both JSON
formats have strict, embedded [JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12)
definitions:

```go
if err := workflow.ValidateGraphJSON(data); err != nil {
    // Structural error: syntax, required field, type, or unknown field.
}

schema := workflow.GraphJSONSchema() // safe copy for an editor or API endpoint
step, err := reg.CompileGraphJSON(data) // repeats structural and Registry checks
```

Node types may also declare a config schema. It is compiled once at
registration and checked before a factory is called; an omitted config is
treated as `{}` so `required` remains meaningful:

```go
reg.MustRegisterSchema("addN", workflow.NodeSchema{
    Input:  workflow.TypeNumber,
    Output: workflow.TypeNumber,
    ConfigSchema: json.RawMessage(`{
      "$schema":"https://json-schema.org/draft/2020-12/schema",
      "type":"object",
      "properties":{"n":{"type":"integer"}},
      "required":["n"],
      "additionalProperties":false
    }`),
})
```

Schemas must be self-contained: external `$ref` loading is deliberately
disabled so startup never performs hidden network or filesystem I/O. JSON
Schema diagnostics retain their instance paths; `SpecError` and `GraphError`
identify the JSON boundary, while `ErrInvalidSpec` and `ErrInvalidGraph` remain
available through `errors.Is`.

Code-defined workflows compose the primitives directly. A composite is already
a `Step`, so there is no final build call:

```go
pipeline := workflow.Sequence(
    load,
    validate,
    workflow.Parallel(saveDB, writeAudit),
    reply,
)

out, err := pipeline.Run(ctx, input)
```

Highlights:

- **Persistent `Store`.** Every write returns a new structural snapshot. Values
  are shared as-is and must be treated as immutable after insertion.
- **Serializable state.** `Store` implements `json.Marshaler` and
  `json.Unmarshaler`; decoding is atomic and uses encoding/json's standard value
  representation.
- **Composites on flow.** `Sequence`/`Branch`/`Loop`/`Parallel`/`Iteration` are
  built from root primitives; `Parallel` merges branch stores, `Iteration`
  scopes each element.
- **Config-driven.** A nested `Spec` or a flat, arbitrarily wired `Graph`
  (topologically layered, cycle-checked) compiles to a runnable `Step`.
- **Typed factories.** `Factory` strictly decodes JSON config and adapts a typed
  node constructor into the common `LeafFactory` shape.
- **Validation.** Embedded JSON Schemas check both DSL shapes;
  `Registry.ValidateSpec` and `Registry.ValidateGraph` add registrations, config
  schemas, unique IDs, cycles, references, and compatible edge types without
  running the workflow.
- **Observability.** Attach an `Observer` with `WithObserver` to receive typed
  step lifecycle events; ordinary functions can use `ObserverFunc`.
- **Introspection.** Every composite describes its own structure via `Describe`,
  leaving rendering and presentation to callers.

## Architecture

Dependencies point inward, toward the stable root package:

```
workflow ─┐
          ├─► flow   (zero dependencies)
flowx ────┘
```

- `flow` is the domain kernel: minimal, and already rich — behavior lives on
  concrete types (`then`, `mapNode`, …) behind the `Node` interface.
- `flowx` adds derived control-flow sugar (fan-out, heterogeneous fan-in, chain,
  fallback); it is a utility layer, not a set of domain entities, so it stays
  functional.
- `workflow` is the dynamic domain layer: a persistent `Store` value object,
  composite domain types (`Sequence`, `Branch`, `Loop`, `Parallel`, `Iteration`)
  that own their behavior and describe themselves, and a `Registry` that compiles
  serialized graphs into runnable steps.

## Design principles

- **Minimal flow.** Only primitives that cannot be expressed in terms of the
  others. If it is derivable, it belongs in a higher layer.
- **Type-safe.** Composition is checked at compile time; no reflection in `flow`.
- **Small interfaces.** `Node` and `Observer` are single-method contracts with
  `NodeFunc`, `BindFunc`, and `ObserverFunc` function adapters.
- **Zero dependencies in `flow`.** Bounded concurrency uses only the standard
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

- `flow.IndexError` identifies the failing item in `Map`, `Race`, and collected
  result errors.
- `workflow.StepError` identifies the step ID and operation (`bind`, `run`, or
  `validate`).
- `workflow.RefError`, `RegistrationError`, `GraphError`, and `SpecError`
  identify the exact reference, registry entry, graph field, or specification
  field that failed.
- Sentinel errors such as `flow.ErrNilNode`, `flow.ErrNoCase`, and
  `flow.ErrMaxIterations` remain discoverable through wrapping.

## Compatibility

The project follows semantic versioning. Before a v1 release, minor versions may
refine public APIs; release notes should call out migrations such as renamed
fields or callback signatures. After v1, exported behavior and error contracts
are compatibility commitments.

Current rewrite migrations:

- The former `github.com/Tangerg/flow/core` package now lives at the module root:
  import `github.com/Tangerg/flow` and use the package name `flow`.
- The former `core.Func` is now `flow.NodeFunc`, following the `http.HandlerFunc` adapter
  convention.
- Bounded operations take a config struct, not `N` variants: `flow.Map` and
  `flow.Loop` accept an optional trailing config; `flowx.FanOut` and
  `workflow.Parallel` take a leading config; `workflow.Iteration` takes an
  `IterationConfig`.
- `flowx` provides control-flow sugar only, with one implementation per shape:
  `Chain`, `FanOut`, `Combine2`, and `Fallback`. Resilience (retry, timeout) and
  observability are the caller's job — wrap a `Node`, or use a library.
- `Race` is a core concurrency primitive (`flow.Race`), the OR twin of `flow.Map`;
  it is no longer in `flowx`. The collecting `flowx.FanOutAll`/`MapAll` and their
  `Result` type were removed — error aggregation is a policy, not control flow.
- `flowx.Identity` was removed; a pass-through is a one-line `NodeFunc`, and
  `flowx.Chain()` with no nodes already returns one.
- `workflow.Adapt` and `FromRef` are now `Leaf` and `From`; custom binders are
  `BindFunc` values.
- Store reads use `Store.Lookup(Ref)` or `workflow.Get[T]`; `Output`, `Item`,
  and `Index` create the conventional references without exposing path-key
  constants.
- Registry registration methods now return errors immediately. Startup code
  that prefers fail-fast chaining can use the `MustRegister*` methods.
- Registry compilation uses explicit `CompileSpec`, `CompileSpecJSON`,
  `CompileGraph`, and `CompileGraphJSON` names; validation uses matching
  `ValidateSpec`, `ValidateGraph`, `ValidateSpecJSON`, and `ValidateGraphJSON`
  names.
- `Sink` and the three event variants are replaced by the single-method
  `Observer` contract and the `Event` value type. Use `ObserverFunc` when a
  function is enough.
- `workflow.Condition` returns `(bool, error)` so condition evaluation failures
  are not mistaken for “keep looping”.
- `Pipeline` was removed; compose sequential and parallel stages directly with
  `Sequence` and `Parallel`.
- Diagram rendering is no longer part of `workflow`; consume `Description`
  directly or render it in an integration package.
- Node metadata uses the explicit `NodeSchema` name; `Schema` is reserved as a
  general concept rather than an ambiguous exported type.

## Non-goals

Durability (surviving restarts / resuming from a checkpoint), distribution
(running one flow across machines), and deterministic replay are out of scope.
For those, use a workflow engine such as [Temporal](https://temporal.io). Keeping
them out is what lets `flow` stay small, fast, and easy to reason about.
