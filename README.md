# flow

A type-safe, composable, in-process control-flow toolkit for Go — with an
optional dynamic layer for building workflows from config or a visual editor.

`flow` is deliberately split into three layers:

| Package | What it is | Types |
| --- | --- | --- |
| [`flow`](.) | The minimal, atomic composition primitives. Compile-time typed, zero third-party dependencies. | `Node[I, O]` |
| [`flowx`](./flowx) | Derived control-flow sugar (`FanOut`, `Combine`, `Chain`, `Fallback`) built on `flow`. | `Node[I, O]` |
| [`workflow`](./workflow) | The dynamic layer: a variable pool (`Store`) threaded through nodes addressed by ID, plus config-driven construction. | `Node[Store, Store]` |
| [`workflow/expr`](./workflow/expr) | Optional. Compiles a small expression over a `Store` into a `Condition` or `Resolver`, so branch and loop rules can live in config. | — |

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
| `Loop` | iteration: repeat until done, configured by `LoopConfig` |
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
- `Combine` — heterogeneous fan-in: merge two differently typed nodes.
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

### Named input ports

A node names every value it reads. `input` is sugar for the single default port;
`inputs` wires ports by name:

```json
{"id":"total","type":"sum","inputs":{
  "left":  {"nodeID":"twice","path":"output"},
  "right": {"nodeID":"start","path":"output"}
}}
```

Naming inputs is what keeps the data flow visible to the layer above: a flat
`Graph` derives its execution order from the wired ports (no hand-written
`dependsOn`), and validation reports both incomplete wiring and incompatible
edges before anything runs. A node that instead reads extra references out of its
own `config` is invisible to both.

`Factory` binds the default port. `BindFactory` builds the `BindFunc` from the
wired ports, so a multi-input node stays statically typed:

```go
sum := workflow.BindFactory(
    func(_ struct{}, in workflow.Inputs) (workflow.BindFunc[[2]int], error) {
        left, leftOK := in.Ref("left")
        right, rightOK := in.Ref("right")
        if !leftOK || !rightOK {
            return nil, fmt.Errorf("%w: want left and right", workflow.ErrMissingPort)
        }
        return func(s workflow.Store) ([2]int, error) { /* read both */ }, nil
    },
    func(struct{}) (flow.Node[[2]int, int], error) { /* … */ },
)

reg.MustRegisterSchema("sum", workflow.NodeSchema{
    Inputs: workflow.Ports{"left": workflow.TypeNumber, "right": workflow.TypeNumber},
    Output: workflow.TypeNumber,
})
```

An unwired declared port is `ErrMissingPort`, a wired undeclared port is
`ErrUnknownPort`, and `Registry.NodeTypes`/`Registry.NodeSchema` expose the
registered vocabulary for an editor to render.

### Branch and loop rules as data

A serialized graph cannot carry closures, so `Spec` names its resolvers and
conditions and the `Registry` supplies the code — which means changing a threshold
means changing Go. The optional `workflow/expr` package closes that gap:

```go
config := []byte(`{
  "conditions": {"converged": "refine.output >= 100"},
  "switches": {"bySize": {
    "cases": [{"when": "refine.output > 500", "then": "large"}],
    "fallback": "small"
  }}
}`)

var bindings expr.Bindings
_ = json.Unmarshal(config, &bindings)
_ = bindings.Register(reg) // "converged" and "bySize" are now usable from a Spec
```

Expressions are parsed with `go/parser` and compiled to closures, so the
supported grammar *is* the compiler — there is no path that evaluates a construct
`Parse` rejected. References are `node.path` (`load.output.items[0]`); operators
are comparison, logical (short-circuiting), and arithmetic; the only functions are
`len` and `has`. No assignment, no conversions, no user calls, no way to reach the
host program. `Expr.Refs` reports what an expression reads, so a rule set can be
checked against the values a graph actually produces.

There is no implicit truthiness: a condition must evaluate to a `bool` and a
resolver to a `string`. An expression that cannot be evaluated returns an error
rather than `false`, so a broken condition is never mistaken for "keep looping".

### Suspension and resumption

A step can stop a run without failing it. `Await` waits for a value the workflow
cannot produce itself; `Suspend` exposes a string or structured application value
from inside any node:

```go
return false, workflow.Suspend(ApprovalRequest{
    Document: document.ID,
    Actions:  []string{"approve", "reject"},
})
```

For a Store gate, wait until an external producer writes the referenced value:

```go
pipeline := workflow.Sequence(write, workflow.Await("review", workflow.At("editor", "verdict")), publish)

journal := workflow.NewJournal()
ctx = workflow.WithConfig(ctx, workflow.RunConfig{Journal: journal})

paused, err := pipeline.Run(ctx, input)
if errors.Is(err, workflow.ErrSuspended) {
    for _, s := range workflow.Suspensions(err) {
        log.Printf("%s needs %s", s.ID, s.Await) // review needs editor.verdict
    }
    save(paused, journal) // both serialize
    return
}
```

Supply what was missing and run again — the `Journal` skips every step that
already finished and restores its result, so a different process can pick the run
up:

```go
out, err := pipeline.Run(ctx, restored.With("editor", "verdict", "approved"))
```

For request/response work, `Interrupt` is an explicit value-producing Step:

```go
approval := workflow.Interrupt("approval", map[string]any{
    "question": "Publish this draft?",
    "actions":  []string{"approve", "reject"},
})
pipeline := workflow.Sequence(write, approval, publish)

paused, err := pipeline.Run(ctx, input)
if errors.Is(err, workflow.ErrSuspended) {
    wait := workflow.Suspensions(err)[0]
    // ID + Path identifies this exact parallel/iteration instance.
    if err := journal.Record(wait.Key(), true); err != nil {
        return err
    }
}

out, err := pipeline.Run(ctx, paused)
approved, err := workflow.Get[bool](out, workflow.Output("approval"))
```

Unlike a function-local continuation, the Step is the replay boundary: no call
stack is retained and no interrupt-call ordinal is matched. The structured
`JournalKey` keeps repeated instances independent, and the response serializes
with the Journal.

Suspension is a **third outcome**, not a kind of failure, and the composites treat
it that way:

| | on a failure | on a suspension |
| --- | --- | --- |
| `Sequence` | stops, returns the partial `Store` | stops, returns the partial `Store` |
| `Parallel` | fail-fast, cancels siblings | siblings **run to completion**, their writes merge, every suspension is reported |
| `Iteration` | fail-fast | remaining elements finish; no partial collection is written |
| `Loop` | stops | stops; completed iterations replay from the `Journal` |

Cancelling a sibling because another branch is waiting would discard its work and
repeat its side effects on the run that resumes.

`Journal` records are keyed by **scope path plus step ID**, which is what keeps
this correct where one step runs many times — element 2 of an `Iteration` is never
mistaken for element 1. `Branch` and `Loop` additionally record the decisions they
made, so a resolver that is not a pure function of the `Store` (a classifier, a
model) cannot send a resumed run down the other branch — and is not called twice.

**What this is not**: a durable workflow engine. There is no scheduler, no timer,
and no exactly-once guarantee — a step that suspends after a side effect and
before recording its result will repeat that effect. This is
checkpoint-and-restart at step granularity, which fits an approval, a callback, or
a retry window. For sagas and durable timers, use Temporal.

### External inputs

References that name no node in the graph are the workflow's parameters. Report
them instead of discovering a missing value mid-run:

```go
workflow.GraphInputs(g)             // every external Ref, deduplicated and sorted
workflow.MissingInputs(g, store)    // the ones this Store cannot satisfy
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
    Inputs: workflow.OnePort(workflow.TypeNumber),
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
    workflow.Parallel(
        []workflow.Step{saveDB, writeAudit},
        workflow.ParallelConfig{},
    ),
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
- **Named ports.** `Inputs` wires a node's inputs by port name and `NodeSchema`
  declares them, so execution order and edge types both derive from the graph
  rather than from a node's private config.
- **Typed factories.** `Factory` strictly decodes JSON config and adapts a typed
  node constructor into the common `LeafFactory` shape; `BindFactory` does the
  same for a node reading several ports.
- **Validation.** Embedded JSON Schemas check both DSL shapes;
  `Registry.ValidateSpec` and `Registry.ValidateGraph` add registrations, config
  schemas, unique IDs, cycles, references, port completeness, and compatible edge
  types without running the workflow.
- **One config per run.** `RunConfig` is a keyed struct holding everything a
  single run needs — its `Observer` and its `Journal` — installed with one
  `WithConfig` call. It lives in the context rather than in a `Step`, because it
  belongs to the run and not to the definition: a compiled workflow is built once
  and run many times, concurrently, each with its own journal.
- **Observability.** Attach an `Observer` through `RunConfig` to receive typed
  step lifecycle events; ordinary functions can use `ObserverFunc`. Each `Event`
  carries a run sequence number, a scope path, elapsed time, and the `Store` the
  step produced.
- **Introspection.** Every composite describes its own structure via `Describe`,
  and `Registry.NodeTypes`/`Registry.NodeSchema` expose the registered node
  vocabulary, leaving rendering and presentation to callers.

## Architecture

Dependencies point inward, toward the stable root package:

```
workflow/expr ─► workflow ─┐
                           ├─► flow   (zero dependencies)
flowx ─────────────────────┘
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
- `workflow/expr` is an optional adapter, not a layer anything depends on: it
  produces ordinary `Condition` and `Resolver` values. Keeping the interpreter
  out here is what lets the core stay reflection-light and free of an expression
  language.

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
- **Out of scope, not out of reach.** Where a concern is deliberately excluded,
  the API still carries what an external implementation needs — an `Event` holds
  the sequence number, scope path, and produced `Store` a tracker or persister
  would record.

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
- `workflow.ErrMissingPort`, `ErrUnknownPort`, and `ErrDuplicatePort` identify a
  wiring mistake; port errors are reported against the `inputs` field, never
  against `config`.
- Sentinel errors such as `flow.ErrNilNode`, `flow.ErrNoCase`, and
  `flow.ErrMaxIterations` remain discoverable through wrapping.

## Compatibility

The project follows semantic versioning. Before a v1 release, minor versions may
refine public APIs; release notes should call out migrations such as renamed
fields or callback signatures. After v1, exported behavior and error contracts
are compatibility commitments.

Construct exported structs (config structs, `Ref`, `Spec`, `Event`, and the error
types) with keyed fields, and prefer the provided constructors (`At`, `Output`,
`Item`, `Index`) where they exist. This is a compatibility contract: it lets new
fields be added in a minor release without breaking callers.

Current rewrite migrations:

- `Get[T]` now **converts** rather than asserts. A value of exactly `T` is still
  returned as-is; anything else is converted through its JSON representation, so a
  typed read survives a serialized `Store` at any path depth. Conversion never
  rounds or reinterprets — reading `42.5` as an `int` still fails.
- `Store.UnmarshalJSON` decodes numbers as `json.Number` instead of `float64`, so
  nothing is rounded on the way in and an `int64` beyond float64's exact range
  survives a round trip. Code that reads a decoded `Store` with `Lookup` and a
  `float64` type assertion must switch to `Get[T]`. A number too large for any Go
  type is now accepted on decode and reported on read rather than failing the whole
  decode.
- `Branch` and `Loop` take an `id` as their first argument, and `branch` and `loop`
  require `"id"` in the JSON DSL. They record their decisions in the `Journal`
  under that ID; without one a resumed run could take a different branch or stop at
  a different iteration.
- `WithScope` is no longer a no-op when there is no observer. A scope identifies a
  step rather than labelling it, so it is always maintained — a `Journal` keys its
  records by it.
- `Event` gained the `EventSuspended` and `EventSkipped` kinds. An observer that
  switches exhaustively on `Event.Kind` needs to handle them.
- `LeafFactory` takes a single `LeafSpec` instead of positional
  `(id, input, config)` arguments, so a node receives all its wired ports and new
  fields can be added without breaking callers:
  `func(spec workflow.LeafSpec) (workflow.Step, error)`.
- `NodeSchema.Input ValueType` became `NodeSchema.Inputs Ports`, keyed by port
  name. Use `workflow.OnePort(t)` for a single-input node. A schema that declares
  ports now also makes wiring mandatory: an unwired declared port is
  `ErrMissingPort`.
- `Factory` reports `ErrMissingPort` when the default port is unwired. Such a node
  could never have run, so the failure moved from run time to compile time. Nodes
  that legitimately take no input should use a custom `LeafFactory` with their own
  `BindFunc`.
- Multi-input nodes should declare ports (`inputs` in JSON, `BindFactory` in Go)
  rather than carrying extra `Ref` values in their config. Config-carried
  references still work, but the graph cannot infer dependency order or check edge
  types for them, so `dependsOn` has to be written by hand.
- `Event` gained `Path`, `Seq`, `Elapsed`, and `Store`. Existing observers keep
  compiling; the new fields are what make an external tracker or persister
  possible.
- The former `github.com/Tangerg/flow/core` package now lives at the module root:
  import `github.com/Tangerg/flow` and use the package name `flow`.
- The former `core.Func` is now `flow.NodeFunc`, following the `http.HandlerFunc` adapter
  convention.
- Bounded operations take exactly one trailing config struct, not `N` variants
  or a pseudo-optional variadic argument: `flow.Map(node, cfg)`,
  `flow.Loop(body, cfg)`, `flowx.FanOut(nodes, cfg)`,
  `workflow.Parallel(branches, cfg)`, `workflow.Loop(id, body, done, cfg)`.
  The zero config selects the documented default. Variadic subjects (FanOut,
  Parallel) take a slice so the config stays trailing.
  `workflow.Iteration(IterationConfig{...})` is fully config-defined, so its
  config is the single required argument.
- `Journal.Keys` returns structured `JournalKey` values and `Journal.Forget`
  accepts one. Journal JSON is a versioned list of `{path,id,value}` records;
  scope and ID are never flattened into a delimiter-separated identity.
- `flowx` provides control-flow sugar only, with one implementation per shape:
  `Chain`, `FanOut`, `Combine`, and `Fallback`. Resilience (retry, timeout) and
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

Distribution (running one flow across machines) and deterministic replay are out
of scope. For those, use a workflow engine such as [Temporal](https://temporal.io).
Keeping them out is what lets `flow` stay small, fast, and easy to reason about.

Suspension and checkpointed resumption **are** supported (see above), but they are
not a durable workflow engine: there is no scheduler, no timer, and no
exactly-once guarantee. A step that suspends after a side effect and before
recording its result will repeat that effect. The unit of recovery is a step, not
an instruction.

Out of scope does not mean out of reach. An `Observer` receives, for every step,
the run sequence number, the scope path, the elapsed time, and the `Store` the
step produced — and `Store` is `json.Marshaler`, with `Store.Changes` narrowing a
snapshot to a single step's writes. A tracker or a state persister keyed by
`(partition, run, sequence, step)` is an ordinary `Observer` in your own code, not
a feature this package has to own:

```go
persist := workflow.ObserverFunc(func(ctx context.Context, e workflow.Event) {
    if e.Kind != workflow.EventCompleted {
        return
    }
    _ = save(ctx, runID, e.Seq, e.ID, e.Store) // Store marshals to JSON
})

ctx = workflow.WithConfig(ctx, workflow.RunConfig{Observer: persist})
out, err := step.Run(ctx, input)
```

What is genuinely absent is suspension: a compiled workflow runs to completion or
returns an error. There is no central driver to pause and resume, because that
driver is what a scheduler-free design trades away.
