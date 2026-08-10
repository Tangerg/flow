# flow

`flow` is a small, type-safe control-flow library for Go. Compose ordinary
functions into reusable nodes, then opt into `workflow` when execution needs
named state, runtime definitions, streaming identity, or checkpointed
resumption.

The library is in-process and explicit:

- one protocol: `Run(context.Context, input) (output, error)`;
- compile-time types for Go-defined pipelines;
- runtime DAGs, named ports, and strict JSON Schema validation when needed;
- checkpointed suspension and resumption without a resident scheduler; and
- standard cancellation and error inspection with `context`, `errors.Is`, and
  `errors.As`.

## Install

`flow` requires Go 1.26 or newer.

```sh
go get github.com/Tangerg/flow
```

## Choose the smallest package

| Package | Use it for |
| --- | --- |
| [`flow`](.) | Typed sequence, selection, iteration, mapping, and races |
| [`flowx`](./flowx) | Derived conveniences such as fan-out, fallback, and same-type chains |
| [`workflow`](./workflow) | Named state, runtime definitions, streaming, and resumption |
| [`workflow/expr`](./workflow/expr) | Optional data-driven branch and loop rules |
| [`workflow/diagram`](./workflow/diagram) | Deterministic ASCII and Mermaid Graph renderings |

Start with `flow`. Use `workflow` when the definition must be assembled at run
time or execution needs its named, observable, resumable step boundaries.

## Quick start

Adapt ordinary functions with `NodeFunc`, then connect unlike types with
`Then`. The complete runnable version is
[`example/node_test.go`](./example/node_test.go):

```go
package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Tangerg/flow"
)

func main() {
	parse := flow.NodeFunc[string, int](
		func(_ context.Context, in string) (int, error) {
			return strconv.Atoi(strings.TrimSpace(in))
		},
	)
	double := flow.NodeFunc[int, int](
		func(_ context.Context, in int) (int, error) {
			return in * 2, nil
		},
	)

	pipeline := flow.Then(parse, double)
	out, err := pipeline.Run(context.Background(), " 21 ")
	if err != nil {
		panic(err)
	}
	fmt.Println(out) // 42
}
```

`pipeline` is a `Node[string, int]`. A composition remains a node, so it can be
run, tested, wrapped, or composed again.

## Typed composition

The root package revolves around one interface:

```go
type Node[I, O any] interface {
	Run(ctx context.Context, in I) (O, error)
}
```

| API | Meaning |
| --- | --- |
| `NodeFunc` | Adapt a function to `Node` |
| `Validate` | Check the complete visible definition without running it |
| `Then` | Pass one node's output to the next |
| `Switch` | Select a node from the current input |
| `Loop` | Repeat a node until a condition is met |
| `Map` | Run one node for every element and preserve input order |
| `Race` | Run several nodes and return the first success |

`Map` and `Race` own the goroutines they start, propagate cancellation, and
wait for started calls to return. Cancellation remains cooperative: nodes must
observe the context they receive.

Composites from `flow`, `flowx`, and `workflow` expose recursive definition
validation through `flow.Validate`. A caller-defined composite can participate
with a pure, concurrency-safe `Validate() error` method; an ordinary node
remains opaque. This distinction lets a replaying boundary reject an invalid
visible definition without running application code.

`flowx` contains only derived composition shapes: `Chain`, `FanOut`, `Combine`,
and `Fallback`. Retry, timeout, tracing, and circuit breaking are policies;
implement them as `Node[I, O]` decorators or use a dedicated package.

See [Nodes and `Then`](./docs/tutorials/01-node-and-then.md) and
[Composition and concurrency](./docs/tutorials/02-composition-and-concurrency.md)
for the typed path.

## Runtime-defined workflows

`workflow.Step` is an alias for `flow.Node[Store, Store]`. A leaf reads a named
value and publishes its result under its own ID. The complete runnable version
is [`example/workflow_test.go`](./example/workflow_test.go):

```go
clean := workflow.LeafFunc(
	"clean",
	workflow.Output("input"),
	func(_ context.Context, in string) (string, error) {
		return strings.TrimSpace(in), nil
	},
)
greet := workflow.LeafFunc(
	"greet",
	workflow.Output("clean"),
	func(_ context.Context, name string) (string, error) {
		return "hello, " + name, nil
	},
)

out, err := workflow.Sequence(clean, greet).Run(
	ctx,
	workflow.NewStore().WithOutput("input", " Ada "),
)
if err != nil {
	return err
}
message, err := workflow.Get[string](out, workflow.Output("greet"))
```

The lower-level `Leaf` keeps data preparation and computation separate:
`Binder[I]` reads a typed input from the Store, and `flow.Node[I, O]` computes
the output. `From` and `FirstOf` provide definition-aware binders; `BinderFunc`
adapts an application function when binding is custom. A stateful custom Binder
can join replay-safe definition checks with the same pure `Validate() error`
method used by composite nodes; `Ref.Validate` supplies the canonical check for
references it retains.

`Resolver` is only a semantic name for `flow.Node[Store, string]`. A resolver
can therefore be composed with `flow.Then` and passed unchanged to `Branch`,
`Route`, or `Registry.RegisterResolver`; adapt a function with `flow.NodeFunc`.

The Store is immutable and copy-on-write. Stored values are shared as-is, so
treat maps, slices, pointers, and other mutable values as immutable after
insertion. Prefer `workflow.Get[T]` for typed reads, including after a Store has
been serialized and restored.

Runtime definitions have two complementary forms:

| Form | Best for |
| --- | --- |
| `Graph` | Flat DAGs with named-port edges, bounded concurrency, and conditional routes |
| `Spec` | Nested sequence, parallel, branch, loop, iteration, and sealed subgraphs |

A code-built or compiled Step can describe itself with `workflow.Describe`.
Descriptions use the same typed `workflow.Kind` values as `Spec`, for example
`workflow.KindGraph` and `workflow.KindSubgraph`, so tooling does not compare
undocumented string literals.

A `Registry` is the capability boundary between external data and executable Go
code. Both definition forms compile to an ordinary `Step`:

```text
JSON -> strict decode -> JSON Schema -> Registry validation -> Step
```

```go
step, err := registry.CompileGraphJSON(data)
```

`CompileGraphJSON` includes strict decoding, JSON Schema validation, and
Registry checks. Use `ValidateGraphJSON` by itself only at a boundary that has
the document bytes but no Registry, such as an editor or generic API ingress.

`GraphJSONSchema` and `SpecJSONSchema` return self-contained Draft 2020-12
schemas for editors and API endpoints. Graph input ports are also dependency
edges: ready nodes start as soon as their own dependencies finish, subject to
`Graph.Concurrency`. Registry factories return one named, Store-sealed boundary;
`Subgraph` is how a composite region crosses that boundary with declared inputs
and one projected result.

The tutorials cover [Stores and references](./docs/tutorials/03-workflow-store-and-ref.md),
[Graph compilation](./docs/tutorials/04-graph-registry-and-ports.md),
[the JSON DSL](./docs/tutorials/05-json-dsl-and-schema.md), and
[conditional graphs](./docs/tutorials/09-conditional-graphs-and-diagrams.md).

## Per-run services

Definitions are reusable. Observation, streaming output, and replay state
belong to one call:

```go
out, err := workflow.Run(ctx, step, input, workflow.RunConfig{
	Observer: observer,
	Emitter:  emitter,
	Journal:  journal,
})
```

| Service | Purpose |
| --- | --- |
| `Observer` | Low-volume observable-boundary events |
| `Emitter` | High-volume intermediate application values |
| `Journal` | Completed boundaries, decisions, and interrupt responses |

`StreamFunc` remains an ordinary typed `Node`; `Leaf` gives its chunks workflow
identity. Emission is synchronous and applies backpressure. If the Emitter
fails, its error remains the stream's cause even when a producer ignores the
stopped stream and returns another error. Chunks describe attempted output, not
durable delivery: rerunning an incomplete leaf may repeat a prefix. See
[Streaming output](./docs/tutorials/08-streaming-output.md).

`Await`, `Interrupt`, and `Suspend` stop a run with an error matching
`ErrSuspended`. Persist the returned Store and Journal when their application
values have a faithful JSON round trip, record the external response, and run
the same definition again. Replay restores completed
boundaries instead of serializing a Go call stack. See
[Suspension and resumption](./docs/tutorials/07-suspension-and-resumption.md).

Store and Journal JSON are persistence values, not an application run record.
Persist the application run ID, active waits, authorization and status, the
workflow-definition version, and the flow module version separately. A Journal
document carries its own wire-format version and unsupported versions are
rejected. `Suspension` and `JournalKey` provide strict, atomic JSON for the wait
identity and callback correlation data stored in that run record. Admit only
one active `Run` for a logical execution; Journal locking
protects concurrent branches inside that run but is not a distributed lease for
competing runs.

## Execution boundary

Public package dependencies point toward the typed core:

```text
workflow/diagram ---> workflow ---> flow
workflow/expr ------> workflow
flowx ---------------------------> flow
```

There is no central orchestrator object or background scheduler. The project
does not provide distributed workers, queues, timers, leases, workflow
migration, deterministic instruction-level replay, or exactly-once external
effects.

Resumption is checkpoint-and-restart at Step boundaries. If a process stops
after an external side effect succeeds but before its result is journaled, the
effect may run again. Use idempotency keys, a transactional outbox, or domain
compensation. Choose a durable workflow service when distributed scheduling and
durable timers are requirements.

## Documentation

- [Tutorials](./docs/tutorials/README.md): progressive paths from one node to
  runtime-defined, resumable workflows.
- [Executable examples](./example/README.md): public-API examples with asserted
  output.
- [Documentation index](./docs/README.md): API reference and maintainer
  documents.
- [Roadmap](./docs/roadmap.md): remaining stabilization work and engine
  boundaries.
- [Changelog](./CHANGELOG.md): user-visible work for the next release.
- [Contributing](./CONTRIBUTING.md): development and API-review requirements.

Package comments and examples are the canonical API reference:

```sh
go doc github.com/Tangerg/flow
go doc github.com/Tangerg/flow/workflow
```

## Stability

The project follows Semantic Versioning. Before v1, minor releases may refine
public APIs and will document migrations in the changelog. After v1, exported
behavior and error contracts become compatibility commitments.

Use keyed fields for exported structs and prefer constructors such as `Output`,
`At`, `Item`, and `ItemIndex` where provided.

## Development

Run the local gate before submitting a change:

```sh
test -z "$(gofmt -l .)"
go mod tidy -diff
go test -race -coverprofile=coverage.out ./...
test "$(go tool cover -func=coverage.out | awk '/^total:/ { print $3 }')" = "100.0%"
go vet ./...
```

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the complete checks and design
boundaries.
