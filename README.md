# flow

`flow` is a small, type-safe control-flow library for Go. Compose ordinary
functions into reusable nodes; add the optional `workflow` layer only when a
definition must come from JSON, a database, or a visual editor.

The library is in-process and explicit:

- One minimal protocol: `Run(context.Context, input) (output, error)`.
- Compile-time types for ordinary Go pipelines.
- Runtime DAGs, named ports, and strict JSON Schema validation when needed.
- Checkpointed suspension and resumption without a central scheduler.
- Standard Go cancellation and error inspection with `errors.Is` and
  `errors.As`.

## Requirements and installation

`flow` requires Go 1.25 or newer.

```sh
go get github.com/Tangerg/flow
```

## Choose the smallest package

| Package | Use it for | Primary abstraction |
| --- | --- | --- |
| [`flow`](.) | Typed sequence, selection, iteration, and concurrency | `Node[I, O]` |
| [`flowx`](./flowx) | Derived convenience shapes such as fan-out and fallback | `Node[I, O]` |
| [`workflow`](./workflow) | Named state, streaming output, JSON DSLs, and resumption | `Step`, `Store` |
| [`workflow/expr`](./workflow/expr) | Optional data-driven branch and loop rules | `Condition`, `Resolver` |
| [`workflow/diagram`](./workflow/diagram) | Deterministic Graph diagnostics and documentation | `ASCII`, `Mermaid` |

Start with `flow`. A pipeline defined in Go rarely needs the dynamic layer.

## Quick start

Adapt ordinary functions with `NodeFunc`, then connect unlike types with
`Then`:

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
run, tested, wrapped, or composed again without an orchestrator object.

## The typed core

The root package revolves around one interface:

```go
type Node[I, O any] interface {
	Run(ctx context.Context, in I) (O, error)
}
```

Its primitives are deliberately small:

| API | Meaning |
| --- | --- |
| `NodeFunc` | Adapt a function to `Node` |
| `Then` | Pass one node's output to the next |
| `Switch` | Select a node from the current input |
| `Loop` | Repeat a node until a condition is met |
| `Map` | Run a node for every element and wait for all results |
| `Race` | Run several nodes and return the first success |

`Map` and `Race` own the goroutines they start, propagate cancellation, and
wait for started calls to return. Cancellation remains cooperative: node
implementations must observe the context they receive.

Derived operations live in `flowx`:

| API | Meaning |
| --- | --- |
| `Chain` | Variadic same-type sequence |
| `FanOut` | Run several nodes on the same input |
| `Combine` | Heterogeneous two-way fan-in |
| `Fallback` | Try a primary node, then an alternate |

Retry, timeout, tracing, and circuit breaking are policies rather than core
control flow. Implement them as decorators from `Node[I, O]` to `Node[I, O]`,
or use a dedicated package.

That advice applies directly to ordinary typed nodes. Once a node is lifted
with `workflow.Leaf`, the returned Step is a named execution boundary and may
run only once per scope in one run. Apply retry or hedging decorators to the
typed business node before lifting it; use `workflow.Branch` for mutually
exclusive Step alternatives. A policy that invokes the returned Step more than
once fails with `workflow.ErrDuplicateStep`.

## Dynamic workflows

`workflow.Step` is an alias for `flow.Node[Store, Store]`. A step reads named
values through `Ref` and returns a new persistent Store snapshot.

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

pipeline := workflow.Sequence(clean, greet)
out, err := pipeline.Run(
	ctx,
	workflow.NewStore().WithOutput("input", " Ada "),
)
if err != nil {
	return err
}
message, err := workflow.Get[string](
	out,
	workflow.Output("greet"),
)
```

The Store structure is immutable and copy-on-write. Stored values are shared
as-is; treat maps, slices, pointers, and other mutable values as immutable after
insertion.

References use RFC 6901 JSON Pointers:

```go
ref := workflow.Output("load").Child("items", "0", "display/name")
fmt.Println(ref) // load#/output/items/0/display~1name
```

Prefer `workflow.Get[T]` for application reads. It preserves typed behavior
after a Store has been serialized and restored.

## Streaming output

Use `StreamLeafFunc` when a named step has a final result but also produces
incremental values such as model tokens, progress updates, or rows:

```go
generate := workflow.StreamLeafFunc(
	"generate",
	workflow.Output("prompt"),
	func(ctx context.Context, prompt string, yield func(string) bool) (string, error) {
		var answer strings.Builder
		for _, token := range []string{"hello", ", ", prompt} {
			if !yield(token) {
				return "", context.Cause(ctx)
			}
			answer.WriteString(token)
		}
		return answer.String(), nil
	},
)

out, err := workflow.Run(ctx, generate, in, workflow.RunConfig{
	Emitter: workflow.EmitterFunc(func(ctx context.Context, chunk workflow.Chunk) error {
		return send(ctx, chunk.Value)
	}),
})
```

`Emitter` is a synchronous, error-returning output boundary. A slow emitter
applies backpressure; an ordinary emitter error cancels the stream and fails its
leaf. `yield` returns `false` after cancellation or an emitter error, and
producers must stop promptly. Different leaves may emit concurrently, so an
emitter must be concurrency-safe.

Chunks carry the leaf ID, repeated-scope path, a zero-based per-invocation
index, and a run-wide sequence shared with lifecycle events. They are attempt
output rather than checkpoints: replaying a completed Journal record emits no
chunks, while rerunning an incomplete or suspended leaf starts at index zero
and may repeat a prefix. Include an application run ID and definition version
when deduplicating output outside the process.

## Runtime definitions

A `Registry` maps configuration-visible names to Go factories, schemas,
conditions, and resolvers. It is the capability boundary between external data
and executable code.

The dynamic layer has two definition forms:

| Form | Best for |
| --- | --- |
| `Graph` | A flat DAG with named-port edges and conditional routes |
| `Spec` | Nested sequence, parallel, branch, loop, iteration, and sealed subgraphs |

Both compile to an ordinary `Step`:

```text
JSON -> strict decode -> JSON Schema -> Registry validation -> Step
```

For a Graph, input ports imply dependencies. Compilation checks registrations,
configuration schemas, missing or unknown ports, edge types, duplicate IDs,
cycles, and routing outlets before any node runs. Independent nodes execute in
dependency order: a node starts as soon as all of its dependencies complete,
without waiting for unrelated branches. `Graph.Concurrency` bounds the whole
graph; zero means unbounded.

```go
if err := workflow.ValidateGraphJSON(data); err != nil {
	return err
}

step, err := registry.CompileGraphJSON(data)
if err != nil {
	return err
}
```

`GraphJSONSchema` and `SpecJSONSchema` expose self-contained Draft 2020-12
schemas for editors and API endpoints. Duplicate JSON members are rejected, and
external schema references are disabled.

`Subgraph` seals a reusable Step behind an explicit boundary. It copies declared
inputs into a fresh Store, scopes the body under the subgraph ID, and projects
one declared result back out:

```go
region := workflow.Subgraph(workflow.SubgraphConfig{
	ID:         "price",
	Inputs:     workflow.Inputs{"request": workflow.Output("order")},
	Body:       body,
	BodyOutput: workflow.Output("total"),
})
```

Inner cells never leak into the outer Store. `SubgraphFactory` exposes the same
boundary as a registered Graph node, so ordinary Graph inputs still power cycle
detection, external-input discovery, and port type checking.

A routing node publishes its selected outlet as an ordinary string output and
declares every possible value in `NodeSchema.Outlets`. Targets opt into that
control flow with `When`; the zero trigger requires every gate, while
`TriggerAny` supports a merge reached through either arm:

```go
approve := workflow.NodeSpec{
	ID: "approve", Type: "send",
	When: []workflow.Gate{
		workflow.When("route", "approve"),
	},
}
result := workflow.NodeSpec{
	ID: "result", Type: "merge",
	When: []workflow.Gate{
		workflow.When("route", "approve"),
		workflow.When("route", "review"),
	},
	Trigger: workflow.TriggerAny,
}
```

An unselected node emits `EventBypassed` and publishes no output. Bypass is
explicit; an ungated missing input remains an error. `FirstOf` is the usual
binder for a merge with mutually exclusive inputs. `Route` adapts an existing
Store-based `Resolver` into a journaled routing leaf.

Render a definition without coupling diagnostics to execution:

```go
fmt.Print(diagram.ASCII(graph))
fmt.Print(diagram.Mermaid(graph))
```

Rendering is deterministic but does not validate the Graph.

The optional `workflow/expr` package keeps simple branch and loop rules in data:

```go
resolve, err := expr.Switch(expr.SwitchSpec{
	Cases: []expr.Case{
		{When: "score.output < 60", Then: "review"},
		{When: "score.output >= 90", Then: "accept"},
	},
	Fallback: "revise",
})
```

Expressions are side-effect-free and intentionally restricted. They cannot
call application functions or access the host process.

## Suspension and resumption

Use `Await` when an external producer will write a Store value, `Interrupt` for
an explicit request-response Step, or `Suspend` from inside a node.

```go
journal := workflow.NewJournal()
cfg := workflow.RunConfig{Journal: journal}

paused, err := workflow.Run(ctx, pipeline, input, cfg)
if errors.Is(err, workflow.ErrSuspended) {
	wait := workflow.Suspensions(err)[0]

	if err := journal.Record(wait.Key(), response); err != nil {
		return err
	}
	out, err := workflow.Run(ctx, pipeline, paused, cfg)
	_ = out
	_ = err
}
```

A resumed run re-enters the workflow at its root. The Journal skips completed
Leaf boundaries, restores their outputs, and preserves branch and loop
decisions. Keys include the scope path as well as the step ID, so repeated
instances remain distinct.

Store and Journal both support JSON persistence. The application must separately
persist active suspension requests, their keys, its own run ID and status, and
the workflow definition version.

Suspension is a third outcome, not a failure. A waiting parallel branch lets its
siblings finish and reports every suspension; a real failure still cancels
siblings promptly.

Caller-defined composites can preserve that distinction with `SuspendedOnly`,
`Suspensions`, and `JoinSuspensions`. Classification walks the complete standard
Go error tree, so a mixed join of a suspension and a real failure is never
mistaken for “not yet.”

## Observation and diagnostics

Use `workflow.Run` when one call needs an Observer, Emitter, or Journal:

```go
out, err := workflow.Run(ctx, step, input, workflow.RunConfig{
	Observer: observer,
	Emitter:  emitter,
	Journal:  journal,
})
```

Events carry the step ID, scope path, per-run sequence number, elapsed time, and
produced Store. `Store.Changes` narrows one snapshot to its writes for audit or
persistence code. A compiled Step remains reusable; run configuration belongs
to the call. Use Observer for low-volume lifecycle transitions and Emitter for
high-volume intermediate values. Events and chunks share the run sequence, so
either receiver may see gaps occupied by the other signal type.

Errors preserve their causes:

- `flow.IndexError` identifies an ordered failing position.
- `workflow.StepError` identifies the step and operation.
- `RefError`, `RegistrationError`, `GraphError`, and `SpecError` retain boundary
  context.
- Sentinel errors remain discoverable through wrapping.

Use `errors.Is` and `errors.As`; do not branch on error text.

## Execution model and scope

Package dependencies point toward the typed core:

```text
workflow/diagram ---> workflow ---> flow
workflow/expr ------> workflow
flowx ---------------------------> flow
```

There is no central scheduler. Dynamic definitions are validated and compiled
into ordinary in-process node composition before execution.

The project intentionally does not provide:

- Distributed scheduling, queues, timers, or leases.
- Deterministic instruction-level replay.
- Workflow-definition migration.
- Exactly-once external side effects.

Resumption is checkpoint-and-restart at Step boundaries. If a process stops
after an external side effect succeeds but before its result is journaled, the
effect may run again. Use idempotency keys, a transactional outbox, or domain
compensation. Use a durable workflow engine when distribution and durable
timers are requirements.

## Documentation

- [Tutorials](./docs/tutorials/README.md) — Level 0 through Level 10, aligned
  with executable examples.
- [Executable examples](./example/README.md) — public-API examples with asserted
  output.
- [Documentation index](./docs/README.md) — learning, API, project, and release
  resources.
- [Changelog](./CHANGELOG.md) — unreleased work and breaking migrations.
- [Contributing](./CONTRIBUTING.md) — development and API-review requirements.

Package comments and examples are the API reference consumed by `go doc` and
pkg.go.dev:

```sh
go doc github.com/Tangerg/flow
go doc github.com/Tangerg/flow/workflow
go doc github.com/Tangerg/flow/workflow/diagram
```

## Stability

The project follows Semantic Versioning. Before v1, minor releases may refine
public APIs and will document migrations in the [changelog](./CHANGELOG.md).
After v1, exported behavior and error contracts become compatibility
commitments.

Use keyed fields for exported structs and prefer constructors such as `Output`,
`At`, `Item`, and `Index` where provided. This leaves room for compatible
additions.

## Development

Run the local gate before submitting a change:

```sh
gofmt -w .
go mod tidy -diff
go test ./...
go test -race ./...
go vet ./...
```

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the complete checks and design
boundaries.
