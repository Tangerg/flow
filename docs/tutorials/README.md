# flow tutorials

This series starts with one typed function and progresses to a JSON-defined,
resumable workflow. Each level corresponds to an executable example in the
[`example`](../../example/README.md) package. The tutorials explain the model
and its trade-offs; the examples are the runnable source of truth.

Install Go 1.25 or newer, then run the complete learning path from the
repository root:

```sh
go test ./example -run Example -v
```

## Learning path

| Level | Tutorial | Executable example | Outcome |
| --- | --- | --- | --- |
| 0 | [Getting started](./00-getting-started.md) | — | Choose the smallest appropriate package |
| 1 | [Nodes and `Then`](./01-node-and-then.md) | [`node_test.go`](../../example/node_test.go) | Build a typed two-stage pipeline |
| 2 | [Composition and concurrency](./02-composition-and-concurrency.md) | [`composition_test.go`](../../example/composition_test.go) | Compose concurrent work as another node |
| 3 | [Stores, references, and steps](./03-workflow-store-and-ref.md) | [`workflow_test.go`](../../example/workflow_test.go) | Connect named steps through a dynamic store |
| 4 | [Registries, ports, and DAGs](./04-graph-registry-and-ports.md) | [`dag_test.go`](../../example/dag_test.go) | Register node types and compile a DAG |
| 5 | [The JSON DSL and schemas](./05-json-dsl-and-schema.md) | [`json_test.go`](../../example/json_test.go) | Safely compile an external workflow definition |
| 6 | [Data-driven rules](./06-data-driven-rules.md) | [`rules_test.go`](../../example/rules_test.go) | Move routing policy into configuration |
| 7 | [Suspension and resumption](./07-suspension-and-resumption.md) | [`resume_test.go`](../../example/resume_test.go) | Resume a workflow across process boundaries |

Read the series in order on your first pass. If your workflow is defined in Go,
Levels 1 and 2 may be all you need. Continue into `workflow` only when the
definition itself must be assembled at run time.

## Principles used throughout

1. **Compose first.** Every composition is another `Node`; no global
   orchestrator is required to nest it again.
2. **Keep boundaries explicit.** Context, input, output, and failure all travel
   through the method signature.
3. **Pay only for what you use.** Start with `flow`; introduce `workflow` only
   for named state, runtime definitions, or resumption.

[Documentation index](../README.md)
