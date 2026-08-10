# flow tutorials

The series starts with one typed function and ends with JSON-defined,
resumable workflows. Each implementation level has an output-checked counterpart
in the [`example`](../../example/README.md) package.

Install Go 1.26 or newer, then run every example from the repository root:

```sh
go test ./example -run Example -v
```

## Choose a path

### Typed Go

Read Levels 0–2 when control flow ships with the binary. This path keeps every
edge typed and introduces no dynamic Store.

### Runtime DAGs

Read Levels 0–4, then 9–10 when an application assembles Graph values in Go or
drives a visual editor.

### Configuration-driven

Read Levels 0–6, then 9–10 for JSON Graph or Spec definitions and optional
data-driven rules.

### Long-running calls

After Level 3, read Levels 7–8 for checkpointed resumption and incremental
output. These are independent capabilities; use either or both.

## Complete learning path

| Level | Tutorial | Executable example | Outcome |
| --- | --- | --- | --- |
| 0 | [Getting started](./00-getting-started.md) | None | Choose the smallest package |
| 1 | [Nodes and `Then`](./01-node-and-then.md) | [`Example_node`](../../example/node_test.go) | Build a typed pipeline |
| 2 | [Composition and concurrency](./02-composition-and-concurrency.md) | [`Example_composition`](../../example/composition_test.go) | Compose bounded concurrent work |
| 3 | [Stores, references, and steps](./03-workflow-store-and-ref.md) | [`Example_workflow`](../../example/workflow_test.go) | Connect named steps through a Store |
| 4 | [Registries, ports, and DAGs](./04-graph-registry-and-ports.md) | [`Example_dag`](../../example/dag_test.go) | Register node types and compile a DAG |
| 5 | [The JSON DSL and schemas](./05-json-dsl-and-schema.md) | [`Example_jsonDSL`](../../example/json_test.go) | Compile an external definition safely |
| 6 | [Data-driven rules](./06-data-driven-rules.md) | [`Example_rules`](../../example/rules_test.go) | Move routing policy into data |
| 7 | [Suspension and resumption](./07-suspension-and-resumption.md) | [`Example_resume`](../../example/resume_test.go) | Resume across process boundaries |
| 8 | [Streaming output](./08-streaming-output.md) | [`Example_streamingOutput`](../../example/stream_test.go) | Deliver incremental output with backpressure |
| 9 | [Conditional graphs and diagrams](./09-conditional-graphs-and-diagrams.md) | [`Example_conditionalGraph`](../../example/routing_test.go) | Route and merge a flat DAG |
| 10 | [Dependency-driven graphs and sealed subgraphs](./10-dependency-graphs-and-subgraphs.md) | [`Example_subgraph`](../../example/subgraph_test.go), [`Example_customRepeatedComposite`](../../example/custom_composite_test.go) | Reuse an isolated region and preserve custom composite identity |

The tutorials explain contracts and trade-offs. The examples are the runnable
source of truth.

## Principles

1. Every composition is another `Node`; nesting is the orchestration model.
2. Context, input, output, and failure remain explicit.
3. Start with `flow`; add `workflow` only for named state, runtime definitions,
   streaming identity, or resumption.
4. Keep application scheduling, persistence infrastructure, and side-effect
   policy outside the engine.

[Documentation index](../README.md)
