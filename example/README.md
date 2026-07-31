# Executable examples

These output-checked examples are the runnable counterpart to the
[tutorials](../docs/tutorials/README.md). Read or copy the smallest example that
matches the capability you need.

| Level | Example | Tutorial | Introduces |
| --- | --- | --- | --- |
| 1 | [`Example_node`](./node_test.go) | [Nodes and `Then`](../docs/tutorials/01-node-and-then.md) | `NodeFunc` and typed composition |
| 2 | [`Example_composition`](./composition_test.go) | [Composition and concurrency](../docs/tutorials/02-composition-and-concurrency.md) | Composites and bounded `Map` concurrency |
| 3 | [`Example_workflow`](./workflow_test.go) | [Stores, references, and steps](../docs/tutorials/03-workflow-store-and-ref.md) | `Step`, `Store`, `Ref`, and `Sequence` |
| 4 | [`Example_dag`](./dag_test.go) | [Registries, ports, and DAGs](../docs/tutorials/04-graph-registry-and-ports.md) | Runtime node types, ports, schemas, fan-out, and fan-in |
| 5 | [`Example_jsonDSL`](./json_test.go) | [The JSON DSL and schemas](../docs/tutorials/05-json-dsl-and-schema.md) | Strict JSON Graph compilation |
| 6 | [`Example_rules`](./rules_test.go) | [Data-driven rules](../docs/tutorials/06-data-driven-rules.md) | Expression-based routing |
| 7 | [`Example_resume`](./resume_test.go) | [Suspension and resumption](../docs/tutorials/07-suspension-and-resumption.md) | Interrupts, persistence, and Journal replay |
| 8 | [`Example_streamingOutput`](./stream_test.go) | [Streaming output](../docs/tutorials/08-streaming-output.md) | Backpressure, chunk identity, and final results |
| 9 | [`Example_conditionalGraph`, `Example_graphDiagram`](./routing_test.go) | [Conditional graphs and diagrams](../docs/tutorials/09-conditional-graphs-and-diagrams.md) | Outlets, bypass, merge gates, and rendering |
| 10 | [`Example_subgraph`](./subgraph_test.go) | [Dependency-driven graphs and sealed subgraphs](../docs/tutorials/10-dependency-graphs-and-subgraphs.md) | Ready-node execution, isolation, scoped reuse, and projection |

Run the complete path:

```sh
go test ./example -run Example -v
```

Run one example:

```sh
go test ./example -run '^Example_jsonDSL$' -v
```

The package exports no helper API. Application code should import `flow`,
`flowx`, `workflow`, or an optional workflow subpackage directly.
