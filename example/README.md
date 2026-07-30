# Executable examples

This package is the runnable counterpart to the
[step-by-step tutorials](../docs/tutorials/README.md). Read the examples in
order; each uses only public APIs and has asserted output.

| Level | Example | Tutorial | Introduces |
| --- | --- | --- | --- |
| 1 | [`node_test.go`](./node_test.go) | [Nodes and `Then`](../docs/tutorials/01-node-and-then.md) | `NodeFunc` and typed composition |
| 2 | [`composition_test.go`](./composition_test.go) | [Composition and concurrency](../docs/tutorials/02-composition-and-concurrency.md) | Composites as nodes and bounded `Map` concurrency |
| 3 | [`workflow_test.go`](./workflow_test.go) | [Stores, references, and steps](../docs/tutorials/03-workflow-store-and-ref.md) | `Step`, `Store`, `Ref`, and `Sequence` |
| 4 | [`dag_test.go`](./dag_test.go) | [Registries, ports, and DAGs](../docs/tutorials/04-graph-registry-and-ports.md) | Runtime node types, named ports, schemas, fan-out, and fan-in |
| 5 | [`json_test.go`](./json_test.go) | [The JSON DSL and schemas](../docs/tutorials/05-json-dsl-and-schema.md) | JSON Graph compilation and Draft 2020-12 schemas |
| 6 | [`rules_test.go`](./rules_test.go) | [Data-driven rules](../docs/tutorials/06-data-driven-rules.md) | Expression-based routing |
| 7 | [`resume_test.go`](./resume_test.go) | [Suspension and resumption](../docs/tutorials/07-suspension-and-resumption.md) | Interrupts, persistence, and Journal replay |
| 8 | [`stream_test.go`](./stream_test.go) | [Streaming output](../docs/tutorials/08-streaming-output.md) | Backpressure, chunk identity, and final results |
| 9 | [`routing_test.go`](./routing_test.go) | [Conditional graphs and diagrams](../docs/tutorials/09-conditional-graphs-and-diagrams.md) | Routing outlets, bypass, merge gates, and visualization |

Run the complete path:

```sh
go test ./example -run Example -v
```

Run one level:

```sh
go test ./example -run Example_jsonDSL -v
```

The package deliberately exports no helper API. Copy the smallest relevant
example into an application, then replace its sample nodes with domain code.
