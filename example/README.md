# Examples

This package is an executable learning path. Read the examples in order:

| Level | Example | What it introduces |
| --- | --- | --- |
| 1 | [`node_test.go`](./node_test.go) | `NodeFunc` and typed `Then` composition |
| 2 | [`composition_test.go`](./composition_test.go) | Composites as nodes and bounded `Map` concurrency |
| 3 | [`workflow_test.go`](./workflow_test.go) | `Step`, `Store`, `Ref`, and `Sequence` |
| 4 | [`dag_test.go`](./dag_test.go) | Registry, named ports, schemas, fan-out, and fan-in |
| 5 | [`json_test.go`](./json_test.go) | JSON DAG compilation and Draft 2020-12 schema support |
| 6 | [`rules_test.go`](./rules_test.go) | Data-driven expressions and branching |
| 7 | [`resume_test.go`](./resume_test.go) | Interrupt, persistence, and resumption with a Journal |

Every example uses only public APIs and has asserted output. Run the complete
set with:

```sh
go test ./example -run Example -v
```

The package deliberately exports no helper API. Copy the smallest relevant
example into an application, then replace its sample nodes with domain code.
