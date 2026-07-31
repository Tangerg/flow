# Documentation

Choose the shortest path that matches how your workflow is defined.

| Goal | Start here | Continue with |
| --- | --- | --- |
| Compose typed Go functions | [Project README](../README.md#quick-start) | [Tutorials 1–2](./tutorials/README.md#typed-go) |
| Build a runtime DAG in Go | [Tutorial 3](./tutorials/03-workflow-store-and-ref.md) | [Tutorials 4, 9, and 10](./tutorials/README.md#runtime-dags) |
| Accept JSON definitions | [Tutorial 5](./tutorials/05-json-dsl-and-schema.md) | [Tutorials 6, 9, and 10](./tutorials/README.md#configuration-driven) |
| Add resumption or streaming | [Tutorial 7](./tutorials/07-suspension-and-resumption.md) | [Tutorial 8](./tutorials/08-streaming-output.md) |
| Copy tested code | [Executable examples](../example/README.md) | Run `go test ./example -run Example -v` |

The [tutorial index](./tutorials/README.md) also provides one continuous path
from a single node to runtime-defined, resumable workflows.

## API reference

Package comments and Go examples are the canonical API reference:

- [`flow`](../doc.go) — typed control-flow primitives.
- [`flowx`](../flowx/doc.go) — derived composition helpers.
- [`workflow`](../workflow/doc.go) — named state, graphs, JSON, streaming,
  observation, and resumption.
- [`workflow/expr`](../workflow/expr/doc.go) — optional data-driven rules.
- [`workflow/diagram`](../workflow/diagram/doc.go) — deterministic ASCII and
  Mermaid Graph renderings.

Read them locally with:

```sh
go doc github.com/Tangerg/flow
go doc github.com/Tangerg/flow/workflow
go doc github.com/Tangerg/flow/workflow/expr
go doc github.com/Tangerg/flow/workflow/diagram
```

## Design and maintenance

- [Roadmap](./roadmap.md) records unresolved engine work and settled
  boundaries.
- [Changelog](../CHANGELOG.md) records user-visible work for the next release
  and compatibility history after releases begin.
- [Contributing](../CONTRIBUTING.md) defines development, documentation, and
  API-review requirements.
- [Release checklist](./releasing.md) covers versioning, compatibility, tags,
  and release verification.
- [Security policy](../SECURITY.md) explains private vulnerability reporting.
