# Documentation

Use this page as the map for learning, API reference, and project maintenance.
Each document has one job; detailed compatibility history stays in the
changelog rather than the user guide.

## Learn

- [Tutorials](./tutorials/README.md) progress from a single typed node to a
  JSON-defined, resumable workflow.
- [Executable examples](../example/README.md) mirror the tutorial levels with
  output-checked public-API examples.
- [Project README](../README.md) explains package selection, the execution
  model, and the shortest path to a working pipeline.

## API reference

Package comments and Go examples are the canonical API reference:

- [`flow`](../doc.go) — typed control-flow primitives.
- [`flowx`](../flowx/doc.go) — derived composition helpers.
- [`workflow`](../workflow/doc.go) — dynamic state, graphs, JSON, observation,
  and resumption.
- [`workflow/expr`](../workflow/expr/doc.go) — optional data-driven rules.

Read them locally with:

```sh
go doc github.com/Tangerg/flow
go doc github.com/Tangerg/flow/workflow
go doc github.com/Tangerg/flow/workflow/expr
```

## Project

- [Changelog](../CHANGELOG.md) records unreleased work and breaking migrations.
- [Contributing](../CONTRIBUTING.md) defines the development and API review
  requirements.
- [Security policy](../SECURITY.md) explains private vulnerability reporting.

## Maintainers

- [Release checklist](./releasing.md) covers versioning, compatibility checks,
  tags, and release verification.
