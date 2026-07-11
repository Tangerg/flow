# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Typed `workflow.Factory` adapter for common JSON-configured leaf nodes.
- Immutable `workflow.Pipeline` fluent API with `Pipe`, `Then`, and `Parallel`;
  a pipeline is directly runnable as a `Step` without a build call.
- Structured `RefError`, `RegistrationError`, `GraphError`, and `SpecError`
  values with stable sentinel errors for `errors.Is` and `errors.As`.
- Strict Draft 2020-12 schemas for the nested Spec and flat Graph JSON DSLs,
  standalone JSON validation, and per-node config schema validation.
- API compatibility, vulnerability, race, vet, lint, fuzz, and benchmark
  coverage in the development workflow.

### Changed

- The minimal typed API now lives in the module root package `flow`.
- Collection combinators accept variadic nodes and expose explicit bounded
  variants such as `FanOutN`, `MapAllN`, and `ParallelN`.
- Workflow registration reports errors immediately; `MustRegister*` helpers are
  available for fail-fast startup code.
- Store references, observers, options, and workflow compilation APIs have been
  reshaped around small interfaces and typed values.
- Workflow Store writes now use bounded persistent overlays, Sequence executes
  iteratively, Parallel merges branch write sets, and DAG planning uses a
  stable linear-time topological traversal.
- Store JSON encoding uses a single successful encoding pass and decoding
  constructs one immutable snapshot; Parallel specializes empty and single
  branches and compacts shared fan-out input at most once.
- The public surface is smaller: bounded root operations use `MapN` and
  `LoopN`, fluent composition is exclusive to `workflow.Pipeline`, conventional
  Store refs use constructors instead of key constants, and diagram rendering
  is left to callers.

### Breaking

- Replace imports of `github.com/Tangerg/flow/core` with
  `github.com/Tangerg/flow`.
- Replace root functional options with `MapN` and `LoopN`; use `workflow.LoopN`
  instead of `LoopLimit`.
- Compose `flowx` decorators as functions. Fluent chaining remains only on the
  deliberately small `workflow.Pipeline` surface.
- Use `Output`, `Item`, and `Index` instead of exported Store path constants;
  use `ObserverFunc` instead of the removed event collector.
- Consume `workflow.Description` directly; Mermaid rendering is no longer part
  of the core workflow package.
- Use `workflow.NodeSchema` instead of the ambiguous `workflow.Schema` name.
- See the migration list in `README.md` for the complete pre-v1 API rewrite.

[Unreleased]: https://github.com/Tangerg/flow/commits/rewrite
