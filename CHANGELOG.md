# Changelog

All notable user-visible changes to this project are documented here. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
releases follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This section describes the public shape being prepared for the first release.
Intermediate pre-release refactors remain available in Git history rather than
being presented as migrations from a version that was never published.

### Added

- A minimal typed core built around `Node[I, O]`, `NodeFunc`, `Then`, `Switch`,
  `Loop`, `Map`, and `Race`.
- Derived composition in `flowx`: `Chain`, `FanOut`, `Combine`, and `Fallback`.
- The optional `workflow` runtime for immutable named state, typed references,
  reusable Steps, sequence, parallel, branch, loop, iteration, and sealed
  subgraphs.
- Flat, dependency-driven `Graph` execution with named ports, explicit control
  dependencies, graph-wide concurrency limits, conditional outlets, bypass,
  and mutually exclusive merges.
- Structured `Spec` definitions for nested control flow.
- A typed `Kind` vocabulary shared by `Spec` and Step descriptions.
- A Registry boundary for named Go capabilities, node schemas, configuration
  schemas, conditions, and resolvers.
- Strict JSON Graph and Spec decoding with duplicate-member rejection,
  self-contained Draft 2020-12 schemas, and disabled external schema loading.
- Suspension as a third outcome through `Await`, `Interrupt`, and `Suspend`,
  with structured waits and checkpoint-and-restart Journal replay.
- JSON persistence for Store and Journal values. Journal records use structured
  scope-aware identities and a versioned wire document.
- Typed streaming producers through `StreamFunc` and a run-scoped,
  backpressure-aware `Emitter`.
- Run-scoped lifecycle observation, ordered events and chunks, and Store write
  deltas.
- The optional `workflow/expr` package for restricted, side-effect-free
  conditions and routing rules defined as data.
- The optional `workflow/diagram` package for deterministic ASCII and Mermaid
  Graph renderings.
- A progressive tutorial series and output-checked public-API examples.

### Changed

- The module requires Go 1.26 or newer.
- Errors preserve their causes and expose stable sentinels or structured
  context for use with `errors.Is` and `errors.As`.
- Validation and persistence diagnostics do not depend on Go map iteration
  order when several entries are invalid.
- Retry, timeout, tracing, durable scheduling, and exactly-once effects remain
  outside the engine. Typed-node decorators and application infrastructure own
  those policies.

[Unreleased]: https://github.com/Tangerg/flow/commits/main
