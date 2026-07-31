# Roadmap

This roadmap records unresolved engine work and decisions. Current behavior
belongs in package documentation and tutorials; released compatibility history
belongs in the [changelog](../CHANGELOG.md).

Last reviewed: 2026-07-31.

## Direction

`flow` is an embeddable, in-process execution engine rather than an application
platform. Every definition reduces to:

```go
Run(context.Context, input) (output, error)
```

The engine owns composition, execution identity, named state, validation,
checkpoint replay, and run-scoped signals. The host application owns run
records, persistence infrastructure, scheduling, authorization, deployment,
and external-effect policy.

The current engine surface is described in the
[project README](../README.md). New concepts must remove a demonstrated
limitation without weakening the single execution model.

## Before v1

The remaining work is stabilization:

1. Exercise conditional Graphs, dependency-driven execution, streaming,
   resumption, and sealed Subgraphs in real downstream programs before freezing
   the exported API.
2. Decide and document Journal wire-format compatibility. The current decoder
   accepts only the wire version it implements; before the first stable release,
   choose whether later releases will read selected older versions or require an
   application migration step.
3. Treat workflow-definition compatibility separately from Journal wire
   compatibility. Applications must version definitions and must not resume a
   Journal against changed IDs, scopes, wiring, or control flow without an
   explicit migration policy.
4. Run compatibility analysis on every exported change and document migrations
   between actual releases.
5. Keep formatting, tests, race detection, vet, lint, fuzzing, and
   vulnerability checks green.
6. Review generic methods only after the module adopts a Go version that ships
   them. Keep package-level generic functions unless a method is clearly
   simpler for callers.

## Evidence required

These concepts are not approved work. Revisit one only after a real use case
shows that composition cannot express it cleanly:

- A replay-aware multi-output leaf, if applications repeatedly need both a
  payload and an independent routing outlet.
- An opt-in schema inference helper for tooling, if maintaining explicit
  schemas becomes a measured burden. Inference must never become authoritative
  registration because Go types and JSON representations can differ.
- Partial collection for `Iteration`, if an application needs a durable
  incomplete result and can define its hole and replay semantics.

Do not add an abstraction merely to mirror a larger workflow product.

## Settled engine boundaries

### Errors are terminal; domain outcomes are data

A recoverable outcome such as `declined`, `not_found`, or `needs_review` is
ordinary typed output and may feed a routing node. A Go `error` terminates
execution.

Routing arbitrary errors would require stable serialization and replay rules
for application failures, cancellation, invalid definitions, Journal conflicts,
emitter failures, and suspension. Keeping errors terminal preserves normal Go
inspection and makes fresh execution and replay easier to reason about.

### Retry and timeout wrap typed work

Retry, timeout, hedging, and circuit breaking wrap the typed node before
`workflow.Leaf`. They are not fields on `GraphNode` or `Spec`.

A named Step is an execution identity and may run once per scope. Retrying that
Step would collide with Journal semantics and could mistake suspension for
failure. Applications or focused node libraries own the domain-specific policy.

Composition-level bounds such as `Graph.Concurrency`,
`ParallelConfig.Concurrency`, and `IterationConfig.Concurrency` remain engine
concerns.

### Graph and Spec remain distinct

`Graph` is the flat editor form: arbitrary named-port edges, conditional
outlets, and cross-arm convergence.

`Spec` is the structured form: nested sequence, parallel, branch, loop,
iteration, and subgraph.

Neither is a strict superset. Keep both public forms and share internal
execution concepts only where their semantics are identical.

### Persistence is a value boundary

Store and Journal are serializable values. The engine does not define
`Storage`, `Queue`, `Scheduler`, `Clock`, or `Lease` interfaces.

An application chooses where to persist values, how to identify and authorize a
run, when to wake it, and how to coordinate ownership. Those systems consume
the engine; the engine does not depend on speculative provider abstractions.

Journal size grows with recorded execution boundaries. A loop of `n`
iterations whose body completes `m` journaled boundaries records `O(n*m)`
entries. The engine deliberately retains complete records so resume remains
equivalent to replay. Applications should bound repetition and retain or
discard a Journal with its logical run.

## Out of scope

- Distributed scheduling, durable timers, queues, workers, leases, and leader
  election.
- Exactly-once external effects, distributed transactions, and saga
  coordination.
- Deterministic instruction-level replay.
- Workflow-definition migration and deployment policy.
- A catalog of HTTP, model, retrieval, or code-execution nodes.
- Engine-level stream middleware.
- Reflection-driven port binding or authoritative schema inference.

These are product, platform, or integration responsibilities. Keeping them out
is what lets the runtime remain small, explicit, and composable.
