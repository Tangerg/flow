# Roadmap

This document records the remaining engine work and the boundaries that prevent
the package from growing into an application platform. Compatibility history
belongs in the [changelog](../CHANGELOG.md).

Last reviewed: 2026-07-30.

## Current ceiling

The in-process runtime can now express:

- typed sequence, selection, repetition, mapping, races, and derived
  compositions in `flow` and `flowx`;
- persistent named state with typed reads and RFC 6901 references;
- nested workflow control flow through `Spec`;
- arbitrary flat DAGs through `Graph`, including named ports, bounded layer
  concurrency, conditional outlets, explicit bypass, and cross-arm merge;
- strict JSON decoding, self-contained Draft 2020-12 schemas, registry
  validation, and configuration schemas;
- suspension as a third outcome, structured waits, persisted Store and Journal
  state, and checkpoint-and-restart replay;
- synchronous streaming with backpressure and consumer failure propagation;
- run-scoped lifecycle observation and Store deltas; and
- deterministic ASCII and Mermaid renderings in `workflow/diagram`.

Every definition still compiles to the same small protocol:

```go
Run(context.Context, input) (output, error)
```

There is no orchestrator object, hidden worker pool, or provider abstraction.

## Landed: conditional Graph execution

The former largest Graph gap is complete.

A routing node publishes its selected outlet as its ordinary string output and
declares the complete set in `NodeSchema.Outlets`. A target uses `NodeSpec.When`;
the zero `Trigger` requires every gate and `TriggerAny` requires one. Gate
sources participate in topological ordering and cycle detection.

An unsatisfied target emits `EventBypassed`, runs no user code, and writes no
output. Bypass is explicit rather than inferred from missing data. A bypassed
routing source makes downstream gates unsatisfied, so conditional regions
propagate correctly.

The runtime preserves these invariants:

- gates are recomputed from replayed routing outputs rather than journaled as a
  second identity;
- every gate source has a registered schema and non-empty outlet declaration;
- runtime output must be a declared outlet, even if a custom factory violates
  its schema;
- gate wrappers preserve static duplicate-ID and nesting-depth validation;
- a compiled Graph owns its internal node cells and clears them at each
  invocation, so reusing a previous Store cannot revive stale branch output;
- suspension ends the current sequence layer before a later gate can run; and
- `FirstOf` skips only absent values, never a real conversion error.

`Route` adapts an existing Store-based `Resolver` into an ordinary replayable
leaf. A typed node that returns a string remains the smallest routing primitive.

## Settled engine boundaries

### Errors are terminal; domain outcomes are data

Generic failure routing is not an engine primitive.

A Go `error` has no stable serialization or replay representation. Catching it
at a graph edge would also need to distinguish node failures from context
cancellation, definition errors, bind errors, Journal conflicts, emitter
failure, and suspension. A generic classifier or codec interface would move
application policy into the engine and still be unable to make arbitrary error
values durable.

A recoverable domain outcome such as `declined`, `not_found`, or
`needs_review` is ordinary typed output. A following routing node maps that data
to a declared outlet. An actual error terminates execution and remains available
through `errors.Is` and `errors.As`.

This rule keeps fresh execution and Journal replay equivalent.

### Retry and timeout wrap typed work

Retry, timeout, hedging, and circuit breaking are policies around the typed node
inside a workflow leaf. They are not fields on `NodeSpec` or `Spec`.

Applying them to a named `Step` would invoke the same execution identity more
than once, conflict with Journal replay, and risk retrying suspension. A generic
workflow decorator also cannot define which domain errors are retryable or how
an emitter timeout interacts with backpressure. Applications or node libraries
own those decisions before calling `Leaf`.

`Graph.Concurrency`, `ParallelConfig.Concurrency`, and
`IterationConfig.Concurrency` remain engine-level resource bounds because they
describe composition itself.

### Graph and Spec keep distinct roles

`Graph` is the flat form produced by an editor: arbitrary data edges,
conditional outlets, and cross-arm convergence.

`Spec` is the structured form: nested sequence, parallel, branch, loop, and
iteration.

Neither is a strict superset. Flattening `Spec` would lose repeated scoped
execution; nesting every Graph branch would lose arbitrary convergence. Keep
both and share internal execution concepts only where their semantics are
actually identical.

### Persistence is a value boundary

Store and Journal are serializable values. The engine does not define a
`Storage`, `Queue`, `Scheduler`, `Clock`, or `Lease` interface.

An application chooses where to persist them, how to identify a run, when to
wake it, and how to coordinate ownership. Those facilities consume the engine;
the engine does not depend on hypothetical abstractions for them.

## Before v1

The remaining work is stabilization rather than another execution subsystem:

1. Freeze the exported API after real downstream use of conditional Graphs.
2. Run compatibility analysis on every exported change and document the final
   pre-v1 migration.
3. Keep statement coverage, race checks, vet, lint, fuzzing, and vulnerability
   checks green.
4. Add property or fuzz cases only where they strengthen a concrete invariant;
   do not optimize or generalize without a measured problem.
5. Review generic methods after Go 1.27 ships and the module adopts it. Preserve
   package-level generic functions where a method is not clearly simpler.

Potential future concepts require evidence:

- A replay-aware multi-output leaf contract, if real routers need both a
  payload and an independent outlet.
- Composite/subgraph nodes in Graph, if editors need reusable structured
  regions rather than flat DAGs.
- An opt-in schema inference helper for tooling, never automatic registration,
  if repeated schemas prove to be a maintenance burden.

None is an approved v1 requirement.

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
is what lets the runtime remain small, embeddable, and composable.
