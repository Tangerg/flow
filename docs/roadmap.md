# Roadmap

This roadmap records unresolved engine work and decisions. Current behavior
belongs in package documentation and tutorials; released compatibility history
belongs in the [changelog](../CHANGELOG.md).

Last reviewed: 2026-08-23.

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
2. Treat workflow-definition compatibility separately from Journal wire
   compatibility. Applications must version definitions and must not resume a
   Journal against changed IDs, scopes, wiring, or control flow without an
   explicit migration policy.
3. Run compatibility analysis on every exported change and document migrations
   between actual releases.
4. Keep formatting, tests, race detection, vet, lint, fuzzing, and
   vulnerability checks green.

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

### A Graph stays acyclic

`Graph` rejects cycles, and that is a design decision rather than a missing
feature. Graph execution is dependency-driven: a node starts as soon as every
declared dependency completes, without waiting for unrelated branches. A node
inside a cycle can never satisfy that condition, so a cyclic engine must
instead evaluate a frontier, synchronize, and evaluate the next one. Adopting
top-level cycles would therefore replace dependency-driven dispatch with
superstep barriers for every graph, including the acyclic ones.

Express repetition inside a node instead:

- Bounded repetition over a known collection or condition uses `Iteration` or
  `Loop`, which the engine journals per invocation through indexed scope
  frames.
- Open-ended repetition, such as an agent that decides when it is finished,
  belongs inside one typed node. The node owns its own loop and, if it must
  survive suspension, its own checkpoint value. The engine sees one boundary
  and journals one result.

The second case is a real limit, not a workaround: the engine offers no
per-round checkpoint for a node-internal loop, and no accumulating write. A
node that needs those owns them. Revisit only if applications repeatedly build
the same checkpoint shape by hand, and prefer giving a node somewhere to keep
private state across suspension over admitting cycles into the graph.

### Persistence is a value boundary

Store and Journal are serializable values. The engine does not define
`Storage`, `Queue`, `Scheduler`, `Clock`, or `Lease` interfaces.

Store is a value pool, not a hidden run namespace. `Run` preserves the Store it
is given; applications assemble a new seed Store for a new logical run. A
compiled Graph alone clears its declared node cells because only that shape has
a complete, explicit ownership set. Structured and caller-defined Steps may be
opaque, so guessing which of their input cells to erase would be unsound.

An application chooses where to persist values, how to identify and authorize a
run, when to wake it, and how to coordinate ownership. Those systems consume
the engine; the engine does not depend on speculative provider abstractions.

Journal wire compatibility is deliberately exact. A decoder accepts only its
current version and never embeds migration logic for earlier documents. A wire
change increments the version; applications migrate or discard durable
checkpoints before handing them to the new engine. This keeps replay code on one
canonical representation and makes accidental cross-version resumption fail
closed. Workflow-definition compatibility remains a separate application
decision.

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
