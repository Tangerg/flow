# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Suspension as a third outcome alongside success and failure. `Suspend` reports
  from inside a node that work cannot proceed yet; `Await` is the common shape of a
  wait, with `AwaitFactory` for the JSON DSL. A suspended run ends with an error
  matching `ErrSuspended`; `Suspensions` reports every wait and what would satisfy
  it. Because a suspension is not a failure, `Parallel` and `Iteration` let their
  remaining work finish and merge it instead of cancelling it — cancelling would
  discard the work and repeat its side effects on the run that resumes. Real errors
  still fail fast.
- `RunConfig` and `WithConfig`: one keyed struct carries everything a single run
  needs — its `Observer` and its `Journal` — installed with one call instead of a
  chain of `WithXxx` wrappers, and extensible without breaking callers. `Config`
  reads it back so a node can hand it to a nested run.
- `Journal` for checkpointed resumption, attached through `RunConfig`: a later run
  skips every step the Journal already holds and restores its result. Records are keyed by
  scope path and step ID, so this is correct where one step runs many times, and
  `Branch` and `Loop` also record the decisions they made — a resolver or condition
  that is not a pure function of the Store cannot send a resumed run down a
  different path, and is not consulted twice. A Journal serializes, so the process
  that resumes need not be the one that started. `Forget` retries a single step;
  `Reset` starts over.
- `Event` gained the `EventSuspended` and `EventSkipped` kinds, so an observer can
  see the third outcome and tell replayed steps from re-run ones.
- New optional package `workflow/expr`: branch and loop rules as data. It
  compiles a small, side-effect-free expression over a `Store` into ordinary
  `workflow.Condition` and `workflow.Resolver` values, so a threshold or a routing
  rule can live in config instead of in Go. `Bindings` registers a JSON document
  of named expressions as a group; `Switch` turns ordered boolean cases into a
  branch resolver; `Expr.Refs` reports what an expression reads.
  Expressions are parsed with `go/parser` and compiled to closures, so the
  supported grammar is the compiler: there is no code path that evaluates a
  construct `Parse` rejected, and nothing in the host program is reachable.
  Neither `flow` nor `workflow` imports it.
- Named input ports. `workflow.Inputs` wires a node's inputs by port name and
  `NodeSchema.Inputs` (a `Ports` map) declares them, so a multi-input node is
  described by the graph rather than by its own config. A flat `Graph` infers
  dependency order from every wired port, and validation reports unwired declared
  ports (`ErrMissingPort`), wired undeclared ports (`ErrUnknownPort`), and
  per-port edge type mismatches. `inputs` is accepted by both JSON DSL shapes.
- `workflow.BindFactory`, the typed factory adapter for a node that reads several
  ports; `workflow.OnePort` for the single-input schema.
- `workflow.GraphInputs` and `MissingInputs` report the external references a
  graph reads, so a run can be pre-flighted instead of failing mid-flight on a
  missing value.
- `Registry.NodeTypes` and `Registry.NodeSchema` expose the registered node
  vocabulary and each type's declared ports for editors and tooling.
- `Event` now carries `Seq` (per-run ordering), `Path` (the enclosing `Loop` and
  `Iteration` scopes, so repeated executions of one step are distinguishable),
  `Elapsed`, and the `Store` the step produced. `WithScope` and `Scope` let a
  custom composite contribute a path segment. Together these make an external
  tracker or state persister implementable without the package owning durability.
- `Store.Changes` returns the writes distinguishing a Store from a base snapshot
  — the delta an audit log records instead of a whole snapshot.
- Typed `workflow.Factory` adapter for common JSON-configured leaf nodes.
- Structured `RefError`, `RegistrationError`, `GraphError`, and `SpecError`
  values with stable sentinel errors for `errors.Is` and `errors.As`.
- Strict Draft 2020-12 schemas for the nested Spec and flat Graph JSON DSLs,
  standalone JSON validation, and per-node config schema validation.
- Vulnerability, race, vet, lint, fuzz, and benchmark coverage in the
  development workflow.

### Changed

- Store reads survive serialization. `Store.UnmarshalJSON` keeps numbers as
  `json.Number` so nothing is rounded on the way in, and `Get[T]` converts through
  the value's JSON representation instead of asserting an exact type. A typed step
  therefore reads the same on a fresh Store and on one restored from JSON —
  including structs, typed slices, an `int64` beyond float64's exact range, and
  values nested under a path. Conversion never rounds or reinterprets.
- `WithScope` always maintains the scope, whether or not an observer is attached. A
  scope identifies a step rather than labelling it: a Journal keys its records by
  it, and tying it to the observer made a journaled `Loop` skip every iteration
  after the first.
- Step IDs are now unique among steps that can run in the same execution rather
  than across the whole `Spec` tree. A `Branch`'s cases are mutually exclusive, so
  sibling cases may reuse an ID — which is how a branch converges on one output
  reference that a downstream step reads without knowing which case ran. Every
  other collision, including a case against a step outside the branch, is still
  `ErrDuplicateStep`.
- The minimal typed API now lives in the module root package `flow`.
- Bounded concurrency is configured with structs (`flow.MapConfig`,
  `workflow.ParallelConfig`, `IterationConfig`, ...); no `N` twin functions.
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
- The public surface is smaller: bounded operations take config structs (no `N`
  twins), conventional Store refs use constructors instead of key constants,
  and diagram rendering is left to callers.
- One shape per purpose: a leaf binder is a `BindFunc`, dropping the redundant
  `Binder` interface and the redundant `Pipeline` fluent builder.
- `flowx` is control-flow sugar only. `Retry`, `Timeout`, and `Trace` were
  removed; resilience and observability are the caller's concern (wrap a `Node`,
  or use a dedicated library).
- `Race` moved into the core as `flow.Race`, the OR (first-success) twin of the
  AND (wait-for-all) `flow.Map`; it is a primitive, not derived sugar.
- `flowx` keeps one implementation per control-flow shape: `Chain`, `FanOut`,
  `Combine`, and `Fallback`.
- Uniform style across the repo: private implementation types are named after
  the interface they satisfy — `thenNode`/`switchNode`/`mapNode`/`loopNode` for
  `flow.Node`, and `sequenceStep`/`branchStep`/`loopStep`/`parallelStep`/
  `iterationStep`/`leafStep` for `Step`. Every config struct is now the last,
  optional argument of its constructor.

### Breaking

- `WithObserver` and `WithJournal` are replaced by one `WithConfig(ctx, RunConfig{...})`.
  A run's configuration still travels in the context — it belongs to the run rather
  than to the definition, since a compiled workflow is run many times concurrently
  and each run wants its own `Journal` — but it is now a single keyed struct.
- `Get[T]` converts instead of asserting. The exact-type fast path is unchanged, so
  existing correct code keeps working; a read that previously failed with
  `ErrTypeMismatch` on a JSON-compatible value now succeeds.
- `Store.UnmarshalJSON` decodes numbers as `json.Number`, not `float64`. Code that
  reads a decoded Store with `Lookup` plus a `float64` type assertion must use
  `Get[T]`. A number outside float64's range is now accepted on decode and reported
  on read, rather than failing the whole decode.
- `workflow.Branch` and `workflow.Loop` take an `id` as their first argument, and
  `branch` and `loop` require `"id"` in the JSON DSL. The ID is where their
  journaled decisions are recorded and it is checked for uniqueness like any other
  step ID.
- `WithScope` is no longer inert without an observer.
- `Event.Kind` has two new values, `EventSuspended` and `EventSkipped`.
- `workflow.LeafFactory` takes a single `LeafSpec` value instead of positional
  `(id string, input Ref, config json.RawMessage)` arguments, so a factory sees
  every wired port and the signature can grow without breaking callers.
- `NodeSchema.Input ValueType` became `NodeSchema.Inputs Ports`, keyed by port
  name. Use `workflow.OnePort(t)` for a single-input node.
- `workflow.Factory` reports `ErrMissingPort` when the default port is unwired.
  Such a node could never have run, so the failure moved from run time to compile
  time; a node that takes no input needs a custom `LeafFactory` supplying its own
  `BindFunc`.
- A registered schema that declares ports now makes wiring mandatory. Graphs that
  relied on a declared input being optional will fail validation.
- Replace imports of `github.com/Tangerg/flow/core` with
  `github.com/Tangerg/flow`.
- Configure bounded operations with config structs (`flow.MapConfig`,
  `flow.LoopConfig`, `workflow.ParallelConfig`, `workflow.IterationConfig`); the
  `XxxN` variants were removed.
- `workflow.Pipeline` and `Pipe` were removed. Build sequential and parallel
  stages with `Sequence` and `Parallel`.
- `flowx.Retry`, `Timeout`, and `Trace` were removed; `flowx` is now control-flow
  sugar only. Wrap a `Node` for resilience, or use a library. The `Binder`
  interface was removed; pass a `BindFunc` to `Leaf`.
- `flowx.Race` moved to `flow.Race` (core primitive). Update the import path.
- `flowx.FanOut` and `workflow.Parallel` now take their nodes/branches as a
  slice with the config as a trailing optional argument —
  `FanOut(nodes, cfg...)` and `Parallel(branches, cfg...)` — replacing the
  previous required leading `cfg` and variadic nodes/branches.
- `flowx.FanOutAll`, `flowx.MapAll`, the `flowx.Result`/`Collect` collecting API,
  and `flowx.Identity` were removed. Use `flow.Race`/`flowx.FanOut` for control
  flow, aggregate errors yourself, and write a one-line `NodeFunc` (or
  `flowx.Chain()`) for a pass-through.
- Use `Output`, `Item`, and `Index` instead of exported Store path constants;
  use `ObserverFunc` instead of the removed event collector.
- Consume `workflow.Description` directly; Mermaid rendering is no longer part
  of the core workflow package.
- Use `workflow.NodeSchema` instead of the ambiguous `workflow.Schema` name.
- See the migration list in `README.md` for the complete pre-v1 API rewrite.

[Unreleased]: https://github.com/Tangerg/flow/commits/rewrite
