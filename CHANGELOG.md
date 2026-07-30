# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `workflow.Subgraph` and `SubgraphConfig`, a sealed composite that copies
  declared outer inputs into a fresh Store, scopes a reusable body under the
  subgraph ID, and projects one declared result back out without leaking inner
  cells. `SubgraphFactory` exposes the same boundary as a registered Graph node,
  and the `Spec` JSON DSL now includes a `subgraph` kind.
- Conditional execution for flat `workflow.Graph` definitions.
  `NodeSchema.Outlets` declares a routing node's possible string outputs;
  `GraphNode.When`, `Gate`, `When`, and `TriggerAny` gate targets and
  re-convergence points; and `EventBypassed` distinguishes an unselected node
  from Journal replay. Gate sources participate in dependency ordering and
  cycle detection, and both schema validation and execution reject undeclared
  outlets through `ErrUnknownOutlet`.
- `workflow.Route`, which adapts an existing Store-based `Resolver` into an
  ordinary leaf whose outlet decision is observed, journaled, and replayed.
- Optional `workflow/diagram` package with deterministic ASCII and Mermaid
  renderings of flat Graph definitions. Rendering escapes Mermaid labels and
  remains separate from Graph validation and execution.
- `workflow.LeafFunc` and `workflow.StreamLeafFunc`, concise adapters for the
  common case of lifting an ordinary typed function with one referenced input.
- `workflow.FirstOf`, a tolerant binder that reads the first available
  reference in declaration order, for mutually exclusive merge paths.
- `Graph.Concurrency`, which bounds the number of nodes running concurrently
  across a dependency-driven graph; zero retains unbounded execution.
- First-class streaming output for Go-defined workflow leaves.
  `StreamNode[I, O, C]` and `StreamNodeFunc` model a typed producer with a
  synchronous, stoppable yield callback; `StreamLeaf` gives it the same binding,
  replay, event, suspension, Journal, and final-output lifecycle as `Leaf`.
  A run-scoped, error-returning `Emitter` receives immutable `Chunk` values with
  step identity, scope, per-invocation index, and ordering shared with lifecycle
  events. Completed Journal replay emits nothing; an incomplete rerun may repeat
  its attempted prefix from index zero.
- `SuspendedOnly` and variadic `JoinSuspensions`, allowing caller-defined
  composites to preserve the package's suspension-as-a-third-outcome semantics
  without depending on internal error types.
- An executable `example` package with a ten-stage learning path from one
  typed node through composition, dynamic DAGs, the JSON DSL, expression-based
  routing, persisted suspension/resumption, and streaming output. The examples
  use only public APIs and run as output-checked Go examples.
- Suspension as a third outcome alongside success and failure. `Suspend` reports
  from inside a node that work cannot proceed yet; `Await` is a Store gate, with
  `AwaitFactory` for the JSON DSL; and `Interrupt` is an explicit request/response
  Step, with `InterruptFactory` for the JSON DSL. A suspended run ends with an
  error matching `ErrSuspended`; `Suspensions` reports every wait and its
  scope-aware key. Record an Interrupt response with
  `Journal.Record(wait.Key(), value)` and it becomes the Step's output on the next
  run. Because a suspension is not a failure, `Parallel` and `Iteration` let
  their remaining work finish and merge it instead of cancelling it — cancelling
  would discard the work and repeat its side effects on the run that resumes.
  Real errors still fail fast.
- `workflow.Run`, the explicit execution boundary for a configured workflow.
  `RunConfig` carries that call's `Observer`, `Emitter`, and `Journal`; every
  call gets fresh run bookkeeping while a Journal may deliberately carry
  completed work into a later call.
- `Journal` for checkpointed resumption, passed to `workflow.Run` through
  `RunConfig`: a later run skips every completed leaf boundary the Journal holds
  and restores its result.
  Records are keyed by scope and step ID, so this is correct where one leaf
  runs many times. `Branch` and `Loop` also record the decisions they made — a
  resolver or condition that is not a pure function of the Store cannot send a
  resumed run down a different path, and is not consulted twice. A Journal
  serializes, so the process that resumes need not be the one that started.
  `Forget` retries a single step; `Reset` starts over.
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
- `Graph.Inputs` and `Graph.MissingInputs` report the external references a
  graph reads, so a run can be pre-flighted instead of failing mid-flight on a
  missing value.
- `Registry.NodeTypes` and `Registry.NodeSchema` expose the registered node
  vocabulary and each type's declared ports for editors and tooling.
- `Event` now carries `Seq` (per-run ordering), `Scope` (the enclosing `Loop` and
  `Iteration` scopes, so repeated executions of one step are distinguishable),
  `Elapsed`, and the `Store` the step produced. `WithScope` and `Scope` let a
  custom composite contribute a scope segment. Together these make an external
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

### Fixed

- `Journal.UnmarshalJSON` now decodes its versioned wire format from one
  case-sensitive JSON-domain view. It accepts only the canonical `version`,
  `records`, `scope`, `id`, and `value` member names, so alternate casing can
  neither panic nor silently overwrite a record, scope, or value through
  encoding/json's case-insensitive struct matching. Missing required members
  and non-array `records` or `scope` values are rejected atomically.

### Changed

- The module requires Go 1.26. `errors.AsType` and the other 1.26 additions are
  used where they replace an older idiom, so 1.25 can no longer build it.
- The indirect `golang.org/x/text` requirement is raised to a release containing
  the fix for GO-2026-5970.
- `flow` now depends on `golang.org/x/sync`. `Map` delegates first-error
  collection and group cancellation to `errgroup`, which replaces a hand-rolled
  `sync.Once` guarding an error and a cancel func. Bounded fan-out keeps its own
  worker pool rather than using `errgroup.SetLimit`: SetLimit bounds calls in
  flight by queueing every element on a semaphore, which allocates per element
  instead of per worker, and a caller who sets a limit means it to bound what
  the fan-out consumes.
- Compiled Graphs now schedule from dependency readiness instead of inserting
  topological `Sequence(Parallel(...))` barriers. `Graph.Concurrency` is a
  graph-wide limit. Nodes receive only the input Store and their declared
  dependencies, and completed Stores merge in declaration order. Suspension
  blocks descendants while unrelated work finishes; a real failure returns
  already completed writes together with the error.
- `GraphNode` and `NodeSchema` gained routing fields. Unkeyed composite literals
  for either type must be converted to keyed fields; keyed literals require no
  migration.
- A compiled `workflow.Graph` now owns every Store cell whose node ID belongs to
  that Graph. Each invocation removes those internal cells before execution and
  rebuilds them from current work or Journal replay, preventing a reused prior
  Store from leaking output from a now-bypassed branch. Callers that seeded
  values under an internal node ID must move them to an external input ID and
  wire that reference through a port.
- `Spec.Input`, `Spec.BodyOutput`, and `GraphNode.Input` are now `Ref` values
  rather than pointers. Callers can write `Input: workflow.Output("source")`
  directly; a zero `Ref` remains omitted from JSON and means the field is unset.
- `RunConfig` now includes an `Emitter`. Callers should continue to construct
  `RunConfig` with keyed fields; unkeyed composite literals must be updated.
- `Event.Seq` now shares one run-wide ordering with streaming `Chunk` values.
  Event-only runs retain their existing dense sequence.
- Leaf validation now recognizes a typed nil `flow.NodeFunc` before Journal
  replay, so a stale record cannot hide an invalid definition.
- Workflow execution now enforces one `(scope, step ID)` identity per run.
  Built-in composites reject duplicate IDs before executing, opaque custom
  wrappers are covered at runtime, Journal write conflicts are returned instead
  of silently keeping stale data, and each run replays a stable Journal snapshot.
  Loop body scopes now include the loop ID (`loop[0]`), so sibling loops may
  safely reuse body IDs; persisted Journals using the old bare `[0]` Loop paths
  must be discarded or migrated.
- Strict JSON, node config, programmatic definition, and Journal path boundaries
  share a maximum nesting depth and return `ErrMaxDepth` instead of allowing
  recursive schema validation to exhaust the process stack. Journal key
  traversal now uses linear live path storage rather than retaining a copied
  path at every trie level.
- `Map` now documents that parent cancellation takes precedence over a completed
  output slice, and `Race` explicitly documents that it waits indefinitely for
  a losing node that ignores cancellation.
- Policy documentation now distinguishes typed node decorators from named
  workflow execution boundaries: retry and hedging belong inside `Leaf`, while
  mutually exclusive Step alternatives belong in `Branch`.
- Repository documentation is now organized by audience: the root README is a
  concise adoption guide, the English tutorial series owns progressive
  learning, executable examples remain the runnable source of truth, package
  comments own API reference, and maintainer documents cover contribution,
  security, and releases.
- Internal code is organized by domain responsibility rather than historical
  growth: references, leaves, sequences, strict JSON decoding, JSON Pointers,
  Store persistence, Journal persistence, awaits, interrupts, suspensions, graph
  planning, and graph validation each own a focused file and receiver-driven
  model. Receiver names are consistent for every type.
- Graph planning now separates the mutable `graphPlanner` from its stable
  `graphPlan`, so traversal counters cannot leak into validation or compilation.
  Leaf and Spec compilation similarly live on dedicated internal compiler
  objects rather than accumulating unrelated private methods on `Registry`.
- Error diagnostics now name the invalid setting, value, path, node index, or
  journal record involved. An unmatched `Branch` result is a structured
  `StepError` that still wraps `flow.ErrNoCase`. Unmarshaling into a nil Store
  or Journal receiver returns an error instead of panicking; marshaling a nil
  Journal remains equivalent to marshaling an empty Journal.
- Strict typed JSON decoding now preserves numbers as `json.Number` when the
  destination contains `any`, matching Store, Journal, schema-validation, and
  interrupt decoding semantics without losing integers above 2^53.
- `flow.Map`, `flow.Race`, workflow leaf execution, JSON Pointer traversal,
  Parallel, and Iteration now use focused execution objects with smaller
  lifecycle methods. This keeps cancellation, replay, suspension, and merge
  state together while preserving their public behavior.
- Stateful implementation logic now lives with the values that own its
  invariants: Graph planning, Spec validation, strict JSON reading, schema
  compilation, Store merging and traversal, suspension error classification,
  Step collection execution, and expression operands are receiver-driven
  internal types rather than unrelated package-level helper functions.
- `Ref.Path` is now an RFC 6901 JSON Pointer. `At` and `Ref.Child` take literal
  path segments and escape them, so empty object keys and keys containing `.`,
  `/`, or `~` are all representable. The JSON Schemas reject ambiguous legacy
  dotted paths, while `workflow/expr` keeps its Go-like source syntax and
  compiles it to pointers. Array traversal accepts only RFC 6901's canonical
  unsigned decimal indexes.
- JSON documents are decoded with duplicate-member detection before schema or
  typed decoding can collapse them. This applies to the workflow DSL, node
  configs, Store and Journal state, and embedded or registered JSON Schemas.
  Store decoding also requires object-shaped state rather than treating `null`
  as an empty Store.
- `flow.Race` now owns the complete lifetime of the goroutines it starts: after
  a winner or parent cancellation it cancels and drains every losing call before
  returning. It also rejects a nil node before starting any sibling.
- Negative concurrency and iteration limits are consistently invalid across the
  root and workflow APIs and match `flow.ErrInvalidConfig`; zero retains the
  documented default.
- Workflow definitions are validated before replay or user code. In particular,
  a stale Journal can no longer hide an invalid Leaf, Branch rejects a nil
  resolver or case before choosing, and ordered composites report the failing
  position with `flow.IndexError`.
- Graph type validation applies a node's declared output type only to its exact
  conventional output. Nested references and custom cells are `TypeAny` because
  `NodeSchema` does not describe their member types.
- Expression number semantics now survive a Store JSON round trip. Integral
  floats follow JSON integer semantics, and `float32` values are normalized to
  the same decimal value that `encoding/json` preserves.
- Store path lookup now uses a typed value's JSON representation when needed, so
  nested reads through structs, typed maps and slices, pointers, and custom JSON
  marshalers behave the same before and after Store serialization. Restored
  Store writes and marshal diagnostics are deterministic.
- Composites validate their required children before invoking any child. A nil
  second `Then` node, empty-input `Map`, later `Sequence` step, `Parallel`
  branch, `Loop` body, or `Iteration` body can no longer hide an invalid
  definition or allow earlier children to perform partial work.
- Programmatic `Spec` and `Graph` validation now matches the strict JSON DSL:
  fields irrelevant to a Spec kind, empty or duplicate explicit dependencies,
  empty ports, empty node types, and malformed raw config are rejected instead
  of ignored. Iteration body IDs are local to the per-element Store and scope,
  so they may be reused safely outside the body.
- Graph leaf construction keeps errors at the graph boundary, rejects a nil Step
  returned by a custom factory, and gives factories owned config bytes. `Factory`
  rejects ports beyond its one default input; whitespace-only config selects the
  zero config like an omitted value.
- Journal records are append-only until `Forget` or `Reset`, and a Branch records
  a resolver decision only after confirming that it names a real, non-nil case.
- `Loop` and `Iteration` now store their definition directly and validate it in
  `Run`, rather than hiding execution inside a constructor-created closure.
- Expression indexes written in Go's hexadecimal or octal literal syntax are
  normalized to the Store's decimal path representation, and unsigned/float
  comparison handles the complete fractional boundary.
- Store reads survive serialization. `Store.UnmarshalJSON` keeps numbers as
  `json.Number` so nothing is rounded on the way in, and `Get[T]` converts through
  the value's JSON representation instead of asserting an exact type. A typed step
  therefore reads the same on a fresh Store and on one restored from JSON —
  including structs, typed slices, an `int64` beyond float64's exact range, and
  values nested under a path. Conversion never rounds or reinterprets.
- Suspension aggregation walks the complete standard Go error tree. Nested
  `Parallel` and `Iteration` compositions preserve every wait, distinguish a
  pure suspension tree from a tree that also contains a real failure, and retain
  completed nested branch writes in the returned Store.
- `Suspension.Value` carries a string or structured application value rather
  than restricting waits to a textual reason. `Suspension.Key` returns the
  structured `ID + Path` identity used to resolve one repeated Interrupt without
  positional matching or waking its siblings.
- Journal identity is structured end to end. Internally it is a trie over scope
  segments; `Journal.Keys` returns `JournalKey` values, `Forget` accepts one, and
  the versioned JSON form stores `{path,id,value}` records. IDs and scope segments
  may contain any delimiter without colliding.
- Expression equality is symmetric and scalar-only, mixed integer/float
  comparison does not round the integer, and `len` accepts concrete arrays,
  slices, and maps before or after Store serialization. Unsigned integers above
  `math.MaxInt64` remain exact as `uint64` across JSON. The quoted
  `node["any ID"].path` form addresses workflow IDs that are not Go identifiers.
- `Scope` and `Expr.Refs` return copies rather than context- or expression-owned
  slices.
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
  `iterationStep`/`leafStep` for `Step`. Every constructor that has a config
  takes exactly one trailing value; its zero value selects the default.

### Breaking

- `Path` named two unrelated things. It now names only one: the RFC 6901 pointer
  in `Ref.Path`, spelled the way JSON Patch spells it. The execution scope it
  used to share the word with is `Scope` on `Event`, `Chunk`, `Suspension`, and
  `JournalKey`, matching the `Scope` and `WithScope` accessors those values were
  always paired with. `Journal` JSON writes `"scope"` and its format version is
  now 2, so a journal persisted under version 1 is rejected rather than
  misread. The variadic segments of `At` and `Ref.Child` are named `segments`,
  since they are unencoded segments rather than a pointer.
- The registry named one concept two ways. It is now `Node` throughout, matching
  the `NodeTypes` and `NodeSchema` it always had: `LeafFactory` is
  `NodeFactory`, `RegisterLeaf` and `MustRegisterLeaf` are `RegisterNode` and
  `MustRegisterNode`, and their `RegistrationError.Kind` is `"node"`. A factory
  may return any Step, so calling it a leaf factory was wrong once
  `SubgraphFactory` existed. The flat graph's node declaration is `GraphNode`,
  which frees `NodeSpec` for the value a factory receives — the former
  `LeafSpec`. JSON field names are unchanged in both DSLs.
- `Store.With` is `Store.WithCell`, naming the unit the Store's own
  documentation uses and pairing with `WithOutput`.
- `Index` is `ItemIndex`, pairing with `Item` and no longer colliding with the
  universal meaning of an index.
- Graph nodes can no longer observe an unrelated node merely because a former
  layer barrier happened to run it earlier; wire every Store read through
  `GraphNode.Input` or `GraphNode.Inputs`, or declare a pure control edge with
  `DependsOn`. Failure may now return writes from nodes that completed before
  the error instead of discarding the whole in-flight layer.
- Invalid `expr.SwitchSpec` values and conflicting `expr.Bindings` names now
  match `workflow.ErrInvalidSpec`; registering bindings into a nil Registry
  matches `workflow.ErrInvalidRegistration`. `expr.ErrUnsupported` is reserved
  for expression grammar rejected by `Parse`, as its documentation promises.
- Graph input introspection now belongs to the graph value. Replace
  `workflow.GraphInputs(g)` with `g.Inputs()` and
  `workflow.MissingInputs(g, store)` with `g.MissingInputs(store)`.
- `WithObserver`, `WithJournal`, `WithConfig`, and configuration readback were
  removed. Replace
  `step.Run(workflow.WithConfig(ctx, cfg), in)` with
  `workflow.Run(ctx, step, in, cfg)`. Call `step.Run` directly when no
  configuration is needed. A configured call always starts a fresh event
  sequence, so run-local mutable state cannot leak through a reused context.
- `Ref.Path` changed from a dotted path to an RFC 6901 JSON Pointer. Replace
  `At("node", "output.items.0")` with
  `At("node", "output", "items", "0")`, and JSON
  `"path":"output.items.0"` with `"path":"/output/items/0"`. `Child()` leaves a
  Ref unchanged; `Child("")` now addresses an empty object key.
- Negative `Concurrency` and `MaxIterations` values now fail with
  `flow.ErrInvalidConfig` instead of selecting an unbounded or default mode.
- `flow.Race` rejects the entire composition when any node is nil and waits for
  all losing calls to honor cancellation before returning.
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
- `workflow.NodeFactory` takes a single `NodeSpec` value instead of positional
  `(id string, input Ref, config json.RawMessage)` arguments, so a factory sees
  every wired port and the signature can grow without breaking callers.
- `NodeSchema.Input ValueType` became `NodeSchema.Inputs Ports`, keyed by port
  name. Use `workflow.OnePort(t)` for a single-input node.
- `workflow.Factory` reports `ErrMissingPort` when the default port is unwired.
  Such a node could never have run, so the failure moved from run time to compile
  time; a node that takes no input needs a custom `NodeFactory` supplying its own
  `BindFunc`.
- A registered schema that declares ports now makes wiring mandatory. Graphs that
  relied on a declared input being optional will fail validation.
- Replace imports of `github.com/Tangerg/flow/core` with
  `github.com/Tangerg/flow`.
- Configure bounded operations with config structs (`flow.MapConfig`,
  `flow.LoopConfig`, `workflow.ParallelConfig`, `workflow.IterationConfig`); the
  `XxxN` variants were removed.
- `flow.Map`, `flow.Loop`, `flowx.FanOut`, `workflow.Parallel`, and
  `workflow.Loop` now require exactly one trailing config value. Pass the zero
  config for defaults. This replaces the former variadic shape, which allowed
  several configurations to compile and silently ignored all but the first.
- `Journal.Keys` returns `[]JournalKey` instead of encoded `[]string` values,
  `Journal.Forget` accepts a `JournalKey`, and Journal JSON uses the versioned
  structured-record format instead of a flat string-keyed object.
- `Suspend` accepts `any`, and `Suspension.Value` replaces
  `Suspension.Reason string`.
- `Suspension` uses lower-case JSON fields (`id`, `path`, `await`, `value`);
  zero-valued optional fields are omitted.
- `AwaitFactory` rejects config and non-default input ports instead of silently
  ignoring them. Use `InterruptFactory` when a serialized wait needs to expose a
  request value.
- `workflow.Pipeline` and `Pipe` were removed. Build sequential and parallel
  stages with `Sequence` and `Parallel`.
- `flowx.Retry`, `Timeout`, and `Trace` were removed; `flowx` is now control-flow
  sugar only. Wrap a `Node` for resilience, or use a library. The `Binder`
  interface was removed; pass a `BindFunc` to `Leaf`.
- `flowx.Race` moved to `flow.Race` (core primitive). Update the import path.
- `flowx.FanOut` and `workflow.Parallel` take their nodes/branches as a slice
  with one trailing config value, replacing the previous required leading config
  and variadic nodes/branches.
- `flowx.FanOutAll`, `flowx.MapAll`, the `flowx.Result`/`Collect` collecting API,
  and `flowx.Identity` were removed. Use `flow.Race`/`flowx.FanOut` for control
  flow, aggregate errors yourself, and write a one-line `NodeFunc` (or
  `flowx.Chain()`) for a pass-through.
- Use `Output`, `Item`, and `ItemIndex` instead of exported Store path constants;
  use `ObserverFunc` instead of the removed event collector.
- Consume `workflow.Description` directly; Mermaid rendering is no longer part
  of the core workflow package.
- Use `workflow.NodeSchema` instead of the ambiguous `workflow.Schema` name.
- See the migration list in `README.md` for the complete pre-v1 API rewrite.

[Unreleased]: https://github.com/Tangerg/flow/commits/main
