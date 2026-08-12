# Changelog

All notable user-visible changes to this project are documented here. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
releases follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This section describes the public shape being prepared for the first release.
Intermediate pre-release refactors remain available in Git history rather than
being presented as migrations from a version that was never published.

### Added

- A Go 1.26 typed core built around `Node[I, O]`, `NodeFunc`, `Then`, `Switch`,
  `Loop`, `Map`, and `Race`, with cooperative cancellation and `Validate` for
  deterministic, recursively composable definition checks.
- Derived composition in `flowx`: `Chain`, `FanOut`, `Combine`, and `Fallback`.
- `flow`, `flowx`, and `workflow` composites participate in the same recursive,
  side-effect-free `flow.Validate` contract, including before Journal replay.
- Workflow loop and parallel configs are semantic aliases of the corresponding
  core configs; Graph, Spec, parallel, and iteration bounds now share their
  validation and `flow.ErrInvalidConfig` category.
- The optional `workflow` runtime for immutable named state, typed references,
  reusable Steps, sequence, parallel, branch, loop, iteration, and sealed
  subgraphs.
- A small `Binder[I]` protocol for preparing typed leaf inputs, with
  `BinderFunc[I]` as its function adapter and definition-aware `From` and
  `FirstOf` implementations that validate references before Journal replay.
  Custom Binders may participate through the same optional `Validate() error`
  convention used by composite nodes.
- `Resolver` is a semantic alias for `flow.Node[Store, string]`, so composed
  typed nodes work directly with `Branch`, `Route`, and Registry registration;
  ordinary functions use the existing `flow.NodeFunc` adapter, and registration
  rejects an invalid visible composition immediately.
- Flat, dependency-driven `Graph` execution with direct-edge Store visibility,
  named ports, explicit control dependencies, graph-wide concurrency limits,
  conditional outlets, bypass, and mutually exclusive merges.
- Structured `Spec` definitions for nested control flow, plus strict Graph and
  Spec JSON decoding and self-contained Draft 2020-12 schemas.
- A Registry boundary for named Go capabilities, port and output schemas,
  configuration schemas, conditions, and resolvers.
- Stable sentinel errors and structured diagnostics for steps, references,
  registration, Graph definitions, and nested Spec definitions. Runtime
  `StepError` values carry the same structured execution scope as events,
  chunks, suspensions, and Journal keys, while definition errors remain
  deliberately unscoped.
- Nil factory and condition registrations preserve `flow.ErrNilFunc` beneath
  their `workflow.ErrInvalidRegistration` category.
- Nested typed-value resolution preserves JSON encoding failures through `Get`,
  `Await`, and expression evaluation instead of misreporting them as missing
  references. The expression `has()` predicate suppresses only true absence;
  malformed nested values remain type errors instead of silently becoming
  `false`.
- Definition and execution identities are validated as UTF-8 before work, so a
  JSON round trip cannot silently rename steps, ports, references, or routes.
- `Ref.Validate` and `ScopeFrame.Validate` expose the same definition checks to
  caller-defined Binders, Steps, and repeated composites.
- `Condition` is `flow.Node[Store, bool]`, matching `Resolver`'s
  `flow.Node[Store, string]`, so the two decision shapes differ only in what
  they return. A condition composes with `Then`, `Map`, and the other typed
  helpers, is validated at registration like a resolver, and may implement the
  optional `Validate` convention. It no longer receives an iteration index
  parameter: a condition runs inside its iteration's indexed scope, so `Scope`
  reports that index when a decision needs it. Registering a nil condition now
  reports `flow.ErrNilNode` rather than `flow.ErrNilFunc`, because a
  `NodeFactory` is the only registered kind that is still a bare function.
- Composite steps have one construction shape. `Branch`, `Loop`, `Iteration`,
  and `Subgraph` each take a single `Config` struct that owns every field,
  including ID and body, so a composite is never configured half positionally
  and half through a config. `Parallel` takes `ParallelConfig{Steps,
  Concurrency}`. `LoopConfig` and `ParallelConfig` are workflow structs rather
  than aliases of `flow.LoopConfig` and `flow.MapConfig`, which is what let them
  carry those fields; the `flow` config still defines what a shared setting
  means. `Sequence` remains variadic, and steps without children keep positional
  parameters. The JSON `Spec` and `Graph` forms are unchanged.
- Every persisted wire type names its members exactly. `Ref`, `ScopeFrame`,
  `JournalKey`, `Suspension`, and the Journal document reject unknown,
  duplicate, and case-folded members, so an alternate spelling cannot satisfy a
  field and two spellings of one member cannot let document order decide which
  value survives. This matters most for `Suspension` and `JournalKey`, which
  applications persist in their own run records outside any schema.
- Suspension as a third outcome through `Await`, `Interrupt`, and `Suspend`,
  with structured waits and checkpoint-and-restart Journal replay.
- JSON persistence for Store and Journal values. Journal wire version 4 uses
  structured scope-aware identities; a scope frame's optional `index` member
  distinguishes repeated invocations without a redundant boolean.
  `WithScope` and `WithScopeIndex` let caller-defined composites preserve those
  identities, and `ScopeFrame.Validate` lets them reject invalid
  zero-invocation definitions. The decoder accepts only the current wire
  version; applications migrate durable checkpoints before upgrading.
- Typed streaming producers through `StreamFunc` and a run-scoped,
  backpressure-aware `Emitter`.
- Run-scoped lifecycle observation, ordered events and chunks, and Store write
  sets for auditing.
- The optional `workflow/expr` package for restricted, side-effect-free
  conditions and routing rules defined as data.
- The optional `workflow/diagram` package for deterministic ASCII and Mermaid
  Graph renderings.
- A progressive tutorial series, output-checked public examples, and CI gates
  for formatting, module tidiness, race detection, statement coverage,
  vet, lint, vulnerability scanning, documentation, and fuzzing.

### Changed

- The CI statement-coverage gate now enforces a 95% minimum instead of exact
  100%. Coverage remains a regression signal without forcing unreachable
  defensive branches or implementation-coupled tests into the library.

### Fixed

- Joined suspensions no longer expose their internal error tree through
  `errors.As`; mutating a returned `Suspension` cannot change later
  `Suspensions` results from the same error.
- Iteration validation now rejects a child path below `ItemIndex`, whose value
  is always a scalar integer, instead of accepting an output reference that can
  only fail while collecting results at run time.
- `Get` now accepts a stored `nil` for every Go type to which `nil` is
  assignable, including `unsafe.Pointer`, instead of misclassifying that one
  nilable kind as a type mismatch.
- `Bindings.UnmarshalJSON` and `SwitchSpec.UnmarshalJSON` now reject nil
  receivers with an error instead of parsing the document and then panicking.
- Expression bindings and switch definitions now reject `null`, arrays, and
  scalars at their top-level JSON boundary instead of silently treating a
  non-object document as an empty configuration.
- Graph validation now rejects nested references that cannot exist within a
  declared scalar output or that use a non-index first child for an array,
  instead of deferring those impossible edges to a run-time missing value.
- Persisted suspension identity can no longer be silently renamed, partially
  decoded, or carry a scope without a step ID. `Suspension` and standalone
  `JournalKey` JSON reject unknown or duplicate members, invalid Unicode,
  malformed identity, and excessive nesting atomically; suspension numbers
  decode as exact `json.Number` values.
- `workflow/expr` configuration now shares the engine's strict, lossless JSON
  document contract. `Bindings` and standalone `SwitchSpec` values reject
  unknown or duplicate members, invalid Unicode, multiple values, and excessive
  nesting; failed decoding is atomic and invalid in-memory UTF-8 cannot be
  silently rewritten during encoding.
- `Graph` and `Spec` JSON encoding is now lossless and structurally bounded:
  invalid UTF-8 identity text, malformed or ambiguous raw config, cyclic Spec
  bodies, and excessive nesting in the fully assembled wire document fail with
  located structured errors instead of being rewritten or producing a
  definition the strict decoder rejects.
- Direct `encoding/json` decoding into `Graph` and `Spec` now uses the same
  strict, atomic DSL boundary as `ValidateGraphJSON`, `ValidateSpecJSON`, and
  Registry compilation. Unknown or duplicate members can no longer disappear
  before programmatic validation, and every JSON Schema integer spelling maps
  to Go `int` consistently.
- Nil detection is now confined to each built-in function adapter instead of
  rejecting every caller-defined named function type whose zero value is nil.
  Such types now retain the same authority over nil-receiver behavior as
  pointer-based implementations, while `NodeFunc`, `BinderFunc`, and
  `StreamFunc` still fail validation before work or Journal replay.
- Expression evaluation now recognizes mathematically integral JSON numbers in
  decimal and exponent notation before converting to `float64`, so values such
  as `9007199254740993.0` retain exact `int64` or `uint64` semantics instead of
  being silently rounded. Shared bounded parsing also prevents a short number
  with a huge exponent from allocating memory in proportion to that exponent.
- An empty `FirstOf` binding is now rejected as an invalid definition before
  Journal replay instead of becoming a permanently missing input only on fresh
  execution.
- Graph and Spec JSON validation and compilation now share one typed decoding
  boundary. JSON Schema integers are decoded by value, so integral decimal and
  exponent forms such as `1.0` and `1e0` are accepted consistently, while
  values outside the platform's `int` range are rejected consistently.
  Application-owned raw node config remains byte-for-byte unchanged.
- Journal wire versions and scope indexes now use the same mathematical JSON
  integer semantics as definitions. Equivalent decimal and exponent spellings
  are accepted exactly, while scope indexes outside `uint64` remain rejected.
- Invalid leaf Binder and Node definitions now report `StepError.OpValidate`;
  `OpBind` is reserved for input-preparation errors and `OpRun` for the admitted
  execution boundary. Component names stay in the wrapped diagnostic.
- Workflow validation can no longer be mistaken for a resumable suspension: a
  validator that returns `ErrSuspended` is rejected as `flow.ErrInvalidConfig`
  before an enclosing composite classifies execution outcomes.
- Node factories likewise cannot turn definition construction into a resumable
  outcome: `CompileGraph` and `CompileSpec` classify a factory suspension as a
  node-type `flow.ErrInvalidConfig` error without retaining `ErrSuspended`.
- Subgraph input diagnostics now identify inner seed IDs instead of calling
  them Graph input ports.
- Loop stop checkpoints now re-sample parent cancellation after the Journal
  write, so cancellation retains the pre-iteration Store and takes precedence
  over a concurrent Journal conflict, matching leaf and branch checkpoints.
- Subgraph input binding, output projection, and graph gate evaluation now
  re-sample parent cancellation around JSON-backed reads. Cancellation takes
  precedence over a simultaneous projection error, and later values are not
  encoded after the run has been cancelled.
- Expression conditions, resolvers, and switches now give cancellation observed
  during evaluation precedence over a computed value or evaluation error, and
  a switch does not evaluate a later case after cancellation.
- A conditionally gated Graph target now snapshots each distinct routing source
  once, so several `TriggerAny` gates on that source compare one stable JSON
  representation instead of repeatedly invoking its custom encoder.
- Graph gates now compare the routing output's JSON string representation, so
  `encoding/json` normalization cannot make a persisted Journal select a
  different branch after restart.
- A package-level `workflow.Run` invoked inside another run now starts an
  independent root scope instead of inheriting the enclosing composite's
  identity. An explicit scope attached before the outermost run remains the
  caller-defined composite's initial scope.
- `expr.Switch` now reports `flow.ErrNoCase` when no expression matches and no
  fallback is configured; missing Store references remain the sole meaning of
  `expr.ErrUndefined`. Expression references and Switch branch names that
  cannot survive workflow persistence are now rejected during compilation.
- `SubgraphFactory` validates its complete captured body and classifies a bad
  fixed definition as a node-type registration error. Structured Specs also
  locate an impossible iteration or subgraph `bodyOutput` at that field.
- `Registry.ValidateSpec` now rejects an iteration or subgraph projection that
  registration metadata can prove absent, matching `CompileSpec` without
  executing node factories. An unschematized leaf remains conservatively
  unknown until compilation inspects its concrete boundary.
- Graph JSON now treats an empty `when` array like an omitted one when the
  trigger is the zero-value “all” rule, matching the Go definition and the
  existing empty `dependsOn` contract. `TriggerAny` still requires at least one
  gate in every representation.
- A Graph nested in Parallel now preserves its Store namespace ownership when a
  node is bypassed. Engine-owned removals carry private change identity through
  fan-out merging and Store compaction, so a stale output cannot be resurrected;
  unrelated Stores still cannot delete cells merely by omitting them.
- Parallel and iteration now re-sample parent cancellation across result
  collection and Store merging, so cancellation during those linear commit
  stages cannot publish a completed output or hide the parent cause. Their
  documentation and caller-defined composite examples state the same Store
  commit rule.
- `flowx.Fallback` now discards a primary or alternate result that races with
  parent cancellation, matching the cancellation commit rule used by the typed
  core's sequential composites.
- A failed streaming Emitter now remains the leaf's error even when a producer
  ignores the stopped stream and returns a different error afterward.
- A `StreamFunc` now closes its invocation-scoped yield capability on every
  stack exit, so a callback retained by a panicking producer cannot emit after
  an outer boundary recovers.
- Observer and Emitter callback contexts are now detached from the producing
  workflow identity while preserving application values and cancellation. A
  callback that directly invokes a Step can no longer claim identities, emit
  signals, or write checkpoints into the run it is consuming.
- A suspension already identified at the root of an independent nested `Run`
  now keeps that root identity when propagated through an outer scoped leaf, so
  `Suspension.Key` records the result under the key the nested run will replay.
- Parent cancellation observed while a leaf output or branch decision is being
  committed now takes precedence even when the Journal operation also fails.
- Subgraph now wraps ordinary body failures in a `StepError` naming the sealed
  instance while leaving suspension identity and scope unchanged.
- Whitespace-only `json.RawMessage` config and config schemas are now rejected
  instead of being treated as absent. Zero length is the single omission form,
  so programmatic definitions follow the same JSON-value boundary as the DSL.
- `Switch` validation now identifies every invalid case and preserves all of
  their causes in one deterministic joined diagnostic instead of arbitrarily
  discarding all but one error from its unordered case map.
- Workflow composites now honor the optional `Validate() error` contract of a
  caller-defined child Step before any work. The boundary remains structurally
  opaque; only its own declared validation is observed, matching `flow`
  composites and top-level `workflow.Run`.
- Suspension classification now treats an error whose `Unwrap` exposes no
  non-nil children as a leaf, preserving a custom `Is(ErrSuspended)` identity
  consistently with the standard `errors.Is` traversal.
- JSON Schema failures now sort and deduplicate their public leaf diagnostics;
  identical invalid Graph or Spec documents no longer inherit nondeterministic
  map traversal order from the replaceable validator backend.
- Registered config schemas now enforce the documented Draft 2020-12 dialect at
  the root and every subschema position. An explicit legacy or mixed dialect can
  no longer silently change keyword semantics through the validator backend;
  the canonical HTTP and HTTPS dialect URIs may carry their equivalent empty
  fragment. Application objects carried by `const`, `enum`, defaults, or
  examples remain ordinary instance data.
- Programmatic Spec validation now reports an unknown `kind` before fields or
  constraints that cannot have meaning until a valid variant is selected.
- Required workflow names now share one non-empty, UTF-8 validation rule;
  programmatic Specs report empty resolver and condition names directly rather
  than misclassifying them as unknown registrations.
- Workflow Step documentation now makes non-success Store ownership explicit:
  transparent Sequence and Branch composition preserves a child's returned
  state, while Loop, Parallel, Iteration, Subgraph, and Graph retain their
  documented commit boundaries.
- Workflow descriptions now return recursively owned snapshots through both
  `workflow.Describe` and every built-in step's `Describer` method, so a
  caller-defined Describer cannot leak mutable child storage through a built-in
  description tree.
- `workflow.Run` now closes the context carrying its run-scoped identity and
  callbacks on every exit, including panic unwinding, so detached custom work
  cannot keep using a completed execution boundary.

[Unreleased]: https://github.com/Tangerg/flow/commits/main
