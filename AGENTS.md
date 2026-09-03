# Flow repository guidance

## Project philosophy

- Prefer designs that are coherent, readable, and easy to explain. Beauty here means that names,
  responsibilities, dependency directions, and runtime behavior agree with one another.
- Make important relationships explicit. Data edges belong in declared references, ownership belongs
  to the boundary that derived the thing, identity belongs in the scope, and runtime choices belong
  in ordinary parameters. Do not infer them from ambient state, call order, or hidden globals.
- Choose the simplest model that completely expresses the requirement. Do not confuse simplicity
  with missing semantics: irreducible complexity should remain visible instead of being hidden
  behind magic.
- Keep the conceptual structure as flat and orthogonal as the domain permits. Add nesting only when
  it represents real ownership or composition, never merely to organize implementation details.
- Keep the API sparse. Every exported concept must earn its place, have one precise responsibility,
  and compose with the existing primitives.
- Optimize for the reader. Prefer ordinary Go, intention-revealing names, small state machines, and
  local reasoning over clever generics, reflection, code generation, or surprising control flow.
- Special cases do not justify a second semantic path. Express them through composition or a higher
  layer unless the underlying abstraction is genuinely different — an arity, an empty collection, or
  a degenerate shape is not a different abstraction.
- Let practicality correct theory. Use tests, benchmarks, and the runnable examples to challenge a
  design, while preserving the invariants that make the system understandable.
- Never let an error disappear accidentally. Propagate, join, or deliberately classify it as
  cancellation or as a suspension; silence it only at a boundary whose behavior is documented.
- Refuse to guess when a document, a wiring, an identity, or a stored value is ambiguous. Reject the
  operation with a precise error that names the field a caller can repair.
- There should be one obvious canonical way to express each semantic operation. A convenience form
  is acceptable only when it mechanically compiles to that path and owns no second state machine:
  `LeafFunc` builds a `Leaf`, `OneInput` builds `Inputs`, `Factory` builds a `NodeFactory`.
- Implement a proven need now, completely. Leave speculative features unimplemented rather than
  shipping a premature abstraction that must later be replaced.
- Treat explainability as an architecture test. If an implementation is hard to explain in terms of
  the public model, first assume the model or the implementation is wrong.
- Use namespaces deliberately. Package layers, the definition-field vocabulary, and the error
  sentinels communicate ownership; a `Store` is not a bag of globally mixed names, because every
  cell is owned by the node that produced it.

## Flow architecture axioms

- Composition is preferred over privilege. Every capability is a `flow.Node[I, O]`, a `Step` is a
  `flow.Node[Store, Store]`, and combinators take nodes and return nodes. There are no framework
  base types, privileged steps, or hooks that only the package itself may install.
- The same capability at the same layer has exactly one canonical API. A composite that names itself
  takes exactly one `Config` struct that owns every field; a step with no children takes positional
  parameters; `Sequence` stays variadic because it has no settings. No functional options, no second
  configuration form, no alias. A second form runs perfectly, so the pairing of a `Config` with the
  one constructor named after it is checked directly — `TestEveryConfigStructHasOneConstructor`.
- The three construction routes agree. Built in Go, compiled from a `Spec`, or compiled from a flat
  `Graph`, the same workflow must produce the same run: a serialized form is a second spelling,
  never a second execution protocol — `TestEveryConstructionFormRunsTheSameWorkflow`.
- Keep the atoms orthogonal. `Store` is data, `Journal` is what a run may replay, `Scope` is
  execution identity, `Event` is observation, `Suspension` is the third outcome, and `Registry` is
  what a name resolves to. Do not make one atom secretly perform another atom's job.
- Scope expresses execution identity only. It is not data isolation, which `Store` namespaces and
  `Subgraph` provide, and it is not a permission boundary. A repeated boundary publishes its index
  through the scope rather than through a parameter its children would have to thread.
- A cell belongs to the node that produced it. A step writes its own output and never another's,
  `Subgraph` is what hides a body's cells, and a merge can neither overwrite a sibling's work nor
  resurrect a cell an engine boundary removed.
- Data edges are declared, not discovered. A step reads exactly what its references name, and one
  immutable `Registry` snapshot serves one validation or compilation, so nothing resolves a name
  through ambient scope, a call stack, or a live proxy.
- Only committed state is visible. A failed or cancelled step returns the Store it was given, a
  parallel merges only the branches that finished, a suspension publishes no partial output, and a
  run replays only the records that existed when it began.
- Ownership is structural. A boundary that derives a context ends it before returning, and work that
  outlived its boundary is refused rather than silently accepted — see
  `TestEveryBoundaryClosesTheContextItDerived`. A parent's cancellation outranks whatever a child
  reported at every boundary.
- Package dependencies point in one direction. `flow` is the primitive layer and depends on nothing
  here; `flowx` derives combinators from it; `workflow` adds named state, durability, and
  serialization; `workflow/expr` and `workflow/diagram` stay optional and derived; `internal/...`
  is not API. A higher layer may add vocabulary, never a second copy of a lower layer's rule. No
  behavior can reveal a broken layer, so the import graph is checked directly —
  `TestPackageDependenciesPointOneWay`.
- A contract stated twice is pinned twice. The embedded JSON Schemas, the field matrices, and the Go
  validators are deliberate second statements, and each has a test that fails when the copies
  disagree — `TestSpecFieldMatricesAgreeWithTheSpecStruct`,
  `TestTheTwoValidatorsRefuseTheSameDefects`.
- Errors carry structure rather than prose. A sentinel gives the category, a `StepError`,
  `GraphError`, `SpecError`, or `expr.Error` gives the location, and a surfaced message names the
  package exactly once — `TestSurfacedErrorsNamePackageExactlyOnce`.

## Working rules

- Backward compatibility is not a goal during the current development stage. When a design changes,
  remove obsolete paths instead of adding compatibility layers, fallbacks, aliases, or migrations.
- Fix causes rather than symptoms. Do not accept a stopgap that is intended to be replaced later;
  make architectural decisions for the long term while breaking changes are inexpensive.
- Grow the system in complete vertical slices. Start with the smallest end-to-end version that
  works, then add capabilities without trading a working product for unfinished infrastructure.
- Keep components modular and responsibilities sharply separated. Introduce a pattern or an
  abstraction only when it makes an existing responsibility clearer or a real composition point
  possible.
- Prefer established, maintained libraries when they reduce total complexity or improve reliability.
  Check the dependencies, documentation, and types already present before reimplementing
  functionality or adding a package.
- Do not optimize from intuition alone. Measure the relevant path, make the simplest change the
  evidence supports, and keep a benchmark or behavioral guard when the regression risk is real.
- Tests protect semantics and architectural boundaries, not implementation trivia. For anything
  worth protecting, confirm that the test fails when the protected behavior is removed rather than
  assuming it would.
- Keep documentation, exported comments, runtime behavior, and the repository's own guards
  consistent within one change. A comment that cites a test has to resolve, a documented API name
  has to exist, and a doc link has to point at something — `TestCitedTestsResolve`,
  `TestDocumentedAPINamesResolve`, `TestGoDocLinksResolve`.
- Use explicit `Config` structs for related construction settings, and give optional fields useful
  zero meanings.
- Treat repository-local usage as no evidence for or against a public API. This is a library: retain
  or remove exported operations and extension points by responsibility, abstraction quality, and
  downstream utility, never merely because code in this repository does or does not call them.
- The cited design boundaries, the review checklist, and the full local gate live in
  [CONTRIBUTING.md](./CONTRIBUTING.md). Read the boundaries before changing behavior or an exported
  API, and add one when a change establishes a rule the next contributor would otherwise rediscover.
