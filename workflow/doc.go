// Package workflow is the dynamic layer built on the core primitives.
//
// Where core composes statically typed nodes at compile time, workflow threads a
// heterogeneous variable pool — the [Store] — through nodes addressed by ID, so
// graphs can be assembled at runtime (from config, a builder, or a visual
// editor).
//
// The Store is immutable: every write returns a new Store that shares untouched
// data with the original. This makes concurrent branches safe by construction
// and turns every intermediate state into a cheap snapshot.
//
// A workflow node is a [Step] — a core.Node[Store, Store] that reads its inputs
// from the Store and returns a Store extended with its output. Typed business
// logic is bridged in with [Adapt], and composites (Sequence, and later Branch,
// Loop, Parallel, Iteration) are built from the core primitives.
package workflow
