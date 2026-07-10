// Package workflow is the dynamic layer built on the core primitives.
//
// Where core composes statically typed nodes at compile time, workflow threads a
// heterogeneous variable pool — the [Store] — through nodes addressed by ID, so
// graphs can be assembled at runtime (from config, a builder, or a visual
// editor).
//
// The Store is persistent: every write returns a new structural snapshot that
// shares untouched cells with the original. Values are held as-is and must be
// treated as immutable after insertion.
// Store snapshots can be serialized with encoding/json; decoding replaces a
// Store atomically.
//
// A workflow node is a [Step] — a core.Node[Store, Store] that reads its inputs
// from the Store and returns a Store extended with its output. Typed business
// logic is bridged in with [Adapt], and composites ([Sequence], [Branch], [Loop],
// [Parallel], [Iteration]) are built from the core primitives.
package workflow
