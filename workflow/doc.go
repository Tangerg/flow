// Package workflow is the dynamic layer built on package flow's primitives.
//
// Where flow composes statically typed nodes at compile time, workflow threads a
// heterogeneous variable pool — the [Store] — through nodes addressed by ID, so
// graphs can be assembled at runtime (from config, a [Pipeline], or a visual
// editor).
//
// The Store is persistent: every write returns a new structural snapshot that
// shares untouched cells with the original. Values are held as-is and must be
// treated as immutable after insertion.
// Store snapshots can be serialized with encoding/json; decoding replaces a
// Store atomically.
//
// A workflow node is a [Step] — a flow.Node[Store, Store] that reads its inputs
// from the Store and returns a Store extended with its output. Typed business
// logic is bridged in with [Leaf]; [Factory] adapts the common case of a typed
// node constructor with JSON config. Composites ([Sequence], [Branch], [Loop],
// [Parallel], [Iteration]) are built from flow's primitives. [Pipe] provides a
// fluent API for assembling the same composites; its [Pipeline] result is
// itself a Step.
//
// [SpecJSONSchema] and [GraphJSONSchema] expose the Draft 2020-12 schemas for
// the two JSON DSL shapes. [ValidateSpecJSON] and [ValidateGraphJSON] perform
// portable structural checks; a Registry adds node, config, type, and graph
// semantics when it validates or compiles the decoded workflow.
//
// Errors preserve their causes for errors.Is and errors.As. [RefError],
// [RegistrationError], [GraphError], [SpecError], and [StepError] identify the
// exact boundary that failed.
package workflow
