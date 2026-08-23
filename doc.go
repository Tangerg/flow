// Package flow provides the minimal, type-safe building blocks for composing
// in-process control flow.
//
// The package keeps one small set of general composition operations:
//
//   - [Node] and [NodeFunc]: the abstraction and its function adapter.
//   - [Then]: sequential composition.
//   - [Switch]: data-dependent selection.
//   - [Loop]: bounded iteration configured by [LoopConfig].
//   - [Map]: concurrent execution over a collection, waiting for all items.
//   - [Race]: concurrent execution over one input, returning the first success.
//
// These cover sequence, selection, bounded iteration, collection concurrency,
// and first-success concurrency. Package flowx builds conveniences such as
// fan-out, heterogeneous fan-in, variadic sequencing, and fallback from this
// core. Keeping derived shapes there leaves this package's protocol and error
// contracts small.
//
// Constructors snapshot the structure of map and slice arguments that become
// part of a definition. Changing the source collection after construction does
// not reconfigure the resulting Node. The Node, function, and other behavior
// values stored in those collections are retained as-is; they must obey the
// concurrency contract of the composite that invokes them.
//
// Cancellation outranks results. Every composite here checks the context before
// invoking a child and again before committing what the child returned, so a
// cause observed at either point is what Run reports — never a result produced
// alongside it. Cancellation is otherwise cooperative: a Node that ignores its
// context delays the composite that started it, and a composite that started
// concurrent work does not return before that work does. Each operation
// documents what it discards or rolls back when this applies.
//
// Errors preserve their causes. Collection and selection operations report
// positions with [IndexError] and [CaseError], allowing callers to use
// [errors.Is] and [errors.As] instead of matching strings. Built-in composites
// validate their complete visible definition before invoking any child, so
// nesting one cannot hide an invalid descendant. [Validate] exposes the same
// read-only check to boundaries that may replay a result without calling the
// Node. Caller-defined composites can participate with a side-effect-free
// Validate() error method; other Nodes are opaque and remain responsible for
// execution-time validation.
//
// The core owns no durable execution state. Package workflow adds named state,
// streaming identity, and checkpoint-and-restart at explicit step boundaries;
// distributed scheduling, leases, and deterministic replay remain the domain
// of a durable workflow service.
package flow
