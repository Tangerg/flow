// Package workflow is the dynamic layer built on package flow's primitives.
//
// Where flow composes statically typed nodes at compile time, workflow threads a
// heterogeneous variable pool — the [Store] — through nodes addressed by ID, so
// graphs can be assembled at runtime (from config or a visual editor).
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
// node constructor with JSON config, and [BindFactory] the case of a node
// reading several inputs. Composites ([Sequence], [Branch], [Loop], [Parallel],
// [Iteration]) are built from flow's primitives.
//
// # Named input ports
//
// A node names each value it reads: [Inputs] wires port names to references, and
// a [NodeSchema] declares the ports a node type expects along with their types.
// Naming inputs is what keeps the data flow visible to the layer above — the
// flat [Graph] derives its execution order from the wired ports, and
// [Registry.ValidateGraph] reports both incomplete wiring and incompatible edges
// before anything runs. A node that instead reads references out of its own
// config is invisible to both. [Registry.NodeTypes] and [Registry.NodeSchema]
// expose the registered vocabulary for an editor to render.
//
// [SpecJSONSchema] and [GraphJSONSchema] expose the Draft 2020-12 schemas for
// the two JSON DSL shapes. [ValidateSpecJSON] and [ValidateGraphJSON] perform
// portable structural checks; a Registry adds node, config, type, and graph
// semantics when it validates or compiles the decoded workflow. [GraphInputs]
// and [MissingInputs] report the external values a Graph reads, so a caller can
// pre-flight a run instead of discovering a missing value mid-flight.
//
// # Configuring a run
//
// One [RunConfig] carries everything a single run needs — what watches it and
// what lets it resume — and [WithConfig] installs it:
//
//	ctx = workflow.WithConfig(ctx, workflow.RunConfig{
//		Observer: workflow.ObserverFunc(log),
//		Journal:  workflow.NewJournal(),
//	})
//	out, err := pipeline.Run(ctx, in)
//
// It travels in the context rather than in a Step's construction because it
// belongs to the run and not to the definition: a compiled workflow is built once
// and run many times, concurrently, and each run wants its own Journal. Adding a
// field later does not break callers, since the struct is built with keyed fields.
//
// # Suspension and resumption
//
// A step can stop a run without failing it. [Suspend], [Await], and [Interrupt]
// report that the work cannot proceed yet — a person has to decide, an external
// job has to finish — and the run ends with an error matching [ErrSuspended].
// [Suspensions] returns every wait, including its structured application value
// and its scope-aware [Suspension.Key]. Suspension is a third outcome, not a kind
// of failure, so [Parallel] and [Iteration] let their remaining work finish
// rather than cancelling it.
//
// Await is a Store gate: it passes through once its Ref exists. Interrupt is a
// request/response Step: it exposes a value, then produces the response under
// [Output] after the caller records it in the Journal:
//
//	wait := workflow.Suspensions(err)[0]
//	if err := journal.Record(wait.Key(), response); err != nil { ... }
//	out, err := pipeline.Run(ctx, paused)
//
// Attach a [Journal] through [RunConfig] and a later run continues instead of
// starting over: every step it already completed is skipped and its result
// restored. Records are keyed by scope path and step ID, so this stays correct
// where one step runs many times, and [Branch] and [Loop] also record the
// decisions they made — a resolver that is not a pure function of the Store cannot
// send a resumed run down the other branch. Both a [Store] and a Journal
// serialize; the Journal uses versioned records with structured scope paths, so
// the run that resumes need not be the process that started. Recording an
// Interrupt response under its ID and path makes repeated instances independently
// resumable without positional matching or delimiter-encoded keys.
//
// Suspension awareness lives in this package's composites. The generic
// combinators in flow and flowx know nothing about a Store, so they treat a
// suspension as any other error and fail fast; compose Steps with [Sequence],
// [Parallel], [Branch], [Loop], and [Iteration] when a workflow can suspend.
//
// What this is not is a durable workflow engine. There is no scheduler, no timer,
// and no exactly-once guarantee: a step that suspends after a side effect and
// before recording its result will repeat that effect. Resumption is
// checkpoint-and-restart at step granularity, which fits an approval, a callback,
// or a retry window — not a distributed saga.
//
// # Observation
//
// Distribution and deterministic replay stay out of scope, but the package
// carries enough on each [Event] to build tracing and durability outside it:
// [Event.Seq] orders a run, [Event.Path] distinguishes repeated executions of one
// step under [Loop] and [Iteration], and [Event.Store] is the serializable
// snapshot a step produced. [Store.Changes] narrows that snapshot to just the
// step's writes, the delta an audit log or an external persister records. Attach a
// receiver through [RunConfig].
//
// # Behavior by name
//
// A serialized workflow cannot carry closures, so it names its leaf types,
// resolvers, and conditions and the [Registry] supplies the code. Subpackage
// expr closes the remaining gap for control flow: it compiles a small,
// side-effect-free expression over a Store into ordinary [Condition] and
// [Resolver] values, so a branch rule or a loop's stop test can live in config
// rather than in Go. It is opt-in and this package does not import it.
//
// Errors preserve their causes for errors.Is and errors.As. [RefError],
// [RegistrationError], [GraphError], [SpecError], and [StepError] identify the
// exact boundary that failed.
package workflow
