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
// logic is bridged in with [Leaf], and streaming logic with [StreamLeaf];
// [Factory] adapts the common case of a typed node constructor with JSON config,
// and [BindFactory] the case of a node reading several inputs. Composites
// ([Sequence], [Branch], [Loop], [Parallel], [Iteration], [Subgraph]) remain
// ordinary Steps.
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
// # Conditional graphs
//
// A flat Graph routes through ordinary node output. A routing node declares its
// possible string [NodeSchema.Outlets], and a target's [GraphNode.When] gates it
// on one or more selected outlets. The zero [Trigger] requires every gate;
// [TriggerAny] runs a merge reached through any one arm. An unsatisfied target
// does not run, writes no output, and emits [EventBypassed]. Bypass is explicit:
// a missing value on an ungated node remains an error.
//
// [Route] turns a Store-based [Resolver] into a journaled routing leaf; a typed
// leaf that already returns a string needs no adapter. [FirstOf] binds the first
// available input at a mutually exclusive merge. Route decisions use the
// routing leaf's ordinary output, so Journal replay restores them without a
// second hidden state channel.
//
// Graph execution is dependency-driven rather than divided into topological
// barriers. A node starts as soon as every declared dependency completes,
// subject to the graph-wide concurrency bound. It receives only the initial
// Store and those dependencies' results, merged in declaration order.
// Suspension blocks descendants but preserves the waiting node's returned
// writes and does not stop unrelated ready work; failure cancels running
// siblings and preserves nodes that had already completed.
//
// A compiled Graph owns cells whose node IDs belong to that Graph. Each
// invocation removes those internal cells from its input Store, then rebuilds
// them from the current execution or Journal replay. Reusing a prior result with
// new external inputs therefore cannot revive an output from a now-bypassed arm.
//
// # Sealed subgraphs
//
// [Subgraph] turns any Step into an isolated composite. Declared inputs are
// copied from the outer Store into a fresh inner Store, the body runs under a
// scope derived from the subgraph ID, and one [SubgraphConfig.BodyOutput] is
// projected back to [Output] of that ID. Inner cells never leak out. The body
// remains responsible for its own static validation and Journal boundaries;
// replay derives the projected output again rather than recording a second
// hidden checkpoint.
//
// [SubgraphFactory] installs the same boundary as a registered Graph node.
// Because its inputs still come from [GraphNode.Inputs], the enclosing Graph can
// detect cycles, report external inputs, and check registered port types without
// inspecting or exposing the subgraph body. [Spec] and its JSON Schema also
// provide the "subgraph" kind for structured definitions.
//
// [SpecJSONSchema] and [GraphJSONSchema] expose the Draft 2020-12 schemas for
// the two JSON DSL shapes. [ValidateSpecJSON] and [ValidateGraphJSON] perform
// portable structural checks; a Registry adds node, config, type, and graph
// semantics when it validates or compiles the decoded workflow. [Graph.Inputs]
// and [Graph.MissingInputs] report the external values a Graph reads, so a caller can
// pre-flight a run instead of discovering a missing value mid-flight.
//
// # Configuring a run
//
// One [RunConfig] carries everything a single run needs — what watches it, what
// receives streaming output, and what lets it resume — and [Run] establishes
// the execution boundary:
//
//	cfg := workflow.RunConfig{
//		Observer: workflow.ObserverFunc(log),
//		Emitter:  workflow.EmitterFunc(send),
//		Journal:  workflow.NewJournal(),
//	}
//	out, err := workflow.Run(ctx, pipeline, in, cfg)
//
// Configuration belongs to the call rather than to the Step definition: a
// compiled workflow can be run many times, concurrently, and each call gets an
// independent signal sequence, receivers, and Journal configuration. Call
// Step.Run directly when no run configuration is needed.
//
// # Suspension and resumption
//
// A step can stop a run without failing it. [Suspend], [Await], and [Interrupt]
// report that the work cannot proceed yet — a person has to decide, an external
// job has to finish — and the run ends with an error matching [ErrSuspended].
// [Suspensions] returns every wait, including its structured application value
// and its scope-aware [Suspension.Key]. Suspension is a third outcome, not a kind
// of failure, so [Parallel], [Iteration], and [Graph] let work that is not
// downstream of the wait finish rather than cancelling it.
//
// Await is a Store gate: it passes through once its Ref exists. Interrupt is a
// request/response Step: it exposes a value, then produces the response under
// [Output] after the caller records it in the Journal:
//
//	wait := workflow.Suspensions(err)[0]
//	if err := journal.Record(wait.Key(), response); err != nil { ... }
//	out, err := workflow.Run(ctx, pipeline, paused, cfg)
//
// Pass a [Journal] to [Run] through [RunConfig] and a later run continues instead
// of starting over: every completed leaf boundary is skipped and its result
// restored. Records are keyed by scope and step ID, so this stays correct
// where one leaf runs many times, and [Branch] and [Loop] also record the decisions
// they made — a resolver that is not a pure function of the Store cannot send a
// resumed run down the other branch. Both a [Store] and a Journal serialize; the
// Journal uses versioned records with structured scopes, so the run that
// resumes need not be the process that started. Recording an Interrupt response
// under its ID and scope makes repeated instances independently resumable without
// positional matching or delimiter-encoded keys.
//
// Suspension awareness lives in this package's composites. The generic
// combinators in flow and flowx know nothing about a Store, so they treat a
// suspension as any other error and fail fast; compose Steps with [Sequence],
// [Parallel], [Branch], [Loop], [Iteration], [Subgraph], and compiled [Graph]
// when a workflow can suspend.
// A caller-defined composite can make the same distinction with
// [SuspendedOnly], collect waits with [Suspensions], and return them together
// with [JoinSuspensions].
// They also do not own Step identity: apply retry or hedging to the typed node
// before [Leaf], and use [Branch] for mutually exclusive Step alternatives.
// Invoking one named Step more than once in the same scope is
// [ErrDuplicateStep].
//
// This runtime does not own durable process orchestration. There is no
// scheduler, timer, lease, or exactly-once guarantee: a step that suspends after
// a side effect and before recording its result will repeat that effect.
// Resumption is checkpoint-and-restart at step granularity, which fits an
// approval, a callback, or a retry window — not a distributed saga.
//
// # Streaming output
//
// [StreamLeaf] is the named workflow boundary for a typed [StreamNode]. Its
// intermediate values go to the run's [Emitter], while its final result is
// written to the Store and Journal exactly like an ordinary [Leaf]. The producer
// calls a synchronous yield function and must stop when it returns false. A slow
// Emitter therefore applies backpressure; an Emitter error cancels that stream
// and returns through the leaf's normal failure or suspension classification.
// Different leaf invocations may emit concurrently.
//
// A [Chunk] carries the leaf ID, scope, a zero-based invocation index, and
// a run sequence shared with [Event]. Chunks describe execution attempts rather
// than durable delivery: replaying a completed Journal record emits nothing,
// while rerunning an incomplete leaf starts again at index zero and may repeat a
// prefix. Applications that persist chunks should key them with their own run
// identity and workflow-definition version as well as the Chunk fields.
//
// # Observation
//
// Distribution and deterministic replay stay out of scope, but the package
// carries enough on each [Event] to build tracing and durability outside it:
// [Event.Seq] orders a run, [Event.Scope] distinguishes repeated executions of one
// leaf or wait boundary under [Loop], [Iteration], and [Subgraph], and
// [Event.Store] is the serializable snapshot that boundary produced.
// [Store.Changes] narrows that snapshot to just its writes, the delta an audit
// log or external persister records. Attach an Observer through [RunConfig] when
// calling [Run]. Use an
// Emitter for high-volume intermediate values; lifecycle events deliberately
// remain a separate, low-volume channel.
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
// Recoverable domain outcomes are ordinary output data and may feed a routing
// node. Go errors remain terminal. Treating arbitrary errors as routable values
// would require a stable serialization and replay contract and could
// accidentally swallow cancellation, invalid definitions, or suspension.
//
// Errors preserve their causes for errors.Is and errors.As. [RefError],
// [RegistrationError], [GraphError], [SpecError], and [StepError] identify the
// exact boundary that failed.
package workflow
