// Package workflow is the dynamic layer built on package flow's primitives.
//
// Where flow composes statically typed nodes at compile time, workflow threads a
// heterogeneous [Store] through nodes addressed by ID, so
// graphs can be assembled at runtime (from config or a visual editor).
//
// Store has immutable snapshot semantics: every write returns a new value that
// shares untouched cells with the original. Values are held as-is and must be
// treated as immutable after insertion.
// Store snapshots can be serialized with encoding/json; decoding replaces a
// Store atomically.
//
// A workflow node is a [Step] — a flow.Node[Store, Store] that reads its inputs
// from the Store and returns a Store extended with its output. Typed business
// logic is bridged in with [Leaf]: a [Binder] prepares its input and a
// flow.Node computes its output. [Ref.Bind] and [FirstOf] retain reference
// definitions for validation, while [BinderFunc] adapts custom binding code. A
// custom Binder can expose immutable definition checks with the same optional
// Validate() error convention used by composite Nodes. A
// [StreamFunc] remains an ordinary typed flow.Node while adding a run-scoped
// intermediate-output side channel;
// [Factory] adapts the common case of a typed node constructor with JSON config,
// and [BindFactory] the case of a node reading several inputs. Composites
// ([Sequence], [Branch], [Loop], [Parallel], [Iteration], [Subgraph]) remain
// ordinary Steps. They honor a caller-defined Step's optional Validate() error
// method before work while treating its internal structure as opaque.
// Leaf validates the complete Node definition visible through [flow.Validate]
// before Journal replay; an opaque caller-defined Node owns its own contract.
// [Describe] returns their structural tree using the same typed [Kind]
// vocabulary as [Spec], so introspection and serialized definitions do not
// invent separate string protocols.
//
// Programmatic constructors snapshot map and slice structure that becomes part
// of a definition. Changing a source case map, branch slice, input map, or
// reference slice after construction does not reconfigure the resulting Step.
// Step, Binder, Node, Resolver, and Condition values are retained as behavior,
// not copied; they must keep their own definition state immutable and satisfy
// the concurrency contract of the composite that invokes them. Application
// values crossing Store, Journal, suspension, event, or streaming boundaries
// follow the separate borrowed-value contract documented by those types.
//
// Names that participate in workflow identity or serialized definitions must
// be valid UTF-8. Built-in definitions reject invalid step IDs, references,
// ports, routing names, and Registry names before application work begins.
// Store itself remains an in-memory value container and can hold arbitrary byte
// strings as names; [Store.MarshalJSON] rejects names JSON cannot preserve.
//
// # Named input ports
//
// A node names each value it reads: [Inputs] wires port names to references, and
// a [NodeSchema] declares the ports a node type expects along with their types.
// A unary node uses [OneInput] to wire [DefaultPort]; it is not a second,
// unnamed edge shape. Naming inputs is what keeps the data flow visible to the
// layer above. The flat [Graph] derives its execution order from the wired
// ports, and a registered [NodeSchema] lets [Registry.ValidateGraph] report both
// incomplete wiring and incompatible edges before anything runs. A node that
// instead reads references out of its own config is invisible to both.
// [Registry.NodeTypes] and [Registry.NodeSchema] expose the registered vocabulary
// for an editor to render.
//
// # Conditional graphs
//
// A flat Graph routes through ordinary node output. A routing node declares its
// possible JSON-string [NodeSchema.Outlets], and a target's [GraphNode.When]
// gates it on one or more selected outlets. Comparing the JSON representation
// keeps selection identical before and after Journal persistence. Every gate
// source is a dependency, so all sources complete before the target is
// considered. The zero [Trigger] then requires every gate; [TriggerAny]
// requires at least one. An unsatisfied target does not run, writes no output,
// and emits [EventBypassed]. Bypass is explicit: a missing value on an ungated
// node remains an error.
//
// [Resolver] is an alias for flow.Node[Store, string], so branch decisions use
// the same typed execution protocol as every other computation. [Route] gives
// one such node a journaled routing-leaf identity. [FirstOf] binds the first
// available input at a mutually exclusive merge. Route decisions use the
// routing leaf's ordinary output, so Journal replay restores them without a
// second hidden state channel.
//
// Graph execution is dependency-driven rather than divided into topological
// barriers. A node starts as soon as every declared dependency completes,
// subject to the graph-wide concurrency bound. It receives only the initial
// Store and the writes of its direct dependencies, applied in declaration
// order.
// Suspension blocks descendants but does not stop unrelated ready work;
// failure cancels running siblings. In either case, the returned Store preserves
// nodes that had already completed, never partial writes from an incomplete
// node boundary.
//
// A compiled Graph owns cells whose node IDs belong to that Graph. Each
// invocation removes those internal cells from its input Store, then rebuilds
// them from the current execution or Journal replay. Reusing a prior result with
// new external inputs therefore cannot revive an output from a now-bypassed arm.
// Namespace cleanup remains composable when the Graph runs inside [Parallel],
// and persists as ordinary absence when the returned Store is serialized.
// Registry factories must return one named, Store-sealed node boundary. Use
// [Subgraph] when a Graph node contains a composite region. Compilation checks
// the concrete boundary as well as optional schema metadata, so an internal data
// edge cannot target a factory node that produces no output.
//
// # Sealed subgraphs
//
// [Subgraph] turns any Step into an isolated composite. Declared inputs are
// copied from the outer Store into a fresh inner Store, the body runs under a
// scope derived from the subgraph ID, and one [SubgraphConfig.BodyOutput] is
// projected back to [Output] of that ID. Inner cells never leak out. Built-in
// body definitions remain visible to recursive static validation; a
// caller-defined opaque body validates its own state when invoked. Journal
// boundaries stay inside the body, and replay derives the projected output
// again rather than recording a second hidden checkpoint.
//
// [SubgraphFactory] installs the same boundary as a registered Graph node.
// Because its inputs still come from [GraphNode.Inputs], the enclosing Graph can
// detect cycles, report external inputs, and check registered port types without
// inspecting or exposing the subgraph body. [Spec] and its JSON Schema also
// provide the "subgraph" kind for structured definitions.
//
// [SpecJSONSchema] and [GraphJSONSchema] expose the Draft 2020-12 schemas for
// the two JSON DSL shapes. [ValidateSpecJSON] and [ValidateGraphJSON] perform
// portable structural checks. Direct encoding/json decoding into Spec or Graph
// uses the same strict, atomic structural boundary. Encoding rejects definition
// text or raw config that JSON would rewrite, as well as cyclic or excessively
// deep Spec bodies; it does not require Registry capabilities to exist. A
// Registry adds node, config, type, and graph semantics when it validates or
// compiles the decoded workflow. [Graph.Inputs] and [Graph.MissingInputs] report
// potential external reads. They support editor tooling and preflight for
// unconditional nodes; routing may bypass a conditional node whose potential
// input is absent.
//
// # Configuring a run
//
// One [RunConfig] carries everything a single run needs: what watches it, what
// receives streaming output, and what lets it resume. [Run] establishes
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
// independent signal sequence, receivers, and Journal configuration. A Step
// built by this package may be called directly when no configuration is needed;
// its child-execution boundary installs zero-config identity bookkeeping. Use
// Run at a caller-defined composite's top-level boundary even with a zero
// RunConfig, so every child invocation shares one identity scope.
// A nested Run is independent and starts at a new root scope. A caller-defined
// composite that wants a child to remain in the same execution must call the
// child Step directly, passing a context derived with [WithScope] or
// [WithScopeIndex] when it introduces a namespace.
//
// [Run] establishes execution bookkeeping; it does not clear its input Store.
// Store has no hidden distinction between seeds and results. Assemble a fresh
// seed Store for a new logical run, or deliberately reuse a snapshot when its
// cells belong in the next input. A compiled Graph is stricter because its
// declared node IDs give it a complete set of internal cells to rebuild.
//
// # Store results on non-success
//
// A Step may return a useful Store together with an error, just as a read may
// return bytes together with a terminal error. Sequence and Branch are
// transparent: they preserve the Store returned by the child that failed or
// suspended, so adding or removing nested grouping does not change visible
// progress. Caller-defined Steps and composites should make the same choice
// deliberately.
//
// Other composites introduce explicit commit boundaries. Loop rolls an
// ordinary failure back to the previous iteration but preserves the current
// Store on suspension. Parallel and Iteration do not publish an incomplete
// collection after ordinary failure. Subgraph never projects a failed or
// suspended body. A compiled Graph retains accepted completed nodes but never a
// failing node's partial writes. Each constructor documents its exact boundary.
// Parent cancellation is stricter throughout: a built-in composite that
// observes cancellation when a child returns discards that child's unaccepted
// result and returns the parent cause.
//
// # Suspension and resumption
//
// A step can stop a run without failing it. [Suspend], [Await], and [Interrupt]
// report that the work cannot proceed yet because a person must decide or an
// external job must finish. The run ends with an error matching [ErrSuspended].
// [Suspensions] returns every wait, including its structured application value
// and its scope-aware [Suspension.Key]. Suspension is a third outcome, not a kind
// of failure, so [Parallel], [Loop], [Iteration], and [Graph] preserve work that
// completed before or independently of the wait rather than cancelling or
// discarding it. It is exclusively a run-time outcome: definition validation
// and construction cannot wait, so a validator or [NodeFactory] that returns
// ErrSuspended is rejected as [flow.ErrInvalidConfig].
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
// they made. A resolver that is not a pure function of the Store cannot send a
// resumed run down the other branch. Both a [Store] and a Journal serialize; the
// Journal uses versioned records with structured scopes, so the run that
// resumes need not be the process that started. Its decoder accepts only the
// current wire version; applications migrate or discard older checkpoints
// before decoding them. Recording an Interrupt response under its ID and scope
// makes repeated instances independently resumable without positional matching
// or delimiter-encoded keys.
//
// Suspension awareness lives in this package's composites. The generic
// combinators in flow and flowx know nothing about a Store, so they apply their
// ordinary error semantics: a fail-fast combinator may cancel siblings, while
// an error-recovery or first-success combinator may hide a suspension. Compose
// Steps with [Sequence], [Parallel], [Branch], [Loop], [Iteration], [Subgraph],
// and compiled [Graph] when a workflow can suspend.
// A caller-defined composite can make the same distinction with
// [SuspendedOnly], collect waits with [Suspensions], and return them together
// with [JoinSuspensions].
// Caller-defined repeated composites attach a structured indexed identity with
// [WithScopeIndex]; composing the index into an ordinary scope string would make
// persisted identity depend on display formatting.
// They also do not own Step identity: apply retry or hedging to the typed node
// before [Leaf], and use [Branch] for mutually exclusive Step alternatives.
// Invoking one named Step more than once in the same scope is
// [ErrDuplicateStep].
//
// This runtime does not own durable process orchestration. There is no
// scheduler, timer, lease, or exactly-once guarantee: a step that suspends after
// a side effect and before recording its result will repeat that effect.
// Resumption is checkpoint-and-restart at step granularity, which fits an
// approval, a callback, or a retry window, but not a distributed saga.
//
// # Streaming output
//
// [StreamFunc] adapts a typed producer into an ordinary flow.Node, so it composes
// directly with flow.Then, flow.Map, flow.Race, and other typed helpers. [Leaf]
// remains the only named workflow boundary: intermediate values emitted anywhere
// inside its composed Node go to the run's [Emitter], while the final result is
// written to the Store and Journal normally. The producer must stop when yield
// returns false. Calls are serialized within one leaf invocation, so an Emitter
// must not wait for another chunk from that invocation. A slow Emitter applies
// backpressure; an Emitter error cancels that leaf and returns through its
// normal failure or suspension classification. That consumer failure remains
// the cause even if a producer ignores the stopped stream and returns another
// error. Different leaf invocations may emit concurrently.
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
// Distribution and deterministic replay stay out of scope, but each [Event]
// carries enough to trace execution and persist its produced Store snapshot:
// [Event.Seq] orders a run, [Event.Scope] distinguishes repeated executions of one
// leaf or wait boundary under [Loop], [Iteration], and [Subgraph], and
// [Event.Store] is the serializable snapshot that boundary produced.
// [Store.Changes] narrows that snapshot to its write set for auditing; it does
// not represent deletions, so persist Event.Store itself when exact snapshot
// synchronization matters. Attach an Observer through [RunConfig] when calling
// [Run]. Use an Emitter for high-volume intermediate values; lifecycle events
// deliberately remain a separate, low-volume channel.
//
// # Behavior by name
//
// A serialized workflow cannot carry closures, so it names its node types,
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
// Errors preserve caller errors and stable package causes for [errors.Is] and
// [errors.As], except that a definition-time validator or NodeFactory error
// matching [ErrSuspended] is deliberately normalized to
// [flow.ErrInvalidConfig]: construction cannot expose a resumable run-time
// outcome. Replaceable backend types such as the JSON Schema validator remain
// implementation details. [RefError],
// [RegistrationError], [GraphError], [SpecError], and [StepError] identify the
// exact boundary that failed. A runtime StepError carries the same structured
// execution scope used by events, chunks, suspensions, and Journal keys; a
// definition error has no runtime scope.
package workflow
