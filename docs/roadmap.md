# Roadmap

Planned work, in priority order, with the reasoning behind each item. This is a
maintainer-facing document: it records what is missing, why it matters, and which
decisions must be made before v1. Compatibility history stays in the
[changelog](../CHANGELOG.md).

Last reviewed: 2026-07-29.

## Where the library stands

The engine core is complete and verified. Cycles are rejected at build time by
Kahn layering, the Store is persistent with a defined merge rule, resumption is
keyed by a structured scope trie, suspension is a third outcome rather than a
failure, streaming is a first-class leaf shape, and step identity is enforced
both statically and at run time. All four packages hold full statement coverage
and the JSON boundaries are fuzzed.

What remains falls into four groups: one missing concept that blocks a whole
class of graphs, one missing capability in the dynamic layer, one open design
decision, and a set of ergonomic gaps.

## A constraint that shapes every item below

The module currently targets Go 1.25. Generic methods are part of the draft
[Go 1.27 release notes](https://go.dev/doc/go1.27), with that release expected
in August 2026, but the roadmap must not depend on an unreleased toolchain.
APIs that capture caller types therefore remain package-level generic functions.

Go 1.27 should trigger an API review, not an automatic rewrite. A method such as
`registry.RegisterNode("http", build)` may benefit because its type parameters
can be inferred from `build`. `store.Get[T](ref)` gains only namespacing because
`T` appears solely in the result and still has to be supplied explicitly. The
existing function forms remain small, composable, and compatible.

## Landed during this review

The roadmap identified four contained improvements that did not require an open
semantic decision:

- `LeafFunc` and `StreamLeafFunc` lift ordinary typed functions with one
  referenced input.
- `FirstOf` reads the first available reference and stops on a real conversion
  error.
- `Graph.Concurrency` bounds each topological layer; zero remains unbounded.
- `Spec.Input`, `Spec.BodyOutput`, and `NodeSpec.Input` are `Ref` values with
  `omitzero`, so `Input: workflow.Output("source")` works inline without changing
  the JSON wire format for omitted inputs.

## P0 — Conditional edges and bypass semantics

The flat `Graph` cannot express a mutually exclusive branch. Its node fields are
`id`, `type`, `input`, `inputs`, `config`, and `dependsOn`; nothing carries a
condition. Compilation produces `Sequence(Parallel(layer)...)`, so every node in
a layer runs. A diamond whose two arms are meant to be exclusive executes both.
There is also no skip state: a node whose upstream did not run fails its bind
with `ErrNotFound` rather than being bypassed.

`Branch` in the nested `Spec` covers conditional control flow, but only as a
subtree. A graph produced by a visual editor routes on edges and re-converges
across branches, which a subtree cannot represent without duplicating nodes.

### The condition belongs to the source node, not the edge

This is what keeps the design small. In editor-produced graphs the routing node
evaluates its own condition and reports which outlet fired; the edge carries only
a label that is compared against it. No expression is attached to an edge, and no
new expression machinery is needed — `expr.Switch` already compiles ordered
boolean cases into the name of the case that matched, which is exactly the shape
a routing node's output needs.

### The smallest coherent contract

**Routing output.** In the first version, a routing node's ordinary `output` is
the selected outlet name. `NodeSchema` gains `Outlets []string`, declared next
to `Inputs` and `Output`, so editors can render branches and validation can
reject unknown outlets. Outlet names must be non-empty and unique; an empty list
means the node is not declared as a router.

Do not add a separate `#/outlet` cell or `RouteFactory` yet. A leaf currently
publishes and journals one final output. Writing a second conventional cell only
on a fresh run would make a completed replay restore different state. If a real
node later needs both a payload and an independently selected outlet, design a
replay-aware multi-output publication contract first.

An ordinary `Factory` returning `string` is sufficient for the initial routing
node, and `expr.Switch` already produces that shape.

**Gate.** A target node declares which routing output it accepts. Edges stay
implicit through `inputs` and `dependsOn`; no explicit edge objects are
introduced:

```json
{
  "id": "merge", "type": "summarize",
  "inputs": { "in": { "nodeID": "yes", "path": "/output" } },
  "when": [
    { "nodeID": "route", "outlet": "true" },
    { "nodeID": "route", "outlet": "false" }
  ],
  "trigger": "any"
}
```

**Trigger.** Two rules are enough for editor-produced graphs: branch targets use
the default, and re-convergence points use `any`.

```go
type Trigger string

const (
    TriggerAll Trigger = ""    // every gate must be satisfied (default)
    TriggerAny Trigger = "any" // one satisfied gate is enough
)
```

Other engines ship a dozen trigger rules. Adding more is only justified once a
real graph needs one.

**Bypassed.** A node whose gate is not satisfied does not run, publishes no
output, and emits `EventBypassed`. It does not write a new reserved Store cell:
the observer is the audit surface, and gates are recomputed from journaled
routing outputs on resume.

Bypass is explicit rather than inferred from missing inputs. Every node in a
conditional region must carry the route gate; an editor may generate the
repeated declaration. An ungated node that reads a bypassed node's absent output
still fails with `ErrNotFound`. This avoids treating a genuine missing input as
control flow and avoids guessing whether a multi-input node is optional.

If a gate source has no output because it was itself bypassed, that gate is
unsatisfied. Gate sources must name graph nodes, never external Store values.

A merge point reached through `any` may find one arm absent, so it needs a
tolerant bind:

```go
// FirstOf reads the first reference that resolves, so a merge point after
// mutually exclusive branches can read whichever arm ran.
func FirstOf[I any](refs ...Ref) BindFunc[I]
```

### The layered model is enough

A runtime edge interpreter is not required. Topological layering already
guarantees that when layer N runs, every dependency's state is in the Store, so a
gate is a Store read. Each compiled leaf is wrapped:

```go
leaf = gated(gateSpec{id: nodeID, when: node.When, trigger: node.Trigger, step: leaf})
```

No cross-goroutine bookkeeping, no inferred merge points, no manual counters.

### Invariants to preserve

**Gates must not be journaled.** A gate reads only a routing leaf's ordinary
output, which completed-leaf replay restores, so a resumed run recomputes the
same decision. This differs from `Branch`, whose resolver may not be a pure
function of the Store and therefore records its choice. It also avoids a
conflict: a journaling gate would claim the same `(scope, id)` identity as the
leaf it wraps. Lock this into a test.

**Gate sources must join the dependency graph.** `connectNodes` has to treat each
`when[].nodeID` as a dependency, or a gate could be evaluated before its source
runs and conditional edges would escape cycle detection.

**The gate wrapper must preserve static definition traversal.** `gated` must
implement the package-private `definitionStep` contract. When the wrapped step
implements `definitionStep`, `workflowDefinition` forwards its definition
unchanged so duplicate IDs and nesting depth remain visible to
`definitionValidator`. For an opaque step, the wrapper reports the graph node ID
as `definitionNamed`; runtime `claim` remains the final defense for identities
hidden inside caller-defined steps. A decorator must not move duplicate-ID
detection from construction time to execution time.

**Outlet validation is intentionally stricter than ordinary port
validation.** Every `when[].nodeID` must name a graph node whose type has a
registered `NodeSchema` with a non-empty `Outlets` declaration, and the requested
outlet must be present in that declaration. An absent schema or empty `Outlets`
is a compile-time `GraphError` on the target node's `when` field, not an
unchecked dynamic comparison. The existing permissive behavior for undeclared
input ports is useful for simple nodes; applying it to routing would let an
arbitrary output value silently control execution.

**Suspension wins before gate evaluation.** A suspended layer ends the enclosing
sequence before any later layer runs. A target must therefore never interpret an
unfinished source as a bypass.

### Naming

`EventSkipped` already means "replayed from the Journal", which is a different
fact from "not selected". A conditional bypass needs its own kind; `EventBypassed`
keeps consumer switches unambiguous.

### Tests that must exist

Exclusive arms where only the selected one runs; explicit gates across a
multi-node conditional region; re-convergence through `any` plus `FirstOf`; an
ungated missing input remaining an error; gate sources ordered before targets;
cycle detection covering conditional edges; resume recomputing gates
identically; a suspended source not being mistaken for a bypass; and unknown,
empty, duplicate, undeclared-schema, and non-routing-source outlets rejected at
compile time. A duplicate ID hidden behind `gated` must still be rejected before
execution, and the wrapper must not hide nesting-depth violations.

## P1 — The dynamic layer cannot declare policy

`Graph.Concurrency` now covers the graph-level resource limit. `Retry` and
`Timeout` still do not have a coherent dynamic-layer contract.

The project's position is that retry, timeout, and tracing are policies rather
than control flow, implemented as decorators. That holds for a pipeline written
in Go. It does not hold for the dynamic layer, whose entire purpose is a
definition that arrives as JSON: there is no way to express "retry this call
three times".

Leaving it to each node type reproduces the problem the package documentation
already names for inputs — a node that reads references out of its own config is
invisible to the layer above. A retry buried in one node type's config is
invisible in the same way: an editor cannot render it, validation cannot check
it, and every node author reimplements it differently.

### Retry conflicts with step identity and factory abstraction

Retry means running the same computation twice, and `claim` rejects a second
invocation of one `(scope, id)` with `ErrDuplicateStep`. Because `LeafFactory`
returns a `Step`, the engine cannot reach inside to wrap the typed node.

The earlier proposal to add policy to `LeafSpec` is not ready to implement:

- `Factory` and `BindFactory` can wrap their typed node, but an arbitrary
  `LeafFactory` returns an opaque `Step` and can silently ignore the policy.
- Workflow suspension must never be retried as a failure. A decorator in the
  root `flow` package cannot import `workflow` to recognize `ErrSuspended`.
- A timeout around a streaming node must define what happens while its emitter
  is applying backpressure and how the cancellation cause is reported.
- JSON still needs stable duration and backoff representations, validation
  limits, and an explicit retryable-error classifier.

Do not relax `claim`; it prevents silent replay corruption. Before policy fields
are exported, define one contract that every registered factory must either
apply or explicitly reject. A capability-bearing registration value is a
possible direction, but it should be justified independently rather than
smuggled in as retry plumbing.

### Minimum useful set

Graph-level `concurrency` is complete. Node-level `retry` and `timeout` remain
the minimum useful policy set once the contract above is closed. Circuit
breaking and hedging stay with custom factories.

## P2 — Failure routing

Editor-produced graphs also route on failure, turning try/catch into an edge. The
library terminates a branch on error instead.

This needs its own design pass, because it collides with two established
semantics: the first failure cancels siblings, and suspension is already a third
outcome. Adding skip makes four states. A reserved outlet such as `error` is the
obvious shape, but which failures are routable must be declared per node type in
`NodeSchema` rather than defaulted globally — a global default would silently
swallow real errors.

Evaluate after P0 and P1 land.

## P3 — Decide whether both DSLs survive v1

`Graph` and `Spec` must stay semantically consistent with each other, with their
JSON Schemas, and with the programmatic API. The paired `allowedFields` and
`populatedFields` tables in `spec_validate.go` are the kind of duplication that
drifts.

The two forms are not currently supersets of each other. Conditional Graph edges
would express cross-branch re-convergence that `Spec` cannot. Conversely, `Spec`
has `Loop` and `Iteration`, which a flat Graph cannot represent as ordinary
nodes. Making Graph the only serialized form would therefore remove real
capability unless Graph first gains composite/subgraph nodes.

The default direction is to keep both with distinct jobs: Graph for
editor-produced flat DAGs, Spec for structured control flow. Reduce drift by
sharing internal validation and compilation concepts, not by pretending the
wire formats are equivalent. Reconsider consolidation only after a concrete
composite-node model exists.

## Ergonomics

The useful low-magic improvements are now implemented. Two reflection-based
ideas remain intentionally unapproved.

### Concise function lifting (complete)

From `example/workflow_test.go`:

```go
clean := workflow.Leaf(
    "clean",
    workflow.From[string](workflow.Output("input")),
    flow.NodeFunc[string, string](func(_ context.Context, in string) (string, error) {
        return strings.TrimSpace(in), nil
    }),
)
```

The caller writes the types three times. A package-level function infers both
from the function literal:

```go
// LeafFunc lifts an ordinary function into a Step that binds its input from ref.
func LeafFunc[I, O any](id string, ref Ref, fn func(context.Context, I) (O, error)) Step
```

```go
clean := workflow.LeafFunc("clean", workflow.Output("input"),
    func(_ context.Context, in string) (string, error) {
        return strings.TrimSpace(in), nil
    })
```

Inference works with no type arguments at the call site, and
`StreamLeafFunc` provides the streaming counterpart. Both are now implemented.

### A multi-port node costs twenty-two lines

From `example/dag_test.go`, a node that adds two numbers spends most of its body
restating port names: fetch each ref, check each `ok`, build the error, then read
each value again inside the bind.

Field names could carry the port names:

```go
// PortsFactory binds a node whose input struct fields name its input ports.
func PortsFactory[C, I, O any](build func(C) (flow.Node[I, O], error)) LeafFactory
```

```go
type pair struct {
    Left  int `json:"left"`
    Right int `json:"right"`
}

sum := workflow.PortsFactory(func(_ struct{}) (flow.Node[pair, int], error) { ... })
```

Do not implement this shape yet. It introduces a second reflection contract:
embedded fields, `json` tags, ignored and unexported fields, duplicate promoted
names, optional ports, and custom unmarshalling all need exact rules. Reusing
`Get` for conversion does not answer how ports are discovered or which are
required. `BindFactory` is explicit and remains the source of truth until a
smaller contract is demonstrated by several real nodes.

### Derive the node schema from the same types

The same example spends twelve lines registering schemas that restate what the Go
types already say: `OnePort(TypeNumber)` and `Output: TypeNumber` for a
`Node[int, int]`. A `ValueType` sometimes follows from a Go type, but not
reliably enough to make an inferred schema authoritative. `json.Marshaler`
implementations, aliases, pointers, and domain constraints can all serialize
differently from their underlying Go kind.

With the current Go version, `LeafFactory` erases `I` and `O`, so any opt-in
derivation would have to happen while the generic types are still visible:

```go
// InferSchema may assist tooling, but does not register anything.
func InferSchema[I, O any]() NodeSchema
```

Keep `NodeSchema` explicit. An opt-in helper may later serve tooling, but it must
never silently register or override a schema. A caller-supplied `ConfigSchema`
and domain constraints remain authoritative.

### Inline references (complete)

`NodeSpec.Input`, `Spec.Input`, and `Spec.BodyOutput` are now plain `Ref` values
with `json:",omitzero"`. A zero `Ref` is already invalid and means “unset”; valid
references can be written inline. This pre-v1 breaking change is complete.

### Sugar for P0

Conditional edges should land with only the sugar that removes accidental
verbosity: `When(nodeID, outlet)` constructs a `Gate`, and `FirstOf` handles
merge inputs. `FirstOf` is complete. Do not add `Route(id, spec)` until repeated
real call sites reveal what it would hide; an ordinary string-output node is
already the honest routing primitive.

## Out of scope

These remain excluded by design, and the reasoning is already recorded in the
package documentation:

- Distributed scheduling, durable timers, leases, and exactly-once effects.
- Deterministic instruction-level replay and workflow-definition migration.
- A node library. Model calls, HTTP, code execution, and retrieval are product
  assets; the library stays an engine plus a registry.
- Engine-level stream middleware. Wrapping `yield` in a `StreamNode` composes
  the same pipeline more explicitly.

## Sequencing

| Order | Item | Notes |
|---|---|---|
| 1 | P0 conditional edges and bypass | Implement the output-based gate contract above; do not add a second publication channel |
| 2 | P1 node policy design | Specify factory participation, suspension, streaming timeout, and JSON representation before code |
| 3 | P3 DSL roles | Document distinct Graph and Spec roles; share internals where it removes drift |
| 4 | P2 failure routing | Reassess after bypass and policy outcomes are stable |

The contained ergonomics work and graph concurrency slice are complete.
`PortsFactory`, automatic schema derivation, and a separate outlet cell are not
approved implementation items.
