# Level 10: Dependency-driven graphs and sealed subgraphs

This level combines two engine properties: a Graph starts work when its actual
dependencies are ready, and a Subgraph makes a reusable region explicit without
exposing its internal Store or step IDs. The executable counterparts are
[`example/subgraph_test.go`](../../example/subgraph_test.go) and
[`example/custom_composite_test.go`](../../example/custom_composite_test.go).

## 1. Understand dependency-triggered execution

A compiled Graph retains its edges. It does not translate them into
`Sequence(Parallel(layer), ...)`.

```text
slow --------------------------> slow-result

fetch --> decode --> validate --> fast-result
```

`decode` starts when `fetch` completes, even if `slow` is still running.
`Graph.Concurrency` is one limit across the complete graph. With a limit of two,
any two ready nodes may occupy the slots; zero means unbounded.

The Store contract stays deterministic:

- a node sees the initial Store plus only its direct dependencies' writes;
- dependency results merge in graph declaration order;
- failure returns already completed node writes with the error; and
- suspension blocks descendants and lets unrelated ready work finish.

The last rule is what makes routing safe. Every gate source is a real dependency,
so a target cannot run while its routing decision is suspended.

## 2. Define a reusable body

The body uses local names:

```go
body := workflow.LeafFunc(
	"double",
	workflow.Output("value"),
	func(_ context.Context, input int) (int, error) {
		return input * 2, nil
	},
)
```

Running this body directly would reserve `double` in the caller's Store and
execution scope. Reusing it twice in one scope would correctly report
`ErrDuplicateStep`.

## 3. Seal it with Subgraph

```go
left := workflow.Subgraph(workflow.SubgraphConfig{
	ID:         "left",
	Inputs:     workflow.Inputs{"value": workflow.Output("leftInput")},
	Body:       body,
	BodyOutput: workflow.Output("double"),
})
```

Subgraph performs four engine operations:

1. It copies `leftInput#/output` to `value#/output` in a fresh Store.
2. It runs the body under scope `left`.
3. It reads `double#/output` from the completed inner Store.
4. It writes only that value to `left#/output` in the outer Store.

No inner cell escapes. Another instance may reuse the same body under ID
`right`; its inner leaf is identified by scope `right`, so Journal replay and
events cannot collide with `left`.

Subgraph does not add another Journal record. Completed inner leaves replay,
then the projected result is derived again. If the body suspends, the outer
output remains absent.

When the body consists only of visible built-in steps, validation also proves
that `BodyOutput` exists on every successful path. An opaque caller-defined body
keeps responsibility for that contract and reports a missing output at run time.

## 4. Register the boundary as a Graph node

```go
registry.MustRegisterNode(
	"double-region",
	workflow.SubgraphFactory(body, workflow.Output("double")),
)
registry.MustRegisterSchema("double-region", workflow.NodeSchema{
	Inputs: workflow.Ports{"value": workflow.TypeNumber},
	Output: workflow.TypeNumber,
})
```

`SubgraphFactory` maps the Graph node's input port names to inner seed IDs. The
body remains sealed, while the boundary stays visible to Graph validation:

- an edge into `value` participates in cycle detection;
- an external edge appears in `Graph.Inputs`;
- `NodeSchema` checks the input and output types; and
- the complete visible body is validated as part of the registered node type,
  rather than being misreported as config on each graph node that uses it.

Do not hide additional references inside factory config. A dependency is
statically useful only when it crosses the boundary through `GraphNode.Inputs`.

## 5. Use the JSON Spec form

The nested DSL exposes the same boundary:

```json
{
  "kind": "subgraph",
  "id": "left",
  "inputs": {
    "value": {"nodeID": "leftInput", "path": "/output"}
  },
  "body": {
    "kind": "leaf",
    "id": "double",
    "type": "double",
    "inputs": {"in": {"nodeID": "value", "path": "/output"}}
  },
  "bodyOutput": {"nodeID": "double", "path": "/output"}
}
```

`SpecJSONSchema` checks the wire shape. `Registry.ValidateSpec` also rejects a
`bodyOutput` that registered node schemas prove unavailable, without invoking a
factory; `Registry.CompileSpecJSON` builds the concrete boundaries and closes
the remaining uncertainty for node types that have no schema.

## 6. Preserve identity in a custom repeated composite

Prefer `Loop` or `Iteration` when either matches the control flow. If you build
a different repeated composite, validate its static definition and attach a
structured index before each body invocation:

```go
func (r repeatStep) Validate() error {
	if err := (workflow.ScopeFrame{ID: r.id, Indexed: true}).Validate(); err != nil {
		return err
	}
	if r.count < 0 {
		return flow.ErrInvalidConfig
	}
	return flow.Validate(r.body)
}

for index := range count {
	if err := context.Cause(ctx); err != nil {
		return current, err
	}
	next, err := body.Run(
		workflow.WithScopeIndex(ctx, id, uint64(index)),
		current,
	)
	if contextErr := context.Cause(ctx); contextErr != nil {
		return current, contextErr
	}
	if err != nil {
		return next, err
	}
	current = next
}
```

Call the custom composite through `workflow.Run`, even with a zero `RunConfig`.
This gives every child one shared run identity and calls the optional validation
contract before work. Built-in workflow composites honor the same contract on a
caller-defined child. The custom composite's own `Run` method should still call
`Validate`, so a direct call or an opaque caller-defined parent remains safe.
Call the child Step directly: a nested package-level `workflow.Run` starts an
independent root scope. Check parent cancellation before admission and after the
child returns; if it races with completion, preserve the Store from before that
invocation. `flow.RunChild` makes those two checks for a plain `Node` and is
what a composite in `flow` or `flowx` uses, but it rolls a cancelled child back
to a zero output — for a Step that is an empty Store, discarding what already
completed — which is why the loop above is written out here. `WithScopeIndex`
isolates Journal, event, chunk, and suspension identity; it does not isolate
Store cells. Wrap the body in `Subgraph` when each
invocation also needs a sealed Store.

## Common mistakes

- Reusing a body directly and then adding ad-hoc `WithScope` calls.
- Formatting an index into a scope ID. Use `WithScopeIndex`; persisted identity
  must not depend on display text.
- Running the body on the outer Store, which leaks inner cells.
- Recording a second Journal cell for the projected output.
- Treating an inner node ID as an outer Graph dependency.
- Expecting Subgraph to provide multiple independent outputs. Its boundary is
  intentionally one projected value.

[Previous: Conditional graphs and diagrams](./09-conditional-graphs-and-diagrams.md) ·
[Tutorial index](./README.md)
