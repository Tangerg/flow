# Level 10: Dependency-driven graphs and sealed subgraphs

This level combines two engine properties: a Graph starts work when its actual
dependencies are ready, and a Subgraph makes a reusable region explicit without
exposing its internal Store or step IDs. The executable counterpart is
[`example/subgraph_test.go`](../../example/subgraph_test.go).

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

- a node sees the initial Store plus only its declared dependencies;
- dependency results merge in graph declaration order;
- completion order cannot change a same-cell conflict;
- failure returns already completed node writes with the error; and
- suspension blocks descendants, retains the waiting composite's returned
  writes, and lets unrelated ready work finish.

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
`right`; its inner leaf is identified by path `right`, so Journal replay and
events cannot collide with `left`.

Subgraph does not add another Journal record. Completed inner leaves replay,
then the projected result is derived again. If the body suspends, the outer
output remains absent.

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
- the body validates its own inner Graph or Spec.

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
    "input": {"nodeID": "value", "path": "/output"}
  },
  "bodyOutput": {"nodeID": "double", "path": "/output"}
}
```

`SpecJSONSchema` checks the shape, and `Registry.CompileSpecJSON` validates and
builds both the boundary and its body.

## Common mistakes

- Reusing a body directly and then adding ad-hoc `WithScope` calls.
- Running the body on the outer Store, which leaks inner cells.
- Recording a second Journal cell for the projected output.
- Treating an inner node ID as an outer Graph dependency.
- Expecting Subgraph to provide multiple independent outputs. Its boundary is
  intentionally one projected value.

[Previous: Conditional graphs and diagrams](./09-conditional-graphs-and-diagrams.md) ·
[Tutorial index](./README.md)
