# Level 9: Conditional graphs and diagrams

A flat DAG sometimes needs mutually exclusive arms and a merge point. `Graph`
models that control flow explicitly: a routing node publishes an outlet name,
and a target declares which outlet permits it to run. The complete example is
[`example/routing_test.go`](../../example/routing_test.go).

## 1. Declare the routing contract

A routing node uses its ordinary string output as the selected outlet. Declare
the complete set on its node type:

```go
registry.
	MustRegisterNode("route", routeFactory).
	MustRegisterSchema("route", workflow.NodeSchema{
		Inputs:  workflow.OnePort(workflow.TypeNumber),
		Output:  workflow.TypeString,
		Outlets: []string{"approve", "review"},
	})
```

Compilation rejects empty or duplicate outlets, a non-string routing output,
and a gate that names an undeclared outlet. A gate source must have a registered
schema with at least one outlet; routing is intentionally stricter than ordinary
untyped ports.

There is no second hidden routing cell. A completed leaf replay restores its
ordinary output, so the same decision remains available after resumption.

## 2. Gate mutually exclusive arms

```go
graph := workflow.Graph{Nodes: []workflow.GraphNode{
	{
		ID: "route", Type: "route",
		Inputs: workflow.DefaultInput(workflow.Output("score")),
	},
	{
		ID: "approve", Type: "decision",
		Inputs: workflow.DefaultInput(workflow.Output("score")),
		When: []workflow.Gate{
			workflow.When("route", "approve"),
		},
	},
	{
		ID: "review", Type: "decision",
		Inputs: workflow.DefaultInput(workflow.Output("score")),
		When: []workflow.Gate{
			workflow.When("route", "review"),
		},
	},
}}
```

`When` is also a dependency: the compiler schedules `route` before either arm
and includes conditional edges in cycle detection. The selected arm runs. The
other publishes no output and emits `EventBypassed`.

The same definition travels in JSON without a separate edge format:

```json
{
  "id": "approve",
  "type": "decision",
  "inputs": {"in": {"nodeID": "score", "path": "/output"}},
  "when": [{"nodeID": "route", "outlet": "approve"}]
}
```

`GraphJSONSchema` includes gates and trigger rules, while
`Registry.ValidateGraph` adds registered-outlet semantics.

Bypass is never inferred from a missing input. An ungated node that reads an
absent value still receives `ErrNotFound`; this keeps data errors distinct from
control flow. If a routing node was itself bypassed, gates that depend on it are
unsatisfied, allowing bypass to propagate through a conditional region.

## 3. Merge either arm

The zero `Trigger` requires every gate. Use `TriggerAny` for a merge reached
through any one mutually exclusive arm:

```go
{
	ID: "result", Type: "merge",
	Inputs: workflow.Inputs{
		"approve": workflow.Output("approve"),
		"review":  workflow.Output("review"),
	},
	When: []workflow.Gate{
		workflow.When("route", "approve"),
		workflow.When("route", "review"),
	},
	Trigger: workflow.TriggerAny,
}
```

Only one input exists at run time, so the merge binder reads the first available
one:

```go
return workflow.FirstOf[string](approveRef, reviewRef), nil
```

`FirstOf` skips only `ErrNotFound`. If an existing value has the wrong type, it
returns that error instead of silently falling through to another arm.

## 4. Adapt Store-based rules with `Route`

A typed node that already returns a string is a routing node without extra
machinery. When the rule is a `workflow.Resolver`, adapt it directly:

```go
resolve, err := expr.Switch(expr.SwitchSpec{
	Cases: []expr.Case{
		{When: "score.output >= 80", Then: "approve"},
	},
	Fallback: "review",
})
if err != nil {
	return err
}

route := workflow.Route("route", resolve)
```

`Route` is an ordinary leaf: its output, events, suspension behavior, and
Journal replay follow the same contract as `Leaf`.

Keep actual Go errors terminal. A recoverable business outcome such as
“declined” or “needs review” belongs in ordinary output data and can feed a
routing node. Turning arbitrary `error` values into graph edges would lose
stable serialization and could accidentally swallow cancellation, invalid
definitions, or suspension.

## 5. Render the definition

The optional `workflow/diagram` package derives diagnostics without becoming
part of execution:

```go
fmt.Print(diagram.ASCII(graph))
fmt.Print(diagram.Mermaid(graph))
```

Both formats are deterministic. Mermaid uses generated safe node IDs and escapes
labels, so application IDs remain display data. Rendering does not validate a
Graph; call `registry.ValidateGraph(graph)` before presenting it as executable.

## 6. Reuse compiled graphs safely

A compiled Graph owns the Store cells named by its internal node IDs. At the
start of every invocation it removes those cells and reconstructs them from the
current execution or Journal replay. Reusing a previous output Store with new
external inputs therefore cannot make an old, now-bypassed branch appear
selected.

This rule is local to the compiled Graph. External seed values keep their cells,
and a Graph's internal node IDs must not be used as external parameter
names.

## Common mistakes

- Omitting `Outlets` on a node used as a gate source.
- Using `TriggerAny` without any gates.
- Expecting a missing input to imply bypass.
- Binding a merge with `From` when only one mutually exclusive input exists.
- Treating Mermaid rendering as validation.
- Routing infrastructure errors instead of returning them.

[Previous: Streaming output](./08-streaming-output.md) ·
[Next: Dependency-driven graphs and sealed subgraphs](./10-dependency-graphs-and-subgraphs.md)
