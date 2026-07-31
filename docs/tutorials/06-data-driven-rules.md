# Level 6: Data-driven rules

Nodes and edges can now come from JSON, but a branch implemented as a Go
closure still requires a new binary for every threshold change. The optional
`workflow/expr` package compiles a small, restricted expression into an
ordinary `Condition` or `Resolver`. The complete example is
[`example/rules_test.go`](../../example/rules_test.go).

## 1. Select a branch with ordered rules

```go
resolve, err := expr.Switch(expr.SwitchSpec{
	Cases: []expr.Case{
		{When: "score.output < 60", Then: "review"},
		{When: "score.output >= 90", Then: "accept"},
	},
	Fallback: "revise",
})
if err != nil {
	return err
}
```

`Switch` evaluates cases in order and returns the first matching target. It
returns `Fallback` when no case matches. Order is part of the configuration
semantics.

Pass the resolver to an ordinary `workflow.Branch`:

```go
decision := func(id, message string) workflow.Step {
	return workflow.LeafFunc(
		id,
		workflow.Output("score"),
		func(_ context.Context, _ int) (string, error) {
			return message, nil
		},
	)
}

route := workflow.Branch("route", resolve, map[string]workflow.Step{
	"review": decision("review", "manual review"),
	"revise": decision("revise", "request changes"),
	"accept": decision("accept", "auto accept"),
})
```

The expression chooses; ordinary steps perform the work. Keep domain side
effects out of rules.

## 2. Know the language boundary

The syntax resembles Go but intentionally supports only a small subset:

| Capability | Example |
| --- | --- |
| Store references | `load.output.items[0]` |
| Non-identifier node IDs | `node["load-user"].output` |
| Literals | `42`, `3.14`, `"ok"`, `true`, `nil` |
| Comparison | `== != < <= > >=` |
| Logic | `&& || !`, with short-circuiting |
| Arithmetic | `+ - * / %` |
| Built-ins | `len(x)` and `has(ref)` |

There are no assignments, function literals, type conversions, user-defined
calls, or access to the host program. Parsing compiles the expression into an
immutable closure; execution does not interpret source again.

There is no implicit truthiness or conversion:

- A condition must produce `bool`.
- A resolver must produce `string`.
- A missing reference returns `ErrUndefined`; guard an optional value with
  `has(ref)`.
- An incompatible operand returns `ErrType` rather than becoming `false`.
- Division or remainder by zero returns `ErrDivideByZero`.

Strict failure matters: a broken stop condition must not look like “not done
yet” and keep a loop running.

## 3. Analyze data dependencies

```go
compiled, err := expr.Parse(
	`has(score.output) && score.output >= 90`,
)
if err != nil {
	return err
}

for _, ref := range compiled.Refs() {
	fmt.Println(ref)
}
```

`Refs` returns structured `workflow.Ref` values, allowing tooling to compare
rule inputs with graph outputs before execution.

## 4. Register rules from configuration

When a `Spec` names conditions and resolvers, their definitions can travel in
configuration too:

```json
{
  "conditions": {
    "converged": "refine.output >= 100"
  },
  "switches": {
    "byScore": {
      "cases": [
        {"when": "score.output >= 90", "then": "accept"}
      ],
      "fallback": "review"
    }
  }
}
```

```go
var bindings expr.Bindings
if err := json.Unmarshal(data, &bindings); err != nil {
	return err
}
if err := bindings.Register(registry); err != nil {
	return err
}
```

The names then become available to a `workflow.Spec`. Call `bindings.Refs()` to
collect all data dependencies in the rule set.

## 5. Preserve decisions during resumption

Calling `route.Run` directly does not enable replay. If a workflow can suspend,
run it with `workflow.Run(..., RunConfig{Journal: journal})`. `Branch` records
its decision in the Journal, so a resumed run follows the same case even if an
external classifier, model, or changing input would choose differently.

Rules should still be deterministic functions of the Store. Journal replay is
a consistency boundary, not a reason to hide time or randomness in a resolver.

## Common mistakes

- Treating the expression language as a general scripting runtime.
- Depending on map iteration order for precedence. Use the ordered `Cases`
  slice.
- Treating an evaluation error as a false condition.
- Reading a reference that neither the graph nor the caller can produce.
- Accepting unbounded expression input without application-level size limits.

## Exercise

Add an explicit `60 <= score.output && score.output < 90` case and reorder the
cases to observe precedence. Remove the input to see `ErrUndefined`, then use
`has(score.output)` to define the missing-value policy explicitly.

[Previous: The JSON DSL and schemas](./05-json-dsl-and-schema.md) ·
[Next: Suspension and resumption](./07-suspension-and-resumption.md)
