# Level 5: The JSON DSL and schemas

A `Graph` can be built in Go or received from an API, database, or visual
editor. This level compiles untrusted JSON into a `Step` without weakening the
application boundary. The complete example is
[`example/json_test.go`](../../example/json_test.go).

## 1. Register only permitted capabilities

External JSON may refer only to node types explicitly registered by the host:

```go
type addConfig struct {
	N int `json:"n"`
}

add := workflow.Factory(
	func(cfg addConfig) (flow.Node[int, int], error) {
		return flow.NodeFunc[int, int](
			func(_ context.Context, in int) (int, error) {
				return in + cfg.N, nil
			},
		), nil
	},
)

registry := workflow.NewRegistry().
	MustRegisterNode("add", add).
	MustRegisterSchema("add", workflow.NodeSchema{
		Inputs: workflow.OnePort(workflow.TypeNumber),
		Output: workflow.TypeNumber,
		ConfigSchema: json.RawMessage(`{
			"type": "object",
			"properties": {"n": {"type": "integer"}},
			"required": ["n"],
			"additionalProperties": false
		}`),
	})
```

The JSON document describes structure and wiring; it does not carry arbitrary
executable code. The Registry is the capability boundary between data and the
host process.

## 2. Write a flat Graph document

`input` is shorthand for the default single-input port:

```json
{
  "nodes": [
    {
      "id": "a",
      "type": "add",
      "input": {"nodeID": "start", "path": "/output"},
      "config": {"n": 10}
    },
    {
      "id": "b",
      "type": "add",
      "input": {"nodeID": "a", "path": "/output"},
      "config": {"n": 5}
    }
  ]
}
```

`path` is an encoded RFC 6901 JSON Pointer. Use Go reference constructors in Go
code and encoded pointers such as `/output/items/0` in JSON.

## 3. Publish the schema to tooling

```go
schema := workflow.GraphJSONSchema()
```

This returns a safe copy of the embedded Draft 2020-12 schema for an editor or
API endpoint. Use `workflow.SpecJSONSchema()` for the nested control-flow form.

The schemas are self-contained. External `$ref` loading is disabled, so
validation never performs hidden network or filesystem access.

## 4. Validate and compile at the correct boundaries

Use standalone validation when only the document shape is available:

```go
if err := workflow.ValidateGraphJSON(data); err != nil {
	return err
}
```

Compile when the Registry is available:

```go
step, err := registry.CompileGraphJSON(data)
if err != nil {
	return err
}
```

`CompileGraphJSON` repeats structural validation before Registry-specific
checks. A server must still compile and validate data even if a client editor
already accepted it.

Four layers protect the path from bytes to executable Step:

| Layer | Detects |
| --- | --- |
| Strict JSON decoding | Syntax errors and duplicate object members |
| Embedded JSON Schema | Missing fields, wrong types, and unknown fields |
| Graph plus Registry | Duplicate IDs, unknown types, port errors, cycles, and incompatible edges |
| Node `ConfigSchema` | Configuration rules for one registered node type |

An omitted `config` is validated as `{}`, so schema `required` fields retain
their meaning.

## 5. Choose `Graph` or `Spec`

| Form | Best for | Dependency shape |
| --- | --- | --- |
| `Graph` | Data-processing DAGs and visual canvases | Flat nodes and port edges |
| `Spec` | Sequence, parallel, branch, loop, iteration, and subgraph | Nested control flow and sealed regions |

Do not force every definition into one DSL. `Graph` makes arbitrary data edges
clear; `Spec` makes structured control flow clear. Both compile to `Step`.

## 6. Handle diagnostic errors structurally

Use `errors.Is` for stable categories and `errors.As` for context:

```go
step, err := registry.CompileGraphJSON(data)
if err != nil {
	if errors.Is(err, workflow.ErrInvalidGraph) {
		var graphErr *workflow.GraphError
		if errors.As(err, &graphErr) {
			// Report graphErr.NodeID, graphErr.Field, and graphErr.Err.
		}
	}
	return err
}
```

The nested DSL uses `SpecError` and `ErrInvalidSpec`. Error text is for people;
sentinels and structured error types are the program contract.

## Common mistakes

- Validating only in the editor or browser.
- Using config schemas that require remote `$ref` resolution.
- Allowing unknown properties and thereby hiding misspelled fields.
- Registering an all-powerful node that bypasses the Registry's capability
  boundary.
- Expecting mutation of the slice returned by `GraphJSONSchema()` to change the
  embedded schema. The function returns a copy.

## Exercise

Create four invalid documents: one with a duplicate JSON member, one missing
`config.n`, one using an unknown node type, and one containing a cycle. Identify
which validation layer rejects each and use `errors.Is` to check
`ErrInvalidGraph`.

[Previous: Registries, ports, and DAGs](./04-graph-registry-and-ports.md) ·
[Next: Data-driven rules](./06-data-driven-rules.md)
