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

Every edge uses the same named-port shape. A single-input node wires the
conventional `in` port:

```json
{
  "nodes": [
    {
      "id": "a",
      "type": "add",
      "inputs": {"in": {"nodeID": "start", "path": "/output"}},
      "config": {"n": 10}
    },
    {
      "id": "b",
      "type": "add",
      "inputs": {"in": {"nodeID": "a", "path": "/output"}},
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
validation never performs hidden network or filesystem access. Node config
schemas use Draft 2020-12 throughout: `$schema` may be omitted, but an explicit
dialect on the root or an embedded schema resource must use the Draft 2020-12
HTTP or HTTPS URI (an empty `#` fragment is equivalent) and cannot switch drafts.

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

Direct `encoding/json` decoding into `workflow.Graph` or `workflow.Spec` uses
the same strict structural contract and replaces the destination only after the
whole document succeeds. Use that form when application code needs to inspect
or transform the typed definition before passing it to `ValidateGraph`,
`ValidateSpec`, `CompileGraph`, or `CompileSpec`. Do not decode through a locally
defined copy type or an intermediate permissive struct: doing so discards the
DSL boundary.

Encoding those types is lossless by contract: `json.Marshal` rejects invalid
UTF-8 identity text, malformed or ambiguous raw config, cyclic Spec bodies, and
values beyond the engine nesting limit instead of emitting bytes that decode as
a different definition. This is a representation guarantee, not Registry
validation; unresolved node types, ports, schemas, and capabilities still
belong to `ValidateGraph`, `ValidateSpec`, or compilation.

`CompileGraphJSON` repeats structural validation before Registry-specific
checks. A server must still compile and validate data even if a client editor
already accepted it.

The same identity rules apply to code-built definitions. Step and node IDs,
registered names, port names, references, branch cases, and routing outlets must
be valid UTF-8; validation rejects them before node code runs. This prevents
`encoding/json` from silently replacing identity bytes during persistence.

Four layers protect the path from bytes to executable Step:

| Layer | Detects |
| --- | --- |
| Strict JSON decoding | Syntax errors, invalid Unicode text, duplicate object members, and engine limits that cannot fit the platform's `int` |
| Embedded JSON Schema | Missing fields, wrong types, and unknown fields |
| Graph plus Registry | Duplicate IDs, unknown types, port errors, cycles, incompatible edges, and invalid factory-built Steps |
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
			// Report graphErr.Path, NodeID, Field, and Err.
		}
	}
	return err
}
```

`GraphError.Path` is an RFC 6901 JSON Pointer to the containing graph node; for
example, the second node is `"/nodes/1"`. Whole-graph failures such as a cycle
have an empty path. The nested DSL uses `SpecError` and `ErrInvalidSpec`.
`SpecError.Path` similarly identifies the containing specification; a bad leaf
type in the first step has `Path == "/steps/0"`. In both errors, `Field` names
the invalid member. Error text is for people; sentinels and structured error
types are the program contract.

## Common mistakes

- Validating only in the editor or browser.
- Passing unbounded request bodies or definitions to the engine. The nesting
  limit protects recursive boundaries; byte, node-count, and application config
  quotas belong at the host API boundary.
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
