# Level 4: Registries, ports, and DAGs

When nodes and edges are known only at run time, separate the Go operations the
application permits from the graph assembled for one definition. `Registry`
defines the vocabulary; `Graph` describes a flat DAG. The complete example is
[`example/dag_test.go`](../../example/dag_test.go).

## 1. Build typed nodes from configuration

Start with a factory for a single-input node:

```go
type unaryConfig struct {
	Value int `json:"value"`
}

unary := func(op func(int, int) int) workflow.LeafFactory {
	return workflow.Factory(
		func(cfg unaryConfig) (flow.Node[int, int], error) {
			return flow.NodeFunc[int, int](
				func(_ context.Context, in int) (int, error) {
					return op(in, cfg.Value), nil
				},
			), nil
		},
	)
}
```

`Factory` strictly decodes JSON into `unaryConfig`, builds the typed node, and
binds the graph's default port to that node.

Register node **types**, not graph instances:

```go
registry := workflow.NewRegistry().
	MustRegisterLeaf(
		"add",
		unary(func(a, b int) int { return a + b }),
	).
	MustRegisterLeaf(
		"multiply",
		unary(func(a, b int) int { return a * b }),
	)
```

`MustRegister...` is appropriate during application startup. Use the
error-returning registration methods when definitions arrive from plugins or
other recoverable sources.

## 2. Make multiple inputs visible with named ports

Use `BindFactory` for a typed node that reads several graph inputs. Binding
resolves port names once; execution reads their values from a Store:

```go
type pair struct {
	Left  int
	Right int
}

sum := workflow.BindFactory(
	func(_ struct{}, inputs workflow.Inputs) (workflow.BindFunc[pair], error) {
		left, leftOK := inputs.Ref("left")
		right, rightOK := inputs.Ref("right")
		if !leftOK || !rightOK {
			return nil, fmt.Errorf(
				"%w: want left and right",
				workflow.ErrMissingPort,
			)
		}
		return func(store workflow.Store) (pair, error) {
			a, err := workflow.Get[int](store, left)
			if err != nil {
				return pair{}, err
			}
			b, err := workflow.Get[int](store, right)
			return pair{Left: a, Right: b}, err
		}, nil
	},
	func(struct{}) (flow.Node[pair, int], error) {
		return flow.NodeFunc[pair, int](
			func(_ context.Context, in pair) (int, error) {
				return in.Left + in.Right, nil
			},
		), nil
	},
)
```

Port names are part of the node type's contract. A missing required port
matches `ErrMissingPort`; wiring a port the type does not declare matches
`ErrUnknownPort`.

## 3. Describe the dynamic boundary with `NodeSchema`

Go's generic type checker cannot inspect external graph data. Register metadata
alongside each dynamic node type:

```go
configSchema := json.RawMessage(`{
	"type": "object",
	"properties": {"value": {"type": "integer"}},
	"required": ["value"],
	"additionalProperties": false
}`)

registry.
	MustRegisterSchema("multiply", workflow.NodeSchema{
		Inputs:       workflow.OnePort(workflow.TypeNumber),
		Output:       workflow.TypeNumber,
		ConfigSchema: configSchema,
	}).
	MustRegisterSchema("sum", workflow.NodeSchema{
		Inputs: workflow.Ports{
			"left":  workflow.TypeNumber,
			"right": workflow.TypeNumber,
		},
		Output: workflow.TypeNumber,
	})
```

`NodeSchema` validates wiring and configuration. The factory and `Get[T]` remain
responsible for the actual Go types at execution time. Keep the schema and the
domain type aligned; they are two views of the same boundary.

## 4. Describe data flow with a Graph

```go
graph := workflow.Graph{Concurrency: 2, Nodes: []workflow.NodeSpec{
	{
		ID:    "twice",
		Type:  "multiply",
		Input: workflow.Output("start"),
		Config: json.RawMessage(`{"value":2}`),
	},
	{
		ID:    "plusTen",
		Type:  "add",
		Input: workflow.Output("start"),
		Config: json.RawMessage(`{"value":10}`),
	},
	{
		ID:   "total",
		Type: "sum",
		Inputs: workflow.Inputs{
			"left":  workflow.Output("twice"),
			"right": workflow.Output("plusTen"),
		},
	},
}}
```

The input ports imply the dependencies:

```text
start --> twice -----\
    `--> plusTen -----+--> total
```

`twice` and `plusTen` share a layer and may run concurrently; `total` waits for
both. Compilation turns the DAG into topological layers equivalent to
`Sequence(Parallel(layer), ...)`. `Graph.Concurrency` bounds each layer; zero
leaves it unbounded.

Use `DependsOn` for a pure control dependency. Ordinary data dependencies
belong in ports. If a factory hides Store references inside its config, graph
ordering, type checking, editors, and readers cannot see the real data flow.

## 5. Preflight external inputs and compile

A reference whose node ID is not in the graph is an external input:

```go
fmt.Println(graph.Inputs())
fmt.Println(graph.MissingInputs(workflow.NewStore()))
// [start#/output]
```

Compile once the Registry and graph are ready:

```go
step, err := registry.CompileGraph(graph)
if err != nil {
	return err
}

out, err := step.Run(
	ctx,
	workflow.NewStore().WithOutput("start", 5),
)
```

Compilation rejects duplicate IDs, unknown node types, invalid ports,
incompatible edge types, and cycles. A successful result is an ordinary,
reusable `workflow.Step`.

## Common mistakes

- Reading undeclared Store references from factory configuration.
- Registering node instances instead of constructors for node types.
- Repeating a data dependency in `DependsOn`.
- Treating a DAG as a node-at-a-time scheduler. Execution uses topological
  layers with a barrier between them.
- Rebuilding the Registry in every request. Assemble it at application startup.

## Exercise

Add a `subtract` node type with `left` and `right` ports and connect the outputs
of `twice` and `plusTen`. First omit one port and inspect the compilation error;
then complete the wiring.

[Previous: Stores, references, and steps](./03-workflow-store-and-ref.md) ·
[Next: The JSON DSL and schemas](./05-json-dsl-and-schema.md)
