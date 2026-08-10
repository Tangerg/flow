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

unary := func(op func(int, int) int) workflow.NodeFactory {
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

For an ordinary struct such as `unaryConfig`, `Factory` validates one complete
JSON value, rejects unknown fields, builds the typed node, and binds the graph's
default port to that node. A config type with its own `UnmarshalJSON` method
owns its field-level decoding rules; pair it with an explicit
`NodeSchema.ConfigSchema` when editor-time validation must enforce the same
contract.

Register node **types**, not graph instances:

```go
registry := workflow.NewRegistry().
	MustRegisterNode(
		"add",
		unary(func(a, b int) int { return a + b }),
	).
	MustRegisterNode(
		"multiply",
		unary(func(a, b int) int { return a * b }),
	)
```

A factory builds an immutable definition and has no execution context. Keep it
prompt and deterministic; network, model, database, and other cancellable work
belongs in the `Node` returned by the factory.

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
	func(_ struct{}, inputs workflow.Inputs) (workflow.Binder[pair], error) {
		left, leftOK := inputs.Ref("left")
		right, rightOK := inputs.Ref("right")
		if !leftOK || !rightOK {
			return nil, fmt.Errorf(
				"%w: want left and right",
				workflow.ErrMissingPort,
			)
		}
		return workflow.BinderFunc[pair](func(store workflow.Store) (pair, error) {
			a, err := workflow.Get[int](store, left)
			if err != nil {
				return pair{}, err
			}
			b, err := workflow.Get[int](store, right)
			return pair{Left: a, Right: b}, err
		}), nil
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

`BindFactory` returns a `Binder`, the same protocol accepted by `Leaf`. The
example uses `BinderFunc` to adapt an ordinary function, just as `flow.NodeFunc`
adapts an ordinary node function.

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

Registration makes the schema authoritative. An empty `Inputs` map means the
node accepts zero ports, so wiring any port is `ErrUnknownPort`. Omit schema
registration only when deliberately accepting no schema-level preflight for
wiring and config; the factory can still reject either during compilation.
Even without a schema, compilation inspects the concrete boundary returned by
the factory and rejects a data edge from a node that produces no output;
`ValidateGraph` stays factory-free and therefore cannot report that case.
Every input port has an explicit type; use `TypeAny` when its shape is
intentionally unrestricted. Leave `Output` zero only for a node such as `Await`
that publishes no conventional output. A data edge from such a node is
rejected; use `DependsOn` when downstream work needs ordering without a value.
Nested references may descend through `TypeObject`, `TypeArray`, or `TypeAny`.
Scalar outputs have no child paths, and the first child of an array output must
be a canonical non-negative index; impossible paths are rejected before run.

## 4. Describe data flow with a Graph

```go
graph := workflow.Graph{Concurrency: 2, Nodes: []workflow.GraphNode{
	{
		ID:     "twice",
		Type:   "multiply",
		Inputs: workflow.DefaultInput(workflow.Output("start")),
		Config: json.RawMessage(`{"value":2}`),
	},
	{
		ID:     "plusTen",
		Type:   "add",
		Inputs: workflow.DefaultInput(workflow.Output("start")),
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

`DefaultInput(ref)` constructs the same one-entry `Inputs` map used by every
edge; it is not an alternate input field. Use an `Inputs` literal when a node
has several ports.

The input ports imply the dependencies:

```text
start --> twice -----\
    `--> plusTen -----+--> total
```

`twice` and `plusTen` may run concurrently; `total` starts as soon as both
complete. Compilation retains the dependency edges rather than inserting
topological barriers, so a fast dependency chain never waits for an unrelated
slow branch. `Graph.Concurrency` bounds the whole graph; zero leaves it
unbounded.

Each node receives the initial Store plus only the writes of its declared direct
dependencies, applied in graph declaration order. Completion timing therefore
cannot expose a transitive or unrelated value.

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

This is the graph's complete potential-input set, not an unconditional
required-parameter set. A conditional node contributes its inputs even when a
particular routing decision bypasses it. `MissingInputs` reports which potential
inputs do not currently resolve; the run itself determines whether a conditional
one is needed.

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
incompatible edge types, cycles, and factory results that are opaque, use the
wrong ID, or expose an unsealed composite. A successful result is an ordinary,
reusable `workflow.Step`.

## Common mistakes

- Reading undeclared Store references from factory configuration.
- Returning a bare Step or composite from a factory. Use `Leaf` for typed work
  and `Subgraph` for a composite region.
- Registering node instances instead of constructors for node types.
- Repeating a data dependency in `DependsOn`.
- Assuming declaration order creates a dependency or that unrelated branches
  form a barrier.
- Rebuilding the Registry in every request. Assemble it at application startup.

## Exercise

Add a `subtract` node type with `left` and `right` ports and connect the outputs
of `twice` and `plusTen`. First omit one port and inspect the compilation error;
then complete the wiring.

[Previous: Stores, references, and steps](./03-workflow-store-and-ref.md) ·
[Next: The JSON DSL and schemas](./05-json-dsl-and-schema.md)
