# Level 3: Stores, references, and steps

The first two levels pass Go values directly between nodes. This level enters
the dynamic layer: each step reads a named value from a `Store` and writes its
result back. That trades some compile-time edge typing for runtime assembly.
See [`example/workflow_test.go`](../../example/workflow_test.go) for the
complete example.

## 1. The dynamic layer is still `Node`

`workflow.Step` is an alias for:

```go
flow.Node[workflow.Store, workflow.Store]
```

It still follows `Run(ctx, in) (out, error)`. The difference is that every step
has the same input and output type, `Store`, and references describe the edges
instead of generic type parameters.

## 2. Lift a typed function with `LeafFunc`

This leaf reads a string from `input#/output`, trims it, and writes the result
under `clean#/output`:

```go
clean := workflow.LeafFunc(
	"clean",
	workflow.Output("input"),
	func(_ context.Context, in string) (string, error) {
		return strings.TrimSpace(in), nil
	},
)
```

Each argument has one responsibility:

| Argument | Responsibility |
| --- | --- |
| `"clean"` | Stable step ID and owner of the conventional output |
| `Output("input")` | Reference to read and convert from the Store |
| Typed function | Perform the domain operation |

Now define a step that consumes the first result:

```go
greet := workflow.LeafFunc(
	"greet",
	workflow.Output("clean"),
	func(_ context.Context, name string) (string, error) {
		return "hello, " + name, nil
	},
)
```

Use the lower-level `Leaf(id, bind, node)` form when binding needs several
references or an existing typed `flow.Node` already carries decorators. A
`Binder[I]` prepares the node's typed input from the current Store. Use `Ref.Bind`
for one reference, `FirstOf` with at least one mutually exclusive alternative,
and adapt a custom binding function with `BinderFunc[I]`. Binding is local data
preparation, so it has no context; put blocking or cancellable work in the typed
`Node`.

`Ref.Bind` and `FirstOf` retain their references as definitions, so `Leaf` validates
them before Journal replay. A malformed reference therefore cannot be hidden by
an older completed record. A custom Binder with static state can provide the
same guarantee with a pure `Validate() error` method and can call
`ref.Validate()` rather than duplicating the JSON Pointer rules.

## 3. Sequence and persistent state

```go
pipeline := workflow.Sequence(clean, greet)

out, err := pipeline.Run(
	context.Background(),
	workflow.NewStore().WithOutput("input", " Ada "),
)
if err != nil {
	return err
}

message, err := out.Get[string](workflow.Output("greet"))
```

`Sequence` passes each returned Store to the next step. `Store` is a persistent
value object: a write returns a new snapshot, the old snapshot remains
unchanged, and the implementation shares untouched structure.

A Store does not label cells as "input" or "output." Build a fresh seed Store
for a new logical run unless carrying earlier cells forward is intentional.
`workflow.Run` resets run-scoped bookkeeping, not Store contents. A compiled
`Graph` can do more: because all Graph node IDs are declared, it removes those
internal cells at its boundary and rebuilds them from execution or Journal
replay.

Stored values themselves are not deep-copied. Treat a map, slice, pointer, or
other mutable value as immutable after insertion and after reading it back;
mutating it would affect every snapshot that shares it and could introduce a
data race. An exact-type `Store.Get[T]` result is borrowed from the Store. A read that
needs JSON conversion produces a newly decoded value instead.

## 4. References are JSON Pointers

`workflow.Output("clean")` addresses `/output` under node `clean`. Build deeper
paths from literal segments:

```go
ref := workflow.Output("load").
	Child("items", "0", "display/name")

fmt.Println(ref.String())
// load#/output/items/0/display~1name
```

`Child` escapes `/` and `~` according to RFC 6901. Do not invent a dotted path
format: JSON keys may contain dots, slashes, tildes, or even be empty.
`Ref.Validate` checks a definition without reading a Store; use it in
caller-defined Binders and Steps that retain references.

## 5. Prefer typed reads

Application code should normally use:

```go
value, err := store.Get[Order](ref)
```

`Store.Get[T]` also converts JSON-domain values after a Store has been serialized and
restored, provided `T` has a faithful JSON round trip. A type with a custom
`MarshalJSON` method needs the corresponding decoding contract if callers expect
to recover that Go type. `store.Lookup(ref)` is useful when infrastructure code
genuinely needs raw `any`. A raw lookup after JSON decoding may return `json.Number`,
`[]any`, or `map[string]any`, so business code should keep the expected type at
the call site with `store.Get[T](ref)`. Built-in scalar conversion is lossless by design:
numeric kinds are not rounded or parsed from strings, and decoding into an
ordinary struct rejects unknown JSON members instead of silently dropping them.
A type implementing `json.Unmarshaler` defines its own decoding contract.

## 6. Direct `Run` or `workflow.Run`

Call `step.Run` when the execution needs no run-level services. Use
`workflow.Run` when a caller supplies an observer, emitter, or journal:

```go
out, err := workflow.Run(
	ctx,
	step,
	store,
	workflow.RunConfig{
		Observer: observer,
		Journal:  journal,
	},
)
```

`RunConfig` belongs to one call. It is not global workflow configuration.
When a caller-defined composite invokes a child in the same execution, it calls
`child.Run` directly. Calling package-level `workflow.Run` from inside another
run creates an independent boundary with a new root scope, identity claims, and
signal sequence.

## Common mistakes

- Lifting every static pipeline into `Step`. Do so only for runtime composition
  or named data.
- Hand-building reference strings instead of using `Output`, `At`, `Item`,
  `ItemIndex`, and `Ref.Child`.
- Spreading type assertions from `Lookup` throughout application code.
- Changing a step ID casually. IDs are data addresses, diagnostics, and replay
  keys.
- Retrying the returned `Step` instead of the typed node inside `Leaf`. A named
  Step may run once per scope; use `Branch` for mutually exclusive alternatives.
- Mutating a map or slice after writing it to a Store.

## Exercise

Make `clean` return a struct with a `Name` field. Read
`workflow.Output("clean").Child("Name")` in `greet`, then serialize and restore
the Store and verify that `Store.Get[string]` still reads the field.

[Previous: Composition and concurrency](./02-composition-and-concurrency.md) ·
[Next: Registries, ports, and DAGs](./04-graph-registry-and-ports.md)
