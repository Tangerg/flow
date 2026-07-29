# Level 0: Getting started

This level establishes the package boundaries before building a workflow.
`flow` is not a central orchestrator that must own every node. It is first a
small set of nodes that compose with other nodes. Dynamic DAGs, the JSON DSL,
and resumption are optional layers built on that core.

## 1. Install

`flow` requires Go 1.25 or newer:

```sh
go get github.com/Tangerg/flow
```

When working from the repository, verify the executable examples first:

```sh
go test ./example -run Example -v
```

Each example has a Go `Output` assertion, so the examples are both
documentation and tests.

## 2. Choose the smallest package

| Requirement | Start with | Primary types |
| --- | --- | --- |
| A statically defined, type-safe pipeline | `flow` | `Node[I, O]` |
| Convenient fan-out, chaining, or fallback | `flowx` | `Node[I, O]` |
| Runtime-defined named steps or a DAG | `workflow` | `Step`, `Store` |
| JSON input or a visual editor | `workflow` | `Registry`, `Graph`, `Spec` |
| Routing rules that must also be data | `workflow/expr` | `Condition`, `Resolver` |

A useful rule is: **if the control flow ships with the binary, begin with
`flow`; if the definition comes from a database, JSON document, or editor, use
`workflow`.** Do not give up Go's type system for a possible future need.

## 3. Understand the minimal protocol

The typed layer revolves around one interface:

```go
type Node[I, O any] interface {
	Run(ctx context.Context, in I) (O, error)
}
```

Like `io.Reader` or `http.Handler`, the protocol is small and composition lives
outside it. A node handles one call:

- `ctx` carries cancellation, deadlines, and request-scoped values.
- `in` is the complete input for the call.
- The return values are the output or an error.
- No package-level runtime is involved.

`NodeFunc` adapts an ordinary function to `Node`. Combinators accept nodes and
return new nodes. Nesting nodes is therefore the basic orchestration model, not
a shortcut around one.

## 4. Two ways to assemble control flow

```text
Definition known in Go
    Node -- Then / Map / Switch --> Node
                  |
                  `-- the composition is still a Node

Definition known only at run time
    JSON / Graph -- Registry.Compile --> workflow.Step
                                           |
                                           `-- still Node[Store, Store]
```

They share an execution protocol but solve different problems:

- Direct composition preserves generic types and gives the strongest refactoring
  guarantees.
- `workflow` trades typed edges for a `Store` and named ports so definitions can
  be data.
- `Registry` is a controlled boundary between configuration and Go code, not a
  resident scheduling service.

## 5. Where to go next

Levels 1 and 2 explain closed composition over `Node`. Level 3 introduces the
reason the dynamic layer needs a `Store`. Levels 4 through 7 add DAGs, JSON,
rules, and resumption one capability at a time.

## Common mistakes

- Starting every pipeline in `workflow`. Most static pipelines only need
  `flow`.
- Replacing the caller's context with `context.Background()` inside a node.
- Defining an interface for every function in advance. In Go, consumers usually
  discover the interface they need.
- Assuming a node instance is single-use. A node may be reused concurrently;
  synchronize any mutable state it owns.

## Exercise

Read the seven levels in the [`example`](../../example/README.md) index and
decide how far your use case needs to go. Run the examples to confirm your
environment is ready.

[Next: Nodes and `Then`](./01-node-and-then.md)
