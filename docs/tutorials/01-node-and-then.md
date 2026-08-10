# Level 1: Nodes and `Then`

This level builds the smallest typed pipeline: parse a string as an integer,
then double it. The complete runnable version is
[`example/node_test.go`](../../example/node_test.go).

## 1. Adapt ordinary functions

`NodeFunc` turns a function with the right signature into a `Node`; no wrapper
struct is required:

```go
parse := flow.NodeFunc[string, int](
	func(_ context.Context, in string) (int, error) {
		return strconv.Atoi(strings.TrimSpace(in))
	},
)

double := flow.NodeFunc[int, int](
	func(_ context.Context, in int) (int, error) {
		return in * 2, nil
	},
)
```

The types already describe the contract:

```text
parse:  string -> int
double: int    -> int
```

## 2. Connect different types with `Then`

`Then` requires the first node's output to match the second node's input:

```go
pipeline := flow.Then(parse, double)

out, err := pipeline.Run(context.Background(), " 21 ")
if err != nil {
	return err
}
fmt.Println(out) // 42
```

`pipeline` is a `flow.Node[string, int]`. It can run directly or become an
argument to another `Then`, `Map`, or other combinator.

Trying to connect `flow.Node[string, int]` to `flow.Node[bool, int]` does not
compile. An invalid edge is rejected while building the program rather than
halfway through a run.

`flow.Validate(pipeline)` checks the complete visible composition without
running either node. Composites from `flow`, `flowx`, and `workflow` participate
recursively. A caller-defined composite can opt in with a pure,
concurrency-safe `Validate() error` method; an ordinary node remains an opaque
boundary. Applications rarely need this directly. It exists for boundaries
such as a replaying workflow leaf that must reject an invalid definition even
when it does not call `Run`.

## 3. Execution semantics

`flow.Then(a, b)`:

1. Calls `a.Run(ctx, in)`.
2. Returns immediately if `a` fails; `b` is not called.
3. Passes `a`'s output to `b.Run(ctx, out)`.
4. Returns `b`'s output and error unchanged.

Both nodes receive the same context. Cancellation is cooperative: a node must
observe `ctx.Done()` or pass the context to downstream HTTP, database, or other
blocking calls.

## 4. Why there is no heterogeneous fluent builder

Go methods cannot introduce new type parameters. A fluent chain whose types
change from `string` to `int` to `bool` must either erase the types or generate
wrappers for every shape. `Then(parse, validate)` is deliberately plain and
retains full generic inference.

For a same-type sequence, use `flowx.Chain` when it improves readability. The
root package keeps the irreducible operation only.

## Common mistakes

- Ignoring the error returned by `Run`. A node error is control flow, not a log
  message.
- Capturing mutable state in a `NodeFunc` without synchronizing it.
- Converting values to `any` only to obtain a fluent surface.
- Replacing the passed context inside reusable library code.

## Exercise

Append a `flow.NodeFunc[int, string]` that formats the result as
`"result=42"`. Then give `parse` invalid input and verify that neither later
node runs.

[Previous: Getting started](./00-getting-started.md) ·
[Next: Composition and concurrency](./02-composition-and-concurrency.md)
