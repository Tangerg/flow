# Level 2: Composition and concurrency

A composition is still a `Node`. This level computes several squares
concurrently, then passes the complete result to a reducer. The runnable
counterpart is
[`example/composition_test.go`](../../example/composition_test.go).

## 1. Define two ordinary nodes

```go
square := flow.NodeFunc[int, int](
	func(_ context.Context, in int) (int, error) {
		return in * in, nil
	},
)

sum := flow.NodeFunc[[]int, int](
	func(_ context.Context, in []int) (int, error) {
		total := 0
		for _, value := range in {
			total += value
		}
		return total, nil
	},
)
```

## 2. `Map` is another node

`Map(square, config)` lifts a `Node[int, int]` into a
`Node[[]int, []int]`, so it connects directly to `sum`:

```go
pipeline := flow.Then(
	flow.Map(square, flow.MapConfig{Concurrency: 2}),
	sum,
)

out, err := pipeline.Run(
	context.Background(),
	[]int{1, 2, 3, 4},
)
if err != nil {
	return err
}
fmt.Println(out) // 30
```

Calls may finish out of order, but the output slice preserves input order.
`Concurrency: 2` permits at most two simultaneous `square` calls.

| `Concurrency` | Meaning |
| --- | --- |
| `0` | No additional concurrency limit |
| Positive | Maximum simultaneous calls |
| Negative | Invalid; the run returns `flow.ErrInvalidConfig` |

## 3. Failure and cancellation

When an element fails, `Map` cancels the context derived for that run and waits
for every already-started call to return. Calls that have not started do not
start. The worker must observe cancellation for it to be prompt:

```go
worker := flow.NodeFunc[Job, Result](
	func(ctx context.Context, job Job) (Result, error) {
		return client.Do(ctx, job)
	},
)
```

The returned error carries the element index. Use `errors.As` with
`*flow.IndexError`; do not parse an error string.

`Switch` applies the same rule to invalid branches: use `errors.As` with
`*flow.CaseError` to read the case key. If several cases are invalid, validation
returns their errors joined in deterministic diagnostic order.

Stopping promptly is not rollback. An external side effect that already
happened is not undone by `Map`. Concurrent side-effecting nodes should be
idempotent, or a later node should own the final commit.

## 4. Concurrency primitives and derived shapes

The core has two concurrency atoms:

- `Map` runs work for every input and waits for all of it: AND.
- `Race` runs several nodes on one input and returns the first success: OR.

`flowx` provides derived convenience shapes: `FanOut`, `Combine`, `Fallback`,
and `Chain`. Choose the shape that communicates intent, but keep domain logic
inside focused nodes.

## Common mistakes

- Assuming outputs follow completion order. `Map` preserves input order.
- Treating the limit as a process-wide goroutine pool. It applies to one
  `Map.Run`.
- Expecting cancellation to stop a function that ignores its context.
- Running non-idempotent external operations without limits, deduplication, or
  compensation.

## Exercise

Make `square` fail for negative values and extract the failing index with
`errors.As`. Try concurrency values `1` and `0`; output order should remain the
same.

[Previous: Nodes and `Then`](./01-node-and-then.md) ·
[Next: Stores, references, and steps](./03-workflow-store-and-ref.md)
