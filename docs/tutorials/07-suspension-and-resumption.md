# Level 7: Suspension and resumption

Some workflows must wait for a person, asynchronous callback, or external job.
`workflow` does not retain a Go call stack. It records completed Step boundaries
and re-enters the same definition with a `Store` and `Journal`. The complete
example is [`example/resume_test.go`](../../example/resume_test.go).

## 1. Choose the right waiting primitive

| API | Use when | Resume by |
| --- | --- | --- |
| `Await(id, ref)` | Another system will write a Store value | Supplying that `Ref` and running again |
| `Interrupt(id, value)` | The workflow emits a request and expects a result | Recording a response in the Journal |
| `Suspend(value)` | A node discovers that it cannot continue | Supplying external state or recording the step result |

This tutorial uses `Interrupt` as an explicit request-response boundary.

## 2. Model the wait as a Step

```go
type approvalRequest struct {
	Question string `json:"question"`
}

approval := workflow.Interrupt("approval", approvalRequest{
	Question: `Publish "guide"?`,
})
```

The first run suspends at `approval`. Once its response is recorded, the step
restores that response under `approval#/output`, exactly like an ordinary leaf:

```go
publish := workflow.Leaf(
	"publish",
	workflow.From[string](workflow.Output("approval")),
	flow.NodeFunc[string, string](
		func(_ context.Context, title string) (string, error) {
			return "published: " + title, nil
		},
	),
)
```

## 3. Run until the workflow suspends

Resumption requires a Journal passed through `workflow.Run`:

```go
journal := workflow.NewJournal()
paused, err := workflow.Run(
	context.Background(),
	pipeline,
	workflow.NewStore().WithOutput("topic", "guide"),
	workflow.RunConfig{Journal: journal},
)
if !errors.Is(err, workflow.ErrSuspended) {
	return err
}

waits := workflow.Suspensions(err)
wait := waits[0]
request := wait.Value.(approvalRequest)
fmt.Println(request.Question)
```

Suspension is a third outcome, not an ordinary failure:

- `paused` is the Store produced by completed work.
- `err` matches `ErrSuspended`.
- `Suspensions(err)` returns one or more structured waits.
- `wait.Key()` combines the scope path and step ID, distinguishing repeated
  instances inside loops and concurrent collections.

Do not persist only `wait.ID`; one step can have several active scoped
instances.

## 4. Persist the complete application state

At minimum, preserve:

| Data | Why |
| --- | --- |
| Paused `Store` | Workflow data produced so far |
| `Journal` | Completed steps, branch decisions, and external responses |
| `Suspension.Key()` | Exact callback-to-wait correlation |
| `Suspension.Value` or equivalent request | Approval UI or external job payload |
| Workflow definition version | A Journal is valid only for that definition |
| Application run ID, status, and authorization | Journal intentionally does not manage business runs |

Both Store and Journal support JSON round trips:

```go
storeJSON, err := json.Marshal(paused)
if err != nil {
	return err
}
journalJSON, err := json.Marshal(journal)
if err != nil {
	return err
}
```

The Store and Journal do **not** include the active suspension list
automatically. Persist each wait key and request in the application's own run
record so an incoming callback can resolve the right wait.

`Suspension.Value` may be a string, map, slice, array, or struct. It is owned by
the application and must be treated as immutable. Decoding a suspension through
an `any` field normally turns a struct into `map[string]any`. When the concrete
type matters, persist an application envelope such as
`kind/version/json.RawMessage` and decode the payload according to `kind`.

## 5. Record a response and resume

After restoring the Store and Journal, record the result under the exact key:

```go
if err := restoredJournal.Record(wait.Key(), "guide"); err != nil {
	return err
}

finished, err := workflow.Run(
	context.Background(),
	pipeline,
	restoredStore,
	workflow.RunConfig{Journal: &restoredJournal},
)
if err != nil {
	return err
}

result, err := workflow.Get[string](
	finished,
	workflow.Output("publish"),
)
```

The run re-enters at the root; it does not continue a serialized call stack.
Journal replay skips completed Leaf boundaries and restores their outputs, so
the earlier `prepare` step does not execute twice.

`Journal.Record` detects conflicts. Treat `ErrJournalConflict` from duplicate or
inconsistent callbacks as an application idempotency and audit concern rather
than silently overwriting the first result.

## 6. Understand composite suspension semantics

| Composite | Suspension behavior |
| --- | --- |
| `Sequence` | Stops and returns the current Store |
| `Parallel` | Lets siblings finish, merges writes, and reports every wait |
| `Iteration` | Lets other elements finish but does not write an incomplete collected output |
| `Loop` | Stops; the Journal replays completed iterations |

A waiting branch must not cancel its siblings as if it had failed. Doing so
would discard their completed work and repeat their side effects after resume.

Caller-defined fan-out composites can preserve the same distinction using only
public APIs:

```go
if err != nil {
	if !workflow.SuspendedOnly(err) {
		return store, err
	}
	waits = append(waits, workflow.Suspensions(err)...)
}
// After every waiting branch has returned:
return store, workflow.JoinSuspensions(waits...)
```

`SuspendedOnly` examines the complete standard Go error tree. It returns false
for a join containing both a suspension and a real failure, even though
`errors.Is(err, workflow.ErrSuspended)` is true.

## 7. Keep the boundary explicit

This is step-level checkpoint and restart, not a durable workflow service. It
does not provide:

- Timers or distributed scheduling.
- Queues, leases, or multi-tenant authorization.
- Workflow definition migration.
- Exactly-once external side effects.

If a process fails after an external side effect succeeds but before the result
is recorded, replay may perform the effect again. Use idempotency keys,
transactional outboxes, or domain compensation; Journal cannot roll back the
external world.

One Journal belongs to one logical run and one workflow definition version. Do
not share it across unrelated runs or replay it after changing step IDs or
graph structure.

## Common mistakes

- Expecting `Interrupt` to resume without a Journal.
- Persisting Store but not Journal and active application waits.
- Replacing the complete `JournalKey` with a step ID.
- Resuming against a modified workflow definition.
- Assuming replay gives exactly-once side effects.
- Mutating a map, slice, or pointer passed to `Interrupt` or `Suspend`.

## Exercise

Replace one approval with parallel legal and editorial approvals. The first run
should report two suspensions. Record only one response and confirm the other
wait remains. Record both and verify that the publishing step runs once.

[Previous: Data-driven rules](./06-data-driven-rules.md) ·
[Tutorial index](./README.md)
