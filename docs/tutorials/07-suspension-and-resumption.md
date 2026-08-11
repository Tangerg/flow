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
publish := workflow.LeafFunc(
	"publish",
	workflow.Output("approval"),
	func(_ context.Context, title string) (string, error) {
		return "published: " + title, nil
	},
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
if len(waits) != 1 {
	return fmt.Errorf("unexpected waits: %d", len(waits))
}
wait := waits[0]
request, ok := wait.Value.(approvalRequest)
if !ok {
	return fmt.Errorf("unexpected request type %T", wait.Value)
}
fmt.Println(request.Question)
```

Suspension is a third outcome, not an ordinary failure:

- `paused` is the Store produced by completed work.
- `err` matches `ErrSuspended`.
- `Suspensions(err)` returns one or more structured waits.
- `wait.Key()` combines the scope and step ID, distinguishing repeated
  instances inside loops and concurrent collections as well as inner steps in
  sealed subgraphs.

Each scope entry is a `workflow.ScopeFrame`. `ID` names the enclosing
composite; `Indexed` and `Index` identify one invocation of a repeated body.
Treat those fields as identity rather than parsing `ScopeFrame.String()`: an
ordinary composite ID is allowed to contain bracket-like text.

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
| flow module version | Journal wire compatibility is a separate concern |
| Application run ID, status, and authorization | Journal intentionally does not manage business runs |

Store, Journal, and each active Suspension support JSON round trips:

```go
storeJSON, err := json.Marshal(paused)
if err != nil {
	return err
}
journalJSON, err := json.Marshal(journal)
if err != nil {
	return err
}
waitJSON, err := json.Marshal(wait)
if err != nil {
	return err
}
```

Encoding rejects values that `encoding/json` cannot represent, malformed or
ambiguous JSON returned by custom marshalers, invalid engine-owned identity
text, and documents whose nesting exceeds `workflow.MaxNestingDepth`. Ordinary
application strings retain `encoding/json` semantics: invalid UTF-8 bytes are
replaced with U+FFFD. Decoding is stricter and rejects invalid input bytes and
unpaired UTF-16 surrogate escapes, so bytes produced successfully by these
types are structurally readable by the corresponding type.

The Store and Journal do **not** include the active suspension list
automatically. Persist each wait key and request in the application's own run
record so an incoming callback can resolve the right wait.

`Suspension` and `JournalKey` also support direct JSON round trips. Their
decoders reject unknown or duplicate members, invalid Unicode, malformed
identity, and excessive nesting without changing an existing destination.
An anonymous suspension has neither a step ID nor a scope; after a boundary
identifies it, an empty scope means the workflow root. A scope without a step ID
is rejected because it cannot identify a resumable invocation.
Suspension application values decode into JSON-domain values, with numbers kept
as `json.Number`; use a typed application envelope when the original concrete
Go type matters.

The Journal v3 document encodes scope as structured frame objects and the
decoder rejects any other wire version. Keep the flow module version with
durable run records and test upgrades against representative archived Journals.
Version and scope-index numbers are read by mathematical value, so integral
decimal and exponent spellings are equivalent; a scope index must fit `uint64`.
This wire contract is separate from the workflow-definition version: compatible
bytes cannot make renamed steps or changed control flow safe to resume.

A nil `*Journal` means resumption is disabled and encodes as JSON `null`. An
allocated zero-value Journal encodes as an empty versioned checkpoint. Persist
the latter when the run has no records yet but will resume later. Always pass a
`*Journal` to `encoding/json`: its synchronized JSON implementation belongs to
the pointer method set. A `Journal` value has only unexported implementation
fields and is not a checkpoint representation.

`Suspension.Value` may be a string, map, slice, array, or struct. It is owned by
the application and must be treated as immutable. Decoding a suspension through
an `any` field normally turns a struct into `map[string]any`. When the concrete
type matters, persist an application envelope such as
`kind/version/json.RawMessage` and decode the payload according to `kind`.

## 5. Record a response and resume

After restoring the Store and Journal, record the result under the exact key:

```go
var restoredWait workflow.Suspension
if err := json.Unmarshal(waitJSON, &restoredWait); err != nil {
	return err
}
if err := restoredJournal.Record(restoredWait.Key(), "guide"); err != nil {
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

Replay applies only at explicit Journal boundaries. `Leaf` records its output,
`Interrupt` consumes a recorded response, and `Branch` and `Loop` record their
decisions. An opaque caller-defined `Step` is not checkpointed merely because
it runs inside `Parallel`, `Iteration`, `Graph`, or `Subgraph`; if it owns work
that must not repeat, expose that work through `Leaf` or another explicit
journaled boundary.

Treat the returned Store and Journal as two parts of one checkpoint. The
Journal may contain a completion absent from a Store returned with cancellation
or failure, for example when a parallel sibling finished first. The next run
replays that record and reconstructs the output; do not try to regenerate the
Journal from Store contents.

`Journal.Record` detects conflicts. Treat `ErrJournalConflict` from duplicate or
inconsistent callbacks as an application idempotency and audit concern rather
than silently overwriting the first result.

## 6. Understand composite suspension semantics

| Composite | Suspension behavior |
| --- | --- |
| `Sequence` | Stops and returns the current Store |
| `Parallel` | Lets siblings finish, merges Store changes, and reports every wait |
| `Iteration` | Lets other elements finish but does not write an incomplete collected output |
| `Loop` | Stops; the Journal replays completed iterations |
| `Graph` | Blocks descendants of a waiting node but lets unrelated ready work finish |
| `Subgraph` | Publishes no outer output; its completed inner boundaries remain in the Journal |

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
graph structure. Use application-level ownership or a lease to admit only one
active `Run` for that logical execution; Journal synchronization coordinates
branches within a run, not competing runs.

## Common mistakes

- Expecting `Interrupt` to resume without a Journal.
- Persisting Store but not Journal and active application waits.
- Calling `Journal.Forget` on one producer but retaining checkpoints of steps
  that consumed its old result. Forget the dependent closure or reset the
  Journal; Journal itself intentionally does not know the definition graph.
- Replacing the complete `JournalKey` with a step ID.
- Resuming against a modified workflow definition.
- Starting competing `Run` calls for one logical execution without an
  application-level ownership guard.
- Assuming replay gives exactly-once side effects.
- Mutating a map, slice, or pointer passed to `Interrupt` or `Suspend`.

## Exercise

Replace one approval with parallel legal and editorial approvals. The first run
should report two suspensions. Record only one response and confirm the other
wait remains. Record both and verify that the publishing step runs once.

[Previous: Data-driven rules](./06-data-driven-rules.md) ·
[Next: Streaming output](./08-streaming-output.md)
