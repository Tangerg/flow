# Contributing

Focused issues and pull requests are welcome. Before changing behavior or an
exported API, read the package boundaries in the
[project README](./README.md#choose-the-smallest-package).

## Requirements

- Go 1.26 or newer.
- `golangci-lint` v2 (CI currently pins v2.12.2).
- `actionlint` (CI currently pins v1.7.10).
- `govulncheck` (CI currently pins v1.6.0).
- Node.js 22 or newer when changing Markdown documentation.
- A clean module graph with no committed `replace` directives.
- Tests written with the standard `testing` package.

## Development workflow

Format and run the fast local checks while iterating:

```sh
gofmt -w .
go test ./...
go vet ./...
```

Before opening a pull request, run the complete gate used by CI:

```sh
test -z "$(gofmt -l .)"
go mod tidy -diff
go test -race -coverprofile=coverage.out ./...
coverage="$(go tool cover -func=coverage.out | awk '/^total:/ { print $3 }')"
awk -v coverage="${coverage%\%}" 'BEGIN { exit coverage < 95.0 }'
go vet ./...
golangci-lint run ./...
actionlint
govulncheck ./...
npx --yes markdownlint-cli2@0.23.2
```

The coverage floor protects behavior without making every defensive statement
a public design constraint. Keep meaningful coverage as high as the tests
naturally support, but do not add white-box tests or distort production control
flow solely to preserve an exact percentage.

Changes to the learning path should also run:

```sh
go test ./example -run Example -v
```

## Design boundaries

- Keep `flow.Node` a single-method interface.
- Put general control-flow primitives in `flow`.
- Put derived combinators and decorators in `flowx`.
- Put named state and runtime definitions in `workflow`.
- Keep the expression language optional in `workflow/expr`.
- Keep definition rendering optional and derived in `workflow/diagram`.
- Keep one named-port shape for workflow data edges; unary nodes use
  `workflow.OneInput`.
- Give a concept the same name in the JSON `Spec` and in the Go config that
  builds the same step, so the two forms map mechanically. The types differ —
  `Spec.Body` is a nested `Spec` while `LoopConfig.Body` is a `Step`, and
  `Spec.Condition` is a registered name while `LoopConfig.Condition` is the node
  — but the field name should not. Naming a field after its type, as in
  `Inputs Inputs`, is preferred over inventing a synonym.
- Express a value the engine computes from a Store as `flow.Node[Store, T]`
  rather than a bespoke function type. `Step`, `Resolver`, and `Condition` are
  aliases for that shape, so they compose with the typed helpers, share one
  validation path, and need no second execution protocol. Do not add a
  parameter that the execution context already carries: a repeated boundary
  publishes its index through the scope, which `Scope` reports.
- Keep one construction shape per category of workflow step. A composite that
  contains other steps and names itself takes exactly one `Config` struct that
  owns every field, including its ID and body: `Branch`, `Loop`, `Iteration`,
  and `Subgraph`. Do not split a composite's inputs between positional
  parameters and a config, and do not alias a `flow` config as a workflow one,
  which prevents the workflow form from carrying its own fields. `Sequence`
  stays variadic because it has no settings, and a step with no children keeps
  positional parameters: `Leaf`, `Await`, `Interrupt`, `Route`. Delegate the
  meaning of a shared setting to the `flow` config that defines it rather than
  restating the rule.
- Let `Store` own every decision about its own representation. A composite must
  not read `Store.depth` or compare it against `storeOverlayLimit`; it calls the
  named `Store` method for what it is about to do. `Store.bounded` is the single
  place the overlay limit is enforced, so every path that extends an overlay
  ends there rather than restating the threshold. A composite that hands one
  Store to concurrent derivers — parallel branches, iteration elements, graph
  nodes — passes it through `Store.sharedBase` first, or each deriver flattens
  the snapshot separately and the fan-out costs one copy per deriver.
  `BenchmarkParallelBaseScaling`, `BenchmarkIterationBaseScaling`, and
  `BenchmarkGraphRunBaseScaling` vary the input overlay length so a new fan-out
  site that skips this shows up as an allocation cliff at the limit.
- State a wire member set once where you can. `expr` encodes and decodes through
  the same struct tags, so its members cannot drift. `workflow` states some sets
  twice on purpose: an explicit member list, or the embedded JSON Schema, rejects
  unknown, duplicate, and case-folded members, which `encoding/json` cannot. A
  second statement therefore has to be pinned, not trusted — a field added to one
  side alone yields a value that marshals to a document it then rejects. Add a
  type with a second statement to `TestWireTypesRoundTripEveryPopulatedField`,
  and keep a kind-discriminated one such as `Spec` in
  `TestSpecFieldMatricesAgreeWithTheSpecStruct`. Both derive what they expect by
  reflecting over the struct, so neither becomes another list to maintain.
- Name the test, and keep the name resolving. A comment that cites a test is how
  a pinned rule proves it is pinned: the reader checks the test instead of taking
  the comment's word. A citation that no longer resolves reads exactly like one
  that does, so a rename quietly turns a pinned rule back into a trusted one.
  `TestCitedTestsResolve` walks every comment and every document and fails on a
  name no test defines — three of eleven citations were already broken when it was
  written, one of them naming a benchmark that had never existed. A citation can
  also resolve to the wrong test: a comment that opens with a test's name must be
  attached to that test, because a new test inserted into an existing comment block
  inherits its opening lines and leaves the old test undocumented.
  `TestTestCommentsNameTheirOwnTest` checks the attachment that
  `TestCitedTestsResolve` cannot see.
- Name the package exactly once in an error. Each package reaches that
  differently, and the difference is forced rather than chosen: `flow`'s
  sentinels carry `flow:` because most of them reach a caller with nothing
  wrapping them, so the locations it adds — `IndexError`, a switch case — state
  only where. `workflow` and `expr` are the mirror image, because a `StepError`,
  `GraphError`, `SpecError`, or `expr.Error` always supplies the name, so the
  sentinels they wrap state only the condition. An error assembled from several
  independent ones adds nothing of its own, the way `errors.Join` does not: a
  fan-out of three suspensions names the package three times, not four.
  `TestErrorsNameThePackageAtMostOnce` in `flow` and `expr`,
  `TestSurfacedErrorsNamePackageExactlyOnce`, and
  `TestAJoinedSuspensionNamesThePackageOncePerWait` hold each package to it.
- Decode through `jsondoc.DecodeInto`. Every exported `UnmarshalJSON` here makes
  the same promise — a nil receiver reported rather than a panic, the whole
  document decoded, and the destination replaced only after complete success —
  and eight of them once implemented it independently, where any one could have
  assigned before checking its error. Supply a `decode` returning the value and
  a `wrap` giving the boundary its own diagnostic; the definition types need the
  second because a `GraphError` carries a field the plain boundaries have no
  place for. A type whose text is identity also needs `MarshalJSON`, because
  `encoding/json` replaces invalid UTF-8 by design and a wire type must refuse
  rather than rename itself: `TestEveryIdentityBearingTypeRefusesToRenameItself`.
- Say what a caller owns when an exported result is a slice or a map. Every one
  in this repo is built fresh and a signature cannot say so, so each says it in
  one clause the way `Journal.Keys` does. Whether the values inside are the
  caller's too needs saying separately — `Store.Changes` returns a fresh slice of
  borrowed values. A `MarshalJSON` needs no clause, since `json.Marshaler`
  already hands its bytes over. A new result that stays silent reads as
  uncertainty rather than as the convention it is; naming the ones that comply
  would be a list to maintain, and it drifted before this sentence replaced it.
- Prefer standard Go contracts, explicit context propagation, and errors that
  work with `errors.Is` and `errors.As`.
- Keep distributed scheduling, durable timers, and exactly-once execution out
  of scope.

New abstraction is justified only when it removes repeated policy without
hiding control flow. Prefer one clear shape per purpose and useful zero values.

## Public API changes

Any exported change must include:

- A package comment or symbol comment that defines behavior and edge cases.
- An external-package test or executable example showing caller usage.
- Error semantics, including stable sentinels or structured errors when callers
  need to branch.
- Cancellation and concurrency semantics where applicable.
- A migration entry in [CHANGELOG.md](./CHANGELOG.md) if existing callers must
  change.

Adding a method to an exported interface is breaking. Raising the `go` directive
also raises every dependent's toolchain floor. Treat both as compatibility
decisions, not routine cleanup.

## Documentation changes

- Keep the root README focused on package choice, first use, and capability
  discovery.
- Put progressive teaching in [`docs/tutorials`](./docs/tutorials/README.md).
- Keep runnable code in [`example`](./example/README.md).
- Keep user-visible release notes and released compatibility history in the
  changelog; pre-release implementation archaeology belongs in Git history.
- Use package comments and examples for API reference.

When documentation contains code, prefer a runnable example as its source of
truth and link to it.

## Pull requests

Keep commits reviewable and avoid mixing unrelated cleanup with behavioral
changes. Explain:

- the problem and user-visible outcome;
- API and behavioral trade-offs;
- error and cancellation behavior;
- benchmark evidence for performance claims;
- migration steps for a compatibility break.

Maintainers should use the [release checklist](./docs/releasing.md) before
tagging.
