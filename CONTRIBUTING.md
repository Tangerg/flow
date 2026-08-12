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
  `workflow.DefaultInput`.
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
