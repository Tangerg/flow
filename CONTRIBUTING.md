# Contributing

Engineering conventions that apply to every change live in [`AGENTS.md`](./AGENTS.md), and the rules unique to
this repository in [`PROJECT_RULES.md`](./PROJECT_RULES.md). This page covers only the contributor workflow:
the toolchain, the gate, and what an exported change owes.

The architecture axioms — package layout, composition protocol, execution ordering, ownership, and the guards
that pin them — are in [`PROJECT_RULES.md`](./PROJECT_RULES.md). Read them before changing behaviour or an
exported API, together with the package boundaries in the
[README](./README.md#choose-the-smallest-package).

## Requirements

- Go 1.27 or newer.
- `golangci-lint` v2 (CI currently pins v2.13.1).
- `actionlint` (CI currently pins v1.7.10).
- `govulncheck` (CI currently pins v1.6.0).
- Node.js 22 or newer when changing Markdown documentation.
- A clean module graph with no committed `replace` directives.
- Tests use the standard `testing` package.

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

The coverage floor protects behaviour without making every defensive statement a public design constraint.
Keep meaningful coverage as high as the tests naturally support, but do not add white-box tests or distort
production control flow solely to preserve an exact percentage.

Changes to the learning path should also run:

```sh
go test ./example -run Example -v
```

## Public API changes

Any exported change must include:

- A package comment or symbol comment that defines behaviour and edge cases.
- An external-package test or executable example showing caller usage.
- Error semantics, including stable sentinels or structured errors when callers need to branch.
- Cancellation and concurrency semantics where applicable.
- A migration entry in [CHANGELOG.md](./CHANGELOG.md) if existing callers must change.

Two changes are compatibility decisions rather than routine cleanup: adding a method to an exported interface
is breaking, and raising the `go` directive raises every dependent's toolchain floor.

## Documentation changes

Each kind of documentation has one home, so a reader knows where to look and a writer knows where to add.

| Document | Holds |
| --- | --- |
| [README](./README.md) | Package choice, first use, capability discovery |
| [`PROJECT_RULES.md`](./PROJECT_RULES.md) | Architecture axioms and the guards that pin them |
| [`docs/tutorials`](./docs/tutorials/README.md) | Progressive teaching |
| [`example`](./example/README.md) | Runnable code |
| [CHANGELOG.md](./CHANGELOG.md) | User-visible release notes and released compatibility history |
| Package comments and examples | API reference |

Pre-release implementation archaeology belongs in Git history, not the changelog. When documentation contains
code, prefer a runnable example as its source of truth and link to it.

## Pull requests

Keep commits reviewable and avoid mixing unrelated cleanup with behavioural changes. Explain:

- the problem and user-visible outcome;
- API and behavioural trade-offs;
- error and cancellation behaviour;
- benchmark evidence for performance claims;
- migration steps for a compatibility break.

Maintainers should use the [release checklist](./docs/releasing.md) before tagging.
