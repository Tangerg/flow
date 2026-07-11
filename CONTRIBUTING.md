# Contributing

Contributions are welcome through focused issues and pull requests.

## Development

The project requires Go 1.25 or newer.

Before submitting a change, run:

```sh
gofmt -w .
go mod tidy -diff
go test ./...
go test -race ./...
go vet ./...
golangci-lint run ./...
govulncheck ./...
```

Changes to exported APIs must include:

- package documentation and testable examples;
- external-package tests demonstrating caller usage;
- an API snapshot update when the change is intentional;
- a migration note in `CHANGELOG.md` when compatibility is affected.

## Design boundaries

- Keep `flow.Node` a single-method interface.
- Put derivable combinators and decorators in `flowx`, not the root package.
- Keep durability, distribution, and deterministic replay out of scope.
- Prefer standard-library contracts and zero third-party runtime dependencies.
- Preserve context cancellation and `errors.Is`/`errors.As` behavior.

## Pull requests

Keep commits small enough to review independently. Explain behavioral and API
tradeoffs, include benchmark evidence for performance changes, and avoid mixing
unrelated refactors with feature work.
