# Release checklist

This repository is a Go library. A published tag is immutable once module
proxies and checksum databases observe it, so fix a bad release by rolling
forward or retracting it; never move or reuse a tag.

## 1. Check release readiness

- Confirm the repository has an owner-approved `LICENSE`. Do not publish the
  first release until one has been selected.
- Confirm `go.mod` has no `replace` directive and does not depend on a local
  workspace.
- Confirm the `go` directive is the oldest toolchain the public API and
  implementation actually require.
- Start from a clean working tree on the intended release commit.

## 2. Choose the version

Follow Semantic Versioning:

- Before v1, document every breaking change in release notes.
- After v1, patches fix compatible behavior and minors add compatible API.
- A future v2 requires both a `v2.x.y` tag and a `/v2` module path.

If at least one release tag exists, compare the public API with the latest tag:

```sh
go run golang.org/x/exp/cmd/gorelease@latest
```

Before the first tag, skip `gorelease`; review `go doc` for every package and
record the public release shape in [CHANGELOG.md](../CHANGELOG.md). Do not turn
intermediate pre-release refactors into migrations from a version users could
not install.

If a release changes the Journal wire format, decide before tagging whether the
new decoder reads the previous format or applications must migrate persisted
documents. Test the supported path with archived Journal fixtures. This is
separate from workflow-definition compatibility, which remains an application
responsibility.

## 3. Prepare release notes

Move relevant entries from `Unreleased` in the changelog into a dated version
section. Summarize user outcomes first, then list deprecations, behavior
changes, and migrations. Update comparison links at the bottom of the file.

Do not treat deprecation as permission to remove an API after v1. A deprecated
symbol must keep working and its comment must begin with `Deprecated:` and name
the replacement.

## 4. Run the release gate

```sh
test -z "$(gofmt -l .)"
go mod tidy -diff
go build ./...
go test -race -coverprofile=coverage.out ./...
test "$(go tool cover -func=coverage.out | awk '/^total:/ { print $3 }')" = "100.0%"
go vet ./...
golangci-lint run ./...
actionlint
govulncheck ./...
npx --yes markdownlint-cli2@0.23.2
```

Also verify the documentation path:

```sh
go test ./example -run Example -v
go doc github.com/Tangerg/flow
go doc github.com/Tangerg/flow/workflow
```

Check package comments and examples as they will appear on pkg.go.dev.

## 5. Tag and publish

Commit the release metadata, then create an annotated tag on the module root:

```sh
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

On a version tag, the CI workflow creates the GitHub release only after its
formatting, module, build, race, exact-coverage, vet, Go and workflow lint,
vulnerability, documentation, and fuzz jobs all pass. Verify that:

- the GitHub release points to the intended commit;
- release notes contain the required migrations;
- a clean temporary module can resolve and test the published version;
- pkg.go.dev displays the expected package documentation.

## 6. Recover from a bad release

Do not delete or retag a version already published. Add a `retract` directive
with the reason to `go.mod`, fix the problem, and publish a new version:

```go
retract v1.4.1 // Broke Parse on empty input; use v1.4.2.
```

The next release carries the retraction so Go tooling can warn users and avoid
the affected version.
