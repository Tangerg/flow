# Release checklist

1. Confirm the repository has an owner-approved `LICENSE`. Do not publish the
   first release until a license has been selected.
2. Choose the version according to Semantic Versioning. Before v1, call out all
   breaking changes explicitly.
3. Move relevant entries from `Unreleased` in `CHANGELOG.md` into a dated release
   section and update comparison links.
4. Verify every public API change is intentional. Once a release exists, run
   `gorelease` against the latest tag; before the first tag, review `go doc` for
   every package and record intentional breaks in `CHANGELOG.md`.
5. Run the complete local gate:

   ```sh
   gofmt -w .
   go mod tidy -diff
   go test ./...
   go test -race ./...
   go vet ./...
   golangci-lint run ./...
   govulncheck ./...
   ```

6. Verify package documentation and examples on `pkg.go.dev` formatting.
7. Commit the release metadata and create an annotated `vX.Y.Z` tag.
8. Push the tag. The release workflow reruns validation and creates GitHub
   release notes.
9. Verify installation from a clean temporary module.
