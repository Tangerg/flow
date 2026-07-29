# Security policy

## Supported versions

Before v1, security fixes are applied to the latest released minor version and
the active development branch. After v1, this document will list the exact
supported release lines.

## Report a vulnerability

Do not open a public issue or pull request for a suspected vulnerability. Use
[GitHub private vulnerability reporting](https://github.com/Tangerg/flow/security/advisories/new)
and include:

- the affected package and version or commit;
- a minimal reproduction or proof of concept;
- the expected confidentiality, integrity, or availability impact;
- any known preconditions or mitigations;
- a safe way to contact the reporter.

Reports will be acknowledged as soon as practical. Please keep details private
until a fix and coordinated disclosure are ready.

## Scope

Security reports may cover the Go packages, embedded workflow JSON Schemas,
GitHub Actions, or release artifacts. General bugs and feature requests belong
in the public issue tracker unless disclosure would expose a vulnerability.
