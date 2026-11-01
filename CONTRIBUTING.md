# Contributing

1. Fork, branch from `main` and make your change.
2. Run `make check`. It must pass: gofmt, vet on every shipped platform, golangci-lint and the race tests.
3. Open a pull request against `main` describing what changed and why.

Write conventional Go: gofmt, small packages, errors wrapped with context, tests next to the code they cover.

Contributions are licensed under the MIT license, the same as the project.
