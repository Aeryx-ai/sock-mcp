# Contributing

`sock-mcp` is the MCP server that lets an agent message through Sock as a puppet: one Go binary in `cmd/sock-mcp`, the MCP tools in `tools/`, the hub client wrapper in `sockclient/` and the channel, Codex and inbox integrations under `internal/`. It talks to the hub through [sock-client](https://github.com/Aeryx-ai/sock-client) and the generated API in [sock-contract](https://github.com/Aeryx-ai/sock-contract). The tool surface is specified in `docs/specs/tools.md`.

## Before you start

- Open an issue before anything beyond a small fix (a typo, a doc correction, an obvious bug). New tools, flags and changes to tool schemas need agreement first.
- Report vulnerabilities privately as [SECURITY.md](SECURITY.md) describes. Never in an issue, pull request or discussion.

## Dev setup

- Go 1.26.9 or newer (`go.mod`). CI reads the version from `go.mod`.
- golangci-lint v2.13.2, the version CI installs:

  ```sh
  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2
  ```

- `zip` and `tar`, only for `make dist`.

Linters beyond the defaults are set in `.golangci.yml`: `errorlint`, `intrange`, `misspell`, `revive`, `unparam`.

| Target | Does |
|---|---|
| `make` / `make build` | Builds `bin/sock-mcp` with the version from `git describe` |
| `make test` | `go test -race ./...` |
| `make install` | `go install` of `./cmd/sock-mcp` with the version stamped |
| `make fmt` | Fails if `gofmt -l .` lists any file |
| `make vet` | `go vet ./...` for the host and again with `GOOS=windows` |
| `make lint` | `golangci-lint run` |
| `make check` | `fmt`, `vet`, `lint`, `test` |
| `make dist` | Cross-builds the release archives into `bin/dist` (see below) |

`make redeploy` prints a note: there is nothing to deploy.

## Working across repos

`go.mod` pins sock-contract and sock-client to public tags and the Go module proxy fetches them. To develop against local checkouts, use an untracked `go.work` (already in `.gitignore`):

```sh
go work init .
go work use ../sock-client ../sock-contract
```

Never commit a `go.work` or a `replace` directive. CI has neither, so before opening a PR check that the change builds against the pinned tags:

```sh
GOWORK=off make check
```

If the change needs something not yet in a client or contract tag, that change ships and gets tagged upstream first. Then bump the pin in the change that first uses it:

```sh
go get github.com/aeryx-ai/sock-client@vX.Y.Z
go mod tidy
```

## Making a change

1. Fork, then branch from `main`.
2. Write the test with the code. A bug fix starts with a test that fails without the fix. Tests run with the race detector (`make test`).
3. Tool argument handling, error classification and the channel transport have fuzz tests: `tools/tools_fuzz_test.go`, `sockclient/classify_fuzz_test.go` and `internal/channel/channel_fuzz_test.go`. Extend them when you change that code and run the target you touched, one at a time:

   ```sh
   go test -run '^$' -fuzz '^FuzzToolArgs$' -fuzztime 60s ./tools
   go test -run '^$' -fuzz '^FuzzClassify$' -fuzztime 60s ./sockclient
   go test -run '^$' -fuzz '^FuzzHandle$' -fuzztime 60s ./internal/channel
   ```

   A failing input is written under the package's `testdata/fuzz/<FuzzName>/`. Fix the bug and commit that file: plain `go test` replays it from then on.
4. Run `make check`. It must pass. CI runs the same target.
5. Keep a PR to one change.

CI skips pull requests that touch only Markdown or `docs/`.

## Flags and tools are a user contract

Agents, plugins and hand written MCP configs start `sock-mcp` with flags and call its tools by name. Treat both as an API:

- Do not rename or remove a flag (`--channel`, `--outbound-only`, `--codex-inbound`, `--claude-inbound`) or a subcommand (`replied`), or change its meaning. Add a new one and keep the old one working. The Homebrew formula's test checks that `sock-mcp --help` mentions `outbound-only`.
- Tool names, argument schemas and result shapes are what agents are prompted with. Add optional arguments and result fields; do not rename, remove or retype existing ones. Update `docs/specs/tools.md` in the same PR.
- The outbound tool list (`sock_whoami`, `sock_list_conversations`, `sock_send`) is pinned byte for byte in `tools/testdata/outbound_tools.golden.json` by `TestOutboundToolsListIsPinned`. A change that fails that test is a breaking change, not a stale golden. Rewrite it only for a change agreed in an issue:

  ```sh
  go test ./tools -run TestOutboundToolsListIsPinned -update
  ```

- Never write a token or other secret to a tool result, log or stderr.

## Cross builds

`make dist` builds static (`CGO_ENABLED=0`, `-trimpath`) binaries for `darwin-arm64`, `darwin-amd64`, `linux-arm64`, `linux-amd64` and `windows-amd64` in parallel and packs each with `LICENSE` as `bin/dist/sock-mcp-<os>-<arch>.tar.gz` (`.zip` on Windows). Platform specific code goes in build constrained files such as `cmd/sock-mcp/lock_unix.go` and `lock_windows.go`. `make vet` already vets Windows; run `make dist` when you touch platform specific code. The release workflow runs the same target.

## Style

- gofmt, `go vet` and golangci-lint clean.
- `for i := range n` rather than a three clause loop for a plain count.
- Wrap errors with context. Tests next to the code they cover.

## Commit messages

- Imperative subject, short (about 50 characters, 72 at most), no trailing period: `Add a limit argument to sock_fetch`.
- A body that explains why, not what the diff already shows.
- One logical change per commit.
- Signed commits are encouraged: `git commit -S` or `git config commit.gpgsign true`. GPG and SSH keys both work.

## Pull requests

- Link the issue it resolves.
- Say what changed and why. Call out any change to flags, tool names, tool schemas or result shapes.
- Include the tests and the `docs/specs` update.
- CI must be green before review. A maintainer reviews every PR. Push review fixes as new commits; the maintainer may squash on merge.

## Releases (maintainers)

A release is a signed annotated `vX.Y.Z` tag on `main`, with the version as its message:

```sh
git tag -s v0.5.1 -m v0.5.1
git push origin v0.5.1
```

The tag runs `.github/workflows/release.yml`:

1. `gate` refuses any tag not shaped `vX.Y.Z`.
2. `build` runs `make dist`.
3. `publish` uploads the five archives to `downloads.joinsock.com` and updates `latest/mcp-<os>-<arch>.json`. A re-run reuses files already published.
4. `tap` runs `scripts/update.sh formula sock-mcp <version>` in [homebrew-tap](https://github.com/Aeryx-ai/homebrew-tap) and pushes the regenerated `Formula/sock-mcp.rb`, retrying if a sock-cli release pushed first.

Tag only a `main` that passes `make check`.

## License

MIT, copyright Aeryx Industries LLC ([LICENSE](LICENSE)). Inbound = outbound: by contributing you agree your contribution is licensed under the same terms.
