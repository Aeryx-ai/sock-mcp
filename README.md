# sock-mcp

MCP server that lets an agent message through [sock](https://github.com/guygrigsby/sock-contract) as a puppet. Stdio transport, built on the official Go SDK.

## Setup

Create a puppet in the sock app, copy its token, and register the server with your MCP host:

    claude mcp add sock --env SOCK_TOKEN=sock_pt_... -- sock-mcp

`SOCK_URL` defaults to `https://api.sock.aeryx.ai`.

## Tools

`sock_whoami`, `sock_list_conversations`, `sock_send`, `sock_fetch`, `sock_ack`. Schemas, retry, failure and dedupe rules: [docs/specs/tools.md](docs/specs/tools.md). Design: [docs/specs/design.md](docs/specs/design.md).

## Configuration

`SOCK_TOKEN`, `SOCK_URL` and `SOCK_ACCESS_GATE` from the environment. When `SOCK_TOKEN` is unset (a host such as a Codex plugin starts the server without your environment), `sock-mcp` reads the `sock` CLI's config file, `$XDG_CONFIG_HOME/sock/config.toml` or `~/.config/sock/config.toml` (mode 600): `machine_key` or `puppet_token`, `url` and `access_gate`. `sock machine-key use` writes `machine_key` (see [Machine key](#machine-key)); `sock puppets use <puppet id>` mints a puppet token and writes `puppet_token`, one puppet shared by every project. The token is the first of `SOCK_TOKEN`, then `machine_key`, then `puppet_token`. The `token` that `sock login` writes is yours and is never used here. One source, never a mix: with the token from the file, the URL and gate come from the file too and an ambient `SOCK_URL` or `SOCK_ACCESS_GATE` is ignored, so the token only goes to the URL it was saved with.

## Machine key

With a machine key (`sock_mk_…`, from `sock machine-key create`) as `SOCK_TOKEN`, or as `machine_key` in the config file (`sock machine-key create --label <host> | sock machine-key use`), `sock-mcp` makes and caches one puppet per project ([ADR 10](https://github.com/guygrigsby/sock-contract/blob/main/docs/adr/0010-machine-keys-mint-project-puppets.md)) and runs as it: the key only reaches the hub to make, list and erase the puppets it made. The project is the repository holding `CLAUDE_PROJECT_DIR` (else the working directory). Without `CLAUDE_PROJECT_DIR`, a working directory outside any git repository or inside `PLUGIN_ROOT` names no project: a Codex plugin server starts in the plugin cache, so `sock-mcp` mints nothing there and uses the file's `puppet_token` (one puppet for every project), or fails without one. Register `sock-mcp` in Codex's `config.toml` (`[mcp_servers.sock]`), which starts it in the project, for a puppet per project. Every worktree of a repository shares one puppet, named `<project> on <host>`. Tokens are cached under `${XDG_STATE_HOME:-~/.local/state}/sock-integrations/puppets/` (mode 600) and puppets of repositories that no longer exist are erased at start.

## Outbound only

`sock-mcp --outbound-only` serves only `sock_whoami`, `sock_list_conversations` and `sock_send`, for an agent that runs beside `sock listen` (which owns the deliveries) or in a host that cannot filter tools. It cannot be combined with `--channel`. The server's instructions tell the model the inbound tools are absent. Details: [docs/specs/tools.md](docs/specs/tools.md).

With `--channel` the server offers MCP protocol versions only up to 2025-11-25. Claude Code 2.1.292 and later negotiates the newest revision a stdio server offers, and a channel server on 2026-07-28 is not registered as a channel (no delivery path); the earlier handshake is. The cap lifts when Claude Code's changelog adds channel delivery on 2026-07-28.

## Claude Code channel

`sock-mcp --channel` pushes messages from your puppet's owner into the running session. Only the owner (and ids in `SOCK_CHANNEL_ALLOW`) can reach Claude this way. Needs `--dangerously-load-development-channels` during the research preview. Details: [docs/specs/tools.md](docs/specs/tools.md).

MIT licensed.
