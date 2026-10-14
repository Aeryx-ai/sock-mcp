# sock-mcp

MCP server that lets an agent message through [sock](https://github.com/guygrigsby/sock-contract) as a puppet. Stdio transport, built on the official Go SDK.

## Setup

Create a puppet in the sock app, copy its token, and register the server with your MCP host:

    claude mcp add sock --env SOCK_TOKEN=sock_pt_... -- sock-mcp

`SOCK_URL` defaults to `https://api.sock.aeryx.ai`.

## Tools

`sock_whoami`, `sock_list_conversations`, `sock_send`, `sock_fetch`, `sock_ack`. Schemas, retry, failure and dedupe rules: [docs/specs/tools.md](docs/specs/tools.md). Design: [docs/specs/design.md](docs/specs/design.md).

## Messaging the owner

`sock_send` without `conversation_id` (NotifyOwner) is refused with a `permanent:` error and nothing reaches the hub, unless this session is the owner's digest channel. `sock_send` with a `conversation_id` always goes through, the owner's direct conversation included: a model that finds that conversation with `sock_list_conversations` can post into it from any session. The control there is the hub's rate limit on puppet posts into the owner's direct (ADR 20), per puppet and per owner, which answers `RATE_LIMITED` with `Retry-After`.

`SOCK_MCP_DIGEST_CHANNEL=1`: this session is the owner's digest channel; set only in an operator-configured session. Only exactly `1` counts. `sock-mcp` reads it once at start from its own environment and removes it, so no child process inherits it; no tool argument, message text or config file field turns it on. Set it in that one server's own `env` block, not in the agent's environment, where every session the agent starts would inherit it.

No session signal stands in for it, because none is positive and unforgeable. In Claude Code 2.1.299 a stdio MCP server never sees `CLAUDE_CODE_SESSION_ATTENDED`, the hooks' signal. A plugin `.mcp.json` `env` of `${CLAUDE_CODE_SESSION_ATTENDED}` expands from Claude Code's own process environment, not the session's attended state: unset in a top-level terminal session, and `1` in a `claude -p` started from an attended session's shell, which it inherits (probed live). `CLAUDE_CODE_ENTRYPOINT=cli` is also what a background job sees. Codex passes no session signal at all. So the owner hears from a session through a reply, or through the sock plugin's hooks, which ask the owner only when a person is at the session.

Not enforced here: a `Send` into the owner's direct conversation is the same post as NotifyOwner at the hub, and sock-mcp does not check that the owner wrote there. A model that looks up that conversation (`sock_whoami`, `sock_list_conversations`) can still reach the owner unprompted. The refusal and the `sock_send` description point only at replying to a received message; closing the gap needs the hub to refuse it.

## Configuration

`SOCK_TOKEN`, `SOCK_URL` and `SOCK_ACCESS_GATE` from the environment. When `SOCK_TOKEN` is unset (a host such as a Codex plugin starts the server without your environment), `sock-mcp` reads the `sock` CLI's config file, `$XDG_CONFIG_HOME/sock/config.toml` or `~/.config/sock/config.toml` (mode 600): `machine_key` or `puppet_token`, `url` and `access_gate`. `sock machine-key use` writes `machine_key` (see [Machine key](#machine-key)); `sock puppets use <puppet id>` mints a puppet token and writes `puppet_token`, one puppet shared by every project. The token is the first of `SOCK_TOKEN`, then `machine_key`, then `puppet_token`. The `token` that `sock login` writes is yours and is never used here. One source, never a mix: with the token from the file, the URL and gate come from the file too and an ambient `SOCK_URL` or `SOCK_ACCESS_GATE` is ignored, so the token only goes to the URL it was saved with.

## Machine key

With a machine key (`sock_mk_…`, from `sock machine-key create`) as `SOCK_TOKEN`, or as `machine_key` in the config file (`sock machine-key create --label <host> | sock machine-key use`), `sock-mcp` makes and caches one puppet per project ([ADR 10](https://github.com/guygrigsby/sock-contract/blob/main/docs/adr/0010-machine-keys-mint-project-puppets.md)) and runs as it: the key only reaches the hub to make, list and erase the puppets it made. The project is the repository holding `CLAUDE_PROJECT_DIR` (else the working directory). Without `CLAUDE_PROJECT_DIR`, a working directory outside any git repository or inside `PLUGIN_ROOT` names no project: a Codex plugin server starts in the plugin cache, so `sock-mcp` mints nothing there and, with the machine key from the file, uses the file's `puppet_token` (one puppet for every project), or fails without one. A machine key from `SOCK_TOKEN` there fails: the file is never mixed in. Register `sock-mcp` in Codex's `config.toml` (`[mcp_servers.sock]`), which starts it in the project, for a puppet per project. Every worktree of a repository shares one puppet, named `<project> on <host>`. Tokens are cached under `${XDG_STATE_HOME:-~/.local/state}/sock-integrations/puppets/` (mode 600) and puppets of repositories that no longer exist are erased at start, at most once every 6 hours per cache.

## Outbound only

`sock-mcp --outbound-only` serves only `sock_whoami`, `sock_list_conversations` and `sock_send`, for an agent that runs beside `sock listen` (which owns the deliveries) or in a host that cannot filter tools. It cannot be combined with `--channel`. The server's instructions tell the model the inbound tools are absent. Details: [docs/specs/tools.md](docs/specs/tools.md).

With `--channel` the server offers MCP protocol versions only up to 2025-11-25. Claude Code 2.1.292 and later negotiates the newest revision a stdio server offers, and a channel server on 2026-07-28 is not registered as a channel (no delivery path); the earlier handshake is. The cap lifts when Claude Code's changelog adds channel delivery on 2026-07-28.

## Claude Code channel

`sock-mcp --channel` pushes messages from your puppet's owner into the running session. Only the owner (and ids in `SOCK_CHANNEL_ALLOW`) can reach Claude this way. Needs `--dangerously-load-development-channels` during the research preview. Details: [docs/specs/tools.md](docs/specs/tools.md).

A push never acknowledges a message. Claude Code drops channel notifications silently in a session started without the channel flags, and the `initialize` it sends is byte for byte the same with and without them (Claude Code 2.1.300, probed live), so sock-mcp cannot tell whether anything was shown. The model's own action acknowledges: a `sock_send` reply acknowledges the messages it was shown in that conversation, and `sock_ack` the ones that need no reply. Until then the message stays queued at the hub for a session that shows it, or for `sock_fetch`.

A message is pushed at most 5 times without being acted on, counted across restarts in `${XDG_STATE_HOME:-~/.local/state}/sock-mcp/pushes/` (mode 700, one 600 file per message, removed on ack and after the hub's 30 day retention; an unreadable count counts as spent). After that it is only reachable with `sock_fetch`. `sock_whoami` reports `pending_unacked`.

A puppet whose sessions never act keeps every message queued until the hub deletes it 30 days after it was sent. The hub's `Subscribe` and `FetchPending` read only the 500 oldest pending messages, so once 500 sit unacknowledged a newer one, the owner's next reply included, reaches no session until older ones are acknowledged or expire. Clear a backlog with `sock_fetch` then `sock_ack`.

MIT licensed.
