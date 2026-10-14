# sock-mcp

MCP server that lets an agent message through [sock](https://github.com/guygrigsby/sock-contract) as a puppet. Stdio transport, built on the official Go SDK.

## Setup

Create a puppet in the sock app, copy its token, and register the server with your MCP host:

    claude mcp add sock --env SOCK_TOKEN=sock_pt_... -- sock-mcp

`SOCK_URL` defaults to `https://api.sock.aeryx.ai`.

## Tools

`sock_whoami`, `sock_list_conversations`, `sock_send`, `sock_fetch`, `sock_ack`. Schemas, retry, failure and dedupe rules: [docs/specs/tools.md](docs/specs/tools.md). Design: [docs/specs/design.md](docs/specs/design.md).

## Outbound only

`sock-mcp --outbound-only` serves only `sock_whoami`, `sock_list_conversations` and `sock_send`, for an agent that runs beside `sock listen` (which owns the deliveries) or in a host that cannot filter tools. It cannot be combined with `--channel`. The server's instructions tell the model the inbound tools are absent. Details: [docs/specs/tools.md](docs/specs/tools.md).

## Claude Code channel

`sock-mcp --channel` pushes messages from your puppet's owner into the running session. Only the owner (and ids in `SOCK_CHANNEL_ALLOW`) can reach Claude this way. Needs `--dangerously-load-development-channels` during the research preview. Details: [docs/specs/tools.md](docs/specs/tools.md).

MIT licensed.
