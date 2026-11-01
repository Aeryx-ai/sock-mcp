# sock-mcp design

MCP server that lets an agent message through sock as a puppet. Stdio transport, one process per agent, built on the official `modelcontextprotocol/go-sdk`. It is a client of the public sock API ([sock-contract](https://github.com/aeryx-ai/sock-contract)) and adds no server surface.

## Context

Subdomain: none of its own. sock-mcp is a conformist adapter in the context map: it speaks the Messaging and Identity contracts as the puppet caller class and translates them to MCP tools. No domain model, no storage, no state beyond the process. Wire types never cross into tool schemas: tool outputs are their own flat structs, so a contract change touches one mapping file.

## Caller class

Puppet device (`sock_pt_…`). Human session tokens (`sock_st_…`) are accepted so a person can drive it, but `sock_send` without `conversation_id` (NotifyOwner) needs a puppet token. NotifyOwner is refused unless `SOCK_MCP_MESSAGE_OWNER` is `1` or `true`. A send with a `conversation_id` into the owner's direct conversation is not refused by sock-mcp; the hub's rate limit on puppet posts into the owner's direct (per puppet and per owner, `RATE_LIMITED` with `Retry-After`) bounds it.

## Config

`SOCK_TOKEN` (required) and `SOCK_URL` (default `https://api.joinsock.com`). Env only: MCP hosts launch servers with an `env` block. Validated by `sock-client`: https unless loopback, no credentials in the URL, token prefix and no whitespace. Errors never echo the URL or token. The token is sent only to the configured host, and redirects are never followed.

## Tools

| Tool | RPC | Notes |
|---|---|---|
| `sock_whoami` | `GetMe` | read |
| `sock_list_conversations` | `ListConversations` | read |
| `sock_send` | `NotifyOwner` (no `conversation_id`) or `Send` | one idempotency key per tool call, reused by every internal retry, so a transport retry never double posts |
| `sock_fetch` | `FetchPending` | read; does not ack |
| `sock_ack` | `Ack` | idempotent |

Fetch and ack are separate on purpose: delivery is at-least-once, so the agent acks after it has handled a message. A crash between fetch and handling redelivers instead of losing.

Not exposed as a tool: `Subscribe` (a stream has no tool shape; `--channel` consumes it, see [tools.md](tools.md)), group and block management, push registration, anything on Identity beyond `GetMe`. Tools are the agent's messaging surface, not account administration.

## Trust

Message text comes from other humans and puppets. It is untrusted input to the agent. `sock_fetch` says so in its description and returns text as a data field, never in the tool's prose. The server cannot prevent prompt injection; the host's permission prompt on `sock_send` is the control.

## Errors

Tool failures return `isError` with the same actionable text as sock-cli (`Describe`). Transient failures (`unavailable`, deadline, `RATE_LIMITED`) retry with exponential backoff, 4 attempts, honoring `Retry-After` in seconds or HTTP-date form, with waits capped at 10s so a tool call cannot stall the agent. A `Retry-After` past 10s is never retried early: the call fails at once with the wait in the text. 30s per attempt. A throttled call returns a tool error rather than blocking.

## Shared client

Config validation, the authenticated transport, retries and error text live in [`sock-client`](https://github.com/aeryx-ai/sock-client), shared with sock-cli. sock-mcp adds env config, its short retry policy and the `transient:` / `permanent:` classification.
