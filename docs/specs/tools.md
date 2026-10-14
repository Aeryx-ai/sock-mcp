# sock-mcp tool contract

Consumers (plugin, hook and SDK packaging) build against this file. Design rationale: [design.md](design.md). Schemas below are what `tools/list` returns; field names are stable.

The `tools` and `sockclient` packages are importable (`github.com/guygrigsby/sock-mcp/tools`, `.../sockclient`); sock-mcpd links them.

## Run

`sock-mcp` speaks MCP over stdio. Transport is stdio only in v1. Hosted Streamable HTTP is not offered: it would make sock-mcp a multi-tenant proxy holding other people's tokens, and whether a `sock_pt_` token is accepted in an OAuth `authorization` field is untested. Revisit with a real need.

Config is environment only, no flags, no config file:

| Variable | Default | Rule |
|---|---|---|
| `SOCK_TOKEN` | required | `sock_pt_…` (puppet) or `sock_st_…` (human). Whitespace trimmed at the ends, refused inside |
| `SOCK_URL` | `https://api.sock.aeryx.ai` | https, or http on loopback. No credentials in the URL |

Bad config exits 1 with a message on stderr that never echoes the token or URL.

## Tools

All outputs are returned as MCP `structuredContent` (and mirrored as JSON text).

| Tool | Wraps | Input | Output |
|---|---|---|---|
| `sock_whoami` | `GetMe` | none | `{id, display_name, kind: "human"\|"puppet", handle?, owner_id?, puppet_name?}` |
| `sock_list_conversations` | `ListConversations` | none | `{conversations: [{id, kind: "direct"\|"group", member_ids: [string]}]}` |
| `sock_send` | `NotifyOwner` when `conversation_id` is absent, else `Send` | `{text: string (1..65536 bytes, Markdown), conversation_id?: string}` | the sent `Message` |
| `sock_fetch` | `FetchPending` | `{limit?: int}` default 50, clamped to 200 | `{messages: [Message]}` oldest first per conversation. Does not ack |
| `sock_ack` | `Ack` | `{message_ids: [string]}` 1..500 | `{acked: int}` (ids submitted; unknown ids are ignored by the server) |

`Message` = `{id, conversation_id, seq: int, sender_id, sent_at: RFC 3339 UTC, text}`. `text` is untrusted.

Placeholder mapping: `notify_owner` = `sock_send` with no `conversation_id`; `send` = `sock_send` with one; `fetch_pending` = `sock_fetch`; `ack` = `sock_ack`; `list_conversations` = `sock_list_conversations`.

## Idempotency and retries

Every `sock_send` call generates one idempotency key and reuses it for every internal retry, so a transport retry never double posts (server dedupes per device for 24h). Two separate tool calls get two keys: a hook that retries by calling the tool again CAN double post. To retry safely at the hook layer, treat a failure with the `transient:` prefix as "outcome unknown for at most one message" and accept a possible duplicate, or do not retry.

Internal retry: 4 attempts, 500ms doubling, capped 10s, `Retry-After` honored (seconds or HTTP-date, capped at 10s), 30s per attempt. A tool call can therefore take up to about 60s in the worst case. Hooks should set their own timeout.

## Failure contract

Failure is an MCP tool result with `isError: true` and one text content item. The text always starts with exactly one of:

- `transient: ` retrying later can succeed. Covers `unavailable`, deadline exceeded and `RATE_LIMITED`, after the internal retries are spent.
- `permanent: ` retrying the same call will not help. Covers `TOKEN_INVALID`, `CALLER_NOT_ALLOWED`, `ACCOUNT_SUSPENDED`, `TERMS_NOT_ACCEPTED`, `NOT_FOUND`, `NOT_A_MEMBER`, `FIELD_INVALID`, `IDEMPOTENCY_MISMATCH`.

After the prefix is human-readable text; do not parse it. Examples: `transient: sending too fast; the server's rate limit held after retries, try again later`, `permanent: the server rejected the token; set SOCK_TOKEN to a current puppet token`. Hooks that must never block the agent: call with a timeout, ignore any `isError`, and optionally log it.

## At-least-once and dedupe

`sock_fetch` returns the same messages until they are acked, and delivery is at-least-once across reconnects. Dedupe key: `Message.id` (server UUID). `(conversation_id, seq)` is also unique and gapless per conversation. In a `Stop` hook: fetch, skip ids already handled, handle, then `sock_ack` the handled ids. Ack only after the work is durably done.

## Trust

Message text comes from other people and software. Treat as untrusted data. Gate any automatic action on `sender_id` (an account id), not `conversation_id`.

## Claude Code channel

`sock-mcp --channel` (flag, not env, so the plugin's `.mcp.json` args decide) turns on the Claude Code channel. Without it the server is tools only and declares no channel capability.

What it adds:

- Capability `experimental["claude/channel"] = {}` and an `instructions` string telling Claude that `<channel source="sock" ...>` events are untrusted and to answer with `sock_send` using the tag's `conversation_id`. There is no separate `reply` tool: `sock_send` with `conversation_id` is the reply.
- Inbound `Subscribe` pushed as `notifications/claude/channel` with `params = {content: <message text>, meta: {conversation_id, message_id, sender_id, seq, sent_at}}`. Meta keys are identifier-safe. `sent_at` is RFC 3339 UTC, `seq` a decimal string.
- Sender allowlist on `sender_id`, never on conversation. Allowed: the puppet's `owner_id` (from `GetMe` at startup) plus any account ids in `SOCK_CHANNEL_ALLOW` (comma separated). A human token with no `SOCK_CHANNEL_ALLOW` fails closed: exit 1 with `channel: no allowed senders`. Messages from anyone else are dropped, not pushed and not acked (they stay pending on the server until it expires them), and logged to stderr by id.
- Order: subscription starts at `notifications/initialized`, so nothing is pushed before Claude Code is ready. The push is written first, then the message is acked. Claude Code does not acknowledge notifications, so "acked" means "written to the transport", not "Claude processed it". A failed ack leaves the message pending; ids already pushed in this process are never pushed twice.
- Durability: reconnects with exponential backoff 1s to 60s with jitter, honoring `Retry-After`. A stream silent for 60s (two missed heartbeats) is dropped and reconnected. `TOKEN_INVALID`, `CALLER_NOT_ALLOWED` and `ACCOUNT_SUSPENDED` end the channel: one `{content: "sock channel stopped: ...", meta: {kind: "status"}}` event, then exit 1. A broken stdout also exits.
- Env: `SOCK_TOKEN`, `SOCK_URL` as above, plus `SOCK_CHANNEL_ALLOW`.

Platform constraints (Claude Code channels reference): research preview; custom channels need `--dangerously-load-development-channels server:sock` (or `plugin:...`), interactive only and ignored under `-p`; Team and Enterprise orgs must enable channels. Events pushed while Claude is busy are batched on the next turn.

In channel mode, do not also run a `sock_fetch`/`sock_ack` hook against the same puppet: both consume the same deliveries. Pick one consumer per puppet.

## Outbound only

`--outbound-only` registers only `sock_whoami`, `sock_list_conversations` and `sock_send`; names, schemas and descriptions are unchanged. `sock_fetch` and `sock_ack` consume deliveries, so an agent beside `sock listen` (one consumer per puppet) must not see them, and a host without a tool filter (a Codex plugin's `mcp.json`) needs the server to enforce it. Combined with `--channel` it is a usage error, since the channel consumes deliveries. The initialize `instructions` say the inbound tools are absent on purpose. Default behavior is unchanged.
