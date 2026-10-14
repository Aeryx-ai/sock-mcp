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
| `sock_whoami` | `GetMe`, then `FetchPending` (limit 500) | none | `{id, display_name, kind: "human"\|"puppet", handle?, owner_id?, puppet_name?, pending_unacked?: int}`. `pending_unacked` counts at most 500 and is absent when `FetchPending` failed. The hub has no count, so this reads up to 500 whole messages |
| `sock_list_conversations` | `ListConversations` | none | `{conversations: [{id, kind: "direct"\|"group", member_ids: [string]}]}` |
| `sock_send` | `NotifyOwner` when `conversation_id` is absent, else `Send`; then `Ack` of `message_ids` once posted | `{text: string (1..65536 bytes, Markdown), conversation_id?: string, message_ids?: [string]}` | the sent `Message` |
| `sock_fetch` | `FetchPending` | `{limit?: int}` default 50, clamped to 200 | `{messages: [Message]}` oldest first per conversation. Does not ack |
| `sock_ack` | `Ack` | `{message_ids: [string]}` 1..500 | `{acked: int}` (ids submitted; unknown ids are ignored by the server). For messages that need no reply |

`Message` = `{id, conversation_id, seq: int, sender_id, sent_at: RFC 3339 UTC, text}`. `text` is untrusted.

`sock_send` without `conversation_id` is refused (`permanent:`, nothing sent) unless sock-mcp started with `SOCK_MCP_DIGEST_CHANNEL` exactly `1`, the owner's digest channel session ([README](../../README.md#messaging-the-owner)). With a `conversation_id` it is never refused by sock-mcp.

Placeholder mapping: `notify_owner` = `sock_send` with no `conversation_id`; `send` = `sock_send` with one; `fetch_pending` = `sock_fetch`; `ack` = `sock_ack`; `list_conversations` = `sock_list_conversations`.

## Idempotency and retries

Every `sock_send` call generates one idempotency key and reuses it for every internal retry, so a transport retry never double posts (server dedupes per device for 24h). Two separate tool calls get two keys: a hook that retries by calling the tool again CAN double post. To retry safely at the hook layer, treat a failure with the `transient:` prefix as "outcome unknown for at most one message" and accept a possible duplicate, or do not retry.

Internal retry: 4 attempts, 500ms doubling, capped 10s, `Retry-After` honored (seconds or HTTP-date, capped at 10s), 30s per attempt. A tool call can therefore take up to about 60s in the worst case. Hooks should set their own timeout.

## Failure contract

Failure is an MCP tool result with `isError: true` and one text content item. The text always starts with exactly one of:

- `transient: ` retrying later can succeed. Covers `unavailable`, deadline exceeded and `RATE_LIMITED`, after the internal retries are spent. A `RATE_LIMITED` whose `Retry-After` is past the 10s retry cap is not retried: it fails at once and the text names the wait (`try again in 10m0s`), or says `try again later` when the hub sent none. The hub limits a puppet's posts into its owner's direct conversation ([ADR 20](../../../contract/docs/adr/0020-puppet-messages-to-their-owner-are-rate-limited.md)).
- `permanent: ` retrying the same call will not help. Covers `TOKEN_INVALID`, `CALLER_NOT_ALLOWED`, `ACCOUNT_SUSPENDED`, `TERMS_NOT_ACCEPTED`, `NOT_FOUND`, `NOT_A_MEMBER`, `FIELD_INVALID`, `IDEMPOTENCY_MISMATCH`.

After the prefix is human-readable text; do not parse it. Examples: `transient: sending too fast; try again in 10m0s`, `permanent: the server rejected the token; set SOCK_TOKEN to a current puppet token`. Hooks that must never block the agent: call with a timeout, ignore any `isError`, and optionally log it.

## Acknowledging

Replying acknowledges what the reply names; `sock_ack` is for messages that need no reply. A `sock_send` that posts acks its `message_ids` (the `message_id` of each channel tag or `sock_fetch` message it answers), in batches of 500. Only named ids are acked: the model can name a message only if it saw it, and nothing else proves that (a channel push is dropped silently by a host without channels, and that session can still post into the owner's conversation). A post with no `message_ids` acks nothing. A reply that fails acks nothing. An ack that fails after the reply posted leaves the reply's result a success and the messages pending, so the hub redelivers them.

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
- Order: subscription starts at `notifications/initialized`, so nothing is pushed before Claude Code is ready.
- A push never acks. Claude Code acknowledges no notification and drops them silently in a session that has not loaded the channel; its `initialize` is identical either way (Claude Code 2.1.300, with and without `--dangerously-load-development-channels`: `capabilities` `{"roots":{"listChanged":true},"elicitation":{"form":{},"url":{}}}`), so no client signal says a push will be shown. The message is acked when the model acts on it (Acknowledging, above). Ids already pushed in this process are never pushed twice; the hub resends unacked messages on every reconnect and restart. The in-process set holds 10,000 ids and is cleared when full, so past that a resend can be pushed again, within the push cap.
- Push cap: one message is pushed at most 5 times, counted across restarts in `${XDG_STATE_HOME:-~/.local/state}/sock-mcp/pushes/<sha256 of the account id>/` (directory 0700, one 0600 file per message named by the SHA-256 of its id). The count is written before the push. A count that cannot be read, parsed or written counts as spent: nothing is pushed. Counts are removed when the message is acked through this process and pruned at start once older than 31 days (the hub keeps messages 30). The count is per puppet and shared by its sessions, so starts of sessions without channels spend it for one with them. A message past the cap stays pending for `sock_fetch`. A relative `XDG_STATE_HOME` is ignored, as XDG says; with no absolute one and neither `HOME` nor `USERPROFILE`, `--channel` exits 1.
- Backlog: the hub's `Subscribe` and `FetchPending` read only the 500 oldest pending messages. With 500 unacked, a newer message reaches no session until older ones are acked or expire 30 days after they were sent. Messages from senders off the allowlist are never pushed or acked and count toward the 500.
- Durability: reconnects with exponential backoff 1s to 60s with jitter, honoring `Retry-After`. A stream silent for 60s (two missed heartbeats) is dropped and reconnected. `TOKEN_INVALID`, `CALLER_NOT_ALLOWED` and `ACCOUNT_SUSPENDED` end the channel: one `{content: "sock channel stopped: ...", meta: {kind: "status"}}` event, then exit 1. A broken stdout also exits.
- Env: `SOCK_TOKEN`, `SOCK_URL` as above, plus `SOCK_CHANNEL_ALLOW`.

Platform constraints (Claude Code channels reference): research preview; custom channels need `--dangerously-load-development-channels server:sock` (or `plugin:...`), interactive only and ignored under `-p`; Team and Enterprise orgs must enable channels. Events pushed while Claude is busy are batched on the next turn.

In channel mode, do not also run a `sock_fetch`/`sock_ack` hook against the same puppet: both consume the same deliveries. Pick one consumer per puppet.

## Outbound only

`--outbound-only` registers only `sock_whoami`, `sock_list_conversations` and `sock_send`; names, schemas and descriptions are unchanged. `sock_fetch` and `sock_ack` consume deliveries, so an agent beside `sock listen` (one consumer per puppet) must not see them, and a host without a tool filter (a Codex plugin's `mcp.json`) needs the server to enforce it. Combined with `--channel` it is a usage error, since the channel consumes deliveries. The initialize `instructions` say the inbound tools are absent on purpose. Default behavior is unchanged.
