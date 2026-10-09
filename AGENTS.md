# kal-sse

Resilient Server-Sent Events: snapshot + cursor data channels with
gap-free reconnect.

## Purpose

This crate is the **single sanctioned implementation** of the
resilient SSE contract. Any project that needs an SSE endpoint with
reliable reconnect MUST go through `kal-sse` rather than hand-rolling
`Sse::new(...)` over a bare broadcast channel. Ad-hoc SSE silently
drops events for any client that reconnects after a transient blip;
this crate exists so that never happens.

## The contract

A *data channel* = REST snapshot + SSE stream. One `sub_id` identifies
one logical subscription to one data flow, not a tab/window/app.

- **REST snapshot = subscription birth/reset.** The snapshot endpoint
  takes an optional client-proposed `sub_id` (UUID-shaped): `None` →
  the server mints one; `Some(id)` → the server adopts it AND
  supersedes any prior handle for that id (invalidating the prior
  subscription to this data flow). `register(sub_id)` or
  `register_at(sub_id, now)` records the current `last_index` as the
  snapshot base and sets its cursor to that base. TTL runs from
  registration until the first attach.
- **Stream resume**: `GET <stream>?sub=<uuid>[&last_event_id=<n>]` or
  `Last-Event-ID`. The header wins. The server caps the last applied
  id at the server cursor; an id below the snapshot base gets `409
  needs_snapshot` (that client never saw the current snapshot, e.g. a
  superseded holder of the same id). Without an id, the server cursor
  is the resume point. Every event carries `id: <index>`. A `sub_id`
  belongs to one client at a time.
- **Delivery-driven upper bound**: the server cursor advances when an
  event is handed to the HTTP stack. A socket write does not prove the
  client application applied it, so the client reconnects with its last
  applied id.
- **Redelivery window**: sent events stay replayable for
  `DEFAULT_REDELIVERY_WINDOW` after being sent (60 s, four 15 s keep-alives),
  configured with `with_redelivery_window`, subject to TTL and `max_events`. A resume gets
  a full replay or `409 needs_snapshot`, never a silent gap.
- **TTL (set by the caller of `EventLog::new`, typically 15 min)**: an
  unattached handle is retained from registration until first attach and
  after disconnect. Attached streams hold their handles indefinitely.
- **`409 needs_snapshot`**: returned when the `sub_id` is unknown
  (never registered / TTL-reclaimed / post-restart) or its cursor fell
  out of the retained buffer. Unknown handles are `AttachError::UnknownSub`;
  unavailable backlogs are `AttachError::Expired` and include earliest
  and current indexes. The client re-takes the REST snapshot.
- **Epoch / supersede**: re-attaching or re-registering a `sub_id`
  allocates a log-wide epoch, never reused during the log's lifetime,
  even after handle reclamation; the superseded stream's `advance`/`disconnect`
  become no-ops and `is_current` reports false. The superseded live
  stream ends when its next event observes that state. At most one live
  attach per `sub_id` is authoritative.

## Architecture

- `src/core.rs` — framework-agnostic `EventLog<E>`: the indexed log,
  the `sub_id` registry, `register` / `attach` / `advance` /
  `disconnect` / `is_current` / `compact`, `new_sub_id`, and
  `AttachError`. The "intelligent buffer" lives here: retention =
  events strictly above the minimum resume floor of all live and within-TTL
  handles. Each floor is `max(snapshot base, min(cursor, lowest cursor
  before a send batch still within the redelivery window))`; without recent
  sends it is the cursor. The window starts at HTTP hand-off, not append.
  Unattached unresumable handles are forgotten using that same floor;
  `max_events` is a hard backstop.
- `src/axum.rs` (feature `axum`) — the HTTP adapter: `last_event_id`,
  `sse_response_from` (capped resume id, replay-then-attach,
  advance-on-yield, epoch-stop-on-supersede, disconnect-on-end),
  `sse_response` for server-cursor-only compatibility, the `409
  needs_snapshot` body, and wire-name constants.

The core MUST NOT depend on axum, serde, or any HTTP type. Keep the
adapter the only place that knows about the wire.

## Key invariants (do not break)

1. `attach` subscribes to the broadcast FIRST, then reads the backlog,
   both under the same lock — so no event slips between the last
   replayed index and the first live one.
2. The cursor advances on hand-off to the HTTP stack and is only an
   upper bound on what the client applied. A connected subscription is
   never TTL-expired mid-stream; recently sent events remain replayable
   for the redelivery window after being sent, subject to TTL and the cap.
3. Reconnect with the same `sub_id` REFRESHES the existing handle in
   place — handles never accumulate across reconnects.
4. The adapter's drop guard calls `disconnect` however a stream ends,
   including a response body dropped after a failed write.
5. Register and attach use the same log-wide epoch allocator. Never derive
   epochs from a handle: a forgotten id must not revive a superseded stream.

## Build / Test

```bash
cargo test                  # core only (default features)
cargo test --features axum  # core + axum adapter
cargo clippy --all-targets --all-features
cargo fmt
```

## Post-Implementation Checklist

1. `cargo fmt`
2. `cargo clippy --all-targets --all-features`
3. `cargo test --features axum`
