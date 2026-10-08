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

A *data channel* = REST snapshot + SSE stream. The cursor lives ONLY
on the server, keyed by subscription id.

- **REST snapshot = subscription birth/reset.** The snapshot endpoint
  takes an optional client-proposed `sub_id` (UUID-shaped): `None` →
  the server mints one; `Some(id)` → the server adopts it AND
  supersedes any prior handle for that id (invalidating the prior
  subscription to this data flow). `register(sub_id)` returns the
  effective id and pins its cursor at the current `last_index`.
- **`sub_id`**: one per logical subscription to one data flow — NOT
  per tab/window/app. The SSE request carries ONLY `?sub=<uuid>`,
  never an index.
- **Delivery-driven cursor**: sending the `N -> N+1` event IS the
  update. The server advances the subscription's cursor on each
  delivered event — no ack, no heartbeat. The client never tells the
  server where it is; the server knows.
- **TTL (set by the caller of `EventLog::new`, typically 15 min)**: a
  dropped subscription's resume point is held for the TTL so a
  reconnect (same `sub_id`, no index) resumes gap-free; after that the
  handle is reclaimed. The TTL also runs from `register` until the
  first `attach`, so a snapshot whose stream never connects does not
  leak its handle. Only an attached stream holds its handle
  indefinitely.
- **`409 needs_snapshot`**: returned when the `sub_id` is unknown
  (never registered / TTL-reclaimed / post-restart) or its cursor fell
  out of the retained buffer. The client re-takes the REST snapshot.
- **Epoch / supersede**: re-attaching or re-registering a `sub_id`
  bumps an epoch; the superseded stream's `advance`/`disconnect`
  become no-ops and `is_current` reports false so the adapter ends it.
  At most one live attach per `sub_id` is authoritative.

## Architecture

- `src/core.rs` — framework-agnostic `EventLog<E>`: the indexed log,
  the `sub_id` registry, `register` / `attach` / `advance` /
  `disconnect` / `is_current` / `compact`, `new_sub_id`, and
  `AttachError`. The "intelligent buffer" lives here: retention =
  events after `min(cursor over all live + within-TTL handles)`, plus
  a `max_events` hard backstop.
- `src/axum.rs` (feature `axum`) — the HTTP adapter: `sse_response`
  (look up cursor by `sub_id`, replay-then-attach, advance-on-yield,
  epoch-stop-on-supersede, disconnect-on-end), the `409 needs_snapshot`
  body, and the `x-snapshot-index` / `sub` wire-name constants.

The core MUST NOT depend on axum, serde, or any HTTP type. Keep the
adapter the only place that knows about the wire.

## Key invariants (do not break)

1. `attach` subscribes to the broadcast FIRST, then reads the backlog,
   both under the same lock — so no event slips between the last
   replayed index and the first live one.
2. The cursor advances on DELIVERY, not on snapshot. A long-lived
   connected subscription is never expired mid-stream regardless of how
   much wall-clock time passes — only disconnected handles age out via
   TTL. (This is the bug class the crate was extracted to kill.)
3. Reconnect with the same `sub_id` REFRESHES the existing handle in
   place — handles never accumulate across reconnects.

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
