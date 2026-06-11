//! `kal-sse` — resilient Server-Sent Events as snapshot + cursor data
//! channels.
//!
//! # The contract
//!
//! A *data channel* pairs a REST snapshot with an SSE update stream:
//!
//! * **REST snapshot** establishes the client's starting position — a
//!   monotonic cursor index `N` on the channel.
//! * **SSE stream** then delivers every update after `N`, in order,
//!   each tagged with its own index so the client always knows where
//!   it is.
//!
//! A client identifies itself to a channel with a **subscription id**
//! (`sub_id`) — one per logical subscription to one data flow, NOT per
//! tab / window / app. As an SPA navigates, some channel subscriptions
//! close, some open, some reconnect; each is its own `sub_id`.
//!
//! # Why "resilient"
//!
//! The cursor is **delivery-driven**: sending the `N -> N+1` event *is*
//! the state transfer. Once the server has emitted that event to a
//! subscription, the subscription is authoritatively at `N+1` — no
//! ack, no heartbeat, no re-snapshot. This is what lets the server
//! retain *exactly* the set of events some live subscription might
//! still need, and no more.
//!
//! When a subscription's SSE stream drops, the server keeps its handle
//! for a TTL (default 15 min). A reconnect with the same `sub_id`
//! (carrying no index — the cursor lives on the server) resumes
//! **gap-free**: the server replays from the cursor it still holds for
//! that `sub_id`, then attaches to live. Only when that cursor has
//! fallen out of the retained buffer (TTL lapsed AND compaction moved
//! past it) does the server answer [`AttachError::Expired`], and the
//! client re-snapshots.
//!
//! # Layers
//!
//! * [`core`] — framework-agnostic. The [`EventLog`] data structure,
//!   the subscription registry, cursor advancement, compaction, and the
//!   [`AttachError`] error. No HTTP, no axum, no `serde`.
//! * `axum` (feature `axum`) — a thin adapter turning a channel
//!   subscription into an `axum` SSE response: `?sub=<uuid>` lookup of
//!   the server-side cursor, `409 needs_snapshot` on unknown / expired
//!   sub, the `id:` field, the `x-snapshot-index` header constant, and
//!   keep-alive.
//!
//! The core never depends on the adapter; the adapter is opt-in so the
//! data structure stays reusable outside any web framework.
//!
//! # Wire exchange
//!
//! Happy path (first connect) and gap-free reconnect in one diagram:
//!
//! ```text
//! Client                              Server
//!   |                                   |
//!   |  GET /api/things                  |
//!   |---------------------------------->|
//!   |                                   | register(None) -> ("3f2a…", 42)
//!   |  200 OK                           |
//!   |  x-snapshot-index: 42             |
//!   |  { …body…, sub: "3f2a…" }         |
//!   |<----------------------------------|
//!   |                                   |
//!   |  GET /api/things/stream?sub=3f2a… |
//!   |---------------------------------->|
//!   |                                   | attach("3f2a…") -> backlog, live
//!   |  200 OK (SSE stream)              |
//!   |<----------------------------------|
//!   |  id: 43 / data: {…}              | advance(…, 43)
//!   |<----------------------------------|
//!   |  : ping                           |  (every 15 s)
//!   |<----------------------------------|
//!   |                                   |
//!   |  [network drop]                   |
//!   |  X---------------------------------| disconnect(…, epoch, now)
//!   |                                   |   TTL countdown starts
//!   |                                   |   (events 44, 45 appended)
//!   |                                   |
//!   |  GET /api/things/stream?sub=3f2a… |  (same sub_id, no index)
//!   |---------------------------------->|
//!   |                                   | attach("3f2a…") -> backlog=[44,45]
//!   |  200 OK (SSE stream)              |
//!   |<----------------------------------|
//!   |  id: 44 / data: {…}              | advance(…, 44)
//!   |  id: 45 / data: {…}              | advance(…, 45)
//!   |<----------------------------------|
//!   |  (live tail continues…)           |
//!   |                                   |
//!   |  [TTL lapsed / server restart]    |
//!   |  GET /api/things/stream?sub=3f2a… |
//!   |---------------------------------->|
//!   |                                   | attach("3f2a…") -> UnknownSub
//!   |  409 Conflict                     |
//!   |  {"error":"needs_snapshot",…}     |
//!   |<----------------------------------|
//!   |  GET /api/things?sub=3f2a…        |  (propose same sub_id)
//!   |---------------------------------->|
//!   |                                   | register(Some("3f2a…")) -> (…, 51)
//!   |  200 OK / x-snapshot-index: 51    |
//!   |<----------------------------------|
//!   |  GET /api/things/stream?sub=3f2a… |
//!   |---------------------------------->|
//!   |  200 OK (SSE stream from 51)      |
//!   |<----------------------------------|
//! ```
//!
//! # Client contract
//!
//! This is a cooperating-client protocol. A native browser `EventSource`
//! is not sufficient: per the WHATWG spec, any non-200 response
//! (including the `409`) permanently closes the connection with no
//! retry and no status code in the error event. The client must wrap
//! `EventSource` or use a fetch-based SSE reader.
//!
//! Client loop:
//!
//! 1. `GET` snapshot endpoint (optionally proposing a stored `sub_id`).
//!    Render the body as current state. Store the returned `sub_id`.
//! 2. Open SSE: `GET <stream>?sub=<sub_id>`.
//! 3. Apply each event in order. (`id:` is diagnostic; do not echo it.)
//! 4. On stream drop: reconnect with the same `sub_id` (step 2).
//!    Within TTL → gap-free resume.
//! 5. On `409 needs_snapshot`: go to step 1, proposing the same `sub_id`.
//!
//! The client never tracks or sends an index. The server holds the cursor.
//!
//! # Limits
//!
//! * **In-memory, single-process.** A server restart loses the registry.
//!   Every client gets `409 needs_snapshot` on its next SSE request and
//!   re-takes the snapshot (handled by design; thundering-herd on restart
//!   is the cost).
//! * **Not horizontally scalable as-is.** The registry is process-local;
//!   no shared-store backend is provided.
//! * **Slowest handle pins the buffer.** The retention floor is the
//!   minimum cursor over all live and within-TTL handles. The
//!   `max_events` hard cap is the escape valve: once hit, the lagging
//!   subscriber gets `AttachError::Expired` on resume.

#![forbid(unsafe_code)]

pub mod core;

pub use crate::core::{new_sub_id, AttachError, Cursor, Epoch, EventLog, Indexed, Replay, SubId};

#[cfg(feature = "axum")]
pub mod axum;
