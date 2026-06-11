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
//! for a TTL (default 15 min). A reconnect with the same `sub_id` and
//! the cursor the client still holds (`?since=N`) resumes **gap-free**:
//! the server replays `N+1..` then attaches to live. Only when `N` has
//! fallen out of the retained buffer (TTL lapsed AND compaction moved
//! past it) does the server answer [`ExpiredCursor`], and the client
//! re-snapshots.
//!
//! # Layers
//!
//! * [`core`] — framework-agnostic. The [`EventLog`] data structure,
//!   the subscription registry, cursor advancement, compaction, and the
//!   [`ExpiredCursor`] error. No HTTP, no axum, no `serde`.
//! * `axum` (feature `axum`) — a thin adapter turning a channel
//!   subscription into an `axum` SSE response: `?since` / `Last-Event-ID`
//!   parsing, `409 expired_cursor`, the `id:` field, `x-snapshot-index`,
//!   and keep-alive.
//!
//! The core never depends on the adapter; the adapter is opt-in so the
//! data structure stays reusable outside any web framework.

#![forbid(unsafe_code)]

pub mod core;

pub use crate::core::{new_sub_id, AttachError, Cursor, Epoch, EventLog, Indexed, Replay, SubId};

#[cfg(feature = "axum")]
pub mod axum;
