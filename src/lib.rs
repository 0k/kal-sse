//! `kal-sse` implements resilient Server-Sent Events as snapshot plus
//! cursor data channels.
//!
//! # Contract
//!
//! A data channel is a REST snapshot and an SSE stream. A `sub_id`
//! identifies one logical subscription to one data flow, not a tab,
//! window, or application. The snapshot calls [`EventLog::register`] or
//! [`EventLog::register_at`], which mints or adopts the id, supersedes a
//! prior handle, records the snapshot index as its base, and sets the
//! server cursor to that base. Its TTL starts at registration and lasts
//! until the first attach.
//!
//! A stream request carries `?sub=<uuid>` and may carry a last-applied
//! event id in `last_event_id` or `Last-Event-ID`. The resume point is
//! that id, capped at the server cursor; an id below the snapshot base
//! gets `409 needs_snapshot`, since that client never saw the current
//! snapshot. Without an id, the resume point is the server cursor. A
//! `sub_id` belongs to one client at a time. The server atomically replays retained events
//! and attaches to live events. Every SSE event carries `id: <index>`.
//!
//! # Why resilient
//!
//! [`EventLog::advance`] records hand-off to the HTTP stack, not client
//! application. A successful socket write places bytes in the server
//! kernel send buffer; it does not prove client receipt or application.
//! The server cursor is therefore an upper bound. A reconnect supplies
//! the client's last applied id to replay events handed into a dead
//! connection.
//!
//! Sent events remain replayable for [`DEFAULT_REDELIVERY_WINDOW`] after
//! being sent, subject to handle TTL and `max_events`; configure it with
//! [`EventLog::with_redelivery_window`]. A resume gets a complete replay
//! or [`AttachError::Expired`] and must take a new snapshot, never a
//! silent gap.
//!
//! ## Delivery boundary
//!
//! The axum adapter advances immediately before yielding an event to its
//! response body. Its drop guard calls [`EventLog::disconnect`] whenever
//! a stream ends, including a server-side dropped response after a failed
//! write. An attached handle never expires by TTL. Once detached,
//! [`EventLog::compact`] reclaims it after TTL.
//!
//! ## Layers
//!
//! * [`core`] owns indexes, handles, snapshot bases, cursor advancement,
//!   atomic replay plus live attachment, retention, and [`AttachError`].
//! * `axum`, behind the `axum` feature, parses resume ids, renders `id:`
//!   events, sends `409 needs_snapshot`, keeps the stream alive, and
//!   disconnects it through a drop guard.
//!
//! # Wire exchange
//!
//! ```text
//! Client                                Server
//!   | GET /things                       |
//!   |---------------------------------->| register(None) -> (sub, 42)
//!   | 200 {sub}, x-snapshot-index: 42   |
//!   |<----------------------------------|
//!   | GET /things/stream?sub=sub&       |
//!   |   last_event_id=42                |
//!   |---------------------------------->| attach_from(sub, Some(42))
//!   | id: 43                            | advance(43), handed to HTTP stack
//!   |<----------------------------------| [client never applies 43; link dies]
//!   | GET /things/stream?sub=sub&       |
//!   |   last_event_id=42                |
//!   |---------------------------------->| attach_from(sub, Some(42))
//!   | id: 43                            | replay
//!   |<----------------------------------|
//!   |                                   |
//!   | GET /things/stream?sub=forgotten  |
//!   |---------------------------------->| AttachError::UnknownSub
//!   | 409 {"error":"needs_snapshot"}   |
//!   |<----------------------------------|
//! ```
//!
//! Re-registering or re-attaching increments an epoch. A superseded
//! stream's advance and disconnect are no-ops; it stops when its next
//! live event observes the new epoch.
//!
//! # Client contract
//!
//! 1. Get a snapshot, proposing a stored `sub_id`; render it, store the
//!    returned id, and take the snapshot index (`x-snapshot-index`) as
//!    the last applied id: an id older than the snapshot gets a `409`.
//! 2. Open `GET <stream>?sub=<uuid>&last_event_id=<last applied id>`.
//! 3. Apply events in order and remember each applied `id`.
//! 4. After a drop, reconnect with the same id and last applied id in the
//!    query or header.
//! 5. On `409 needs_snapshot`, or a browser `EventSource` reconnect that
//!    never opens and hides the status, return to step 1 with the same id.
//!
//! An `EventSource` cannot set headers. Browser retries supply
//! `Last-Event-ID`; a newly created `EventSource` must use
//! `?last_event_id=<n>`. The header wins when both are present. Malformed
//! values produce `400 bad_last_event_id`. The adapter's `sse_response`
//! accepts no id and uses only the server cursor, so it cannot recover
//! events lost after a silent disconnect; use `sse_response_from`.
//!
//! # Limits
//!
//! Handles and events are in-memory and process-local, so restart and
//! multi-instance deployments require resnapshots or an external shared
//! implementation. Retention keeps events after the lowest surviving
//! resume floor plus recent sent events during the redelivery window.
//! `max_events` is a hard memory cap and can force `needs_snapshot`.

#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod core;

pub use crate::core::{
    new_sub_id, AttachError, Cursor, Epoch, EventLog, Indexed, Replay, SubId,
    DEFAULT_REDELIVERY_WINDOW,
};

#[cfg(feature = "axum")]
#[cfg_attr(docsrs, doc(cfg(feature = "axum")))]
pub mod axum;
