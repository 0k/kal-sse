//! `axum` adapter (feature `axum`).
//!
//! Renders a [`crate::core::EventLog`] subscription onto the SSE wire:
//!
//! * looks up the subscription's resume cursor by `sub_id` — the
//!   cursor is server-side state (set by the REST snapshot via
//!   [`EventLog::register`](crate::core::EventLog::register), then
//!   advanced on every delivered event), so the SSE request carries
//!   ONLY `?sub=<uuid>`, never an index,
//! * atomically replays the backlog then attaches to live,
//! * tags every event with `id: <index>` (diagnostic / parity with the
//!   SSE spec; the client does not need to echo it back),
//! * **advances the subscription's server-side cursor as each event is
//!   delivered** (the heart of the contract),
//! * opens the response body immediately with a comment that carries no
//!   data or event id and does not advance the subscription cursor,
//! * marks the subscription disconnected when the stream drops (starts
//!   its TTL grace),
//! * answers `409 needs_snapshot` when the `sub_id` is unknown or its
//!   cursor fell out of the retained window,
//! * keeps the connection warm with a 15 s `: ping` comment.
//!
//! The caller owns the `Arc<Mutex<EventLog<E>>>` and provides a
//! `serialize` closure (`&E -> String`) so this crate stays free of any
//! `serde` assumption about the event type.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::Json;
use futures_core::Stream;

use crate::core::{AttachError, EventLog, Indexed, SubId};

/// Keep-alive interval: an SSE comment line is sent after this many
/// seconds without an event, so idle proxies keep the stream open.
const KEEPALIVE_SECS: u64 = 15;

/// Header carrying the snapshot's cursor on a REST snapshot response.
/// Informational: the client does NOT need to send it back (the server
/// keys the cursor by `sub_id`), but it is useful for debugging and for
/// non-`kal-sse` consumers.
pub const SNAPSHOT_INDEX_HEADER: &str = "x-snapshot-index";

/// Header / query name carrying the subscription id on an SSE request.
pub const SUB_ID_PARAM: &str = "sub";

/// The `409 Conflict` body returned when a subscription cannot resume
/// (unknown `sub_id` or its cursor expired). The client must re-take
/// the REST snapshot to (re)register and obtain a fresh subscription.
fn needs_snapshot_body(
    sub_id: &str,
    err: AttachError,
    resync_hint: &str,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut body = serde_json::json!({
        "error": "needs_snapshot",
        "sub": sub_id,
        "hint": resync_hint,
    });
    if let AttachError::Expired { earliest, current } = err {
        body["earliest"] = serde_json::json!(earliest);
        body["current"] = serde_json::json!(current);
    }
    (StatusCode::CONFLICT, Json(body))
}

/// Build a resilient SSE response for a subscription to `log`.
///
/// * `log` — the shared event log behind the channel.
/// * `sub_id` — the subscription id from the SSE request (`?sub=<uuid>`).
///   It must already have been registered by the REST snapshot
///   ([`EventLog::register`](crate::core::EventLog::register)).
/// * `serialize` — turns one event into its SSE `data:` payload.
/// * `resync_hint` — human-readable pointer to the snapshot endpoint,
///   embedded in the `409` body (e.g. `"resync via GET /api/projects"`).
///
/// On success returns an [`Sse`] streaming the backlog then live
/// updates; on an unknown / expired subscription returns the `409`
/// tuple so the caller can `?`-propagate it as an axum response.
#[allow(clippy::type_complexity)]
pub fn sse_response<E, F>(
    log: Arc<Mutex<EventLog<E>>>,
    sub_id: SubId,
    serialize: F,
    resync_hint: &str,
) -> Result<
    Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>,
    (StatusCode, Json<serde_json::Value>),
>
where
    E: Clone + Send + 'static,
    F: Fn(&E) -> String + Send + 'static,
{
    let replay = {
        let mut guard = log.lock().expect("event log mutex poisoned");
        match guard.attach(&sub_id) {
            Ok(r) => r,
            Err(err) => {
                return Err(needs_snapshot_body(&sub_id, err, resync_hint));
            }
        }
    };

    let epoch = replay.epoch;
    let stream = build_stream(log, sub_id, epoch, replay.backlog, replay.live, serialize);

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(KEEPALIVE_SECS))
            .text("ping"),
    ))
}

/// Internal: open with an immediate comment, then chain the backlog
/// stream and live stream, advancing the subscription cursor on every
/// delivered event and marking it disconnected when the live stream
/// ends.
///
/// `epoch` is this attach's generation: if a later attach supersedes
/// this `sub_id` (the client re-subscribed to the same data flow), the
/// stream stops as soon as it notices it is no longer current, and its
/// final `disconnect` is gated on the epoch so it cannot clobber the
/// replacement handle.
fn build_stream<E, F>(
    log: Arc<Mutex<EventLog<E>>>,
    sub_id: SubId,
    epoch: crate::core::Epoch,
    backlog: Vec<Indexed<E>>,
    live: tokio::sync::broadcast::Receiver<Indexed<E>>,
    serialize: F,
) -> impl Stream<Item = Result<Event, std::convert::Infallible>>
where
    E: Clone + Send + 'static,
    F: Fn(&E) -> String + Send + 'static,
{
    use async_stream::stream;

    stream! {
        let serialize = serialize;
        yield Ok(Event::default().comment("connected"));

        // 1) Replay the backlog. Each delivered item advances the
        //    subscription cursor so compaction can release it.
        for item in backlog {
            {
                let mut g = log.lock().expect("event log mutex poisoned");
                g.advance(&sub_id, epoch, item.index);
            }
            yield Ok(Event::default()
                .id(item.index.to_string())
                .data(serialize(&item.event)));
        }

        // 2) Live tail.
        let mut live = live;
        loop {
            match live.recv().await {
                Ok(item) => {
                    // Stop immediately if a newer attach superseded us.
                    {
                        let mut g = log.lock().expect("event log mutex poisoned");
                        if !g.is_current(&sub_id, epoch) {
                            break;
                        }
                        g.advance(&sub_id, epoch, item.index);
                    }
                    yield Ok(Event::default()
                        .id(item.index.to_string())
                        .data(serialize(&item.event)));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Consumer fell behind the broadcast buffer: the
                    // next `recv()` would return a LATER event,
                    // silently skipping the gap (and `advance` would
                    // move the cursor past it, losing those events for
                    // this `sub_id` forever). Break instead — ending
                    // the stream marks the handle disconnected with
                    // its cursor UN-advanced past the gap, so the
                    // client reconnect (same `sub_id` only — the
                    // cursor is server-side, never sent by the client)
                    // replays gap-free from the retained log via
                    // `attach`, or receives `409 needs_snapshot` if
                    // the gap fell out of the retained buffer.
                    break;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }

        // 3) Stream ended (client gone / server shutdown / superseded):
        //    start the disconnect TTL so a reconnect within the grace
        //    window resumes gap-free. Epoch-gated: a superseded stream
        //    must NOT start a TTL on the handle that replaced it.
        {
            let mut g = log.lock().expect("event log mutex poisoned");
            g.disconnect(&sub_id, epoch, Instant::now());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn immediate_comment_precedes_backlog_without_advancing_cursor() {
        use axum::response::IntoResponse;
        use futures::StreamExt;

        let log: Arc<Mutex<EventLog<u32>>> =
            Arc::new(Mutex::new(EventLog::new(Duration::from_secs(900), 256, 64)));
        let sub = {
            let mut g = log.lock().unwrap();
            let (id, _) = g.register(None);
            g.append(10);
            g.append(20);
            g.append(30);
            id
        };

        let response = sse_response(
            log.clone(),
            sub.clone(),
            |e: &u32| e.to_string(),
            "resync via GET /api/test",
        )
        .unwrap()
        .into_response();
        let mut body = response.into_body().into_data_stream();

        let first = body.next().await.unwrap().unwrap();
        assert_eq!(&first[..], b": connected\n\n");
        drop(body);

        // Consuming only the comment must not advance the cursor. A
        // reconnect therefore still receives the complete backlog.
        let resumed = sse_response(
            log,
            sub,
            |e: &u32| e.to_string(),
            "resync via GET /api/test",
        )
        .unwrap()
        .into_response();
        let mut resumed_body = resumed.into_body().into_data_stream();

        assert_eq!(
            &resumed_body.next().await.unwrap().unwrap()[..],
            b": connected\n\n"
        );
        assert_eq!(
            &resumed_body.next().await.unwrap().unwrap()[..],
            b"id: 1\ndata: 10\n\n"
        );
        assert_eq!(
            &resumed_body.next().await.unwrap().unwrap()[..],
            b"id: 2\ndata: 20\n\n"
        );
        assert_eq!(
            &resumed_body.next().await.unwrap().unwrap()[..],
            b"id: 3\ndata: 30\n\n"
        );
    }

    #[tokio::test]
    async fn attach_after_register_returns_sse() {
        let log: Arc<Mutex<EventLog<u32>>> =
            Arc::new(Mutex::new(EventLog::new(Duration::from_secs(900), 256, 64)));
        let sub = {
            let mut g = log.lock().unwrap();
            g.append(10);
            // REST snapshot registers the subscription at the current
            // cursor, returning the (minted) sub_id.
            let (id, _cursor) = g.register(None);
            id
        };
        let res = sse_response(
            log,
            sub,
            |e: &u32| e.to_string(),
            "resync via GET /api/test",
        );
        assert!(res.is_ok(), "registered sub should produce an SSE response");
    }

    #[test]
    fn unknown_sub_returns_409_needs_snapshot() {
        let log: Arc<Mutex<EventLog<u32>>> =
            Arc::new(Mutex::new(EventLog::new(Duration::from_secs(900), 256, 64)));
        {
            let mut g = log.lock().unwrap();
            g.append(1);
        }
        // Never registered this sub_id.
        let res = sse_response(
            log,
            "never-registered".to_string(),
            |e: &u32| e.to_string(),
            "resync via GET /api/test",
        );
        let (status, body) = match res {
            Ok(_) => panic!("expected 409 needs_snapshot"),
            Err(t) => t,
        };
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body.0["error"], "needs_snapshot");
        assert_eq!(body.0["sub"], "never-registered");
        assert_eq!(body.0["hint"], "resync via GET /api/test");
    }

    #[tokio::test]
    async fn lagged_broadcast_ends_stream_for_gap_free_resume() {
        // SPEC: when the live broadcast lags, the subscription stream
        // MUST end (so the client reconnects) rather than silently
        // skip past the gap. A subsequent attach for the same `sub_id`
        // MUST replay every missed event still retained in the log —
        // the server-side cursor was NOT advanced past the gap.
        use futures::StreamExt;

        // channel_capacity=1 so a couple of unpolled appends overflow
        // the receiver and force `Lagged` on the next recv().
        let log: Arc<Mutex<EventLog<u32>>> =
            Arc::new(Mutex::new(EventLog::new(Duration::from_secs(900), 256, 1)));
        let (sub, epoch, backlog, live) = {
            let mut g = log.lock().unwrap();
            let (id, _) = g.register(None);
            let r = g.attach(&id).unwrap();
            (id, r.epoch, r.backlog, r.live)
        };
        assert!(backlog.is_empty(), "fresh register+attach has no backlog");

        let stream = build_stream(log.clone(), sub.clone(), epoch, backlog, live, |e: &u32| {
            e.to_string()
        });
        tokio::pin!(stream);

        // Append events WITHOUT polling the stream so the capacity-1
        // broadcast receiver falls behind.
        {
            let mut g = log.lock().unwrap();
            g.append(10);
            g.append(20);
            g.append(30);
            g.append(40);
        }

        // The immediate comment is the first item and does not poll the
        // lagged receiver or advance the cursor.
        assert!(
            stream.next().await.is_some(),
            "stream must start with the immediate comment"
        );

        // The Lagged arm MUST then end the stream. With the buggy
        // `continue`, the next recv() returns a LATER event (index 4)
        // which the stream yields and `advance`s past — silently
        // skipping 1..3.
        let next = stream.next().await;
        assert!(
            next.is_none(),
            "stream must end on Lagged; otherwise gap events are silently dropped",
        );

        // Reconnect with the same `sub_id` (no index — server-side
        // cursor): the held cursor was never advanced past the gap, so
        // `attach` replays every event still retained, gap-free.
        let resumed = {
            let mut g = log.lock().unwrap();
            g.attach(&sub).unwrap()
        };
        let idxs: Vec<_> = resumed.backlog.iter().map(|e| e.index).collect();
        assert_eq!(
            idxs,
            vec![1, 2, 3, 4],
            "resume must replay every missed event gap-free"
        );
    }

    #[test]
    fn expired_sub_returns_409_with_bounds() {
        let log: Arc<Mutex<EventLog<u32>>> =
            Arc::new(Mutex::new(EventLog::new(Duration::from_secs(900), 2, 64)));
        let sub = {
            let mut g = log.lock().unwrap();
            g.append(1);
            // Register at cursor 1...
            let (id, _) = g.register(None);
            // ...then append past the hard cap so the registered cursor
            // (1) falls behind the retained window (earliest becomes 2).
            g.append(2);
            g.append(3);
            g.append(4); // cap=2 -> earliest now 3
            id
        };
        let res = sse_response(
            log,
            sub,
            |e: &u32| e.to_string(),
            "resync via GET /api/test",
        );
        let (status, body) = match res {
            Ok(_) => panic!("expected 409 needs_snapshot (expired)"),
            Err(t) => t,
        };
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body.0["error"], "needs_snapshot");
        // earliest retained = 3, current = last_index = 4.
        assert_eq!(body.0["earliest"], 3);
        assert_eq!(body.0["current"], 4);
        assert_eq!(body.0["hint"], "resync via GET /api/test");
    }
}
