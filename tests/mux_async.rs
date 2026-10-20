//! Async snapshot acceptance ledger:
//! - pending open isolation + open wake: slow_open_does_not_delay_neighbor
//! - streamed fairness + part wake + gap-free barrier: streamed_parts_interleave_and_hold_deltas
//! - cancellation + handle release + new generation: removal_cancels_loading_and_readd_is_fresh
//! - G8 load/part errors + neighbors continue: loading_and_parts_errors_are_isolated
//! - incomplete/empty parts rejected: premature_end_is_a_channel_error
//! - disconnect/supersede cancellation: disconnect_cancels_pending_open
//! - barrier after asynchronous loading: source_can_take_barrier_after_loading
//! - active producer cancellation: removal_drops_parts_and_releases_handle
//! - cancelling the sync adapter itself: cancelled_default_open_cannot_leak_a_handle
//!
//! Sync compatibility remains covered by mux_core and mux_typed unchanged.
#[path = "support/async_source.rs"]
mod async_source;
mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Context;

use async_source::DelayedSource;
use kal_sse::mux::{Mux, MuxConfig, MuxFrame, MuxSource, MuxStream, ResumeHints, SnapshotPart};
use support::*;

struct Wakes(AtomicUsize);
impl futures::task::ArcWake for Wakes {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn setup(a: Arc<dyn MuxSource>, b: Arc<dyn MuxSource>) -> (Mux, String, MuxStream) {
    let mux = Mux::new(MuxConfig::default())
        .family("a", move |_: Option<&str>| Ok(a.clone()))
        .unwrap()
        .family("b", move |_: Option<&str>| Ok(b.clone()))
        .unwrap();
    let r = mux.reserve(vec!["a".into(), "b".into()]).unwrap();
    let stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    (mux, r.conn, stream)
}

async fn next(stream: &mut MuxStream) -> MuxFrame {
    tokio::time::timeout(STEP, stream.next())
        .await
        .unwrap()
        .unwrap()
}

fn delta(ch: &str, gen: u64, cursor: u64) -> MuxFrame {
    MuxFrame::Delta {
        ch: ch.into(),
        gen,
        cursor,
        data: cursor.to_string(),
    }
}

fn snapshot(part: usize, last: bool) -> MuxFrame {
    MuxFrame::Snapshot {
        ch: "a".into(),
        gen: 1,
        cursor: 0,
        part,
        last,
        data: "0".into(),
    }
}

fn error(reason: &str) -> MuxFrame {
    MuxFrame::Error {
        ch: "a".into(),
        gen: 1,
        reason: reason.into(),
    }
}

#[tokio::test]
async fn slow_open_does_not_delay_neighbor() {
    let (a, load, parts) = DelayedSource::new(channel(32, 8, 1));
    let b = channel(32, 8, 1);
    let (_, _, mut stream) = setup(a, b.clone());
    next(&mut stream).await; // hello
    assert!(matches!(next(&mut stream).await, MuxFrame::Snapshot { ch, .. } if ch == "b"));
    append(&b, 1);
    assert_eq!(next(&mut stream).await, delta("b", 2, 1));
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = futures::task::waker(wakes.clone());
    assert!(stream
        .poll_next(&mut Context::from_waker(&waker))
        .is_pending());
    parts
        .send(Ok(SnapshotPart {
            data: "0".into(),
            last: true,
        }))
        .await
        .unwrap();
    load.send(Ok(())).unwrap();
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert_eq!(next(&mut stream).await, snapshot(0, true));
}

#[tokio::test]
async fn streamed_parts_interleave_and_hold_deltas() {
    let state = channel(32, 1, 1); // Force log replay after loading/parts.
    let (a, load, parts) = DelayedSource::new(state.clone());
    let b = channel(32, 8, 1);
    let (_, _, mut stream) = setup(a, b.clone());
    next(&mut stream).await;
    next(&mut stream).await; // B snapshot, A still loading
    append(&state, 2);
    load.send(Ok(())).unwrap();
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = futures::task::waker(wakes.clone());
    assert!(stream
        .poll_next(&mut Context::from_waker(&waker))
        .is_pending());
    parts
        .send(Ok(SnapshotPart {
            data: "0".into(),
            last: false,
        }))
        .await
        .unwrap();
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert_eq!(next(&mut stream).await, snapshot(0, false));
    append(&b, 1);
    parts
        .send(Ok(SnapshotPart {
            data: "0".into(),
            last: false,
        }))
        .await
        .unwrap();
    assert_eq!(next(&mut stream).await, delta("b", 2, 1));
    assert_eq!(next(&mut stream).await, snapshot(1, false));
    assert!(stream
        .poll_next(&mut Context::from_waker(&waker))
        .is_pending());
    append(&state, 2);
    parts
        .send(Ok(SnapshotPart {
            data: "0".into(),
            last: true,
        }))
        .await
        .unwrap();
    assert_eq!(next(&mut stream).await, snapshot(2, true));
    assert!(
        parts.is_closed(),
        "last part must drop the producer immediately"
    );
    for cursor in 1..=4 {
        assert_eq!(next(&mut stream).await, delta("a", 1, cursor));
    }
    assert!(stream
        .poll_next(&mut Context::from_waker(&waker))
        .is_pending());
}

#[tokio::test]
async fn removal_cancels_loading_and_readd_is_fresh() {
    let state = channel(32, 8, 1);
    let (a, load, _) = DelayedSource::new(state.clone());
    let dropped = a.dropped.clone();
    let replacement = state.clone();
    let opens = Arc::new(AtomicUsize::new(0));
    let mux = Mux::new(MuxConfig::default())
        .family("a", move |_: Option<&str>| {
            if opens.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(a.clone() as Arc<dyn MuxSource>)
            } else {
                Ok(replacement.clone() as Arc<dyn MuxSource>)
            }
        })
        .unwrap();
    let r = mux.reserve(vec!["a".into()]).unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    next(&mut stream).await;
    let waker = futures::task::noop_waker();
    assert!(stream
        .poll_next(&mut Context::from_waker(&waker))
        .is_pending());
    assert_eq!(state.update(|_, log| log.handle_count()), 1);
    mux.change(&r.conn, vec![], vec!["a".into()]).unwrap();
    assert!(dropped.load(Ordering::SeqCst));
    assert!(load.is_closed());
    assert_eq!(state.update(|_, log| log.handle_count()), 0);
    assert_eq!(
        next(&mut stream).await,
        MuxFrame::Removed {
            ch: "a".into(),
            gen: 1
        }
    );
    assert_eq!(
        mux.change(&r.conn, vec!["a".into()], vec![]).unwrap()["a"],
        2
    );
    assert_eq!(
        next(&mut stream).await,
        MuxFrame::Snapshot {
            ch: "a".into(),
            gen: 2,
            cursor: 0,
            part: 0,
            last: true,
            data: "0".into()
        }
    );
}

#[tokio::test]
async fn loading_and_parts_errors_are_isolated() {
    for during_parts in [false, true] {
        let state = channel(32, 8, 1);
        let (a, load, parts) = DelayedSource::new(state.clone());
        let b = channel(32, 8, 1);
        let (_, _, mut stream) = setup(a, b.clone());
        next(&mut stream).await;
        next(&mut stream).await;
        if during_parts {
            load.send(Ok(())).unwrap();
            parts
                .send(Err("snapshot upstream failed".into()))
                .await
                .unwrap();
        } else {
            load.send(Err("snapshot upstream failed".into())).unwrap();
        }
        assert_eq!(next(&mut stream).await, error("snapshot upstream failed"));
        assert_eq!(state.update(|_, log| log.handle_count()), 0);
        append(&b, 1);
        assert_eq!(next(&mut stream).await, delta("b", 2, 1));
    }
}

#[tokio::test]
async fn premature_end_is_a_channel_error() {
    let (a, load, parts) = DelayedSource::new(channel(32, 8, 1));
    let (_, _, mut stream) = setup(a, channel(32, 8, 1));
    next(&mut stream).await;
    next(&mut stream).await;
    load.send(Ok(())).unwrap();
    drop(parts);
    assert_eq!(
        next(&mut stream).await,
        error("snapshot ended before its last part")
    );
}

#[tokio::test]
async fn disconnect_cancels_pending_open() {
    let state = channel(32, 8, 1);
    let (a, load, _) = DelayedSource::new(state.clone());
    let dropped = a.dropped.clone();
    let (_, _, stream) = setup(a, channel(32, 8, 1));
    drop(stream);
    assert!(dropped.load(Ordering::SeqCst));
    assert!(load.is_closed());
    assert_eq!(state.update(|_, log| log.handle_count()), 0);
}

#[tokio::test]
async fn source_can_take_barrier_after_loading() {
    let state = channel(32, 8, 1);
    let (a, load, parts) = DelayedSource::after_load(state.clone());
    let (_, _, mut stream) = setup(a, channel(32, 8, 1));
    next(&mut stream).await;
    next(&mut stream).await;
    assert_eq!(state.update(|_, log| log.handle_count()), 0);
    append(&state, 2); // These belong in the snapshot, not in deltas.
    load.send(Ok(())).unwrap();
    parts
        .send(Ok(SnapshotPart {
            data: "2".into(),
            last: true,
        }))
        .await
        .unwrap();
    assert_eq!(
        next(&mut stream).await,
        MuxFrame::Snapshot {
            ch: "a".into(),
            gen: 1,
            cursor: 2,
            part: 0,
            last: true,
            data: "2".into()
        }
    );
    append(&state, 1);
    assert_eq!(next(&mut stream).await, delta("a", 1, 3));
}

#[tokio::test]
async fn removal_drops_parts_and_releases_handle() {
    let state = channel(32, 8, 1);
    let (a, load, parts) = DelayedSource::new(state.clone());
    let (mux, conn, mut stream) = setup(a, channel(32, 8, 1));
    next(&mut stream).await;
    next(&mut stream).await;
    load.send(Ok(())).unwrap();
    parts
        .send(Ok(SnapshotPart {
            data: "0".into(),
            last: false,
        }))
        .await
        .unwrap();
    assert_eq!(next(&mut stream).await, snapshot(0, false));
    mux.change(&conn, vec![], vec!["a".into()]).unwrap();
    assert!(parts.is_closed());
    assert_eq!(state.update(|_, log| log.handle_count()), 0);
    assert_eq!(
        next(&mut stream).await,
        MuxFrame::Removed {
            ch: "a".into(),
            gen: 1
        }
    );
}

#[tokio::test]
async fn synchronous_source_uses_ready_async_adapter_unchanged() {
    let state = channel(32, 8, 1);
    let mux = support::mux(&[("a", state.clone())]);
    let r = mux.reserve(vec!["a".into()]).unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    assert_eq!(state.update(|_, log| log.handle_count()), 1);
    append(&state, 1);
    next(&mut stream).await;
    assert_eq!(next(&mut stream).await, snapshot(0, true));
    assert_eq!(next(&mut stream).await, delta("a", 1, 1));
}

#[test]
fn cancelled_default_open_cannot_leak_a_handle() {
    let state = channel(32, 8, 1);
    let opening = state.clone().open(std::time::Instant::now());
    drop(opening);
    assert_eq!(state.update(|_, log| log.handle_count()), 0);
}
