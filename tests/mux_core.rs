//! Acceptance ledger: each case in doc/mux.org maps to its numbered test below.
//! Additional tests cover validation, bounded controls, cleanup, and pull-only delivery.
mod support;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::task::Context;
use std::time::Instant;

use kal_sse::mux::{Mux, MuxConfig, MuxError, MuxFrame, MuxStream, ResumeHints};
use support::*;

struct WakeCounter(AtomicUsize);

impl futures::task::ArcWake for WakeCounter {
    fn wake_by_ref(counter: &Arc<Self>) {
        counter.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn keys(keys: &[&str]) -> Vec<String> {
    keys.iter().map(|key| (*key).into()).collect()
}

async fn next(stream: &mut MuxStream) -> MuxFrame {
    tokio::time::timeout(STEP, stream.next())
        .await
        .expect("frame timeout")
        .expect("unexpected stream end")
}

fn snapshot(key: &str, gen: u64, cursor: u64, part: usize, last: bool) -> MuxFrame {
    MuxFrame::Snapshot {
        ch: key.into(),
        gen,
        cursor,
        part,
        last,
        data: cursor.to_string(),
    }
}

fn delta(key: &str, gen: u64, cursor: u64) -> MuxFrame {
    MuxFrame::Delta {
        ch: key.into(),
        gen,
        cursor,
        data: cursor.to_string(),
    }
}

#[tokio::test]
async fn case_01_snapshot_barrier_concurrent_append_exactly_once() {
    let a = channel(1024, 1024, 1);
    let mux = mux(&[("a", a.clone())]);
    let reservation = mux.reserve(keys(&["a"])).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let writer = {
        let (a, barrier) = (a.clone(), barrier.clone());
        std::thread::spawn(move || {
            barrier.wait();
            append(&a, 100);
        })
    };
    barrier.wait();
    let mut stream = mux.stream(&reservation.conn, ResumeHints::new()).unwrap();
    writer.join().unwrap();
    assert!(matches!(next(&mut stream).await, MuxFrame::Hello { .. }));
    let frame = next(&mut stream).await;
    let base = match frame {
        MuxFrame::Snapshot {
            cursor, ref data, ..
        } => {
            assert_eq!(data, &cursor.to_string());
            cursor
        }
        frame => panic!("expected snapshot, got {frame:?}"),
    };
    for cursor in base + 1..=100 {
        assert_eq!(next(&mut stream).await, delta("a", 1, cursor));
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), stream.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn case_02_add_keeps_other_channels_flowing_without_replay() {
    let (a, b, c) = (channel(32, 8, 1), channel(32, 8, 1), channel(32, 8, 2));
    let mux = mux(&[("a", a.clone()), ("b", b.clone()), ("c", c.clone())]);
    let r = mux.reserve(keys(&["a", "b"])).unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    next(&mut stream).await;
    assert_eq!(next(&mut stream).await, snapshot("a", 1, 0, 0, true));
    assert_eq!(next(&mut stream).await, snapshot("b", 2, 0, 0, true));
    append(&a, 1);
    append(&b, 1);
    let gens = mux.change(&r.conn, keys(&["c"]), vec![]).unwrap();
    assert_eq!(gens, BTreeMap::from([("c".into(), 3)]));
    append(&c, 1);
    assert_eq!(next(&mut stream).await, delta("a", 1, 1));
    assert_eq!(next(&mut stream).await, delta("b", 2, 1));
    assert_eq!(next(&mut stream).await, snapshot("c", 3, 1, 0, false));
    append(&a, 1);
    assert_eq!(next(&mut stream).await, delta("a", 1, 2));
    assert_eq!(next(&mut stream).await, snapshot("c", 3, 1, 1, true));
    append(&c, 1);
    assert_eq!(next(&mut stream).await, delta("c", 3, 2));
}

#[tokio::test]
async fn case_03_remove_releases_immediately_and_stops_frames() {
    let (a, b) = (channel(32, 8, 1), channel(32, 8, 1));
    let mux = mux(&[("a", a.clone()), ("b", b.clone())]);
    let r = mux.reserve(keys(&["a", "b"])).unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    for _ in 0..3 {
        next(&mut stream).await;
    }
    append(&a, 2);
    mux.change(&r.conn, vec![], keys(&["a"])).unwrap();
    assert_eq!(a.update(|_, log| log.handle_count()), 0);
    assert_eq!(
        next(&mut stream).await,
        MuxFrame::Removed {
            ch: "a".into(),
            gen: 1
        }
    );
    append(&b, 1);
    assert_eq!(next(&mut stream).await, delta("b", 2, 1));
}

#[tokio::test]
async fn case_04_readd_has_a_strictly_new_generation() {
    let a = channel(32, 8, 1);
    let mux = mux(&[("a", a.clone())]);
    let r = mux.reserve(keys(&["a"])).unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    next(&mut stream).await;
    next(&mut stream).await;
    mux.change(&r.conn, vec![], keys(&["a"])).unwrap();
    let gens = mux.change(&r.conn, keys(&["a"]), vec![]).unwrap();
    assert_eq!(gens["a"], 2);
    append(&a, 1);
    assert_eq!(
        next(&mut stream).await,
        MuxFrame::Removed {
            ch: "a".into(),
            gen: 1
        }
    );
    assert_eq!(next(&mut stream).await, snapshot("a", 2, 1, 0, true));
}

#[tokio::test]
async fn case_05_lag_replays_gap_without_interrupting_neighbor() {
    let (a, b) = (channel(32, 1, 1), channel(32, 8, 1));
    let mux = mux(&[("a", a.clone()), ("b", b.clone())]);
    let r = mux.reserve(keys(&["a", "b"])).unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    for _ in 0..3 {
        next(&mut stream).await;
    }
    append(&a, 4);
    append(&b, 1);
    assert_eq!(next(&mut stream).await, delta("a", 1, 1));
    assert_eq!(next(&mut stream).await, delta("b", 2, 1));
    for cursor in 2..=4 {
        assert_eq!(next(&mut stream).await, delta("a", 1, cursor));
    }
    assert_eq!(a.update(|_, log| log.handle_count()), 1);
}

#[tokio::test]
async fn case_06_expiry_resnapshots_only_the_expired_channel() {
    let (a, b) = (channel(1, 1, 1), channel(32, 8, 1));
    let mux = mux(&[("a", a.clone()), ("b", b.clone())]);
    let r = mux.reserve(keys(&["a", "b"])).unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    for _ in 0..3 {
        next(&mut stream).await;
    }
    append(&a, 4);
    append(&b, 1);
    assert_eq!(next(&mut stream).await, snapshot("a", 3, 4, 0, true));
    assert_eq!(next(&mut stream).await, delta("b", 2, 1));
    assert_eq!(a.update(|_, log| log.handle_count()), 1);
    append(&a, 1);
    assert_eq!(next(&mut stream).await, delta("a", 3, 5));
}

#[tokio::test]
async fn case_07_snapshot_parts_are_round_robin_fair_and_precede_deltas() {
    let (a, b) = (channel(32, 8, 20), channel(32, 8, 1));
    let mux = mux(&[("a", a.clone()), ("b", b.clone())]);
    let r = mux.reserve(keys(&["a", "b"])).unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    next(&mut stream).await;
    assert_eq!(next(&mut stream).await, snapshot("a", 1, 0, 0, false));
    next(&mut stream).await; // B's snapshot
    append(&a, 1);
    append(&b, 1);
    assert_eq!(next(&mut stream).await, snapshot("a", 1, 0, 1, false));
    assert_eq!(next(&mut stream).await, delta("b", 2, 1));
    for part in 2..20 {
        assert_eq!(
            next(&mut stream).await,
            snapshot("a", 1, 0, part, part == 19)
        );
    }
    assert_eq!(next(&mut stream).await, delta("a", 1, 1));
}

#[tokio::test]
async fn case_08_resume_hints_replay_handed_off_but_unapplied_frames() {
    let (a, b) = (channel(32, 8, 1), channel(32, 8, 1));
    let mux = mux(&[("a", a.clone()), ("b", b.clone())]);
    let r = mux.reserve(keys(&["a", "b"])).unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    for _ in 0..3 {
        next(&mut stream).await;
    }
    append(&a, 3);
    append(&b, 3);
    for _ in 0..6 {
        next(&mut stream).await;
    }
    drop(stream);
    let hints = BTreeMap::from([("a".into(), (1, 1)), ("b".into(), (2, 2))]);
    let mut stream = mux.stream(&r.conn, hints).unwrap();
    next(&mut stream).await;
    assert_eq!(next(&mut stream).await, delta("a", 1, 2));
    assert_eq!(next(&mut stream).await, delta("b", 2, 3));
    assert_eq!(next(&mut stream).await, delta("a", 1, 3));
    assert_eq!(a.update(|_, log| log.handle_count()), 1);
    assert_eq!(b.update(|_, log| log.handle_count()), 1);
}

#[tokio::test]
async fn case_08b_stale_or_missing_hints_resnapshot_only_that_channel() {
    for stale in [false, true] {
        let (a, b) = (channel(32, 8, 1), channel(32, 8, 1));
        let mux = mux(&[("a", a.clone()), ("b", b.clone())]);
        let r = mux.reserve(keys(&["a", "b"])).unwrap();
        let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
        for _ in 0..3 {
            next(&mut stream).await;
        }
        append(&a, 1);
        append(&b, 1);
        next(&mut stream).await;
        next(&mut stream).await;
        drop(stream);
        let mut hints = BTreeMap::from([("b".into(), (2, 0))]);
        if stale {
            hints.insert("a".into(), (0, 0));
        }
        let mut stream = mux.stream(&r.conn, hints).unwrap();
        next(&mut stream).await;
        assert_eq!(next(&mut stream).await, snapshot("a", 3, 1, 0, true));
        assert_eq!(next(&mut stream).await, delta("b", 2, 1));
    }
}

#[tokio::test]
async fn case_09_supersede_wakes_idle_stream_and_old_drop_cannot_expire_replacement() {
    let a = channel(32, 8, 1);
    let mux = mux(&[("a", a.clone())]);
    let r = mux.reserve(keys(&["a"])).unwrap();
    let mut old = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    next(&mut old).await;
    next(&mut old).await;
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = futures::task::waker(counter.clone());
    assert!(old.poll_next(&mut Context::from_waker(&waker)).is_pending());
    let hints = BTreeMap::from([("a".into(), (1, 0))]);
    let mut new = mux.stream(&r.conn, hints).unwrap();
    assert_eq!(
        counter.0.load(Ordering::SeqCst),
        1,
        "supersede must wake an idle HTTP body without waiting for data"
    );
    assert_eq!(tokio::time::timeout(STEP, old.next()).await.unwrap(), None);
    drop(old);
    mux.compact(Instant::now() + TTL * 10);
    a.update(|_, log| log.compact(Instant::now() + TTL * 10));
    assert_eq!(mux.connection_count(), 1);
    assert_eq!(a.update(|_, log| log.handle_count()), 1);
    next(&mut new).await;
    append(&a, 1);
    assert_eq!(next(&mut new).await, delta("a", 1, 1));
}

#[tokio::test]
async fn case_10_ttl_reclaims_unopened_and_disconnected_connections_and_handles() {
    let a = channel(32, 8, 1);
    let mux = mux(&[("a", a.clone())]);
    let now = Instant::now();
    let unopened = mux.reserve_at(keys(&["a"]), now).unwrap();
    let opened = mux.reserve_at(keys(&["a"]), now).unwrap();
    let stream = mux
        .stream_at(&opened.conn, ResumeHints::new(), now)
        .unwrap();
    assert_eq!(a.update(|_, log| log.handle_count()), 1);
    stream.disconnect_at(now);
    mux.compact(now + TTL - std::time::Duration::from_nanos(1));
    assert_eq!(mux.connection_count(), 2);
    mux.compact(now + TTL);
    assert_eq!(mux.connection_count(), 0);
    assert_eq!(a.update(|_, log| log.handle_count()), 0);
    for conn in [&opened.conn, &unopened.conn] {
        assert!(matches!(
            mux.stream_at(conn, ResumeHints::new(), now + TTL),
            Err(MuxError::NeedsReservation { .. })
        ));
    }
}

#[tokio::test]
async fn unpolled_and_partial_snapshot_reconnects_do_not_skip_state() {
    let a = channel(32, 8, 2);
    let mux = mux(&[("a", a.clone())]);
    let r = mux.reserve(keys(&["a"])).unwrap();
    drop(mux.stream(&r.conn, ResumeHints::new()).unwrap());
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    next(&mut stream).await;
    assert_eq!(next(&mut stream).await, snapshot("a", 1, 0, 0, false));
    drop(stream);
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    next(&mut stream).await;
    assert_eq!(next(&mut stream).await, snapshot("a", 2, 0, 0, false));
    assert_eq!(next(&mut stream).await, snapshot("a", 2, 0, 1, true));
}

#[tokio::test]
async fn validation_is_atomic_and_release_ends_an_idle_stream() {
    let a = channel(32, 8, 1);
    let mux = mux(&[("a", a.clone())]);
    for invalid in ["A", "a,", "a:x,y", "missing", ""] {
        assert!(mux.reserve(keys(&["a", invalid])).is_err(), "{invalid}");
        assert_eq!(mux.connection_count(), 0);
    }
    let r = mux.reserve(keys(&["a", "a"])).unwrap();
    assert_eq!(r.gens.len(), 1);
    assert!(mux.change(&r.conn, keys(&["a"]), keys(&["a"])).is_err());
    assert!(mux
        .change(&r.conn, keys(&["missing"]), keys(&["a"]))
        .is_err());
    assert_eq!(
        mux.change(&r.conn, keys(&["a"]), keys(&["absent"]))
            .unwrap(),
        r.gens
    );
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    next(&mut stream).await;
    next(&mut stream).await;
    mux.release(&r.conn);
    assert_eq!(a.update(|_, log| log.handle_count()), 0);
    assert_eq!(
        tokio::time::timeout(STEP, stream.next()).await.unwrap(),
        None
    );
}

#[test]
fn family_registration_rejects_malformed_and_duplicate_names() {
    let source = channel(32, 8, 1);
    for name in ["", "A", "a:b", "a,b", "1a"] {
        let source = source.clone();
        assert!(Mux::new(MuxConfig::default())
            .family(name, move |_: Option<&str>| Ok(
                source.clone() as Arc<dyn kal_sse::mux::MuxSource>
            ))
            .is_err());
    }
    let first = source.clone();
    let second = source.clone();
    let mux = Mux::new(MuxConfig::default())
        .family("a", move |_: Option<&str>| {
            Ok(first.clone() as Arc<dyn kal_sse::mux::MuxSource>)
        })
        .unwrap();
    assert!(mux
        .family("a", move |_: Option<&str>| Ok(
            second.clone() as Arc<dyn kal_sse::mux::MuxSource>
        ))
        .is_err());
}

#[tokio::test]
async fn bounded_control_queue_refuses_atomically_then_recovers_after_drain() {
    let a = channel(32, 8, 1);
    let source = a.clone();
    let mux = Mux::new(MuxConfig {
        control_capacity: 1,
        ..MuxConfig::default()
    })
    .family("a", move |_: Option<&str>| {
        Ok(source.clone() as Arc<dyn kal_sse::mux::MuxSource>)
    })
    .unwrap();
    let r = mux.reserve(keys(&["a:x", "a:y"])).unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    for _ in 0..3 {
        next(&mut stream).await;
    }
    mux.change(&r.conn, vec![], keys(&["a:x"])).unwrap();
    assert!(matches!(
        mux.change(&r.conn, keys(&["a:z"]), keys(&["a:y"])),
        Err(MuxError::Busy { .. })
    ));
    assert_eq!(
        a.update(|_, log| log.handle_count()),
        1,
        "failed command must not release y"
    );
    assert_eq!(
        next(&mut stream).await,
        MuxFrame::Removed {
            ch: "a:x".into(),
            gen: 1
        }
    );
    assert_eq!(
        mux.change(&r.conn, keys(&["a:y"]), vec![]).unwrap()["a:y"],
        2
    );
    let gens = mux.change(&r.conn, keys(&["a:z"]), keys(&["a:y"])).unwrap();
    assert_eq!(
        gens["a:z"], 3,
        "failed command must not consume a generation"
    );
    assert_eq!(a.update(|_, log| log.handle_count()), 0);
}

#[tokio::test]
async fn polling_not_append_or_prefetch_advances_the_delivery_cursor() {
    let a = Arc::new(kal_sse::mux::Channel::new(
        0u64,
        kal_sse::EventLog::new(TTL, 32, 8).with_redelivery_window(std::time::Duration::ZERO),
        |state| Ok(vec![state.to_string()]),
    ));
    let mux = mux(&[("a", a.clone())]);
    let r = mux.reserve(keys(&["a"])).unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    next(&mut stream).await;
    next(&mut stream).await;
    append(&a, 3);
    a.update(|_, log| {
        log.compact(Instant::now());
        assert_eq!(log.len(), 3);
    });
    assert_eq!(next(&mut stream).await, delta("a", 1, 1));
    a.update(|_, log| {
        log.compact(Instant::now());
        assert_eq!(log.len(), 2);
    });
    drop(stream);
    mux.compact(Instant::now() + TTL * 2);
    assert_eq!(a.update(|_, log| log.handle_count()), 0);
}

#[tokio::test]
async fn opaque_parameters_and_snapshot_provider_errors_are_isolated() {
    let source = Arc::new(kal_sse::mux::Channel::new(
        false,
        kal_sse::EventLog::new(TTL, 32, 8),
        |ready| {
            if *ready {
                Ok(vec!["null".into()])
            } else {
                Err("state unavailable".into())
            }
        },
    ));
    let family = source.clone();
    let mux = Mux::new(MuxConfig::default())
        .family("room_1-a", move |_: Option<&str>| {
            Ok(family.clone() as Arc<dyn kal_sse::mux::MuxSource>)
        })
        .unwrap();
    let r = mux
        .reserve(keys(&["room_1-a:", "room_1-a:雪:other / ?"]))
        .unwrap();
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    next(&mut stream).await;
    for (key, gen) in [("room_1-a:", 1), ("room_1-a:雪:other / ?", 2)] {
        assert_eq!(
            next(&mut stream).await,
            MuxFrame::Error {
                ch: key.into(),
                gen,
                reason: "state unavailable".into()
            }
        );
    }
    assert_eq!(source.update(|_, log| log.handle_count()), 0);
    source.update(|ready, _| *ready = true);
    drop(stream);
    let mut stream = mux.stream(&r.conn, ResumeHints::new()).unwrap();
    next(&mut stream).await;
    assert!(matches!(
        next(&mut stream).await,
        MuxFrame::Snapshot { gen: 3, .. }
    ));
}
