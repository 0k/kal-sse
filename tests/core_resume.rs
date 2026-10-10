//! Resume and retention contract of [`EventLog`], through its public API.
//!
//! * Events already sent stay replayable for the redelivery window
//!   (default 60 s), so a client resuming from its `Last-Event-ID` gets
//!   what was sent into a dead connection.
//! * The client's last id is capped at the server cursor; an id below the
//!   snapshot index gets `Expired`.
//! * A handle with no attached stream that can no longer resume is
//!   forgotten at the next compaction.

use std::time::{Duration, Instant};

use kal_sse::{AttachError, Cursor, EventLog};

const TTL: Duration = Duration::from_secs(900);

fn indices(log: &mut EventLog<u32>, sub: &str, last_event_id: Option<Cursor>) -> Vec<Cursor> {
    let replay = log.attach_from(sub, last_event_id).unwrap();
    replay.backlog.iter().map(|e| e.index).collect()
}

/// A subscription whose stream was sent events 1..=3, then dropped at `t0`.
fn sent_three_then_dropped(log: &mut EventLog<u32>, t0: Instant) -> String {
    let (sub, _) = log.register_at(None, t0);
    let epoch = log.attach(&sub).unwrap().epoch;
    for v in 1..=3 {
        let index = log.append(v);
        log.advance_at(&sub, epoch, index, t0);
    }
    log.disconnect(&sub, epoch, t0);
    sub
}

#[test]
fn sent_events_stay_replayable_for_the_default_redelivery_window() {
    let mut log = EventLog::new(TTL, 256, 64);
    let t0 = Instant::now();
    let sub = sent_three_then_dropped(&mut log, t0);

    log.compact(t0 + Duration::from_secs(59));

    assert_eq!(indices(&mut log, &sub, Some(1)), vec![2, 3]);
}

#[test]
fn sent_events_are_dropped_after_the_default_redelivery_window() {
    let mut log = EventLog::new(TTL, 256, 64);
    let t0 = Instant::now();
    let sub = sent_three_then_dropped(&mut log, t0);

    log.compact(t0 + Duration::from_secs(61));

    assert!(log.is_empty());
    assert!(matches!(
        log.attach_from(&sub, Some(1)),
        Err(AttachError::Expired { .. })
    ));
}

#[test]
fn redelivery_window_is_configurable() {
    let mut log = EventLog::new(TTL, 256, 64).with_redelivery_window(Duration::from_secs(10));
    let t0 = Instant::now();
    sent_three_then_dropped(&mut log, t0);

    log.compact(t0 + Duration::from_secs(5));
    assert_eq!(log.len(), 3);
    log.compact(t0 + Duration::from_secs(11));
    assert!(log.is_empty());
}

#[test]
fn last_event_id_above_the_cursor_resumes_from_the_cursor() {
    let mut log = EventLog::new(TTL, 256, 64);
    log.append(0); // index 1, before the snapshot
    let (sub, _) = log.register(None); // snapshot at 1
    let epoch = log.attach(&sub).unwrap().epoch;
    log.append(0); // 2
    log.advance(&sub, epoch, 2); // sent up to 2
    log.append(0); // 3, not sent

    assert_eq!(indices(&mut log, &sub, Some(99)), vec![3]);
}

#[test]
fn last_event_id_at_the_snapshot_resumes_from_the_snapshot() {
    let mut log = EventLog::new(TTL, 256, 64);
    log.append(0); // index 1, before the snapshot
    let (sub, _) = log.register(None); // snapshot at 1
    log.append(0); // 2
    log.append(0); // 3

    assert_eq!(indices(&mut log, &sub, Some(1)), vec![2, 3]);
}

#[test]
fn last_event_id_below_the_snapshot_needs_a_new_snapshot() {
    // A client whose last id predates the current snapshot of this
    // sub_id never saw that snapshot (e.g. a superseded holder of the
    // same id): replaying from the snapshot would silently skip events.
    let mut log = EventLog::new(TTL, 256, 64);
    let (sub, _) = log.register(None);
    let epoch = log.attach(&sub).unwrap().epoch;
    for v in 1..=2 {
        let index = log.append(v);
        log.advance(&sub, epoch, index);
    }
    log.append(3);
    log.append(4);
    log.register(Some(sub.clone())); // another holder re-snapshots at 4

    assert!(matches!(
        log.attach_from(&sub, Some(2)),
        Err(AttachError::Expired { .. })
    ));
}

#[test]
fn a_proposed_sub_id_that_is_not_a_uuid_is_replaced_by_a_minted_one() {
    let mut log = EventLog::<u32>::new(TTL, 256, 64);
    let proposed = "0f3c9a52-1b7e-4d2a-9c6e-8a1f2b3c4d5e".to_string();

    let (adopted, _) = log.register(Some(proposed.clone()));
    let (minted, _) = log.register(Some("x".repeat(1 << 20)));

    assert_eq!(adopted, proposed);
    assert_eq!(minted.len(), 36);
    assert!(log.attach(&minted).is_ok());
}

#[test]
fn many_sends_within_the_window_stay_cheap() {
    // Each advance must not rescan every send record of the window
    // (quadratic, under the log's lock).
    let mut log = EventLog::new(TTL, 200_000, 64);
    let (sub, _) = log.register(None);
    let epoch = log.attach(&sub).unwrap().epoch;
    let t0 = Instant::now();
    let start = Instant::now();
    for v in 0..60_000u32 {
        let index = log.append(v);
        log.advance_at(&sub, epoch, index, t0);
    }
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "60k sends took {:?}",
        start.elapsed()
    );
}

#[test]
fn compact_forgets_an_unattached_handle_that_fell_behind_the_buffer() {
    let mut log = EventLog::new(TTL, 2, 64);
    let (sub, _) = log.register(None); // snapshot at 0, never attached
    for v in 1..=4 {
        log.append(v); // cap 2: only 3, 4 retained
    }

    log.compact(Instant::now());

    assert_eq!(log.handle_count(), 0);
    assert_eq!(log.attach(&sub).err(), Some(AttachError::UnknownSub));
}

#[test]
fn compact_keeps_an_attached_handle_that_fell_behind_the_buffer() {
    let mut log = EventLog::new(TTL, 2, 64);
    let (sub, _) = log.register(None);
    let _replay = log.attach(&sub).unwrap(); // stream attached, not consuming
    for v in 1..=4 {
        log.append(v);
    }

    log.compact(Instant::now());

    assert_eq!(log.handle_count(), 1);
}

#[test]
fn forgotten_handle_never_reuses_a_still_open_stream_epoch() {
    let mut log = EventLog::<u32>::new(TTL, 256, 64);
    let t0 = Instant::now();
    let (sub, _) = log.register_at(None, t0);
    let stream_a = log.attach(&sub).unwrap();
    log.register_at(Some(sub.clone()), t0);
    log.compact(t0 + TTL);
    assert_eq!(log.handle_count(), 0);

    let now = t0 + TTL;
    log.register_at(Some(sub.clone()), now);
    let stream_b = log.attach(&sub).unwrap();
    assert!(!log.is_current(&sub, stream_a.epoch));
    assert!(log.is_current(&sub, stream_b.epoch));
    log.disconnect(&sub, stream_a.epoch, now);
    log.compact(now + 10 * TTL);
    assert_eq!(log.handle_count(), 1);
    assert!(log.is_current(&sub, stream_b.epoch));
}

#[test]
fn old_events_get_a_full_window_after_send_not_append() {
    for elapsed in [59, 61] {
        let mut log = EventLog::new(TTL, 256, 64);
        let appended_at = Instant::now();
        let (sub, _) = log.register_at(None, appended_at);
        for value in 1..=3 {
            log.append(value);
        }
        let sent_at = appended_at + Duration::from_secs(120);
        let epoch = log.attach(&sub).unwrap().epoch;
        log.advance_at(&sub, epoch, 3, sent_at);
        log.disconnect(&sub, epoch, sent_at);
        log.compact(sent_at + Duration::from_secs(elapsed));

        if elapsed == 59 {
            assert_eq!(indices(&mut log, &sub, Some(1)), vec![2, 3]);
        } else {
            assert!(log.is_empty());
            assert!(matches!(
                log.attach_from(&sub, Some(1)),
                Err(AttachError::Expired { .. })
            ));
        }
    }
}

#[test]
fn staggered_sends_expire_individually_and_keep_unsent_events() {
    let mut log = EventLog::new(TTL, 256, 64);
    let t0 = Instant::now();
    let (sub, _) = log.register_at(None, t0);
    let epoch = log.attach(&sub).unwrap().epoch;
    for value in 1..=3 {
        log.append(value);
    }
    log.advance_at(&sub, epoch, 1, t0);
    log.advance_at(&sub, epoch, 2, t0 + Duration::from_secs(30));
    log.compact(t0 + Duration::from_secs(60));
    assert_eq!(log.len(), 2);
    assert_eq!(indices(&mut log, &sub, Some(1)), vec![2, 3]);
    let epoch = log.attach(&sub).unwrap().epoch;
    log.advance_at(&sub, epoch, 2, t0 + Duration::from_secs(60));
    log.compact(t0 + Duration::from_secs(90));
    assert_eq!(log.len(), 2, "re-sending starts a new window");
    log.compact(t0 + Duration::from_secs(120));
    assert_eq!(indices(&mut log, &sub, None), vec![3]);
}

#[test]
fn reconnect_preserves_send_history_and_noop_advances_do_not_extend_it() {
    let mut log = EventLog::new(TTL, 256, 64);
    let t0 = Instant::now();
    let sub = sent_three_then_dropped(&mut log, t0);
    let old_epoch = log.attach(&sub).unwrap().epoch;
    let epoch = log.attach(&sub).unwrap().epoch;
    log.advance_at(&sub, old_epoch, 4, t0 + Duration::from_secs(30));
    log.advance_at(&sub, epoch, 3, t0 + Duration::from_secs(30));
    log.advance_at(&sub, epoch, 2, t0 + Duration::from_secs(30));
    log.compact(t0 + Duration::from_secs(59));
    assert_eq!(log.len(), 3);
    log.compact(t0 + Duration::from_secs(60));
    assert!(log.is_empty());
}

#[test]
fn unattached_resumability_uses_the_redelivery_floor_after_cap_eviction() {
    let mut log = EventLog::new(TTL, 2, 64);
    let t0 = Instant::now();
    let (sub, _) = log.register_at(None, t0);
    let epoch = log.attach(&sub).unwrap().epoch;
    for value in 1..=2 {
        let index = log.append(value);
        log.advance_at(&sub, epoch, index, t0);
    }
    log.append(3);
    log.disconnect(&sub, epoch, t0);
    log.compact(t0 + Duration::from_secs(59));
    assert_eq!(log.handle_count(), 0);
    assert_eq!(log.attach(&sub).err(), Some(AttachError::UnknownSub));
}
