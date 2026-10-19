//! Typed-source acceptance ledger:
//! - existing typed log + lazy serialization + shared single-channel use:
//!   typed_log_source_delivers_snapshot_and_lazy_deltas_without_replacing_log
//! - external state -> log lock barrier under concurrent appends:
//!   external_state_and_log_locks_keep_concurrent_open_gap_free
//! - typed lag recovery + hinted reconnect:
//!   typed_source_recovers_lag_and_replays_from_client_hint
//! - erased backlog/live indexes, epoch, errors and Send (without E: Sync):
//!   erased_replay_preserves_backlog_epoch_laziness_and_receive_errors
//! - rejected snapshot cannot leave an attached handle:
//!   open_on_log_rejects_empty_snapshot_before_registering

use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::poll_fn;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, TryLockError};
use std::time::{Duration, Instant};

use kal_sse::mux::{
    erase_replay, open_on_log, ErasedReplay, EventSerializer, Mux, MuxConfig, MuxFrame, MuxSource,
    MuxStream, Opened, ResumeHints,
};
use kal_sse::{AttachError, Cursor, Epoch, EventLog, Indexed};
use tokio::sync::broadcast::error::RecvError;

const TTL: Duration = Duration::from_secs(60);
const STEP: Duration = Duration::from_secs(2);

/// Models an existing materialized map and a separately shared typed log.
/// Writers and fresh snapshots both acquire state before log.
struct TypedSource {
    state: Mutex<u32>,
    log: Arc<Mutex<EventLog<u32>>>,
    serialize: EventSerializer<u32>,
    serialized: Arc<AtomicUsize>,
    opening: Option<Arc<Barrier>>,
}

impl TypedSource {
    fn new(capacity: usize) -> Self {
        let serialized = Arc::new(AtomicUsize::new(0));
        let calls = serialized.clone();
        Self {
            state: Mutex::new(0),
            log: Arc::new(Mutex::new(EventLog::new(TTL, 256, capacity))),
            serialize: Arc::new(move |value| {
                calls.fetch_add(1, Ordering::SeqCst);
                value.to_string()
            }),
            serialized,
            opening: None,
        }
    }

    fn append(&self, count: u32) {
        for _ in 0..count {
            let mut state = self.state.lock().expect("state mutex poisoned");
            let mut log = self.log.lock().expect("event log mutex poisoned");
            *state += 1;
            log.append(*state);
        }
    }

    fn mux(self: &Arc<Self>) -> Mux {
        let source = self.clone();
        Mux::new(MuxConfig::default())
            .family("typed", move |_: Option<&str>| {
                Ok(source.clone() as Arc<dyn MuxSource>)
            })
            .unwrap()
    }
}

impl MuxSource for TypedSource {
    fn open_fresh(&self, now: Instant) -> Result<Opened, String> {
        let state = self.state.lock().expect("state mutex poisoned");
        let parts = vec![state.to_string()];
        if let Some(opening) = &self.opening {
            opening.wait();
            opening.wait();
        }
        let mut log = self.log.lock().expect("event log mutex poisoned");
        // Keep BOTH guards until snapshot registration and attachment finish.
        open_on_log(&mut log, parts, self.serialize.clone(), now)
    }

    fn resume(&self, sub_id: &str, hint: Option<Cursor>) -> Result<ErasedReplay, AttachError> {
        let replay = self
            .log
            .lock()
            .expect("event log mutex poisoned")
            .attach_from(sub_id, hint)?;
        Ok(erase_replay(replay, self.serialize.clone()))
    }

    fn is_current(&self, sub_id: &str, epoch: Epoch) -> bool {
        self.log
            .lock()
            .expect("event log mutex poisoned")
            .is_current(sub_id, epoch)
    }

    fn advance(&self, sub_id: &str, epoch: Epoch, cursor: Cursor, now: Instant) {
        self.log
            .lock()
            .expect("event log mutex poisoned")
            .advance_at(sub_id, epoch, cursor, now);
    }

    fn disconnect(&self, sub_id: &str, epoch: Epoch, now: Instant) {
        self.log
            .lock()
            .expect("event log mutex poisoned")
            .disconnect(sub_id, epoch, now);
    }

    fn release(&self, sub_id: &str) {
        self.log
            .lock()
            .expect("event log mutex poisoned")
            .release(sub_id);
    }
}

async fn next(stream: &mut MuxStream) -> MuxFrame {
    tokio::time::timeout(STEP, stream.next())
        .await
        .expect("frame timeout")
        .expect("stream ended")
}

fn snapshot(cursor: Cursor) -> MuxFrame {
    MuxFrame::Snapshot {
        ch: "typed".into(),
        gen: 1,
        cursor,
        part: 0,
        last: true,
        data: cursor.to_string(),
    }
}

fn delta(cursor: Cursor) -> MuxFrame {
    MuxFrame::Delta {
        ch: "typed".into(),
        gen: 1,
        cursor,
        data: cursor.to_string(),
    }
}

#[tokio::test]
async fn typed_log_source_delivers_snapshot_and_lazy_deltas_without_replacing_log() {
    let source = Arc::new(TypedSource::new(8));
    source.append(1);
    let (single_sub, _) = source
        .log
        .lock()
        .expect("event log mutex poisoned")
        .register(None);
    let mux = source.mux();
    let reservation = mux.reserve(vec!["typed".into()]).unwrap();
    let mut stream = mux.stream(&reservation.conn, ResumeHints::new()).unwrap();
    source.append(2);
    assert_eq!(
        source.serialized.load(Ordering::SeqCst),
        0,
        "append/open must not serialize live deltas"
    );
    assert!(matches!(next(&mut stream).await, MuxFrame::Hello { .. }));
    assert_eq!(next(&mut stream).await, snapshot(1));
    assert_eq!(source.serialized.load(Ordering::SeqCst), 0);
    assert_eq!(next(&mut stream).await, delta(2));
    assert_eq!(source.serialized.load(Ordering::SeqCst), 1);
    assert_eq!(next(&mut stream).await, delta(3));
    assert_eq!(source.serialized.load(Ordering::SeqCst), 2);
    let replay = source
        .log
        .lock()
        .expect("event log mutex poisoned")
        .attach(&single_sub)
        .unwrap();
    assert_eq!(
        replay.backlog,
        vec![
            Indexed {
                index: 2,
                event: 2u32
            },
            Indexed {
                index: 3,
                event: 3u32
            }
        ]
    );
    assert_eq!(
        source
            .log
            .lock()
            .expect("event log mutex poisoned")
            .handle_count(),
        2
    );
    mux.release(&reservation.conn);
    assert_eq!(
        source
            .log
            .lock()
            .expect("event log mutex poisoned")
            .handle_count(),
        1
    );
}

#[tokio::test]
async fn external_state_and_log_locks_keep_concurrent_open_gap_free() {
    let opening = Arc::new(Barrier::new(2));
    let source = Arc::new(TypedSource {
        opening: Some(opening.clone()),
        ..TypedSource::new(256)
    });
    source.append(2);
    let mux = source.mux();
    let reservation = mux.reserve(vec!["typed".into()]).unwrap();
    let writer = {
        let source = source.clone();
        std::thread::spawn(move || {
            opening.wait(); // Snapshot bytes are rendered, registration has not run.
            assert!(
                matches!(source.state.try_lock(), Err(TryLockError::WouldBlock)),
                "the state guard must survive snapshot rendering until attachment"
            );
            opening.wait();
            source.append(100);
        })
    };
    let mut stream = mux.stream(&reservation.conn, ResumeHints::new()).unwrap();
    writer.join().unwrap();
    next(&mut stream).await;
    assert_eq!(next(&mut stream).await, snapshot(2));
    for cursor in 3..=102 {
        assert_eq!(next(&mut stream).await, delta(cursor));
    }
    assert_eq!(source.serialized.load(Ordering::SeqCst), 100);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), stream.next())
            .await
            .is_err(),
        "no duplicate delivery after all concurrent appends"
    );
}

#[tokio::test]
async fn typed_source_recovers_lag_and_replays_from_client_hint() {
    let source = Arc::new(TypedSource::new(1));
    let mux = source.mux();
    let reservation = mux.reserve(vec!["typed".into()]).unwrap();
    let mut stream = mux.stream(&reservation.conn, ResumeHints::new()).unwrap();
    next(&mut stream).await;
    assert_eq!(next(&mut stream).await, snapshot(0));
    source.append(4);
    for cursor in 1..=4 {
        assert_eq!(next(&mut stream).await, delta(cursor));
    }
    drop(stream);
    let mut resumed = mux
        .stream(
            &reservation.conn,
            BTreeMap::from([("typed".into(), (1, 1))]),
        )
        .unwrap();
    next(&mut resumed).await;
    for cursor in 2..=4 {
        assert_eq!(next(&mut resumed).await, delta(cursor));
    }
    source.append(1);
    assert_eq!(next(&mut resumed).await, delta(5));
    assert_eq!(
        source
            .log
            .lock()
            .expect("event log mutex poisoned")
            .handle_count(),
        1
    );
}

#[tokio::test]
async fn erased_replay_preserves_backlog_epoch_laziness_and_receive_errors() {
    // Cell<u32> is Send but NOT Sync; erasure must not add an E: Sync bound.
    fn assert_send_static<T: Send + 'static>(_: &T) {}
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let serialize: EventSerializer<Cell<u32>> = Arc::new(move |value| {
        counted.fetch_add(1, Ordering::SeqCst);
        value.get().to_string()
    });
    let mut log = EventLog::new(TTL, 32, 1);
    let (sub, _) = log.register(None);
    log.append(Cell::new(10));
    let replay = log.attach(&sub).unwrap();
    let epoch = replay.epoch;
    let mut erased = erase_replay(replay, serialize);
    assert_send_static(&erased);
    assert_eq!(erased.epoch, epoch);
    assert_eq!(
        erased.backlog,
        vec![Indexed {
            index: 1,
            event: "10".into()
        }]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    log.append(Cell::new(20));
    log.append(Cell::new(30));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        poll_fn(|cx| erased.live.poll_recv(cx)).await,
        Err(RecvError::Lagged(1))
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "lag must not serialize or skip to a later event"
    );
    assert_eq!(
        poll_fn(|cx| erased.live.poll_recv(cx)).await,
        Ok(Indexed {
            index: 3,
            event: "30".into()
        })
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        log.attach_from(&sub, Some(0))
            .unwrap()
            .backlog
            .iter()
            .map(|item| item.index)
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "erasure alone must not advance the cursor"
    );
    drop(log);
    assert_eq!(
        poll_fn(|cx| erased.live.poll_recv(cx)).await,
        Err(RecvError::Closed)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn open_on_log_rejects_empty_snapshot_before_registering() {
    let mut log = EventLog::<u32>::new(TTL, 32, 8);
    let error = open_on_log(&mut log, vec![], Arc::new(u32::to_string), Instant::now())
        .err()
        .expect("empty snapshot must fail");
    assert_eq!(error, "snapshot must have at least one part");
    assert_eq!(log.handle_count(), 0);
}
