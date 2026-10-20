use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use kal_sse::mux::{
    Channel, ErasedReplay, MuxSource, OpenFuture, Opened, SnapshotPart, SnapshotParts,
};
use kal_sse::{AttachError, Cursor, Epoch};
use tokio::sync::{mpsc, oneshot};

/// A controlled slow load, with a barrier before or after waiting and explicit
/// cleanup for any handle the future owns until it transfers it to the mux.
pub struct DelayedSource {
    pub channel: Arc<Channel<u64>>,
    pub dropped: Arc<AtomicBool>,
    load: Mutex<Option<oneshot::Receiver<Result<(), String>>>>,
    parts: Mutex<Option<mpsc::Receiver<Result<SnapshotPart, String>>>>,
    after_load: bool,
}

pub type Load = oneshot::Sender<Result<(), String>>;
pub type Parts = mpsc::Sender<Result<SnapshotPart, String>>;

impl DelayedSource {
    pub fn new(channel: Arc<Channel<u64>>) -> (Arc<Self>, Load, Parts) {
        Self::create(channel, false)
    }

    pub fn after_load(channel: Arc<Channel<u64>>) -> (Arc<Self>, Load, Parts) {
        Self::create(channel, true)
    }

    fn create(channel: Arc<Channel<u64>>, after_load: bool) -> (Arc<Self>, Load, Parts) {
        let (load, loaded) = oneshot::channel();
        let (parts, receiver) = mpsc::channel(8);
        (
            Arc::new(Self {
                channel,
                dropped: Arc::new(AtomicBool::new(false)),
                load: Mutex::new(Some(loaded)),
                parts: Mutex::new(Some(receiver)),
                after_load,
            }),
            load,
            parts,
        )
    }
}

struct PendingHandle {
    source: Arc<Channel<u64>>,
    opened: Option<Opened>,
    dropped: Arc<AtomicBool>,
}

impl Drop for PendingHandle {
    fn drop(&mut self) {
        if let Some(opened) = self.opened.take() {
            self.source.release(&opened.sub_id);
        }
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl MuxSource for DelayedSource {
    fn open(self: Arc<Self>, now: Instant) -> OpenFuture {
        let load = self.load.lock().unwrap().take().expect("one async open");
        let mut parts = self.parts.lock().unwrap().take().unwrap();
        let mut handle = PendingHandle {
            source: self.channel.clone(),
            opened: if self.after_load {
                None
            } else {
                Some(self.channel.open_fresh(now).unwrap())
            },
            dropped: self.dropped.clone(),
        };
        Box::pin(async move {
            load.await.map_err(|_| "load cancelled".to_string())??;
            if handle.opened.is_none() {
                handle.opened = Some(handle.source.open_fresh(Instant::now())?);
            }
            Ok(handle
                .opened
                .take()
                .unwrap()
                .with_snapshot(SnapshotParts::new(move |cx| parts.poll_recv(cx))))
        })
    }

    fn resume(&self, sub: &str, hint: Option<Cursor>) -> Result<ErasedReplay, AttachError> {
        self.channel.resume(sub, hint)
    }
    fn is_current(&self, sub: &str, epoch: Epoch) -> bool {
        self.channel.is_current(sub, epoch)
    }
    fn advance(&self, sub: &str, epoch: Epoch, cursor: Cursor, now: Instant) {
        self.channel.advance(sub, epoch, cursor, now);
    }
    fn disconnect(&self, sub: &str, epoch: Epoch, now: Instant) {
        self.channel.disconnect(sub, epoch, now);
    }
    fn release(&self, sub: &str) {
        self.channel.release(sub);
    }
}
