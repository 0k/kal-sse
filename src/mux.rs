//! Pull-driven multiplexed channels, independent of serialization and HTTP.
//!
//! [`Mux`] owns reservations, generations and stream authority, not event
//! retention: every channel delegates that to its [`EventLog`]. [`Channel`]
//! provides the shared-lock snapshot barrier for application state and log.
//! Sources may share any existing typed log: [`erase_replay`] converts its
//! backlog and lazily serializes its live events. [`open_on_log`] supports
//! snapshot barriers protected by the source's own application locks.
//! Frames contain serialized strings (valid JSON when using the axum adapter).
//! The mux never parses them.
//!
//! No tasks are spawned. [`MuxStream::next`] and [`MuxStream::poll_next`]
//! work without features; with `axum`, `MuxStream` also implements
//! `futures_core::Stream`. A frame's return from polling is its hand-off
//! boundary: don't prefetch frames into an application queue.
//!
//! Lock order is registry -> connection -> source. Source callbacks must not
//! call back into the mux. Call [`Mux::compact`] periodically, as well as
//! compacting the application's event logs, to reclaim idle reservations.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

use crate::{AttachError, Cursor, Epoch, EventLog, Indexed, Replay, SubId};

/// Generations are strictly increasing across every channel of a connection.
pub type Generation = u64;
/// Per-channel last-applied positions: channel -> (generation, cursor).
pub type ResumeHints = BTreeMap<String, (Generation, Cursor)>;
/// Generations assigned by reservation or channel-set changes.
pub type Generations = BTreeMap<String, Generation>;

/// Connection lifetime, heartbeat and bounded control acknowledgement queue.
#[derive(Clone, Debug)]
pub struct MuxConfig {
    /// Grace after reservation or stream disconnect. Default: fifteen minutes.
    pub ttl: Duration,
    /// HTTP keep-alive interval. Must be nonzero. Default: fifteen seconds.
    pub heartbeat: Duration,
    /// Maximum pending removal acknowledgements on a stalled stream.
    /// A change exceeding this bound is refused atomically; retry after draining.
    pub control_capacity: usize,
}

impl Default for MuxConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(900),
            heartbeat: Duration::from_secs(15),
            control_capacity: 256,
        }
    }
}

/// A refusal always contains a human-readable reason.
///
/// Downstream matches must allow future refusal variants:
///
/// ```compile_fail,E0004
/// use kal_sse::mux::MuxError;
///
/// fn reason(error: MuxError) -> String {
///     match error {
///         MuxError::InvalidChannel { reason, .. }
///         | MuxError::NeedsReservation { reason, .. }
///         | MuxError::InvalidChange { reason }
///         | MuxError::Busy { reason } => reason,
///     }
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MuxError {
    /// Invalid key, unregistered family, or invalid family registration.
    InvalidChannel { channel: String, reason: String },
    /// Unknown or TTL-reclaimed connection.
    NeedsReservation { conn: String, reason: String },
    /// Invalid command or attempted modification of a shared family registry.
    InvalidChange { reason: String },
    /// The stream must drain its bounded control queue before another change.
    Busy { reason: String },
}

impl std::fmt::Display for MuxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidChannel { channel, reason } => write!(f, "{channel}: {reason}"),
            Self::NeedsReservation { conn, reason } => write!(f, "{conn}: {reason}"),
            Self::InvalidChange { reason } | Self::Busy { reason } => f.write_str(reason),
        }
    }
}

impl std::error::Error for MuxError {}

/// Successful reservation; snapshots are not taken until stream attachment.
#[derive(Clone, Debug)]
pub struct Reservation {
    pub conn: String,
    pub gens: Generations,
    pub heartbeat_ms: u64,
}

/// Transport-independent frames. `data` is the provider's serialized payload.
///
/// Downstream matches must allow future frame variants:
///
/// ```compile_fail,E0004
/// use kal_sse::mux::MuxFrame;
///
/// fn classify(frame: MuxFrame) {
///     match frame {
///         MuxFrame::Hello { .. }
///         | MuxFrame::Snapshot { .. }
///         | MuxFrame::Delta { .. }
///         | MuxFrame::Removed { .. }
///         | MuxFrame::Error { .. } => (),
///     }
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MuxFrame {
    Hello {
        conn: String,
        heartbeat_ms: u64,
    },
    Snapshot {
        ch: String,
        gen: Generation,
        cursor: Cursor,
        part: usize,
        last: bool,
        data: String,
    },
    Delta {
        ch: String,
        gen: Generation,
        cursor: Cursor,
        data: String,
    },
    Removed {
        ch: String,
        gen: Generation,
    },
    Error {
        ch: String,
        gen: Generation,
        reason: String,
    },
}

/// A family's provider. Refusals become per-channel error frames, not a
/// connection-wide failure. Parameter validation belongs to the provider.
pub trait MuxFamily: Send + Sync + 'static {
    fn open(&self, param: Option<&str>) -> Result<Arc<dyn MuxSource>, String>;
}

impl<F> MuxFamily for F
where
    F: Fn(Option<&str>) -> Result<Arc<dyn MuxSource>, String> + Send + Sync + 'static,
{
    fn open(&self, param: Option<&str>) -> Result<Arc<dyn MuxSource>, String> {
        self(param)
    }
}

/// Shared serializer for one typed event. Must not re-enter the mux or acquire
/// locks held by its caller; serialization is synchronous and infallible.
pub type EventSerializer<E> = Arc<dyn Fn(&E) -> String + Send + Sync + 'static>;

/// Type-erased replay retaining the original attach's epoch and event indexes.
/// The bounded backlog is serialized during [`erase_replay`]; live events are
/// serialized only when [`ErasedLive::poll_recv`] receives them.
pub struct ErasedReplay {
    pub epoch: Epoch,
    pub backlog: Vec<Indexed<String>>,
    pub live: ErasedLive,
}

/// An owned, type-erased broadcast receiver. No task or forwarding queue is
/// created. Pending receives survive cancellation of the caller's poll future.
pub struct ErasedLive {
    receiver: Box<dyn LiveReceiver>,
}

impl ErasedLive {
    /// Receive and serialize at most one event. Broadcast `Lagged` and `Closed`
    /// errors pass through unchanged and never invoke the serializer. Receipt
    /// alone does not advance a log cursor; the mux advances at frame hand-off.
    pub fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Indexed<String>, broadcast::error::RecvError>> {
        self.receiver.poll_recv(cx)
    }
}

/// Adapt a typed log's replay without changing the log or its subscription.
///
/// Eagerly serializes the existing bounded backlog, preserving epoch/indexes.
/// The live receiver retains typed events and serializes successful receives
/// lazily. Neither conversion nor receipt advances a cursor. No tasks spawn.
/// `E` needs `Send`, not `Sync`.
///
/// Backlog serialization runs synchronously here. Callers may release their
/// log guard after `attach_from` and before calling this helper; the replay
/// already owns its gap-free backlog/live boundary. The serializer must not
/// re-enter the mux or acquire locks held by the caller.
pub fn erase_replay<E>(replay: Replay<E>, serialize: EventSerializer<E>) -> ErasedReplay
where
    E: Clone + Send + 'static,
{
    let backlog = replay
        .backlog
        .into_iter()
        .map(|item| Indexed {
            index: item.index,
            event: serialize(&item.event),
        })
        .collect();
    ErasedReplay {
        epoch: replay.epoch,
        backlog,
        live: ErasedLive {
            receiver: Box::new(TypedLive {
                receive: receive(replay.live),
                serialize,
            }),
        },
    }
}

/// Register and attach a fresh mux subscription on an existing typed log.
///
/// Call while holding every lock under which the state and the log change.
/// Render `snapshot_parts` under those same guards BEFORE calling this helper,
/// and keep all guards until it returns. For separate state and log mutexes,
/// acquire state -> log, matching the writers' lock order. A store performing
/// flush -> build snapshot -> register must keep its store guard through attach
/// too. Do not separately register before calling this helper.
///
/// This performs `register_at` + `attach` under the log's exclusive borrow and
/// erases the resulting replay; no event can intervene between registration
/// and attachment. Application state/flush/rendering remains the caller's job.
///
/// Returns an error BEFORE registering if `snapshot_parts` is empty.
pub fn open_on_log<E>(
    log: &mut EventLog<E>,
    snapshot_parts: Vec<String>,
    serialize: EventSerializer<E>,
    now: Instant,
) -> Result<Opened, String>
where
    E: Clone + Send + 'static,
{
    if snapshot_parts.is_empty() {
        return Err("snapshot must have at least one part".into());
    }
    let (sub_id, cursor) = log.register_at(None, now);
    let replay = log
        .attach(&sub_id)
        .expect("freshly registered channel must attach");
    Ok(Opened {
        snapshot: snapshot_parts,
        sub_id,
        cursor,
        replay: erase_replay(replay, serialize),
    })
}

/// Snapshot rendering + registration + attachment under the source's barrier
/// locks. Use [`open_on_log`] with the guards still held after rendering.
pub struct Opened<P = Vec<String>> {
    /// At least one snapshot part, in order. The default preserves synchronous
    /// sources; async opens use [`SnapshotParts`].
    pub snapshot: P,
    pub sub_id: SubId,
    pub cursor: Cursor,
    pub replay: ErasedReplay,
}

impl<P> Opened<P> {
    /// Replace snapshot production without changing the established log barrier.
    /// The replacement MUST describe the same immutable state at `cursor`.
    pub fn with_snapshot<Q>(self, snapshot: Q) -> Opened<Q> {
        Opened {
            snapshot,
            sub_id: self.sub_id,
            cursor: self.cursor,
            replay: self.replay,
        }
    }
}

/// An owned asynchronous open. Before returning `Opened`, the future owns any
/// registered handles and MUST release them on cancellation or error (RAII).
/// After success the mux owns their lifecycle. No source locks may cross await.
pub type OpenFuture =
    Pin<Box<dyn Future<Output = Result<Opened<SnapshotParts>, String>> + Send + 'static>>;

/// One snapshot frame. Explicit `last` avoids lookahead: a ready part is emitted
/// immediately even when the next part is not ready yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotPart {
    pub data: String,
    pub last: bool,
}

type PollPart = dyn FnMut(&mut Context<'_>) -> Poll<Option<Result<SnapshotPart, String>>> + Send;

/// Owned, pull-driven snapshot production, without a forwarding task or queue.
/// Pending producers MUST register the supplied waker. At least one part with
/// `last = true` is required; premature EOF and errors fail only this channel.
/// Production stops and the producer is dropped immediately after `last`.
pub struct SnapshotParts {
    poll: Box<PollPart>,
}

impl SnapshotParts {
    pub fn new(
        poll: impl FnMut(&mut Context<'_>) -> Poll<Option<Result<SnapshotPart, String>>>
            + Send
            + 'static,
    ) -> Self {
        Self {
            poll: Box::new(poll),
        }
    }

    pub fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<SnapshotPart, String>>> {
        (self.poll)(cx)
    }
}

impl From<Vec<String>> for SnapshotParts {
    fn from(parts: Vec<String>) -> Self {
        let mut parts = parts.into_iter();
        Self::new(move |_| {
            Poll::Ready(parts.next().map(|data| {
                Ok(SnapshotPart {
                    data,
                    last: parts.len() == 0,
                })
            }))
        })
    }
}

/// Type-erased channel source. Log operations must use the same log lock as
/// appends. Fresh opens must additionally hold the application state barrier
/// through rendering, registration and attachment. Prefer [`Channel`] for the
/// single-lock case, or [`open_on_log`] under the source's own guards. Typed
/// sources use [`erase_replay`] after `attach_from` to implement resume.
pub trait MuxSource: Send + Sync + 'static {
    /// Start promptly; slow work belongs in the returned future. The default
    /// adapts existing synchronous sources, ready on its first poll. The owned
    /// receiver makes cancelling even an unpolled adapter safe. The mux polls
    /// once at attachment to preserve existing synchronous open timing.
    /// Async-only sources override this method and need not implement open_fresh.
    /// `now` is the attempt's start time; after waiting, register with a fresh
    /// Instant at the actual snapshot barrier, not with this stale timestamp.
    fn open(self: Arc<Self>, now: Instant) -> OpenFuture {
        Box::pin(async move {
            let opened = self.open_fresh(now)?;
            if opened.snapshot.is_empty() {
                self.release(&opened.sub_id);
                return Err("snapshot must have at least one part".into());
            }
            let Opened {
                snapshot,
                sub_id,
                cursor,
                replay,
            } = opened;
            Ok(Opened {
                snapshot: snapshot.into(),
                sub_id,
                cursor,
                replay,
            })
        })
    }

    fn open_fresh(&self, _now: Instant) -> Result<Opened, String> {
        Err("source requires asynchronous open".into())
    }
    fn resume(&self, sub_id: &str, hint: Option<Cursor>) -> Result<ErasedReplay, AttachError>;
    fn is_current(&self, sub_id: &str, epoch: Epoch) -> bool;
    fn advance(&self, sub_id: &str, epoch: Epoch, cursor: Cursor, now: Instant);
    fn disconnect(&self, sub_id: &str, epoch: Epoch, now: Instant);
    fn release(&self, sub_id: &str);
}

type Snapshot<S> = dyn Fn(&S) -> Result<Vec<String>, String> + Send + Sync;

/// Application state and serialized event log protected by one lock.
pub struct Channel<S> {
    inner: Mutex<(S, EventLog<String>)>,
    snapshot: Box<Snapshot<S>>,
}

impl<S> Channel<S> {
    pub fn new(
        state: S,
        log: EventLog<String>,
        snapshot: impl Fn(&S) -> Result<Vec<String>, String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Mutex::new((state, log)),
            snapshot: Box::new(snapshot),
        }
    }

    /// Change materialized state and append its corresponding events atomically.
    /// Also use this for log compaction and diagnostics. Never call the mux here.
    pub fn update<R>(&self, update: impl FnOnce(&mut S, &mut EventLog<String>) -> R) -> R {
        let mut guard = self.inner.lock().expect("channel mutex poisoned");
        let (state, log) = &mut *guard;
        update(state, log)
    }
}

impl<S: Send + 'static> MuxSource for Channel<S> {
    fn open_fresh(&self, now: Instant) -> Result<Opened, String> {
        let mut guard = self.inner.lock().expect("channel mutex poisoned");
        let (state, log) = &mut *guard;
        let snapshot = (self.snapshot)(state)?;
        open_on_log(log, snapshot, Arc::new(String::clone), now)
    }

    fn resume(&self, sub_id: &str, hint: Option<Cursor>) -> Result<ErasedReplay, AttachError> {
        self.update(|_, log| log.attach_from(sub_id, hint))
            .map(|replay| erase_replay(replay, Arc::new(String::clone)))
    }

    fn is_current(&self, sub_id: &str, epoch: Epoch) -> bool {
        self.update(|_, log| log.is_current(sub_id, epoch))
    }

    fn advance(&self, sub_id: &str, epoch: Epoch, cursor: Cursor, now: Instant) {
        self.update(|_, log| log.advance_at(sub_id, epoch, cursor, now));
    }

    fn disconnect(&self, sub_id: &str, epoch: Epoch, now: Instant) {
        self.update(|_, log| log.disconnect(sub_id, epoch, now));
    }

    fn release(&self, sub_id: &str) {
        self.update(|_, log| log.release(sub_id));
    }
}

/// Cloneable connection registry. Register families before cloning the handle.
#[derive(Clone)]
pub struct Mux {
    inner: Arc<Registry>,
}

struct Registry {
    config: MuxConfig,
    families: HashMap<String, Arc<dyn MuxFamily>>,
    connections: Mutex<HashMap<String, Arc<Mutex<Connection>>>>,
}

impl Mux {
    /// Panics if the heartbeat is zero or cannot be expressed in milliseconds.
    pub fn new(config: MuxConfig) -> Self {
        assert!(
            config.heartbeat.as_millis() > 0 && config.heartbeat.as_millis() <= u64::MAX as u128,
            "mux heartbeat must fit in nonzero u64 milliseconds"
        );
        Self {
            inner: Arc::new(Registry {
                config,
                families: HashMap::new(),
                connections: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Register a unique `[a-z][a-z0-9_-]*` family before sharing the mux.
    pub fn family(mut self, name: &str, family: impl MuxFamily) -> Result<Self, MuxError> {
        if !valid_family(name) {
            return Err(invalid(name, "invalid family name"));
        }
        let registry = Arc::get_mut(&mut self.inner).ok_or_else(|| MuxError::InvalidChange {
            reason: "register families before cloning the mux".into(),
        })?;
        if registry.families.contains_key(name) {
            return Err(invalid(name, "family already registered"));
        }
        registry.families.insert(name.into(), Arc::new(family));
        Ok(self)
    }

    pub fn config(&self) -> &MuxConfig {
        &self.inner.config
    }

    pub fn reserve(&self, channels: Vec<String>) -> Result<Reservation, MuxError> {
        self.reserve_at(channels, Instant::now())
    }

    pub fn reserve_at(&self, channels: Vec<String>, now: Instant) -> Result<Reservation, MuxError> {
        let entries = self.validate(&channels)?;
        self.compact(now);
        let mut connection = Connection::new(now + self.inner.config.ttl);
        let mut gens = Generations::new();
        for (key, mut entry) in entries {
            entry.gen = allocate(&mut connection.next_gen);
            gens.insert(key.clone(), entry.gen);
            connection.order.push_back(key.clone());
            connection.channels.insert(key, entry);
        }
        let conn = uuid::Uuid::new_v4().to_string();
        self.inner
            .connections
            .lock()
            .expect("mux registry mutex poisoned")
            .insert(conn.clone(), Arc::new(Mutex::new(connection)));
        Ok(Reservation {
            conn,
            gens,
            heartbeat_ms: self.heartbeat_ms(),
        })
    }

    /// Attach or supersede a stream; matching hints rewind via `attach_from`.
    pub fn stream(&self, conn: &str, hints: ResumeHints) -> Result<MuxStream, MuxError> {
        self.stream_at(conn, hints, Instant::now())
    }

    pub fn stream_at(
        &self,
        conn: &str,
        hints: ResumeHints,
        now: Instant,
    ) -> Result<MuxStream, MuxError> {
        self.compact(now);
        let registry = self
            .inner
            .connections
            .lock()
            .expect("mux registry mutex poisoned");
        let shared = registry.get(conn).ok_or_else(|| unknown(conn))?.clone();
        let mut connection = shared.lock().expect("mux connection mutex poisoned");
        connection.wake();
        connection.epoch = connection
            .epoch
            .checked_add(1)
            .expect("mux stream epoch exhausted");
        connection.expires_at = None;
        connection.removed.clear();
        let Connection {
            channels, next_gen, ..
        } = &mut *connection;
        for (key, entry) in channels {
            entry.attach(hints.get(key).copied(), next_gen, now);
        }
        let epoch = connection.epoch;
        drop(connection);
        Ok(MuxStream {
            connection: shared,
            conn: conn.into(),
            epoch,
            ttl: self.inner.config.ttl,
            heartbeat_ms: self.heartbeat_ms(),
            hello: true,
            ended: false,
        })
    }

    /// Atomically update the desired set. Repeated adds/removes are idempotent.
    pub fn change(
        &self,
        conn: &str,
        add: Vec<String>,
        remove: Vec<String>,
    ) -> Result<Generations, MuxError> {
        self.change_at(conn, add, remove, Instant::now())
    }

    pub fn change_at(
        &self,
        conn: &str,
        add: Vec<String>,
        remove: Vec<String>,
        now: Instant,
    ) -> Result<Generations, MuxError> {
        self.compact(now);
        let registry = self
            .inner
            .connections
            .lock()
            .expect("mux registry mutex poisoned");
        let shared = registry.get(conn).ok_or_else(|| unknown(conn))?;
        if add.iter().any(|key| remove.contains(key)) {
            return Err(MuxError::InvalidChange {
                reason: "a channel cannot be both added and removed".into(),
            });
        }
        let additions = self.validate(&add)?;
        // Removal of an unknown key is a no-op, but malformed keys still refuse.
        for key in &remove {
            parse_key(key)?;
        }
        let mut connection = shared.lock().expect("mux connection mutex poisoned");
        let removals: std::collections::BTreeSet<_> = remove
            .into_iter()
            .filter(|key| connection.channels.contains_key(key))
            .collect();
        if connection.expires_at.is_none()
            && connection.removed.len() + removals.len() > self.inner.config.control_capacity
        {
            return Err(MuxError::Busy {
                reason: "stream control queue is full; drain the stream and retry".into(),
            });
        }
        for key in removals {
            let entry = connection.channels.remove(&key).expect("checked removal");
            connection.order.retain(|existing| existing != &key);
            if connection.expires_at.is_none() {
                connection.removed.push_back(MuxFrame::Removed {
                    ch: key,
                    gen: entry.gen,
                });
            }
            drop(entry); // Releases its EventLog handle before returning the command.
        }
        let mut gens = Generations::new();
        for (key, mut entry) in additions {
            if let Some(existing) = connection.channels.get(&key) {
                gens.insert(key, existing.gen);
            } else {
                entry.gen = allocate(&mut connection.next_gen);
                gens.insert(key.clone(), entry.gen);
                connection.order.push_back(key.clone());
                connection.channels.insert(key, entry);
            }
        }
        connection.wake();
        Ok(gens)
    }

    /// Idempotently release all handles and wake any attached stream to end it.
    pub fn release(&self, conn: &str) {
        if let Some(shared) = self
            .inner
            .connections
            .lock()
            .expect("mux registry mutex poisoned")
            .remove(conn)
        {
            shared
                .lock()
                .expect("mux connection mutex poisoned")
                .release();
        }
    }

    /// Reclaim expired reservations and every handle they own. No sleeping.
    pub fn compact(&self, now: Instant) {
        self.inner
            .connections
            .lock()
            .expect("mux registry mutex poisoned")
            .retain(|_, shared| {
                let mut connection = shared.lock().expect("mux connection mutex poisoned");
                if connection.expires_at.is_some_and(|expiry| expiry <= now) {
                    connection.release();
                    false
                } else {
                    true
                }
            });
    }

    pub fn connection_count(&self) -> usize {
        self.inner
            .connections
            .lock()
            .expect("mux registry mutex poisoned")
            .len()
    }

    fn heartbeat_ms(&self) -> u64 {
        self.inner.config.heartbeat.as_millis() as u64
    }

    fn validate(&self, keys: &[String]) -> Result<Vec<(String, Entry)>, MuxError> {
        let mut entries = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for key in keys {
            let (family, param) = parse_key(key)?;
            let provider = self
                .inner
                .families
                .get(family)
                .ok_or_else(|| invalid(key, "unknown family"))?;
            if seen.insert(key) {
                entries.push((
                    key.clone(),
                    Entry {
                        gen: 0,
                        family: provider.clone(),
                        param: param.map(str::to_owned),
                        source: None,
                        active: None,
                        loading: None,
                        attempted: false,
                        error: None,
                    },
                ));
            }
        }
        Ok(entries)
    }
}

fn valid_family(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
        })
}

fn parse_key(key: &str) -> Result<(&str, Option<&str>), MuxError> {
    let (family, param) = key
        .split_once(':')
        .map_or((key, None), |(family, param)| (family, Some(param)));
    if !valid_family(family) || key.contains(',') {
        return Err(invalid(
            key,
            "expected family or family:param, without commas",
        ));
    }
    Ok((family, param))
}

fn invalid(key: &str, reason: &str) -> MuxError {
    MuxError::InvalidChannel {
        channel: key.into(),
        reason: reason.into(),
    }
}

fn unknown(conn: &str) -> MuxError {
    MuxError::NeedsReservation {
        conn: conn.into(),
        reason: "connection unknown or expired; reserve again".into(),
    }
}

fn allocate(next: &mut Generation) -> Generation {
    *next = next.checked_add(1).expect("mux generation exhausted");
    *next
}

struct Connection {
    channels: BTreeMap<String, Entry>,
    order: VecDeque<String>,
    removed: VecDeque<MuxFrame>,
    next_gen: Generation,
    epoch: u64,
    expires_at: Option<Instant>,
    released: bool,
    waker: Option<Waker>,
}

impl Connection {
    fn new(expiry: Instant) -> Self {
        Self {
            channels: BTreeMap::new(),
            order: VecDeque::new(),
            removed: VecDeque::new(),
            next_gen: 0,
            epoch: 0,
            expires_at: Some(expiry),
            released: false,
            waker: None,
        }
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn release(&mut self) {
        self.released = true;
        self.channels.clear();
        self.order.clear();
        self.removed.clear();
        self.wake();
    }
}

struct Entry {
    gen: Generation,
    family: Arc<dyn MuxFamily>,
    param: Option<String>,
    source: Option<Arc<dyn MuxSource>>,
    active: Option<Active>,
    loading: Option<OpenFuture>,
    attempted: bool,
    error: Option<String>,
}

impl Entry {
    fn attach(
        &mut self,
        hint: Option<(Generation, Cursor)>,
        next_gen: &mut Generation,
        now: Instant,
    ) {
        if let (Some(source), Some(active)) = (&self.source, &mut self.active) {
            let usable = hint.filter(|(gen, _)| *gen == self.gen && active.snapshot.is_none());
            if usable.is_some() || !active.handed {
                if let Ok(replay) = source.resume(&active.sub_id, usable.map(|(_, cursor)| cursor))
                {
                    active.set_replay(replay);
                    return;
                }
            }
        }
        self.fresh(next_gen, now);
    }

    fn fresh(&mut self, next_gen: &mut Generation, now: Instant) {
        self.release_active();
        if self.attempted {
            self.gen = allocate(next_gen);
        }
        self.attempted = true;
        self.error = None;
        if self.source.is_none() {
            match self.family.open(self.param.as_deref()) {
                Ok(source) => self.source = Some(source),
                Err(reason) => {
                    self.fail(reason);
                    return;
                }
            }
        }
        self.loading = Some(
            self.source
                .as_ref()
                .expect("opened source")
                .clone()
                .open(now),
        );
        // Preserve immediate sync-open attachment (including unpolled streams).
        // Pending futures are polled again with the real stream waker before
        // the stream can return Pending, so this initial probe cannot lose wakeups.
        let waker = Waker::from(Arc::new(OpenWake));
        self.poll_open(&mut Context::from_waker(&waker));
    }

    fn poll_open(&mut self, cx: &mut Context<'_>) {
        let Some(loading) = &mut self.loading else {
            return;
        };
        let Poll::Ready(opened) = loading.as_mut().poll(cx) else {
            return;
        };
        self.loading = None;
        match opened {
            Ok(opened) => {
                let mut active = Active {
                    sub_id: opened.sub_id,
                    epoch: opened.replay.epoch,
                    base: opened.cursor,
                    snapshot: Some(opened.snapshot),
                    part: 0,
                    handed: false,
                    backlog: VecDeque::new(),
                    live: None,
                };
                active.set_replay(opened.replay);
                self.active = Some(active);
            }
            Err(reason) => self.fail(reason),
        }
    }

    fn fail(&mut self, reason: String) {
        self.release_active();
        self.error = Some(if reason.is_empty() {
            "provider refused the channel".into()
        } else {
            reason
        });
    }

    fn release_active(&mut self) {
        self.loading = None;
        if let (Some(source), Some(active)) = (&self.source, self.active.take()) {
            source.release(&active.sub_id);
        }
    }

    fn poll(
        &mut self,
        key: &str,
        next_gen: &mut Generation,
        cx: &mut Context<'_>,
    ) -> Poll<MuxFrame> {
        if !self.attempted {
            self.fresh(next_gen, Instant::now());
        }
        // One retry only: a continuously mutating provider cannot spin this poll.
        for _ in 0..2 {
            self.poll_open(cx);
            if let Some(reason) = self.error.take() {
                return Poll::Ready(MuxFrame::Error {
                    ch: key.into(),
                    gen: self.gen,
                    reason,
                });
            }
            let Some(active) = &mut self.active else {
                return Poll::Pending;
            };
            let source = self.source.as_ref().expect("active channel source");
            if !source.is_current(&active.sub_id, active.epoch) {
                self.fresh(next_gen, Instant::now());
                continue;
            }
            if let Some(snapshot) = &mut active.snapshot {
                let item = match snapshot.poll_next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(item))) => item,
                    Poll::Ready(result) => {
                        let reason = match result {
                            Some(Err(reason)) => reason,
                            None => "snapshot ended before its last part".into(),
                            Some(Ok(_)) => unreachable!(),
                        };
                        self.fail(reason);
                        continue;
                    }
                };
                let part = active.part;
                active.part += 1;
                active.handed = true;
                if item.last {
                    active.snapshot = None;
                }
                return Poll::Ready(MuxFrame::Snapshot {
                    ch: key.into(),
                    gen: self.gen,
                    cursor: active.base,
                    part,
                    last: item.last,
                    data: item.data,
                });
            }
            match active.poll_event(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(item)) => {
                    source.advance(&active.sub_id, active.epoch, item.index, Instant::now());
                    active.handed = true;
                    return Poll::Ready(MuxFrame::Delta {
                        ch: key.into(),
                        gen: self.gen,
                        cursor: item.index,
                        data: item.event,
                    });
                }
                Poll::Ready(Err(broadcast::error::RecvError::Lagged(_))) => {
                    match source.resume(&active.sub_id, None) {
                        Ok(replay) => active.set_replay(replay),
                        Err(_) => self.fresh(next_gen, Instant::now()),
                    }
                }
                Poll::Ready(Err(broadcast::error::RecvError::Closed)) => {
                    self.release_active();
                    return Poll::Ready(MuxFrame::Error {
                        ch: key.into(),
                        gen: self.gen,
                        reason: "channel event source closed".into(),
                    });
                }
            }
            if let Some(reason) = self.error.take() {
                return Poll::Ready(MuxFrame::Error {
                    ch: key.into(),
                    gen: self.gen,
                    reason,
                });
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

struct OpenWake;

impl Wake for OpenWake {
    fn wake(self: Arc<Self>) {}
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.release_active();
    }
}

type Received<E> = Result<Indexed<E>, broadcast::error::RecvError>;
type Receive<E> =
    Pin<Box<dyn Future<Output = (broadcast::Receiver<Indexed<E>>, Received<E>)> + Send>>;

trait LiveReceiver: Send {
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Received<String>>;
}

struct TypedLive<E> {
    receive: Receive<E>,
    serialize: EventSerializer<E>,
}

impl<E: Clone + Send + 'static> LiveReceiver for TypedLive<E> {
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Received<String>> {
        match self.receive.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((live, result)) => {
                self.receive = receive(live);
                Poll::Ready(result.map(|item| Indexed {
                    index: item.index,
                    event: (self.serialize)(&item.event),
                }))
            }
        }
    }
}

fn receive<E: Clone + Send + 'static>(mut live: broadcast::Receiver<Indexed<E>>) -> Receive<E> {
    Box::pin(async move {
        let result = live.recv().await;
        (live, result)
    })
}

struct Active {
    sub_id: SubId,
    epoch: Epoch,
    base: Cursor,
    snapshot: Option<SnapshotParts>,
    part: usize,
    handed: bool,
    backlog: VecDeque<Indexed<String>>,
    live: Option<ErasedLive>,
}

impl Active {
    fn set_replay(&mut self, replay: ErasedReplay) {
        self.epoch = replay.epoch;
        self.backlog = replay.backlog.into();
        self.live = Some(replay.live);
    }

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Received<String>> {
        if let Some(item) = self.backlog.pop_front() {
            return Poll::Ready(Ok(item));
        }
        self.live.as_mut().expect("live receiver").poll_recv(cx)
    }
}

/// One authoritative pull stream. Dropping even an unpolled stream disconnects
/// every handle; stale stream cleanup cannot affect its replacement.
pub struct MuxStream {
    connection: Arc<Mutex<Connection>>,
    conn: String,
    epoch: u64,
    ttl: Duration,
    heartbeat_ms: u64,
    hello: bool,
    ended: bool,
}

impl MuxStream {
    /// Cancellation-safe: a cancelled pending `next` never consumes a frame.
    pub async fn next(&mut self) -> Option<MuxFrame> {
        poll_fn(|cx| self.poll_next(cx)).await
    }

    /// Return at most one frame, advancing a delta's log cursor at this boundary.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<MuxFrame>> {
        if self.ended {
            return Poll::Ready(None);
        }
        let mut connection = self
            .connection
            .lock()
            .expect("mux connection mutex poisoned");
        if connection.released || connection.epoch != self.epoch {
            self.ended = true;
            return Poll::Ready(None);
        }
        connection.waker = Some(cx.waker().clone());
        if self.hello {
            self.hello = false;
            return Poll::Ready(Some(MuxFrame::Hello {
                conn: self.conn.clone(),
                heartbeat_ms: self.heartbeat_ms,
            }));
        }
        if let Some(frame) = connection.removed.pop_front() {
            return Poll::Ready(Some(frame));
        }
        let Connection {
            channels,
            order,
            next_gen,
            ..
        } = &mut *connection;
        for _ in 0..order.len() {
            let key = order.pop_front().expect("nonempty rotation");
            let result = channels
                .get_mut(&key)
                .expect("desired channel in rotation")
                .poll(&key, next_gen, cx);
            order.push_back(key);
            if let Poll::Ready(frame) = result {
                return Poll::Ready(Some(frame));
            }
        }
        Poll::Pending
    }

    /// Explicit-clock disconnect for deterministic lifetime management and tests.
    pub fn disconnect_at(mut self, now: Instant) {
        self.disconnect(now);
    }

    fn disconnect(&mut self, now: Instant) {
        if self.ended {
            return;
        }
        let mut connection = self
            .connection
            .lock()
            .expect("mux connection mutex poisoned");
        if !connection.released && connection.epoch == self.epoch {
            for entry in connection.channels.values_mut() {
                // A pending open may own a handle not yet visible to the mux.
                // Dropping it invokes the source's cancellation cleanup.
                entry.loading = None;
                if let (Some(source), Some(active)) = (&entry.source, &entry.active) {
                    source.disconnect(&active.sub_id, active.epoch, now);
                }
            }
            connection.expires_at = Some(now + self.ttl);
            connection.waker = None;
        }
        self.ended = true;
    }
}

impl Drop for MuxStream {
    fn drop(&mut self) {
        self.disconnect(Instant::now());
    }
}

#[cfg(feature = "axum")]
impl futures_core::Stream for MuxStream {
    type Item = MuxFrame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        MuxStream::poll_next(self.get_mut(), cx)
    }
}
