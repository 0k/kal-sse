//! Framework-agnostic core of the resilient SSE contract.
//!
//! [`EventLog`] is an append-only, index-tagged event log with a
//! per-subscription cursor registry. It owns the "intelligent buffer":
//! it retains undelivered events and a timed redelivery window of sent
//! events after each subscription's snapshot, subject to a hard cap.
//! A resume hint can rewind the server cursor within those bounds.
//!
//! It knows nothing about HTTP, SSE wire format, axum, or `serde` — it
//! is a pure data structure plus a `tokio` broadcast channel for live
//! fan-out. The `axum` adapter (feature `axum`) renders it onto the
//! wire.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

/// Monotonic position on a channel. Starts at `0` (a fresh log has
/// `last_index == 0` and no events, so a subscription `since == 0`
/// replays nothing). Each appended event gets `previous + 1`.
pub type Cursor = u64;

/// Default retention window after events are sent: 60 s.
///
/// This is four times the adapter's 15 s keep-alive interval, so a closed
/// client is normally detected well within the window. The hard event cap
/// still takes precedence over this grace period.
pub const DEFAULT_REDELIVERY_WINDOW: Duration = Duration::from_secs(60);

/// Stable identifier for one logical subscription to one data channel.
///
/// One per data flow a client subscribes to — NOT per tab / window /
/// app, and held by one client at a time. The server mints it at the
/// first snapshot ([`EventLog::register`] with `None`); the client
/// proposes it again on every later snapshot and sends it on every
/// stream request, so the server refreshes the existing handle rather
/// than leak a new one. A proposed id that is not a hyphenated UUID is
/// ignored and a fresh one minted.
pub type SubId = String;

/// An event paired with the monotonic index at which it was appended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Indexed<E> {
    /// Position of `event` on the channel (`1` for the first event).
    pub index: Cursor,
    /// The event payload.
    pub event: E,
}

/// Why an [`EventLog::attach`] could not resume a subscription. In
/// every case the remedy is the same: the client must re-take the REST
/// snapshot (which mints/re-registers the `sub_id` at a fresh cursor)
/// and reconnect. The variants exist for diagnostics / wire detail.
///
/// Marked `#[non_exhaustive]`: future versions may add variants (for
/// example a malformed-`sub_id` rejection), so match with a wildcard
/// arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AttachError {
    /// No handle exists for this `sub_id`: it was never registered via
    /// [`EventLog::register`], or its disconnect TTL lapsed and it was
    /// reclaimed by [`EventLog::compact`] (also when an unattached handle
    /// can no longer resume), or the daemon restarted and
    /// lost in-memory state. The client must snapshot to (re)register.
    UnknownSub,
    /// A handle exists but events after the requested resume point are
    /// no longer retained, due to hard-cap eviction or expiration of the
    /// redelivery window for previously sent events.
    /// `earliest` is the oldest retained index; `current` is
    /// `last_index`.
    Expired {
        /// Oldest index still retained in the buffer.
        earliest: Cursor,
        /// The log's `last_index` at the time of the attach.
        current: Cursor,
    },
}

impl std::fmt::Display for AttachError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttachError::UnknownSub => {
                write!(f, "unknown subscription: snapshot required")
            }
            AttachError::Expired { earliest, current } => write!(
                f,
                "expired cursor: earliest retained = {earliest}, current = {current}"
            ),
        }
    }
}

impl std::error::Error for AttachError {}

/// Opaque generation token identifying ONE attach of a `sub_id`.
///
/// Re-attaching the same `sub_id` (a client opening a second
/// subscription to the same data flow, or a reconnect) bumps the
/// generation and thereby **invalidates the prior attach**: the
/// superseded stream's [`EventLog::advance`] / [`EventLog::disconnect`]
/// calls become no-ops, and [`EventLog::is_current`] reports `false`
/// so the adapter can terminate it. At most one live attach per
/// `sub_id` is ever authoritative.
/// Generations are never reused during the life of an event log, even after
/// a handle is forgotten and its subscription id is registered again.
pub type Epoch = u64;

/// The result of attaching a subscription: the events to replay
/// (strictly after the requested cursor) plus a live receiver for
/// everything appended afterwards. The two are produced atomically
/// (under the same critical section) so no event slips between the
/// last replayed index and the first live one.
pub struct Replay<E: Clone> {
    /// Generation token for THIS attach. Pass it to `advance` /
    /// `disconnect` / `is_current`; a superseded attach holds a stale
    /// epoch and its mutations are ignored.
    pub epoch: Epoch,
    /// Events with index strictly greater than the requested `since`,
    /// in order. May be empty when the subscription is already current.
    pub backlog: Vec<Indexed<E>>,
    /// Live events appended after the subscription attached.
    pub live: broadcast::Receiver<Indexed<E>>,
}

/// Per-subscription bookkeeping kept in the registry.
#[derive(Debug)]
struct Handle {
    /// Generation of the CURRENT authoritative attach for this
    /// `sub_id`. Bumped on every `attach`; mutations carrying a stale
    /// epoch are ignored (the prior subscription was superseded).
    epoch: Epoch,
    /// Snapshot index, reset only by registration. Resume hints and
    /// retention cannot rewind below this position.
    base: Cursor,
    /// Server delivery position (initially `base`). Advanced by
    /// [`EventLog::advance`]; a successful attach with a resume hint may
    /// rewind it, but never below `base`.
    cursor: Cursor,
    /// Time of the last advance that moved the cursor.
    advanced_at: Option<Instant>,
    /// Cursor before each send batch and its hand-off time. Recent batches
    /// survive reconnects, including cursor rewinds, until their window ends.
    sent: VecDeque<(Cursor, Instant)>,
    /// `None` while the subscription's stream is attached (it pins the
    /// buffer indefinitely as long as it keeps consuming). `Some(t)`
    /// while no stream is attached — between the snapshot
    /// ([`EventLog::register`]) and the first attach, and after the
    /// stream drops: the handle is retained until `t`, then reclaimed
    /// by [`EventLog::compact`], or sooner if it cannot resume.
    expires_at: Option<Instant>,
}

impl Handle {
    fn prune_sent(&mut self, now: Instant, window: Duration) {
        if self
            .advanced_at
            .is_some_and(|sent_at| now.saturating_duration_since(sent_at) >= window)
        {
            self.sent.clear();
            return;
        }
        // Send times are nondecreasing, so expired records are at the front.
        while self
            .sent
            .front()
            .is_some_and(|(_, sent_at)| now.saturating_duration_since(*sent_at) >= window)
        {
            self.sent.pop_front();
        }
    }

    fn resume_floor(&self) -> Cursor {
        self.base.max(
            self.sent
                .iter()
                .map(|(before, _)| *before)
                .min()
                .unwrap_or(self.cursor)
                .min(self.cursor),
        )
    }
}

/// Append-only, index-tagged event log with a per-subscription cursor
/// registry and cursor-frontier retention with a timed redelivery window.
///
/// `E` is the channel's event type. It must be `Clone` (each live
/// subscriber gets its own copy via the broadcast channel) and
/// `Send + 'static` (it crosses the broadcast boundary).
pub struct EventLog<E: Clone + Send + 'static> {
    /// Retained events, oldest first. Indices are
    /// contiguous and ascending; the last event has `last_index` when
    /// non-empty.
    events: VecDeque<Indexed<E>>,
    /// Index of the most recently appended event. Starts at `0`.
    last_index: Cursor,
    /// Log-wide generation allocator, independent of handle reclamation.
    next_epoch: Epoch,
    /// Live fan-out. Lagged receivers get `Lagged` and must resync via
    /// a fresh snapshot + [`EventLog::attach`].
    subs: broadcast::Sender<Indexed<E>>,
    /// Subscription registry: `sub_id -> handle`. A handle pins the
    /// buffer for undelivered events and young sent events after `base`.
    /// Attached handles do not expire; unattached handles survive up to
    /// `ttl`, unless the retained buffer no longer permits resume.
    handles: HashMap<SubId, Handle>,
    /// How long a handle with no attached stream (registered but not
    /// yet attached, or disconnected) is retained before
    /// [`compact`](Self::compact) may reclaim it.
    ttl: Duration,
    /// Grace period after hand-off during which sent events remain
    /// replayable on behalf of surviving handles, bounded by the cap.
    redelivery_window: Duration,
    /// Hard upper bound on `events.len()`. Even when a handle would pin
    /// more, the oldest events past this are dropped (such a handle
    /// then gets [`AttachError::Expired`] on resume, or is forgotten by
    /// compaction if unattached — a safety valve against unbounded memory).
    max_events: usize,
}

impl<E: Clone + Send + 'static> EventLog<E> {
    /// Build an empty log.
    ///
    /// * `ttl` — grace period of a subscription with no attached
    ///   stream: how long a dropped subscription's resume point is held
    ///   so a reconnect resumes gap-free, and how long a snapshot's
    ///   registration waits for its first attach. There is no default;
    ///   15 minutes is a reasonable choice.
    /// * `max_events` — hard cap on retained events (memory safety
    ///   valve).
    /// * `channel_capacity` — `tokio::broadcast` buffer depth for live
    ///   fan-out; a consumer slower than this many events behind gets
    ///   `Lagged` and must resync.
    ///
    /// Sent events are retained for [`DEFAULT_REDELIVERY_WINDOW`] after
    /// being sent on behalf of surviving handles. Override this with
    /// [`with_redelivery_window`](Self::with_redelivery_window).
    pub fn new(ttl: Duration, max_events: usize, channel_capacity: usize) -> Self {
        let (subs, _) = broadcast::channel(channel_capacity.max(1));
        Self {
            events: VecDeque::new(),
            last_index: 0,
            next_epoch: 0,
            subs,
            handles: HashMap::new(),
            ttl,
            redelivery_window: DEFAULT_REDELIVERY_WINDOW,
            max_events: max_events.max(1),
        }
    }

    /// Set the retention window for redelivering already-sent events.
    ///
    /// The window runs after being sent (cursor advancement), never retains
    /// events covered by a handle's snapshot, and does not override handle
    /// TTL or the `max_events` cap.
    pub fn with_redelivery_window(mut self, window: Duration) -> Self {
        self.redelivery_window = window;
        self
    }

    /// Append an event, returning its freshly assigned index. Fans it
    /// out to all live subscribers (best-effort: lagged receivers will
    /// resync).
    pub fn append(&mut self, event: E) -> Cursor {
        self.last_index += 1;
        let idx = self.last_index;
        let indexed = Indexed { index: idx, event };
        self.events.push_back(indexed.clone());
        // Enforce the hard cap immediately (cursor-aware trimming
        // happens in `compact`; this is only the safety valve).
        while self.events.len() > self.max_events {
            self.events.pop_front();
        }
        let _ = self.subs.send(indexed);
        idx
    }

    /// Current `last_index`. This is the cursor a REST snapshot hands
    /// out alongside the materialised state. Registration pins the
    /// handle's snapshot base and cursor here to receive subsequent events.
    pub fn last_index(&self) -> Cursor {
        self.last_index
    }

    /// Number of events currently retained (diagnostics / tests).
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the retained buffer is empty (diagnostics / tests).
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Number of registered subscription handles (diagnostics / tests).
    pub fn handle_count(&self) -> usize {
        self.handles.len()
    }

    /// Release a subscription immediately, invalidating any attached stream.
    /// Unknown ids are a no-op. Unlike disconnect, this does not wait for TTL.
    pub fn release(&mut self, sub_id: &str) {
        self.handles.remove(sub_id);
    }

    /// Raw live broadcast receiver, bypassing the `sub_id` registry.
    ///
    /// This is the escape hatch for **internal** consumers that want
    /// live fan-out without participating in the resilient-SSE contract
    /// — they have no `sub_id`, no replay backlog, no cursor advance,
    /// no TTL retention.  A lagged consumer simply misses events; if it
    /// needs to resync it must do so out-of-band (the log will not
    /// retain anything on its behalf).
    ///
    /// Typical caller: an in-process reconciler that recomputes a
    /// derived view from current state on every event tick.  Such a
    /// consumer doesn't care about backlog (it always rebuilds from
    /// scratch) and explicitly does NOT want to pin the buffer, so
    /// using the regular [`register`](Self::register) / [`attach`](Self::attach)
    /// dance would leak a live handle that never TTL-expires.
    ///
    /// External consumers (HTTP/SSE clients) MUST go through
    /// `register` + `attach` to get gap-free reconnect.
    pub fn subscribe_raw(&self) -> broadcast::Receiver<Indexed<E>> {
        self.subs.subscribe()
    }

    /// Register a subscription at the current cursor — called by the
    /// REST **snapshot** endpoint, which is where a subscription is
    /// born or reset. Both the snapshot base and server cursor are reset
    /// to `last_index`; a later resume hint cannot rewind below this base.
    ///
    /// * `sub_id` — `Some(id)` to adopt a client-proposed hyphenated UUID
    ///   (which **supersedes any prior handle** for that id —
    ///   invalidating the prior subscription to this data flow), or
    ///   `None` to mint a fresh UUID. A proposed id that is not a
    ///   hyphenated UUID is ignored and a fresh one minted.
    ///
    /// Returns the effective `sub_id` (minted or adopted) and the
    /// cursor it was pinned at (`last_index` — the client is current up
    /// to here, the SSE stream delivers everything after).
    ///
    /// The TTL starts at registration: a client that takes the snapshot
    /// but never opens the SSE stream does not leak its handle — it is
    /// reclaimed by [`compact`](Self::compact) once the TTL lapses.
    /// [`attach`](Self::attach) clears the TTL for as long as the stream
    /// stays connected.
    ///
    /// Uses [`Instant::now`] as the registration time; see
    /// [`register_at`](Self::register_at) to supply it.
    pub fn register(&mut self, sub_id: Option<String>) -> (SubId, Cursor) {
        self.register_at(sub_id, Instant::now())
    }

    /// [`register`](Self::register) with an explicit registration time
    /// `now`, from which the TTL of a not-yet-attached handle runs (same
    /// clock as [`disconnect`](Self::disconnect) and
    /// [`compact`](Self::compact)).
    pub fn register_at(&mut self, sub_id: Option<String>, now: Instant) -> (SubId, Cursor) {
        let id = sub_id.filter(|id| is_sub_id(id)).unwrap_or_else(new_sub_id);
        let cursor = self.last_index;
        let epoch = self.allocate_epoch();
        self.handles.insert(
            id.clone(),
            Handle {
                epoch,
                base: cursor,
                cursor,
                advanced_at: None,
                sent: VecDeque::new(),
                expires_at: Some(now + self.ttl),
            },
        );
        (id, cursor)
    }

    /// Attach the SSE stream for an already-registered `sub_id`,
    /// atomically producing its replay backlog + live receiver and a
    /// fresh [`Epoch`].
    ///
    /// The resume cursor is read from the server-side handle (set by
    /// [`register`](Self::register) at snapshot time, then advanced on
    /// every delivered event). Equivalent to
    /// [`attach_from`](Self::attach_from) with no resume hint.
    ///
    /// Attaching bumps the handle's epoch, so if the same `sub_id` had
    /// another live stream (e.g. a reconnect racing the old socket's
    /// teardown), the older one is superseded: its `advance` /
    /// `disconnect` become no-ops and [`is_current`](Self::is_current)
    /// reports `false` for it.
    ///
    /// Errors:
    /// * [`AttachError::UnknownSub`] — no handle (never registered, or
    ///   reclaimed by compaction, or post-restart). Client must snapshot.
    /// * [`AttachError::Expired`] — handle exists but its cursor fell
    ///   behind the retained buffer (hard-cap eviction). Client must
    ///   snapshot.
    ///
    /// The live receiver is taken *before* the backlog is read so the
    /// two are consistent: any event appended after this call lands in
    /// the receiver; everything up to `last_index` is in the backlog.
    pub fn attach(&mut self, sub_id: &str) -> Result<Replay<E>, AttachError> {
        self.attach_from(sub_id, None)
    }

    /// Attach using an optional last-received event index as a resume hint.
    ///
    /// With no hint, resume from the server cursor. Otherwise resume from
    /// the hint, capped at the server cursor; a hint below the snapshot
    /// base returns [`AttachError::Expired`]: that client never saw the
    /// current snapshot of this `sub_id`. On success the
    /// cursor is set to this resume point, the epoch is bumped, and the
    /// unattached TTL is cleared without changing the snapshot base.
    /// The live receiver is obtained before reading the backlog, under
    /// the same exclusive borrow, just as in [`attach`](Self::attach).
    ///
    /// Returns [`AttachError::UnknownSub`] for an unknown handle, or
    /// [`AttachError::Expired`] if the requested backlog is unavailable.
    /// A failed attach leaves the handle unchanged.
    pub fn attach_from(
        &mut self,
        sub_id: &str,
        last_event_id: Option<Cursor>,
    ) -> Result<Replay<E>, AttachError> {
        let since = match self.handles.get(sub_id) {
            // A client behind the snapshot base never saw this snapshot of
            // the subscription (e.g. a superseded holder of the same id).
            Some(h) if last_event_id.is_some_and(|id| id < h.base) => {
                return Err(self.expired());
            }
            Some(h) => last_event_id.map_or(h.cursor, |id| id.min(h.cursor)),
            None => return Err(AttachError::UnknownSub),
        };
        // Subscribe first so nothing can be appended between computing
        // the backlog and obtaining the receiver.
        let live = self.subs.subscribe();
        let backlog = self.events_since(since)?;
        let epoch = self.allocate_epoch();
        // Bump the generation: any prior live stream for this sub_id is
        // now superseded. Re-pin live (clear any disconnect TTL).
        let handle = self.handles.get_mut(sub_id).expect("handle checked above");
        handle.epoch = epoch;
        handle.cursor = since;
        handle.expires_at = None;
        Ok(Replay {
            epoch,
            backlog,
            live,
        })
    }

    /// Whether `epoch` is the current authoritative attach for
    /// `sub_id`. A superseded stream sees `false` and should stop. An
    /// unknown `sub_id` (already reclaimed) is also `false`.
    pub fn is_current(&self, sub_id: &str, epoch: Epoch) -> bool {
        self.handles.get(sub_id).is_some_and(|h| h.epoch == epoch)
    }

    /// Advance a subscription's cursor to `index` — call this each time
    /// the corresponding event has been *delivered* to that
    /// subscription's SSE stream. The server delivery position advances,
    /// but sent events after the snapshot remain retained within the
    /// redelivery window, subject to the hard cap.
    ///
    /// No-op if the `sub_id` is unknown, if `epoch` is stale (a
    /// superseded subscription), or if `index` does not move the cursor
    /// forward.
    pub fn advance(&mut self, sub_id: &str, epoch: Epoch, index: Cursor) {
        self.advance_at(sub_id, epoch, index, Instant::now());
    }

    /// [`advance`](Self::advance) with an explicit HTTP hand-off time.
    ///
    /// Only an advance that moves the current attach's cursor starts a
    /// redelivery window. A batch advance treats all indexes between the
    /// previous cursor and `index` as sent at `now`. Use the same clock as
    /// [`compact`](Self::compact); callers should supply nondecreasing times.
    /// Recent send records are retained across reconnects and pruned to the
    /// window, so their number is bounded by advances within that window.
    pub fn advance_at(&mut self, sub_id: &str, epoch: Epoch, index: Cursor, now: Instant) {
        if let Some(h) = self.handles.get_mut(sub_id) {
            if h.epoch == epoch && index > h.cursor {
                h.prune_sent(now, self.redelivery_window);
                if !self.redelivery_window.is_zero() {
                    h.sent.push_back((h.cursor, now));
                }
                h.advanced_at = Some(now);
                h.cursor = index;
            }
        }
    }

    /// Mark a subscription's stream as disconnected: start its TTL
    /// countdown (`now + ttl`) so a reconnect within the window resumes
    /// from retained events, after which [`compact`](Self::compact) may
    /// reclaim it (or sooner if its cursor cannot resume). No-op for an
    /// unknown `sub_id` or a stale `epoch` (a superseded stream ending
    /// must NOT start a TTL on the handle that replaced it).
    pub fn disconnect(&mut self, sub_id: &str, epoch: Epoch, now: Instant) {
        if let Some(h) = self.handles.get_mut(sub_id) {
            if h.epoch == epoch {
                h.expires_at = Some(now + self.ttl);
            }
        }
    }

    /// Reclaim expired or unresumable unattached handles, then trim the
    /// buffer on a periodic tick.
    ///
    /// Order: expire TTLs, forget unattached handles whose next event
    /// precedes the buffer remaining after append's immediate cap trim,
    /// compute retention, then enforce the cap again. Attached handles
    /// are never forgotten for falling behind.
    ///
    /// Each surviving handle's floor is `max(base, min(cursor, before))`,
    /// where `before` is the lowest cursor before a send batch whose hand-off
    /// is still within the redelivery window (or `cursor` if none). This same
    /// floor decides whether an unattached handle can resume after cap
    /// eviction. Keep events above the minimum floor; no handles drops all.
    pub fn compact(&mut self, now: Instant) {
        // Drop handles whose disconnect grace has lapsed.
        self.handles
            .retain(|_, h| h.expires_at.is_none_or(|t| t > now));
        for h in self.handles.values_mut() {
            h.prune_sent(now, self.redelivery_window);
        }

        // append already enforced the cap: judge resumability against
        // the actual retained buffer before choosing a retention floor.
        let available_floor = self
            .events
            .front()
            .map_or(self.last_index, |first| first.index.saturating_sub(1));
        self.handles
            .retain(|_, h| h.expires_at.is_none() || h.resume_floor() >= available_floor);
        let floor = self.handles.values().map(Handle::resume_floor).min();
        let threshold = floor.unwrap_or(self.last_index);

        while let Some(front) = self.events.front() {
            if front.index <= threshold {
                self.events.pop_front();
            } else {
                break;
            }
        }

        // Hard cap (safety valve) — a handle pinning more than
        // `max_events` will get `AttachError::Expired` on resume.
        while self.events.len() > self.max_events {
            self.events.pop_front();
        }
    }

    /// [`AttachError::Expired`] describing the current retained buffer.
    fn expired(&self) -> AttachError {
        AttachError::Expired {
            earliest: self.events.front().map_or(self.last_index, |e| e.index),
            current: self.last_index,
        }
    }

    /// Events strictly after `since`, in order. `since == last_index`
    /// yields an empty vec. Returns [`AttachError::Expired`] when the
    /// next required event is no longer retained (including an empty
    /// buffer with `since < last_index`), or `since` is ahead of the log.
    fn events_since(&self, since: Cursor) -> Result<Vec<Indexed<E>>, AttachError> {
        if since == self.last_index {
            return Ok(Vec::new());
        }
        if since > self.last_index {
            return Err(AttachError::Expired {
                earliest: self.events.front().map(|e| e.index).unwrap_or(0),
                current: self.last_index,
            });
        }
        let earliest = self
            .events
            .front()
            .map(|e| e.index)
            .unwrap_or(self.last_index);
        // The client holds `since` and needs `since + 1` onward. If the
        // oldest event we still have is newer than `since + 1`, the gap
        // is unrecoverable.
        if self.events.is_empty() || since.saturating_add(1) < earliest {
            return Err(AttachError::Expired {
                earliest,
                current: self.last_index,
            });
        }
        Ok(self
            .events
            .iter()
            .filter(|e| e.index > since)
            .cloned()
            .collect())
    }

    fn allocate_epoch(&mut self) -> Epoch {
        let epoch = self.next_epoch;
        self.next_epoch = epoch.checked_add(1).expect("event log epoch exhausted");
        epoch
    }
}

/// Whether `id` is a hyphenated UUID, the only accepted `sub_id` shape.
fn is_sub_id(id: &str) -> bool {
    id.len() == 36 && uuid::Uuid::try_parse(id).is_ok()
}

/// Mint a fresh UUID-shaped subscription id (v4, random).
///
/// Kept as a free function (not requiring an `EventLog`) so callers
/// that mint ids outside the log — e.g. a snapshot handler that wants
/// to echo the id in the REST response before registering — can use
/// the same shape.
pub fn new_sub_id() -> SubId {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: &str = "0f3c9a52-1b7e-4d2a-9c6e-8a1f2b3c4d5e";

    fn log() -> EventLog<u32> {
        // Generous TTL/cap; tests drive time explicitly via `now`.
        EventLog::new(Duration::from_secs(900), 256, 64)
    }

    #[test]
    fn release_drops_handle_immediately_and_invalidates_its_attach() {
        let mut l = log();
        let (id, _) = l.register(None);
        let epoch = l.attach(&id).unwrap().epoch;
        l.append(1);
        l.release(&id);
        assert_eq!(l.handle_count(), 0);
        assert!(!l.is_current(&id, epoch));
        assert_eq!(l.attach(&id).err(), Some(AttachError::UnknownSub));
        l.release(&id); // Idempotent.
        l.compact(Instant::now());
        assert!(l.is_empty());
    }

    #[test]
    fn fresh_log_starts_at_zero() {
        let l = log();
        assert_eq!(l.last_index(), 0);
        assert!(l.is_empty());
    }

    #[test]
    fn redelivery_floor_uses_send_times_at_the_window_boundary() {
        let mut l = log();
        let (id, _) = l.register(None);
        let epoch = l.attach(&id).unwrap().epoch;
        let now = Instant::now();
        for value in 1..=3 {
            l.append(value);
        }
        l.advance_at(&id, epoch, 1, now - DEFAULT_REDELIVERY_WINDOW);
        l.advance_at(&id, epoch, 3, now);

        l.compact(now);

        assert_eq!(
            l.events.iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![2, 3]
        );
        // Even a window too large to represent as an Instant keeps sends.
        l.redelivery_window = Duration::MAX;
        l.compact(now);
        assert_eq!(l.len(), 2);
    }

    #[test]
    fn compact_forgets_capped_handle_before_computing_retention() {
        let mut l = EventLog::<u32>::new(Duration::from_secs(900), 2, 64);
        let (lost, _) = l.register(None);
        for value in 1..=4 {
            l.append(value);
        }
        let (current, _) = l.register(None);

        l.compact(Instant::now());

        assert_eq!(l.attach(&lost).err(), Some(AttachError::UnknownSub));
        assert_eq!(l.handle_count(), 1);
        assert!(
            l.is_empty(),
            "the current snapshot covers the capped buffer"
        );
        assert!(l.attach(&current).unwrap().backlog.is_empty());
    }

    #[test]
    fn append_assigns_monotonic_indices() {
        let mut l = log();
        assert_eq!(l.append(10), 1);
        assert_eq!(l.append(20), 2);
        assert_eq!(l.append(30), 3);
        assert_eq!(l.last_index(), 3);
    }

    // --- register: minting / adopting sub_id, pinning at cursor ---

    #[test]
    fn register_mints_uuid_when_none() {
        let mut l = log();
        l.append(10);
        let (id, cursor) = l.register(None);
        assert_eq!(cursor, 1, "pinned at current last_index");
        // UUID v4 string shape: 36 chars with dashes.
        assert_eq!(id.len(), 36);
        assert_eq!(id.matches('-').count(), 4);
        assert_eq!(l.handle_count(), 1);
    }

    #[test]
    fn register_adopts_proposed_sub_id() {
        let mut l = log();
        l.append(10);
        l.append(20);
        let (id, cursor) = l.register(Some(UUID.to_string()));
        assert_eq!(id, UUID);
        assert_eq!(cursor, 2);
    }

    #[test]
    fn register_same_sub_id_supersedes_prior() {
        // Re-using the same sub_id invalidates the prior subscription:
        // the old epoch is no longer current.
        let mut l = log();
        l.append(1);
        let (id, _) = l.register(Some(UUID.to_string()));
        // Grab the first epoch by attaching.
        let ep1 = l.attach(&id).unwrap().epoch;
        // Re-register the same id (client proposed it again) -> supersede.
        l.register(Some(UUID.to_string()));
        assert!(
            !l.is_current(UUID, ep1),
            "prior attach is no longer current after re-register"
        );
        assert_eq!(l.handle_count(), 1, "still a single handle for the id");
    }

    #[test]
    fn registered_but_never_attached_sub_is_reclaimed_after_ttl() {
        // A snapshot taken without ever opening the SSE stream must not
        // leak its handle (nor pin the buffer) forever.
        let mut l = EventLog::<u32>::new(Duration::from_secs(60), 256, 64);
        let t0 = Instant::now();
        let (id, _) = l.register_at(None, t0);
        l.append(1);
        l.append(2);

        // Within TTL: handle survives and pins the events after its cursor.
        l.compact(t0 + Duration::from_secs(30));
        assert_eq!(l.handle_count(), 1);
        assert_eq!(l.len(), 2);
        assert!(l.attach(&id).is_ok(), "attach within TTL resumes");

        // Same scenario, never attached: reclaimed once the TTL lapses.
        let mut l = EventLog::<u32>::new(Duration::from_secs(60), 256, 64);
        let (id, _) = l.register_at(None, t0);
        l.append(1);
        l.compact(t0 + Duration::from_secs(61));
        assert_eq!(l.handle_count(), 0);
        assert!(l.is_empty(), "reclaimed handle no longer pins the buffer");
        assert_eq!(l.attach(&id).err(), Some(AttachError::UnknownSub));
    }

    #[test]
    fn attach_clears_the_registration_ttl() {
        let mut l = EventLog::<u32>::new(Duration::from_secs(60), 256, 64);
        let t0 = Instant::now();
        let (id, _) = l.register_at(None, t0);
        l.attach(&id).unwrap();
        l.compact(t0 + Duration::from_secs(10_000));
        assert_eq!(l.handle_count(), 1, "attached sub never expires");
    }

    // --- attach: server-owned cursor without a resume hint ---

    #[test]
    fn attach_replays_from_registered_cursor() {
        let mut l = log();
        l.append(10);
        // Snapshot registers at cursor 1 (client is current up to 1).
        let (id, _) = l.register(None);
        // More events arrive before the SSE stream connects.
        l.append(20);
        l.append(30);
        let replay = l.attach(&id).unwrap();
        let idxs: Vec<_> = replay.backlog.iter().map(|e| e.index).collect();
        assert_eq!(idxs, vec![2, 3], "replays everything after the snapshot");
    }

    #[test]
    fn attach_at_current_replays_nothing() {
        let mut l = log();
        l.append(10);
        let (id, _) = l.register(None); // pinned at 1 == last_index
        let replay = l.attach(&id).unwrap();
        assert!(replay.backlog.is_empty());
    }

    #[test]
    fn attach_unknown_sub_is_error() {
        let mut l = log();
        l.append(1);
        match l.attach("never-registered") {
            Err(AttachError::UnknownSub) => {}
            Err(e) => panic!("expected UnknownSub, got {e:?}"),
            Ok(_) => panic!("expected UnknownSub, got Ok"),
        }
    }

    #[tokio::test]
    async fn live_receiver_gets_post_attach_events() {
        let mut l = log();
        l.append(10);
        let (id, _) = l.register(None);
        let mut replay = l.attach(&id).unwrap();
        assert!(replay.backlog.is_empty());
        l.append(99);
        let got = replay.live.recv().await.unwrap();
        assert_eq!(got.index, 2);
        assert_eq!(got.event, 99);
    }

    #[test]
    fn no_gap_between_backlog_and_live() {
        // The event appended "between" computing backlog and reading
        // live must appear in exactly one of them, never neither.
        let mut l = log();
        l.append(1);
        l.append(2);
        let (id, _) = l.register(Some("s1".into()));
        // Re-register at 0? No — register pins at last_index (2). To
        // exercise a backlog, register BEFORE the events. Do it again:
        let mut l = log();
        let (id2, _) = l.register(Some("s1".into())); // pinned at 0
        l.append(1);
        l.append(2);
        let mut replay = l.attach(&id2).unwrap();
        // backlog has 1,2. Append 3 -> must be live, not lost.
        l.append(3);
        let last_backlog = replay.backlog.last().unwrap().index;
        let first_live = replay.live.try_recv().unwrap().index;
        assert_eq!(last_backlog, 2);
        assert_eq!(first_live, 3);
        let _ = id;
    }

    // --- delivery-driven cursor + cursor-frontier retention ---

    #[test]
    fn connected_subscription_pins_buffer_then_advances() {
        let mut l = log();
        let (id, _) = l.register(Some("s1".into())); // pinned at 0
        for v in 0..5 {
            l.append(v);
        }
        let ep = l.attach(&id).unwrap().epoch;
        let now = Instant::now();
        // Live (no expiry), pinned at 0 -> compaction keeps everything.
        l.compact(now);
        assert_eq!(l.len(), 5, "live sub pinned at 0 retains everything");
        // Deliver events up to index 3 to it.
        l.advance_at(&id, ep, 3, now);
        l.compact(now);
        assert_eq!(l.len(), 5, "young sent events remain replayable");
        l.compact(now + DEFAULT_REDELIVERY_WINDOW);
        let idxs: Vec<_> = l.events.iter().map(|e| e.index).collect();
        assert_eq!(idxs, vec![4, 5]);
    }

    #[test]
    fn long_lived_connected_sub_is_never_expired_midstream() {
        // The bug class this crate kills: a cursor registered at
        // snapshot time and never advanced gets TTL-expired while the
        // client is happily connected. Here, an advancing live sub is
        // NEVER reclaimed regardless of how far time moves — it never
        // disconnected.
        let mut l = log();
        let (id, _) = l.register(Some("s1".into()));
        for v in 0..3 {
            l.append(v);
        }
        let ep = l.attach(&id).unwrap().epoch;
        let start = Instant::now();
        let way_later = start + Duration::from_secs(10_000);
        l.advance(&id, ep, 3);
        l.compact(way_later);
        assert_eq!(l.handle_count(), 1, "live sub survives unbounded time");
        l.append(42);
        assert_eq!(l.last_index(), 4);
    }

    #[test]
    fn disconnected_sub_retained_until_ttl_then_reclaimed() {
        let mut l = EventLog::<u32>::new(Duration::from_secs(60), 256, 64);
        let (id, _) = l.register(Some("s1".into()));
        l.append(1);
        l.append(2);
        let ep = l.attach(&id).unwrap().epoch;
        l.advance(&id, ep, 1); // delivered event 1
        let t0 = Instant::now();
        l.disconnect(&id, ep, t0);

        // Within TTL and redelivery window: even sent event 1 is retained.
        l.compact(t0 + Duration::from_secs(30));
        assert_eq!(l.handle_count(), 1);
        let idxs: Vec<_> = l.events.iter().map(|e| e.index).collect();
        assert_eq!(idxs, vec![1, 2], "retains young sent events too");

        // After TTL: handle reclaimed, buffer free to drop.
        l.compact(t0 + Duration::from_secs(61));
        assert_eq!(l.handle_count(), 0);
    }

    #[test]
    fn reconnect_resumes_gap_free_from_server_cursor() {
        let mut l = log();
        let (id, _) = l.register(Some("s1".into()));
        l.append(1);
        l.append(2);
        let ep = l.attach(&id).unwrap().epoch;
        l.advance(&id, ep, 2); // delivered 1 and 2
        let t0 = Instant::now();
        l.disconnect(&id, ep, t0);
        assert_eq!(l.handle_count(), 1);
        // Reconnect with the SAME sub_id (no index supplied) -> resumes
        // from the server-held cursor (2), gap-free.
        l.append(3);
        let replay = l.attach(&id).unwrap();
        assert_eq!(l.handle_count(), 1, "same sub_id refreshes in place");
        let idxs: Vec<_> = replay.backlog.iter().map(|e| e.index).collect();
        assert_eq!(idxs, vec![3], "resumes from the held cursor");
    }

    #[test]
    fn expired_when_cursor_behind_hard_capped_window() {
        // cap=2: a sub registered at 1 then the log races past, dropping
        // the events it still needed -> Expired on attach.
        let mut l = EventLog::<u32>::new(Duration::from_secs(900), 2, 64);
        l.append(1);
        let (id, _) = l.register(Some("s".into())); // pinned at 1
        l.append(2);
        l.append(3);
        l.append(4); // cap=2 -> earliest now 3, cursor 1 is behind
        match l.attach(&id) {
            Err(AttachError::Expired { earliest, current }) => {
                assert_eq!(earliest, 3);
                assert_eq!(current, 4);
            }
            Err(e) => panic!("expected Expired, got {e:?}"),
            Ok(_) => panic!("expected Expired, got Ok"),
        }
    }

    #[test]
    fn no_handles_lets_buffer_drop() {
        let mut l = log();
        l.append(1);
        l.append(2);
        l.compact(Instant::now());
        assert!(l.is_empty(), "nothing pins the buffer -> fully dropped");
    }

    #[tokio::test]
    async fn subscribe_raw_receives_live_events_without_registering_a_handle() {
        // The escape hatch for internal consumers: live fan-out with
        // no sub_id, no backlog, no buffer pinning.  Critical that it
        // does NOT create a handle (otherwise it would leak a live
        // pin that never TTL-expires).
        let mut l = log();
        let mut rx = l.subscribe_raw();
        assert_eq!(
            l.handle_count(),
            0,
            "subscribe_raw must NOT register a handle"
        );
        l.append(42);
        let got = rx.recv().await.unwrap();
        assert_eq!(got.index, 1);
        assert_eq!(got.event, 42);
        assert_eq!(l.handle_count(), 0, "still no handle after delivery");
    }

    #[test]
    fn slowest_handle_sets_retention_floor() {
        let mut l = log();
        let (fast, _) = l.register(Some("fast".into()));
        let (slow, _) = l.register(Some("slow".into()));
        for _ in 0..5 {
            l.append(0);
        }
        let ef = l.attach(&fast).unwrap().epoch;
        let es = l.attach(&slow).unwrap().epoch;
        l.advance(&fast, ef, 5);
        l.advance(&slow, es, 2);
        l.compact(Instant::now() + DEFAULT_REDELIVERY_WINDOW);
        // Floor is the SLOW sub at 2 -> keep 3,4,5.
        let idxs: Vec<_> = l.events.iter().map(|e| e.index).collect();
        assert_eq!(idxs, vec![3, 4, 5]);
    }
}
