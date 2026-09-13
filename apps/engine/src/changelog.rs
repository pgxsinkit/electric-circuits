//! The **segmented change log** (ADR-0006): the single ordered log of committed changes, rotated
//! into `changes/<n>` streams so a long-running deployment's disk stays bounded.
//!
//! The durable-streams server offers whole-stream TTL but no prefix trimming, so a single
//! ever-growing `changes` stream fills the disk. The engine rotates instead. At a transaction
//! boundary — after the commit's append and its acknowledgement — the ingestor asks
//! [`should_rotate`] whether the current segment is over its byte or age budget; if it is, it
//!
//! 1. creates `changes/<n+1>`,
//! 2. appends ONE **control envelope** to `changes/<n>` naming the successor (see
//!    [`rotation_envelope`]),
//! 3. **closes** `changes/<n>` — every live reader's long-poll is released at once with
//!    `stream-closed`, and no append can ever land on it again,
//! 4. records `ChangesRotated { segment: n+1, at }` in the durable catalog, and
//! 5. continues in `changes/<n+1>`.
//!
//! Readers follow the pointer: the sequencer's live loop and `replay_changes_for_shape` both
//! continue on the pointed-to segment from `"-1"`. Every position in the log is therefore a
//! [`LogPosition`] — `(segment, offset)` — not a bare durable-streams offset: the sequencer's
//! checkpoint, a dormant shape's resume state, and `GET /tables/{name}/offset` all carry the
//! segment.
//!
//! **Control envelopes are recognised by TYPE, never by position** ([`CONTROL_TYPE`] is
//! `__circuits.control`, a spelling no Postgres table the engine tracks can produce — the ingestor
//! stamps a canonical `schema.name`, and `__circuits.*` is refused as a tracked table at the config
//! boundary). Every reader drops them **unconditionally**, before anything else looks at the batch,
//! so they can never reach a table executor, an arrangement or a shape stream.
//!
//! That "never by position" is load-bearing, because the pointer is **not** guaranteed to be the
//! last item in a segment. Rotation appends the pointer and then closes; if the close fails (a
//! storage hiccup) the rotation is abandoned and retried at the next commit — so the segment keeps
//! taking commits *after* a pointer, and the eventual successful rotation appends a **second**
//! pointer. Readers are safe by construction anyway: they cross only on `closed` AND drained, so an
//! abandoned pointer is read, ignored as data, and superseded by the one that actually preceded the
//! close. (Both name the same successor: the number is derived from the current segment, which a
//! failed attempt never moved.)
//!
//! Deletion is the other half (see [`plan_segment_deletion`]): a closed segment is retired once the
//! sequencer's checkpoint is past it and no dormant shape resumes inside it. A dormant shape that
//! would pin a segment beyond the retain window is evicted first, which unpins it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::ds::{Appended, DsClient, Envelope, EnvelopeHeaders};
use crate::heap_size::HeapSize;

/// Path prefix of every change-log segment. The unqualified `changes` stream itself is never
/// created or read — segment 0 is `changes/0` (ADR-0006).
pub const CHANGES_PREFIX: &str = "changes";

/// The durable-streams path of segment `n`.
pub fn segment_path(n: u32) -> String {
    format!("{CHANGES_PREFIX}/{n}")
}

/// The `type` of a change-log **control** envelope. Deliberately unspellable as a tracked table:
/// the ingestor stamps `TableRef::to_string()` (a canonical `schema.name` of a relation with a
/// primary key that the engine introspected), and `__circuits` is not a schema it ever introspects.
/// Readers skip these by comparing this type — never by position in the batch.
pub const CONTROL_TYPE: &str = "__circuits.control";

/// `key` / `headers.operation` of the rotation pointer control envelope.
pub const ROTATED: &str = "rotated";

/// A position in the segmented change log: which segment, and the opaque durable-streams offset
/// within it.
///
/// Ordering is `(segment, offset)` — lexicographic on the offset, which is what durable-streams
/// offsets are built for (fixed-width zero-padded `<seq>_<byte>`), and `"-1"` (the start sentinel)
/// sorts below every real offset.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct LogPosition {
    pub segment: u32,
    pub offset: String,
}

impl LogPosition {
    /// The beginning of segment `n`.
    pub fn start_of(n: u32) -> LogPosition {
        LogPosition { segment: n, offset: "-1".to_string() }
    }

    /// The beginning of the log (segment 0, offset `-1`).
    pub fn start() -> LogPosition {
        LogPosition::start_of(0)
    }

    /// The durable-streams path this position reads from.
    pub fn path(&self) -> String {
        segment_path(self.segment)
    }
}

impl std::fmt::Display for LogPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{CHANGES_PREFIX}/{}@{}", self.segment, self.offset)
    }
}

impl HeapSize for LogPosition {
    /// Only the offset string owns heap; `segment` is inline.
    fn heap_bytes(&self) -> usize {
        self.offset.heap_bytes()
    }
}

/// The rotation pointer envelope appended as the LAST item of segment `n` before it is closed.
///
/// It carries no `lsn`/`txid`/`seq`: it is not a change, it belongs to no transaction, and stamping
/// it would corrupt the sequencer's de-duplication highwater.
pub fn rotation_envelope(next: u32) -> Envelope {
    Envelope {
        type_: CONTROL_TYPE.to_string(),
        key: ROTATED.to_string(),
        value: Some(serde_json::json!({ "next": segment_path(next), "segment": next })),
        old: None,
        headers: EnvelopeHeaders {
            operation: ROTATED.to_string(),
            txid: None,
            offset: None,
            lsn: None,
            seq: None,
            last: None,
            schema: None,
        },
    }
}

/// Is this envelope change-log control metadata (and therefore not data any reader may process)?
pub fn is_control(env: &Envelope) -> bool {
    env.type_ == CONTROL_TYPE
}

/// The segment a rotation-pointer envelope points at, if this envelope is one.
pub fn rotation_target(env: &Envelope) -> Option<u32> {
    if !is_control(env) || env.headers.operation != ROTATED {
        return None;
    }
    let value = env.value.as_ref()?;
    if let Some(n) = value.get("segment").and_then(serde_json::Value::as_u64) {
        return u32::try_from(n).ok();
    }
    // Fallback: derive it from the human-readable path form, so a pointer written by a peer that
    // only set `next` is still followable.
    let next = value.get("next").and_then(serde_json::Value::as_str)?;
    next.strip_prefix(&format!("{CHANGES_PREFIX}/"))?.parse().ok()
}

/// The last rotation pointer in a read batch (`None` if it carries none). At most one can appear —
/// the segment is closed immediately after the pointer is appended — but taking the last is the
/// defensive reading.
pub fn rotation_target_in(envs: &[Envelope]) -> Option<u32> {
    envs.iter().rev().find_map(rotation_target)
}

/// The byte position a durable-streams offset encodes, when it encodes one.
///
/// The Rust durable-streams server formats offsets as `<16-digit read-seq>_<16-digit byte
/// position>`, so a segment's own tail offset IS its size — authoritative even for bytes this
/// process did not append (unlike [`DsClient::appended_bytes`], which only counts this process's
/// appends and so undercounts after a restart mid-segment). A server whose offsets are not
/// byte-positioned simply yields `None` and the engine-side accounting is used instead.
pub fn offset_bytes(offset: &str) -> Option<u64> {
    let (seq, bytes) = offset.split_once('_')?;
    if seq.len() != 16 || bytes.len() != 16 || !bytes.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    bytes.parse().ok()
}

/// Change-log segmentation tuning, read from the environment once at engine construction.
///
/// | Env var | Default | Meaning |
/// |---|---|---|
/// | `ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES` | `1073741824` (1 GiB) | Rotate once the current segment reaches this size. `0` disables the size criterion. |
/// | `ELECTRIC_CIRCUITS_CHANGES_SEGMENT_SECS` | `86400` (1 day) | Rotate once the current segment is this old. `0` disables the age criterion. |
/// | `ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS` | `604800` (7 days) | How long a rotated-out segment may stay pinned by a dormant shape before that shape is evicted (and the segment deleted). `0` disables evict-before-delete — a dormant shape then pins its segment forever. |
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChangeLogConfig {
    pub segment_bytes: u64,
    pub segment_age: Duration,
    pub retain: Duration,
}

impl Default for ChangeLogConfig {
    fn default() -> Self {
        ChangeLogConfig {
            segment_bytes: 1024 * 1024 * 1024,
            segment_age: Duration::from_secs(86_400),
            retain: Duration::from_secs(604_800),
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.trim().parse::<u64>().ok()).unwrap_or(default)
}

impl ChangeLogConfig {
    pub fn from_env() -> Self {
        let d = ChangeLogConfig::default();
        ChangeLogConfig {
            segment_bytes: env_u64("ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES", d.segment_bytes),
            segment_age: Duration::from_secs(env_u64(
                "ELECTRIC_CIRCUITS_CHANGES_SEGMENT_SECS",
                d.segment_age.as_secs(),
            )),
            retain: Duration::from_secs(env_u64("ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS", d.retain.as_secs())),
        }
    }
}

/// Is the current segment over budget? Pure, so the whole policy is a unit test: either criterion
/// alone triggers, and `0` disables that criterion (both `0` = never rotate).
pub fn should_rotate(cfg: &ChangeLogConfig, bytes: u64, age: Duration) -> bool {
    (cfg.segment_bytes > 0 && bytes >= cfg.segment_bytes) || (!cfg.segment_age.is_zero() && age >= cfg.segment_age)
}

/// Unix seconds now (the wire form of a `ChangesRotated.at`). Wall clock on purpose: it must
/// survive a restart, which an `Instant` cannot.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Live state of the segmented change log, shared between the ingestor's writer, the retention
/// sweeper and the observability routes.
#[derive(Debug, Default)]
pub struct ChangesState {
    inner: std::sync::Mutex<ChangesInner>,
}

#[derive(Debug, Default)]
struct ChangesInner {
    /// The segment the ingestor appends to.
    current: u32,
    /// Segment → the unix second it BECAME current (segment 0's is its creation). Segment `n` was
    /// therefore closed when `n+1` began, which is what the retain window measures against.
    starts: BTreeMap<u32, u64>,
    /// `stream-next-offset` of the last append to the current segment (`None` until this process
    /// appends). Reported as `GET /replication/lsn` → `changes.offset`.
    tail: Option<String>,
}

impl ChangesState {
    /// The segment the ingestor is appending to.
    pub fn current(&self) -> u32 {
        self.inner.lock().unwrap().current
    }

    /// The ingestor's position: the current segment plus the tail offset of its last append.
    pub fn position(&self) -> LogPosition {
        let g = self.inner.lock().unwrap();
        LogPosition { segment: g.current, offset: g.tail.clone().unwrap_or_else(|| "-1".to_string()) }
    }

    /// Every segment the engine believes exists, with the unix second it became current.
    pub fn segments(&self) -> BTreeMap<u32, u64> {
        self.inner.lock().unwrap().starts.clone()
    }

    /// How many segments the engine currently retains (the `changes_segments_retained` gauge).
    pub fn retained(&self) -> u64 {
        self.inner.lock().unwrap().starts.len() as u64
    }

    /// Install the boot state: the resolved current segment, the segment start times folded out of
    /// the catalog, and the current segment's tail as storage reports it.
    ///
    /// The tail matters: without it a restart reads its (possibly nearly-full) segment as empty and
    /// would not rotate on size until this process had appended a whole budget of its own.
    pub fn adopt(&self, current: u32, starts: BTreeMap<u32, u64>, tail: Option<String>) {
        let mut g = self.inner.lock().unwrap();
        g.current = current;
        g.starts = starts;
        g.tail = tail;
    }

    /// Record that `segment` became current at `at`.
    fn began(&self, segment: u32, at: u64) {
        let mut g = self.inner.lock().unwrap();
        g.current = segment;
        g.starts.insert(segment, at);
        g.tail = None;
    }

    fn set_tail(&self, offset: Option<String>) {
        if let Some(o) = offset {
            self.inner.lock().unwrap().tail = Some(o);
        }
    }

    /// Forget a segment the engine has deleted.
    pub fn forget(&self, segment: u32) {
        self.inner.lock().unwrap().starts.remove(&segment);
    }

    /// Age of the current segment (now − when it became current). `Duration::ZERO` if unknown.
    fn current_age(&self) -> Duration {
        let g = self.inner.lock().unwrap();
        let started = g.starts.get(&g.current).copied().unwrap_or_else(now_secs);
        Duration::from_secs(now_secs().saturating_sub(started))
    }
}

/// The ingestor's writer for the segmented change log: appends whole commits to the current
/// segment and rotates at a transaction boundary (ADR-0006).
///
/// It is the only thing that appends to the change log, so it is also the only thing that rotates —
/// there is never a second writer to coordinate with. `rotated` is called after each rotation so
/// the engine can record `ChangesRotated` durably.
#[derive(Clone)]
pub struct ChangeLogWriter {
    ds: DsClient,
    state: Arc<ChangesState>,
    cfg: ChangeLogConfig,
    rotated: Arc<dyn Fn(u32, u64) + Send + Sync>,
}

impl ChangeLogWriter {
    pub fn new(
        ds: DsClient,
        state: Arc<ChangesState>,
        cfg: ChangeLogConfig,
        rotated: Arc<dyn Fn(u32, u64) + Send + Sync>,
    ) -> ChangeLogWriter {
        ChangeLogWriter { ds, state, cfg, rotated }
    }

    pub fn state(&self) -> &Arc<ChangesState> {
        &self.state
    }

    pub fn config(&self) -> &ChangeLogConfig {
        &self.cfg
    }

    /// Append one commit — or, for a commit too large for a single request body, one **chunk** of
    /// it (ADR-0003) — to the current segment.
    ///
    /// A chunked commit calls this repeatedly, in order. Since the ingestor is the only writer, the
    /// chunks land contiguously and carry one `(txid, lsn)`, and the last envelope of the last chunk
    /// carries the transaction-end marker — which together are what let the sequencer treat the
    /// several appends as one transaction (it holds a run back until the marker arrives; a reader
    /// that split on `(txid, lsn)` alone would fan out a fraction of a commit, because
    /// durable-streams exposes every append atomically). The caller acknowledges the slot only after
    /// the last call returns `Ok`. Rotation is unaffected: it is a transaction-boundary decision, so
    /// a segment is never rotated *between* two chunks of the same commit.
    ///
    /// Errors propagate (unlike a shape append, which may discard on a retired stream): a dropped
    /// change-log append loses data outright, so the ingestor must tear the connection down
    /// unacknowledged and let Postgres re-deliver. The one exception is a segment that turns out to
    /// be **closed** — a routing signal, not a loss: the writer walks forward to the successor and
    /// re-appends there.
    pub async fn append_commit(&self, envs: &[Envelope]) -> Result<()> {
        if envs.is_empty() {
            return Ok(());
        }
        for _ in 0..8 {
            let segment = self.state.current();
            let path = segment_path(segment);
            match self.ds.append_checked(&path, envs).await? {
                Appended::Ok { next_offset } => {
                    self.state.set_tail(next_offset);
                    return Ok(());
                }
                Appended::Retired(status) => {
                    // Never append to a closed segment. Somebody (a predecessor process that
                    // rotated and died before recording it) already closed this one; resolve the
                    // successor and continue there.
                    tracing::warn!(
                        "change log: {path} is retired ({status}) under the ingestor; resolving the current segment"
                    );
                    self.resolve_forward().await?;
                }
            }
        }
        bail!("change log: the current segment kept moving while appending a commit")
    }

    /// Rotate if the current segment is over its byte or age budget. Called at a transaction
    /// boundary AFTER the commit is appended and acknowledged, so a rotation failure never fails
    /// (or duplicates) a commit — it is logged and retried at the next one.
    pub async fn maybe_rotate(&self) {
        let segment = self.state.current();
        let bytes = self.segment_bytes(segment);
        // An untouched segment is not worth rotating: an age-based rotation on an idle engine would
        // otherwise mint an empty segment per interval forever.
        if bytes == 0 {
            return;
        }
        if !should_rotate(&self.cfg, bytes, self.state.current_age()) {
            return;
        }
        if let Err(e) = self.rotate().await {
            tracing::error!(
                "change log: rotating {} failed: {e:#}; retrying at the next commit",
                segment_path(segment)
            );
        }
    }

    /// Rotate now, whatever the policy says — the epoch reset's use (ADR-0006): the segments of the
    /// epoch that just ended become closed, and therefore deletable, immediately. A segment nothing
    /// was ever appended to is left alone (there is nothing to release).
    pub async fn force_rotate(&self) {
        if self.segment_bytes(self.state.current()) == 0 {
            return;
        }
        if let Err(e) = self.rotate().await {
            tracing::error!("change log: forced rotation failed: {e:#}");
        }
    }

    /// The current segment's size: its own tail offset when durable-streams offsets are
    /// byte-positioned, else this process's append accounting.
    fn segment_bytes(&self, segment: u32) -> u64 {
        let path = segment_path(segment);
        let tail = self.state.inner.lock().unwrap().tail.clone();
        tail.as_deref().and_then(offset_bytes).unwrap_or_else(|| self.ds.appended_bytes(&path))
    }

    /// The rotation itself, in the one order that is crash-safe at every step: the successor exists
    /// before anything points at it, the pointer lands before the close makes the segment terminal,
    /// and the catalog record comes last (a crash before it is repaired by [`resolve_current`] at
    /// the next boot, which finds a closed segment and walks forward).
    ///
    /// A failure part-way is retried at the next commit rather than unwound, so the intermediate
    /// states are all readable: an orphan successor (created, never pointed at — the next attempt
    /// re-`ensure`s the same one, which is idempotent), and a segment carrying a pointer it never
    /// got closed behind (see the module docs: readers ignore pointers as data and cross only on
    /// closed-and-drained, so the extra one is inert; both name the same successor, since the failed
    /// attempt never moved `current`).
    async fn rotate(&self) -> Result<()> {
        let from = self.state.current();
        let to = from + 1;
        let from_path = segment_path(from);
        let to_path = segment_path(to);
        self.ds.ensure_stream(&to_path).await.with_context(|| format!("creating {to_path}"))?;
        self.ds
            .append(&from_path, &[rotation_envelope(to)])
            .await
            .with_context(|| format!("appending the rotation pointer to {from_path}"))?;
        self.ds.close_stream(&from_path).await.with_context(|| format!("closing {from_path}"))?;
        let at = now_secs();
        self.state.began(to, at);
        (self.rotated)(to, at);
        crate::metrics::metrics().changes_rotations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::info!("change log: rotated {from_path} -> {to_path}");
        Ok(())
    }

    /// Walk forward over closed segments (see [`resolve_current`]) and adopt the result, recording
    /// any `ChangesRotated` the crashed predecessor missed.
    async fn resolve_forward(&self) -> Result<()> {
        let from = self.state.current();
        let resolved = resolve_current(&self.ds, from, true).await?;
        for n in (from + 1)..=resolved {
            let at = now_secs();
            self.state.began(n, at);
            (self.rotated)(n, at);
        }
        Ok(())
    }
}

/// The successor a **reader** moves to when the segment it is on turns out to be closed and it
/// never saw the rotation pointer (its checkpoint was already past it, or storage lost it).
///
/// Always EXACTLY `closed + 1`, verified to exist — never a walk. A reader must visit every segment
/// in order: jumping to the first OPEN segment would silently skip every closed segment in between,
/// which is a span of unread changes. (The writer is the opposite case: it may not append to
/// anything but the open segment, so it walks — see [`resolve_current`].)
///
/// The successor is derived and verified with a `HEAD` rather than read out of the closed segment's
/// final control envelope: a segment is up to a gigabyte and durable-streams has no bounded tail
/// read (a byte offset into a JSON stream is not item-aligned), so reading it back would mean
/// streaming the whole thing. The pointer's *content* is `closed + 1` by construction, and a reader
/// that is streaming the segment normally follows the real envelope.
pub async fn next_segment_for_reader(ds: &DsClient, closed: u32) -> Result<u32> {
    let next = closed + 1;
    if ds.head(&segment_path(next)).await?.is_none() {
        bail!(
            "change log: {} is closed but its successor {} does not exist — rotation always creates \
             the successor before it closes the predecessor, so this storage state cannot have been \
             produced by the engine. Refusing to skip ahead.",
            segment_path(closed),
            segment_path(next)
        );
    }
    Ok(next)
}

/// Resolve the segment the log is actually being **written** at, starting from the catalog fold's
/// `current_segment`. Writer-only: a reader steps one segment at a time
/// ([`next_segment_for_reader`]) because it must see what is in between.
///
/// A process can die between closing segment `n` and recording `ChangesRotated { segment: n+1 }`,
/// so the fold's answer is a lower bound. While the candidate segment is **closed** the walk moves
/// to its successor; the successor is verified to exist first, because a closed segment with no
/// successor is a state rotation cannot produce (it creates `n+1` before it appends the pointer to
/// `n`, and appends the pointer before it closes `n`) and must be refused rather than papered over.
///
/// `recorded` says whether the catalog has ever recorded `from` as a segment that began. Absence
/// from storage means two opposite things, and only `recorded` tells them apart: nothing was ever
/// written (a genuine first boot — the caller creates the segment), or a segment the engine knows
/// existed has been deleted, which can only mean later rotations whose records were lost plus a
/// sweep. Recreating it empty there would leave the sequencer parked on a stream the ingestor is not
/// writing to, forever — so that one is refused.
pub async fn resolve_current(ds: &DsClient, from: u32, recorded: bool) -> Result<u32> {
    let mut cur = from;
    for _ in 0..1_000_000 {
        let head = ds.head(&segment_path(cur)).await?;
        let Some(head) = head else {
            if cur == from && !recorded {
                return Ok(cur); // genuine first boot: nothing has ever been written
            }
            bail!(
                "change log: {} is recorded as a segment that existed, but storage does not have it. \
                 That can only mean rotations whose catalog records were lost AND a sweep that then \
                 deleted it; recreating it empty would park the sequencer on a stream nothing writes \
                 to. Refusing to boot — reset the durable-streams data directory.",
                segment_path(cur)
            );
        };
        if !head.closed {
            return Ok(cur);
        }
        let next = next_segment_for_reader(ds, cur).await?;
        tracing::warn!(
            "change log: {} is closed; the rotation to {} was never recorded (crash between the \
             close and the catalog append) — walking forward",
            segment_path(cur),
            segment_path(next)
        );
        cur = next;
    }
    bail!("change log: walking forward from segment {from} did not terminate")
}

/// One shape's pin on the change log, for [`plan_segment_deletion`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentPin {
    pub shape_id: String,
    /// The segment its resume position lives in.
    pub segment: u32,
    /// Can the sweeper release this pin by evicting the shape? True for a **dormant** shape (its
    /// resume position is a promise nobody is currently using). False for a **reactivating** one:
    /// its replay is reading that segment right now, so the pin holds however old it is — an
    /// unreleasable pin simply keeps the floor where it is.
    pub evictable: bool,
}

/// What one sweep should do about the change log's segments.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SegmentPlan {
    /// Dormant shapes pinning a segment past the retain window: evicted first, which unpins it.
    pub evict: Vec<String>,
    /// Closed segments nothing can resume inside any more, ascending.
    pub delete: Vec<u32>,
}

/// Decide which segments a sweep may delete, and which dormant shapes must be evicted to get there.
///
/// Pure — every piece of engine state is passed in — so the interlock is a unit test:
///
/// - a dormant shape whose resume segment was rotated out more than `retain` ago is **evicted**
///   (the normal eviction path: its stream is closed, then deleted), because a shape nobody has
///   touched for a week must not pin a week of change log;
/// - `min_needed` is the lowest segment anything still needs: the sequencer's checkpoint segment,
///   and the resume segment of every dormant shape that survived the step above;
/// - every known segment strictly below `min_needed` **and** below the current one is deleted. The
///   current segment is never deleted (it is being appended to), and neither is a segment the
///   sequencer has not passed.
pub fn plan_segment_deletion(
    segments: &BTreeMap<u32, u64>,
    current: u32,
    checkpoint: u32,
    pins: &[SegmentPin],
    retain: Duration,
    now: u64,
) -> SegmentPlan {
    let mut plan = SegmentPlan::default();
    // When was segment `n` rotated out? When its successor became current. A pin on the current
    // segment (or on one whose successor is unknown) is never stale — nothing has rotated it away.
    let rotated_out_at = |n: u32| -> Option<u64> {
        if n >= current {
            return None;
        }
        segments.get(&(n + 1)).copied()
    };
    let mut alive: Vec<&SegmentPin> = Vec::new();
    for pin in pins {
        let stale = pin.evictable
            && !retain.is_zero()
            && rotated_out_at(pin.segment).is_some_and(|at| Duration::from_secs(now.saturating_sub(at)) >= retain);
        if stale {
            plan.evict.push(pin.shape_id.clone());
        } else {
            alive.push(pin);
        }
    }
    let min_needed = alive.iter().map(|p| p.segment).chain(std::iter::once(checkpoint)).min().unwrap_or(checkpoint);
    plan.delete = segments.keys().copied().filter(|&n| n < min_needed && n < current).collect();
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ChangeLogConfig {
        ChangeLogConfig::default()
    }

    #[test]
    fn positions_order_by_segment_then_offset() {
        let a = LogPosition { segment: 0, offset: "0000000000000000_0000000000000099".into() };
        let b = LogPosition { segment: 1, offset: "0000000000000000_0000000000000001".into() };
        assert!(a < b, "a later segment always wins, whatever the offsets say");
        let c = LogPosition { segment: 1, offset: "0000000000000000_0000000000000002".into() };
        assert!(b < c);
        // The start sentinel sorts below every real offset in its segment.
        assert!(LogPosition::start_of(1) < b);
        assert!(LogPosition::start() < a);
    }

    #[test]
    fn positions_round_trip_and_display_as_a_path() {
        let p = LogPosition { segment: 3, offset: "0000000000000000_0000000000000042".into() };
        assert_eq!(p.to_string(), "changes/3@0000000000000000_0000000000000042");
        assert_eq!(p.path(), "changes/3");
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["segment"], 3);
        assert_eq!(json["offset"], "0000000000000000_0000000000000042");
        assert_eq!(serde_json::from_value::<LogPosition>(json).unwrap(), p);
    }

    /// The bare-string form is what a pre-ADR-0006 catalog holds. Deserialisation must REFUSE it
    /// rather than coerce it to segment 0: coercing would resume a dormant shape (or the sequencer)
    /// at an offset from a different segment's byte space — silently replaying, or silently
    /// skipping, an arbitrary span of changes.
    #[test]
    fn a_bare_offset_string_is_not_a_position() {
        assert!(serde_json::from_value::<LogPosition>(serde_json::json!("0000000000000000_0000000000000042")).is_err());
        assert!(serde_json::from_value::<LogPosition>(serde_json::json!("-1")).is_err());
        // A number is not one either.
        assert!(serde_json::from_value::<LogPosition>(serde_json::json!(42)).is_err());
    }

    #[test]
    fn rotation_is_decided_by_bytes_or_age_and_either_can_be_disabled() {
        let c = ChangeLogConfig { segment_bytes: 100, segment_age: Duration::from_secs(60), retain: cfg().retain };
        assert!(!should_rotate(&c, 99, Duration::from_secs(59)), "under both budgets");
        assert!(should_rotate(&c, 100, Duration::ZERO), "at the byte budget");
        assert!(should_rotate(&c, 0, Duration::from_secs(60)), "at the age budget");

        let bytes_only = ChangeLogConfig { segment_age: Duration::ZERO, ..c };
        assert!(!should_rotate(&bytes_only, 99, Duration::from_secs(1 << 20)), "age disabled");
        assert!(should_rotate(&bytes_only, 100, Duration::ZERO));

        let age_only = ChangeLogConfig { segment_bytes: 0, ..c };
        assert!(!should_rotate(&age_only, u64::MAX, Duration::from_secs(59)), "size disabled");
        assert!(should_rotate(&age_only, 0, Duration::from_secs(60)));

        let off = ChangeLogConfig { segment_bytes: 0, segment_age: Duration::ZERO, ..c };
        assert!(!should_rotate(&off, u64::MAX, Duration::from_secs(1 << 20)), "both disabled = never");
    }

    #[test]
    fn defaults_are_a_gib_a_day_and_a_week() {
        let d = ChangeLogConfig::default();
        assert_eq!(d.segment_bytes, 1024 * 1024 * 1024);
        assert_eq!(d.segment_age, Duration::from_secs(86_400));
        assert_eq!(d.retain, Duration::from_secs(604_800));
    }

    #[test]
    fn the_control_envelope_is_recognised_by_type_and_carries_its_successor() {
        let env = rotation_envelope(7);
        assert!(is_control(&env));
        assert_eq!(rotation_target(&env), Some(7));
        assert_eq!(env.value.as_ref().unwrap()["next"], "changes/7");
        // It belongs to no transaction: stamping it would poison the sequencer's (lsn, seq)
        // de-duplication highwater.
        assert!(env.headers.lsn.is_none() && env.headers.txid.is_none() && env.headers.seq.is_none());

        // An ordinary change is never mistaken for one, however it is spelled.
        let data = Envelope {
            type_: "public.items".into(),
            key: "rotated".into(),
            value: Some(serde_json::json!({ "segment": 9 })),
            old: None,
            headers: EnvelopeHeaders {
                operation: "rotated".into(),
                txid: None,
                offset: None,
                lsn: None,
                seq: None,
                last: None,
                schema: None,
            },
        };
        assert!(!is_control(&data));
        assert_eq!(rotation_target(&data), None);
    }

    #[test]
    fn the_pointer_is_found_anywhere_in_a_batch() {
        let mut envs = vec![Envelope {
            type_: "public.items".into(),
            key: "1".into(),
            value: None,
            old: None,
            headers: EnvelopeHeaders {
                operation: "insert".into(),
                txid: None,
                offset: None,
                lsn: None,
                seq: None,
                last: None,
                schema: None,
            },
        }];
        assert_eq!(rotation_target_in(&envs), None);
        envs.push(rotation_envelope(2));
        assert_eq!(rotation_target_in(&envs), Some(2));
    }

    #[test]
    fn a_byte_positioned_offset_yields_the_segments_size() {
        assert_eq!(offset_bytes("0000000000000000_0000000000001234"), Some(1234));
        assert_eq!(offset_bytes("-1"), None);
        assert_eq!(offset_bytes("tip"), None);
        assert_eq!(offset_bytes("0_1"), None, "the fixed width is part of the format");
    }

    fn segments(pairs: &[(u32, u64)]) -> BTreeMap<u32, u64> {
        pairs.iter().copied().collect()
    }

    #[test]
    fn segments_below_the_checkpoint_are_deleted_and_the_current_one_never_is() {
        let segs = segments(&[(0, 100), (1, 200), (2, 300)]);
        let plan = plan_segment_deletion(&segs, 2, 2, &[], Duration::from_secs(60), 1000);
        assert_eq!(plan.delete, vec![0, 1]);
        assert!(plan.evict.is_empty());

        // The sequencer has not left segment 1: segment 1 stays, whatever its age.
        let plan = plan_segment_deletion(&segs, 2, 1, &[], Duration::from_secs(60), 1_000_000);
        assert_eq!(plan.delete, vec![0]);
    }

    #[test]
    fn a_dormant_shape_pins_its_segment_until_the_retain_window_elapses() {
        let segs = segments(&[(0, 100), (1, 200), (2, 300)]);
        let pins = vec![SegmentPin { shape_id: "s1".into(), segment: 0, evictable: true }];
        // Segment 0 was rotated out when segment 1 began (t=200); at t=250 that is 50s ago.
        let plan = plan_segment_deletion(&segs, 2, 2, &pins, Duration::from_secs(60), 250);
        assert!(plan.evict.is_empty(), "still inside the retain window");
        assert!(plan.delete.is_empty(), "and the pin holds everything from segment 0 up");

        // Past the window: the shape is evicted, which unpins segment 0 and both go.
        let plan = plan_segment_deletion(&segs, 2, 2, &pins, Duration::from_secs(60), 300);
        assert_eq!(plan.evict, vec!["s1".to_string()]);
        assert_eq!(plan.delete, vec![0, 1]);
    }

    #[test]
    fn the_lowest_surviving_pin_sets_the_floor() {
        let segs = segments(&[(0, 100), (1, 200), (2, 300), (3, 400)]);
        let pins = vec![
            SegmentPin { shape_id: "old".into(), segment: 0, evictable: true }, // stale: evicted
            SegmentPin { shape_id: "mid".into(), segment: 2, evictable: true }, // fresh: pins from 2 up
        ];
        // now=410: segment 0 rotated out at 200 (210s ago, stale), segment 2 at 400 (10s ago).
        let plan = plan_segment_deletion(&segs, 3, 3, &pins, Duration::from_secs(100), 410);
        assert_eq!(plan.evict, vec!["old".to_string()]);
        assert_eq!(plan.delete, vec![0, 1], "segment 2 is still pinned by 'mid'");
    }

    #[test]
    fn a_pin_on_the_current_segment_is_never_stale() {
        let segs = segments(&[(0, 100)]);
        let pins = vec![SegmentPin { shape_id: "s1".into(), segment: 0, evictable: true }];
        let plan = plan_segment_deletion(&segs, 0, 0, &pins, Duration::from_secs(1), u64::MAX);
        assert!(plan.evict.is_empty(), "nothing has rotated segment 0 away yet");
        assert!(plan.delete.is_empty());
    }

    #[test]
    fn a_zero_retain_window_disables_evict_before_delete() {
        let segs = segments(&[(0, 100), (1, 200)]);
        let pins = vec![SegmentPin { shape_id: "s1".into(), segment: 0, evictable: true }];
        let plan = plan_segment_deletion(&segs, 1, 1, &pins, Duration::ZERO, u64::MAX);
        assert!(plan.evict.is_empty());
        assert!(plan.delete.is_empty(), "the pin holds forever");
    }

    /// A REACTIVATING shape is replaying from its pinned segment right now: the pin is not the
    /// sweeper's to release, however old the segment is, or the replay would read a deleted stream.
    #[test]
    fn a_pin_that_is_being_replayed_is_never_evicted_or_deleted_around() {
        let segs = segments(&[(0, 100), (1, 200), (2, 300)]);
        let pins = vec![SegmentPin { shape_id: "replaying".into(), segment: 0, evictable: false }];
        let plan = plan_segment_deletion(&segs, 2, 2, &pins, Duration::from_secs(1), u64::MAX);
        assert!(plan.evict.is_empty(), "the sweeper cannot evict a shape mid-reactivation");
        assert!(plan.delete.is_empty(), "and the segment it is reading stays");
    }

    #[test]
    fn already_deleted_segments_are_simply_absent_from_the_plan() {
        // Segments 0-1 were deleted by an earlier sweep and forgotten; nothing re-proposes them.
        let segs = segments(&[(2, 300), (3, 400)]);
        let plan = plan_segment_deletion(&segs, 3, 3, &[], Duration::from_secs(60), 1000);
        assert_eq!(plan.delete, vec![2]);
    }
}
