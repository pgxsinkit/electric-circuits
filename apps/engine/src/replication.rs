//! Logical-replication ingestor: streams a Postgres `pgoutput` slot (walsender protocol, push
//! delivery — no poll floor) and turns each row change into a State-Protocol envelope (carrying
//! old + new and the change's COMMIT LSN), appended — whole commits, in commit order — to the
//! single durable-streams change log (the envelope's `type` carries the table). The engine's
//! sequencer consumes that log, so global transaction order survives end to end. The log is
//! **segmented** (ADR-0006): the ingestor appends to the current `changes/<n>` through a
//! [`ChangeLogWriter`], which also decides — at a transaction boundary, after the commit is
//! appended and acknowledged — whether the segment is over its size/age budget and must be
//! rotated. The ingestor itself knows nothing about segments, the catalog or shapes.
//!
//! Delivery is append-then-acknowledge: a transaction's changes are buffered between `Begin` and
//! `Commit`, appended to durable-streams, and only then acknowledged to Postgres
//! (`update_applied_lsn` → the slot's `confirmed_flush_lsn`). A failed append tears the
//! replication connection down instead of acknowledging; on reconnect the server resends from the
//! confirmed position, so nothing is lost. (Acknowledgements are flushed on an interval, so a
//! crash can re-deliver whole transactions. Delivery is therefore at-least-once; the sequencer
//! restores exactly-once effect by de-duplicating on the stamped `(lsn, seq)`.)
//!
//! **Large transactions do not have to fit in memory** (ADR-0003). The buffer between `Begin` and
//! `Commit` is a [`TxnBuffer`]: past `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` it spills to a local
//! temporary file, and at `Commit` the transaction is streamed back out in **chunks**, each
//! appended to the current segment and each within `ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES`. The
//! slot is acknowledged — `last_lsn` published, the drain barrier's sentinel released — only after
//! the **last** chunk has landed. A failure on any chunk tears the connection down unacknowledged,
//! so Postgres re-delivers the whole transaction and the sequencer's `(lsn, seq)` de-duplication
//! discards the chunks that already landed.
//!
//! Chunking would otherwise be visible downstream as several transactions: durable-streams exposes
//! each append atomically, so a reader long-polling the segment tail gets chunk 1 on its own. The
//! ingestor therefore stamps a **transaction-end marker** (`headers.last`) on the final envelope of
//! the final chunk — on single-chunk commits too, so "no marker" unambiguously means "there is more
//! of this transaction coming" — and the sequencer holds an unterminated trailing run back until it
//! arrives (see `engine/sequencer.rs`). Contiguity on the segment is what makes that work: the
//! ingestor is the only writer, and rotation is a transaction-boundary decision, so a commit's
//! chunks are never interleaved with anything else.
//!
//! Each envelope is stamped with its transaction's COMMIT LSN (not the per-change record LSN), so
//! the backfill/replication boundary (see `pg::SnapshotGate`) lines up with snapshot *commit*
//! visibility, plus the transaction's xid and the change's position within the transaction.
//!
//! Values are pgoutput **text-mode** tuples (the `binary` option is never enabled): Postgres
//! renders them with the same type output functions the backfill's `::text` casts use, keeping
//! backfilled and replicated representations byte-identical (see `pg.rs::row_json_expr`).
//!
//! Every connection is gated on the engine's **epoch** check ([`EpochEvents::before_connect`],
//! ADR-0004): the slot must still be the one the engine bound to, or `START_REPLICATION` would
//! resume a slot the engine has no history with — silently, at the current WAL head. Refusals and
//! connection failures alike back off exponentially with jitter (1 s → 30 s); a connection that was
//! actually established resets the schedule, and an operator's epoch reset cuts the wait short.
//!
//! The ingestor is also where **schema drift** is noticed (ADR-0005). Postgres re-sends a
//! `Relation` message after any DDL that changes a table, so every `R` is compared with the
//! compiled [`SchemaFingerprint`]; a difference — a column added/dropped/retyped/reordered, or a
//! replica identity that is no longer FULL — and `TRUNCATE` are reported to the engine through
//! [`SchemaEvents`] and **awaited inline** before the next message is decoded. DDL is rare, so the
//! brief ingest pause buys two things nothing else can: the table's dependents are already retired
//! before any post-DDL DML for it is decoded, and that DML decodes against the NEW schema.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context, Result};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};
use serde_json::{Map, Value as Json};

use crate::changelog::ChangeLogWriter;
use crate::ds::{Envelope, EnvelopeHeaders};
use crate::pgoutput::{self, Cell, Message, OldTuple, RelColumn, Tuple};
use crate::schema::{ColumnType, SchemaFingerprint, SharedTables, TableSchema};
use crate::table_ref::TableRef;
use crate::txn_buffer::{Stamp, TxnBuffer, TxnBufferConfig};

/// A boxed future — the trait-object-safe form of the two `async fn`s on [`SchemaEvents`] (the
/// engine implements it, so a native `async fn` in trait would make it non-dyn-safe).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The replication transaction a schema event was observed inside.
///
/// It matters for one decision: whether the engine may restart the process over the event.
/// Delivery is at-least-once (a commit is acknowledged only after its append lands, and
/// acknowledgements are flushed on an interval), so a restart re-delivers the very transaction that
/// caused it. Carrying the xid lets the engine ask "does the state I would rebuild already reflect
/// this transaction?" and decline to restart again — otherwise a `TRUNCATE` on a circuit-served
/// table is an exit loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TxnRef {
    pub xid: u32,
}

/// What the ingestor reports to the engine when Postgres tells it a table moved underneath the
/// compiled schema (ADR-0005). One narrow trait rather than an `Engine` dependency, so the
/// ingestor keeps knowing nothing about shapes, streams or the catalog.
///
/// Both calls are **awaited** by the ingestor before it decodes the next message, and neither
/// returns anything: the ingestor's view of what it may decode is [`SharedTables`], which the
/// engine has already updated by the time the call returns.
pub trait SchemaEvents: Send + Sync {
    /// The observed fingerprint of `table` differs from the compiled one (or its replica identity
    /// is no longer FULL). The engine re-introspects, retires every dependent, and swaps the
    /// compiled schema.
    fn on_schema_drift<'a>(
        &'a self,
        table: &'a TableRef,
        observed: SchemaFingerprint,
        txn: Option<TxnRef>,
    ) -> BoxFuture<'a, ()>;

    /// `TRUNCATE` landed on these tables. There is no row copy in the engine from which to
    /// synthesise the deletes, so every dependent is retired; the schema does not change.
    fn on_truncate<'a>(&'a self, tables: Vec<TableRef>, txn: Option<TxnRef>) -> BoxFuture<'a, ()>;
}

/// Why the engine will not let the ingestor open a replication connection right now.
///
/// Every variant is a *wait*, never a give-up: the loop backs off and asks again. They differ only
/// in what could make the answer change — an operator (`EpochBroken`), another engine going away
/// (`SlotBusy`), or Postgres coming back (`CheckFailed`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    /// The epoch is broken and `ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS=false`: ingest stays stopped
    /// until an operator posts `/epoch/reset` (ADR-0004).
    EpochBroken(&'static str),
    /// Another walsender holds the slot — Postgres allows exactly one, so this is a second engine
    /// on the same slot (or our own predecessor that Postgres has not yet reaped), not an epoch
    /// break.
    SlotBusy(Option<i32>),
    /// The verification itself could not run (Postgres unreachable). No verdict, so no connect.
    CheckFailed,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refused::EpochBroken(reason) => {
                write!(f, "epoch broken ({reason}); waiting for POST /epoch/reset")
            }
            Refused::SlotBusy(Some(pid)) => write!(f, "the slot is held by walsender pid {pid}"),
            Refused::SlotBusy(None) => f.write_str("the slot is held by another walsender"),
            Refused::CheckFailed => f.write_str("the slot could not be verified"),
        }
    }
}

/// The engine's gate on the replication connection itself (ADR-0004). Split from [`SchemaEvents`]
/// because it is about the **slot**, not about any table: the ingestor must never call
/// `START_REPLICATION` on a slot the engine is not still bound to, or it would silently resume a
/// fresh slot at the current WAL head and every shape would miss the gap.
pub trait EpochEvents: Send + Sync {
    /// Verify the slot before connecting, and apply the configured slot-loss policy. `Ok` = connect
    /// now; `Err` = keep waiting (the loop backs off and asks again).
    fn before_connect(&self) -> BoxFuture<'_, std::result::Result<(), Refused>>;

    /// Resolves when the engine (re)binds an epoch — an operator reset, so the answer to
    /// [`Self::before_connect`] has just changed and the backoff should be cut short.
    fn epoch_rebound(&self) -> BoxFuture<'_, ()>;
}

/// Reconnect backoff bounds (ADR-0004). One second is long enough that a flapping server is not
/// hammered and short enough that an ordinary blip is invisible; 30 s is the ceiling for an outage
/// that needs an operator anyway.
pub const RECONNECT_MIN: std::time::Duration = std::time::Duration::from_secs(1);
pub const RECONNECT_MAX: std::time::Duration = std::time::Duration::from_secs(30);

/// The worker spawned by `pgwire-replication` performs TCP, authentication and
/// `START_REPLICATION` asynchronously.  Its `connect` future only starts that worker, so the
/// first receive is part of setup and needs the same bounded wait as an ordinary Postgres
/// connection.  Once one frame/keepalive has arrived, the stream is intentionally long-lived and
/// uses the worker's idle wakeup behavior instead.
pub const REPLICATION_SETUP_TIMEOUT: std::time::Duration = crate::pg::CONNECT_TIMEOUT;

/// Outcome of an await which is interruptible by shutdown and bounded by a diagnostic deadline.
/// Keeping this seam generic makes the timeout and cancellation contract testable without a real
/// blackholed socket, while callers retain the original error type for classification.
#[derive(Debug, PartialEq, Eq)]
enum AwaitOutcome<T, E> {
    Ready(T),
    Failed(E),
    Shutdown,
    TimedOut,
}

async fn await_with_shutdown<F, T, E>(
    future: F,
    timeout: std::time::Duration,
    shutdown: &crate::shutdown::ShutdownToken,
) -> AwaitOutcome<T, E>
where
    F: Future<Output = std::result::Result<T, E>> + Send,
{
    tokio::select! {
        biased;
        _ = shutdown.wait() => AwaitOutcome::Shutdown,
        result = tokio::time::timeout(timeout, future) => match result {
            Ok(Ok(value)) => AwaitOutcome::Ready(value),
            Ok(Err(error)) => AwaitOutcome::Failed(error),
            Err(_) => AwaitOutcome::TimedOut,
        },
    }
}

/// Un-jittered backoff for the n-th consecutive failed attempt: 1s, 2s, 4s … capped at
/// [`RECONNECT_MAX`]. Pure, so the schedule is a unit test rather than a stopwatch. Shared with the
/// binary's boot connect loop, which retries a retryable Postgres failure on the same schedule.
pub fn backoff_base(attempt: u32) -> std::time::Duration {
    RECONNECT_MIN.saturating_mul(1u32 << attempt.min(16)).min(RECONNECT_MAX)
}

/// Spread `base` by ±25% from `nanos` (the clock's sub-second noise — no RNG dependency, and
/// precision is irrelevant here). Pure in `nanos` so the bounds are testable.
pub fn jitter(base: std::time::Duration, nanos: u32) -> std::time::Duration {
    let spread = base.as_millis() as u64 / 2; // the full ±25% window
    if spread == 0 {
        return base;
    }
    base.saturating_sub(std::time::Duration::from_millis(spread / 2))
        + std::time::Duration::from_millis(u64::from(nanos) % spread)
}

/// One step of the reconnect schedule: the (un-jittered) delay before the next attempt, and the
/// attempt counter to carry forward.
///
/// `connected` is "this attempt got a message out of the server", not "a socket was opened" — see
/// [`stream_loop`]. Only that resets the schedule, so a run of auth/`pg_hba`/slot-busy failures
/// actually climbs 1 s → 30 s instead of retrying at the floor forever. Pure, so the whole schedule
/// is a unit test.
fn next_backoff(attempt: u32, connected: bool) -> (std::time::Duration, u32) {
    let attempt = if connected { 0 } else { attempt };
    (backoff_base(attempt), attempt.saturating_add(1))
}

/// The sub-second clock noise the jitter is drawn from.
pub fn clock_nanos() -> u32 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(0)
}

/// Wait out the backoff, cut short by an epoch rebind (so an operator's `/epoch/reset` is followed
/// by a connect attempt at once rather than up to 30 s later) — or by a shutdown, so a pod that is
/// terminating during an outage exits in milliseconds instead of sleeping out a 30 s ceiling inside
/// the termination grace.
async fn backoff_wait(epoch: &dyn EpochEvents, shutdown: &crate::shutdown::ShutdownToken, d: std::time::Duration) {
    tokio::select! {
        _ = tokio::time::sleep(d) => {}
        _ = epoch.epoch_rebound() => {}
        _ = shutdown.wait() => {}
    }
}

/// The drain-barrier bookkeeping table: `public.__el_sync` specifically — a same-named table in
/// another schema is ordinary data, never the sentinel.
fn sync_table() -> &'static TableRef {
    static T: std::sync::OnceLock<TableRef> = std::sync::OnceLock::new();
    T.get_or_init(|| TableRef::public("__el_sync").expect("valid sentinel table ref"))
}

/// Long-running ingestor. Reconnects on any connection-level failure; the server resends
/// everything after the last acknowledged commit.
///
/// **Every** connection is gated on [`EpochEvents::before_connect`] (ADR-0004): a slot that vanished
/// or was recreated under the engine is caught before `START_REPLICATION`, not discovered as a
/// permanent failure. Failures — refusals and connection errors alike — back off exponentially with
/// jitter (1 s → 30 s); a connection that was actually established resets the schedule.
///
/// `tables` is the engine's **live** schema view, not a boot-time copy: schema drift swaps an entry
/// in place and the very next decode uses it (ADR-0005).
#[allow(clippy::too_many_arguments)]
pub async fn run(
    pg_url: String,
    slot: String,
    publication: String,
    log: ChangeLogWriter,
    tables: SharedTables,
    events: Arc<dyn SchemaEvents>,
    epoch: Arc<dyn EpochEvents>,
    last_lsn: Arc<std::sync::Mutex<String>>,
    sync_seq: Arc<AtomicI64>,
    txn_cfg: TxnBufferConfig,
    shutdown: crate::shutdown::ShutdownToken,
    // Held for this task's lifetime: dropping it is what tells the shutdown "the ingestor is done".
    _party: crate::shutdown::ShutdownParty,
) {
    // A spill file lives only between a `Begin` and its `Commit`, so anything of ours still in the
    // spill dir belongs to a process that died mid-transaction (ADR-0003). Sweep it once, here,
    // before the first connection.
    let swept = crate::txn_buffer::sweep_spill_dir(&txn_cfg.spill_dir);
    if swept > 0 {
        tracing::warn!(
            "replicator: removed {swept} transaction spill file(s) left in {} by a previous process",
            txn_cfg.spill_dir.display()
        );
    }
    let mut attempt: u32 = 0;
    loop {
        // Shutdown is checked at the TOP, so a terminating process never opens a new replication
        // connection it is about to abandon.
        if shutdown.is_shutting_down() {
            tracing::info!("replicator: shutdown requested; not reconnecting");
            return;
        }
        // The epoch gate. A refusal is never fatal — it is "not yet", and what could change the
        // answer differs per reason (see `Refused`).
        if let Err(refused) = epoch.before_connect().await {
            tracing::warn!("replicator: not connecting — {refused}");
            let (base, next) = next_backoff(attempt, false);
            backoff_wait(epoch.as_ref(), &shutdown, jitter(base, clock_nanos())).await;
            attempt = next;
            continue;
        }
        let cfg = match replication_config(&pg_url, &slot, &publication) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("replicator: bad connection config: {e:#}; retrying");
                let (base, next) = next_backoff(attempt, false);
                backoff_wait(epoch.as_ref(), &shutdown, jitter(base, clock_nanos())).await;
                attempt = next;
                continue;
            }
        };
        let mut connected = false;
        match stream_loop(
            cfg,
            &log,
            &tables,
            events.as_ref(),
            &last_lsn,
            &sync_seq,
            &txn_cfg,
            &shutdown,
            &mut connected,
        )
        .await
        {
            Ok(StreamEnd::ShuttingDown) => {
                tracing::info!("replicator: stopped for shutdown");
                return;
            }
            Ok(StreamEnd::Ended) => tracing::warn!("replicator: stream ended; reconnecting"),
            Err(e) => tracing::error!("replicator: {e:#}; reconnecting"),
        }
        // A connection that actually delivered is evidence the far side is healthy: start the next
        // outage's schedule from the bottom rather than from wherever the last one ended.
        let (base, next) = next_backoff(attempt, connected);
        backoff_wait(epoch.as_ref(), &shutdown, jitter(base, clock_nanos())).await;
        attempt = next;
    }
}

/// Why one replication connection's loop returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamEnd {
    /// The server stopped sending (the caller reconnects).
    Ended,
    /// A graceful shutdown was requested at a safe point (the caller returns).
    ShuttingDown,
}

/// Build the walsender connection config from a `postgres://` URL. TLS is disabled — parity with
/// the engine's other connections (`pg::connect` uses `NoTls`).
fn replication_config(pg_url: &str, slot: &str, publication: &str) -> Result<ReplicationConfig> {
    let u = url::Url::parse(pg_url).context("parse postgres url")?;
    let user = match u.username() {
        "" => "postgres".to_string(),
        s => percent_decode(s),
    };
    let database = match u.path().trim_start_matches('/') {
        "" => user.clone(),
        s => percent_decode(s),
    };
    Ok(ReplicationConfig {
        host: u.host_str().unwrap_or("127.0.0.1").to_string(),
        port: u.port().unwrap_or(5432),
        user,
        password: u.password().map(percent_decode).unwrap_or_default(),
        database,
        tls: TlsConfig::default(),
        slot: slot.to_string(),
        publication: publication.to_string(),
        // 0/0: the server streams from the slot's confirmed_flush_lsn when asked for an older
        // position, which is exactly "resume where we left off".
        start_lsn: Lsn::ZERO,
        stop_at_lsn: None,
        // How often acknowledged progress is flushed to the server. Bounds the duplicate window
        // after a reconnect (the tailer de-duplicates anyway).
        status_interval: std::time::Duration::from_secs(1),
        idle_wakeup_interval: std::time::Duration::from_secs(10),
        buffer_events: 8192,
    })
}

fn percent_decode(s: &str) -> String {
    // Minimal %XX decoding for URL userinfo/path segments (connection strings rarely need more).
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() + 1 && i + 2 < b.len() + 1 {
            if let (Some(h), Some(l)) = (
                b.get(i + 1).and_then(|c| (*c as char).to_digit(16)),
                b.get(i + 2).and_then(|c| (*c as char).to_digit(16)),
            ) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// One replication connection's lifetime: decode pgoutput frames, buffer per transaction, append
/// at commit, acknowledge. Returns `Err` on any failure so the caller reconnects (the server then
/// resends from the confirmed position).
#[allow(clippy::too_many_arguments)]
async fn stream_loop(
    cfg: ReplicationConfig,
    log: &ChangeLogWriter,
    tables: &SharedTables,
    events: &dyn SchemaEvents,
    last_lsn: &Arc<std::sync::Mutex<String>>,
    sync_seq: &Arc<AtomicI64>,
    txn_cfg: &TxnBufferConfig,
    shutdown: &crate::shutdown::ShutdownToken,
    // Set once the server has actually delivered something, so the caller can tell "the server
    // never let us in" from "we streamed and then something went wrong" when choosing the next
    // backoff. NOT set at `connect`: that only spawns the worker task, and TCP, auth, `pg_hba` and
    // `START_REPLICATION` failures (a slot held by someone else, most of all) all surface on the
    // first `recv`. Setting it there would peg every one of those at the 1 s floor forever.
    connected: &mut bool,
) -> Result<StreamEnd> {
    // `ReplicationClient::connect` only spawns its worker; TCP/authentication and
    // START_REPLICATION happen before the worker can deliver its first frame. Bound both the
    // spawn call and that first receive, and make shutdown cancel either wait. Once the first
    // frame/keepalive is received, the ordinary stream loop below is intentionally long-lived.
    let mut client = match await_with_shutdown(ReplicationClient::connect(cfg), REPLICATION_SETUP_TIMEOUT, shutdown)
        .await
    {
        AwaitOutcome::Ready(client) => client,
        AwaitOutcome::Failed(e) => return Err(anyhow::Error::new(e)).context("replication connect"),
        AwaitOutcome::Shutdown => return Ok(StreamEnd::ShuttingDown),
        AwaitOutcome::TimedOut => anyhow::bail!("replication connect timed out after {:?}", REPLICATION_SETUP_TIMEOUT),
    };
    let initial = match await_with_shutdown(client.recv(), REPLICATION_SETUP_TIMEOUT, shutdown).await {
        AwaitOutcome::Ready(Some(event)) => event,
        AwaitOutcome::Ready(None) => return Ok(StreamEnd::Ended),
        AwaitOutcome::Failed(e) => return Err(anyhow::Error::new(e)).context("replication stream setup"),
        AwaitOutcome::Shutdown => {
            client.abort();
            return Ok(StreamEnd::ShuttingDown);
        }
        AwaitOutcome::TimedOut => {
            client.abort();
            anyhow::bail!("replication setup timed out after {:?}", REPLICATION_SETUP_TIMEOUT);
        }
    };
    let mut dec = Decoder::new(tables.clone());
    // Dropped on every exit path (a replaced `Begin`, an error return, the stream ending), which is
    // what removes a spilled transaction's temporary file (ADR-0003).
    let mut txn: Option<TxnBuffer> = None;
    let mut initial = Some(initial);
    loop {
        // The ONE safe point for a graceful stop: between messages, never inside the `Commit` arm.
        //
        // A commit that is being APPENDED runs to completion — the arm below is not a select branch,
        // so shutdown cannot interrupt it, and the chunked append + acknowledgement finish exactly
        // as they would otherwise. Stopping mid-TRANSACTION (buffered changes, no `Commit` yet) is
        // free: nothing was acknowledged, so Postgres re-delivers the whole transaction to the next
        // process, and the spill file goes with the dropped buffer.
        let ev = match initial.take() {
            Some(ev) => Some(ev),
            None => tokio::select! {
                biased;
                _ = shutdown.wait() => {
                    if txn.is_some() {
                        tracing::info!(
                            "replicator: shutdown requested mid-transaction; dropping the buffered changes \
                             unacknowledged — Postgres re-delivers the whole transaction after the restart"
                        );
                    }
                    return Ok(StreamEnd::ShuttingDown);
                }
                ev = client.recv() => ev.context("replication stream")?,
            },
        };
        *connected = true;
        let Some(ev) = ev else { return Ok(StreamEnd::Ended) };
        match ev {
            ReplicationEvent::Begin { xid, .. } => {
                txn = Some(TxnBuffer::new(xid, txn_cfg.clone()));
            }
            ReplicationEvent::XLogData { data, .. } => {
                let msg = pgoutput::decode(&data)?;
                // Schema-bearing messages are handled — and the engine's handling AWAITED — before
                // anything else is decoded, so the dependents of a drifted/truncated table are
                // already retired and its compiled schema already swapped by the time the next
                // change for it arrives (ADR-0005).
                let txn_ref = txn.as_ref().map(|t: &TxnBuffer| TxnRef { xid: t.xid() });
                if let Message::Relation { .. } = msg {
                    dec.on_relation(msg, events, txn_ref).await;
                    continue;
                }
                if let Message::Truncate { rel_ids } = &msg {
                    dec.on_truncate(rel_ids, events, txn_ref).await;
                    continue;
                }
                let Some(t) = txn.as_mut() else { continue };
                match dec.on_change(msg) {
                    // Serialized once, here, and only the bytes are kept — the buffer spills to
                    // disk past its memory cap, so a transaction of any size stays bounded
                    // (ADR-0003). A spill-file write failure tears the connection down
                    // unacknowledged, exactly like a failed append.
                    Decoded::Env(env) => t.push(env, data.len() as u64).context("buffering a change")?,
                    Decoded::Sync(n) => t.set_sync(n),
                    Decoded::None => {}
                }
            }
            ReplicationEvent::Commit { lsn, end_lsn, .. } => {
                let Some(mut t) = txn.take() else { continue };
                let t0 = std::time::Instant::now();
                let commit_lsn = lsn.to_string();
                let (ops, raw_bytes, sync) = (t.len(), t.raw_bytes(), t.sync());
                // Append the whole transaction — from memory, or from its spill file — in chunks,
                // in order, to the CURRENT segment. `?` here is the ack-after-the-last-chunk rule:
                // nothing below runs unless every chunk landed.
                append_commit_chunked(log, &mut t, &commit_lsn, t0).await?;
                // ...and only NOW, with every chunk on the log, is the transaction acknowledged.
                client.update_applied_lsn(end_lsn);
                *last_lsn.lock().unwrap() = commit_lsn;
                // Publish the drain-barrier sentinel only after the whole commit is on the streams
                // and acknowledged locally, so the barrier can't claim "drained" early.
                if let Some(n) = sync {
                    sync_seq.fetch_max(n, Ordering::Relaxed);
                }
                // Releasing the buffer here (rather than at the end of the arm) removes the spill
                // file as soon as the commit is durable, not after the rotation check.
                drop(t);
                // Per-txn replication metrics. `receive_lag` here is ingest-side append latency
                // (commit frame received → appended), not source-commit→receipt lag.
                if ops > 0 && crate::statsd::enabled() {
                    let lag_ms = t0.elapsed().as_secs_f64() * 1000.0;
                    crate::statsd::replication_txn(ops, raw_bytes, lag_ms);
                }
                // Rotation is a TRANSACTION-BOUNDARY decision, taken after the commit is on the log
                // and acknowledged: a segment never splits a transaction — chunking does not change
                // that, every chunk of a commit goes to the segment that was current when the
                // commit began — and a rotation that fails (storage hiccup) is logged and retried at
                // the next commit rather than failing (or duplicating) a commit that already landed
                // (ADR-0006). The one way a transaction still straddles segments is a predecessor
                // process that closed the segment under us mid-commit; `append_commit` routes the
                // remaining chunks forward to the open segment, and readers cross the pointer, so
                // the run stays ordered and complete.
                log.maybe_rotate().await;
            }
            ReplicationEvent::KeepAlive { .. }
            | ReplicationEvent::Message { .. }
            | ReplicationEvent::StoppedAt { .. } => {}
        }
    }
}

/// Append one whole transaction to the change log's CURRENT segment, in **chunks** (ADR-0003), and
/// report how many chunks it took.
///
/// The buffered changes are streamed out — from memory, or from the spill file if this transaction
/// outgrew its cap — and each is stamped with the commit LSN, the transaction's xid (the backfill
/// snapshot's xid-visibility fence) and its position within the transaction (`seq`, the sequencer's
/// de-duplication key, contiguous `0..n` across every chunk). Chunks are appended **in order**, each
/// small enough to be one durable-streams request body.
///
/// The final envelope of the final chunk additionally carries the
/// **transaction-end marker** (`headers.last`), which is what lets the sequencer recognise the run
/// as complete rather than fanning out chunk 1 on its own.
///
/// The chunks are contiguous on the segment (the ingestor is the only writer) and carry the same
/// `(txid, lsn)`, so the sequencer folds them back into one transaction. **Every** failure
/// propagates — the caller must not acknowledge the slot, must not publish `last_lsn` and must not
/// release the drain barrier unless this returns `Ok`. Postgres then re-delivers the whole
/// transaction on reconnect and the `(lsn, seq)` de-duplication discards the chunks that already
/// landed.
pub async fn append_commit_chunked(
    log: &ChangeLogWriter,
    buf: &mut TxnBuffer,
    commit_lsn: &str,
    started: std::time::Instant,
) -> Result<u64> {
    let (xid, changes, bytes, spilled) = (buf.xid(), buf.len(), buf.buffered_bytes(), buf.spilled());
    let stamp = Stamp { lsn: commit_lsn.to_string(), txid: xid.to_string() };
    let mut chunks = 0u64;
    let mut drain = buf.drain(stamp).context("reading the buffered transaction")?;
    while let Some(chunk) = drain.next_chunk().context("reading the buffered transaction")? {
        log.append_commit(&chunk).await.context("append changes")?;
        chunks += 1;
    }
    if chunks > 1 {
        crate::metrics::metrics().txn_chunked_appends.fetch_add(chunks, Ordering::Relaxed);
    }
    if spilled || chunks > 1 {
        tracing::info!(
            xid,
            changes,
            bytes,
            chunks,
            spilled,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0,
            "large transaction appended to the change log"
        );
    }
    Ok(chunks)
}

/// What one decoded DML message amounts to for the ingestor.
enum Decoded {
    Env(Envelope),
    Sync(i64),
    None,
}

/// Relation metadata learned from `R` messages on this connection. The `R` message's namespace is
/// KEPT (upstream decoded and discarded it), so two same-named tables in different schemas decode
/// to distinct identities instead of colliding.
struct RelMeta {
    /// `None` when `(namespace, name)` has no canonical `schema.name` spelling — a quoted
    /// identifier containing a dot or a quote (ADR-0002). Such a relation is untracked (its changes
    /// are dropped), which matches introspection: `pg::list_tables` skips it too, so no shape over
    /// it can exist to go stale.
    table: Option<TableRef>,
    /// Column names in `attnum` order — what a tuple's cells are zipped against.
    columns: Vec<String>,
}

/// Build the observed fingerprint from a decoded `Relation` message.
///
/// `pk: None` — deliberately. Under `REPLICA IDENTITY FULL` pgoutput flags EVERY column as part of
/// the identity key, so the wire cannot tell us the primary key; claiming one here would make every
/// `R` look like a primary-key change. A real PK change is caught by the reconciler's catalog read
/// instead (see [`SchemaFingerprint`]).
fn observed_fingerprint(replident: u8, columns: &[RelColumn]) -> SchemaFingerprint {
    SchemaFingerprint {
        columns: columns
            .iter()
            .map(|c| crate::schema::FingerprintColumn { name: c.name.clone(), type_oid: c.type_oid, typmod: c.typmod })
            .collect(),
        replident,
        pk: None,
    }
}

/// Stateful pgoutput→envelope decoder: tracks relation metadata and builds envelopes for tracked
/// tables (and sync counters for the `__el_sync` bookkeeping table).
///
/// `tables` is the engine's live, swappable schema view — never a private copy — so a schema the
/// drift handler swapped is in effect for the very next change decoded (ADR-0005).
struct Decoder {
    tables: SharedTables,
    rels: HashMap<u32, RelMeta>,
}

impl Decoder {
    fn new(tables: SharedTables) -> Self {
        Decoder { tables, rels: HashMap::new() }
    }

    /// Record a relation and, if what Postgres now reports differs from the compiled schema, report
    /// the drift to the engine and **wait** for it to be handled.
    async fn on_relation(&mut self, msg: Message, events: &dyn SchemaEvents, txn: Option<TxnRef>) {
        let Message::Relation { rel_id, namespace, name, replident, columns } = msg else { return };
        let table = TableRef::new(&namespace, &name)
            .map_err(|e| tracing::error!("replicator: unusable relation '{namespace}.{name}': {e:#}"))
            .ok();
        let observed = observed_fingerprint(replident, &columns);
        self.rels.insert(rel_id, RelMeta { table: table.clone(), columns: observed.column_names() });
        // Only a TRACKED table can drift into something the engine is serving wrongly. An unknown
        // relation (never introspected, or the sentinel) and a library-mode table (no compiled
        // fingerprint) are recorded and left alone.
        //
        // There is deliberately no "this relation is dead" latch here: what the ingestor may decode
        // is `tables` and only `tables`, which the engine updates as it resolves. A latch would
        // survive the resolution (a fresh `R` arrives only after more DDL or a reconnect) and
        // silently drop — and acknowledge — every change for a table that is working again.
        if let Some(table) = table {
            let compiled = self.tables.read().unwrap().get(&table).and_then(|ts| ts.fingerprint.clone());
            if let Some(compiled) = compiled
                && !compiled.still_serves(&observed)
            {
                events.on_schema_drift(&table, observed, txn).await;
            }
        }
    }

    /// `TRUNCATE`: the engine holds no row copy from which to synthesise the deletes, so every
    /// dependent of every truncated table is retired (ADR-0005). Awaited, like drift.
    ///
    /// Only **tracked** tables are reported. The publication is `FOR ALL TABLES`, so a truncate of
    /// any application table in the database reaches us; reporting one the engine does not sync
    /// would log a retirement and bump the drift metric over nothing.
    async fn on_truncate(&self, rel_ids: &[u32], events: &dyn SchemaEvents, txn: Option<TxnRef>) {
        // Scoped so the (non-`Send`) read guard cannot be held across the await below.
        let tables: Vec<TableRef> = {
            let tracked = self.tables.read().unwrap();
            rel_ids
                .iter()
                .filter_map(|id| self.rels.get(id))
                .filter_map(|r| r.table.clone())
                .filter(|t| t != sync_table() && tracked.contains_key(t))
                .collect()
        };
        if !tables.is_empty() {
            events.on_truncate(tables, txn).await;
        }
    }

    fn on_change(&self, msg: Message) -> Decoded {
        let rel_id = match &msg {
            Message::Insert { rel_id, .. } | Message::Update { rel_id, .. } | Message::Delete { rel_id, .. } => *rel_id,
            _ => return Decoded::None,
        };
        let Some(rel) = self.rels.get(&rel_id) else {
            tracing::error!("replicator: change for unknown relation id {rel_id} (no R message seen)");
            return Decoded::None;
        };
        let Some(table) = rel.table.as_ref() else { return Decoded::None };
        if table == sync_table() {
            if let Message::Insert { new, .. } | Message::Update { new, .. } = &msg {
                if let Some(n) = sync_counter(rel, new) {
                    return Decoded::Sync(n);
                }
            }
            return Decoded::None;
        }
        let tables = self.tables.read().unwrap();
        let Some(ts) = tables.get(table) else { return Decoded::None };
        match build_envelope(table, ts, &rel.columns, msg) {
            Some(env) => Decoded::Env(env),
            None => Decoded::None,
        }
    }
}

/// Extract the `n` counter from an `__el_sync` tuple.
fn sync_counter(rel: &RelMeta, tuple: &Tuple) -> Option<i64> {
    let idx = rel.columns.iter().position(|c| c == "n")?;
    match tuple.get(idx)? {
        Cell::Text(s) => s.parse().ok(),
        _ => None,
    }
}

/// Build an envelope from a decoded pgoutput DML message; LSN/xid/seq are stamped at `Commit`.
fn build_envelope(table: &TableRef, ts: &TableSchema, columns: &[String], msg: Message) -> Option<Envelope> {
    let make = |operation: &str, value: Option<Json>, old: Option<Json>, key_src: &Json| Envelope {
        // The wire `type` is the canonical `schema.name` — always qualified, never bare.
        type_: table.to_string(),
        key: key_from_obj(key_src, ts),
        value,
        old,
        headers: EnvelopeHeaders {
            operation: operation.to_string(),
            txid: None,
            offset: None,
            lsn: None,
            seq: None,
            last: None,
        },
    };
    match msg {
        Message::Insert { new, .. } => {
            let new = Json::Object(tuple_to_map(&new, columns, ts));
            Some(make("insert", Some(new.clone()), None, &new))
        }
        Message::Update { old, new, .. } => {
            let old_map = match old {
                Some(OldTuple::Full(t)) => Some(tuple_to_map(&t, columns, ts)),
                Some(OldTuple::Key(_)) | None => {
                    // No full old image: this change was written while the table's REPLICA IDENTITY
                    // was not FULL. The preceding `R` reported that identity, so the engine has
                    // already re-asserted FULL and retired every dependent of the table (ADR-0005)
                    // — nothing is being served from this row any more. Forward the new image
                    // without an old one, and say why.
                    tracing::warn!(
                        "replicator: UPDATE on {table} carries no full old image (REPLICA IDENTITY was \
                         not FULL when it was written); the table's dependents have been retired"
                    );
                    None
                }
            };
            let mut new_map = tuple_to_map(&new, columns, ts);
            // TOASTed-but-unchanged columns are omitted from new (see tuple_to_map); fill from old.
            if let Some(ref om) = old_map {
                for (k, v) in om {
                    new_map.entry(k.clone()).or_insert_with(|| v.clone());
                }
            }
            let new = Json::Object(new_map);
            let old = old_map.map(Json::Object);
            Some(make("update", Some(new.clone()), old, &new))
        }
        Message::Delete { old, .. } => {
            let OldTuple::Full(t) = old else {
                // Key-only image: written while REPLICA IDENTITY was not FULL. Retracting a phantom
                // mostly-NULL row would be wrong, and there is nothing left to retract from — the
                // identity regression already retired the table's dependents (ADR-0005).
                tracing::warn!(
                    "replicator: DELETE on {table} carries no full old image (REPLICA IDENTITY was not \
                     FULL when it was written); the table's dependents have been retired"
                );
                return None;
            };
            let old = Json::Object(tuple_to_map(&t, columns, ts));
            Some(make("delete", None, Some(old.clone()), &old))
        }
        _ => None,
    }
}

/// Zip a pgoutput tuple with its relation's column names into a JSON object, converting each text
/// value by the column's schema type. `UnchangedToast` cells are OMITTED so the caller can fill
/// them from the old image; columns not in the schema are skipped.
fn tuple_to_map(tuple: &Tuple, columns: &[String], ts: &TableSchema) -> Map<String, Json> {
    let mut out = Map::new();
    for (cell, name) in tuple.iter().zip(columns) {
        let Some(ty) = ts.index.get(name).map(|&idx| ts.columns[idx].1) else { continue };
        match cell {
            Cell::UnchangedToast => {}
            Cell::Null => {
                out.insert(name.clone(), Json::Null);
            }
            Cell::Text(text) => {
                out.insert(name.clone(), text_to_json(text, ty));
            }
        }
    }
    out
}

/// Extract the primary-key string from a parsed row object. For composite primary keys the column
/// values go through the same escape-and-join encoding [`TableSchema::key_string`] uses
/// ([`crate::schema::join_key_components`]), so envelope keys match the engine's byte for byte.
fn key_from_obj(obj: &Json, ts: &TableSchema) -> String {
    let one = |name: &str| -> String {
        match obj.get(name) {
            Some(Json::Null) | None => "null".to_string(),
            Some(Json::String(s)) => s.clone(),
            // Canonicalize through f64 for float pk columns so the envelope key matches the
            // engine's `Value::to_key_string` (serde would print `1.0` where f64 prints `1`).
            Some(Json::Number(n)) => match n.as_f64() {
                Some(f) if ts.index.get(name).is_some_and(|&i| ts.columns[i].1 == ColumnType::Float) => f.to_string(),
                _ => n.to_string(),
            },
            Some(Json::Bool(b)) => b.to_string(),
            Some(v) => v.to_string(),
        }
    };
    if ts.pk_cols.len() == 1 {
        return one(&ts.pk_name);
    }
    crate::schema::join_key_components(ts.pk_cols.iter().map(|&i| one(&ts.columns[i].0)))
}

/// Convert a pgoutput text-mode scalar to JSON per the column type (NULL arrives as its own cell
/// kind, never as text). A value that fails its type's parse (e.g. `NaN`/`Infinity` floats,
/// out-of-range numerics) degrades to NULL — logged, because a real value silently becoming SQL
/// NULL downstream is a corruption, not a convenience.
fn text_to_json(text: &str, ty: ColumnType) -> Json {
    let fail = |ty: &str| {
        tracing::error!("replicator: unparseable {ty} value {text:?} degraded to NULL");
        Json::Null
    };
    match ty {
        ColumnType::Int => text.parse::<i64>().map(Json::from).unwrap_or_else(|_| fail("int")),
        ColumnType::Float => text
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Json::Number)
            .unwrap_or_else(|| fail("float")),
        ColumnType::Bool => match text {
            "t" | "true" => Json::Bool(true),
            "f" | "false" => Json::Bool(false),
            _ => fail("bool"),
        },
        ColumnType::Text => Json::String(text.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::{Cell, Message, OldTuple};
    use crate::schema::{ColumnDef, FingerprintColumn, TableDef};
    use std::collections::BTreeMap;

    /// A recording [`SchemaEvents`] sink standing in for the engine: it records what the decoder
    /// reported and, like the real handler, updates the shared schema view — which is the ONLY
    /// thing the decoder consults about what it may decode.
    #[derive(Default)]
    struct RecordingEvents {
        drifts: std::sync::Mutex<Vec<(TableRef, SchemaFingerprint, Option<TxnRef>)>>,
        truncates: std::sync::Mutex<Vec<Vec<TableRef>>>,
        /// The shared view the "engine" resolves into: `Some(schema)` installs it, `None` drops the
        /// table (the dropped/unresolved outcome).
        tables: std::sync::Mutex<Option<SharedTables>>,
        resolves_to: std::sync::Mutex<Option<TableSchema>>,
    }

    impl RecordingEvents {
        fn drifted(&self) -> Vec<TableRef> {
            self.drifts.lock().unwrap().iter().map(|(t, ..)| t.clone()).collect()
        }
    }

    impl SchemaEvents for RecordingEvents {
        fn on_schema_drift<'a>(
            &'a self,
            table: &'a TableRef,
            observed: SchemaFingerprint,
            txn: Option<TxnRef>,
        ) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                self.drifts.lock().unwrap().push((table.clone(), observed, txn));
                if let Some(shared) = self.tables.lock().unwrap().as_ref() {
                    let mut w = shared.write().unwrap();
                    match self.resolves_to.lock().unwrap().clone() {
                        Some(ts) => {
                            w.insert(table.clone(), ts);
                        }
                        None => {
                            w.remove(table);
                        }
                    }
                }
            })
        }

        fn on_truncate<'a>(&'a self, tables: Vec<TableRef>, _txn: Option<TxnRef>) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                self.truncates.lock().unwrap().push(tables);
            })
        }
    }

    fn shared(tables: HashMap<TableRef, TableSchema>) -> SharedTables {
        Arc::new(std::sync::RwLock::new(tables))
    }

    fn fp_col(name: &str, type_oid: u32) -> FingerprintColumn {
        FingerprintColumn { name: name.into(), type_oid, typmod: -1 }
    }

    /// The `id/tenant/name` fingerprint in `attnum` order (which is NOT the compiled schema's
    /// alphabetical order — the two are deliberately independent).
    fn users_fingerprint() -> SchemaFingerprint {
        SchemaFingerprint {
            columns: vec![fp_col("id", 23), fp_col("tenant", 23), fp_col("name", 25)],
            replident: crate::schema::REPLICA_IDENTITY_FULL,
            pk: Some(vec!["id".into()]),
        }
    }

    fn users_def(fingerprint: Option<SchemaFingerprint>) -> TableDef {
        let mut columns = BTreeMap::new();
        columns.insert("id".to_string(), ColumnDef { ty: ColumnType::Int, pg_type: None, has_default: false });
        columns.insert("tenant".to_string(), ColumnDef { ty: ColumnType::Int, pg_type: None, has_default: false });
        columns.insert("name".to_string(), ColumnDef { ty: ColumnType::Text, pg_type: None, has_default: false });
        TableDef { columns, primary_key: vec!["id".to_string()], fingerprint }
    }

    /// A `users` schema for an arbitrary reference, so a test can track the same table NAME in two
    /// different schemas. No fingerprint: these tests exercise envelope building, not drift.
    fn users_in(refs: &[&str]) -> HashMap<TableRef, TableSchema> {
        let def = users_def(None);
        refs.iter()
            .map(|r| {
                let t = TableRef::parse(r).unwrap();
                (t.clone(), TableSchema::from_def(&t, &def).unwrap())
            })
            .collect()
    }

    fn users() -> HashMap<TableRef, TableSchema> {
        users_in(&["users"])
    }

    /// A `public.users` tracked WITH a fingerprint — the Postgres-mode shape, where drift applies.
    fn fingerprinted_users() -> HashMap<TableRef, TableSchema> {
        let t = TableRef::parse("users").unwrap();
        let ts = TableSchema::from_def(&t, &users_def(Some(users_fingerprint()))).unwrap();
        HashMap::from([(t, ts)])
    }

    fn rel_col(name: &str, type_oid: u32) -> crate::pgoutput::RelColumn {
        crate::pgoutput::RelColumn { name: name.into(), type_oid, typmod: -1, key: name == "id" }
    }

    /// An `R` message with `REPLICA IDENTITY FULL` and the given `(name, type oid)` columns.
    fn rel_msg(rel_id: u32, namespace: &str, name: &str, cols: &[(&str, u32)]) -> Message {
        Message::Relation {
            rel_id,
            namespace: namespace.into(),
            name: name.into(),
            replident: crate::schema::REPLICA_IDENTITY_FULL,
            columns: cols.iter().map(|(n, o)| rel_col(n, *o)).collect(),
        }
    }

    fn users_rel(rel_id: u32, namespace: &str) -> Message {
        rel_msg(rel_id, namespace, "users", &[("id", 23), ("tenant", 23), ("name", 25)])
    }

    async fn decoder(tables: &SharedTables) -> (Decoder, Arc<RecordingEvents>) {
        let ev = Arc::new(RecordingEvents::default());
        let mut d = Decoder::new(tables.clone());
        d.on_relation(users_rel(1, "public"), ev.as_ref(), None).await;
        d.on_relation(rel_msg(2, "public", sync_table().name(), &[("id", 23), ("n", 20)]), ev.as_ref(), None).await;
        (d, ev)
    }

    fn t(s: &str) -> Cell {
        Cell::Text(s.into())
    }

    fn env_of(d: Decoded) -> Envelope {
        match d {
            Decoded::Env(e) => e,
            _ => panic!("expected an envelope"),
        }
    }

    /// Register `public.users` as rel 1 and `other.users` as rel 2 — the SAME relname in two
    /// namespaces, which upstream's namespace-discarding decoder collapsed into one relation.
    async fn two_schema_decoder(tables: &SharedTables) -> Decoder {
        let ev = RecordingEvents::default();
        let mut d = Decoder::new(tables.clone());
        for (rel_id, namespace) in [(1u32, "public"), (2, "other")] {
            d.on_relation(users_rel(rel_id, namespace), &ev, None).await;
        }
        d
    }

    /// Tracking `public.users` must NOT make `other.users` a tracked table: its changes are dropped,
    /// not decoded onto the public table's shapes.
    #[tokio::test]
    async fn a_same_named_table_in_another_schema_is_not_tracked() {
        let tables = shared(users_in(&["public.users"]));
        let d = two_schema_decoder(&tables).await;

        let pubs = env_of(d.on_change(Message::Insert { rel_id: 1, new: vec![t("1"), t("7"), t("a")] }));
        assert_eq!(pubs.type_, "public.users");

        // rel 2 is `other.users` — untracked, so nothing is decoded for it.
        assert!(matches!(d.on_change(Message::Insert { rel_id: 2, new: vec![t("1"), t("7"), t("a")] }), Decoded::None));
    }

    /// With BOTH tracked, the two relations decode to distinct identities: the envelope `type` is the
    /// canonical `schema.name`, so the sequencer routes each to its own table's shapes.
    #[tokio::test]
    async fn both_schemas_tracked_decode_to_distinct_envelope_types() {
        let tables = shared(users_in(&["public.users", "other.users"]));
        let d = two_schema_decoder(&tables).await;

        let a = env_of(d.on_change(Message::Insert { rel_id: 1, new: vec![t("1"), t("7"), t("pub")] }));
        let b = env_of(d.on_change(Message::Insert { rel_id: 2, new: vec![t("1"), t("7"), t("oth")] }));
        assert_eq!(a.type_, "public.users");
        assert_eq!(b.type_, "other.users");
        // Same pk, same relname — only the `type` tells them apart.
        assert_eq!(a.key, b.key);
        assert_eq!(a.value.as_ref().unwrap()["name"], "pub");
        assert_eq!(b.value.as_ref().unwrap()["name"], "oth");
    }

    /// A relation whose name has no canonical `schema.name` spelling (a quoted identifier carrying a
    /// dot or a quote) is recorded as `table: None` and simply not tracked — never split into a
    /// schema + name that would address a different relation, and never a panic.
    #[tokio::test]
    async fn unspellable_relation_names_are_untracked_not_split() {
        let tables = shared(users_in(&["public.users"]));
        let ev = RecordingEvents::default();
        let mut d = Decoder::new(tables.clone());
        for (rel_id, name) in [(10u32, "odd.users"), (11, "od\"d")] {
            d.on_relation(rel_msg(rel_id, "public", name, &[("id", 23), ("tenant", 23), ("name", 25)]), &ev, None)
                .await;
            assert!(d.rels[&rel_id].table.is_none(), "{name} must not resolve to a table identity");
            assert!(matches!(
                d.on_change(Message::Insert { rel_id, new: vec![t("1"), t("7"), t("a")] }),
                Decoded::None
            ));
            // TRUNCATE on it must not panic either (there is no identity to retire).
            d.on_truncate(&[rel_id], &ev, None).await;
        }
        assert!(ev.truncates.lock().unwrap().is_empty());
        // In particular `public`.`odd.users` did NOT become the schema `odd`, table `users`.
        assert!(!tables.read().unwrap().contains_key(&TableRef::parse("odd.users").unwrap()));
    }

    #[tokio::test]
    async fn builds_insert_update_delete_with_old() {
        let tables = shared(users());
        let (d, _ev) = decoder(&tables).await;

        let ins = env_of(d.on_change(Message::Insert { rel_id: 1, new: vec![t("1"), t("7"), t("a")] }));
        assert_eq!(ins.headers.operation, "insert");
        assert_eq!(ins.key, "1");
        assert_eq!(ins.value.as_ref().unwrap()["name"], "a");
        assert_eq!(ins.value.as_ref().unwrap()["tenant"], 7);
        assert!(ins.old.is_none());

        let upd = env_of(d.on_change(Message::Update {
            rel_id: 1,
            old: Some(OldTuple::Full(vec![t("1"), t("7"), t("a")])),
            new: vec![t("1"), t("7"), t("b")],
        }));
        assert_eq!(upd.headers.operation, "update");
        assert_eq!(upd.old.as_ref().unwrap()["name"], "a");
        assert_eq!(upd.value.as_ref().unwrap()["name"], "b");

        let del = env_of(d.on_change(Message::Delete { rel_id: 1, old: OldTuple::Full(vec![t("1"), t("7"), t("b")]) }));
        assert_eq!(del.headers.operation, "delete");
        assert_eq!(del.key, "1");
        assert_eq!(del.old.as_ref().unwrap()["tenant"], 7);
        assert!(del.value.is_none());
    }

    #[tokio::test]
    async fn handles_null_and_utf8() {
        let tables = shared(users());
        let (d, _ev) = decoder(&tables).await;
        let e =
            env_of(d.on_change(Message::Insert {
                rel_id: 1, new: vec![t("5"), Cell::Null, t("a b 'c' café ☃ 北京")]
            }));
        assert_eq!(e.value.as_ref().unwrap()["tenant"], Json::Null);
        assert_eq!(e.value.as_ref().unwrap()["name"], "a b 'c' café ☃ 北京");
    }

    #[tokio::test]
    async fn toast_unchanged_value_filled_from_old() {
        let tables = shared(users());
        let (d, _ev) = decoder(&tables).await;
        let upd = env_of(d.on_change(Message::Update {
            rel_id: 1,
            old: Some(OldTuple::Full(vec![t("1"), t("7"), t("big original")])),
            new: vec![t("1"), t("9"), Cell::UnchangedToast],
        }));
        assert_eq!(upd.value.as_ref().unwrap()["tenant"], 9); // changed col taken from new
        assert_eq!(upd.value.as_ref().unwrap()["name"], "big original"); // unchanged toast from old
    }

    /// A DELETE / UPDATE without the full old image (the change was written while REPLICA IDENTITY
    /// was not FULL) must not fabricate retractions.
    #[tokio::test]
    async fn degraded_forms_are_skipped() {
        let tables = shared(users());
        let (d, _ev) = decoder(&tables).await;
        assert!(matches!(d.on_change(Message::Delete { rel_id: 1, old: OldTuple::Key(vec![t("1")]) }), Decoded::None));
        // Update with key-only old image still emits (new row is valid) but without `old`.
        let upd = env_of(d.on_change(Message::Update {
            rel_id: 1,
            old: Some(OldTuple::Key(vec![t("1"), Cell::Null, Cell::Null])),
            new: vec![t("1"), t("7"), t("b")],
        }));
        assert!(upd.old.is_none());
    }

    #[tokio::test]
    async fn sync_counter_from_sentinel_table_only() {
        let tables = shared(users());
        let (mut d, ev) = decoder(&tables).await;
        // A users row whose TEXT value mentions the sentinel is just data.
        let e = d.on_change(Message::Insert { rel_id: 1, new: vec![t("1"), t("7"), t("__el_sync n:999")] });
        assert!(matches!(e, Decoded::Env(_)));
        // The real sentinel update yields a sync counter, not an envelope.
        let s = d.on_change(Message::Update { rel_id: 2, old: None, new: vec![t("1"), t("5")] });
        assert!(matches!(s, Decoded::Sync(5)));

        // The sentinel is `public.__el_sync` SPECIFICALLY: a same-named table in another schema is
        // ordinary (here untracked) data, never a drain-barrier counter.
        d.on_relation(rel_msg(3, "other", sync_table().name(), &[("id", 23), ("n", 20)]), ev.as_ref(), None).await;
        let s = d.on_change(Message::Update { rel_id: 3, old: None, new: vec![t("1"), t("999")] });
        assert!(matches!(s, Decoded::None), "other.__el_sync must not bump the drain barrier");
    }

    /// Changes for relations that are not tracked (and not the sentinel) are ignored.
    #[tokio::test]
    async fn untracked_relations_are_ignored() {
        let tables = shared(users());
        let (mut d, ev) = decoder(&tables).await;
        d.on_relation(rel_msg(9, "public", "not_tracked", &[("id", 23)]), ev.as_ref(), None).await;
        assert!(matches!(d.on_change(Message::Insert { rel_id: 9, new: vec![t("1")] }), Decoded::None));
        assert!(ev.drifted().is_empty(), "an unknown relation is not drift");
    }

    #[tokio::test]
    async fn float_pk_key_is_canonicalized() {
        let mut columns = BTreeMap::new();
        columns.insert("id".to_string(), ColumnDef { ty: ColumnType::Float, pg_type: None, has_default: false });
        let def = TableDef { columns, primary_key: vec!["id".to_string()], fingerprint: None };
        let f = TableRef::parse("f").unwrap();
        let tables = shared(HashMap::from([(f.clone(), TableSchema::from_def(&f, &def).unwrap())]));
        let ev = RecordingEvents::default();
        let mut d = Decoder::new(tables);
        d.on_relation(rel_msg(3, "public", "f", &[("id", 701)]), &ev, None).await;
        let e = env_of(d.on_change(Message::Insert { rel_id: 3, new: vec![t("1")] }));
        assert_eq!(e.key, "1"); // f64 canonical form, not "1.0"
    }

    // ---- schema drift (ADR-0005) ----

    /// Every connection re-sends an `R` for each relation before its first change. A fingerprint
    /// that MATCHES the compiled one is a no-op — the engine is never told about it.
    #[tokio::test]
    async fn an_identical_relation_resend_is_not_drift() {
        let tables = shared(fingerprinted_users());
        let ev = RecordingEvents::default();
        let mut d = Decoder::new(tables);
        for _ in 0..3 {
            d.on_relation(users_rel(1, "public"), &ev, None).await;
        }
        assert!(ev.drifted().is_empty());
        // ...and the relation is still decodable.
        assert!(matches!(
            d.on_change(Message::Insert { rel_id: 1, new: vec![t("1"), t("7"), t("a")] }),
            Decoded::Env(_)
        ));
    }

    /// A changed relation reports EXACTLY ONCE per change: the drift, then silence while the new
    /// shape keeps being re-sent.
    #[tokio::test]
    async fn a_changed_relation_reports_once_per_change() {
        let tables = shared(fingerprinted_users());
        let ev = Arc::new(RecordingEvents::default());
        // The "engine" swaps in a schema whose fingerprint is the new one, as the real handler does.
        let t_ref = TableRef::parse("users").unwrap();
        let altered = SchemaFingerprint {
            columns: vec![fp_col("id", 23), fp_col("tenant", 23), fp_col("name", 25), fp_col("extra", 25)],
            replident: crate::schema::REPLICA_IDENTITY_FULL,
            pk: None, // as observed off the wire
        };
        let mut def = users_def(Some(altered.clone()));
        def.columns.insert("extra".to_string(), ColumnDef { ty: ColumnType::Text, pg_type: None, has_default: false });
        *ev.resolves_to.lock().unwrap() = Some(TableSchema::from_def(&t_ref, &def).unwrap());
        *ev.tables.lock().unwrap() = Some(tables.clone());

        let mut d = Decoder::new(tables.clone());
        let altered_msg = rel_msg(1, "public", "users", &[("id", 23), ("tenant", 23), ("name", 25), ("extra", 25)]);
        d.on_relation(altered_msg.clone(), ev.as_ref(), Some(TxnRef { xid: 7 })).await;
        assert_eq!(ev.drifted(), [t_ref.clone()]);
        assert_eq!(ev.drifts.lock().unwrap()[0].1, altered);
        // The enclosing transaction travels with the report — it is what lets the engine tell a
        // first delivery from a replay.
        assert_eq!(ev.drifts.lock().unwrap()[0].2, Some(TxnRef { xid: 7 }));

        // The handler swapped the compiled schema, so the re-sent `R` now matches and reports
        // nothing more.
        d.on_relation(altered_msg, ev.as_ref(), None).await;
        assert_eq!(ev.drifted().len(), 1, "an identical re-send after the swap is not drift again");

        // The next change decodes against the NEW schema: the added column is in the envelope.
        let e = env_of(d.on_change(Message::Insert { rel_id: 1, new: vec![t("1"), t("7"), t("a"), t("x")] }));
        assert_eq!(e.value.as_ref().unwrap()["extra"], "x");
    }

    /// Identical columns with a regressed replica identity is drift too.
    #[tokio::test]
    async fn a_replica_identity_regression_reports_drift() {
        let tables = shared(fingerprinted_users());
        let ev = RecordingEvents::default();
        let mut d = Decoder::new(tables);
        let mut msg = users_rel(1, "public");
        if let Message::Relation { replident, .. } = &mut msg {
            *replident = b'd';
        }
        d.on_relation(msg, &ev, None).await;
        assert_eq!(ev.drifted(), [TableRef::parse("users").unwrap()]);
        assert_eq!(ev.drifts.lock().unwrap()[0].1.replident, b'd');
    }

    /// A library-mode table (no compiled fingerprint) is never drift-checked, whatever `R` says.
    #[tokio::test]
    async fn library_mode_tables_are_never_drift_checked() {
        let tables = shared(users()); // no fingerprint
        let ev = RecordingEvents::default();
        let mut d = Decoder::new(tables);
        d.on_relation(rel_msg(1, "public", "users", &[("id", 23), ("gone", 25)]), &ev, None).await;
        assert!(ev.drifted().is_empty());
    }

    /// A table the engine drops (or parks) leaves the shared view, and THAT is what stops the
    /// decoder — there is no separate latch to go stale. The relation resumes decoding the moment
    /// the engine puts the table back, with no new `R` needed (which is exactly what a retry that
    /// resolves an unresolved table does).
    #[tokio::test]
    async fn the_shared_view_alone_decides_what_is_decoded() {
        let tables = shared(fingerprinted_users());
        let ev = RecordingEvents::default(); // `resolves_to` stays None = the table goes away
        *ev.tables.lock().unwrap() = Some(tables.clone());
        let mut d = Decoder::new(tables.clone());
        d.on_relation(rel_msg(1, "public", "users", &[("id", 23)]), &ev, None).await;
        assert_eq!(ev.drifted(), [TableRef::parse("users").unwrap()]);
        assert!(matches!(d.on_change(Message::Insert { rel_id: 1, new: vec![t("1")] }), Decoded::None));
        // ...and it is not reported as a truncate target while it is out of the view.
        d.on_truncate(&[1], &ev, None).await;
        assert!(ev.truncates.lock().unwrap().is_empty());

        // Put it back — as a resolution does — and the SAME relation decodes again immediately.
        tables.write().unwrap().extend(fingerprinted_users());
        let e = env_of(d.on_change(Message::Insert { rel_id: 1, new: vec![t("1")] }));
        assert_eq!(e.type_, "public.users");
    }

    /// TRUNCATE reports every truncated TRACKED table in one call; the sentinel is not one, and
    /// neither is an application table the engine does not sync (the publication is FOR ALL TABLES,
    /// so those reach the ingestor too).
    #[tokio::test]
    async fn truncate_reports_only_the_tracked_tables_it_hit() {
        let tables = shared(users());
        let (mut d, ev) = decoder(&tables).await;
        d.on_relation(rel_msg(9, "public", "not_tracked", &[("id", 23)]), ev.as_ref(), None).await;
        d.on_truncate(&[1, 2, 9], ev.as_ref(), None).await;
        assert_eq!(ev.truncates.lock().unwrap().as_slice(), [vec![TableRef::parse("users").unwrap()]]);

        // A truncate that hits ONLY untracked relations reports nothing at all.
        d.on_truncate(&[2, 9], ev.as_ref(), None).await;
        assert_eq!(ev.truncates.lock().unwrap().len(), 1);
    }
}

#[cfg(test)]
mod backoff_tests {
    use super::*;

    /// The setup wait must turn a worker that is stuck before its first frame (for example, a
    /// blackholed TCP endpoint) into a retryable timeout instead of pinning the ingestor forever.
    #[tokio::test]
    async fn blackholed_replication_setup_times_out() {
        let shutdown = crate::shutdown::ShutdownToken::new();
        let outcome = await_with_shutdown(
            std::future::pending::<std::result::Result<Option<u8>, &'static str>>(),
            std::time::Duration::from_millis(10),
            &shutdown,
        )
        .await;
        assert!(matches!(outcome, AwaitOutcome::TimedOut));
    }

    /// Shutdown wins an in-flight setup wait, including when the socket never produces a frame.
    #[tokio::test]
    async fn shutdown_interrupts_replication_setup_wait() {
        let shutdown = crate::shutdown::ShutdownToken::new();
        let waiter = await_with_shutdown(
            std::future::pending::<std::result::Result<Option<u8>, &'static str>>(),
            std::time::Duration::from_secs(30),
            &shutdown,
        );
        shutdown.begin();
        assert!(matches!(waiter.await, AwaitOutcome::Shutdown));
    }

    /// A healthy setup that completes promptly is returned unchanged; the timeout is only for
    /// setup, so the normal long-lived idle stream path remains available after this point.
    #[tokio::test]
    async fn healthy_replication_setup_completes_before_timeout() {
        let shutdown = crate::shutdown::ShutdownToken::new();
        let outcome = await_with_shutdown(
            async { Ok::<_, &'static str>(Some(7u8)) },
            std::time::Duration::from_secs(1),
            &shutdown,
        )
        .await;
        assert!(matches!(outcome, AwaitOutcome::Ready(Some(7))));
    }

    /// The reconnect schedule doubles from one second and stops at thirty (ADR-0004). A flat retry
    /// hammers a server that is down; an uncapped one leaves an engine asleep long after the outage
    /// ended.
    #[test]
    fn the_backoff_schedule_doubles_and_caps() {
        let secs = |a: u32| backoff_base(a).as_secs();
        assert_eq!([secs(0), secs(1), secs(2), secs(3), secs(4)], [1, 2, 4, 8, 16]);
        // 32s would exceed the ceiling, so it and everything after it sit at 30s.
        assert_eq!(secs(5), 30);
        assert_eq!(secs(9), 30);
        // …including attempt counts big enough to overflow a naive `1 << attempt`.
        assert_eq!(secs(64), 30);
        assert_eq!(secs(u32::MAX), 30);
    }

    /// Jitter stays inside ±25% of the base for every possible clock reading — many engines
    /// reconnecting to the same server must not do it in lockstep, but neither may the delay
    /// collapse to zero or run away.
    #[test]
    fn jitter_stays_within_a_quarter_of_the_base() {
        for attempt in 0..8u32 {
            let base = backoff_base(attempt);
            let lo = base.mul_f64(0.75);
            let hi = base.mul_f64(1.25);
            for nanos in [0u32, 1, 12_345, 500_000_000, 999_999_999] {
                let d = jitter(base, nanos);
                assert!(d >= lo && d <= hi, "attempt {attempt}, nanos {nanos}: {d:?} outside {lo:?}..{hi:?}");
            }
        }
        // The whole window is actually used: the extremes of the clock reading differ.
        let base = backoff_base(3);
        assert_ne!(jitter(base, 0), jitter(base, 999_999_999));
    }

    /// A sub-millisecond base has no jitter window to speak of; it must be returned untouched
    /// rather than underflowing.
    #[test]
    fn a_tiny_base_is_returned_as_is() {
        let tiny = std::time::Duration::from_micros(10);
        assert_eq!(jitter(tiny, 12_345), tiny);
    }

    /// Drive the schedule the way the ingest loop does, over a sequence of outcomes.
    fn schedule(outcomes: &[bool]) -> Vec<u64> {
        let mut attempt = 0u32;
        outcomes
            .iter()
            .map(|&connected| {
                let (base, next) = next_backoff(attempt, connected);
                attempt = next;
                base.as_secs()
            })
            .collect()
    }

    /// A run of failures that never got a message out of the server — a wrong password, a `pg_hba`
    /// rule, another engine holding the slot — must actually climb to the ceiling. This is the case
    /// that regressed when `connected` was set at `connect` (which only spawns the worker) instead
    /// of at the first `recv`: every such failure looked like a healthy connection that ended, and
    /// the loop retried at the 1 s floor forever.
    #[test]
    fn repeated_connection_failures_climb_to_the_ceiling() {
        assert_eq!(schedule(&[false; 8]), [1, 2, 4, 8, 16, 30, 30, 30]);
    }

    /// A connection that delivered resets the schedule: the wait after it drops back to the floor
    /// and the NEXT outage climbs from the bottom again, rather than resuming wherever the last one
    /// had escalated to.
    #[test]
    fn a_delivering_connection_resets_the_schedule() {
        assert_eq!(schedule(&[false, false, false, true, false, false]), [1, 2, 4, 1, 2, 4]);
        // …and an engine that keeps losing a working stream never escalates past the floor at all.
        assert_eq!(schedule(&[true; 4]), [1, 1, 1, 1]);
    }
}
