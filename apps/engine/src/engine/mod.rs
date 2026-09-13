//! Engine orchestration: schema/shape registries and one tailer task per table. A tailer holds only
//! per-shape routing metadata (no table data): it fans each change out to standalone filters and to
//! equality shapes routed by key, and appends the filtered deltas (as State-Protocol envelopes) to
//! the shape streams. Shapes backfill from Postgres on registration; see `add_shape_routed`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::value::{Tup2, ZWeight};
use anyhow::{Context, Result, bail};
use tokio::sync::{Mutex, mpsc};

use std::sync::atomic::Ordering;

use crate::changelog::{ChangeLogWriter, ChangesState, LogPosition, segment_path};
use crate::ds::{DsClient, Envelope, EnvelopeHeaders};
use crate::heap_size::HeapSize;
use crate::metrics::{Timer, metrics};
use crate::predicate::{CompiledPredicate, PredicateJson};
use crate::retention::{EvictReason, Evicted, LifeState, RetentionConfig, ShapeLife, SweepShape};
use crate::schema::{Schema, SharedTables, TableSchema, compile_schema};
use crate::subquery::{SubqueryRegistry, predicate_has_subquery, referenced_tables};
use crate::table_ref::{TableRef, TableSelector};
use crate::value::{Row, Value};

mod catalog;
mod circuit_serving;
mod drift;
pub(crate) mod emission;
pub(crate) mod epoch;
mod executors;
mod introspection;
mod lifecycle;
pub(crate) mod membership;
mod output;
mod planning;
mod retirement;
mod sequencer;
#[cfg(test)]
mod tests;

use catalog::*;
use circuit_serving::*;
use epoch::*;
use executors::*;
use introspection::*;
use planning::*;
use retirement::*;
use sequencer::*;

pub use catalog::RestoreStreamVanished;
pub use drift::ResolveLock;
pub use epoch::{EpochBreakReason, EpochBroken, EpochResetting, SlotBinding};
pub use executors::AggFn;
pub use introspection::{
    AggInfo, ArrConsumer, ArrCounts, ArrIndex, ArrInput, ArrangementGraph, EngineGraph, FamilyStat, GraphEdge,
    GraphNode, GraphShape, NodeIndex, NodeStateSummary, NodeValue, OpEdge, OpNode, ShapeRecord, StateSnapshot,
    TableColumnInfo, TableSchemaInfo, TableStats,
};
pub(crate) use output::{
    absolute_envelope, apply_envelope, delete_envelopes, needs_absolute_emission, translate_output,
};
pub use planning::CircuitPlacement;

/// `GET /v1/health` phases (see [`Engine::health`]).
const HEALTH_WAITING: u8 = 0;
const HEALTH_STARTING: u8 = 1;
const HEALTH_ACTIVE: u8 = 2;

/// The engine computed membership effects it could not deliver (see [`DegradeState`]), so what it
/// serves is silently wrong. A typed error: the HTTP layer maps it to 503 by downcast, never by
/// matching on message text.
#[derive(Debug)]
pub struct Degraded;

impl std::fmt::Display for Degraded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("degraded: subquery membership effects were lost; restart required")
    }
}

impl std::error::Error for Degraded {}

/// The Postgres boot has not finished: the durable catalog is not restored yet, so there is no
/// registry to create into, join, reactivate or release against (ADR-0009). Typed, like
/// [`Degraded`], so both HTTP surfaces answer 503 with `Retry-After` by downcast.
#[derive(Debug)]
pub struct Booting;

impl std::fmt::Display for Booting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("engine is still booting (the durable shape catalog is not restored yet); retry")
    }
}

impl std::error::Error for Booting {}

/// `POST /schema` against a Postgres-mode engine. The schema there IS Postgres's — introspected at
/// boot and kept current by drift detection (ADR-0005) — so a caller-supplied one has no meaning,
/// and honouring it would do real damage: it creates the change-log segment and records a rotation
/// before the boot has folded the catalog, replaces the compiled tables under the restore, and
/// spawns an ordinary reading sequencer the restore does not own. Typed, so the HTTP layer answers
/// 409 (the request conflicts with how this engine runs) by downcast.
#[derive(Debug)]
pub struct SchemaIsPostgres;

impl std::fmt::Display for SchemaIsPostgres {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "this engine runs in Postgres mode: its schema is introspected from Postgres, and POST /schema \
             is for library mode only",
        )
    }
}

impl std::error::Error for SchemaIsPostgres {}

/// A create or join finished its **catalog durability wait** only to find that the shape it was
/// about to acknowledge is no longer the shape it made: retired by a schema drift / `TRUNCATE` /
/// purge while the wait was in flight, or belonging to a superseded epoch.
///
/// `send_durable` is an unbounded wait on external storage, so it opens a whole new — externally
/// controllable — interval between a create's last check and its answer. Typed, because the request
/// itself is still perfectly valid: only this attempt lost a race, so the create is redone rather
/// than making every client implement that retry (see `Engine::recheck_after_durability`). Reaching
/// a caller at all means the retries were exhausted, which is a 503: come back.
#[derive(Debug)]
pub struct CreateRaced(pub String);

impl std::fmt::Display for CreateRaced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}; retry", self.0)
    }
}

impl std::error::Error for CreateRaced {}

/// A create named a **subscription id another shape already holds** (ADR-0008).
///
/// Typed because it is a 409, not a 500 and not a retry: nothing about waiting changes the answer,
/// and the request is not malformed either — the caller simply used one name for two different
/// shapes. Accepting it would leave that caller holding a single id against two subscriptions,
/// unable to release either without saying which, which is the ambiguity the id exists to remove.
#[derive(Debug)]
pub struct SubscriptionConflict {
    pub subscription: String,
    /// The shape that holds it now.
    pub shape: String,
}

impl std::fmt::Display for SubscriptionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "subscription '{}' already belongs to shape '{}'; a subscription id names one shape — \
             release it first, or use a different id for this one",
            self.subscription, self.shape
        )
    }
}

impl std::error::Error for SubscriptionConflict {}

/// Fail-closed degradation state, latched when a flip batch exhausts its retries.
///
/// The inner-set node was already reconciled under the registry lock before the batch's query-backs
/// ran, so an abandoned batch's effects can never be re-derived — only a restart, which re-seeds
/// every node from Postgres, makes the engine right again. Until then it refuses every read that
/// would carry membership rather than serve the lie.
pub(crate) struct DegradeState {
    degraded: std::sync::atomic::AtomicBool,
    /// Flip batches abandoned so far (`GET /replication/lsn` → `flipFailures`).
    failures: std::sync::atomic::AtomicU64,
    /// Wakes the stream reaper on the transition (see `Engine::ensure_degrade_reaper`).
    wake: tokio::sync::watch::Sender<bool>,
    reaper_started: std::sync::atomic::AtomicBool,
}

impl DegradeState {
    fn new() -> Arc<Self> {
        Arc::new(DegradeState {
            degraded: std::sync::atomic::AtomicBool::new(false),
            failures: std::sync::atomic::AtomicU64::new(0),
            wake: tokio::sync::watch::channel(false).0,
            reaper_started: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Count one abandoned batch and latch the engine degraded (idempotent; never cleared).
    fn mark(&self) {
        self.failures.fetch_add(1, Ordering::SeqCst);
        self.degraded.store(true, Ordering::SeqCst);
        let _ = self.wake.send(true); // no reaper subscribed = no subquery shape to reap
    }
}

#[derive(Clone)]
pub struct Engine {
    ds: DsClient,
    state: Arc<Mutex<EngineState>>,
    /// Postgres connection string when running in Postgres mode (logical replication + query-back
    /// backfill, no in-memory `table_state`). `None` keeps the engine usable only as a library shell.
    pg_url: Option<String>,
    /// Last commit LSN the replication ingestor has appended (observability).
    repl_lsn: Arc<std::sync::Mutex<String>>,
    /// Highest `__el_sync` sentinel counter the ingestor has decoded-and-appended. The drain barrier
    /// bumps the sentinel and waits for this to catch up — robust under a shared multi-database
    /// Postgres (per-database, no dependence on server-global WAL LSNs).
    repl_sync: Arc<std::sync::atomic::AtomicI64>,
    /// Set once `setup_postgres` has succeeded — its last fallible step is behind it and the
    /// replication ingestor has been spawned. A second call is refused against it (see
    /// `setup_postgres`).
    replicator_started: Arc<std::sync::atomic::AtomicBool>,
    /// Boot readiness phase driving `GET /v1/health`: 0 = `waiting` (Postgres not connected), 1 =
    /// `starting` (connected; introspecting / creating slot / spawning ingest), 2 = `active` (ingest
    /// loop running). Library mode (no Postgres) is `active` from construction.
    health: Arc<std::sync::atomic::AtomicU8>,
    /// Cross-table subquery registry: maintained inner-set nodes (shared by canonical signature) + the
    /// outer subquery shapes that depend on them. Every tailer routes its deltas here so an inner-table
    /// change moves outer rows. `None`-free; empty until a subquery shape is created.
    subqueries: Arc<Mutex<SubqueryRegistry>>,
    /// Best-effort per-envelope trace broadcast (see [`crate::trace`]). Events are serialized once
    /// and only when someone is subscribed; slow subscribers lag and drop.
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
    /// Sender to the single flip-propagator task: inner-set flips detected by a tailer are handed
    /// off here so their Postgres query-backs run off the tailer hot path (see
    /// [`crate::subquery::propagate_flips`]).
    flip_tx: mpsc::UnboundedSender<FlipWork>,
    /// Flip batches enqueued but not yet fully propagated. Part of the convergence barrier:
    /// drained change log + `pending_flips == 0` ⇒ all subquery effects have landed. An abandoned
    /// batch keeps its count forever, so the barrier can never report "everything landed" over
    /// effects that were lost — the engine degrades instead (see [`DegradeState`]).
    pending_flips: Arc<std::sync::atomic::AtomicI64>,
    /// Fail-closed degradation latch + abandoned-batch count (see [`DegradeState`]).
    degrade: Arc<DegradeState>,
    /// Table schemas shared with the sequencer task and the replication ingestor — the set of
    /// tables the engine can **currently decode and route**, and the single place a drift swaps a
    /// schema (ADR-0005).
    ///
    /// It is a subset of `EngineState::tables` (which is what the engine *knows about*, and what
    /// the reconciler reconciles): a table whose drift is unresolved is removed from here, so its
    /// changes are dropped at the decoder rather than decoded against a schema the engine knows is
    /// stale, while staying in `tables` so it is still reconciled and still gives API callers the
    /// specific "unresolved, retry" refusal rather than "unknown table".
    tables_shared: SharedTables,
    /// Ordered writer for the durable shape catalog (see [`CATALOG_STREAM`]).
    catalog_tx: CatalogWriter,
    /// Shape streams whose retirement (close, then delete — ADR-0007) storage refused, retried in
    /// the background until it lands. See [`crate::engine::retirement`]: the `Dropped` record is the
    /// durable intent, so nothing here is lost by a restart.
    retirements: RetirementQueue,
    /// Durable purge completion barriers keyed by shape id. A retry joins the original teardown,
    /// rather than launching a second completion task or acknowledging before retirement lands.
    purge_barriers: Arc<std::sync::Mutex<HashMap<String, Arc<PurgeBarrier>>>>,
    #[cfg(test)]
    pub(crate) purge_test_hook: Arc<PurgeTestHook>,
    /// The segmented change log (ADR-0006): which segment the ingestor appends to, when each
    /// segment began, and the rotation policy. Held by the engine (not just the ingestor) because
    /// the retention sweeper deletes segments and the epoch reset rotates one.
    changes: ChangeLogWriter,
    /// Change-log position the sequencer starts from (set by catalog restore before the spawn).
    seq_start: Arc<std::sync::Mutex<LogPosition>>,
    /// The `(lsn, seq)` de-duplication highwater the sequencer starts from, restored with the
    /// checkpoint (ADR-0003). `None` = start de-duplicating from nothing.
    seq_highwater: Arc<std::sync::Mutex<Option<(u64, u64)>>>,
    /// Per-shape retention lifecycle + last-read instant. A separate sync mutex (not
    /// `EngineState`) so hot read paths can touch it without the async engine lock. Lock order:
    /// when both are held, `state` first, then `lives`; never across `.await`.
    lives: Arc<std::sync::Mutex<HashMap<String, ShapeLife>>>,
    /// Retention policy knobs (see `crate::retention`).
    retention: Arc<RetentionConfig>,
    /// Set once the background retention sweeper has been spawned (lazy, idempotent).
    retention_started: Arc<std::sync::atomic::AtomicBool>,
    /// Set once the background schema reconciler has been spawned (see `engine::drift`).
    reconciler_started: Arc<std::sync::atomic::AtomicBool>,
    /// Per-table serialisation of drift resolutions, plus the set currently running (see
    /// [`drift::Resolving`]). Every trigger runs its own resolution, one at a time per table, and a
    /// shape create on a table being resolved is refused rather than allowed to install against a
    /// schema that is mid-swap.
    resolving: Arc<drift::Resolving>,
    /// Tables with an unresolved-retry task running (coalescing — one task per table). A std lock
    /// so the task's drop guard can clear it without an await.
    retrying: Arc<std::sync::Mutex<HashSet<TableRef>>>,
    /// dbsp arrangement settings (`ELECTRIC_CIRCUITS_DBSP*`), set before `setup_postgres`.
    dbsp_cfg: Arc<std::sync::Mutex<Option<crate::config::DbspConfig>>>,
    /// Large-transaction settings for the ingestor's per-transaction buffer (ADR-0003), set before
    /// `setup_postgres` (which spawns the ingestor). Defaults apply when nothing sets them — the
    /// binary always does, from the boot config.
    txn_cfg: Arc<std::sync::Mutex<crate::txn_buffer::TxnBufferConfig>>,
    /// The dbsp arrangement layer, once started (see [`crate::arrangements`]).
    arrangements: Arc<std::sync::Mutex<Option<crate::arrangements::Arrangements>>>,
    /// Per-table seed-snapshot gates fencing the arrangement feed (fresh seeds only; empty
    /// after a checkpoint restore, where the highwater does the fencing instead).
    arr_gates: Arc<std::sync::RwLock<HashMap<TableRef, crate::pg::SnapshotGate>>>,
    /// Which replication slot, in which cluster, this engine is bound to — and what to do when that
    /// stops being true (see [`engine::epoch`], ADR-0004).
    epoch: Arc<EpochState>,
    /// The change-log envelope the sequencer could not process, if it has parked on one (ADR-0010).
    /// Written by the sequencer task through [`sequencer::FailClosed`], cleared by an epoch reset, and
    /// served on `GET /metrics` and `GET /replication/lsn` — "degraded" alone does not tell an
    /// operator which envelope to look at, and nothing else in the process knows.
    change_log_failure: Arc<std::sync::Mutex<Option<ChangeLogFailure>>>,
    /// The process's graceful-shutdown state (see [`crate::shutdown`]). Held here — not in a global
    /// — because every part that must join it (the sequencer's select, the ingestor, the `/v1/shape`
    /// live poll, `GET /ready`) already has an `Engine`.
    shutdown: crate::shutdown::ShutdownToken,
    /// Per-process nonce for the subscription ids the engine mints for creates that named none
    /// (ADR-0008). The counter alone would not do: the catalog outlives the process, so a restart
    /// would re-mint ids a restored shape still holds.
    sub_nonce: Arc<str>,
}

pub(crate) struct PurgeBarrier {
    dropped_durable: std::sync::atomic::AtomicBool,
    dropped_notify: tokio::sync::Notify,
    done: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct PurgeTestHook {
    pub(crate) pause_after_remove: std::sync::atomic::AtomicBool,
    pub(crate) removed: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
}

impl PurgeBarrier {
    fn new() -> Self {
        Self {
            dropped_durable: std::sync::atomic::AtomicBool::new(false),
            dropped_notify: tokio::sync::Notify::new(),
            done: std::sync::atomic::AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn mark_dropped_durable(&self) {
        self.dropped_durable.store(true, Ordering::Release);
        self.dropped_notify.notify_waiters();
    }

    async fn wait_dropped_durable(&self) {
        loop {
            let notified = self.dropped_notify.notified();
            if self.dropped_durable.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn complete(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

/// A unit of deferred subquery propagation for the flip propagator (see [`Engine::flip_tx`]).
pub(crate) enum FlipWork {
    /// One tailer envelope's (or one create's replay's) worth of inner-set flips, walked up the
    /// dependency DAG from the flipped nodes.
    Walk {
        work: std::collections::VecDeque<(crate::predicate::SubquerySig, crate::subquery::Flip)>,
        txid: Option<String>,
        /// The originating write's commit lsn, threaded through to the deferred flip's trace event
        /// so it carries the same lsn/txid as the direct-change event that triggered the
        /// propagation — letting the activity log group them as one write (see
        /// `subquery::emit_flip_trace`).
        lsn: Option<String>,
    },
    /// Work that reached a shape while it was still being created, run against that one shape now
    /// that it is installed (see `SubqueryRegistry::finish_create`). Its dependency walk already
    /// happened, so there is nothing left to walk.
    Deferred { shape_id: String, work: std::collections::VecDeque<crate::subquery::DeferredShapeWork> },
    /// Work that reached a fresh parent NODE while a create was still seeding it, re-derived now
    /// that its set is installed (see `SubqueryRegistry::finish_create`). Unlike `Deferred`, this
    /// one DOES walk: reconciling the node produces flips, and the dependents below it — the
    /// create's own shape included — have not seen them.
    DeferredNode {
        work: std::collections::VecDeque<(crate::predicate::SubquerySig, crate::subquery::DeferredNodeWork)>,
    },
}

/// Everything a tailer needs to route deltas through the subquery layer: the shared registry for
/// the synchronous node-reconcile + outer-emission phases, and the deferral channel + pending
/// counter for flip propagation.
#[derive(Clone)]
struct SubqueryHandle {
    registry: Arc<Mutex<SubqueryRegistry>>,
    flip_tx: mpsc::UnboundedSender<FlipWork>,
    pending_flips: Arc<std::sync::atomic::AtomicI64>,
}

/// Spawn the flip-propagation dispatcher: FlipWork batches run **concurrently**, bounded by a
/// semaphore (`ELECTRIC_CIRCUITS_FLIP_WORKERS`, default 8) — the Postgres round-trips are the
/// dominant cost and are independent across batches. Correctness does not depend on
/// propagation order: membership evaluation happens under the registry lock and the resulting
/// envelopes are **enqueued under that same lock** into per-stream FIFO emission lanes
/// (`engine::emission`), so per-shape append order equals eval order regardless of which
/// worker ran the query-back; absolute per-pk emission makes concurrent re-derivations
/// convergent (see `subquery.rs`).
fn spawn_flip_propagator(
    registry: Arc<Mutex<SubqueryRegistry>>,
    mut rx: mpsc::UnboundedReceiver<FlipWork>,
    pending: Arc<std::sync::atomic::AtomicI64>,
    degrade: Arc<DegradeState>,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
) {
    let workers: usize =
        std::env::var("ELECTRIC_CIRCUITS_FLIP_WORKERS").ok().and_then(|v| v.parse().ok()).unwrap_or(8).max(1);
    tokio::spawn(async move {
        let sem = Arc::new(tokio::sync::Semaphore::new(workers));
        while let Some(fw) = rx.recv().await {
            let permit = sem.clone().acquire_owned().await.expect("flip semaphore");
            let registry = registry.clone();
            let pending = pending.clone();
            let degrade = degrade.clone();
            let trace_tx = trace_tx.clone();
            tokio::spawn(async move {
                // Both arms report `(error, items the batch never got through)`; a deferred batch
                // is treated exactly like a live one, because its effects are just as lost.
                let failed = match fw {
                    FlipWork::Walk { mut work, txid, lsn } => {
                        crate::subquery::propagate_with_retry(&registry, &mut work, txid, lsn, &trace_tx)
                            .await
                            .err()
                            .map(|e| (e, work.len()))
                    }
                    FlipWork::Deferred { shape_id, mut work } => {
                        crate::subquery::propagate_deferred_with_retry(&registry, &shape_id, &mut work)
                            .await
                            .err()
                            .map(|e| (e, work.len()))
                    }
                    FlipWork::DeferredNode { mut work } => {
                        // The walk the re-derivations feed starts empty and is carried across the
                        // retries with them, so an exhausted batch reports both halves of what it
                        // never got through.
                        let mut walk = std::collections::VecDeque::new();
                        crate::subquery::propagate_deferred_node_with_retry(&registry, &mut work, &mut walk, &trace_tx)
                            .await
                            .err()
                            .map(|e| (e, work.len() + walk.len()))
                    }
                };
                if let Some((e, unfinished)) = failed {
                    // Fail closed. The inner-set node was reconciled before these query-backs ran,
                    // so nothing will ever re-derive the rows this batch was carrying: the effects
                    // are lost, not delayed. Keep the batch's pending count held — a consumer
                    // gating on `pendingFlips == 0` must never be told everything landed when it
                    // did not — and latch the engine degraded so every membership-bearing route
                    // refuses instead of serving membership the engine knows is wrong.
                    tracing::error!(
                        "subquery flip propagation ABANDONED after {} attempts ({} item(s) unfinished): {e:#}; \
                         membership effects lost; engine degraded",
                        crate::subquery::FLIP_ATTEMPTS,
                        unfinished
                    );
                    degrade.mark();
                    drop(permit);
                    return;
                }
                // Decremented only after propagation finished enqueueing every resulting
                // batch — each batch carries its own pending increment until it lands, so
                // the barrier never reads zero with effects in flight.
                pending.fetch_sub(1, Ordering::SeqCst);
                drop(permit);
            });
        }
    });
}

struct EngineState {
    tables: HashMap<TableRef, TableSchema>,
    sequencer: Option<SequencerHandle>,
    shapes: HashMap<String, ShapeRecord>,
    next_shape_id: u64,
    /// Shape sharing. Any two **equal** shapes — same kind and definition (see `shape_signature` /
    /// `agg_signature`: table + canonical predicate + columns + changes-only, or table + predicate +
    /// func + column for aggregates) — share ONE durable stream + ONE routed/standalone/registry entry,
    /// ref-counted, so the engine maintains + appends once for all subscribers instead of once each. A
    /// joiner positions itself with its own snapshot LSN (client-side `< S` drop), so sharing is safe.
    /// Covers plain, subquery, and aggregate shapes. `feed_by_sig`: signature -> shape_id;
    /// `feed_shares`: shape_id -> (sig, refcount).
    feed_by_sig: HashMap<String, String>,
    /// Circuit-served placement per shape id (label like `all` / `static:project_id` /
    /// `dynamic:project_id` / `counts`), plus the arrangement column serving it — feeds the
    /// graph payload so the visualizer can draw pipeline→shape edges.
    circuit_placement: HashMap<String, CircuitPlacement>,
    feed_shares: HashMap<String, FeedShare>,
    /// Per-table **schema generation**, bumped in the same critical section that enumerates a
    /// table's dependents for retirement (`Engine::retire_dependents`). A shape create captures the
    /// generation of every table it touches when it registers and re-checks it before returning
    /// success, so a create that overlapped a drift is rolled back instead of being installed
    /// against a schema that is already gone (ADR-0005) — the same shape as the degradation latch's
    /// two-check pattern (see [`Engine::ensure_create_not_degraded`]).
    schema_gen: HashMap<TableRef, u64>,
    /// Tables whose drift could not be resolved (Postgres unreachable, the catalog read failed, the
    /// `REPLICA IDENTITY FULL` re-assert timed out, or the wire keeps disagreeing with the
    /// catalog). Creates on them are refused with a retryable error and the ingestor decodes
    /// nothing for them; a per-table retry task keeps trying. Never a silent stale-serve.
    unresolved: HashSet<TableRef>,
    /// **Epoch generation**, bumped in the same critical section in which an epoch reset enumerates
    /// the shapes it is about to retire (`Engine::reset_epoch`, ADR-0004). The per-table
    /// `schema_gen` closes the create-overtaken-by-a-drift race; this one closes its whole-engine
    /// twin — a create that registered before the reset's enumeration would otherwise be purged by
    /// it, or (worse) backfill at a snapshot taken before the NEW slot's consistent point and
    /// permanently miss the window between them.
    epoch_gen: u64,
    /// Which shape each live subscription belongs to: `subscription id -> shape id` (ADR-0008).
    ///
    /// The reverse of `feed_shares[*].subs`, maintained by the same three methods
    /// ([`Self::subscribe`], [`Self::unsubscribe`], [`Self::forget_subscriptions`]) so the two can
    /// only move together. It exists for one question a create must answer before it does anything:
    /// **is this subscription id already someone else's?** Re-using one id for a second predicate is
    /// refused (409) rather than silently accepted, because the caller would then hold one name for
    /// two shapes and be unable to release either without ambiguity.
    subs_by_id: HashMap<String, String>,
    /// Counter behind the ids the engine mints for creates that named no subscription. Combined
    /// with a per-process nonce (see [`Engine::mint_subscription`]), so a minted id is unique
    /// across restarts too — the catalog outlives the process that wrote it.
    next_minted_sub: u64,
}

/// The generations a create captured for everything that can invalidate it: the schema of each table
/// it depends on, and the engine's epoch.
///
/// A create reads several tables (a subquery shape reads its outer table AND every referenced inner
/// table), and any of them drifting invalidates the whole create — so the whole set travels
/// together and is re-checked as one. The epoch rides along for the same reason and is re-checked in
/// the same place: one closing check, whatever moved.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SchemaGens {
    tables: Vec<(TableRef, u64)>,
    /// `EngineState::epoch_gen` as it was when this create registered (ADR-0004).
    epoch: u64,
}

impl EngineState {
    /// Capture the current generation of each table, and of the epoch. Call under the state lock, in
    /// the same critical section that registers the create.
    fn capture_gens(&self, tables: &[TableRef]) -> SchemaGens {
        SchemaGens {
            tables: tables.iter().map(|t| (t.clone(), self.schema_gen.get(t).copied().unwrap_or(0))).collect(),
            epoch: self.epoch_gen,
        }
    }

    /// The first table whose generation moved since [`capture_gens`](Self::capture_gens), if any.
    fn drifted_since(&self, gens: &SchemaGens) -> Option<TableRef> {
        gens.tables.iter().find(|(t, g)| self.schema_gen.get(t).copied().unwrap_or(0) != *g).map(|(t, _)| t.clone())
    }

    /// Did an epoch reset run since [`capture_gens`](Self::capture_gens)?
    fn epoch_reset_since(&self, gens: &SchemaGens) -> bool {
        self.epoch_gen != gens.epoch
    }

    /// The first of `tables` whose schema is unresolved, if any.
    fn first_unresolved(&self, tables: &[TableRef]) -> Option<TableRef> {
        tables.iter().find(|t| self.unresolved.contains(*t)).cloned()
    }

    // --- subscriptions (ADR-0008) ---------------------------------------------------------------

    /// Which shape holds this subscription id right now, if any.
    fn subscription_owner(&self, sub: &str) -> Option<&String> {
        self.subs_by_id.get(sub)
    }

    /// Add (or RENEW) a subscription on a shape's share entry. Renewing is the same call: an id the
    /// shape already holds keeps its place in the set and only moves its lease forward.
    ///
    /// Returns whether this was a NEW claim — the caller uses it to decide between a durable
    /// `Joined` (a claim the client is about to be told about) and a queued one (a renewal, which
    /// promises nothing new).
    fn subscribe(&mut self, shape: &str, sub: String, at: u64) -> bool {
        let Some(share) = self.feed_shares.get_mut(shape) else { return false };
        let fresh = match share.subs.get_mut(&sub) {
            Some(lease) => {
                *lease = (*lease).max(at);
                false
            }
            None => {
                share.subs.insert(sub.clone(), at);
                true
            }
        };
        self.subs_by_id.insert(sub, shape.to_string());
        fresh
    }

    /// Release one subscription. Returns whether it was actually held — a release for an id the
    /// shape does not hold changes nothing and writes nothing, which is exactly what makes a
    /// client's retried `DELETE` safe.
    fn unsubscribe(&mut self, shape: &str, sub: &str) -> bool {
        let removed = self.feed_shares.get_mut(shape).is_some_and(|s| s.subs.remove(sub).is_some());
        if removed {
            // Only if it still points HERE: an id released from one shape and immediately claimed
            // on another must keep the newer owner.
            if self.subs_by_id.get(sub).is_some_and(|owner| owner == shape) {
                self.subs_by_id.remove(sub);
            }
        }
        removed
    }

    /// The legacy anonymous release (`DELETE /shapes/{id}` with no `subscription`): drop ONE
    /// subscription without being told which.
    ///
    /// Engine-minted ids go first, oldest lease first, and only then caller-named ones — a caller
    /// that never learned the protocol must not be able to steal the claim of one that did. It is
    /// still not retry-safe (nothing identifies which claim the caller meant), which is why the
    /// route documents it as legacy.
    ///
    /// (A shape created with `share = false` has no share entry at all, so it holds no tracked
    /// subscriptions and there is nothing here to release — as before ADR-0008. Nothing in the tree
    /// creates one.)
    fn unsubscribe_anonymous(&mut self, shape: &str) -> Option<String> {
        let victim = {
            let share = self.feed_shares.get(shape)?;
            let pick = |minted: bool| {
                share
                    .subs
                    .iter()
                    .filter(|(id, _)| id.starts_with(MINTED_SUB_PREFIX) == minted)
                    // Oldest lease first, and a tie broken by the minted id's COUNTER rather than
                    // its spelling: `~n-10` is younger than `~n-9`, which a string compare has
                    // backwards. Ties only arise within one wall-clock second, so this is about
                    // being predictable rather than about correctness.
                    .min_by_key(|(id, at)| (**at, minted_seq(id), (*id).clone()))
                    .map(|(id, _)| id.clone())
            };
            pick(true).or_else(|| pick(false))?
        };
        self.unsubscribe(shape, &victim).then_some(victim)
    }

    /// Forget every subscription a shape held — the removal half of a purge/eviction/rollback,
    /// where the share entry itself goes. Without it the id index would keep pointing at a shape
    /// that no longer exists and refuse a perfectly good re-subscription with a 409.
    fn forget_subscriptions(&mut self, shape: &str) {
        if let Some(share) = self.feed_shares.get(shape) {
            let ids: Vec<String> = share.subs.keys().cloned().collect();
            for id in ids {
                if self.subs_by_id.get(&id).is_some_and(|owner| owner == shape) {
                    self.subs_by_id.remove(&id);
                }
            }
        }
    }

    /// Publish the live-subscription gauge. A gauge and not a counter: it describes how many claims
    /// are pinning shapes right now, which is the number an operator watching a shape that will not
    /// go dormant needs.
    fn publish_subscription_gauge(&self) {
        let live: usize = self.feed_shares.values().map(FeedShare::refcount).sum();
        crate::metrics::metrics().subscriptions_live.store(live as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Every subscription whose lease has not been renewed within `window`, as
    /// `(shape id, subscription id)` (ADR-0008). Pure bookkeeping — the caller records the `Left`s
    /// and the retention sweep then treats the shape exactly as it would after an explicit release.
    ///
    /// `window == 0` disables leases entirely, together with dormancy: an engine that never parks
    /// a shape has no use for the liveness signal, and expiring subscriptions under it would only
    /// break sharing.
    ///
    /// The comparison is **strictly greater**, deliberately. The lease clock is wall-clock SECONDS,
    /// so an age of `n` means "somewhere in `[n, n+1)` of real time"; lapsing at `>=` would end a
    /// one-second window after as little as a few milliseconds, which is not the window the operator
    /// asked for. A client renewing at any fraction of its window is unaffected either way — this
    /// decides only the boundary case, and the boundary belongs to the client.
    fn lapsed_subscriptions(&self, window: std::time::Duration, now: u64) -> Vec<(String, String)> {
        if window.is_zero() {
            return Vec::new();
        }
        let secs = window.as_secs().max(1);
        let mut out = Vec::new();
        for (shape, share) in &self.feed_shares {
            for (sub, at) in &share.subs {
                if now.saturating_sub(*at) > secs {
                    out.push((shape.clone(), sub.clone()));
                }
            }
        }
        out.sort();
        out
    }
}

struct FeedShare {
    sig: String,
    /// The shape's **live subscriptions** (ADR-0008): `subscription id -> the wall-clock second its
    /// lease was last renewed at`. The refcount is this set's size, and it is a set precisely so
    /// that repeating a create or a release is one claim, not two.
    ///
    /// The lease clock is deliberately WALL CLOCK, not the `Instant` the rest of retention uses: a
    /// lease has to survive a restart, and only a wall-clock second can be written to the catalog
    /// and read back as an age. (`last_read` stays an `Instant` — it is process-local and must not
    /// move when the system clock does.)
    subs: std::collections::BTreeMap<String, u64>,
    /// Creation outcome, observed by joiners (see [`ShareOutcome`]).
    ready: tokio::sync::watch::Receiver<ShareOutcome>,
}

impl FeedShare {
    /// Live subscriptions — what the retention sweep calls the refcount.
    fn refcount(&self) -> usize {
        self.subs.len()
    }
}

/// The prefix of an **engine-minted** subscription id — a pure MARKER, not a namespace the engine
/// polices (`http::validate_new_subscription`).
///
/// A create that names no subscription still gets one (it is in the response, and the caller can
/// renew or release with it), but it is marked as un-named so the legacy anonymous
/// `DELETE /shapes/{id}` — which has no id to go on — releases one of THOSE first rather than
/// stealing an identified subscriber's claim.
///
/// A caller MAY name an id with this prefix; the engine does not check whether it minted it. All
/// such a caller achieves is making its own claim the expendable one — see
/// `http::validate_new_subscription`.
pub(crate) const MINTED_SUB_PREFIX: char = '~';

/// The counter out of an engine-minted id (`~<nonce>-<n>` -> `n`); `None` for anything else, which
/// sorts first. Used only to break a tie between two claims minted in the same second.
fn minted_seq(id: &str) -> Option<u64> {
    id.rsplit_once('-')?.1.parse().ok()
}

/// What a shared shape's creator publishes to the joiners waiting on it.
///
/// Typed rather than a bare `bool` because the REASON travels with it: a create refused because the
/// engine degraded must give every joiner the same typed [`Degraded`] refusal the creator returns
/// (503), not a generic initialization failure (500) — identical requests from identical clients
/// cannot be allowed to disagree about why the engine said no.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShareOutcome {
    /// The creator's backfill/registration is still in flight.
    Pending,
    /// The shape is live: its snapshot is readable.
    Ready,
    /// Creation failed (the entry is removed; joiners must error, not return a dead stream).
    Failed,
    /// The create overlapped a degradation and refused (see [`Engine::ensure_create_not_degraded`]).
    Degraded,
    /// The creator's closing re-check after its catalog durability wait found the shape retired
    /// underneath it (see [`Engine::recheck_after_durability`]). Distinct from [`Self::Failed`]
    /// because the creator does not give up on that: it redoes the create. A joiner must reach the
    /// same conclusion — it arrives typed [`CreateRaced`], so the joiner's own attempt is redone
    /// too. Reported as `Failed` it would answer 500 while the creator quietly succeeded on its
    /// next attempt, which is two identical requests disagreeing about the same outcome.
    Raced,
}

/// Wait until a shared shape's creator reports the shape live (or failed). Joining before the
/// backfill lands would hand the caller a stream whose snapshot isn't readable yet.
async fn await_share_ready(mut rx: tokio::sync::watch::Receiver<ShareOutcome>, id: &str) -> Result<()> {
    loop {
        let state = *rx.borrow();
        match state {
            ShareOutcome::Ready => return Ok(()),
            // The creator's own refusal, verbatim: the HTTP layer downcasts it to 503 exactly as
            // it does for the creator's error.
            ShareOutcome::Degraded => return Err(anyhow::Error::new(Degraded)),
            // Likewise typed, so this joiner's attempt is redone exactly as the creator's is.
            ShareOutcome::Raced => {
                return Err(anyhow::Error::new(CreateRaced(format!(
                    "shared shape '{id}' was retired during its creator's catalog durability wait"
                ))));
            }
            ShareOutcome::Failed => bail!("shared shape '{id}' failed to initialize; retry the create"),
            ShareOutcome::Pending => {
                if rx.changed().await.is_err() {
                    bail!("shared shape '{id}' creator died before completing; retry the create");
                }
            }
        }
    }
}

/// Canonical signature for feed sharing: table + serialized predicate + sorted projection indices.
/// Two subset feeds with an equal signature are interchangeable and share one stream.
/// Order-insensitive predicate canonicalization (same form used for subquery-node sharing), so
/// `a AND b` and `b AND a` collapse to one shape.
fn canon_where(where_: &Option<PredicateJson>) -> String {
    where_.as_ref().map(crate::predicate::canonical_pred).unwrap_or_default()
}

/// The coarse engine column type as a stable string for the schema endpoint's JSON.
fn col_type_str(ty: crate::schema::ColumnType) -> &'static str {
    use crate::schema::ColumnType::*;
    match ty {
        Int => "int",
        Text => "text",
        Bool => "bool",
        Float => "float",
    }
}

fn canon_cols(out_cols: &Option<Arc<Vec<usize>>>) -> String {
    out_cols
        .as_ref()
        .map(|v| {
            let mut idx = v.as_ref().clone();
            idx.sort_unstable();
            idx.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",")
        })
        .unwrap_or_default()
}

/// The sharing key for a **row shape** (materialized or changes-only feed, plain or subquery). Two
/// shapes are interchangeable — and so share one maintained stream — iff these all match. `changes_only`
/// is part of the key: a backfilled shape and a no-backfill feed over the same rows are NOT the same
/// stream.
fn shape_signature(
    table: &TableRef,
    where_: &Option<PredicateJson>,
    out_cols: &Option<Arc<Vec<usize>>>,
    changes_only: bool,
) -> String {
    format!("shape\u{1f}{}\u{1f}{table}\u{1f}{}\u{1f}{}", changes_only, canon_where(where_), canon_cols(out_cols))
}

/// The sharing key for an **aggregation shape**: table + predicate + function + column. Namespaced so it
/// never collides with a row shape's key.
fn agg_signature(table: &TableRef, where_: &Option<PredicateJson>, func: &AggFn, col_idx: Option<usize>) -> String {
    format!("agg\u{1f}{table}\u{1f}{}\u{1f}{:?}\u{1f}{:?}", canon_where(where_), func, col_idx)
}

/// Broadcast a graph-lifecycle event on the trace channel (zero cost with no subscribers).
fn trace_lifecycle(tx: &tokio::sync::broadcast::Sender<Arc<String>>, ev: crate::trace::GraphLifecycle) {
    if tx.receiver_count() == 0 {
        return;
    }
    if let Ok(json) = serde_json::to_string(&ev) {
        let _ = tx.send(Arc::new(json));
    }
}

/// The graph/trace node id of a family router: `family:<table>:<col,col>` (canonical `schema.name`
/// table + column NAMES, matching
/// the hop ids `process_envelope` emits and the ids the visualizer renders).
fn family_node_id(ts: &TableSchema, key_cols: &[usize]) -> String {
    let cols = key_cols
        .iter()
        .map(|i| ts.columns.get(*i).map(|(n, _)| n.clone()).unwrap_or_else(|| format!("col{i}")))
        .collect::<Vec<_>>()
        .join(",");
    format!("family:{}:{cols}", ts.table)
}

/// Shape id (`s<N>`) from its stream path (`shape/s<N>`) — the key `emitted` counters are kept by.
fn sid_of_path(stream_path: &str) -> &str {
    stream_path.strip_prefix("shape/").unwrap_or(stream_path)
}

impl Engine {
    pub fn new(ds: DsClient) -> Self {
        Self::new_inner(ds, None)
    }

    /// Engine in Postgres mode: data lives in Postgres, ingested via logical replication and read
    /// back for backfill. Call [`setup_postgres`](Self::setup_postgres) before serving.
    pub fn new_pg(ds: DsClient, pg_url: String) -> Self {
        let e = Self::new_inner(ds, Some(pg_url));
        // Postgres mode starts `waiting` until the connection + introspection + slot + ingest are up.
        e.health.store(HEALTH_WAITING, std::sync::atomic::Ordering::Relaxed);
        e
    }

    fn new_inner(ds: DsClient, pg_url: Option<String>) -> Self {
        let subqueries = Arc::new(Mutex::new(SubqueryRegistry::new(ds.clone(), pg_url.clone())));
        let trace_tx = tokio::sync::broadcast::channel(crate::trace::CHANNEL_CAP).0;
        let (flip_tx, flip_rx) = mpsc::unbounded_channel();
        let pending_flips = Arc::new(std::sync::atomic::AtomicI64::new(0));
        // Ordered emission lanes for subquery-shape appends (network out from under the
        // registry lock; per-stream FIFO keeps append order = eval order). They share the
        // pendingFlips counter so the convergence barrier covers queued batches.
        let lanes = emission::EmissionLanes::spawn(
            ds.clone(),
            std::env::var("ELECTRIC_CIRCUITS_EMIT_LANES").ok().and_then(|v| v.parse().ok()).unwrap_or(8),
            pending_flips.clone(),
        );
        subqueries.try_lock().expect("fresh registry").set_lanes(lanes);
        let degrade = DegradeState::new();
        spawn_flip_propagator(subqueries.clone(), flip_rx, pending_flips.clone(), degrade.clone(), trace_tx.clone());
        // Created before the writer so the writer can register a shutdown party while it is
        // retrying an append (see `spawn_catalog_writer`).
        let shutdown = crate::shutdown::ShutdownToken::new();
        let catalog_tx = spawn_catalog_writer(ds.clone(), shutdown.clone());
        let retirements = spawn_retirement_queue(ds.clone(), catalog_tx.clone(), shutdown.clone());
        // The change log's writer records every rotation in the durable catalog, so a restart knows
        // which segment is current (and when each one began, for the retain window).
        let changes = ChangeLogWriter::new(
            ds.clone(),
            Arc::new(ChangesState::default()),
            crate::changelog::ChangeLogConfig::from_env(),
            {
                let catalog_tx = catalog_tx.clone();
                Arc::new(move |segment, at| catalog_tx.send(CatalogEvent::ChangesRotated { segment, at }))
            },
        );
        let engine = Engine {
            ds,
            state: Arc::new(Mutex::new(EngineState {
                tables: HashMap::new(),
                sequencer: None,
                shapes: HashMap::new(),
                next_shape_id: 1,
                feed_by_sig: HashMap::new(),
                feed_shares: HashMap::new(),
                circuit_placement: HashMap::new(),
                schema_gen: HashMap::new(),
                unresolved: HashSet::new(),
                epoch_gen: 0,
                subs_by_id: HashMap::new(),
                next_minted_sub: 1,
            })),
            pg_url,
            repl_lsn: Arc::new(std::sync::Mutex::new("0/0".to_string())),
            repl_sync: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            replicator_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            // Library mode: no Postgres to wait on, so report `active` immediately.
            health: Arc::new(std::sync::atomic::AtomicU8::new(HEALTH_ACTIVE)),
            subqueries,
            trace_tx,
            flip_tx,
            pending_flips,
            degrade,
            tables_shared: Arc::new(std::sync::RwLock::new(HashMap::new())),
            catalog_tx,
            retirements,
            purge_barriers: Arc::new(std::sync::Mutex::new(HashMap::new())),
            #[cfg(test)]
            purge_test_hook: Arc::new(PurgeTestHook::default()),
            changes,
            seq_start: Arc::new(std::sync::Mutex::new(LogPosition::start())),
            seq_highwater: Arc::new(std::sync::Mutex::new(None)),
            lives: Arc::new(std::sync::Mutex::new(HashMap::new())),
            retention: Arc::new(RetentionConfig::from_env()),
            retention_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            reconciler_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            resolving: Arc::new(drift::Resolving::default()),
            retrying: Arc::new(std::sync::Mutex::new(HashSet::new())),
            dbsp_cfg: Arc::new(std::sync::Mutex::new(None)),
            txn_cfg: Arc::new(std::sync::Mutex::new(crate::txn_buffer::TxnBufferConfig::default())),
            arrangements: Arc::new(std::sync::Mutex::new(None)),
            arr_gates: Arc::new(std::sync::RwLock::new(HashMap::new())),
            epoch: EpochState::new(),
            change_log_failure: Arc::new(std::sync::Mutex::new(None)),
            shutdown,
            sub_nonce: crate::engine::catalog::process_nonce().into(),
        };
        // The streams client must be able to ask the engine whether a terminal append answer is
        // real before a live shape's batch is discarded (see `Engine::install_gone_reconciler`).
        engine.install_gone_reconciler();
        engine
    }

    /// The process's shutdown token — the binary flips it on `SIGTERM`/`SIGINT` and every
    /// long-running part of the engine joins it (see [`crate::shutdown`]).
    pub fn shutdown_token(&self) -> crate::shutdown::ShutdownToken {
        self.shutdown.clone()
    }

    /// Wait for every durable-catalog event sent so far to be appended (or `timeout`). The last
    /// step of a graceful shutdown: the sequencer's final `Offset` is *queued* when it stops, and
    /// only a drained queue makes it the position the next boot resumes from.
    pub async fn drain_catalog(&self, timeout: std::time::Duration) -> bool {
        self.catalog_tx.drain(timeout).await
    }

    /// Configure the always-on dbsp arrangement layer (call before
    /// [`setup_postgres`](Self::setup_postgres), which builds and seeds it).
    pub fn set_dbsp_config(&self, cfg: crate::config::DbspConfig) {
        *self.dbsp_cfg.lock().unwrap() = Some(cfg);
    }

    /// Configure large-transaction handling on the ingest path — the per-transaction memory cap,
    /// the spill directory and the append byte budget (ADR-0003). Call before
    /// [`setup_postgres`](Self::setup_postgres), which spawns the ingestor.
    pub fn set_txn_config(&self, cfg: crate::txn_buffer::TxnBufferConfig) {
        *self.txn_cfg.lock().unwrap() = cfg;
    }

    /// Start the dbsp counts layer and seed it, when configured. Seeds each counts pipeline
    /// from one group-aggregated Postgres snapshot per table (capturing the gate that fences the live
    /// feed); restored state skips seeding — the sequencer replays the change-log gap instead.
    async fn maybe_start_arrangements(&self, schemas: &HashMap<TableRef, TableSchema>) -> Result<()> {
        let Some(cfg) = self.dbsp_cfg.lock().unwrap().clone() else { return Ok(()) };
        if !cfg.indexes.is_empty() {
            tracing::warn!(
                "ELECTRIC_CIRCUITS_DBSP_INDEXES is deprecated and ignored: row data lives in Postgres \
                 (lookups are pooled queries); the circuit holds counts pipelines only"
            );
        }
        if cfg.cache_mib.is_some() || cfg.max_rss_bytes.is_some() {
            tracing::warn!(
                "ELECTRIC_CIRCUITS_DBSP_{{CACHE_MIB,MIN_STORAGE_KB,MAX_RSS_MB,CHECKPOINT_SECS,DIR}} are \
                 deprecated no-ops: the circuit is in-memory counts only (no storage layer)"
            );
        }
        if std::env::var("ELECTRIC_CIRCUITS_FEED_TRACE").is_ok() {
            tracing::warn!(
                "ELECTRIC_CIRCUITS_FEED_TRACE is deprecated and ignored: the feed relation now lives host-side"
            );
        }
        let mut counts: Vec<crate::arrangements::CountSpec> = Vec::new();
        for (t, cols) in &cfg.counts {
            let Some(ts) = schemas.get(t) else {
                tracing::warn!("ELECTRIC_CIRCUITS_DBSP_COUNTS: unknown table {t}; skipping");
                continue;
            };
            let resolved: Option<Vec<usize>> = cols.iter().map(|c| ts.index.get(c).copied()).collect();
            match resolved {
                Some(group_cols) => counts.push(crate::arrangements::CountSpec { table: t.clone(), group_cols }),
                None => tracing::warn!("ELECTRIC_CIRCUITS_DBSP_COUNTS: unknown column in {t}:{cols:?}; skipping"),
            }
        }
        if counts.is_empty() {
            return Ok(()); // nothing for the circuit to maintain
        }
        let arr = crate::arrangements::Arrangements::start(counts.clone())?;
        // Seed each counts pipeline from ONE group-aggregated query per table — O(groups),
        // not O(rows); row data stays in Postgres. State is in-memory only, so this runs on
        // every boot; the seed's SnapshotGate fences change-log replay exactly like a shape
        // backfill.
        let url = self.pg_url.clone().context("counts pipelines need a pg_url to seed")?;
        let client = crate::pg::pool_for(&url).get().await?;
        let mut gates = HashMap::new();
        for spec in &counts {
            let ts = schemas.get(&spec.table).expect("resolved above");
            let (groups, gate) = crate::pg::backfill_group_counts(&client, ts, &spec.group_cols).await?;
            let total = groups.len();
            arr.seed_groups(&spec.table, groups).await?;
            gates.insert(spec.table.clone(), gate);
            arr.finish_seed(&spec.table);
            tracing::info!("arrangements: seeded counts for '{}' ({total} groups)", spec.table);
        }
        *self.arr_gates.write().unwrap() = gates;
        *self.arrangements.lock().unwrap() = Some(arr);
        Ok(())
    }

    /// Sender for the per-envelope trace broadcast — subscribe via `.subscribe()` (used by the
    /// `/trace` SSE endpoint); tailers publish through a clone.
    pub fn trace_sender(&self) -> tokio::sync::broadcast::Sender<Arc<String>> {
        self.trace_tx.clone()
    }

    /// Flip batches enqueued but not yet propagated (convergence-barrier term; see `flip_tx`).
    pub fn pending_flips(&self) -> i64 {
        self.pending_flips.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Flip batches abandoned after exhausting their retries — membership effects the engine
    /// computed and could not deliver. Reported as `flipFailures`; non-zero means degraded.
    pub fn flip_failures(&self) -> u64 {
        self.degrade.failures.load(Ordering::SeqCst)
    }

    /// True once membership effects have been lost (see [`DegradeState`]). Latched until restart.
    pub fn degraded(&self) -> bool {
        self.degrade.degraded.load(Ordering::SeqCst)
    }

    /// The guard every membership-bearing create/read path takes: once degraded, refuse rather than
    /// answer with membership the engine knows is wrong.
    ///
    /// Two independent degradations reach it, each with its own typed error (both 503): lost
    /// membership effects ([`Degraded`], latched until restart) and a broken epoch under the refuse
    /// policy ([`EpochBroken`], cleared by `POST /epoch/reset`). The epoch is checked first — it is
    /// the more fundamental "this engine is not serving this database right now".
    pub fn ensure_not_degraded(&self) -> Result<()> {
        // A reset in flight is checked FIRST: it is the transient, actionable one, and while it runs
        // the epoch is also (correctly) latched broken — "retry in a moment" is the useful answer.
        // Taken under the engine-state lock by every create, so a create either registered before
        // the reset's enumeration (and is retired by it, then rolled back by its own closing check)
        // or is refused here — never installed against a slot that is being replaced.
        if self.epoch_resetting() {
            return Err(anyhow::Error::new(EpochResetting));
        }
        if let Some(reason) = self.epoch_broken() {
            return Err(anyhow::Error::new(EpochBroken { reason }));
        }
        if self.degraded() {
            return Err(anyhow::Error::new(Degraded));
        }
        Ok(())
    }

    /// The boot gate (ADR-0009): in Postgres mode, refuse every client shape mutation — create, join,
    /// reactivation, release, purge — until `setup_postgres` has finished. Library mode has no boot
    /// to wait for and is always past it. Creates, joins and reactivations take it inside the engine
    /// (`create_shape_as`, `create_aggregate_as`, `ensure_active`); release and purge take it at their
    /// HTTP route, because the engine's own purges — an epoch reset during boot above all — must run
    /// before the boot resolves.
    ///
    /// The catalog restore depends on it. Before the restore runs, `next_shape_id` has not been
    /// moved past the catalog's ids, so a create could mint a stream a restored record still owns.
    /// While it runs, a create would spawn an ordinary, reading sequencer and a retention sweeper —
    /// and after a rolled-back attempt, with no sequencer at all, that sequencer would read the
    /// backlog with no shape registered and checkpoint past it, and the sweeper would advance the
    /// durable checkpoint on its own: either way the retried restore would resume its shapes after
    /// changes they never saw. With every such entry point closed — and `define_schema`, the other
    /// way to spawn a sequencer or replace the tables, refused outright in Postgres mode
    /// ([`SchemaIsPostgres`]) — the boot is the only thing that touches the registry, the tables or
    /// the sequencer until it resolves.
    ///
    /// Checked against the raw boot phase, not [`Self::health_status`]: a broken epoch reports
    /// `degraded` but has booted, and its refusals are [`Self::ensure_not_degraded`]'s.
    /// `setup_postgres` is retried only after a failure and refuses a second call after a success,
    /// so once the phase is `HEALTH_ACTIVE` nothing moves it back, and a request that passed the gate
    /// stays past it.
    pub fn ensure_booted(&self) -> Result<()> {
        if self.pg_url.is_some() && self.health.load(std::sync::atomic::Ordering::Relaxed) != HEALTH_ACTIVE {
            return Err(anyhow::Error::new(Booting));
        }
        Ok(())
    }

    /// Latch the engine degraded. Exposed for tests that drive the refusal surface without a lost
    /// flip to cause it; the engine itself degrades only from the propagator's abandonment path.
    #[doc(hidden)]
    pub fn force_degraded(&self) {
        self.degrade.mark();
    }

    /// Start the stream reaper, once, for an engine that now has subquery shapes to reap. Lazy for
    /// the same reason the retention sweeper is: an engine that never serves a subquery can never
    /// lose a flip, so it never needs the task.
    pub(crate) fn ensure_degrade_reaper(&self) {
        if self.degrade.reaper_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let engine = self.clone();
        let mut wake = self.degrade.wake.subscribe();
        tokio::spawn(async move {
            while !engine.degraded() {
                if wake.changed().await.is_err() {
                    return; // engine gone
                }
            }
            engine.reap_subquery_streams().await;
        });
    }

    /// Delete every registered subquery shape's durable stream. Clients read durable-streams
    /// directly, past the HTTP surface that now answers 503, so a deleted stream is the only way
    /// they learn their shape is no longer maintained. The shape RECORDS stay registered (the
    /// engine answers 503, not 404). This destroys nothing a restart would have kept: a restart
    /// re-seeds every node from Postgres but DROPS every subquery shape — their inner-node state
    /// is not persisted, so the catalog restore deliberately does not restore them (see
    /// `Engine::apply_catalog`) — and clients recreate them with `POST /shapes`.
    async fn reap_subquery_streams(&self) {
        let paths: Vec<String> = {
            let st = self.state.lock().await;
            st.shapes.values().filter(|r| r.is_subquery).map(|r| r.stream_path.clone()).collect()
        };
        tracing::error!(
            "degraded: deleting {} subquery shape stream(s); clients must recreate against a restarted engine",
            paths.len()
        );
        for path in paths {
            // Retirement (close, then delete), retried until storage accepts the delete
            // (`delete_stream` counts a 404 as done): a stream left behind keeps serving rows the
            // engine can no longer maintain, and the close releases subscribers tailing it now.
            let mut attempt = 0u32;
            while let Err(e) = self.ds.retire_stream(&path).await {
                attempt += 1;
                let backoff = std::time::Duration::from_millis(100u64.saturating_mul(1 << attempt.min(5)).min(2000));
                tracing::warn!(
                    "degraded: deleting stream {path} failed (attempt {attempt}), retrying in {backoff:?}: {e:#}"
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }

    fn subquery_handle(&self) -> SubqueryHandle {
        SubqueryHandle {
            registry: self.subqueries.clone(),
            flip_tx: self.flip_tx.clone(),
            pending_flips: self.pending_flips.clone(),
        }
    }

    /// Get (or spawn) the single sequencer task consuming the global change log.
    fn ensure_sequencer<'a>(&self, st: &'a mut EngineState) -> &'a SequencerHandle {
        if st.sequencer.is_none() {
            st.sequencer = Some(self.spawn_sequencer_task(false));
        }
        st.sequencer.as_ref().expect("sequencer just spawned")
    }

    /// Spawn a sequencer from the restored start position and the current arrangement layer.
    /// `hold_reads` is the catalog restore's: register every shape before a single change is read
    /// (see `SequencerCmd::ReleaseReads`).
    fn spawn_sequencer_task(&self, hold_reads: bool) -> SequencerHandle {
        let start = self.seq_start.lock().unwrap().clone();
        let highwater = *self.seq_highwater.lock().unwrap();
        spawn_sequencer(
            self.ds.clone(),
            self.tables_shared.clone(),
            start,
            highwater,
            self.catalog_tx.clone(),
            self.subquery_handle(),
            self.trace_tx.clone(),
            self.arrangements.lock().unwrap().clone(),
            self.arr_gates.read().unwrap().clone(),
            self.pg_url.is_none(),
            hold_reads,
            // Two std-locked handles, not an `Engine`: see `FailClosed`.
            FailClosed { failure: self.change_log_failure.clone(), epoch: self.epoch.clone() },
            self.shutdown.clone(),
        )
    }

    /// The change-log envelope the sequencer parked on, as JSON (ADR-0010), or `None` while the engine
    /// is processing changes normally. Served on `GET /metrics` and `GET /replication/lsn`.
    pub fn change_log_failure_json(&self) -> Option<serde_json::Value> {
        let f = self.change_log_failure.lock().unwrap().clone()?;
        Some(serde_json::json!({
            "position": { "segment": f.position.segment, "path": f.position.path(), "offset": f.position.offset },
            "table": f.table,
            "key": f.key,
            "txid": f.txid,
            "lsn": f.lsn,
            "envelopeOffset": f.envelope_offset,
            "error": f.error,
            // Spelled out rather than left implicit: this is the one state whose recovery is a
            // deliberate operator act (the break is never auto-reset).
            "recovery": "POST /epoch/reset",
        }))
    }

    /// Number of tables with a known schema (tables being tailed) — for the boot `consumers_ready` metric.
    pub async fn table_count(&self) -> usize {
        self.state.lock().await.tables.len()
    }

    /// Every table the engine tracks, canonically sorted (`GET /tables`).
    pub async fn tracked_tables(&self) -> Vec<TableRef> {
        let mut v: Vec<TableRef> = self.state.lock().await.tables.keys().cloned().collect();
        v.sort();
        v
    }

    /// The `/v1/health` status string: `degraded` | `waiting` | `starting` | `active` (exact, no
    /// whitespace). `degraded` outranks every boot phase — an engine that has lost membership
    /// effects, or whose epoch broke under the refuse policy (ADR-0004), is not healthy however far
    /// along its boot got. The two are one status word on purpose: the fleet healthcheck
    /// string-compares the body, and `GET /replication/lsn` is where the *reason* lives
    /// (`flipFailures` vs `epoch.reason`).
    pub fn health_status(&self) -> &'static str {
        if self.degraded() || self.epoch_broken().is_some() {
            return "degraded";
        }
        match self.health.load(std::sync::atomic::Ordering::Relaxed) {
            HEALTH_WAITING => "waiting",
            HEALTH_STARTING => "starting",
            _ => "active",
        }
    }

    /// Put the boot phase back to `waiting` — "not connected to Postgres, and about to try again".
    ///
    /// The binary's connect loop calls this after a retryable failure, because a failed attempt
    /// leaves the phase wherever it got to (`starting`, once the connection was open) and a whole
    /// backoff reported as `starting` tells an orchestrator the engine is making progress when it
    /// is in fact waiting to redial.
    pub fn set_waiting(&self) {
        self.health.store(HEALTH_WAITING, std::sync::atomic::Ordering::Relaxed);
    }

    /// The `GET /ready` status word — the **readiness** probe, as distinct from the liveness one
    /// (`GET /health`, which is "the process is running" and nothing more).
    ///
    /// `active` (200) means every precondition for serving is met: Postgres connected, the slot
    /// verified against the epoch binding, the durable catalog restored, the ingestor spawned, no
    /// degradation, no broken epoch. Anything else is 503 with the word that says why —
    /// `waiting` (no Postgres yet), `starting` (introspecting / restoring), `degraded`,
    /// or `shutting_down`.
    ///
    /// Shutdown outranks everything: the instant a `SIGTERM` lands this reports `shutting_down`, so
    /// a load balancer stops routing to the pod BEFORE the engine starts winding anything down.
    pub fn readiness_status(&self) -> &'static str {
        if self.shutdown.is_shutting_down() {
            return "shutting_down";
        }
        self.health_status()
    }

    /// Introspect the configured tables from Postgres, set `REPLICA IDENTITY FULL`, create the
    /// replication slot, register the schema, restore the durable catalog and start the replication
    /// ingestor.
    ///
    /// Retryable after a FAILURE — the binary's connect loop calls it again, and a failed attempt
    /// leaves nothing installed (the restore undoes itself, ADR-0009). Not callable again after a
    /// SUCCESS: that would put the boot phase back to `waiting` (closing the boot gate on a serving
    /// engine), restore the catalog over the shapes it already restored, and fight the running
    /// ingestor for the slot. So a second call after success is refused before it touches anything.
    pub async fn setup_postgres(&self, selectors: &[TableSelector], slot: &str) -> Result<()> {
        if self.replicator_started.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("setup_postgres has already completed on this engine; it runs once to success");
        }
        let url = self.pg_url.clone().context("setup_postgres called without a pg_url")?;
        // A retryable failure below brings the boot back here (see the binary's connect loop), so
        // the phase is reset rather than left wherever the last attempt stopped: `/ready` must say
        // `waiting` while the engine is trying to reach Postgres, not `starting`.
        self.health.store(HEALTH_WAITING, std::sync::atomic::Ordering::Relaxed);
        let client = crate::pg::connect(&url).await?;
        // `wal_level` is checked explicitly, first, rather than left to surface as a slot-creation
        // failure: it needs a Postgres RESTART to change, so it deserves its own named refusal.
        crate::pg::check_wal_level(&client).await?;
        // Postgres connection established: leave `waiting`, enter `starting` (introspection + slot +
        // ingest spawn still ahead). `/v1/health` reports 202 until the ingest loop is running.
        self.health.store(HEALTH_STARTING, std::sync::atomic::Ordering::Relaxed);
        // An empty setting means `*`, i.e. `public.*`: every table with a PK in `public` — NOT every
        // schema (introspect-all sets REPLICA IDENTITY FULL, which is not ours to do to managed
        // system schemas). `schema.*` opts another schema in explicitly.
        let default_all = [TableSelector::AllIn(crate::table_ref::PUBLIC_SCHEMA.to_string())];
        // Was the wildcard ASKED for, or is it the default? An explicit `schema.*` that finds
        // nothing is a misconfiguration (wrong schema name, or the migrations never ran) and aborts
        // the boot; the default `public.*` finding nothing is just an empty database, which is a
        // legitimate cold start.
        let explicit = !selectors.is_empty();
        let selectors: &[TableSelector] = if explicit { selectors } else { &default_all };
        // Resolve the selectors to a de-duplicated, ordered table set (a table named twice — say
        // `other.*` plus `other.items` — is introspected once).
        let mut tables: Vec<TableRef> = Vec::new();
        for sel in selectors {
            match sel {
                TableSelector::AllIn(schema) => {
                    let discovered = crate::pg::list_tables(&client, schema).await?;
                    if explicit && discovered.is_empty() {
                        bail!(
                            "ELECTRIC_CIRCUITS_PG_TABLES selects '{schema}.*', but schema '{schema}' \
                             has no base tables with a primary key"
                        );
                    }
                    tracing::info!("introspect-all '{schema}.*': {} table(s)", discovered.len());
                    tables.extend(discovered);
                }
                TableSelector::One(t) => tables.push(t.clone()),
            }
        }
        tables.sort();
        tables.dedup();
        // The publication is inspected BEFORE the first introspection, because it decides what a
        // fingerprint even contains: pgoutput publishes stored generated columns only when
        // `pubgencols = 's'`, and a column list means the wire can never deliver the row the
        // catalog describes — a boot-fatal misconfiguration rather than runtime drift (ADR-0005).
        let publication = format!("{slot}_pub");
        crate::pg::ensure_publication(&client, &publication).await?;
        let pubinfo = crate::pg::inspect_publication(&client, &publication, &tables).await?;
        crate::pg::set_publish_generated(pubinfo.publish_generated);
        if pubinfo.publish_generated {
            tracing::info!("publication '{publication}' publishes stored generated columns");
        }
        let mut compiled = HashMap::new();
        for t in &tables {
            // Identity FIRST, then introspect: the compiled schema carries a fingerprint that
            // includes `relreplident`, and reading it before the ALTER would record the pre-boot
            // identity — which the ingestor's first `Relation` message would then report as drift
            // on every single boot (ADR-0005).
            crate::pg::ensure_replica_identity_full(&client, t).await?;
            let def = crate::pg::introspect(&client, t).await?;
            let ts = TableSchema::from_def(t, &def)?;
            compiled.insert(t.clone(), ts);
        }
        *self.tables_shared.write().unwrap() = compiled.clone();
        self.state.lock().await.tables = compiled.clone();
        self.subqueries.lock().await.set_schemas(Arc::new(compiled.clone()));

        // --- The epoch (ADR-0004) ---
        //
        // Read the durable catalog and DECIDE before restoring anything: the epoch the catalog's
        // shapes belong to is recorded in that same log, and no shape may be resumed until the slot
        // it depends on has been vouched for. A slot the engine cannot vouch for is not recreated
        // quietly — every shape over it is missing an unknown span of WAL.
        self.set_epoch_slot(slot);
        // A catalog the engine could not READ is not a catalog with no epoch in it. Booting past an
        // unreadable one would take the `FirstBoot` branch — create a slot at the current WAL head,
        // append a `SlotBound` on top of whatever is already in the log — and the next boot, with
        // storage healthy again, would Resume-restore shapes that were never `Dropped` straight over
        // the gap. So it is fatal, exactly like a catalog written before ADR-0002: nothing may claim
        // an epoch unless the log was read and demonstrably contained none.
        let fold = self.fold_catalog().await.map_err(|e| {
            if e.downcast_ref::<catalog::CatalogPredatesQualification>().is_some() {
                return e;
            }
            e.context(
                "durable catalog unreadable; refusing to decide the epoch (an unreadable catalog is \
                 not an empty one — booting on would create a slot at the current WAL head and \
                 silently orphan every shape already in the log). Fix durable-streams and restart.",
            )
        })?;
        self.adopt_epoch_binding(fold.binding.clone());
        // The change log is segmented (ADR-0006), and which segment is CURRENT is folded out of the
        // same catalog — so this necessarily comes after the read, not before it as the old
        // unqualified `ensure_stream("changes")` did.
        self.init_change_log(fold.current_segment, fold.segment_starts.clone(), &fold.start_pos()).await?;
        let restored = match self.verify_epoch_at_boot(&client, slot).await? {
            // Either the epoch is intact or this boot just started one. Restore as usual.
            epoch::Verdict::FirstBoot | epoch::Verdict::Ok { .. } | epoch::Verdict::Busy { .. } => Some(fold),
            epoch::Verdict::Break(reason) => {
                // Park the records (see `RestoreMode::Park`): nothing is resumed, so no old-epoch
                // shape is ever maintained, and the reset — now, or whenever the operator asks —
                // still retires each one properly instead of orphaning its stream. Parking BEFORE
                // the policy runs is what gives the auto reset something to retire.
                self.apply_catalog(fold, &compiled, catalog::RestoreMode::Park).await?;
                // The same latch/count/act path the ingestor's pre-connect check uses. A refusal is
                // the expected outcome under the refuse policy, and boot continues (every route
                // answers 503); a failed auto-reset leaves the break latched and the ingestor
                // retries it.
                if let Err(refused) = self.on_epoch_break(reason, slot).await {
                    tracing::warn!("boot: ingest will not start — {refused}");
                }
                None
            }
        };
        // Start (and seed) the dbsp arrangement layer BEFORE the catalog restore: the restore spawns
        // the sequencer (which captures the handle + seed gates) and may re-register circuit-served
        // shapes, both of which need the layer up. It is also after the epoch step, so a reset's new
        // slot exists before the seed snapshot is taken — otherwise the circuit would miss the
        // changes between the two. A failure here degrades to Postgres query-backs (the engine still
        // runs), it does not abort boot.
        if let Err(e) = self.maybe_start_arrangements(&compiled).await {
            tracing::error!("dbsp arrangements failed to start (falling back to Postgres): {e:#}");
        }
        // Replay the durable shape catalog (restores shapes + the change-log replay offset), then
        // start the sequencer from the restored position. Runs before the ingestor so the restored
        // routing sees every replayed change.
        //
        // A restore that cannot complete fails the boot (ADR-0009). Serving on would be serving an
        // empty or partial registry over a catalog that still names every shape: their subscribers
        // would read streams nothing maintains, and the next checkpoint would move the replay start
        // past changes those shapes never saw. `apply_catalog` has already undone what it
        // installed, so the binary's loop either retries this whole setup (storage or Postgres
        // having a moment) or refuses by name (`pg::boot_disposition`).
        if let Some(fold) = restored {
            self.apply_catalog(fold, &compiled, catalog::RestoreMode::Resume)
                .await
                .context("restoring the durable shape catalog")?;
        }
        {
            let mut st = self.state.lock().await;
            self.ensure_sequencer(&mut st);
        }
        // The boot's last fallible step is behind it: from here it succeeds, and a later call is
        // refused up front (see the doc comment).
        self.replicator_started.store(true, std::sync::atomic::Ordering::SeqCst);
        // The ingestor reads the LIVE schema view (not a boot-time copy) and reports schema drift
        // / TRUNCATE back through `SchemaEvents`, which `Engine` implements (see `engine::drift`).
        // It is spawned even when the epoch is broken and the policy is refuse: `EpochEvents`
        // refuses every connection until `POST /epoch/reset`, so it parks in its backoff loop and
        // starts streaming the moment the operator acts — no ingest happens in the meantime.
        // Registered BEFORE the spawn so the shutdown wait can never observe "no parties" in the
        // window between deciding to start the ingestor and the task actually running.
        let party = self.shutdown.party("replication ingestor");
        tokio::spawn(crate::replication::run(
            url,
            slot.to_string(),
            publication,
            self.changes.clone(),
            self.tables_shared.clone(),
            Arc::new(self.clone()) as Arc<dyn crate::replication::SchemaEvents>,
            Arc::new(self.clone()) as Arc<dyn crate::replication::EpochEvents>,
            self.repl_lsn.clone(),
            self.repl_sync.clone(),
            self.txn_cfg.lock().unwrap().clone(),
            self.shutdown.clone(),
            party,
        ));
        // DDL with no following DML produces no `Relation` message; the reconciler catches it.
        self.ensure_schema_reconciler();
        // The retention sweeper is lazy elsewhere (a library user that never creates a shape never
        // needs it), but a Postgres-mode engine always has a change log growing under it: the same
        // sweep is what deletes the segments nothing can resume inside (ADR-0006), so it must run
        // whether or not anyone has ever created a shape.
        self.ensure_retention_sweeper();
        // Introspection + slot + ingest loop are up: report `active` (200 on `/v1/health`).
        self.health.store(HEALTH_ACTIVE, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Adopt the change log's segmentation state at boot and make the CURRENT segment exist
    /// (ADR-0006).
    ///
    /// `from_fold` is the catalog's answer, which is a **lower bound**: a process can die between
    /// closing a segment and recording its `ChangesRotated`, so the boot walks forward over closed
    /// segments (`changelog::resolve_current`) and writes the rotation records the crashed
    /// predecessor missed. On a genuine first boot nothing is in the log at all, and segment 0's
    /// creation is recorded as `ChangesRotated { segment: 0, at }` so the age criterion has a start
    /// time to measure from.
    ///
    /// It is also where the **sequencer's** start position is vouched for. A segment is deleted only
    /// once the durable checkpoint is past it, so a checkpoint naming a segment storage no longer
    /// has is an impossible state — and one the sequencer cannot recover from, since it would spin
    /// on a 404 forever. It is fatal here, at boot, with a message that names the segment, rather
    /// than a retry loop inside a spawned task nobody is watching.
    async fn init_change_log(
        &self,
        from_fold: u32,
        starts: std::collections::BTreeMap<u32, u64>,
        seq_start: &LogPosition,
    ) -> Result<()> {
        // "Recorded" tells a first boot (nothing was ever written) from a segment the engine knows
        // existed having been deleted — see `resolve_current`.
        let recorded = starts.contains_key(&from_fold);
        let resolved = crate::changelog::resolve_current(&self.ds, from_fold, recorded)
            .await
            .context("resolving the change log's current segment")?;
        let mut starts = starts;
        self.ds
            .ensure_stream(&segment_path(resolved))
            .await
            .with_context(|| format!("creating {}", segment_path(resolved)))?;
        // Every segment from the fold's answer up to the resolved one began without its record
        // being written (or, for segment 0 on a first boot, has just been created here).
        for n in from_fold..=resolved {
            if starts.contains_key(&n) {
                continue;
            }
            let at = crate::changelog::now_secs();
            starts.insert(n, at);
            self.catalog_tx.send(CatalogEvent::ChangesRotated { segment: n, at });
        }
        // The sequencer resumes here; if that segment is gone it can never advance (see above).
        let start_head =
            self.ds.head(&segment_path(seq_start.segment)).await.with_context(|| {
                format!("checking the sequencer's start segment {}", segment_path(seq_start.segment))
            })?;
        if start_head.is_none() {
            bail!(
                "durable catalog resumes the change log at {seq_start}, but {} does not exist. A segment is \
                 deleted only once the durable checkpoint has passed it, so this cannot happen while the \
                 engine is the only writer — refusing to boot rather than spin on a deleted stream. Reset \
                 the durable-streams data directory.",
                segment_path(seq_start.segment)
            );
        }
        // Seed the writer's tail from storage, so the size budget and `force_rotate` are right from
        // the first commit after a restart instead of from the first append this process makes
        // (which would read the segment as empty and never rotate on size).
        let tail = if seq_start.segment == resolved {
            start_head.and_then(|h| h.next_offset)
        } else {
            self.ds.head(&segment_path(resolved)).await.ok().flatten().and_then(|h| h.next_offset)
        };
        self.changes.state().adopt(resolved, starts, tail);
        crate::metrics::metrics().changes_segments_retained.store(self.changes.state().retained(), Ordering::Relaxed);
        tracing::info!("change log: current segment is {}", segment_path(resolved));
        Ok(())
    }

    /// Last commit LSN appended by the replication ingestor (text form, e.g. "0/1A2B3C").
    pub fn replication_lsn(&self) -> String {
        self.repl_lsn.lock().unwrap().clone()
    }

    /// Highest `__el_sync` sentinel counter the ingestor has decoded-and-appended.
    pub fn replication_sync(&self) -> i64 {
        self.repl_sync.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn stream_url(&self, path: &str) -> String {
        self.ds.stream_url(path)
    }

    /// Library mode's schema definition. Refused outright in Postgres mode ([`SchemaIsPostgres`]):
    /// there the schema is Postgres's, and everything below — the change-log init, the table swap,
    /// the sequencer spawn — belongs to `setup_postgres` alone.
    pub async fn define_schema(&self, schema: &Schema) -> Result<()> {
        if self.pg_url.is_some() {
            return Err(anyhow::Error::new(SchemaIsPostgres));
        }
        let compiled = compile_schema(schema)?;
        // Library mode has no durable catalog to fold, so the current segment is whatever this
        // process already resolved (0 on a fresh engine) and nothing rotates it — there is no
        // ingestor. The CURRENT segment stream is what gets created, never a bare `changes`.
        self.init_change_log(
            self.changes.state().current(),
            self.changes.state().segments(),
            &self.changes.state().position(),
        )
        .await?;
        self.subqueries.lock().await.set_schemas(Arc::new(compiled.clone()));
        *self.tables_shared.write().unwrap() = compiled.clone();
        {
            let mut st = self.state.lock().await;
            st.tables = compiled;
            self.ensure_sequencer(&mut st);
        }
        Ok(())
    }

    /// Run a one-shot **subset query** (the non-materialized counterpart to a shape): a single
    /// `SELECT … WHERE … ORDER BY … LIMIT … OFFSET …` against Postgres, returning the projected page
    /// rows (as JSON) + the snapshot LSN. Creates no shape, no stream, no live state — paging never
    /// becomes server-side range state, so a change can never fan out across ranges. The caller follows
    /// the live tail separately (a base-predicate feed) to keep the page live.
    pub async fn query_subset(
        &self,
        table: &TableRef,
        where_: Option<PredicateJson>,
        columns: Option<Vec<String>>,
        order_by: Option<(String, bool)>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<(Vec<serde_json::Value>, String)> {
        let (ts, schemas) = {
            let st = self.state.lock().await;
            let ts = st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?;
            // Clone the table schemas so the subquery SQL emitter can cast each leaf's param to its
            // column's native Postgres type (query_subset is one-shot; the clone is off the hot path).
            (ts, st.tables.clone())
        };
        let out_cols = resolve_columns(&ts, columns)?;
        let order = match order_by {
            Some((col, desc)) => Some((ts.column_index(&col)?, desc)),
            None => None,
        };
        // Subquery predicates are evaluated natively by Postgres in the one-shot query-back (no engine
        // subquery state needed for a non-live page); other predicates use the compiled-form emitter.
        let where_sql = match where_.as_ref() {
            Some(p) if crate::subquery::predicate_has_subquery(p) => {
                Some(crate::sql::predicate_json_to_sql(p, 1, &schemas, table))
            }
            Some(p) => {
                let cp = CompiledPredicate::compile_opt(Some(p), &ts)?;
                crate::sql::predicate_to_sql(&cp, &ts)
            }
            None => None,
        };
        let url = self.pg_url.clone().context("query_subset requires postgres mode")?;
        let client = crate::pg::pool_for(&url).get().await?;
        let sq = crate::pg::query_subset_where(&client, &ts, where_sql, order, limit, offset).await?;
        let proj = out_cols.as_deref().map(Vec::as_slice);
        let rows = sq.rows.iter().map(|r| ts.row_to_json_cols(r, proj)).collect();
        Ok((rows, sq.lsn))
    }

    /// The column list + primary key of a replicated table, for the visualizer's add-row form. Reads the
    /// in-memory `TableSchema` (introspected at startup) — no Postgres round-trip.
    pub async fn table_schema_info(&self, table: &TableRef) -> Result<TableSchemaInfo> {
        let ts = {
            let st = self.state.lock().await;
            st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?
        };
        let pk_set: HashSet<usize> = ts.pk_cols.iter().copied().collect();
        let columns = ts
            .columns
            .iter()
            .enumerate()
            .map(|(i, (name, ty))| TableColumnInfo {
                name: name.clone(),
                ty: col_type_str(*ty),
                pg_type: ts.pg_types.get(i).cloned().flatten(),
                pk: pk_set.contains(&i),
                has_default: ts.has_defaults.get(i).copied().unwrap_or(false),
            })
            .collect();
        let primary_key = ts.pk_cols.iter().map(|&i| ts.columns[i].0.clone()).collect();
        Ok(TableSchemaInfo {
            table: ts.table.clone(),
            columns,
            primary_key,
            schema_digest: ts.schema_digest.map(crate::schema::digest_hex),
        })
    }

    /// Insert one row into a replicated table's Postgres relation, so the change is captured by logical
    /// replication and flows through the pipeline (backing the visualizer's add-row action). `values`
    /// maps column name → value; only known columns are accepted (unknown ⇒ error), omitted columns take
    /// their Postgres default / NULL. Identifiers are quoted and values are **bound parameters** cast to
    /// each column's native type — no string-concatenated SQL.
    pub async fn insert_row(
        &self,
        table: &TableRef,
        values: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let ts = {
            let st = self.state.lock().await;
            st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?
        };
        if values.is_empty() {
            bail!("no columns provided");
        }
        let mut cols: Vec<String> = Vec::with_capacity(values.len());
        let mut placeholders: Vec<String> = Vec::with_capacity(values.len());
        let mut params: Vec<String> = Vec::new();
        for (col, val) in values {
            // Reject unknown columns (also closes the identifier-injection surface: only catalog columns
            // are ever emitted, each independently quoted).
            if !ts.index.contains_key(col) {
                bail!("unknown column '{col}' on table '{table}'");
            }
            cols.push(crate::pg::quote_ident(col));
            if val.is_null() {
                placeholders.push("NULL".to_string());
                continue;
            }
            // Bind the value as a text parameter, then cast it to the column's native Postgres type
            // (uuid/int8/bool/timestamptz/…). The leading `::text` pins the parameter's inferred type to
            // text so any value serializes as a string; the second cast converts it to the column type
            // (a bare `$n::int8` would instead make Postgres infer the param itself as int8 and reject a
            // String). A JSON string binds its contents; other scalars bind their compact text form.
            let n = params.len() + 1;
            let placeholder = match ts.pg_type_of(col) {
                Some(t) => format!("${n}::text::{}", crate::pg::quote_ident(t)),
                None => format!("${n}::text"),
            };
            placeholders.push(placeholder);
            let s = match val {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            params.push(s);
        }
        let sql = format!(
            "insert into {} ({}) values ({})",
            table.quote_qualified(),
            cols.join(", "),
            placeholders.join(", "),
        );
        let url = self.pg_url.clone().context("insert_row requires postgres mode")?;
        let client = crate::pg::pool_for(&url).get().await?;
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
        let n = client.execute(&sql, &param_refs).await.with_context(|| format!("insert into {table}"))?;
        Ok(serde_json::json!({ "ok": true, "inserted": n }))
    }

    /// Delete rows from a replicated table's Postgres relation by primary key, so the deletes are
    /// captured by logical replication and flow through the pipeline (backing the visualizer's
    /// delete-rows action). `keys` holds one map per row: primary-key column → value. Every key must
    /// supply exactly the table's primary-key columns, non-NULL. All rows go in one parameterized
    /// statement (identifiers quoted, values bound and cast to the columns' native types), so a
    /// multi-row delete is a single transaction — one replication batch, one pipeline delta.
    pub async fn delete_rows(
        &self,
        table: &TableRef,
        keys: &[serde_json::Map<String, serde_json::Value>],
    ) -> Result<serde_json::Value> {
        const MAX_KEYS: usize = 1000;
        let ts = {
            let st = self.state.lock().await;
            st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?
        };
        if keys.is_empty() {
            bail!("no keys provided");
        }
        if keys.len() > MAX_KEYS {
            bail!("too many keys ({}); at most {MAX_KEYS} rows per delete", keys.len());
        }
        if ts.pk_cols.is_empty() {
            bail!("table '{table}' has no primary key");
        }
        let pk_names: Vec<&str> = ts.pk_cols.iter().map(|&i| ts.columns[i].0.as_str()).collect();
        let mut clauses: Vec<String> = Vec::with_capacity(keys.len());
        let mut params: Vec<String> = Vec::with_capacity(keys.len() * pk_names.len());
        for key in keys {
            // Only primary-key columns are accepted (as with insert, this also closes the
            // identifier-injection surface: every emitted identifier comes from the catalog).
            for col in key.keys() {
                if !pk_names.contains(&col.as_str()) {
                    bail!("column '{col}' is not in table '{table}''s primary key");
                }
            }
            let mut conj: Vec<String> = Vec::with_capacity(pk_names.len());
            for &col in &pk_names {
                let val = key.get(col).ok_or_else(|| anyhow::anyhow!("key is missing primary-key column '{col}'"))?;
                if val.is_null() {
                    bail!("primary-key column '{col}' must not be NULL");
                }
                // Same bind-as-text-then-cast scheme as insert_row (see the comment there).
                let n = params.len() + 1;
                let placeholder = match ts.pg_type_of(col) {
                    Some(t) => format!("${n}::text::{}", crate::pg::quote_ident(t)),
                    None => format!("${n}::text"),
                };
                conj.push(format!("{} = {placeholder}", crate::pg::quote_ident(col)));
                params.push(match val {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                });
            }
            clauses.push(format!("({})", conj.join(" and ")));
        }
        let sql = format!("delete from {} where {}", table.quote_qualified(), clauses.join(" or "));
        let url = self.pg_url.clone().context("delete_rows requires postgres mode")?;
        let client = crate::pg::pool_for(&url).get().await?;
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
        let n = client.execute(&sql, &param_refs).await.with_context(|| format!("delete from {table}"))?;
        Ok(serde_json::json!({ "ok": true, "deleted": n }))
    }

    /// Number of maintained subquery nodes (for the sharing-topology introspection endpoint).
    pub async fn subquery_node_count(&self) -> usize {
        self.subqueries.lock().await.node_count()
    }

    /// Per-node subquery topology (signature, inner table, distinct values, refcount).
    pub async fn subquery_stats(&self) -> Vec<crate::subquery::NodeStat> {
        self.subqueries.lock().await.stats()
    }

    /// The schema for `table`, if known (used by the Electric-protocol adapter for the schema header and
    /// value encoding).
    pub async fn table_schema(&self, table: &TableRef) -> Option<TableSchema> {
        self.state.lock().await.tables.get(table).cloned()
    }

    /// Read a shape's durable stream (catch-up or long-poll live) — used by the Electric adapter to turn
    /// the engine's shape output into Electric `/v1/shape` change messages.
    pub async fn read_shape_stream(&self, path: &str, offset: &str, live: bool) -> Result<crate::ds::ReadResult> {
        // A data read is a full retention touch: reactivate a dormant shape before reading (so a
        // parked stream is never served stale) and refresh `last_read`. `ensure_active` is a cheap
        // lifecycle-map check when the shape is active (the common case).
        self.ensure_active(sid_of_path(path)).await?;
        self.ds.read(path, offset, live).await
    }

    /// Engine-internal cardinalities for the memory probe — the structures whose growth drives RSS:
    /// registered shapes, per-table tailers, shared **family circuits** (the M× join-trace amplifier:
    /// each holds the base table once), standalone per-shape circuits, and the subquery registry's
    /// nodes/contributor-pks. Read directly from in-memory state (cheap; no tailer round-trip, no
    /// byte-walk, no sequencer round-trip).
    ///
    /// This is the ONLY cardinality path the 500ms background sampler (`mem::spawn_sampler`) is
    /// allowed to call. It deliberately never touches `HeapSize::heap_bytes` or sends
    /// `SequencerCmd::MemBytes` — every `bytes_*` field on the returned [`crate::mem::Cardinalities`]
    /// is left at its `Default` zero. Byte-level self-accounting (Phase 0 of the memory-reduction
    /// effort) lives in the sibling [`Self::mem_bytes`], called only by `GET /memory`; see its doc
    /// comment for why that split exists (a prior regression: this method used to do the walk
    /// inline, which meant a ~100MB recursive walk + a `MemBytes` sequencer round-trip ran twice a
    /// second at 50k+ shapes).
    pub async fn mem_cardinalities(&self) -> crate::mem::Cardinalities {
        let (shapes, tailers, tables, families, family_shapes, standalone) = {
            let st = self.state.lock().await;
            let mut families = 0usize;
            let mut family_shapes = 0usize;
            let mut standalone = 0usize;
            let mut tables_with_execs = 0usize;
            if let Some(seq) = st.sequencer.as_ref()
                && let Ok(per_table) = seq.stats.lock()
            {
                tables_with_execs = per_table.len();
                for s in per_table.values() {
                    families += s.families.len();
                    family_shapes += s.families.iter().map(|f| f.shapes).sum::<usize>();
                    standalone += s.standalone;
                }
            }
            (st.shapes.len(), tables_with_execs, st.tables.len(), families, family_shapes, standalone)
        };
        let sq = {
            let reg = self.subqueries.lock().await;
            reg.mem_totals()
        };
        let shapes_dormant =
            self.lives.lock().unwrap().values().filter(|l| matches!(l.state, LifeState::Dormant { .. })).count();
        crate::mem::Cardinalities {
            shapes,
            shapes_dormant,
            tailers,
            tables,
            families,
            family_shapes,
            standalone,
            subquery_nodes: sq.nodes,
            subquery_contributors: sq.contributors,
            subquery_distinct_values: sq.distinct,
            subquery_shapes: sq.shapes,
            subquery_edges: sq.edges,
            subquery_feed_entries: sq.feed_entries,
            ..Default::default()
        }
    }

    /// Diagnostic only: full dbsp profiler dumps for EVERY dbsp circuit the engine runs — the
    /// (single, engine-wide) subquery membership circuit plus the counts/arrangements circuit
    /// when configured. The engine's "family" and "standalone" circuits are host-side executor
    /// structures (no dbsp runtime; sized by `bytes_executors`), so they have no profile here.
    /// Heavy (profiler round-trip through each circuit thread, holds the subquery-registry lock
    /// for the membership dump); serves `GET /debug/dbsp-profile` on demand ONLY — never call
    /// this from the 500 ms sampler (see `mem::spawn_sampler`).
    pub async fn dbsp_profile_dump(&self) -> serde_json::Value {
        fn entry(used: usize, stored: usize, json: String) -> serde_json::Value {
            serde_json::json!({
                "total_used_bytes": used,
                "total_storage_bytes": stored,
                "profile": serde_json::from_str::<serde_json::Value>(&json)
                    .unwrap_or(serde_json::Value::String(json)),
            })
        }
        let membership = {
            let reg = self.subqueries.lock().await;
            let (used, stored, json) = reg.circuit_profile_dump().await;
            entry(used, stored, json)
        };
        let arr = self.arrangements.lock().unwrap().clone();
        let counts = match arr {
            Some(a) => {
                let (used, stored, json) = a.profile_dump().await;
                entry(used, stored, json)
            }
            None => serde_json::Value::Null,
        };
        serde_json::json!({ "membership_circuit": membership, "counts_circuit": counts })
    }

    /// On-demand byte-level self-accounting (Phase 0 of the memory-reduction effort): a
    /// [`crate::heap_size::HeapSize`] lower-bound owned-heap estimate per major structure. These
    /// are LOWER BOUNDS (owned heap, not allocator slack) — the gap vs. `process.rss_bytes` is the
    /// allocator/pinning term this phase is instrumenting to measure.
    ///
    /// Expensive: locks engine state, round-trips a one-off `SequencerCmd::MemBytes` to the
    /// sequencer task (mirroring the `DumpNode` command's pattern — see `dump_node` below), locks
    /// the subquery registry, and walks roughly the engine's entire owned heap. Call this ONLY
    /// from the `GET /memory` HTTP handler — never from the 500ms background sampler
    /// (`mem::spawn_sampler`), which calls `mem_cardinalities` instead. Mixing this into the
    /// sampler's path was exactly the prior regression (a large peak/steady RSS increase from
    /// twice-a-second byte walks); see `mem::spawn_sampler`'s doc comment.
    ///
    /// `bytes_executors` (standalone shapes + their conjunct index, family routers, aggregate
    /// folds + their index) is the one term this method cannot read out of already-published
    /// state: those structures are privately owned by the sequencer task's `execs` map, never
    /// exposed through a shared mutex (unlike `stats`/`node_states`, which are republished after
    /// every batch specifically so other tasks can read them cheaply). Walking them for real bytes
    /// is not cheap enough to piggyback on every batch (see `sequencer::publish_all`/`stats_of`),
    /// so instead this method round-trips the one-off `SequencerCmd::MemBytes` so the byte-walk
    /// itself only ever runs on this on-demand path, never per batch.
    pub async fn mem_bytes(&self) -> crate::mem::HeapBytes {
        let (bytes_shape_records, cmd_tx) = {
            let st = self.state.lock().await;
            (st.shapes.heap_bytes(), st.sequencer.as_ref().map(|seq| seq.cmd_tx.clone()))
        };
        // Byte-walk every table's live executor state: ask the sequencer task directly, since it
        // privately owns `execs`.
        let bytes_executors = match cmd_tx {
            Some(tx) => {
                let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                if tx.send(SequencerCmd::MemBytes { resp: resp_tx }).is_ok() { resp_rx.await.unwrap_or(0) } else { 0 }
            }
            None => 0,
        };
        let (circuit_bytes, bytes_feed_sets, bytes_subquery_registry, bytes_pk_dict) = {
            let reg = self.subqueries.lock().await;
            (reg.circuit_bytes(), reg.feed_sets_bytes(), reg.heap_bytes(), reg.pk_dict_bytes())
        };
        let bytes_retention = self.lives.lock().unwrap().heap_bytes();
        let bytes_electric_adapter = crate::electric::ttl_registry_heap_bytes().await;
        crate::mem::HeapBytes {
            bytes_shape_records,
            bytes_executors,
            bytes_retention,
            bytes_subquery_registry,
            bytes_membership_circuit: circuit_bytes.total_bytes(),
            bytes_circuit_integral: circuit_bytes.integral_bytes(),
            bytes_circuit_snapshots: circuit_bytes.snapshot_bytes(),
            bytes_feed_sets,
            bytes_pk_dict,
            bytes_electric_adapter,
        }
    }

    pub async fn get_shape(&self, id: &str) -> Option<ShapeRecord> {
        self.state.lock().await.shapes.get(id).cloned()
    }

    /// How many live subscriptions a shape has (`GET /shapes/{id}` — ADR-0008).
    ///
    /// The COUNT, not the ids: an operator needs to know why a shape will not go dormant, and the
    /// ids are other callers' handles. Deliberately not a retention touch, like the rest of that
    /// route.
    pub async fn subscription_count(&self, id: &str) -> usize {
        self.state.lock().await.feed_shares.get(id).map(FeedShare::refcount).unwrap_or(0)
    }

    /// The lease window a subscription must renew within (`ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS`),
    /// handed to clients in every create response so the renewal cadence is the server's to set.
    /// `0` = dormancy is off, so leases never lapse.
    pub fn lease_seconds(&self) -> u64 {
        self.retention.idle_timeout.as_secs()
    }

    /// The change-log position up to which the sequencer has processed (global — all tables share
    /// the single ordered log), or `None` if the sequencer is not running yet.
    pub async fn table_offset(&self, _table: &TableRef) -> Option<LogPosition> {
        let st = self.state.lock().await;
        st.sequencer.as_ref().map(|s| s.processed.lock().unwrap().clone())
    }

    /// The ingestor's own change-log position: the CURRENT segment and the tail offset of its last
    /// append (`GET /replication/lsn` → `changes`). Additive observability — and the only place a
    /// consumer can learn which segment is current, which is what a convergence barrier HEADs for
    /// the tail.
    pub fn changes_position(&self) -> LogPosition {
        self.changes.state().position()
    }

    /// The table's current circuit topology (shared families + standalone count), or `None` if no
    /// tailer exists.
    pub async fn table_stats(&self, table: &TableRef) -> Option<TableStats> {
        let st = self.state.lock().await;
        st.sequencer.as_ref().and_then(|s| s.stats.lock().unwrap().get(table).cloned())
    }
}
