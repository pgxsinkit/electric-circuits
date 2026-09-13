//! Lightweight, lock-free engine telemetry: counters + log-bucketed latency histograms, exposed
//! via `GET /metrics`. Used by the benchmark harness to attribute bottlenecks (per-envelope
//! fan-out, family-step, and shape-append latencies) under sustained load.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

const NB: usize = 40; // buckets cover [2^0, 2^40) microseconds (~12 days) — plenty of headroom.

/// A lock-free latency histogram with power-of-two buckets (bucket `i` = `[2^(i-1), 2^i)` µs).
/// Percentiles are reported as the bucket's upper bound — coarse but allocation-free and contention
/// free, which is what we want on the hot path.
pub struct Hist {
    buckets: [AtomicU64; NB],
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}

impl Hist {
    const fn new() -> Self {
        Hist {
            buckets: [const { AtomicU64::new(0) }; NB],
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }

    pub fn record(&self, us: u64) {
        self.buckets[bucket(us)].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(us, Ordering::Relaxed);
        self.max.fetch_max(us, Ordering::Relaxed);
    }

    fn quantile(&self, q: f64) -> u64 {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return 0;
        }
        let target = ((q * count as f64).ceil() as u64).max(1);
        let mut cum = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            cum += b.load(Ordering::Relaxed);
            if cum >= target {
                return 1u64 << i; // upper bound of bucket i
            }
        }
        1u64 << (NB - 1)
    }

    fn snapshot(&self) -> serde_json::Value {
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        serde_json::json!({
            "count": count,
            "mean_us": if count > 0 { sum / count } else { 0 },
            "p50_us": self.quantile(0.50),
            "p99_us": self.quantile(0.99),
            "p999_us": self.quantile(0.999),
            "max_us": self.max.load(Ordering::Relaxed),
        })
    }

    fn reset(&self) {
        for b in &self.buckets {
            b.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
        self.sum.store(0, Ordering::Relaxed);
        self.max.store(0, Ordering::Relaxed);
    }
}

fn bucket(us: u64) -> usize {
    if us == 0 {
        return 0;
    }
    ((64 - us.leading_zeros()) as usize).min(NB - 1)
}

/// A scoped timer: records its elapsed microseconds into `hist` on drop.
pub struct Timer<'a> {
    hist: &'a Hist,
    start: std::time::Instant,
}
impl<'a> Timer<'a> {
    pub fn new(hist: &'a Hist) -> Self {
        Timer { hist, start: std::time::Instant::now() }
    }
}
impl Drop for Timer<'_> {
    fn drop(&mut self) {
        self.hist.record(self.start.elapsed().as_micros() as u64);
    }
}

pub struct Metrics {
    pub envelopes: AtomicU64,          // table change events processed
    pub shape_appends: AtomicU64,      // appends to shape streams
    pub family_steps: AtomicU64,       // family circuit transactions (write path)
    pub shapes_dormanted: AtomicU64,   // retention: active -> dormant transitions
    pub shapes_reactivated: AtomicU64, // retention: dormant -> active (table-stream replay)
    pub shapes_evicted: AtomicU64,     // retention: dormant shapes evicted (stream deleted)
    pub retention_pressure: AtomicU64, // retention: sweeps where a cap/budget was exceeded with nothing dormant to evict
    /// ADR-0008 COUNTER: subscriptions released by the sweeper because their lease was not renewed
    /// within the idle window. A climbing value with healthy clients means the renewal cadence is
    /// longer than `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS`, not that anything crashed.
    pub subscriptions_lapsed: AtomicU64,
    pub schema_drift: AtomicU64, // ADR-0005: tables whose dependents were retired (drift / TRUNCATE / identity regression / drop)
    pub schema_unresolved: AtomicU64, // ADR-0005: drifts that could not be resolved (table parked, retrying)
    pub epoch_breaks: AtomicU64, // ADR-0004: slots the engine could no longer vouch for
    pub epoch_resets: AtomicU64, // ADR-0004: new epochs bound (every shape retired, fresh slot)
    pub changes_rotations: AtomicU64, // ADR-0006: change-log segments closed and succeeded
    pub changes_segments_deleted: AtomicU64, // ADR-0006: rotated-out segments retired (nothing could resume inside)
    /// Catalog appends re-attempted because durable-streams was unavailable. Never a dropped event
    /// (the writer retries in place, forever) — this is the visible cost of that promise, and a
    /// climbing value with a flat `shapes_*` means creates are waiting on storage.
    pub catalog_append_retries: AtomicU64,
    /// ADR-0007: retirement attempts that failed and were re-queued.
    pub retirement_retries: AtomicU64,
    /// ADR-0009: shape records the boot's catalog restore retired instead of resuming, one counter
    /// per [`RestoreRetireReason`] (indexed by it). Non-zero after a boot is expected for `schema`
    /// (a migration the engine slept through) and `subquery` (never restorable); `table_gone`,
    /// `stream_missing` and `stream_closed` say something outside the engine moved.
    pub catalog_restore_retired: [AtomicU64; RestoreRetireReason::ALL.len()],
    pub txn_spills: AtomicU64, // ADR-0003: transactions whose buffer outgrew the memory cap and went to disk
    /// COUNTER (cumulative), not a gauge: bytes ever written to transaction spill files. A gauge
    /// would be meaningless — a spill file exists only between one `Begin` and its `Commit`, so any
    /// sample would almost always read zero. `reset()` clears it with the other counters.
    pub txn_spill_bytes: AtomicU64,
    /// ADR-0003: chunk appends made by commits too large for a single append. A commit that fits in
    /// one append contributes 0, so this counts exactly the chunked commits' POSTs.
    pub txn_chunked_appends: AtomicU64,
    /// Backfills whose snapshot was too large for one append and went out in chunks: the number
    /// of chunk POSTs those backfills made (a backfill that fits in one append contributes 0, same
    /// accounting as `txn_chunked_appends`).
    pub backfill_chunked_appends: AtomicU64,
    /// ADR-0003: incomplete transaction fragments the sequencer discarded because a DIFFERENT
    /// transaction followed them on the log (a reconnect re-delivering earlier complete commits
    /// first, or an epoch reset abandoning the fragment). Never zero-cost silence: the fragment is
    /// re-delivered in full or was abandoned, and either way an operator should be able to see it.
    pub sequencer_orphan_fragments: AtomicU64,
    /// ADR-0010: change-log envelopes the sequencer CONSUMED without decoding, because the schema
    /// they were decoded under is not the one the engine holds any more — a drift (ADR-0005) swapped
    /// it and retired every shape that could have wanted them, while the sequencer was still behind
    /// the ingestor. The one legitimate reason an envelope is not processed; everything else parks the
    /// sequencer (`change_log_failure`). Non-zero after a migration is expected, and the count is
    /// bounded by the DML the migrating transaction outran.
    pub sequencer_stale_schema_skipped: AtomicU64,
    /// ADR-0010: change-log envelopes CONSUMED without decoding because the engine does not compile
    /// their table at all — it was dropped, or is parked unresolved (ADR-0005). The second (and last)
    /// legitimate reason an envelope is not processed: the table's dependents went with it, so the
    /// change has no consumer. A `type` that is not a canonical `schema.name` is NOT counted here — no
    /// producer can write one, so it parks the sequencer instead.
    pub sequencer_unknown_table_skipped: AtomicU64,
    /// GAUGE, not a counter: how many change-log segments exist right now (republished by every
    /// retention sweep). `reset()` leaves it alone — a gauge describes the world, not the window.
    pub changes_segments_retained: AtomicU64,
    /// GAUGE: 1 while the sequencer is holding an incomplete transaction (ADR-0003), 0 otherwise.
    /// A hold pins the change-log position — the restart point, the convergence barrier and the
    /// segment-deletion floor — so a hold that does not end must be visible as a level, not only as
    /// a once-a-minute log line.
    pub sequencer_held_run: AtomicU64,
    /// GAUGE: 1 once a `SIGTERM`/`SIGINT` graceful shutdown has begun (see [`crate::shutdown`]).
    pub shutdown_in_progress: AtomicU64,
    /// ADR-0008 GAUGE: live subscriptions across every shape — the claims pinning shapes against
    /// dormancy right now. Republished by every retention sweep. A shape that will not go dormant
    /// is explained by this number, not by the shape count.
    pub subscriptions_live: AtomicU64,
    /// GAUGE: shape streams dropped from the engine's records whose deletion storage has not yet
    /// accepted (ADR-0007). Non-zero means public stream URLs are outliving their shapes right now;
    /// it should return to 0 on its own, and a boot re-derives it from the catalog.
    pub retirements_pending: AtomicU64,
    /// GAUGE: `pg_current_wal_lsn() - restart_lsn` for the engine's slot — the WAL Postgres is
    /// holding on disk **for this engine**. The number that fills the source database's disk when
    /// the engine falls behind or stops. Sampled ~every 10 s on a pooled connection.
    pub replication_slot_retained_wal_bytes: AtomicU64,
    /// GAUGE: `pg_current_wal_lsn() - confirmed_flush_lsn` — how far behind the engine's
    /// acknowledged position is. Ingest lag, in bytes of WAL.
    pub replication_confirmed_flush_lag_bytes: AtomicU64,
    /// GAUGE: 1 while a walsender holds the slot (i.e. this engine is streaming), 0 when it is not
    /// — and 0 when the slot does not exist at all, which is the epoch-break case.
    pub replication_slot_active: AtomicU64,
    pub process_envelope: Hist, // end-to-end fan-out latency per table envelope
    pub family_step: Hist,      // one family circuit transaction
    pub append: Hist,           // one shape-stream append (durable-streams round-trip)
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(|| Metrics {
        envelopes: AtomicU64::new(0),
        shape_appends: AtomicU64::new(0),
        family_steps: AtomicU64::new(0),
        shapes_dormanted: AtomicU64::new(0),
        shapes_reactivated: AtomicU64::new(0),
        shapes_evicted: AtomicU64::new(0),
        retention_pressure: AtomicU64::new(0),
        subscriptions_lapsed: AtomicU64::new(0),
        subscriptions_live: AtomicU64::new(0),
        schema_drift: AtomicU64::new(0),
        schema_unresolved: AtomicU64::new(0),
        epoch_breaks: AtomicU64::new(0),
        epoch_resets: AtomicU64::new(0),
        changes_rotations: AtomicU64::new(0),
        changes_segments_deleted: AtomicU64::new(0),
        changes_segments_retained: AtomicU64::new(0),
        catalog_append_retries: AtomicU64::new(0),
        retirement_retries: AtomicU64::new(0),
        catalog_restore_retired: std::array::from_fn(|_| AtomicU64::new(0)),
        retirements_pending: AtomicU64::new(0),
        backfill_chunked_appends: AtomicU64::new(0),
        sequencer_orphan_fragments: AtomicU64::new(0),
        sequencer_stale_schema_skipped: AtomicU64::new(0),
        sequencer_unknown_table_skipped: AtomicU64::new(0),
        sequencer_held_run: AtomicU64::new(0),
        shutdown_in_progress: AtomicU64::new(0),
        replication_slot_retained_wal_bytes: AtomicU64::new(0),
        replication_confirmed_flush_lag_bytes: AtomicU64::new(0),
        replication_slot_active: AtomicU64::new(0),
        txn_spills: AtomicU64::new(0),
        txn_spill_bytes: AtomicU64::new(0),
        txn_chunked_appends: AtomicU64::new(0),
        process_envelope: Hist::new(),
        family_step: Hist::new(),
        append: Hist::new(),
    })
}

/// Why the boot's catalog restore retired a shape record rather than resuming it (ADR-0009). Every
/// one of them is a **definitive** answer about that one shape; a transient failure is never a
/// reason — it fails the restore and the boot retries it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreRetireReason {
    /// The table's schema moved while the engine was down (ADR-0005).
    Schema,
    /// A subquery shape: its inner-node state is not persisted, so it is never restorable.
    Subquery,
    /// The table is no longer in the compiled set — dropped while the engine was down under a
    /// wildcard selector, or no longer selected by `ELECTRIC_CIRCUITS_PG_TABLES`. (A table an explicit
    /// entry still names refuses the boot before the restore runs.)
    TableGone,
    /// Storage answered the stream's `HEAD` with 404/410.
    StreamMissing,
    /// Storage answered the stream's `HEAD` with `stream-closed`: it can never be appended to again.
    StreamClosed,
}

impl RestoreRetireReason {
    pub const ALL: [RestoreRetireReason; 5] = [
        RestoreRetireReason::Schema,
        RestoreRetireReason::Subquery,
        RestoreRetireReason::TableGone,
        RestoreRetireReason::StreamMissing,
        RestoreRetireReason::StreamClosed,
    ];

    /// The label an operator sees: the `reason` attribute on Prometheus, the key suffix in JSON.
    pub fn label(self) -> &'static str {
        match self {
            RestoreRetireReason::Schema => "schema",
            RestoreRetireReason::Subquery => "subquery",
            RestoreRetireReason::TableGone => "table_gone",
            RestoreRetireReason::StreamMissing => "stream_missing",
            RestoreRetireReason::StreamClosed => "stream_closed",
        }
    }
}

impl Metrics {
    /// Count one record the catalog restore retired (ADR-0009).
    pub fn restore_retired(&self, reason: RestoreRetireReason) {
        self.catalog_restore_retired[reason as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> serde_json::Value {
        let mut value = serde_json::json!({
            "counters": {
                "envelopes_processed": self.envelopes.load(Ordering::Relaxed),
                "shape_appends": self.shape_appends.load(Ordering::Relaxed),
                "family_steps": self.family_steps.load(Ordering::Relaxed),
                "shapes_dormanted": self.shapes_dormanted.load(Ordering::Relaxed),
                "shapes_reactivated": self.shapes_reactivated.load(Ordering::Relaxed),
                "shapes_evicted": self.shapes_evicted.load(Ordering::Relaxed),
                "retention_pressure": self.retention_pressure.load(Ordering::Relaxed),
                "subscriptions_lapsed_total": self.subscriptions_lapsed.load(Ordering::Relaxed),
                "schema_drift_total": self.schema_drift.load(Ordering::Relaxed),
                "schema_unresolved_total": self.schema_unresolved.load(Ordering::Relaxed),
                "epoch_breaks_total": self.epoch_breaks.load(Ordering::Relaxed),
                "epoch_resets_total": self.epoch_resets.load(Ordering::Relaxed),
                "changes_rotations_total": self.changes_rotations.load(Ordering::Relaxed),
                "changes_segments_deleted_total": self.changes_segments_deleted.load(Ordering::Relaxed),
                "catalog_append_retries_total": self.catalog_append_retries.load(Ordering::Relaxed),
                "retirement_retries_total": self.retirement_retries.load(Ordering::Relaxed),
                "txn_spills_total": self.txn_spills.load(Ordering::Relaxed),
                "txn_spill_bytes": self.txn_spill_bytes.load(Ordering::Relaxed),
                "txn_chunked_appends_total": self.txn_chunked_appends.load(Ordering::Relaxed),
                "backfill_chunked_appends_total": self.backfill_chunked_appends.load(Ordering::Relaxed),
                "sequencer_orphan_fragments_total": self.sequencer_orphan_fragments.load(Ordering::Relaxed),
                "sequencer_stale_schema_skipped_total": self.sequencer_stale_schema_skipped.load(Ordering::Relaxed),
                "sequencer_unknown_table_skipped_total": self.sequencer_unknown_table_skipped.load(Ordering::Relaxed),
            },
            "gauges": {
                "changes_segments_retained": self.changes_segments_retained.load(Ordering::Relaxed),
                "sequencer_held_run": self.sequencer_held_run.load(Ordering::Relaxed),
                "shutdown_in_progress": self.shutdown_in_progress.load(Ordering::Relaxed),
                "subscriptions_live": self.subscriptions_live.load(Ordering::Relaxed),
                "retirements_pending": self.retirements_pending.load(Ordering::Relaxed),
                "replication_slot_retained_wal_bytes": self.replication_slot_retained_wal_bytes.load(Ordering::Relaxed),
                "replication_confirmed_flush_lag_bytes": self.replication_confirmed_flush_lag_bytes.load(Ordering::Relaxed),
                "replication_slot_active": self.replication_slot_active.load(Ordering::Relaxed),
            },
            "process_envelope_us": self.process_envelope.snapshot(),
            "family_step_us": self.family_step.snapshot(),
            "append_us": self.append.snapshot(),
        });
        // Flat keys, one per reason, so `counters` stays a map of numbers like every other entry.
        for reason in RestoreRetireReason::ALL {
            value["counters"][format!("catalog_restore_retired_{}_total", reason.label())] =
                self.catalog_restore_retired[reason as usize].load(Ordering::Relaxed).into();
        }
        value
    }

    /// Zero all counters and histograms — the benchmark calls this after shape registration so the
    /// load-phase percentiles aren't skewed by setup.
    pub fn reset(&self) {
        self.envelopes.store(0, Ordering::Relaxed);
        self.shape_appends.store(0, Ordering::Relaxed);
        self.family_steps.store(0, Ordering::Relaxed);
        self.shapes_dormanted.store(0, Ordering::Relaxed);
        self.shapes_reactivated.store(0, Ordering::Relaxed);
        self.shapes_evicted.store(0, Ordering::Relaxed);
        self.retention_pressure.store(0, Ordering::Relaxed);
        self.subscriptions_lapsed.store(0, Ordering::Relaxed);
        self.schema_drift.store(0, Ordering::Relaxed);
        self.schema_unresolved.store(0, Ordering::Relaxed);
        self.epoch_breaks.store(0, Ordering::Relaxed);
        self.epoch_resets.store(0, Ordering::Relaxed);
        self.changes_rotations.store(0, Ordering::Relaxed);
        self.changes_segments_deleted.store(0, Ordering::Relaxed);
        self.catalog_append_retries.store(0, Ordering::Relaxed);
        self.retirement_retries.store(0, Ordering::Relaxed);
        for counter in &self.catalog_restore_retired {
            counter.store(0, Ordering::Relaxed);
        }
        self.txn_spills.store(0, Ordering::Relaxed);
        self.txn_spill_bytes.store(0, Ordering::Relaxed);
        self.txn_chunked_appends.store(0, Ordering::Relaxed);
        self.backfill_chunked_appends.store(0, Ordering::Relaxed);
        self.sequencer_orphan_fragments.store(0, Ordering::Relaxed);
        self.sequencer_stale_schema_skipped.store(0, Ordering::Relaxed);
        self.sequencer_unknown_table_skipped.store(0, Ordering::Relaxed);
        // The gauges below are deliberately NOT reset: a gauge describes the world, not the window
        // (`changes_segments_retained`, `sequencer_held_run`, `shutdown_in_progress`,
        // `retirements_pending`, and the three replication-slot gauges the sampler republishes).
        self.process_envelope.reset();
        self.family_step.reset();
        self.append.reset();
    }
}

// ---- replication-slot sampler ------------------------------------------------------------------

/// How often the replication-slot gauges are refreshed. One small catalog query per tick, on a
/// **pooled** connection — a dedicated one would sit idle 99.99% of the time and still count
/// against `max_connections`.
const SLOT_SAMPLE_PERIOD: std::time::Duration = std::time::Duration::from_secs(10);

/// Spawn the replication-slot gauge sampler (Postgres mode only).
///
/// These are engine-owned gauges, not StatsD-owned ones: `GET /metrics`,
/// `GET /metrics/prometheus` and StatsD all read the same sample, so an operator without StatsD can
/// still see the number that fills the source database's disk (`replication_slot_retained_wal_bytes`)
/// and how far behind ingest is (`replication_confirmed_flush_lag_bytes`). It stops when the
/// process begins shutting down.
pub fn spawn_replication_slot_sampler(pg_url: String, slot: String, shutdown: crate::shutdown::ShutdownToken) {
    tokio::spawn(async move {
        let mut logged_err = false;
        // Sample FIRST, then sleep. Sleeping first left the three gauges reading 0 for the first
        // ten seconds of every process — and 0 retained WAL / 0 lag / an inactive slot is not
        // "unknown", it is a specific and reassuring claim that happens to be false.
        let mut first = true;
        loop {
            if !first {
                tokio::select! {
                    _ = shutdown.wait() => return,
                    _ = tokio::time::sleep(SLOT_SAMPLE_PERIOD) => {}
                }
            }
            first = false;
            match sample_replication_slot(&pg_url, &slot).await {
                Ok(()) => logged_err = false,
                Err(e) => {
                    // Once per outage, not once per tick: a Postgres blip must not become a log flood.
                    if !logged_err {
                        tracing::warn!("replication-slot sampler: {e:#}; will retry every {SLOT_SAMPLE_PERIOD:?}");
                        logged_err = true;
                    }
                }
            }
        }
    });
}

/// One sample: read `pg_current_wal_lsn()` and the slot's row, publish the engine gauges, and
/// forward the same numbers to StatsD (a no-op when StatsD is off).
///
/// A slot that is not there at all leaves `replication_slot_active` at 0 and the two byte gauges
/// **untouched** — their last real value, never a fabricated zero (a zero would read as "no lag",
/// which is the opposite of what a missing slot means).
async fn sample_replication_slot(pg_url: &str, slot: &str) -> anyhow::Result<()> {
    let client = crate::pg::pool_for(pg_url).get().await?;
    let q = "select pg_current_wal_lsn()::text, restart_lsn::text, confirmed_flush_lsn::text, active \
             from pg_replication_slots where slot_name = $1";
    let Some(row) = client.query_opt(q, &[&slot]).await? else {
        metrics().replication_slot_active.store(0, Ordering::Relaxed);
        return Ok(());
    };
    let wal: String = row.get(0);
    let restart: Option<String> = row.get(1);
    let confirmed: Option<String> = row.get(2);
    let active: bool = row.get(3);
    publish_slot_gauges(&wal, restart.as_deref(), confirmed.as_deref(), active);
    crate::statsd::replication_slot_gauges(&wal, restart.as_deref(), confirmed.as_deref());
    Ok(())
}

/// Publish one slot sample into the engine gauges (split out so it is unit-testable without a
/// database). A `None` LSN — a freshly created slot has no `restart_lsn` yet — leaves that gauge at
/// its last real value rather than storing a fabricated zero, which would read as "no lag".
pub fn publish_slot_gauges(wal: &str, restart: Option<&str>, confirmed: Option<&str>, active: bool) {
    let m = metrics();
    let wal_u = crate::pg::lsn_to_u64(wal);
    if let Some(r) = restart {
        let retained = wal_u.saturating_sub(crate::pg::lsn_to_u64(r));
        m.replication_slot_retained_wal_bytes.store(retained, Ordering::Relaxed);
    }
    if let Some(c) = confirmed {
        let lag = wal_u.saturating_sub(crate::pg::lsn_to_u64(c));
        m.replication_confirmed_flush_lag_bytes.store(lag, Ordering::Relaxed);
    }
    m.replication_slot_active.store(u64::from(active), Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The gauge arithmetic (and the deliberate "leave it alone" on a NULL LSN). The metrics
    /// singleton is process-global, so this test owns the three slot gauges.
    #[test]
    fn slot_gauges_are_deltas_from_the_wal_head() {
        publish_slot_gauges("0/1000", Some("0/0400"), Some("0/0C00"), true);
        let m = metrics();
        assert_eq!(m.replication_slot_retained_wal_bytes.load(Ordering::Relaxed), 0x1000 - 0x400);
        assert_eq!(m.replication_confirmed_flush_lag_bytes.load(Ordering::Relaxed), 0x1000 - 0xC00);
        assert_eq!(m.replication_slot_active.load(Ordering::Relaxed), 1);

        // A slot with no restart_lsn yet (freshly created): the byte gauge keeps its last real
        // value rather than reporting a fake zero; `active` still tracks reality.
        publish_slot_gauges("0/2000", None, None, false);
        assert_eq!(m.replication_slot_retained_wal_bytes.load(Ordering::Relaxed), 0x1000 - 0x400);
        assert_eq!(m.replication_confirmed_flush_lag_bytes.load(Ordering::Relaxed), 0x1000 - 0xC00);
        assert_eq!(m.replication_slot_active.load(Ordering::Relaxed), 0);
    }
}
