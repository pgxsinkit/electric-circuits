//! The LSN-ordered sequencer: command protocol, per-table executors, the main loop,
//! envelope processing, activation/backfill, and the reliable flush.

use super::*;

/// Handle to the engine's single **sequencer** task — the LSN-ordered executor consuming the
/// global `changes` stream (Electric's `ShapeLogCollector` pattern): one task processes every
/// table's changes in commit order and flushes each transaction's shape appends before the next
/// transaction, restoring per-transaction atomic emission across tables.
pub(crate) struct SequencerHandle {
    pub(crate) cmd_tx: mpsc::UnboundedSender<SequencerCmd>,
    /// Change-log position up to which every envelope has been processed AND fanned to every shape
    /// (appends landed) — `(segment, offset)`, since the log is segmented (ADR-0006). A harness
    /// polls this against the current segment's tail as the convergence barrier.
    pub(crate) processed: Arc<std::sync::Mutex<LogPosition>>,
    /// Per-table circuit topology (shared families + standalone count), for tests/observability.
    pub(crate) stats: Arc<std::sync::Mutex<HashMap<TableRef, TableStats>>>,
    /// Live per-node state summaries, merged across all tables, keyed by graph node id.
    /// Republished after every processed batch and on shape add/remove; read by `GET /state`.
    pub(crate) node_states: Arc<std::sync::Mutex<HashMap<String, NodeStateSummary>>>,
}

pub(crate) enum SequencerCmd {
    /// Phase 1 of shape creation: register a PENDING shape that buffers this table's deltas while
    /// the creator runs the Postgres backfill concurrently — the sequencer itself never blocks on
    /// Postgres, so one slow backfill cannot stall the whole change pipeline. Buffer registration
    /// is acknowledged BEFORE the creator takes its snapshot, so no change can fall between the
    /// snapshot and activation.
    BeginShape {
        table: TableRef,
        shape_id: String,
        num_id: u64,
        stream_path: String,
        pred: Arc<CompiledPredicate>,
        /// Output projection (column indices to emit), or `None` for the full row.
        out_cols: Option<Arc<Vec<usize>>>,
        kind: CreateKind,
        ack: tokio::sync::oneshot::Sender<()>,
    },
    /// Phase 2: the creator's backfill snapshot has been appended chunk by chunk (plain) or folded
    /// into `agg_seed` (aggregates); drain the buffered deltas through the shape's snapshot gate
    /// and go live.
    /// `ready` mirrors the old add-shape handshake: `Ok(())` once the shape is live and its
    /// snapshot + gated buffer are on the stream, `Err(reason)` otherwise.
    ActivateShape {
        table: TableRef,
        shape_id: String,
        gate: crate::pg::SnapshotGate,
        /// An aggregate's fold, already seeded by the creator from the streamed backfill (`None`
        /// for plain shapes — the creator appended their snapshot envelopes chunk by chunk — and
        /// for a reactivation, whose state comes from the change-log replay).
        agg_seed: Option<AggSeed>,
        /// Snapshot envelopes the creator appended (seeds the shape's emit counter).
        emitted_seed: u64,
        /// The error travels typed, not formatted: the catalog restore hands it to the boot, which
        /// retries a storage outage and refuses a real refusal (`pg::boot_disposition`), and a
        /// string cannot say which it was.
        ready: tokio::sync::oneshot::Sender<Result<()>>,
    },
    /// Creation failed after `BeginShape`: drop the pending buffer.
    AbortShape {
        table: TableRef,
        shape_id: String,
    },
    /// Retention: unregister a plain row shape's routing and hand back its resume state — the
    /// sequencer's fully-processed change-log position (the batch preceding this command was fully
    /// fanned out + flushed, so the shape's stream is complete up to here) and the shape's
    /// backfill-snapshot gate. `None` if the shape is unknown (or an aggregate — not parkable).
    DeactivateShape {
        table: TableRef,
        shape_id: String,
        resp: tokio::sync::oneshot::Sender<Option<(LogPosition, crate::pg::SnapshotGate)>>,
    },
    RemoveShape {
        table: TableRef,
        shape_id: String,
    },
    /// Schema drift (ADR-0005): forget everything the sequencer holds for this table. The executor
    /// is keyed by the OLD `TableSchema`, so it is dropped outright rather than patched; the next
    /// envelope for the table lazily rebuilds it from the (already swapped) shared schema view.
    /// Every shape it was routing has been retired by the same handler, so there is nothing to
    /// preserve.
    ResetTable {
        table: TableRef,
    },
    /// Create a **circuit-served** COUNT aggregate over the table's counts pipeline: seeded by
    /// summing matching groups, then updated from the pipeline's per-transaction group deltas.
    CreateCircuitAgg {
        table: TableRef,
        shape_id: String,
        stream_path: String,
        constraints: Vec<Option<std::collections::HashSet<Value>>>,
        /// Typed for the same reason as `ActivateShape`'s.
        ready: tokio::sync::oneshot::Sender<Result<()>>,
    },
    /// Start reading the change log. Only a sequencer spawned with `hold_reads` needs it: the
    /// catalog restore sends it once EVERY restored shape is registered (ADR-0009). Acknowledged
    /// from the loop, so the sender knows the next thing the sequencer does is read.
    ReleaseReads {
        done: tokio::sync::oneshot::Sender<()>,
    },
    /// Stop a sequencer whose reads were never released: the undo of a catalog restore that could
    /// not complete (ADR-0009). It exits without a checkpoint — it has read nothing, so it has no
    /// position of its own to record, and the registrations it holds belong to a restore that is
    /// being rolled back. The retried boot spawns a fresh one from the catalog it re-folds.
    Discard {
        done: tokio::sync::oneshot::Sender<()>,
    },
    /// Dump the full internal state of one node (`family:<t>:<cols>` → the routing index
    /// contents; an aggregate `shape:<sid>` → the fold internals incl. the MIN/MAX multiset).
    /// `None` if the node id is unknown. Serves `GET /state/node`.
    DumpNode {
        table: TableRef,
        node_id: String,
        resp: tokio::sync::oneshot::Sender<Option<serde_json::Value>>,
    },
    /// On-demand owned-heap byte-walk of every table's live executor state (see
    /// `introspection::exec_heap_bytes`) — the memory probe's `bytes_executors` term. Sent only
    /// from `Engine::mem_bytes` (called only by `GET /memory` — never the 500ms background
    /// sampler, which calls the cheap `Engine::mem_cardinalities` instead), never from the
    /// per-batch write path, so the walk's cost never lands on ingestion or on the sampler.
    MemBytes {
        resp: tokio::sync::oneshot::Sender<usize>,
    },
}

/// What kind of shape a pending creation becomes at activation.
#[derive(Clone)]
pub(crate) enum CreateKind {
    Plain,
    Aggregate { func: AggFn, col: Option<usize> },
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_sequencer(
    ds: DsClient,
    tables: SharedTables,
    start: LogPosition,
    restore_highwater: Option<(u64, u64)>,
    catalog_tx: CatalogWriter,
    subq: SubqueryHandle,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
    arr: Option<crate::arrangements::Arrangements>,
    arr_gates: HashMap<TableRef, crate::pg::SnapshotGate>,
    // No Postgres behind the engine: writes arrive on the change log through the native write API
    // and carry no replication old-image, so the sequencer keeps the current row per key itself
    // (see `TableExec::library_rows`).
    library_mode: bool,
    // Do not read the change log until `ReleaseReads` (see the command). A restore registers its
    // shapes one at a time, and a sequencer reading while it does would consume — and checkpoint
    // past — changes that a shape registered a moment later never sees.
    hold_reads: bool,
    shutdown: crate::shutdown::ShutdownToken,
) -> SequencerHandle {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let processed = Arc::new(std::sync::Mutex::new(start.clone()));
    let stats = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let node_states = Arc::new(std::sync::Mutex::new(HashMap::new()));
    // Registered before the spawn: the shutdown wait must never see "no parties" in the window
    // between deciding to run a sequencer and the task existing.
    let party = shutdown.party("sequencer");
    tokio::spawn(sequencer_loop(
        ds,
        tables,
        start,
        restore_highwater,
        catalog_tx,
        cmd_rx,
        processed.clone(),
        stats.clone(),
        node_states.clone(),
        subq,
        trace_tx,
        arr,
        arr_gates,
        library_mode,
        hold_reads,
        shutdown,
        party,
    ));
    SequencerHandle { cmd_tx, processed, stats, node_states }
}

/// Rebuild + publish the merged node-state map and per-table stats to the sequencer's shared
/// handles and, when anyone is subscribed to `/trace`, broadcast the merged map (plus the
/// subquery registry's summaries) as a `{"type":"state"}` event.
pub(crate) async fn publish_all(
    execs: &HashMap<String, TableExec>,
    offset: &str,
    emitted: &HashMap<String, u64>,
    stats: &std::sync::Mutex<HashMap<TableRef, TableStats>>,
    node_states: &std::sync::Mutex<HashMap<String, NodeStateSummary>>,
    subqueries: &Arc<Mutex<SubqueryRegistry>>,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
) {
    let mut stats_map = HashMap::new();
    let mut merged: HashMap<String, NodeStateSummary> = HashMap::new();
    for exec in execs.values() {
        stats_map.insert(exec.ts.table.clone(), stats_of(exec));
        merged.extend(build_node_states(
            &exec.ts,
            offset,
            exec.envelopes_total,
            &exec.shapes,
            &exec.families,
            &exec.family_of,
            &exec.aggregates,
            &exec.circuit_aggs,
            emitted,
        ));
    }
    *stats.lock().unwrap() = stats_map;
    *node_states.lock().unwrap() = merged.clone();
    if trace_tx.receiver_count() == 0 {
        return;
    }
    let mut ev_nodes = merged;
    for (id, s) in subqueries.lock().await.state_summaries() {
        ev_nodes.insert(id, s);
    }
    if let Ok(json) = serde_json::to_string(&crate::trace::StateEvent::new(ev_nodes)) {
        let _ = trace_tx.send(Arc::new(json));
    }
}

/// Per-table executor state owned by the sequencer: the routing structures a table's changes fan
/// out through, plus any in-flight (pending) shape creations buffering deltas.
pub(crate) struct TableExec {
    pub(crate) ts: TableSchema,
    pub(crate) shapes: HashMap<String, StandaloneShape>,
    pub(crate) shape_index: StandaloneIndex,
    pub(crate) families: HashMap<Vec<usize>, KeyRouter>,
    pub(crate) family_of: HashMap<String, (Vec<usize>, u64, Row)>,
    pub(crate) aggregates: HashMap<String, AggShape>,
    /// Necessary-conjunct index over the aggregates' predicates (same structure as
    /// `shape_index`): per change only candidate aggregates are folded, so aggregate count
    /// stops being a linear per-change term. Match-all / un-indexable predicates land on the
    /// index's scan list and stay always-candidates.
    pub(crate) agg_index: StandaloneIndex,
    /// Circuit-served COUNT aggregates on this table (see [`CircuitAgg`]).
    pub(crate) circuit_aggs: HashMap<String, CircuitAgg>,
    pub(crate) pending: HashMap<String, PendingShape>,
    pub(crate) envelopes_total: u64,
    /// **Library mode only** — the current row per primary key, as last seen on the change log.
    ///
    /// Stays empty (and unallocated) in Postgres mode, because only [`Self::stamp_before_image`]
    /// ever writes to it and the sequencer calls that only when there is no Postgres. In Postgres
    /// mode the system of record supplies the before-image on every envelope (`REPLICA IDENTITY
    /// FULL`) — which is exactly why the engine's hot path holds no table copy. Library mode has
    /// no such source: the native write API takes `(table, op, pk, row)` and a delete/update
    /// carries no prior row, so the retraction half of every Z-set delta would be missing and a
    /// deleted row could never leave a shape.
    ///
    /// Scope: this is the sequencer's view of the change log as it consumes it, and it is EXACT
    /// from boot rather than best-effort. It is not persisted, but it does not need to be — library
    /// mode has no catalog checkpoint to resume from (`apply_catalog` runs only on the Postgres
    /// boot), so a starting process replays the log from `LogPosition::start()` and rebuilds the
    /// whole view before it serves anything. The one reader this cannot serve is
    /// `replay_changes_for_shape`, which reads the log at a DORMANT SHAPE's resume position while
    /// the view is at the head; that path decides membership absolutely instead
    /// (`output::absolute_envelope`).
    pub(crate) library_rows: HashMap<String, serde_json::Value>,
}

impl TableExec {
    pub(crate) fn new(ts: TableSchema) -> TableExec {
        TableExec {
            ts,
            shapes: HashMap::new(),
            shape_index: StandaloneIndex::default(),
            families: HashMap::new(),
            family_of: HashMap::new(),
            aggregates: HashMap::new(),
            agg_index: StandaloneIndex::default(),
            circuit_aggs: HashMap::new(),
            pending: HashMap::new(),
            envelopes_total: 0,
            library_rows: HashMap::new(),
        }
    }

    /// Give a library-mode envelope the before-image a replicated one would already carry, and
    /// record its after-image for the next change to the same key. Called by the sequencer for
    /// every change-log envelope **when the engine runs without Postgres**, before anything else
    /// looks at it, so a library-mode change is indistinguishable from a replicated one from
    /// `apply_envelope` downwards.
    ///
    /// An envelope that already carries an `old` keeps it — a producer that knows the prior row is
    /// always believed over this cache.
    pub(crate) fn stamp_before_image(&mut self, env: &mut Envelope) {
        if env.old.is_none() {
            env.old = self.library_rows.get(&env.key).cloned();
        }
        match env.headers.operation.as_str() {
            "delete" => {
                self.library_rows.remove(&env.key);
            }
            _ => {
                // An insert/update with no row body says nothing about the key's current value;
                // dropping the remembered one would only lose the next retraction.
                if let Some(v) = env.value.as_ref() {
                    self.library_rows.insert(env.key.clone(), v.clone());
                }
            }
        }
    }
}

/// A shape between `BeginShape` and `ActivateShape`: buffers every processed delta of its table so
/// activation can replay exactly what the backfill snapshot did not see (through the gate).
pub(crate) struct PendingShape {
    pub(crate) num_id: u64,
    pub(crate) stream_path: String,
    pub(crate) pred: Arc<CompiledPredicate>,
    pub(crate) out_cols: Option<Arc<Vec<usize>>>,
    pub(crate) kind: CreateKind,
    pub(crate) buffered: Vec<Envelope>,
}

/// Get (or lazily create) the executor for `table`; `None` if `table` is not a known table's
/// **canonical** `schema.name`.
///
/// `execs` is keyed by that canonical string, not by [`TableRef`]: every caller already holds it —
/// the envelope's `type` on the hot path, `cmd.table.as_str()` off it — so the steady-state lookup
/// is a plain `&str` probe with no parse and no allocation.
///
/// **Strict, deliberately.** A non-canonical spelling (a bare `users`) is refused rather than
/// resolved, because resolving it here would be resolved in only HALF the engine: the live fan-out
/// would route it while `replay_changes_for_shape` — the dormant-reactivation / retained-stream
/// catch-up — compares `env.type_ != table.as_str()` and would skip the very same envelopes, so a
/// shape's live stream and its replayed stream would disagree. No legitimate writer produces a
/// non-canonical `type`: the replication ingestor stamps `TableRef::to_string()`, and library-mode
/// writes go through the protocol's `toTableEnvelope`/`canonicalTable`. One that somehow appears
/// falls into the caller's "unknown table" branch — logged, highwater-advanced, dropped.
pub(crate) fn exec_for<'a>(
    execs: &'a mut HashMap<String, TableExec>,
    tables: &SharedTables,
    table: &str,
) -> Option<&'a mut TableExec> {
    if !execs.contains_key(table) {
        // Cold miss: resolve the schema registry (keyed by `TableRef`), but only for a spelling
        // that IS already canonical — `parse` alone would accept the bare-name sugar.
        let tref = TableRef::parse(table).ok().filter(|t| t.as_str() == table)?;
        let ts = tables.read().unwrap().get(&tref).cloned()?;
        execs.insert(tref.to_string(), TableExec::new(ts));
    }
    execs.get_mut(table)
}

/// The engine's single LSN-ordered executor: consumes the global `changes` stream in commit order
/// and dispatches each envelope to its table's executor. Each transaction's shape appends are
/// flushed **before the next transaction is processed**, so every shape stream reflects source
/// transactions atomically and in commit order — cross-table included (Electric's
/// `ShapeLogCollector` pattern; the property the old per-table tailers lost).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sequencer_loop(
    ds: DsClient,
    tables: SharedTables,
    start: LogPosition,
    // The `(lsn, seq)` de-duplication highwater restored with `start` (ADR-0003).
    restore_highwater: Option<(u64, u64)>,
    catalog_tx: CatalogWriter,
    mut cmd_rx: mpsc::UnboundedReceiver<SequencerCmd>,
    processed: Arc<std::sync::Mutex<LogPosition>>,
    stats: Arc<std::sync::Mutex<HashMap<TableRef, TableStats>>>,
    node_states: Arc<std::sync::Mutex<HashMap<String, NodeStateSummary>>>,
    subq: SubqueryHandle,
    trace_tx: tokio::sync::broadcast::Sender<Arc<String>>,
    arr: Option<crate::arrangements::Arrangements>,
    arr_gates: HashMap<TableRef, crate::pg::SnapshotGate>,
    library_mode: bool,
    hold_reads: bool,
    shutdown: crate::shutdown::ShutdownToken,
    // Held for the task's lifetime: dropping it is what tells the shutdown "the sequencer is done".
    _party: crate::shutdown::ShutdownParty,
) {
    let mut execs: HashMap<String, TableExec> = HashMap::new();
    // False until `ReleaseReads` for a held sequencer (see `spawn_sequencer`). Commands are served
    // either way — registering the restored shapes is exactly what a held sequencer is for.
    let mut reading = !hold_reads;
    let mut pos = start;
    // Offset checkpointing: persist the processed position (the restart replay start) at most
    // every ~2s of change — and ALWAYS the moment a segment boundary is crossed, so a restart
    // resumes on the segment the log actually moved to.
    let mut last_ckpt = std::time::Instant::now();
    let mut ckpt_pos = pos.clone();
    // The highwater the last checkpoint carried. While a transaction is HELD the position is
    // pinned, so the position alone stops changing — but transactions completed before the hold
    // keep advancing the highwater, and that progress has to reach the catalog or a crash
    // re-applies them (ADR-0003).
    let mut ckpt_hw: Option<(u64, u64)> = restore_highwater;
    // The rotation pointer this segment carried (ADR-0006). It can arrive in a batch that is not
    // yet flagged `closed` — the pointer append and the close are two requests — so it is
    // remembered until the close actually lands, and the segment is only left once BOTH are in.
    let mut rotate_to: Option<u32> = None;
    // Envelopes appended per shape id — the counters behind the per-node state summaries.
    let mut emitted: HashMap<String, u64> = HashMap::new();
    // De-duplication highwater: the ingestor's delivery is at-least-once (unacknowledged commits
    // re-deliver after a reconnect), and deltas are NOT idempotent for aggregates/subquery
    // weights. Every ingestor envelope carries (commit lsn, seq = position in txn), strictly
    // increasing on the single ordered log, so anything at/below the highwater has already been
    // applied and is skipped. Envelopes without both stamps (library mode) bypass this.
    let mut highwater: Option<(u64, u64)> = restore_highwater;

    // The TRAILING RUN of a page whose last envelope is not marked `headers.last` (ADR-0003).
    //
    // Durable-streams exposes each append atomically, so a commit appended in several chunks
    // arrives as several pages. Splitting on `(txid, lsn)` alone would make chunk 1 look like a
    // whole transaction and flush it to shape streams on its own — a subscriber would see a
    // fraction of a commit, which per-transaction atomic emission forbids. So an unterminated
    // trailing run is HELD here and carried into the next read, and the transaction is processed
    // (and flushed) only once the marker arrives.
    let mut held: Vec<Envelope> = Vec::new();
    // Where a restart must resume while a run is held: the read position from BEFORE the page the
    // held run started in. `processed` (and therefore the checkpoint, and therefore the segment
    // deletion floor) never moves past it, so a crash re-reads the whole held transaction. The read
    // cursor `pos` moves on normally — only what is *published* is pinned.
    let mut held_from: Option<LogPosition> = None;
    // When the current hold started, and how many times it has been reported. A hold is normal and
    // short (the ingestor is appending the next chunk), but it freezes `processed` — the restart
    // point, the convergence barrier and the segment-deletion floor — so a hold that does NOT end
    // has to be visible rather than looking like an idle engine.
    let mut held_since: Option<std::time::Instant> = None;
    let mut held_warnings: u32 = 0;

    loop {
        let (read_path, read_off) = (pos.path(), pos.offset.clone());
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => match cmd {
                Some(SequencerCmd::BeginShape { table, shape_id, num_id, stream_path, pred, out_cols, kind, ack }) => {
                    match exec_for(&mut execs, &tables, table.as_str()) {
                        Some(exec) => {
                            exec.pending.insert(
                                shape_id,
                                PendingShape { num_id, stream_path, pred, out_cols, kind, buffered: Vec::new() },
                            );
                        }
                        None => tracing::error!("begin_shape: unknown table '{table}'"),
                    }
                    let _ = ack.send(());
                }
                Some(SequencerCmd::ActivateShape { table, shape_id, gate, agg_seed, emitted_seed, ready }) => {
                    let res = activate_shape(
                        &ds, &mut execs, &table, &shape_id, gate, agg_seed, emitted_seed, &mut emitted,
                        &shutdown,
                    ).await;
                    if let Err(e) = &res {
                        tracing::error!("activate_shape failed: {e:#}");
                    }
                    let _ = ready.send(res);
                    publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::AbortShape { table, shape_id }) => {
                    if let Some(exec) = execs.get_mut(table.as_str()) {
                        exec.pending.remove(&shape_id);
                    }
                }
                Some(SequencerCmd::DeactivateShape { table, shape_id, resp }) => {
                    // Capture-and-unregister is atomic w.r.t. envelope processing (commands run
                    // between fully-flushed transactions), so the shape's stream is complete up to
                    // the PUBLISHED position — which is the read cursor unless a transaction is
                    // being held mid-append (ADR-0003), in which case it is where that run began.
                    // Handing back the read cursor there would park the shape past a transaction it
                    // never saw, and its reactivation replay would start after those rows. Resuming
                    // from the pin instead re-reads the completed run, which is idempotent: the
                    // replay appends absolute per-pk rows.
                    let gate = execs.get_mut(table.as_str()).and_then(|exec| {
                        if let Some(shape) = exec.shapes.remove(&shape_id) {
                            exec.shape_index.remove(&shape_id);
                            Some(shape.gate)
                        } else if let Some((key_cols, num_id, key_tuple)) = exec.family_of.remove(&shape_id) {
                            let mut gate = None;
                            if let Some(router) = exec.families.get_mut(&key_cols) {
                                if let Some(routed) = router.index.get_mut(&key_tuple) {
                                    if let Some(pos) = routed.iter().position(|rs| rs.num_id == num_id) {
                                        gate = Some(routed.remove(pos).gate);
                                    }
                                    if routed.is_empty() {
                                        router.index.remove(&key_tuple);
                                    }
                                }
                                if router.index.is_empty() {
                                    exec.families.remove(&key_cols);
                                }
                            }
                            gate
                        } else {
                            None // unknown, pending, or an aggregate — not parkable from here
                        }
                    });
                    if gate.is_some() {
                        emitted.remove(&shape_id);
                    }
                    let _ = resp.send(gate.map(|g| (published(&pos, &held_from), g)));
                    publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::RemoveShape { table, shape_id }) => {
                    if let Some(exec) = execs.get_mut(table.as_str()) {
                        exec.pending.remove(&shape_id);
                        if exec.circuit_aggs.remove(&shape_id).is_some() {
                            // a circuit-served COUNT — nothing else to unwind
                        } else if exec.aggregates.remove(&shape_id).is_some() {
                            // an aggregation shape — drop its conjunct-index entry too
                            exec.agg_index.remove(&shape_id);
                        } else if exec.shapes.remove(&shape_id).map(|_| exec.shape_index.remove(&shape_id)).is_none()
                            && let Some((key_cols, num_id, key_tuple)) = exec.family_of.remove(&shape_id)
                            && let Some(router) = exec.families.get_mut(&key_cols)
                        {
                            // Drop the shape from its key's routing list (the shape stream is torn
                            // down elsewhere); discard the router once it routes to no shapes.
                            if let Some(routed) = router.index.get_mut(&key_tuple) {
                                routed.retain(|rs| rs.num_id != num_id);
                                if routed.is_empty() {
                                    router.index.remove(&key_tuple);
                                }
                            }
                            if router.index.is_empty() {
                                exec.families.remove(&key_cols);
                            }
                        }
                    }
                    emitted.remove(&shape_id);
                    publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::ResetTable { table }) => {
                    if execs.remove(table.as_str()).is_some() {
                        tracing::warn!("sequencer: dropped the executor for '{table}' (schema drift)");
                    }
                    publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::CreateCircuitAgg { table, shape_id, stream_path, constraints, ready }) => {
                    let res = create_circuit_agg(
                        &ds, arr.as_ref(), &mut execs, &tables, &table, &shape_id, &stream_path, constraints,
                        &shutdown,
                    )
                    .await;
                    if res.is_ok() {
                        emitted.insert(shape_id.clone(), 1);
                    }
                    let _ = ready.send(res);
                    publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                }
                Some(SequencerCmd::ReleaseReads { done }) => {
                    reading = true;
                    let _ = done.send(());
                }
                Some(SequencerCmd::Discard { done }) => {
                    tracing::info!("sequencer: discarded before it read anything (a catalog restore was rolled back)");
                    let _ = done.send(());
                    break;
                }
                Some(SequencerCmd::DumpNode { table, node_id, resp }) => {
                    let val =
                        execs.get(table.as_str()).and_then(|exec| dump_node_json(exec, &pos.to_string(), &emitted, &node_id));
                    let _ = resp.send(val);
                }
                Some(SequencerCmd::MemBytes { resp }) => {
                    // On-demand only (see the variant's doc comment): this sums `exec_heap_bytes`
                    // across every live table executor, which is exactly the walk `stats_of` used
                    // to do on every processed batch before it moved here.
                    let total: usize = execs.values().map(exec_heap_bytes).sum();
                    let _ = resp.send(total);
                }
                None => break,
            },
            // Graceful shutdown. Placed AFTER commands (so anything already queued is still
            // answered — a create waiting on `ready` gets its answer rather than a dropped channel)
            // and BEFORE the read, so the change-log long-poll below is abandoned at once instead
            // of holding the pod's termination grace for its full window. Abandoning a read loses
            // nothing: `pos` is only advanced inside the branch body, so the next boot re-reads
            // from the checkpoint written just below.
            _ = shutdown.wait() => {
                tracing::info!("sequencer: shutdown requested; checkpointing at {}", published(&pos, &held_from));
                break;
            }
            res = ds.read(&read_path, &read_off, true), if reading => match res {
                Ok(rr) => {
                    let next = rr.next_offset.clone();
                    // How much this read delivered, BEFORE control envelopes are filtered out: a
                    // closed segment is only left once a read comes back empty, which is the proof
                    // that everything in it has been consumed.
                    let delivered = rr.envelopes.len();
                    let advanced = rr.next_offset.as_deref().is_some_and(|n| n != pos.offset);
                    if let Some(n) = rr.next_offset { pos.offset = n; }
                    // Change-log CONTROL envelopes (the rotation pointer, ADR-0006) are routing
                    // metadata, not data: they are recognised by TYPE and removed here —
                    // UNCONDITIONALLY, before anything (transaction splitting, the arrangements
                    // feed, `exec_for`, `process_envelope`) looks at the batch. Not "only when a
                    // pointer is present": any control envelope, recognised or not, is ours and
                    // must never be mistaken for a table's change.
                    let mut envs = rr.envelopes;
                    if let Some(n) = crate::changelog::rotation_target_in(&envs) {
                        rotate_to = Some(n);
                    }
                    envs.retain(|e| !crate::changelog::is_control(e));
                    // Re-attach the run held from an earlier page (ADR-0003).
                    //
                    // A re-delivery complicates this: when the ingestor fails part-way through a
                    // chunked commit it acknowledges nothing, so Postgres re-sends that whole
                    // transaction — and, because acknowledgements are flushed on an interval, it
                    // can re-send earlier COMPLETE commits ahead of it too. So the "already held,
                    // skip it" filter applies to the LEADING run of this page and only while that
                    // run is the held transaction: `seq` is the running index over one transaction,
                    // so it is meaningless against any other one, and applying it page-wide would
                    // silently drop whole transactions whose seqs happen to be lower.
                    let held_key = held.last().map(run_key_owned);
                    let mut merged_from = None;
                    if let Some(key) = held_key {
                        let lead = envs.iter().take_while(|e| run_key(e) == key_ref(&key)).count();
                        if lead == 0 && !envs.is_empty() {
                            // The held transaction is not what came next. Two things produce that,
                            // and neither can ever complete this fragment: a reconnect re-delivering
                            // earlier complete commits first (the held transaction's own
                            // re-delivery follows in full, and the complete ones are skipped by the
                            // `(lsn, seq)` highwater if already applied), and an epoch reset, which
                            // abandons the fragment outright. Discarding it is the only correct
                            // move — emitting a fragment of a transaction is what the marker exists
                            // to prevent.
                            tracing::warn!(
                                "sequencer: the held (incomplete) transaction {:?} was followed by a different one \
                                 {:?}; discarding the fragment — it will arrive again in full (a reconnect \
                                 re-delivers earlier complete commits first) or has been abandoned (epoch reset)",
                                key,
                                run_key_owned(&envs[0]),
                            );
                            metrics().sequencer_orphan_fragments.fetch_add(1, Ordering::Relaxed);
                            held.clear();
                            held_from = None;
                            held_since = None;
                            held_warnings = 0;
                            metrics().sequencer_held_run.store(0, Ordering::Relaxed);
                        } else {
                            // Only the leading run is filtered; everything after the first envelope
                            // of a different transaction passes through untouched.
                            let last_seq = held.last().and_then(|e| e.headers.seq);
                            let mut rest = envs.split_off(lead);
                            envs.retain(|e| match (e.headers.seq, last_seq) {
                                (Some(seq), Some(hs)) => seq > hs,
                                _ => true,
                            });
                            envs.append(&mut rest);
                            let mut merged = std::mem::take(&mut held);
                            merged.append(&mut envs);
                            envs = merged;
                            merged_from = Some(key);
                        }
                    }
                    // Hold back a TRAILING run that is not terminated by the transaction-end marker
                    // (ADR-0003): it is a commit whose remaining chunks have not been appended yet.
                    // Everything before it is complete and processed now.
                    match unterminated_tail(&envs) {
                        Some(cut) => {
                            // Is this the SAME transaction that was already held, or a new one that
                            // starts after the held one completed on this page? A new hold must
                            // re-pin to THIS page, or a catch-up over consecutive chunked commits
                            // would keep the pin (and the checkpoint) frozen at the first page for
                            // the whole run.
                            let continuation = merged_from.as_ref().is_some_and(|k| run_key(&envs[cut]) == key_ref(k));
                            if !continuation {
                                held_from = Some(LogPosition { segment: pos.segment, offset: read_off.clone() });
                                held_since = Some(std::time::Instant::now());
                                held_warnings = 0;
                            }
                            held = envs.split_off(cut);
                            metrics().sequencer_held_run.store(1, Ordering::Relaxed);
                            let since = *held_since.get_or_insert_with(std::time::Instant::now);
                            // A hold longer than a minute is no longer "the next chunk is coming":
                            // ingest has stalled mid-transaction, and everything downstream of
                            // `processed` is frozen with it. Say so, then once a minute after that.
                            if since.elapsed() >= HELD_RUN_WARN_AFTER * (held_warnings + 1) {
                                held_warnings += 1;
                                tracing::warn!(
                                    "sequencer: transaction {:?} at lsn {:?} has been incomplete on the change \
                                     log for {:?} ({} envelope(s) held); the change-log position is pinned at \
                                     {} until its final chunk arrives",
                                    held.first().and_then(|e| e.headers.txid.clone()),
                                    held.first().and_then(|e| e.headers.lsn.clone()),
                                    since.elapsed(),
                                    held.len(),
                                    published(&pos, &held_from),
                                );
                            }
                        }
                        // Nothing is held any more (the marker arrived, or never was one): release
                        // the pin so `processed`, the checkpoint and the deletion floor move again.
                        // Safe here — the run it was pinning is processed and flushed below, before
                        // anything is published.
                        None => {
                            held_from = None;
                            held_since = None;
                            held_warnings = 0;
                            metrics().sequencer_held_run.store(0, Ordering::Relaxed);
                        }
                    }
                    // Split the read batch into transactions (runs of equal (txid, lsn) — the
                    // ingestor appends whole commits contiguously, in commit order) and flush each
                    // transaction's appends before processing the next: atomic per-transaction
                    // emission, across tables. Every run left here is COMPLETE: it ends on an
                    // envelope carrying `headers.last`, so a commit appended in several chunks
                    // (ADR-0003) is processed once, whole, not chunk by chunk.
                    let mut touched = false;
                    let mut i = 0;
                    while i < envs.len() {
                        let txid = envs[i].headers.txid.clone();
                        let lsn = envs[i].headers.lsn.clone();
                        let mut j = i + 1;
                        while j < envs.len() && envs[j].headers.txid == txid && envs[j].headers.lsn == lsn {
                            j += 1;
                        }
                        // Feed this transaction into the dbsp counts pipelines and step the
                        // circuit BEFORE fanning it out, so circuit-served aggregates emit
                        // within the transaction that changed them. The counts layer re-checks
                        // its own (lsn, seq) highwater, so feeding pre-dedup envelopes is safe.
                        let txn_count_deltas = if let Some(arr) = &arr {
                            let deltas: Vec<_> = envs[i..j]
                                .iter()
                                .filter_map(|env| stamped_delta_for_arrangements(&tables, arr, &arr_gates, env))
                                .collect();
                            arr.apply_batch(deltas).await
                        } else {
                            Vec::new()
                        };
                        let mut txn_pending: HashMap<String, Vec<Envelope>> = HashMap::new();
                        for k in i..j {
                            // Skip redelivered changes (see `highwater` above).
                            let pos = match (envs[k].headers.lsn.as_deref(), envs[k].headers.seq) {
                                (Some(l), Some(seq)) => Some((crate::pg::lsn_to_u64(l), seq)),
                                _ => None,
                            };
                            if let (Some(p), Some(hw)) = (pos, highwater) {
                                if p <= hw {
                                    tracing::debug!("sequencer: skipping duplicate change at {p:?}");
                                    continue;
                                }
                            }
                            let Some(exec) = exec_for(&mut execs, &tables, &envs[k].type_) else {
                                tracing::error!("sequencer: change for unknown table '{}'", envs[k].type_);
                                if let Some(p) = pos { highwater = Some(p); }
                                continue;
                            };
                            // LIBRARY MODE: no Postgres wrote this change, so nothing stamped a
                            // before-image on it. Fill it in from the sequencer's own per-key view
                            // (`TableExec::library_rows`) HERE — after the de-duplication highwater
                            // (a re-delivered duplicate must never fold into the view twice) and
                            // ahead of everything downstream: the pending-shape buffers, the
                            // fan-out, the aggregate folds. From this point the envelope is
                            // indistinguishable from a replicated one.
                            //
                            // The counts pipelines were fed above, from the un-stamped page. That
                            // is not a gap: the circuit tier seeds from a Postgres snapshot and
                            // refuses to start without a `pg_url`, so `arr` is always `None` here
                            // in library mode.
                            if library_mode {
                                exec.stamp_before_image(&mut envs[k]);
                            }
                            // Buffer for in-flight creations on this table: their `BeginShape` was
                            // acknowledged before the creator's snapshot, so everything the
                            // snapshot cannot contain lands in the buffer.
                            for pending in exec.pending.values_mut() {
                                pending.buffered.push(envs[k].clone());
                            }
                            if let Err(e) = process_envelope(
                                &exec.ts, &exec.shapes, &exec.shape_index, &exec.families,
                                &mut exec.aggregates, &exec.agg_index, envs[k].clone(), &mut txn_pending,
                                &subq, &trace_tx, library_mode,
                            )
                            .await
                            {
                                tracing::error!("process_envelope failed: {e:#}");
                            }
                            exec.envelopes_total += 1;
                            touched = true;
                            if let Some(p) = pos {
                                highwater = Some(p);
                            }
                        }
                        // Counts pipeline → circuit-served aggregates.
                        if !txn_count_deltas.is_empty() {
                            apply_count_deltas(
                                &mut execs, txn_count_deltas, txid.clone(), lsn.clone(), &mut txn_pending,
                                &trace_tx,
                            );
                        }
                        emit_storage_txn_metrics(&txn_pending);
                        for (path, envs) in &txn_pending {
                            *emitted.entry(sid_of_path(path).to_string()).or_insert(0) += envs.len() as u64;
                        }
                        // Transaction boundary: every append of this commit lands before the next
                        // commit is processed.
                        flush_pending(&ds, txn_pending).await;
                        i = j;
                    }
                    // Publish the processed position only after the whole batch is fanned out +
                    // flushed — and never past a run still being HELD (ADR-0003): a restart must
                    // re-read the whole incomplete transaction, and `processed` is also what
                    // `GET /tables/{name}/offset` and the segment-deletion floor read.
                    if next.is_some() {
                        *processed.lock().unwrap() = published(&pos, &held_from);
                    }
                    // The segment ended (ADR-0006). Cross only once a read of it comes back EMPTY
                    // (or stops advancing): "closed" can arrive alongside a page of data, and
                    // leaving on that page would skip whatever a partial page left behind. A closed
                    // stream answers a long-poll instantly, so the confirming read costs one round
                    // trip per rotation. The crossing is checkpointed immediately — a 2 s-lazy
                    // checkpoint still naming the closed segment's tail would make a restart
                    // re-derive it.
                    let mut crossed = false;
                    if rr.closed && (delivered == 0 || !advanced) {
                        match rotate_to.take() {
                            Some(n) => {
                                tracing::info!("sequencer: {} closed; continuing on {}", pos.path(), segment_path(n));
                                pos = LogPosition::start_of(n);
                                *processed.lock().unwrap() = published(&pos, &held_from);
                                crossed = true;
                            }
                            None => {
                                // No pointer in this run: the checkpoint this process resumed from
                                // was already past it, or storage lost it. Step to EXACTLY the next
                                // segment, verified to exist — never a walk to the first OPEN one,
                                // which would skip every closed segment in between and with it a
                                // whole span of unread changes.
                                match crate::changelog::next_segment_for_reader(&ds, pos.segment).await {
                                    Ok(n) => {
                                        tracing::warn!(
                                            "sequencer: {} is closed and carried no rotation pointer in this run; \
                                             stepping to {}",
                                            pos.path(),
                                            segment_path(n)
                                        );
                                        pos = LogPosition::start_of(n);
                                        *processed.lock().unwrap() = published(&pos, &held_from);
                                        crossed = true;
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            "sequencer: cannot leave the closed segment {}: {e:#}. Backing off; \
                                             nothing is skipped.",
                                            pos.path()
                                        );
                                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                    }
                                }
                            }
                        }
                    }
                    let ckpt = published(&pos, &held_from);
                    // Either half moving is progress worth persisting: the position, or — while the
                    // position is pinned behind a held run — the de-duplication highwater.
                    let ckpt_moved = ckpt != ckpt_pos || highwater != ckpt_hw;
                    if crossed || (next.is_some() && ckpt_moved && last_ckpt.elapsed() >= std::time::Duration::from_secs(2)) {
                        ckpt_pos = ckpt.clone();
                        ckpt_hw = highwater;
                        last_ckpt = std::time::Instant::now();
                        // The de-duplication highwater rides WITH the position (ADR-0003): a crash
                        // after a prefix of a chunked commit was applied and checkpointed would
                        // otherwise re-apply that prefix on re-delivery, and aggregate/subquery
                        // weights are not idempotent under duplicates.
                        catalog_tx.send(CatalogEvent::Offset { pos: ckpt, highwater });
                    }
                    if touched {
                        publish_all(&execs, &pos.to_string(), &emitted, &stats, &node_states, &subq.registry, &trace_tx).await;
                    }
                }
                Err(e) if crate::ds::is_stream_gone(&e) => {
                    // The segment the sequencer is reading has been DELETED under it. The sweeper
                    // deletes only below the durable checkpoint, and the boot refuses to start on a
                    // missing segment, so this is unreachable by design — say so at ERROR and back
                    // off rather than spin quietly at 5 Hz on a 404.
                    tracing::error!(
                        "sequencer: the change-log segment it is reading is GONE ({e:#}). Nothing may delete a \
                         segment the durable checkpoint has not passed, so storage has lost data or something \
                         outside the engine deleted it; the sequencer cannot advance. Backing off.",
                    );
                    // Cut short by a shutdown: this backoff is dead time, and spending 5 s of a
                    // 25 s grace on it would be for nothing (the loop re-selects and breaks anyway).
                    back_off(&shutdown, std::time::Duration::from_secs(5)).await;
                }
                Err(e) => {
                    tracing::warn!("sequencer read error on {read_path}: {e:#}; backing off");
                    back_off(&shutdown, std::time::Duration::from_millis(200)).await;
                }
            },
        }
    }
    // The loop is only left on shutdown (or on the command channel closing, i.e. the engine going
    // away). Either way the batch it was in the middle of is fully fanned out and flushed — the
    // select only chooses at the TOP of the loop — so one last `Offset` makes the position (and the
    // de-duplication highwater riding with it) durable. Without it, everything since the last lazy
    // 2 s checkpoint would be replayed on the next boot: correct, but a needless storm, and for a
    // held run it would also re-read a transaction the ingestor never finished.
    //
    // A sequencer that never read has nothing to record: its position is still the one it was
    // started from, which the catalog already holds, and a restore being rolled back must not
    // append anything on its way out.
    if !reading {
        tracing::info!("sequencer: stopped before reading the change log; no checkpoint to write");
        return;
    }
    let ckpt = published(&pos, &held_from);
    catalog_tx.send(CatalogEvent::Offset { pos: ckpt.clone(), highwater });
    tracing::info!("sequencer: stopped at {ckpt} (highwater {highwater:?})");
}

/// Wait out a read-error backoff, cut short by a shutdown. The backoff exists to stop a failing
/// read spinning; once the process is going away there is nothing left to pace, and sleeping it out
/// would spend the termination grace on dead time.
async fn back_off(shutdown: &crate::shutdown::ShutdownToken, d: std::time::Duration) {
    tokio::select! {
        _ = tokio::time::sleep(d) => {}
        _ = shutdown.wait() => {}
    }
}

/// How long a transaction may stay incomplete on the change log before the sequencer says so (and
/// again every interval after that). Long enough that appending the next chunk of a genuinely huge
/// commit is never reported, short enough that a stalled ingest is not silent.
const HELD_RUN_WARN_AFTER: std::time::Duration = std::time::Duration::from_secs(60);

/// The position a HELD run pins publication to: while a transaction is only partly on the log
/// (ADR-0003), `processed` — and therefore the restart point, `GET /tables/{name}/offset`, and the
/// segment-deletion floor — stays at the page the held run began in. The read cursor moves on
/// regardless; only what is published is pinned.
fn published(pos: &LogPosition, held_from: &Option<LogPosition>) -> LogPosition {
    held_from.clone().unwrap_or_else(|| pos.clone())
}

/// The `(txid, lsn)` pair identifying the transaction an envelope belongs to — the key the change
/// log's runs are split on.
fn run_key(env: &Envelope) -> (Option<&str>, Option<&str>) {
    (env.headers.txid.as_deref(), env.headers.lsn.as_deref())
}

/// [`run_key`], owned — needed to remember a held run's identity across the move that re-attaches it.
fn run_key_owned(env: &Envelope) -> (Option<String>, Option<String>) {
    (env.headers.txid.clone(), env.headers.lsn.clone())
}

/// Borrow an owned run key back for comparison with [`run_key`].
fn key_ref(k: &(Option<String>, Option<String>)) -> (Option<&str>, Option<&str>) {
    (k.0.as_deref(), k.1.as_deref())
}

/// The index at which a page's trailing, INCOMPLETE transaction begins — `None` when the page ends
/// on a complete one, or carries nothing that could be incomplete.
///
/// "Complete" is the ingestor's transaction-end marker (`headers.last`), set on the final envelope
/// of the final chunk of every commit (ADR-0003). Without it a commit appended in several chunks
/// would be processed chunk by chunk, and each chunk flushed to shape streams as if it were a whole
/// transaction.
///
/// An envelope with no `seq` is NOT held: only the ingestor structures its envelopes into
/// transactions, and a library-mode write is a one-envelope transaction. Holding one would wait for
/// a marker no producer is going to send.
fn unterminated_tail(envs: &[Envelope]) -> Option<usize> {
    let last = envs.last()?;
    if last.headers.last == Some(true) {
        return None;
    }
    last.headers.seq?;
    let (txid, lsn) = (&last.headers.txid, &last.headers.lsn);
    let mut i = envs.len() - 1;
    while i > 0 && (&envs[i - 1].headers.txid, &envs[i - 1].headers.lsn) == (txid, lsn) {
        i -= 1;
    }
    Some(i)
}

/// Make a pending shape live: register its routing, then replay its buffered deltas through the
/// snapshot gate — emitting exactly the changes the backfill snapshot did not see. The buffered
/// replay is appended before the sequencer processes any further change, so the shape stream stays
/// in commit order.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn activate_shape(
    ds: &DsClient,
    execs: &mut HashMap<String, TableExec>,
    table: &TableRef,
    shape_id: &str,
    gate: crate::pg::SnapshotGate,
    agg_seed: Option<AggSeed>,
    emitted_seed: u64,
    emitted: &mut HashMap<String, u64>,
    shutdown: &crate::shutdown::ShutdownToken,
) -> Result<()> {
    let exec = execs.get_mut(table.as_str()).with_context(|| format!("no executor for table '{table}'"))?;
    let p = exec.pending.remove(shape_id).with_context(|| format!("no pending shape '{shape_id}' (aborted?)"))?;
    if emitted_seed > 0 {
        emitted.insert(shape_id.to_string(), emitted_seed);
    }
    match p.kind {
        CreateKind::Plain => {
            // Register routing first (an equality template joins/creates its family's KeyRouter;
            // everything else is a standalone indexed filter)...
            match p.pred.equality_template() {
                Some(pairs) => {
                    let key_cols: Vec<usize> = pairs.iter().map(|(c, _)| *c).collect();
                    let key_tuple = Row(pairs.into_iter().map(|(_, v)| v).collect());
                    let router = exec
                        .families
                        .entry(key_cols.clone())
                        .or_insert_with(|| KeyRouter { key_cols: key_cols.clone(), index: HashMap::new() });
                    router.index.entry(key_tuple.clone()).or_default().push(RoutedShape {
                        num_id: p.num_id,
                        stream_path: p.stream_path.clone(),
                        gate: gate.clone(),
                        out_cols: p.out_cols.clone(),
                    });
                    exec.family_of.insert(shape_id.to_string(), (key_cols, p.num_id, key_tuple));
                }
                None => {
                    exec.shape_index.insert(shape_id, &p.pred);
                    exec.shapes.insert(
                        shape_id.to_string(),
                        StandaloneShape {
                            pred: p.pred.clone(),
                            stream_path: p.stream_path.clone(),
                            gate: gate.clone(),
                            out_cols: p.out_cols.clone(),
                        },
                    );
                }
            }
            // ...then drain the buffer through the gate. `matches()` evaluates equality templates
            // and standalone predicates alike, so one replay path covers both placements.
            let mut outs: Vec<Envelope> = Vec::new();
            for env in &p.buffered {
                let Ok((delta, txid, lsn)) = apply_envelope(&exec.ts, env) else { continue };
                if delta.is_empty() {
                    continue;
                }
                let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
                let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
                if gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                let matched = eval_standalone(&p.pred, &delta);
                if matched.is_empty() {
                    continue;
                }
                outs.extend(translate_output(&exec.ts, matched, txid, lsn, p.out_cols.as_deref().map(Vec::as_slice)));
            }
            if !outs.is_empty() {
                *emitted.entry(shape_id.to_string()).or_insert(0) += outs.len() as u64;
                ds.append_reliable(&p.stream_path, &outs).await;
            }
        }
        CreateKind::Aggregate { func, col } => {
            // Adopt the fold the creator built while STREAMING the backfill (no backfill rows ever
            // existed in one place), emit the initial value, then fold the gated buffer (emitting a
            // value envelope whenever the aggregate moves).
            let seed = agg_seed.unwrap_or_default();
            let mut agg = AggShape {
                pred: p.pred.clone(),
                func,
                col,
                stream_path: p.stream_path.clone(),
                gate: gate.clone(),
                count: seed.count,
                nn_count: seed.nn_count,
                sum: seed.sum,
                multiset: seed.multiset,
                last: None,
            };
            let mut outs = vec![agg.envelope(&exec.ts, None, None)];
            agg.last = Some(agg.value());
            for env in &p.buffered {
                let Ok((delta, txid, lsn)) = apply_envelope(&exec.ts, env) else { continue };
                if delta.is_empty() {
                    continue;
                }
                let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
                let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
                if gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                if agg.apply(&delta) {
                    let val = agg.value();
                    if agg.last.as_ref() != Some(&val) {
                        agg.last = Some(val.clone());
                        outs.push(agg.envelope(&exec.ts, txid, lsn));
                    }
                }
            }
            *emitted.entry(shape_id.to_string()).or_insert(0) += outs.len() as u64;
            // Retried, not propagated on the first failure: at RESTORE this append's error fails the
            // whole restore (ADR-0009), and one transient 503 must not cost a boot attempt — it once
            // cost the acknowledged aggregate itself.
            ds.append_retrying(&p.stream_path, &outs, DsClient::RESTORE_APPEND_BUDGET, shutdown).await?;
            exec.agg_index.insert(shape_id, &agg.pred);
            exec.aggregates.insert(shape_id.to_string(), agg);
        }
    }
    Ok(())
}

/// Replay the global change log from `from` for one dormant shape: apply each of its table's
/// envelopes through the shape's snapshot gate + predicate + projection and append the matches to
/// the retained stream. Pages until the log reports up-to-date on the OPEN segment, following each
/// closed segment's rotation pointer on the way (ADR-0006) exactly as the live loop does. Appends
/// are direct (`ds.append`): a retired stream (404/410/closed) means it vanished (evicted/purged
/// mid-replay) and must fail the resume.
///
/// A resume segment that is GONE (the sweeper deleted it — which it only does once nothing can
/// resume inside it, so this should be unreachable) surfaces as a read error, which fails the
/// resume and drops the shape. That is the right outcome: a shape whose replay start no longer
/// exists can never be brought up to date, and its subscribers must recreate it.
///
/// It deliberately does NOT hold back an unterminated transaction the way the live loop does
/// (ADR-0003). Nothing here depends on transaction boundaries: it filters by table through the
/// shape's snapshot gate and appends **absolute per-pk** rows (`upsert`/`delete` by key), so a
/// partly-appended commit produces a prefix of the same absolute rows and the next page produces
/// the rest — no delta is counted twice and no intermediate state is wrong, only briefly
/// incomplete, on a stream the shape is not live on yet. The live loop's rule is what governs from
/// the moment the shape is registered.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn replay_changes_for_shape(
    ds: &DsClient,
    ts: &TableSchema,
    table: &TableRef,
    pred: &CompiledPredicate,
    out_cols: Option<&Arc<Vec<usize>>>,
    gate: &crate::pg::SnapshotGate,
    stream_path: &str,
    from: &LogPosition,
    library_mode: bool,
    shutdown: &crate::shutdown::ShutdownToken,
) -> Result<u64> {
    let mut pos = from.clone();
    let mut rotate_to: Option<u32> = None;
    let mut emitted = 0u64;
    loop {
        let rr = ds
            .read(&pos.path(), &pos.offset, false)
            .await
            .with_context(|| format!("replaying the change log from {pos}"))?;
        if let Some(n) = crate::changelog::rotation_target_in(&rr.envelopes) {
            rotate_to = Some(n);
        }
        let delivered = rr.envelopes.len();
        let mut outs: Vec<Envelope> = Vec::new();
        for env in &rr.envelopes {
            // The change log's `type` is the canonical `schema.name`; a control envelope's never
            // is, so it is skipped by TYPE here as everywhere else.
            if env.type_ != table.as_str() {
                continue;
            }
            let Ok((delta, txid, lsn)) = apply_envelope(ts, env) else { continue };
            // Library mode replays RAW change-log envelopes: nothing stamped a before-image on
            // them (the sequencer's per-key view is the state at the log's HEAD, not at this
            // replay position), so membership is decided ABSOLUTELY per pk — matches now ⇒
            // `upsert`, otherwise ⇒ `delete <key>`. Without this a shape coming back from
            // dormancy would never learn about the deletes it slept through.
            let absolute = library_mode && needs_absolute_emission(env);
            if delta.is_empty() && !absolute {
                continue;
            }
            let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
            let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
            if gate.should_skip(lsn_u64, xid) {
                continue;
            }
            if absolute {
                let held = delta.iter().find(|Tup2(_, w)| *w > 0).map(|Tup2(r, _)| r).filter(|r| pred.matches(r));
                if let Some(e) = absolute_envelope(ts, &env.key, held, txid, lsn, out_cols.map(|c| c.as_slice())) {
                    outs.push(e);
                }
                continue;
            }
            let matched = eval_standalone(pred, &delta);
            if matched.is_empty() {
                continue;
            }
            outs.extend(translate_output(ts, matched, txid, lsn, out_cols.map(|c| c.as_slice())));
        }
        if !outs.is_empty() {
            emitted += outs.len() as u64;
            // A reactivation that fails is EVICTED (its subscribers lose the shape), so a transient
            // storage failure is retried rather than propagated — same rule as the aggregate re-seed.
            ds.append_retrying(stream_path, &outs, DsClient::RESTORE_APPEND_BUDGET, shutdown)
                .await
                .context("append replay to retained stream")?;
        }
        let advanced = rr.next_offset.as_deref().is_some_and(|n| n != pos.offset);
        if let Some(n) = rr.next_offset {
            pos.offset = n;
        }
        if rr.closed && (delivered == 0 || !advanced) {
            // The segment is drained AND closed: follow the pointer, exactly as the live loop does.
            let next = match rotate_to.take() {
                Some(n) => n,
                // No pointer seen on this page: step to EXACTLY the next segment (verified to
                // exist). A walk to the first open segment would skip the closed ones in between,
                // and this replay is precisely what has to read them.
                None => crate::changelog::next_segment_for_reader(ds, pos.segment).await?,
            };
            pos = LogPosition::start_of(next);
            continue;
        }
        if !advanced {
            break; // the segment stopped advancing and is still open: nothing more to replay
        }
        if rr.up_to_date && !rr.closed {
            break;
        }
    }
    Ok(emitted)
}

/// Creator-side half of the two-phase shape creation: await the pending-buffer ack, **stream** the
/// Postgres backfill on a pooled connection (appending it chunk by chunk for a plain shape, folding
/// it for an aggregate), then activate. The sequencer keeps processing other work the whole time —
/// a slow backfill only delays THIS shape. Returns the creation outcome, with its cause kept typed:
/// the catalog restore hands it to the boot's retry-or-refuse decision (ADR-0009).
///
/// **Nothing here ever holds a whole backfill.** The snapshot arrives in chunks bounded by
/// `ELECTRIC_CIRCUITS_BACKFILL_APPEND_BYTES`, and each chunk is appended (or folded) and dropped
/// before the next is read, so the creator's memory is one chunk for a table of any size. Chunking
/// needs no protocol change: the shape is PENDING until `ActivateShape` lands, so a partly-appended
/// snapshot is on a stream no subscriber is reading yet, and a failure aborts the pending shape and
/// rolls the whole creation back exactly as before.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn backfill_and_activate(
    ds: &DsClient,
    pg_url: &Option<String>,
    cmd_tx: &mpsc::UnboundedSender<SequencerCmd>,
    ts: &TableSchema,
    table: &TableRef,
    shape_id: &str,
    stream_path: &str,
    pred: &Arc<CompiledPredicate>,
    out_cols: Option<&Arc<Vec<usize>>>,
    changes_only: bool,
    // `Some((func, col))` makes this an aggregate: the backfill is FOLDED into an `AggSeed` rather
    // than appended as snapshot envelopes.
    aggregate: Option<(AggFn, Option<usize>)>,
    shutdown: &crate::shutdown::ShutdownToken,
    ack_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    let abort = || {
        let _ = cmd_tx.send(SequencerCmd::AbortShape { table: table.clone(), shape_id: shape_id.to_string() });
    };
    if ack_rx.await.is_err() {
        bail!("sequencer dropped the begin-shape ack");
    }
    // Backfill: current matching rows from a REPEATABLE READ snapshot, predicate pushed into the
    // SELECT; `matches()` is the final authority (a safety net if the SQL is ever a looser
    // superset). A `changes_only` feed skips the backfill and forwards only future matches
    // (passthrough gate) — the non-materialized live tail a subset query follows.
    let (gate, agg_seed, emitted_seed) = if changes_only {
        (crate::pg::SnapshotGate::passthrough(), None, 0u64)
    } else {
        let t0 = std::time::Instant::now();
        match stream_backfill(ds, pg_url, ts, pred, out_cols, stream_path, aggregate, shutdown, t0).await {
            Ok(v) => v,
            Err(e) => {
                abort();
                return Err(e);
            }
        }
    };
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    if cmd_tx
        .send(SequencerCmd::ActivateShape {
            table: table.clone(),
            shape_id: shape_id.to_string(),
            gate,
            agg_seed,
            emitted_seed,
            ready: ready_tx,
        })
        .is_err()
    {
        bail!("sequencer is gone");
    }
    ready_rx.await.unwrap_or_else(|_| Err(anyhow::anyhow!("sequencer dropped the ready channel")))
}

/// What a create is refused with when a shutdown interrupts it. The client's move is to retry
/// against the next process, so it reads as a retry instruction, not as a failure of the request.
pub(crate) const SHUTTING_DOWN: &str = "engine is shutting down; retry the create against the next process";

/// Stream one shape's backfill: chunk in, chunk out, nothing accumulated.
///
/// For a plain shape each chunk is filtered, translated and appended to the (still pending) shape
/// stream; for an aggregate each chunk is folded into the seed. Returns the snapshot gate, the
/// aggregate seed, and how many envelopes the snapshot appended (which seeds the shape's emit
/// counter).
#[allow(clippy::too_many_arguments)]
async fn stream_backfill(
    ds: &DsClient,
    pg_url: &Option<String>,
    ts: &TableSchema,
    pred: &Arc<CompiledPredicate>,
    out_cols: Option<&Arc<Vec<usize>>>,
    stream_path: &str,
    aggregate: Option<(AggFn, Option<usize>)>,
    shutdown: &crate::shutdown::ShutdownToken,
    t0: std::time::Instant,
) -> Result<(crate::pg::SnapshotGate, Option<AggSeed>, u64)> {
    // Library/no-source mode: the shape simply starts empty (and an aggregate starts at its
    // empty-set value), exactly as the materialising version did.
    let Some(url) = pg_url.as_deref() else {
        return Ok((crate::pg::SnapshotGate::passthrough(), aggregate.map(|_| AggSeed::default()), 0));
    };
    let client = crate::pg::pool_for(url).get().await?;
    let mut reader = crate::pg::backfill_reader(&client, ts, Some(pred.as_ref())).await?;

    let mut agg_seed = aggregate.map(|_| AggSeed::default());
    let mut rows_total = 0u64;
    let mut snapshot_bytes = 0u64;
    let mut emitted_seed = 0u64;
    let mut appends = 0u64;
    loop {
        // A chunk boundary is this loop's safe point. Without it a backfill over a large table is
        // an un-interruptible span of the termination grace — the exact shape of the boot's
        // un-raced connect. Aborting costs nothing: the shape is still PENDING, so the caller's
        // rollback removes the partly-appended stream and the client simply creates it again.
        if shutdown.is_shutting_down() {
            bail!(SHUTTING_DOWN);
        }
        let Some(chunk) = reader.next_chunk().await? else { break };
        rows_total += chunk.len() as u64;
        match (&mut agg_seed, aggregate) {
            // The sequencer no longer receives rows for an aggregate: the fold happens here, one
            // chunk at a time, and the chunk is dropped.
            (Some(seed), Some((func, col))) => seed.fold_rows(pred.as_ref(), func, col, &chunk),
            _ => {
                let out: Vec<(Row, ZWeight)> = chunk.into_iter().filter(|r| pred.matches(r)).map(|r| (r, 1)).collect();
                if out.is_empty() {
                    continue;
                }
                let envs = translate_output(ts, out, None, None, out_cols.map(|c| c.as_slice()));
                if crate::statsd::enabled() {
                    snapshot_bytes += envs_bytes(&envs);
                }
                emitted_seed += envs.len() as u64;
                if let Err(e) = ds.append(stream_path, &envs).await {
                    return Err(e.context("append snapshot"));
                }
                appends += 1;
            }
        }
    }
    // A backfill that fit in one append contributes 0, same accounting as a chunked commit's.
    if appends > 1 {
        metrics().backfill_chunked_appends.fetch_add(appends, Ordering::Relaxed);
        tracing::info!(
            table = %ts.table,
            rows = rows_total,
            chunks = appends,
            duration_ms = t0.elapsed().as_secs_f64() * 1000.0,
            "large backfill appended in chunks"
        );
    }
    let fences = reader.finish().await;
    if agg_seed.is_none() {
        crate::statsd::snapshot_stored(rows_total, snapshot_bytes, t0.elapsed().as_secs_f64() * 1000.0);
    }
    Ok((fences.gate, agg_seed, emitted_seed))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_envelope(
    ts: &TableSchema,
    shapes: &HashMap<String, StandaloneShape>,
    shape_index: &StandaloneIndex,
    families: &HashMap<Vec<usize>, KeyRouter>,
    aggregates: &mut HashMap<String, AggShape>,
    agg_index: &StandaloneIndex,
    env: Envelope,
    pending: &mut HashMap<String, Vec<Envelope>>,
    subq: &SubqueryHandle,
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
    library_mode: bool,
) -> Result<()> {
    let (delta, txid, lsn) = apply_envelope(ts, &env)?;
    // No before-image (library mode only — see `needs_absolute_emission`): the plain tiers below
    // decide membership ABSOLUTELY per pk instead of from the delta, so a delete with no delta at
    // all still has work to do.
    let absolute = library_mode && needs_absolute_emission(&env);
    if delta.is_empty() && !absolute {
        return Ok(());
    }
    // Per-envelope trace collection (hops, reached shape ids). `None` when nobody is subscribed,
    // so the untraced hot path pays only this one atomic load — see `crate::trace`.
    let mut tr: Option<(Vec<crate::trace::TraceHop>, Vec<String>)> = if trace_tx.receiver_count() > 0 {
        Some((vec![crate::trace::TraceHop::new(format!("table:{}", ts.table), "passed")], Vec::new()))
    } else {
        None
    };
    // `lsn` (the commit-LSN string) is stamped onto output envelopes so a subset client can position
    // its live tail at the page snapshot (drop deltas with `lsn < snapshot_lsn`); `lsn_u64` is the
    // numeric fallback for the per-shape backfill-skip compare, and `xid` (the transaction id the
    // ingestor stamps as `txid`) is the primary fence — see `pg::SnapshotGate` for why xid visibility,
    // not LSN order, is the sound backfill↔replication reconciliation.
    let lsn_u64 = lsn.as_deref().map(crate::pg::lsn_to_u64).unwrap_or(0);
    let xid = txid.as_deref().and_then(|s| s.parse::<u64>().ok());
    metrics().envelopes.fetch_add(1, Ordering::Relaxed);
    let _t = Timer::new(&metrics().process_envelope);
    // Standalone shapes: evaluate each stateless filter directly on the delta (no thread, no clone).
    // Skip changes already visible to the shape's backfill snapshot (xid-visibility gate, LSN
    // fallback for changes without a parseable xid). On the untraced hot path only the index's
    // candidates are visited (a non-candidate's necessary conjunct fails, so it cannot match);
    // with a trace subscriber the full scan is kept so every filter node still reports a hop.
    //
    // ABSOLUTE emission takes over both plain tiers when the envelope has no before-image: the
    // row's current value (`row_now`, `None` for a delete) decides each shape's membership
    // outright — matches ⇒ `upsert`, otherwise ⇒ `delete <key>`. It costs a visit to EVERY shape
    // on the table (a shape the row no longer matches is exactly the one that must be told to drop
    // the key, so an index of *candidates* cannot find it), which is the price of not having an
    // old row. On the LIVE path that price is almost never paid: the per-key view is exact from
    // boot, so an old-less delete/update here means a key the change log never carried.
    let row_now: Option<&Row> = delta.iter().find(|Tup2(_, w)| *w > 0).map(|Tup2(r, _)| r);
    if absolute {
        for (sid, shape) in shapes.iter() {
            if shape.gate.should_skip(lsn_u64, xid) {
                if let Some((hops, _)) = tr.as_mut() {
                    hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "dropped"));
                }
                continue;
            }
            let held = row_now.filter(|r| shape.pred.matches(r));
            if let Some((hops, ids)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(
                    format!("filter:{sid}"),
                    if held.is_some() { "passed" } else { "dropped" },
                ));
                hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
                ids.push(sid.clone());
            }
            if let Some(e) = absolute_envelope(
                ts,
                &env.key,
                held,
                txid.clone(),
                lsn.clone(),
                shape.out_cols.as_deref().map(Vec::as_slice),
            ) {
                pending.entry(shape.stream_path.clone()).or_default().push(e);
            }
        }
        let _s = Timer::new(&metrics().family_step);
        for router in families.values() {
            // A routed shape's membership IS the key match, so the row belongs to exactly the one
            // key group its CURRENT value names; every other group must drop it.
            let new_key = row_now.map(|r| key_of(r, &router.key_cols));
            for (key, routed) in router.index.iter() {
                let held = if new_key.as_ref() == Some(key) { row_now } else { None };
                for rs in routed {
                    if rs.gate.should_skip(lsn_u64, xid) {
                        continue;
                    }
                    if let Some((hops, ids)) = tr.as_mut() {
                        let sid = format!("s{}", rs.num_id);
                        hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
                        ids.push(sid);
                    }
                    if let Some(e) = absolute_envelope(
                        ts,
                        &env.key,
                        held,
                        txid.clone(),
                        lsn.clone(),
                        rs.out_cols.as_deref().map(Vec::as_slice),
                    ) {
                        pending.entry(rs.stream_path.clone()).or_default().push(e);
                    }
                }
            }
        }
    }
    let candidate_ids;
    let candidates: Box<dyn Iterator<Item = (&String, &StandaloneShape)>> = if absolute {
        Box::new(std::iter::empty())
    } else if tr.is_some() {
        Box::new(shapes.iter())
    } else {
        candidate_ids = shape_index.candidates(&delta);
        Box::new(candidate_ids.iter().filter_map(|sid| shapes.get_key_value(sid)))
    };
    for (sid, shape) in candidates {
        if shape.gate.should_skip(lsn_u64, xid) {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "dropped"));
            }
            continue;
        }
        let out = eval_standalone(&shape.pred, &delta);
        if out.is_empty() {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "dropped"));
            }
            continue;
        }
        if let Some((hops, ids)) = tr.as_mut() {
            hops.push(crate::trace::TraceHop::new(format!("filter:{sid}"), "passed"));
            hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
            ids.push(sid.clone());
        }
        let envs = translate_output(ts, out, txid.clone(), lsn.clone(), shape.out_cols.as_deref().map(Vec::as_slice));
        pending.entry(shape.stream_path.clone()).or_default().extend(envs);
    }
    // Equality routers: route each delta row by its key to exactly the shapes registered on that key.
    // No table copy, no join state — membership is the key match (an equality-template predicate matches a
    // row iff its key equals the shape's constants). Each shape's own snapshot gate is applied, so
    // changes already in that shape's backfill are skipped.
    let _s = Timer::new(&metrics().family_step);
    for router in families.values().filter(|_| !absolute) {
        type ShapeOut<'a> = (&'a str, Option<&'a [usize]>, Vec<(Row, ZWeight)>);
        let mut by_shape: HashMap<u64, ShapeOut> = HashMap::new();
        let mut routed_keys: Vec<Row> = Vec::new();
        for Tup2(row, w) in &delta {
            let key = key_of(row, &router.key_cols);
            let Some(routed) = router.index.get(&key) else { continue };
            if tr.is_some() && !routed_keys.contains(&key) {
                routed_keys.push(key);
            }
            for rs in routed {
                if rs.gate.should_skip(lsn_u64, xid) {
                    continue;
                }
                by_shape
                    .entry(rs.num_id)
                    .or_insert_with(|| (rs.stream_path.as_str(), rs.out_cols.as_deref().map(Vec::as_slice), Vec::new()))
                    .2
                    .push((row.clone(), *w));
            }
        }
        if let Some((hops, ids)) = tr.as_mut() {
            // Node id matches the visualizer's logical graph: family:<table>:<key cols by name>.
            let cols = router
                .key_cols
                .iter()
                .map(|i| ts.columns.get(*i).map(|(n, _)| n.clone()).unwrap_or_else(|| format!("col{i}")))
                .collect::<Vec<_>>()
                .join(",");
            let node = format!("family:{}:{cols}", ts.table);
            if by_shape.is_empty() {
                hops.push(crate::trace::TraceHop::new(node, "dropped"));
            } else {
                for key in &routed_keys {
                    let key_json = serde_json::Value::Array(key.0.iter().map(crate::value::Value::to_json).collect());
                    hops.push(crate::trace::TraceHop::routed(node.clone(), key_json));
                }
                for num_id in by_shape.keys() {
                    let sid = format!("s{num_id}");
                    hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "passed"));
                    ids.push(sid);
                }
            }
        }
        if by_shape.is_empty() {
            continue;
        }
        metrics().family_steps.fetch_add(1, Ordering::Relaxed);
        for (_sid, (stream_path, out_cols, rows)) in by_shape {
            let envs = translate_output(ts, rows, txid.clone(), lsn.clone(), out_cols);
            if !envs.is_empty() {
                pending.entry(stream_path.to_string()).or_default().extend(envs);
            }
        }
    }
    // A delete with no before-image carries no delta at all, so the subquery registry and the
    // aggregate folds have nothing to apply — and nothing they COULD apply, since correcting them
    // needs the old row. That is sound rather than a gap: the per-key view supplies the old row for
    // every key the change log actually carried (so a real delete DOES reach the folds), an
    // aggregate never goes dormant so the replay path never feeds one, and a delete for a key that
    // was never inserted must move no aggregate anyway.
    if delta.is_empty() {
        publish_envelope_trace(trace_tx, tr, ts, &delta, &txid, &lsn);
        return Ok(());
    }
    // Subquery shapes/nodes: route this delta through the cross-table registry. Under the lock it
    // updates the shared inner-set nodes (in-memory) and emits outer-shape deltas; the flip-driven
    // Postgres query-backs are handed to the engine's flip-propagator task so they never block
    // this tailer. The convergence barrier is processed offsets + a drained flip queue
    // (`pending_flips == 0`).
    {
        let mut work = std::collections::VecDeque::new();
        {
            let mut reg = subq.registry.lock().await;
            if reg.touches(&ts.table) {
                let mut sq_hops: Option<Vec<crate::trace::TraceHop>> = tr.as_ref().map(|_| Vec::new());
                work = reg.on_table_delta(ts, &delta, lsn_u64, xid, txid.clone(), sq_hops.as_mut()).await?;
                if let (Some((hops, ids)), Some(sq)) = (tr.as_mut(), sq_hops) {
                    for h in &sq {
                        if h.outcome == "passed"
                            && let Some(sid) = h.node.strip_prefix("shape:")
                            && !ids.iter().any(|i| i == sid)
                        {
                            ids.push(sid.to_string());
                        }
                    }
                    hops.extend(sq);
                }
            }
        }
        if !work.is_empty() {
            subq.pending_flips.fetch_add(1, Ordering::SeqCst);
            if subq.flip_tx.send(FlipWork::Walk { work, txid: txid.clone(), lsn: lsn.clone() }).is_err() {
                // Propagator gone (shutdown) — don't leave the barrier stuck.
                subq.pending_flips.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
    // Scalar aggregations: fold this delta into each *candidate* aggregate (necessary-conjunct
    // index — a non-candidate's predicate provably matches no delta row, so skipping it leaves
    // the fold unchanged); emit the new value when it changes. Skips changes already counted in
    // the seed (the aggregate's snapshot gate). Under an attached trace subscriber the index is
    // bypassed (like the standalone tier) so every aggregate node reports a folded/dropped hop.
    let agg_candidates: Option<HashSet<String>> =
        if tr.is_none() { Some(agg_index.candidates(&delta).into_iter().collect()) } else { None };
    for (sid, agg) in aggregates.iter_mut() {
        if let Some(c) = &agg_candidates {
            if !c.contains(sid) {
                continue;
            }
        }
        if agg.gate.should_skip(lsn_u64, xid) {
            if let Some((hops, _)) = tr.as_mut() {
                hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), "dropped"));
            }
            continue;
        }
        let mut folded = false;
        if agg.apply(&delta) {
            let val = agg.value();
            if agg.last.as_ref() != Some(&val) {
                agg.last = Some(val.clone());
                let env = agg.envelope(ts, txid.clone(), lsn.clone());
                pending.entry(agg.stream_path.clone()).or_default().push(env);
                folded = true;
            }
        }
        if let Some((hops, ids)) = tr.as_mut() {
            hops.push(crate::trace::TraceHop::new(format!("shape:{sid}"), if folded { "folded" } else { "dropped" }));
            if folded {
                ids.push(sid.clone());
            }
        }
    }
    publish_envelope_trace(trace_tx, tr, ts, &delta, &txid, &lsn);
    Ok(())
}

/// Publish one envelope's trace event (serialize once; lossy send — see `crate::trace`). No-op when
/// nothing was collected, i.e. when nobody is subscribed.
fn publish_envelope_trace(
    trace_tx: &tokio::sync::broadcast::Sender<Arc<String>>,
    tr: Option<(Vec<crate::trace::TraceHop>, Vec<String>)>,
    ts: &TableSchema,
    delta: &[Tup2<Row, ZWeight>],
    txid: &Option<String>,
    lsn: &Option<String>,
) {
    let Some((hops, shape_ids)) = tr else { return };
    let ev = crate::trace::TraceEvent {
        lsn: lsn.clone(),
        txid: txid.clone(),
        table: ts.table.clone(),
        delta: delta
            .iter()
            .take(crate::trace::DELTA_CAP)
            .map(|Tup2(row, w)| crate::trace::TraceDelta { row: ts.row_to_json(row), w: *w })
            .collect(),
        hops,
        shapes: shape_ids,
    };
    if let Ok(json) = serde_json::to_string(&ev) {
        let _ = trace_tx.send(Arc::new(json));
    }
}

/// Total serialized byte size of a set of output envelopes (for storage/snapshot byte metrics).
pub(crate) fn envs_bytes(envs: &[Envelope]) -> u64 {
    envs.iter().map(|e| serde_json::to_string(e).map(|s| s.len() as u64).unwrap_or(0)).sum()
}

/// Emit the per-source-transaction storage StatsD metrics from one txn's staged appends.
/// `affected_shape_count` = distinct shape streams the txn touched; `operations`/`bytes` = output
/// envelopes appended + their serialized size. (Subquery-registry appends go out synchronously inside
/// `process_envelope` and are not reflected here.) No-op when the txn produced no appends.
pub(crate) fn emit_storage_txn_metrics(txn_pending: &HashMap<String, Vec<Envelope>>) {
    let ops: u64 = txn_pending.values().map(|v| v.len() as u64).sum();
    if ops == 0 {
        return;
    }
    let bytes: u64 =
        txn_pending.values().flatten().map(|e| serde_json::to_string(e).map(|s| s.len() as u64).unwrap_or(0)).sum();
    crate::statsd::storage_txn(ops, bytes, txn_pending.len() as u64);
}

/// Flush the batch's staged appends, bounded-concurrently. Each envelope keeps its own txid, so
/// `awaitTxId` semantics are preserved; only the HTTP round-trips are coalesced + parallelized.
///
/// Appends are **reliable**: transient failures retry with backoff (`append_reliable`) rather than
/// being dropped — a lost shape append is a permanent divergence for that shape's subscribers, and
/// the tailer's processed-offset barrier (published after this returns) must mean "every subscriber
/// stream reflects the batch". The only non-retried case is a retired stream (404/410/closed — the
/// shape was dropped or evicted mid-flush), which discards cleanly.
pub(crate) async fn flush_pending(ds: &DsClient, pending: HashMap<String, Vec<Envelope>>) {
    const CAP: usize = 32; // bound in-flight appends so we don't swamp the storage server
    let mut items: Vec<(String, Vec<Envelope>)> = pending.into_iter().collect();
    while !items.is_empty() {
        let take = items.len().min(CAP);
        let batch = items.split_off(items.len() - take);
        let mut set = tokio::task::JoinSet::new();
        for (path, envs) in batch {
            let ds = ds.clone();
            set.spawn(async move {
                let _t = Timer::new(&metrics().append);
                ds.append_reliable(&path, &envs).await;
                metrics().shape_appends.fetch_add(1, Ordering::Relaxed);
            });
        }
        while set.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod txn_boundary_tests {
    use super::*;
    use crate::ds::EnvelopeHeaders;

    /// One change-log envelope as the ingestor stamps it.
    fn env(lsn: &str, txid: &str, seq: u64, last: bool) -> Envelope {
        Envelope {
            type_: "public.t".into(),
            key: seq.to_string(),
            value: None,
            old: None,
            headers: EnvelopeHeaders {
                operation: "insert".into(),
                txid: Some(txid.into()),
                offset: None,
                lsn: Some(lsn.into()),
                seq: Some(seq),
                last: last.then_some(true),
            },
        }
    }

    /// A library-mode write: no transaction structure at all (no `seq`).
    fn lib_env(key: &str) -> Envelope {
        Envelope {
            type_: "public.t".into(),
            key: key.into(),
            value: None,
            old: None,
            headers: EnvelopeHeaders {
                operation: "insert".into(),
                txid: None,
                offset: None,
                lsn: None,
                seq: None,
                last: None,
            },
        }
    }

    /// A page that ends on a marked envelope is entirely processable; one that does not has its
    /// trailing run held back, from the run's FIRST envelope — never mid-transaction.
    #[test]
    fn a_page_ending_mid_transaction_holds_back_the_whole_trailing_run() {
        // Complete: nothing held.
        let done = vec![env("0/10", "1", 0, false), env("0/10", "1", 1, true)];
        assert_eq!(unterminated_tail(&done), None);

        // Incomplete: the trailing run starts at index 0.
        let open = vec![env("0/10", "1", 0, false), env("0/10", "1", 1, false)];
        assert_eq!(unterminated_tail(&open), Some(0));

        // A complete transaction followed by the first chunk of the next: only the second is held.
        let mixed = vec![env("0/10", "1", 0, true), env("0/20", "2", 0, false), env("0/20", "2", 1, false)];
        assert_eq!(unterminated_tail(&mixed), Some(1));

        // Empty page: nothing to hold.
        assert_eq!(unterminated_tail(&[]), None);
    }

    /// A library-mode envelope is a one-envelope transaction with no `seq`. Holding one would wait
    /// forever for a marker no producer sends, so it is never held.
    #[test]
    fn an_envelope_with_no_transaction_structure_is_never_held() {
        assert_eq!(unterminated_tail(&[lib_env("a")]), None);
        assert_eq!(unterminated_tail(&[env("0/10", "1", 0, true), lib_env("a")]), None);
    }

    /// While a run is held, everything PUBLISHED — the restart point, `GET /tables/{n}/offset`, the
    /// segment-deletion floor — stays at the page the run began in, even after the read cursor has
    /// moved on (and even across a segment crossing). Otherwise a crash would resume past a
    /// transaction that was never applied.
    #[test]
    fn publication_is_pinned_to_where_a_held_run_began() {
        let cursor = LogPosition { segment: 3, offset: "99".into() };
        assert_eq!(published(&cursor, &None), cursor, "nothing held: the read cursor is published");

        let held_from = LogPosition { segment: 2, offset: "40".into() };
        assert_eq!(published(&cursor, &Some(held_from.clone())), held_from);
    }
}
