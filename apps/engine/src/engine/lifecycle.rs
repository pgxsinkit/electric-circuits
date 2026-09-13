//! Shape lifecycle: creation (all strategies), sharing, and the retention state machine
//! (release/touch/dormancy/eviction/sweep).

use super::*;

/// How many times a create/join is redone after losing a race during its catalog durability wait
/// (see [`Engine::recheck_after_durability`]). Three: the race needs an *external* retirement to
/// land inside the wait, so a second loss is already unusual and a third means something is
/// retiring this table continuously — at which point the honest answer is the retryable error.
pub(crate) const CREATE_RACE_ATTEMPTS: u32 = 3;

/// A short, JITTERED pause between redo attempts.
///
/// The race that caused the redo is an external retirement landing inside the durability wait, and
/// the resolution that drove it holds the table's resolve lock for a moment afterwards — redoing
/// instantly would just walk into "schema of 'x' is being resolved after a change; retry", which is
/// not retryable and would surface to the client. The jitter matters because the interesting case is
/// N clients racing the SAME drift: redoing in lockstep puts all of them back on that lock together.
/// 50-250 ms is well inside any client's create timeout.
async fn redo_backoff() {
    let jitter = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()) % 200)
        .unwrap_or(0);
    tokio::time::sleep(std::time::Duration::from_millis(50 + jitter)).await;
}

/// Decide what one create attempt's outcome means: `Ok(result)` to return it, `Err(())` to redo the
/// attempt. Only a [`CreateRaced`] with attempts left is redone.
fn retry_create(
    res: Result<ShapeRecord>,
    attempt: u32,
    what: &str,
    table: &TableRef,
) -> std::result::Result<Result<ShapeRecord>, ()> {
    match &res {
        Err(e) if e.downcast_ref::<CreateRaced>().is_some() && attempt < CREATE_RACE_ATTEMPTS => {
            tracing::warn!(
                "{what} create on '{table}' lost a race during its catalog durability wait ({e:#}); \
                 attempt {attempt}/{CREATE_RACE_ATTEMPTS}, redoing it"
            );
            Err(())
        }
        _ => Ok(res),
    }
}

impl Engine {
    /// `share`: when true, an identical existing shape (same table, canonical predicate, and columns) is
    /// joined by ref-count instead of creating a second stream — so N app clients subscribing to the same
    /// reference shape (e.g. `project_members WHERE user_id = me`) share one maintained output. Both
    /// public surfaces pass `true` — the `/v1/shape` adapter included, which keys its per-request live
    /// state by the SHARED shape id (`electric.rs`), so identical Electric definitions collapse onto one
    /// maintained stream like any other. `false` exists for callers that genuinely need their own
    /// handle; nothing in the tree currently does.
    ///
    /// A shared create therefore always pays the join path's stream-liveness `HEAD`
    /// ([`Self::retire_join_target_if_stream_lost`]), `/v1/shape` requests included. That is the
    /// intended trade: one storage round trip per join is cheaper than handing an Electric client a
    /// handle whose stream storage has already lost, which it cannot detect except as an empty sync.
    ///
    /// An attempt that lost a race during its catalog durability wait ([`CreateRaced`], see
    /// [`Self::recheck_after_durability`]) is **redone**, up to [`CREATE_RACE_ATTEMPTS`] times: the
    /// definition is still valid, only the shape it made was retired underneath it, and every client
    /// would otherwise have to implement this loop itself. Every other failure — including the
    /// pre-durability drift check, which a client should see — propagates on the first attempt.
    pub async fn create_shape(
        &self,
        table: &TableRef,
        where_: Option<PredicateJson>,
        columns: Option<Vec<String>>,
        changes_only: bool,
        share: bool,
    ) -> Result<ShapeRecord> {
        self.create_shape_as(table, where_, columns, changes_only, share, None).await.map(|(rec, _)| rec)
    }

    /// [`Self::create_shape`] with a caller-chosen **subscription id** (ADR-0008), returning the id
    /// the shape was subscribed under alongside the record.
    ///
    /// The id makes the create idempotent: repeating it with the same id renews that subscription
    /// and returns the same handle instead of taking a second claim, so a client whose success
    /// response was lost can simply ask again. `None` mints one — the caller still gets it back and
    /// can renew or release with it, but it had no idempotency on THIS create, because the engine
    /// could not have recognised a repeat. An id another shape already holds is refused
    /// ([`SubscriptionConflict`]): one name, one shape.
    pub async fn create_shape_as(
        &self,
        table: &TableRef,
        where_: Option<PredicateJson>,
        columns: Option<Vec<String>>,
        changes_only: bool,
        share: bool,
        subscription: Option<String>,
    ) -> Result<(ShapeRecord, String)> {
        // Minted ONCE, outside the redo loop: a redone attempt must be the same subscription, or
        // the retry would take a second claim — the very thing the id exists to prevent.
        let sub = self.resolve_subscription(subscription).await;
        for attempt in 1..=CREATE_RACE_ATTEMPTS {
            let res = self.create_shape_once(table, where_.clone(), columns.clone(), changes_only, share, &sub).await;
            match retry_create(res, attempt, "shape", table) {
                Ok(out) => return out.map(|rec| (rec, sub)),
                Err(()) => redo_backoff().await,
            }
        }
        unreachable!("retry_create returns the error on the last attempt")
    }

    /// The caller's subscription id, or one minted for it (`~<nonce>-<n>` — see
    /// [`MINTED_SUB_PREFIX`]).
    pub(crate) async fn resolve_subscription(&self, subscription: Option<String>) -> String {
        match subscription {
            Some(s) => s,
            None => self.mint_subscription().await,
        }
    }

    /// Mint an engine-side subscription id. Unique across restarts (the catalog outlives the
    /// process) via the same per-process nonce the catalog's event ids use.
    async fn mint_subscription(&self) -> String {
        let mut st = self.state.lock().await;
        let n = st.next_minted_sub;
        st.next_minted_sub += 1;
        format!("{MINTED_SUB_PREFIX}{}-{n}", self.sub_nonce)
    }

    async fn create_shape_once(
        &self,
        table: &TableRef,
        where_: Option<PredicateJson>,
        columns: Option<Vec<String>>,
        changes_only: bool,
        share: bool,
        sub: &str,
    ) -> Result<ShapeRecord> {
        // A degraded engine cannot say what belongs in a shape (see `Engine::ensure_not_degraded`),
        // so it refuses to make one rather than backfill from state it knows is wrong. This early
        // check is the cheap refusal and the only one the *join* path takes; a create that gets
        // past it checks again under the state lock before registering, and once more before
        // handing back a handle (see `ensure_create_not_degraded`).
        self.ensure_not_degraded()?;
        // Whole shape-creation timer (backfill + registration); emitted by the creator on success only
        // (joiners return early before this fires) as `create_snapshot_task.stop.duration`.
        let created_at = std::time::Instant::now();
        let mut st = self.state.lock().await;
        let ts = match st.tables.get(table) {
            Some(ts) => ts.clone(),
            None => bail!("unknown table '{table}'"),
        };
        let col_names = columns.clone();
        let out_cols = resolve_columns(&ts, columns)?;
        // **`ts` and `gens` come from ONE critical section.** Everything this create derives from the
        // schema — the compiled predicate's column indices, the family key, the record's fingerprint,
        // the backfill's row expression — is built from the `ts` cloned just above, so the generations
        // it is re-checked against must be the ones that were current when that clone was taken. The
        // lock is released below (the join path's storage `HEAD`), and a drift resolution completing in
        // that window would otherwise leave a pre-drift `ts` paired with post-bump generations: both
        // closing checks pass and the shape is installed against a schema the table no longer has.
        // Capturing here is also strictly safer than capturing later — it can only refuse more.
        //
        // The set is every table this create reads: the outer table plus every table a subquery
        // predicate references (empty for a plain predicate, so the plain path captures exactly
        // `[table]` as before).
        let mut dep_tables = vec![table.clone()];
        if let Some(w) = &where_ {
            dep_tables.extend(referenced_tables(w));
        }
        let gens = st.capture_gens(&dep_tables);

        // Shape sharing: an identical shape (subset feed, materialized, OR subquery) that already exists
        // is joined (ref-count++), returning the same stream — no second stream, no per-subscriber append
        // fan-out. Subquery shapes share their inner-set nodes in the registry regardless; sharing the
        // *outer* shape here collapses identical subquery shapes fully.
        let feed_sig = if share { Some(shape_signature(table, &where_, &out_cols, changes_only)) } else { None };
        // Storage may have lost the retained shape's stream since it was made; a stale entry is
        // retired here so the join below falls through to a fresh create (see
        // `retire_join_target_if_stream_lost`).
        if let Some(sig) = &feed_sig {
            drop(st);
            self.retire_join_target_if_stream_lost(sig).await?;
            st = self.state.lock().await;
        }
        if let Some(sig) = &feed_sig {
            if let Some(existing_id) = st.feed_by_sig.get(sig).cloned() {
                if let Some(rec) = st.shapes.get(&existing_id).cloned() {
                    // Refuse BEFORE taking the claim: a join that is going to be turned away must
                    // not leave a `Joined` in the durable catalog or a subscription for the caller
                    // to give back. A joiner is re-checked against the same `gens` a creator is —
                    // the ones captured with `ts` above, so a drift that landed during the `HEAD`
                    // window refuses this join instead of acknowledging a shape the drift retired.
                    self.ensure_schema_resolved(&st, &dep_tables)?;
                    // One subscription id belongs to one shape (ADR-0008). Reusing it for a
                    // different predicate is refused rather than silently accepted: the caller
                    // would hold one name for two shapes and could release neither unambiguously.
                    ensure_subscription_free(&st, sub, &existing_id)?;
                    // A join and a RENEWAL are the same call: `subscribe` returns false when the
                    // shape already holds this id, in which case nothing is claimed and only the
                    // lease moves.
                    let now = crate::changelog::now_secs();
                    let fresh = st.subscribe(&existing_id, sub.to_string(), now);
                    let ev = CatalogEvent::Joined { id: existing_id.clone(), subscription: sub.to_string(), at: now };
                    // A NEW claim is enqueued here (under the lock, so the log order matches the
                    // state order) and WAITED on immediately before the join is acknowledged: a
                    // subscription the durable record does not carry is one a restart forgets. A
                    // renewal promises nothing new — the claim is already in the log — so its
                    // record is queued like any engine-initiated event and the caller is not made
                    // to wait on storage to keep a lease it already holds.
                    let joined_durable = if fresh {
                        Some(self.catalog_tx.send_durable(ev))
                    } else {
                        self.catalog_tx.send(ev);
                        None
                    };
                    let ready = st.feed_shares.get(&existing_id).expect("share entry for live feed").ready.clone();
                    // Release the lock, then wait for the creator's backfill to land: a joiner must not
                    // see a stream whose snapshot isn't readable yet, and must surface (not mask) a
                    // failed creation.
                    drop(st);
                    // Everything the join has claimed before its first await, given back if it
                    // never reaches its own end — including when the CLIENT disappears mid-wait.
                    let mut joining = JoinGuard::new(self, &existing_id, sub, fresh);
                    if let Err(e) = await_share_ready(ready, &existing_id).await {
                        // The failed creator already removed the share entries; undo nothing.
                        // A degraded outcome arrives typed, so this joiner answers 503 with the
                        // same reason the creator did.
                        joining.rollback().await;
                        return Err(e);
                    }
                    // The creator succeeded — but it may have succeeded a moment BEFORE the
                    // degradation mark, in which case the reaper is about to delete the very
                    // stream this handle points at. This is the joiner's equivalent of the
                    // creator's final `ensure_create_not_degraded`: check the same latch after
                    // the work is done, and give back the claim taken above so a refused
                    // join does not pin the shape.
                    if let Err(e) = self.ensure_not_degraded() {
                        joining.rollback().await;
                        return Err(e);
                    }
                    // A rejoin is a touch: if the shape went dormant since the last subscriber
                    // left, reactivate it (change-log replay) before handing out the stream.
                    if let Err(e) = self.ensure_active(&existing_id).await {
                        // Roll the failed join back so the dead subscription doesn't pin the shape.
                        joining.rollback().await;
                        return Err(e);
                    }
                    if let Err(e) = self.ensure_schema_unchanged(&gens).await {
                        joining.rollback().await;
                        return Err(e);
                    }
                    if let Some(durable) = joined_durable {
                        durable.await;
                    }
                    // ...and the same check ONCE MORE, after the wait: the target may have been
                    // purged or retired while this join was blocked on storage (see
                    // `recheck_after_durability`). Give the provisional claim back and answer
                    // retryable rather than hand out a handle whose stream is already 404.
                    if let Err(e) = self.recheck_after_durability(&existing_id, &rec.stream_path, &gens).await {
                        joining.rollback().await;
                        return Err(e);
                    }
                    joining.complete();
                    return Ok(rec);
                }
            }
        }
        // Nothing to join: this create is about to mint a shape, so the id must be free of any
        // OTHER shape as well (the join path checked it against the one it was joining).
        ensure_subscription_free(&st, sub, "")?;

        let num_id = st.next_shape_id;
        let id = format!("s{num_id}");
        st.next_shape_id += 1;
        let stream_path = format!("shape/{id}");
        // NOTE: the stream itself is created AFTER the state lock is released (per path below):
        // the PUT is a storage round-trip with a durability fsync, and holding the global lock
        // across it serializes concurrent shape creations.

        // Subquery shapes (`col IN (SELECT …)`) are maintained by the cross-table registry, not by a
        // tailer's local routing. Ensure a tailer exists for the outer table AND every referenced inner
        // table (so their deltas reach the registry), then register + backfill via the registry.
        if where_.as_ref().is_some_and(predicate_has_subquery) {
            let where_json = where_.expect("subquery predicate present");
            let mut tables = referenced_tables(&where_json);
            tables.push(table.clone());
            for t in &tables {
                if !st.tables.contains_key(t) {
                    bail!("unknown table '{t}' referenced by subquery");
                }
            }
            self.ensure_schema_resolved(&st, &tables)?;
            // `gens` (outer + every inner table) was captured with `ts`, before the lock was released
            // for the join path's `HEAD` — see the capture site for why the two must be one snapshot.
            // The sequencer feeds every table's deltas to the registry; just make sure it runs.
            self.ensure_sequencer(&mut st);
            let rec = ShapeRecord {
                id: id.clone(),
                table: table.clone(),
                stream_path: stream_path.clone(),
                changes_only,
                where_json: Some(where_json.clone()),
                columns: col_names.clone(),
                family_key: None,
                is_subquery: true,
                aggregate: None,
                fingerprint: ts.fingerprint.clone(),
            };
            // The load-bearing degrade check: taken under the state lock, in the same critical
            // section as the registration below. The degradation reaper snapshots every registered
            // subquery stream under this same lock, so with respect to that snapshot a create either
            // registered before it (and is refused by its final check — see
            // `ensure_create_not_degraded`) or observes `degraded` here and never registers at all.
            self.ensure_not_degraded()?;
            st.shapes.insert(id.clone(), rec.clone());
            // Durable BEFORE the create is acknowledged (awaited at the bottom of the success
            // path): a `POST /shapes` that returns 200 promises a shape that survives a restart.
            let created = self.catalog_tx.send_durable(CatalogEvent::Created {
                rec: rec.clone(),
                sig: feed_sig.clone(),
                subscription: sub.to_string(),
                at: crate::changelog::now_secs(),
            });
            self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
            self.ensure_retention_sweeper();
            // First subquery shape: from here on a lost flip is possible, so the stream reaper that
            // fires on degradation needs to exist.
            self.ensure_degrade_reaper();
            // Register this (first) subquery shape so later identical ones join it by ref-count.
            // Joiners wait on `ready_tx` — the shape isn't live until the registry has seeded its
            // nodes and backfilled the stream.
            let (ready_tx, ready_rx) = tokio::sync::watch::channel(ShareOutcome::Pending);
            if let Some(sig) = feed_sig {
                st.feed_by_sig.insert(sig.clone(), id.clone());
                st.feed_shares.insert(id.clone(), FeedShare { sig, subs: Default::default(), ready: ready_rx });
                st.subscribe(&id, sub.to_string(), crate::changelog::now_secs());
            }
            // Release the engine-state lock before the registry work. Creation is three-phase:
            // begin (brief registry lock: nodes/edges/pending buffer registered) → Postgres
            // seeding + backfill with NO lock held (concurrent creates parallelize on the
            // shared pool) → finish (brief lock: install seeds, gated replay of buffered
            // deltas, register the shape). Replay flips propagate through the worker pool.
            drop(st);
            let mut creating = CreateGuard::new(self, &id, table, &stream_path, Registration::Registry);
            let res = async {
                self.ds.ensure_stream(&stream_path).await?;
                self.create_subquery_three_phase(&id, table, &stream_path, &where_json, out_cols, changes_only).await
            }
            .await;
            match res {
                Ok(()) => {
                    // The create's work is done, but it may have overlapped a degradation — its stream is
                    // then already reaped and the handle would be dead on arrival. Refuse instead of
                    // answering success (see `ensure_create_not_degraded`).
                    if let Err(e) = self.ensure_create_not_degraded() {
                        let _ = ready_tx.send(ShareOutcome::Degraded);
                        creating.rollback().await;
                        return Err(e);
                    }
                    // ...or a schema drift on the outer table or ANY table its subquery reads. This
                    // is also what unwinds a create the drift handler purged mid-phase-B: the
                    // registry install would otherwise land on a deleted stream.
                    if let Err(e) = self.ensure_schema_unchanged(&gens).await {
                        let _ = ready_tx.send(ShareOutcome::Failed);
                        creating.rollback().await;
                        return Err(e);
                    }
                    // Durability BEFORE the guard is disarmed, so a client that gives up while
                    // storage is down still unwinds cleanly: the create is rolled back exactly as
                    // any other cancellation, and the `Created` that lands anyway is followed by the
                    // rollback's `Dropped`. With storage down this waits — the client's own timeout
                    // is the bound — and it never returns a shape the catalog lacks.
                    created.await;
                    // The wait is an interval of its own: re-check before acknowledging.
                    if let Err(e) = self.recheck_after_durability(&id, &stream_path, &gens).await {
                        // `Raced`, not `Failed`: this creator is about to redo the create, and a
                        // joiner parked on the share must redo too rather than answer 500.
                        let _ = ready_tx.send(ShareOutcome::Raced);
                        creating.rollback().await;
                        return Err(e);
                    }
                    creating.complete();
                    let _ = ready_tx.send(ShareOutcome::Ready);
                    trace_lifecycle(
                        &self.trace_tx,
                        crate::trace::GraphLifecycle::ShapeAdded { shape: id, table: table.clone() },
                    );
                    crate::statsd::create_snapshot_task(created_at.elapsed());
                    return Ok(rec);
                }
                Err(e) => {
                    // Registration failed: wake any joiners with the failure, then undo everything
                    // this create registered so later identical creates don't join a dead stream.
                    let _ = ready_tx.send(ShareOutcome::Failed);
                    creating.rollback().await;
                    return Err(e);
                }
            }
        }

        self.ensure_schema_resolved(&st, std::slice::from_ref(table))?;
        // `gens` was captured with `ts`, before the lock was released for the join path's `HEAD` —
        // the predicate compiled on the next line uses that `ts`'s column indices.
        let pred = Arc::new(CompiledPredicate::compile_opt(where_.as_ref(), &ts)?);
        // Family placement (for graph introspection): an equality template routes by these key columns
        // via a shared family; otherwise it's a standalone filter.
        let family_key = pred
            .equality_template()
            .map(|pairs| pairs.iter().map(|(i, _)| ts.columns[*i].0.clone()).collect::<Vec<_>>());

        let cmd_tx = self.ensure_sequencer(&mut st).cmd_tx.clone();
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: table.clone(),
                shape_id: id.clone(),
                num_id,
                stream_path: stream_path.clone(),
                pred: pred.clone(),
                out_cols: out_cols.clone(),
                kind: CreateKind::Plain,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;

        let rec = ShapeRecord {
            id: id.clone(),
            table: table.clone(),
            stream_path,
            changes_only,
            where_json: where_.clone(),
            columns: col_names,
            family_key,
            is_subquery: false,
            aggregate: None,
            fingerprint: ts.fingerprint.clone(),
        };
        // Under the state lock, immediately before registering — see `ensure_create_not_degraded`
        // for why this check and the one after the create's work are together sufficient.
        self.ensure_not_degraded()?;
        st.shapes.insert(id.clone(), rec.clone());
        // Durable before the create is acknowledged — see the subquery path above.
        let created = self.catalog_tx.send_durable(CatalogEvent::Created {
            rec: rec.clone(),
            sig: feed_sig.clone(),
            subscription: sub.to_string(),
            at: crate::changelog::now_secs(),
        });
        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
        self.ensure_retention_sweeper();
        // Register the (first) shared feed so later identical subset feeds join it. Joiners wait on
        // `share_tx` for the backfill outcome.
        let (share_tx, share_rx) = tokio::sync::watch::channel(ShareOutcome::Pending);
        if let Some(sig) = feed_sig {
            st.feed_by_sig.insert(sig.clone(), id.clone());
            st.feed_shares.insert(id.clone(), FeedShare { sig, subs: Default::default(), ready: share_rx });
            st.subscribe(&id, sub.to_string(), crate::changelog::now_secs());
        }
        // Release the engine-state lock, then run the two-phase backfill+activate so the shape's
        // snapshot is readable when we return (the Electric adapter folds the stream immediately).
        // The sequencer keeps processing all tables meanwhile, buffering this shape's deltas.
        drop(st);
        let mut creating = CreateGuard::new(self, &id, table, &rec.stream_path, Registration::Sequencer);
        let outcome = match self.ds.ensure_stream(&rec.stream_path).await {
            Err(e) => Err(format!("creating shape stream: {e:#}")),
            Ok(()) => {
                backfill_and_activate(
                    &self.ds,
                    &self.pg_url,
                    &cmd_tx,
                    &ts,
                    table,
                    &id,
                    &rec.stream_path,
                    &pred,
                    out_cols.as_ref(),
                    changes_only,
                    None,
                    &self.shutdown_token(),
                    ack_rx,
                )
                .await
            }
        };
        match outcome {
            Ok(()) => {
                // The create's work is done, but it may have overlapped a degradation — its stream is
                // then already reaped and the handle would be dead on arrival. Refuse instead of
                // answering success (see `ensure_create_not_degraded`).
                if let Err(e) = self.ensure_create_not_degraded() {
                    let _ = share_tx.send(ShareOutcome::Degraded);
                    creating.rollback().await;
                    return Err(e);
                }
                // ...and the table must still have the schema this backfill was read through.
                if let Err(e) = self.ensure_schema_unchanged(&gens).await {
                    let _ = share_tx.send(ShareOutcome::Failed);
                    creating.rollback().await;
                    return Err(e);
                }
                // Durable before the guard is disarmed — see the subquery path above.
                created.await;
                // The wait is an interval of its own: re-check before acknowledging.
                if let Err(e) = self.recheck_after_durability(&id, &rec.stream_path, &gens).await {
                    // `Raced`, not `Failed` — see the subquery path.
                    let _ = share_tx.send(ShareOutcome::Raced);
                    creating.rollback().await;
                    return Err(e);
                }
                creating.complete();
                let _ = share_tx.send(ShareOutcome::Ready);
                trace_lifecycle(
                    &self.trace_tx,
                    crate::trace::GraphLifecycle::ShapeAdded { shape: rec.id.clone(), table: rec.table.clone() },
                );
                crate::statsd::create_snapshot_task(created_at.elapsed());
                Ok(rec)
            }
            Err(e) => {
                // Backfill/registration failed: wake any joiners, then undo the whole registration
                // (no zombie shape a later identical create would join) and surface the error.
                let _ = share_tx.send(ShareOutcome::Failed);
                creating.rollback().await;
                bail!("shape '{id}' creation failed: {e}")
            }
        }
    }

    /// Create a scalar **aggregation** shape (COUNT/SUM/AVG/MIN/MAX over `where`), maintained
    /// incrementally. An electric-circuits extension — not part of the Electric-compatible API. Rejects
    /// subquery predicates (use a plain filter); SUM/AVG/MIN/MAX require a column.
    pub async fn create_aggregate(
        &self,
        table: &TableRef,
        where_: Option<PredicateJson>,
        func: AggFn,
        col: Option<String>,
    ) -> Result<ShapeRecord> {
        self.create_aggregate_as(table, where_, func, col, None).await.map(|(rec, _)| rec)
    }

    /// [`Self::create_aggregate`] with a caller-chosen **subscription id** — identical semantics to
    /// [`Self::create_shape_as`] (idempotent repeat = renewal, foreign id = 409).
    pub async fn create_aggregate_as(
        &self,
        table: &TableRef,
        where_: Option<PredicateJson>,
        func: AggFn,
        col: Option<String>,
        subscription: Option<String>,
    ) -> Result<(ShapeRecord, String)> {
        let sub = self.resolve_subscription(subscription).await;
        for attempt in 1..=CREATE_RACE_ATTEMPTS {
            let res = self.create_aggregate_once(table, where_.clone(), func, col.clone(), &sub).await;
            match retry_create(res, attempt, "aggregate", table) {
                Ok(out) => return out.map(|rec| (rec, sub)),
                Err(()) => redo_backoff().await,
            }
        }
        unreachable!("retry_create returns the error on the last attempt")
    }

    async fn create_aggregate_once(
        &self,
        table: &TableRef,
        where_: Option<PredicateJson>,
        func: AggFn,
        col: Option<String>,
        sub: &str,
    ) -> Result<ShapeRecord> {
        // Same refusal as `create_shape`: a degraded engine does not get to answer for a fold over
        // rows whose membership it can no longer vouch for.
        self.ensure_not_degraded()?;
        let mut st = self.state.lock().await;
        let ts = st.tables.get(table).cloned().ok_or_else(|| anyhow::anyhow!("unknown table '{table}'"))?;
        self.ensure_schema_resolved(&st, std::slice::from_ref(table))?;
        // Captured in the SAME critical section as `ts` — deliberately, not incidentally: the lock is
        // released below for the join path's storage `HEAD`, and `ts` is what `col_idx`, the signature
        // and the compiled predicate are all derived from (see `create_shape_once`'s capture site).
        let gens = st.capture_gens(std::slice::from_ref(table));
        if where_.as_ref().is_some_and(predicate_has_subquery) {
            bail!("aggregations over subquery predicates are not supported");
        }
        let col_idx = match &col {
            Some(c) => Some(ts.column_index(c)?),
            None => None,
        };
        if matches!(func, AggFn::Sum | AggFn::Avg | AggFn::Min | AggFn::Max) && col_idx.is_none() {
            bail!("aggregation {func:?} requires a column");
        }

        // Aggregate sharing: an identical aggregation (same table, predicate, function, column) is joined
        // by ref-count — one maintained fold feeds every subscriber (e.g. the same live COUNT opened by
        // many clients).
        let agg_sig = agg_signature(table, &where_, &func, col_idx);
        // Same stream-liveness check as the row-shape join path.
        drop(st);
        self.retire_join_target_if_stream_lost(&agg_sig).await?;
        st = self.state.lock().await;
        if let Some(existing_id) = st.feed_by_sig.get(&agg_sig).cloned() {
            if let Some(rec) = st.shapes.get(&existing_id).cloned() {
                // Refuse before taking the claim — see the row-shape join path. Re-checked against
                // the `gens` captured with `ts`, so a drift inside the `HEAD` window refuses the join.
                self.ensure_schema_resolved(&st, std::slice::from_ref(table))?;
                ensure_subscription_free(&st, sub, &existing_id)?;
                let now = crate::changelog::now_secs();
                let fresh = st.subscribe(&existing_id, sub.to_string(), now);
                let ev = CatalogEvent::Joined { id: existing_id.clone(), subscription: sub.to_string(), at: now };
                // Durable for a new claim, queued for a renewal — see the row-shape join path.
                let joined = if fresh {
                    Some(self.catalog_tx.send_durable(ev))
                } else {
                    self.catalog_tx.send(ev);
                    None
                };
                let ready = st.feed_shares.get(&existing_id).expect("share entry for aggregate").ready.clone();
                drop(st);
                let mut joining = JoinGuard::new(self, &existing_id, sub, fresh);
                if let Err(e) = await_share_ready(ready, &existing_id).await {
                    joining.rollback().await;
                    return Err(e);
                }
                if let Err(e) = self.ensure_schema_unchanged(&gens).await {
                    joining.rollback().await;
                    return Err(e);
                }
                self.touch_shape(&existing_id); // aggregates never park, but the read is a touch
                if let Some(durable) = joined {
                    durable.await;
                }
                // ...and again after the wait — see `recheck_after_durability`.
                if let Err(e) = self.recheck_after_durability(&existing_id, &rec.stream_path, &gens).await {
                    joining.rollback().await;
                    return Err(e);
                }
                joining.complete();
                return Ok(rec);
            }
        }
        ensure_subscription_free(&st, sub, "")?;

        let pred = Arc::new(CompiledPredicate::compile_opt(where_.as_ref(), &ts)?);

        let num_id = st.next_shape_id;
        let id = format!("s{num_id}");
        st.next_shape_id += 1;
        let stream_path = format!("shape/{id}");
        self.ds.ensure_stream(&stream_path).await?;

        // Circuit-served path: a bare COUNT whose predicate decomposes over the table's counts
        // pipeline is seeded by summing groups and updated from group deltas — no Postgres.
        if matches!(func, AggFn::Count) && col_idx.is_none() {
            let arr = self.arrangements.lock().unwrap().clone();
            if let Some(arr) = arr {
                if let Some(gcols) = arr.counts_group_cols(table).map(|g| g.to_vec()) {
                    if let Some(constraints) = plan_circuit_agg(where_.as_ref(), &ts, &gcols) {
                        let cmd_tx = self.ensure_sequencer(&mut st).cmd_tx.clone();
                        let (ready_tx2, ready_rx2) = tokio::sync::oneshot::channel();
                        cmd_tx
                            .send(SequencerCmd::CreateCircuitAgg {
                                table: table.clone(),
                                shape_id: id.clone(),
                                stream_path: stream_path.clone(),
                                constraints,
                                ready: ready_tx2,
                            })
                            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
                        let rec = ShapeRecord {
                            id: id.clone(),
                            table: table.clone(),
                            stream_path: stream_path.clone(),
                            changes_only: false,
                            where_json: where_,
                            columns: None,
                            family_key: None,
                            is_subquery: false,
                            aggregate: Some(AggInfo { func, col }),
                            fingerprint: ts.fingerprint.clone(),
                        };
                        // Under the state lock, immediately before registering — see
                        // `ensure_create_not_degraded` for why this check and the one after the
                        // create's work are together sufficient.
                        self.ensure_not_degraded()?;
                        st.shapes.insert(id.clone(), rec.clone());
                        st.circuit_placement
                            .insert(id.clone(), CircuitPlacement { label: "counts".into(), col: None, counts: true });
                        // Durable before the create is acknowledged — see `create_shape`.
                        let created = self.catalog_tx.send_durable(CatalogEvent::Created {
                            rec: rec.clone(),
                            sig: Some(agg_sig.clone()),
                            subscription: sub.to_string(),
                            at: crate::changelog::now_secs(),
                        });
                        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
                        self.ensure_retention_sweeper();
                        let (share_tx, share_rx) = tokio::sync::watch::channel(ShareOutcome::Pending);
                        st.feed_by_sig.insert(agg_sig.clone(), id.clone());
                        st.feed_shares
                            .insert(id.clone(), FeedShare { sig: agg_sig, subs: Default::default(), ready: share_rx });
                        st.subscribe(&id, sub.to_string(), crate::changelog::now_secs());
                        drop(st);
                        let mut creating =
                            CreateGuard::new(self, &id, table, &rec.stream_path, Registration::Sequencer);
                        return match ready_rx2
                            .await
                            .unwrap_or_else(|_| Err("sequencer dropped the ready channel".to_string()))
                        {
                            Ok(()) => {
                                // The create's work is done, but it may have overlapped a degradation — its stream is
                                // then already reaped and the handle would be dead on arrival. Refuse instead of
                                // answering success (see `ensure_create_not_degraded`).
                                if let Err(e) = self.ensure_create_not_degraded() {
                                    let _ = share_tx.send(ShareOutcome::Degraded);
                                    creating.rollback().await;
                                    return Err(e);
                                }
                                if let Err(e) = self.ensure_schema_unchanged(&gens).await {
                                    let _ = share_tx.send(ShareOutcome::Failed);
                                    creating.rollback().await;
                                    return Err(e);
                                }
                                created.await;
                                // The wait is an interval of its own: re-check before acknowledging.
                                if let Err(e) = self.recheck_after_durability(&id, &rec.stream_path, &gens).await {
                                    // `Raced`, not `Failed` — see the subquery path.
                                    let _ = share_tx.send(ShareOutcome::Raced);
                                    creating.rollback().await;
                                    return Err(e);
                                }
                                creating.complete();
                                let _ = share_tx.send(ShareOutcome::Ready);
                                trace_lifecycle(
                                    &self.trace_tx,
                                    crate::trace::GraphLifecycle::ShapeAdded {
                                        shape: rec.id.clone(),
                                        table: rec.table.clone(),
                                    },
                                );
                                Ok(rec)
                            }
                            Err(e) => {
                                let _ = share_tx.send(ShareOutcome::Failed);
                                creating.rollback().await;
                                bail!("aggregate '{id}' creation failed: {e}")
                            }
                        };
                    }
                }
            }
        }

        let cmd_tx = self.ensure_sequencer(&mut st).cmd_tx.clone();
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: table.clone(),
                shape_id: id.clone(),
                num_id,
                stream_path: stream_path.clone(),
                pred: pred.clone(),
                out_cols: None,
                kind: CreateKind::Aggregate { func, col: col_idx },
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;

        let stream_path_c = stream_path.clone();
        let rec = ShapeRecord {
            id: id.clone(),
            table: table.clone(),
            stream_path,
            changes_only: false,
            where_json: where_,
            columns: None,
            family_key: None,
            is_subquery: false,
            aggregate: Some(AggInfo { func, col }),
            fingerprint: ts.fingerprint.clone(),
        };
        // Under the state lock, immediately before registering — see `ensure_create_not_degraded`
        // for why this check and the one after the create's work are together sufficient.
        self.ensure_not_degraded()?;
        st.shapes.insert(id.clone(), rec.clone());
        // Durable before the create is acknowledged — see `create_shape`.
        let created = self.catalog_tx.send_durable(CatalogEvent::Created {
            rec: rec.clone(),
            sig: Some(agg_sig.clone()),
            subscription: sub.to_string(),
            at: crate::changelog::now_secs(),
        });
        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
        self.ensure_retention_sweeper();
        // Register this (first) aggregate so later identical ones join it by ref-count.
        let (share_tx, share_rx) = tokio::sync::watch::channel(ShareOutcome::Pending);
        st.feed_by_sig.insert(agg_sig.clone(), id.clone());
        st.feed_shares.insert(id.clone(), FeedShare { sig: agg_sig, subs: Default::default(), ready: share_rx });
        st.subscribe(&id, sub.to_string(), crate::changelog::now_secs());
        drop(st);
        let mut creating = CreateGuard::new(self, &id, table, &rec.stream_path, Registration::Sequencer);
        let outcome = backfill_and_activate(
            &self.ds,
            &self.pg_url,
            &cmd_tx,
            &ts,
            table,
            &id,
            &stream_path_c,
            &pred,
            None,
            false,
            Some((func, col_idx)),
            &self.shutdown_token(),
            ack_rx,
        )
        .await;
        match outcome {
            Ok(()) => {
                // The create's work is done, but it may have overlapped a degradation — its stream is
                // then already reaped and the handle would be dead on arrival. Refuse instead of
                // answering success (see `ensure_create_not_degraded`).
                if let Err(e) = self.ensure_create_not_degraded() {
                    let _ = share_tx.send(ShareOutcome::Degraded);
                    creating.rollback().await;
                    return Err(e);
                }
                if let Err(e) = self.ensure_schema_unchanged(&gens).await {
                    let _ = share_tx.send(ShareOutcome::Failed);
                    creating.rollback().await;
                    return Err(e);
                }
                created.await;
                // The wait is an interval of its own: re-check before acknowledging.
                if let Err(e) = self.recheck_after_durability(&id, &rec.stream_path, &gens).await {
                    // `Raced`, not `Failed` — see the subquery path.
                    let _ = share_tx.send(ShareOutcome::Raced);
                    creating.rollback().await;
                    return Err(e);
                }
                creating.complete();
                let _ = share_tx.send(ShareOutcome::Ready);
                trace_lifecycle(
                    &self.trace_tx,
                    crate::trace::GraphLifecycle::ShapeAdded { shape: rec.id.clone(), table: rec.table.clone() },
                );
                Ok(rec)
            }
            Err(e) => {
                let _ = share_tx.send(ShareOutcome::Failed);
                creating.rollback().await;
                bail!("aggregate '{id}' creation failed: {e}")
            }
        }
    }

    /// The **legacy anonymous** release: drop one subscription without being told which
    /// (`DELETE /shapes/{id}` with no `subscription`, the `/v1/shape` adapter's handle eviction).
    ///
    /// Kept for callers that never learned their subscription id, and NOT retry-safe: nothing
    /// identifies the claim the caller meant, so a repeat drops a second one. Engine-minted claims
    /// go first (see [`EngineState::unsubscribe_anonymous`]) so it cannot steal an identified
    /// subscriber's. Prefer [`Self::release_subscription`].
    pub async fn release_shape(&self, id: &str) {
        self.release_subscription(id, None).await;
    }

    /// Release one subscription on a shape (extended-API `DELETE /shapes/{id}?subscription=…`).
    /// An empty live set does **not** tear the shape down: it stays active (a brief reconnect
    /// rejoins it warm), goes dormant after the retention idle timeout, and is eventually evicted
    /// by the layered policy (see `crate::retention`). Releasing is also a touch, so the idle
    /// countdown starts at the disconnect. Infallible: it only adjusts in-memory state.
    ///
    /// **Idempotent** (ADR-0008): releasing an id the shape does not hold changes nothing and
    /// records nothing, so a client whose success response was lost may simply ask again — the
    /// double-delete that used to steal another subscriber's claim is gone.
    ///
    /// This engine-internal form queues `Left`. Native HTTP uses
    /// [`Self::release_subscription_durable`] instead: an acknowledged release must survive even
    /// when leases are disabled with `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS=0`.
    pub async fn release_subscription(&self, id: &str, sub: Option<&str>) {
        self.release_subscription_inner(id, sub, false).await;
    }

    /// Native client-facing release: do not acknowledge until `Left` is in the restart contract.
    ///
    /// Unlike the purge, this needs nothing spawned: the whole mutation is the in-memory unsubscribe
    /// plus the record, both of which happen BEFORE the wait, and nothing runs after it. A client
    /// that times out and drops this future therefore loses only its answer — the `Left` is the
    /// writer's from the moment it was enqueued, and lands regardless.
    pub async fn release_subscription_durable(&self, id: &str, sub: Option<&str>) {
        self.release_subscription_inner(id, sub, true).await;
    }

    async fn release_subscription_inner(&self, id: &str, sub: Option<&str>, durable: bool) {
        let mut st = self.state.lock().await;
        let released = match sub {
            Some(s) => st.unsubscribe(id, s).then(|| s.to_string()),
            None => st.unsubscribe_anonymous(id),
        };
        let wait = released.and_then(|subscription| {
            let event = CatalogEvent::Left { id: id.to_string(), subscription, lapsed: false };
            if durable {
                Some(self.catalog_tx.send_durable(event))
            } else {
                self.catalog_tx.send(event);
                None
            }
        });
        let needs_barrier = durable && wait.is_none();
        drop(st);
        self.touch_shape(id);
        if let Some(wait) = wait {
            wait.await;
        } else if needs_barrier {
            self.catalog_tx.wait_durable().await;
        }
    }

    /// Force-drop a shape NOW, bypassing the retention lifecycle: full teardown (record, share
    /// entries, lifecycle entry, sequencer routing, subquery-registry entry, durable stream)
    /// regardless of refcount or lifecycle state. An admin/debug operation (`DELETE
    /// /shapes/{id}?purge=true`, the visualizer's trash button) — subscribed clients see their
    /// stream vanish and recreate via the normal 404 / must-refetch path. The sequencer command
    /// queue is FIFO, so a purge ordered after an in-flight resume removes whatever the resume
    /// registered.
    ///
    /// This engine-internal form queues `Dropped`; schema drift, `TRUNCATE`, epoch reset and
    /// retention already provide their own completion barriers. Native HTTP uses
    /// [`Self::purge_shape_durable`] so its success response is a restart-safe promise, including
    /// when leases are disabled.
    ///
    /// It does NOT wait on the retirement either: the stream may still be being deleted in the
    /// background when this returns, and `GET /shapes/{id}` is 404 from now on regardless.
    pub async fn purge_shape(&self, id: &str) -> Result<()> {
        self.purge_shape_inner(id, false).await
    }

    /// Native client-facing purge: durably record `Dropped` before acknowledging or retiring.
    pub async fn purge_shape_durable(&self, id: &str) -> Result<()> {
        self.purge_shape_inner(id, true).await
    }

    async fn purge_shape_inner(&self, id: &str, durable: bool) -> Result<()> {
        let mut st = self.state.lock().await;
        self.lives.lock().unwrap().remove(id);
        st.forget_subscriptions(id);
        if let Some(share) = st.feed_shares.remove(id) {
            st.feed_by_sig.remove(&share.sig);
        }
        let removed = st.shapes.remove(id);
        st.circuit_placement.remove(id);
        // The `Dropped` INTENT goes first, always, and the retirement follows it (ADR-0007): a crash
        // between the two leaves a record the boot can act on, the other order leaves an orphan.
        let wait = removed.as_ref().and_then(|_| {
            let event = CatalogEvent::Dropped { id: id.to_string() };
            if durable {
                Some(self.catalog_tx.send_durable(event))
            } else {
                self.catalog_tx.send(event);
                None
            }
        });
        if let Some(rec) = &removed {
            if let Some(seq) = st.sequencer.as_ref() {
                let _ =
                    seq.cmd_tx.send(SequencerCmd::RemoveShape { table: rec.table.clone(), shape_id: id.to_string() });
            }
        }
        drop(st);
        if durable {
            // The teardown this purge promised must NOT depend on the request that asked for it.
            // Giving up on a `DELETE` while storage is down is exactly what a client does during an
            // outage, and that drops this handler future — but the `Dropped` still lands, because
            // the writer owns it. Left inline, the retirement and the subquery-registry removal
            // would simply never run in this process: the stream would linger alive until the next
            // boot's GC and a subquery shape would stay registered and maintained as a zombie. So
            // the work after the durability wait is owned by the PROCESS, not by the request.
            let (engine, owned) = (self.clone(), id.to_string());
            tokio::spawn(async move {
                // ADR-0007 order is preserved inside the task: `Dropped` durable, then close/delete.
                if let Some(wait) = wait {
                    wait.await;
                }
                engine.finish_purge(&owned, removed).await;
            });
            // The request waits only for the durability BARRIER — everything enqueued up to and
            // including that `Dropped` is in the restart contract when this returns. A concurrent
            // retry finds `removed == None`, enqueues nothing, and waits on the same barrier.
            self.catalog_tx.wait_durable().await;
        } else {
            self.finish_purge(id, removed).await;
        }
        Ok(())
    }

    /// The half of a purge that must outlive the request that asked for it: registry cleanup, the
    /// stream retirement, the trace. Ordered strictly AFTER the `Dropped` intent (ADR-0007) — a
    /// crash between the two leaves a record the boot can act on, the other order leaves an orphan.
    async fn finish_purge(&self, id: &str, removed: Option<ShapeRecord>) {
        // Subquery shapes live in the registry (a no-op for plain shapes).
        self.subqueries.lock().await.drop_subquery_shape(id).await;
        if let Some(rec) = removed {
            // Retirement: close (releasing any tailing long-poll with `stream-closed`) then delete,
            // and record the completion. A storage failure is queued, never forgotten.
            self.retire_shape_stream(id, &rec.stream_path).await;
            trace_lifecycle(&self.trace_tx, crate::trace::GraphLifecycle::ShapeDropped { shape: id.to_string() });
            tracing::info!("purged shape {id} (forced)");
        }
    }

    /// Renew a subscription's lease **in memory only** — no catalog event (ADR-0008).
    ///
    /// For a subscriber the engine can actually SEE: the `/v1/shape` adapter, whose every poll goes
    /// through the engine, unlike a native subscriber reading durable-streams directly. Its handle
    /// is the liveness signal, so the poll renews the claim the handle holds and
    /// `subscriptions_live` keeps describing reality rather than decaying to zero under a client
    /// that is plainly still there.
    ///
    /// Nothing is recorded because there is nothing a restart could use it for: an Electric handle
    /// does not survive one (the registry is in-memory, and a returning client gets `must-refetch`
    /// and re-snapshots, which takes a fresh claim). A claim that lapsed before this call is
    /// re-taken here for the same reason — the handle is demonstrably live — and if the shape is
    /// gone entirely this is a no-op, which is exactly the `must-refetch` path.
    pub(crate) async fn renew_subscription_local(&self, id: &str, sub: &str) {
        let mut st = self.state.lock().await;
        st.subscribe(id, sub.to_string(), crate::changelog::now_secs());
    }

    /// Record an engine-visible read of a shape (drives the retention idle timer + LRU order).
    pub(crate) fn touch_shape(&self, id: &str) {
        if let Some(life) = self.lives.lock().unwrap().get_mut(id) {
            life.last_read = std::time::Instant::now();
        }
    }

    /// The shape's retention lifecycle, for introspection (`GET /shapes/{id}`).
    pub async fn shape_lifecycle(&self, id: &str) -> Option<&'static str> {
        self.lives.lock().unwrap().get(id).map(|l| match l.state {
            LifeState::Active => "active",
            LifeState::Deactivating { .. } => "deactivating",
            LifeState::Dormant { .. } => "dormant",
            LifeState::Reactivating { .. } => "reactivating",
        })
    }

    /// Make sure a shape is active, reactivating it from dormancy if needed ("any touch
    /// reactivates"): replay the change log from the shape's resume position through its predicate
    /// onto the retained stream — no Postgres backfill — then re-register it for live routing.
    /// Concurrent touches coalesce onto one replay; a touch during deactivation waits for the
    /// transition to settle first. Also refreshes `last_read`.
    pub async fn ensure_active(&self, id: &str) -> Result<()> {
        loop {
            enum Step {
                Done,
                WaitDeactivate(tokio::sync::watch::Receiver<bool>),
                WaitReactivate(tokio::sync::watch::Receiver<Option<bool>>),
            }
            let step = {
                let mut lives = self.lives.lock().unwrap();
                match lives.get_mut(id) {
                    // Unknown to retention (already evicted, or never tracked): nothing to do here —
                    // the caller's own record lookup decides between 404 and normal service.
                    None => Step::Done,
                    Some(life) => {
                        life.last_read = std::time::Instant::now();
                        match &life.state {
                            LifeState::Active => Step::Done,
                            LifeState::Deactivating { done } => Step::WaitDeactivate(done.clone()),
                            LifeState::Reactivating { done, .. } => Step::WaitReactivate(done.clone()),
                            LifeState::Dormant { resume, gate, .. } => {
                                // Kick off the replay in a DETACHED task: `ensure_active` futures
                                // are dropped when an HTTP client disconnects, and a cancelled
                                // in-place replay would strand the shape in `Reactivating`. The
                                // task always settles the lifecycle state and publishes the
                                // outcome; this caller then awaits THIS attempt's channel like any
                                // concurrent toucher.
                                let resume = resume.clone();
                                let gate = gate.clone();
                                let (tx, rx) = tokio::sync::watch::channel(None);
                                life.state = LifeState::Reactivating { done: rx.clone(), resume: resume.clone() };
                                let engine = self.clone();
                                let id = id.to_string();
                                tokio::spawn(async move {
                                    let res = engine.resume_dormant(&id, resume.clone(), gate.clone()).await;
                                    let err = match res {
                                        Ok(()) => {
                                            let mut lives = engine.lives.lock().unwrap();
                                            if let Some(life) = lives.get_mut(&id) {
                                                life.state = LifeState::Active;
                                                life.last_read = std::time::Instant::now();
                                            }
                                            drop(lives);
                                            let _ = tx.send(Some(true));
                                            return;
                                        }
                                        Err(e) => e,
                                    };
                                    // The replay's resume SEGMENT is gone (ADR-0006). Nothing can
                                    // bring this shape up to date — the changes it is missing are
                                    // not anywhere any more — so it is evicted rather than parked
                                    // back as dormant with a resume position that will 404 on every
                                    // future touch. Subscribers get 404 / `stream-closed` and
                                    // recreate, which backfills them from Postgres.
                                    if crate::ds::is_stream_gone(&err) {
                                        tracing::error!(
                                            "reactivating shape {id}: its change-log resume segment {} is gone \
                                             ({err:#}); evicting the shape — it can never be brought up to date",
                                            crate::changelog::segment_path(resume.segment)
                                        );
                                        // Put it back as dormant first: `evict_shape` only evicts a
                                        // settled shape, and this one is still `Reactivating`.
                                        if let Some(life) = engine.lives.lock().unwrap().get_mut(&id) {
                                            life.state = LifeState::Dormant {
                                                since: std::time::Instant::now(),
                                                resume: resume.clone(),
                                                gate: gate.clone(),
                                            };
                                        }
                                        if let Err(e) = engine.evict_shape(&id, EvictReason::ChangeLogRetention).await {
                                            tracing::warn!("evicting unresumable shape {id} failed: {e:#}");
                                        }
                                        let _ = tx.send(Some(false));
                                        return;
                                    }
                                    tracing::warn!("reactivating shape {id} failed: {err:#}");
                                    // Restore the dormant resume state so a later touch retries.
                                    if let Some(life) = engine.lives.lock().unwrap().get_mut(&id) {
                                        life.state =
                                            LifeState::Dormant { since: std::time::Instant::now(), resume, gate };
                                    }
                                    let _ = tx.send(Some(false));
                                });
                                Step::WaitReactivate(rx)
                            }
                        }
                    }
                }
            };
            match step {
                Step::Done => return Ok(()),
                Step::WaitDeactivate(mut rx) => {
                    // Deactivation in flight: wait for it to settle, then loop (we'll see Dormant).
                    while !*rx.borrow_and_update() {
                        if rx.changed().await.is_err() {
                            break; // deactivator vanished; re-inspect the state
                        }
                    }
                }
                Step::WaitReactivate(mut rx) => loop {
                    let outcome = *rx.borrow_and_update();
                    match outcome {
                        Some(true) => return Ok(()),
                        Some(false) => bail!("shape '{id}' reactivation failed; retry the read"),
                        None => {
                            if rx.changed().await.is_err() {
                                bail!("shape '{id}' reactivator died; retry the read");
                            }
                        }
                    }
                },
            }
        }
    }

    /// The replay half of a reactivation: re-register the shape through the sequencer's two-phase
    /// pending-buffer handshake, but replay the change log from the dormant resume position instead
    /// of taking a Postgres snapshot (following rotation pointers across segments, ADR-0006). Live deltas arriving during the replay buffer in the pending
    /// shape and drain through the same gate at activation; any overlap between the replay and the
    /// buffer double-applies only absolute per-pk upserts/deletes — idempotent for stream readers.
    /// Split from [`ensure_active`] so the lifecycle bookkeeping stays in one place.
    pub(crate) async fn resume_dormant(
        &self,
        id: &str,
        resume: LogPosition,
        gate: crate::pg::SnapshotGate,
    ) -> Result<()> {
        let (rec, ts, pred, out_cols, num_id, cmd_tx, gens) = {
            let mut st = self.state.lock().await;
            let rec =
                st.shapes.get(id).cloned().with_context(|| format!("shape '{id}' vanished during reactivation"))?;
            self.ensure_schema_resolved(&st, std::slice::from_ref(&rec.table))?;
            let gens = st.capture_gens(std::slice::from_ref(&rec.table));
            let ts = st.tables.get(&rec.table).cloned().with_context(|| format!("unknown table '{}'", rec.table))?;
            let pred = Arc::new(CompiledPredicate::compile_opt(rec.where_json.as_ref(), &ts)?);
            let out_cols = resolve_columns(&ts, rec.columns.clone())?;
            let num_id: u64 = id.strip_prefix('s').and_then(|n| n.parse().ok()).context("unparseable shape id")?;
            let cmd_tx = self.ensure_sequencer(&mut st).cmd_tx.clone();
            (rec, ts, pred, out_cols, num_id, cmd_tx, gens)
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: rec.table.clone(),
                shape_id: id.to_string(),
                num_id,
                stream_path: rec.stream_path.clone(),
                pred: pred.clone(),
                out_cols: out_cols.clone(),
                kind: CreateKind::Plain,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
        ack_rx.await.map_err(|_| anyhow::anyhow!("sequencer dropped the begin-shape ack"))?;
        // Replay everything the retained stream is missing (buffering live deltas meanwhile).
        let emitted = match replay_changes_for_shape(
            &self.ds,
            &ts,
            &rec.table,
            &pred,
            out_cols.as_ref(),
            &gate,
            &rec.stream_path,
            &resume,
            self.pg_url.is_none(),
            &self.shutdown_token(),
        )
        .await
        {
            Ok(n) => n,
            Err(e) => {
                let _ = cmd_tx.send(SequencerCmd::AbortShape { table: rec.table.clone(), shape_id: id.to_string() });
                return Err(e.context(format!("shape '{id}' reactivation replay failed")));
            }
        };
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::ActivateShape {
                table: rec.table.clone(),
                shape_id: id.to_string(),
                gate,
                // A reactivation replays the change log; there is no backfill to seed a fold from
                // (and an aggregate never goes dormant, so this is always a plain shape).
                agg_seed: None,
                emitted_seed: emitted,
                ready: ready_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
        ready_rx
            .await
            .unwrap_or_else(|_| Err("sequencer dropped the ready channel".to_string()))
            .map_err(|e| anyhow::anyhow!("shape '{id}' reactivation failed: {e}"))?;
        // The replay read the change log through the schema captured above; if the table drifted
        // meanwhile the shape has been retired underneath us and must not be reported live.
        if let Err(e) = self.ensure_schema_unchanged(&gens).await {
            let _ = cmd_tx.send(SequencerCmd::RemoveShape { table: rec.table.clone(), shape_id: id.to_string() });
            return Err(e.context(format!("shape '{id}' reactivation")));
        }
        self.catalog_tx.send(CatalogEvent::Reactivated { id: id.to_string() });
        metrics().shapes_reactivated.fetch_add(1, Ordering::Relaxed);
        trace_lifecycle(
            &self.trace_tx,
            crate::trace::GraphLifecycle::ShapeReactivated { shape: id.to_string(), table: rec.table.clone() },
        );
        tracing::info!("reactivated dormant shape {id} (table {})", rec.table);
        Ok(())
    }

    /// Move an idle refcount-0 shape from active to dormant: the sequencer unregisters its
    /// routing and hands back the resume state (fully-processed change-log position + the shape's
    /// snapshot gate); the stream and record are retained. Rechecks eligibility under the locks —
    /// a touch or rejoin racing the sweep wins.
    ///
    /// Parking is NOT retirement: the retained stream is never closed, because reactivation appends
    /// the replayed changes to it (closing is terminal for appends).
    pub(crate) async fn deactivate_shape(&self, id: &str) -> Result<()> {
        let st = self.state.lock().await;
        let Some(rec) = st.shapes.get(id).cloned() else { return Ok(()) }; // already gone
        if rec.is_subquery || rec.aggregate.is_some() {
            return Ok(()); // never dormant (state not rebuildable from a bounded replay)
        }
        if st.feed_shares.get(id).is_some_and(|s| s.refcount() > 0) {
            return Ok(()); // resubscribed since the sweep snapshot
        }
        let Some(cmd_tx) = st.sequencer.as_ref().map(|s| s.cmd_tx.clone()) else { return Ok(()) };
        let (done_tx, done_rx) = tokio::sync::watch::channel(false);
        {
            let mut lives = self.lives.lock().unwrap();
            let Some(life) = lives.get_mut(id) else { return Ok(()) };
            if !matches!(life.state, LifeState::Active) || life.last_read.elapsed() < self.retention.idle_timeout {
                return Ok(()); // touched or already transitioning since the sweep snapshot
            }
            life.state = LifeState::Deactivating { done: done_rx };
        }
        drop(st);

        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let sent = cmd_tx
            .send(SequencerCmd::DeactivateShape { table: rec.table.clone(), shape_id: id.to_string(), resp: resp_tx })
            .is_ok();
        let resume = if sent { resp_rx.await.ok().flatten() } else { None };
        let mut lives = self.lives.lock().unwrap();
        let Some(life) = lives.get_mut(id) else { return Ok(()) };
        match resume {
            Some((resume, gate)) => {
                life.state =
                    LifeState::Dormant { since: std::time::Instant::now(), resume: resume.clone(), gate: gate.clone() };
                drop(lives);
                self.catalog_tx.send(CatalogEvent::Dormant { id: id.to_string(), resume, gate });
                metrics().shapes_dormanted.fetch_add(1, Ordering::Relaxed);
                trace_lifecycle(&self.trace_tx, crate::trace::GraphLifecycle::ShapeDormant { shape: id.to_string() });
                tracing::debug!("shape {id} went dormant (idle)");
            }
            None => {
                // The sequencer didn't know the shape (or is gone): leave it active. Reset the
                // idle clock so the sweep backs off a full idle window instead of re-attempting
                // (and re-warning) every sweep.
                life.state = LifeState::Active;
                life.last_read = std::time::Instant::now();
                drop(lives);
                tracing::warn!("deactivating shape {id}: sequencer returned no resume state; left active");
            }
        }
        let _ = done_tx.send(true);
        Ok(())
    }

    /// Evict a shape: delete its record, share entries, lifecycle entry, and durable stream. A
    /// returning `/v1/shape` client gets `409 must-refetch`; an extended-API client gets `404` and
    /// recreates. Normally only **dormant** shapes are evicted; the exception is non-parkable
    /// shapes (subquery / aggregate — see [`crate::retention`]), which the TTL layer evicts
    /// straight from active with a full teardown. Rechecks eligibility under the locks — a
    /// reactivation or rejoin racing the sweep wins.
    pub(crate) async fn evict_shape(&self, id: &str, reason: EvictReason) -> Result<Evicted> {
        let mut st = self.state.lock().await;
        // No record: the shape is already gone, so whatever it held (a change-log segment pin
        // included) is released — that IS the outcome the caller asked for.
        let Some(rec) = st.shapes.get(id).cloned() else { return Ok(Evicted::Yes) };
        let parkable = !rec.is_subquery && rec.aggregate.is_none();
        {
            let mut lives = self.lives.lock().unwrap();
            let evictable = match lives.get(id) {
                Some(life) if matches!(life.state, LifeState::Dormant { .. }) => true,
                // A non-parkable shape is evicted from active only if it is still idle past the
                // full grace window (a touch since the sweep snapshot wins).
                Some(life) if !parkable && matches!(life.state, LifeState::Active) => {
                    life.last_read.elapsed() >= self.retention.idle_timeout + self.retention.dormant_ttl
                }
                _ => false, // transitioning (or already evicted) since the sweep snapshot
            };
            if !evictable {
                return Ok(Evicted::Skipped);
            }
            if st.feed_shares.get(id).is_some_and(|s| s.refcount() > 0) {
                return Ok(Evicted::Skipped);
            }
            lives.remove(id);
        }
        st.forget_subscriptions(id);
        if let Some(share) = st.feed_shares.remove(id) {
            st.feed_by_sig.remove(&share.sig);
        }
        let removed = st.shapes.remove(id);
        st.circuit_placement.remove(id);
        if removed.is_some() {
            self.catalog_tx.send(CatalogEvent::Dropped { id: id.to_string() });
        }
        // A dormant shape is already unregistered from the sequencer; a non-parkable one is still
        // live and needs the full teardown (sequencer routing for aggregates, registry for subqueries).
        if !parkable {
            if let Some(seq) = st.sequencer.as_ref() {
                let _ =
                    seq.cmd_tx.send(SequencerCmd::RemoveShape { table: rec.table.clone(), shape_id: id.to_string() });
            }
        }
        drop(st);
        if !parkable {
            self.subqueries.lock().await.drop_subquery_shape(id).await;
        }
        if let Some(rec) = removed {
            // Eviction is terminal (unlike deactivation), so the stream is retired: closed, then
            // deleted — a client still tailing it is released at once with `stream-closed` — and the
            // completion recorded. A storage failure is queued, never forgotten.
            self.retire_shape_stream(id, &rec.stream_path).await;
            metrics().shapes_evicted.fetch_add(1, Ordering::Relaxed);
            trace_lifecycle(&self.trace_tx, crate::trace::GraphLifecycle::ShapeDropped { shape: id.to_string() });
            tracing::info!("evicted shape {id} ({})", reason.as_str());
        }
        Ok(Evicted::Yes)
    }

    /// One retention sweep: snapshot every shape's status, run the pure layered policy
    /// ([`crate::retention::plan_sweep`]), then execute the plan. Public so a harness can force a
    /// sweep instead of waiting for the background interval.
    pub async fn retention_sweep(&self) {
        let cfg = self.retention.clone();
        self.sweep_leases(&cfg).await;
        let snapshot: Vec<SweepShape> = {
            let st = self.state.lock().await;
            let bytes = self.ds.appended_bytes_with_prefix("shape/");
            let lives = self.lives.lock().unwrap();
            st.shapes
                .values()
                .map(|rec| {
                    let life = lives.get(&rec.id);
                    let (idle, dormant_for, in_transition) = match life {
                        None => (std::time::Duration::ZERO, None, true), // mid-create; leave alone
                        Some(l) => match &l.state {
                            LifeState::Active => (l.last_read.elapsed(), None, false),
                            LifeState::Dormant { since, .. } => (l.last_read.elapsed(), Some(since.elapsed()), false),
                            LifeState::Deactivating { .. } | LifeState::Reactivating { .. } => {
                                (l.last_read.elapsed(), None, true)
                            }
                        },
                    };
                    SweepShape {
                        id: rec.id.clone(),
                        refcount: st.feed_shares.get(&rec.id).map(FeedShare::refcount).unwrap_or(0),
                        idle,
                        dormant_for,
                        in_transition,
                        dormancy_eligible: !rec.is_subquery && rec.aggregate.is_none(),
                        stream_bytes: bytes.get(&rec.stream_path).copied().unwrap_or(0),
                    }
                })
                .collect()
        };
        let plan = crate::retention::plan_sweep(&cfg, &snapshot);
        if plan.over_capacity {
            metrics().retention_pressure.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                "retention: {} shapes exceed max_shapes={} but nothing dormant is left to evict — \
                 every shape is actively subscribed or recently read; raise ELECTRIC_CIRCUITS_MAX_SHAPES or lower the idle timeout",
                snapshot.len(),
                cfg.max_shapes
            );
        }
        if plan.over_budget {
            metrics().retention_pressure.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                "retention: shape streams exceed the disk budget ({} bytes) but nothing dormant is left to evict — \
                 raise ELECTRIC_CIRCUITS_SHAPE_DISK_BUDGET_MB or lower the idle timeout",
                cfg.disk_budget_bytes
            );
        }
        for id in &plan.deactivate {
            if let Err(e) = self.deactivate_shape(id).await {
                tracing::warn!("retention: deactivating shape {id} failed: {e:#}");
            }
        }
        for (id, reason) in &plan.evict {
            if let Err(e) = self.evict_shape(id, *reason).await {
                tracing::warn!("retention: evicting shape {id} failed: {e:#}");
            }
        }
        // Same tick, second half: the change log's own retention (ADR-0006).
        self.sweep_change_log().await;
    }

    /// The lease half of a sweep (ADR-0008): release every subscription that has not been renewed
    /// within [`RetentionConfig::idle_timeout`], exactly as an explicit `DELETE` would.
    ///
    /// This is the only liveness signal the engine has for a native subscriber. Its reads go
    /// straight to durable-streams, so an un-renewed subscription is indistinguishable from a
    /// client that vanished — and treating it as live pins the shape (and its stream, and its
    /// change-log segment) for ever. A shape whose live set empties this way then follows the
    /// ordinary lifecycle: idle → dormant → evicted, with the same grace as any other.
    ///
    /// A lapse is a `Left` like any other, marked `lapsed` so the durable record says who released
    /// it. `idle_timeout == 0` disables dormancy, and with it leases: an engine that never parks a
    /// shape has no use for the signal.
    async fn sweep_leases(&self, cfg: &crate::retention::RetentionConfig) {
        let lapsed = {
            let mut st = self.state.lock().await;
            let now = crate::changelog::now_secs();
            let expired = st.lapsed_subscriptions(cfg.idle_timeout, now);
            for (shape, sub) in &expired {
                st.unsubscribe(shape, sub);
            }
            st.publish_subscription_gauge();
            expired
        };
        for (shape, subscription) in lapsed {
            tracing::debug!(
                "retention: subscription {subscription} on shape {shape} was not renewed within {:?}; released",
                cfg.idle_timeout
            );
            self.catalog_tx.send(CatalogEvent::Left { id: shape, subscription, lapsed: true });
            metrics().subscriptions_lapsed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Delete the change-log segments nothing can resume inside any more, evicting first the
    /// dormant shapes that would pin one past the retain window (ADR-0006).
    ///
    /// The decision itself is [`crate::changelog::plan_segment_deletion`] — pure, so the interlock
    /// (floor, pins, retain window, never the current segment) is unit-tested without storage. This
    /// half snapshots the inputs, executes, and enforces three things the pure planner cannot:
    ///
    /// - **the floor is the DURABLE checkpoint**, not the sequencer's in-memory position. The
    ///   sequencer publishes a crossing the instant it happens, but the checkpoint that survives a
    ///   restart is an async catalog append; deleting on the in-memory position would let a sweep
    ///   delete segment `n` while a restart would still resume inside it;
    /// - **the plan is recomputed after the evictions**, from a fresh snapshot and with the retain
    ///   window switched off, so what is deleted is only what the pins that *actually remain* leave
    ///   unpinned. An eviction that turned into a no-op (the shape was touched between planning and
    ///   eviction, and is now reactivating) therefore cannot license deleting its segment;
    /// - **nothing is deleted while a shape is `Deactivating`**. Its resume position is captured by
    ///   the sequencer and only lands in `LifeState::Dormant` a moment later; in that window it pins
    ///   nothing observable, so the sweep simply waits for the next tick.
    pub(crate) async fn sweep_change_log(&self) {
        let current = self.changes.state().current();
        let has_sequencer = self.state.lock().await.sequencer.is_some();
        // With no sequencer there is no consumer of the log at all — a Postgres-mode engine over
        // which no shape has ever been created, or one whose restored catalog held none. The log
        // still grows under the ingestor, so "nothing is deletable" would break the disk bound.
        // The floor is then the CURRENT segment: nothing can ever need an earlier one, because a
        // shape created later backfills from Postgres behind its own snapshot gate and starts
        // reading from wherever the sequencer is spawned. Advance the DURABLE checkpoint to say so
        // — that is what a later sequencer starts from, and it is also what licenses the delete.
        if !has_sequencer {
            let target = LogPosition::start_of(current);
            if self.catalog_tx.durable_offset().is_none_or(|d| d < target) {
                // Both halves of "a sequencer spawned later starts here": the durable record (which
                // is also what licenses the delete, once it lands) and the in-memory start position
                // `ensure_sequencer` reads.
                *self.seq_start.lock().unwrap() = target.clone();
                // No highwater: nothing has been applied at this position — there is no sequencer.
                // Both halves move together, in memory and durably, so a sequencer spawned later
                // starts from exactly what the catalog says (ADR-0003).
                *self.seq_highwater.lock().unwrap() = None;
                self.catalog_tx.send(CatalogEvent::Offset { pos: target, highwater: None });
            }
        }
        // The floor. `None` = nothing durable yet (a first boot that has not checkpointed), which
        // reads as segment 0 — i.e. nothing is deletable, which is the safe answer.
        let floor = self.catalog_tx.durable_offset().map(|p| p.segment).unwrap_or(0);
        let segments = self.changes.state().segments();
        let plan = crate::changelog::plan_segment_deletion(
            &segments,
            current,
            floor,
            &self.segment_pins(),
            self.changes.config().retain,
            crate::changelog::now_secs(),
        );
        let mut evicted_all = true;
        for id in &plan.evict {
            tracing::info!(
                "change log: evicting dormant shape {id} — its resume segment was rotated out more than {:?} ago",
                self.changes.config().retain
            );
            match self.evict_shape(id, EvictReason::ChangeLogRetention).await {
                Ok(Evicted::Yes) => {}
                Ok(Evicted::Skipped) => {
                    tracing::info!("change log: shape {id} was touched or re-subscribed; its pin stands");
                    evicted_all = false;
                }
                Err(e) => {
                    tracing::warn!("change log: evicting shape {id} failed: {e:#}");
                    evicted_all = false;
                }
            }
        }
        // Re-snapshot and re-plan with the retain window OFF: every pin that still exists now
        // counts, however old. This is the step that makes a failed/skipped eviction harmless.
        let pins = self.segment_pins();
        let deletable = crate::changelog::plan_segment_deletion(
            &self.changes.state().segments(),
            self.changes.state().current(),
            self.catalog_tx.durable_offset().map(|p| p.segment).unwrap_or(0),
            &pins,
            std::time::Duration::ZERO,
            crate::changelog::now_secs(),
        );
        let deactivating =
            self.lives.lock().unwrap().values().any(|l| matches!(l.state, LifeState::Deactivating { .. }));
        if !evicted_all || deactivating {
            if !deletable.delete.is_empty() {
                tracing::info!(
                    "change log: deferring {} segment deletion(s) to the next sweep ({})",
                    deletable.delete.len(),
                    if deactivating { "a shape is mid-deactivation" } else { "a planned eviction did not happen" }
                );
            }
        } else {
            for segment in &deletable.delete {
                let path = crate::changelog::segment_path(*segment);
                // Retirement (close, then delete): closing a rotated-out segment is a no-op (the
                // rotation closed it) but keeps every engine-initiated removal on one path.
                match self.ds.retire_stream(&path).await {
                    Ok(()) => {
                        self.changes.state().forget(*segment);
                        self.catalog_tx.send(CatalogEvent::ChangesSegmentDeleted { segment: *segment });
                        metrics().changes_segments_deleted.fetch_add(1, Ordering::Relaxed);
                        tracing::info!("change log: deleted {path} (behind the durable checkpoint, unpinned)");
                    }
                    Err(e) => tracing::warn!("change log: deleting {path} failed: {e:#}"),
                }
            }
        }
        metrics().changes_segments_retained.store(self.changes.state().retained(), Ordering::Relaxed);
    }

    /// Every shape currently holding a change-log segment against deletion (ADR-0006).
    ///
    /// A **dormant** shape pins the segment it will resume from, and the sweeper may release that
    /// pin by evicting it. A **reactivating** one is replaying from that same position right now: it
    /// pins just as hard, but the pin is not the sweeper's to release (`evict_shape` refuses a shape
    /// mid-transition, so proposing it would produce a no-op that reads as "unpinned"). Neither an
    /// active nor a deactivating shape can pin anything below the floor — their resume position is
    /// captured from the sequencer, which is at or past it.
    fn segment_pins(&self) -> Vec<crate::changelog::SegmentPin> {
        self.lives
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(id, life)| match &life.state {
                LifeState::Dormant { resume, .. } => Some(crate::changelog::SegmentPin {
                    shape_id: id.clone(),
                    segment: resume.segment,
                    evictable: true,
                }),
                LifeState::Reactivating { resume, .. } => Some(crate::changelog::SegmentPin {
                    shape_id: id.clone(),
                    segment: resume.segment,
                    evictable: false,
                }),
                LifeState::Active | LifeState::Deactivating { .. } => None,
            })
            .collect()
    }

    /// Spawn (once) the background retention sweeper. Started lazily from the shape-create paths
    /// (and after a catalog restore) so library users that never create shapes never run it.
    pub(crate) fn ensure_retention_sweeper(&self) {
        if self.retention_started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let engine = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(engine.retention.sweep_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // the first tick fires immediately; skip it
            loop {
                tick.tick().await;
                engine.retention_sweep().await;
            }
        });
    }
}

impl Engine {
    /// Orchestrate the registry's three-phase subquery-shape creation (see
    /// `SubqueryRegistry::begin_create`): the Postgres seeding queries and the outer backfill
    /// run WITHOUT the registry lock, so concurrent creates parallelize on the shared pool
    /// (`ELECTRIC_DB_POOL_SIZE`) instead of serializing behind one create's round-trips.
    /// A begin-conflict (sharing a node another create is still seeding) retries briefly.
    async fn create_subquery_three_phase(
        &self,
        id: &str,
        table: &TableRef,
        stream_path: &str,
        where_json: &PredicateJson,
        out_cols: Option<Arc<Vec<usize>>>,
        changes_only: bool,
    ) -> Result<()> {
        // Phase A (brief lock), with conflict retry.
        let begin = {
            let mut attempt = 0u32;
            loop {
                let res = self.subqueries.lock().await.begin_create(
                    id,
                    table,
                    stream_path,
                    where_json,
                    out_cols.clone(),
                    changes_only,
                );
                match res {
                    Ok(b) => break b,
                    Err(e) if e.to_string().contains("subquery create conflict") && attempt < 100 => {
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                    Err(e) => return Err(e),
                }
            }
        };
        // Phase B (no registry lock): seed fresh nodes + backfill the shape, all from pooled PG.
        let phase_b = async {
            let mut node_seeds = Vec::with_capacity(begin.seeds.len());
            for (sig, inner_table, inner_where) in &begin.seeds {
                let ts = begin
                    .schemas
                    .get(inner_table)
                    .cloned()
                    .with_context(|| format!("seed: unknown inner table '{inner_table}'"))?;
                let wsql =
                    inner_where.as_ref().map(|w| crate::sql::predicate_json_to_sql(w, 1, &begin.schemas, inner_table));
                let client = crate::pg::pool_for(self.pg_url.as_deref().context("subquery work requires postgres")?)
                    .get()
                    .await?;
                // `collect`: an inner-set node's seed IS engine state — the set it will maintain
                // — so there is nothing to stream it to. It is read through the same streamed
                // reader as every other backfill, so the transport (and the fences) are identical.
                let (rows, fences) = crate::pg::backfill_where_reader(&client, &ts, wsql).await?.collect().await?;
                node_seeds.push((sig.clone(), rows, fences.gate));
            }
            let outer_ts =
                begin.schemas.get(table).cloned().with_context(|| format!("unknown outer table '{table}'"))?;
            let (outer_gate, seeded, seeded_pks) = if changes_only {
                (crate::pg::SnapshotGate::passthrough(), 0u64, HashSet::new())
            } else {
                let (wsql, params) = crate::sql::predicate_json_to_sql(where_json, 1, &begin.schemas, table);
                let client = crate::pg::pool_for(self.pg_url.as_deref().context("subquery work requires postgres")?)
                    .get()
                    .await?;
                // The OUTER shape's backfill is streamed: each chunk is appended to the (not yet
                // installed) shape stream and dropped, so a wide subquery shape costs one chunk of
                // memory, not a table's worth. Only the pk SET is kept — it is what
                // `finish_create`'s gated replay is fenced against, and keys are small.
                let mut reader = crate::pg::backfill_where_reader(&client, &outer_ts, Some((wsql, params))).await?;
                let mut seeded_pks: HashSet<String> = HashSet::new();
                let mut seeded = 0u64;
                let mut appends = 0u64;
                while let Some(chunk) = reader.next_chunk().await? {
                    // Same safe point as the plain path: a wide subquery shape's outer backfill
                    // must not be an un-interruptible span of the termination grace.
                    if self.shutdown_token().is_shutting_down() {
                        anyhow::bail!("{}", crate::engine::sequencer::SHUTTING_DOWN);
                    }
                    for r in &chunk {
                        seeded_pks.insert(outer_ts.key_string(r).unwrap_or_default());
                    }
                    let out: Vec<(Row, ZWeight)> = chunk.into_iter().map(|r| (r, 1)).collect();
                    let envs = translate_output(&outer_ts, out, None, None, out_cols.as_deref().map(Vec::as_slice));
                    if envs.is_empty() {
                        continue;
                    }
                    self.ds.append(stream_path, &envs).await?;
                    seeded += envs.len() as u64;
                    appends += 1;
                }
                if appends > 1 {
                    metrics().backfill_chunked_appends.fetch_add(appends, Ordering::Relaxed);
                }
                (reader.finish().await.gate, seeded, seeded_pks)
            };
            Ok::<_, anyhow::Error>((node_seeds, outer_gate, seeded, seeded_pks))
        }
        .await;
        // Phase C (brief lock): install + gated replay. A failure in either phase unwinds through
        // the caller's create rollback (which reaches the registry via `abort_create`), so there is
        // exactly one undo path whether the create failed or was cancelled.
        let (node_seeds, outer_gate, seeded, seeded_pks) = phase_b?;
        let finished =
            self.subqueries.lock().await.finish_create(id, node_seeds, outer_gate, seeded, seeded_pks).await?;
        enqueue_finished_create_work(&self.flip_tx, &self.pending_flips, id, finished)?;
        Ok(())
    }
}

/// Hand off the effects computed by a subquery create after its registry lock is released.
///
/// Each non-empty batch is counted before sending so the convergence barrier cannot observe a
/// partially delivered create. The sender is unbounded, but it still closes during shutdown; a
/// failed send releases its count and refuses the create so its guard can roll back the install.
fn enqueue_finished_create_work(
    flip_tx: &mpsc::UnboundedSender<FlipWork>,
    pending_flips: &std::sync::atomic::AtomicI64,
    id: &str,
    finished: crate::subquery::FinishedCreate,
) -> Result<()> {
    let crate::subquery::FinishedCreate { work, deferred, node_work } = finished;
    if !node_work.is_empty() {
        pending_flips.fetch_add(1, Ordering::SeqCst);
        if flip_tx.send(FlipWork::DeferredNode { work: node_work }).is_err() {
            pending_flips.fetch_sub(1, Ordering::SeqCst);
            return Err(anyhow::anyhow!(crate::engine::sequencer::SHUTTING_DOWN));
        }
    }
    if !work.is_empty() {
        pending_flips.fetch_add(1, Ordering::SeqCst);
        if flip_tx.send(FlipWork::Walk { work, txid: None, lsn: None }).is_err() {
            pending_flips.fetch_sub(1, Ordering::SeqCst);
            return Err(anyhow::anyhow!(crate::engine::sequencer::SHUTTING_DOWN));
        }
    }
    if !deferred.is_empty() {
        pending_flips.fetch_add(1, Ordering::SeqCst);
        if flip_tx.send(FlipWork::Deferred { shape_id: id.to_string(), work: deferred }).is_err() {
            pending_flips.fetch_sub(1, Ordering::SeqCst);
            return Err(anyhow::anyhow!(crate::engine::sequencer::SHUTTING_DOWN));
        }
    }
    Ok(())
}

/// Is this subscription id free to be claimed on `shape`? (ADR-0008.)
///
/// `shape` is the shape the caller is about to join — pass `""` for a create that is about to mint
/// one. A subscription id already held by ANY other shape is refused with a typed
/// [`SubscriptionConflict`] (409): the caller would otherwise hold one name for two shapes and be
/// unable to release either of them unambiguously. Evaluated under the state lock, in the same
/// critical section that takes the claim.
fn ensure_subscription_free(st: &EngineState, sub: &str, shape: &str) -> Result<()> {
    match st.subscription_owner(sub) {
        Some(owner) if owner != shape => {
            Err(anyhow::Error::new(SubscriptionConflict { subscription: sub.to_string(), shape: owner.clone() }))
        }
        _ => Ok(()),
    }
}

/// Everything a JOIN claimed before its first await, given back if it never reaches its own end —
/// the join-side twin of [`CreateGuard`] (ADR-0008).
///
/// The join's future is awaited straight from the HTTP handler, so a client that disconnects takes
/// it away mid-flight — including while it is blocked on the `Joined` record's durability. Without
/// this, the claim stays: an in-memory subscription nobody holds, pinning the shape past every
/// retention layer, plus a durable `Joined` no client can ever balance (it never learned the id it
/// was joining under, because it never got an answer).
///
/// A RENEWAL claims nothing (`fresh == false`), so its guard is inert: an id the caller already held
/// before the request must survive the request failing.
struct JoinGuard {
    engine: Engine,
    shape_id: String,
    subscription: String,
    /// Armed only for a NEW claim: `fresh == false` is a renewal, which claimed nothing and must
    /// survive its request failing.
    armed: bool,
}

impl JoinGuard {
    fn new(engine: &Engine, shape_id: &str, subscription: &str, fresh: bool) -> Self {
        Self {
            engine: engine.clone(),
            shape_id: shape_id.to_string(),
            subscription: subscription.to_string(),
            armed: fresh,
        }
    }

    /// The join reached its end: the claim stays.
    fn complete(&mut self) {
        self.armed = false;
    }

    /// The join failed and its caller is still there to be told: give the claim back in place, so
    /// the error it returns is already true of the engine's state.
    async fn rollback(&mut self) {
        if !std::mem::take(&mut self.armed) {
            return;
        }
        self.engine.release_subscription(&self.shape_id, Some(&self.subscription)).await;
    }
}

impl Drop for JoinGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Cancelled. The compensation needs the async state lock, which `drop` cannot await, so it
        // runs DETACHED — the same shape as `CreateGuard`, and for the same reason. The `Left` it
        // records is idempotent and names this join's OWN subscription, so it can neither
        // double-release nor steal another subscriber's claim, whatever else has happened since.
        tracing::warn!(
            "join of shape '{}' as subscription '{}' was cancelled; releasing it",
            self.shape_id,
            self.subscription
        );
        let (engine, shape_id, subscription) = (self.engine.clone(), self.shape_id.clone(), self.subscription.clone());
        tokio::spawn(async move {
            engine.release_subscription(&shape_id, Some(&subscription)).await;
        });
    }
}

/// Which per-kind registration a rolled-back create must undo alongside the engine-state entries.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Registration {
    /// Plain + aggregate shapes: the sequencer's entry for the shape, pending or live.
    Sequencer,
    /// Subquery shapes: the cross-table registry's entry, pending or installed.
    Registry,
}

/// Everything a create has registered before its first storage/registry await, given back if the
/// create never reaches its own end.
///
/// The create's future is awaited straight from the HTTP handler, so a client that disconnects
/// takes it away mid-flight — during the backfill, a stream append, the subquery conflict retry, or
/// phase C. Without this, the half-made shape stays in `feed_by_sig` forever: every later identical
/// create joins it and waits on a creator that no longer exists (and bumps its refcount, so
/// retention never evicts it either), while its sequencer/registry entry keeps buffering deltas for
/// a shape nobody can read.
struct CreateGuard {
    engine: Engine,
    shape_id: String,
    table: TableRef,
    stream_path: String,
    registration: Registration,
    armed: bool,
}

impl CreateGuard {
    fn new(engine: &Engine, shape_id: &str, table: &TableRef, stream_path: &str, registration: Registration) -> Self {
        Self {
            engine: engine.clone(),
            shape_id: shape_id.to_string(),
            table: table.clone(),
            stream_path: stream_path.to_string(),
            registration,
            armed: true,
        }
    }

    /// The create reached its end: everything it registered stays.
    fn complete(&mut self) {
        self.armed = false;
    }

    /// The create failed and its caller is still there to be told: roll back in place, so the error
    /// the caller returns is already true of the engine's state.
    async fn rollback(&mut self) {
        self.armed = false;
        self.engine.rollback_create(&self.shape_id, &self.table, &self.stream_path, self.registration).await;
    }
}

impl Drop for CreateGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Cancelled. The rollback needs the async engine/registry locks, which `drop` cannot await,
        // so it runs DETACHED — the same shape as the reactivation path, and for the same reason.
        tracing::warn!("create of shape '{}' was cancelled; rolling back", self.shape_id);
        let (engine, shape_id, table, stream_path, registration) = (
            self.engine.clone(),
            self.shape_id.clone(),
            self.table.clone(),
            self.stream_path.clone(),
            self.registration,
        );
        tokio::spawn(async move {
            engine.rollback_create(&shape_id, &table, &stream_path, registration).await;
        });
    }
}

impl Engine {
    /// The second half of a create's degrade gate: taken after the create's work is done and before
    /// it hands back a handle. `Err(Degraded)` means the caller must roll the create back and answer
    /// 503 rather than return the record.
    ///
    /// **Why this check plus the one under the state lock is sufficient.** The degradation reaper
    /// snapshots every registered subquery stream under the state lock
    /// (`Engine::reap_subquery_streams`) and deletes exactly those. Every create checks `degraded`
    /// under that same lock in the same critical section that registers its record. So, against that
    /// snapshot, a create either
    ///
    /// * observed `degraded` under the lock and never registered — nothing to reap, nothing handed
    ///   back; or
    /// * registered BEFORE the snapshot, so its stream is IN the snapshot and will be deleted — and
    ///   this final check, which runs after the create's own work finished and therefore after the
    ///   mark that caused the snapshot, sees `degraded` and refuses.
    ///
    /// A successful create can therefore never return a handle whose stream the reaper has already
    /// deleted. The one window left is a mark landing after this check: indistinguishable from a
    /// degradation an instant after the response, which every client must handle anyway — and that
    /// create's stream is registered, so the reaper deletes it like any other.
    fn ensure_create_not_degraded(&self) -> Result<()> {
        self.ensure_not_degraded()
    }

    /// Refuse work on a table the engine cannot currently vouch for (ADR-0005). Both cases are
    /// retryable errors rather than "unknown table" — the table exists and the engine intends to
    /// serve it again:
    ///
    /// - **unresolved**: a drift that could not be settled; a retry task is working on it;
    /// - **being resolved right now**: a drift resolution holds the table's lock. Registering here
    ///   would be worse than losing the race — the retirement has already enumerated its victims, so
    ///   this create would NOT be purged, would pass its own closing generation check (the bump
    ///   happened before it captured), and would then be orphaned by the `ResetTable` that ends the
    ///   swap.
    ///
    /// Evaluated under the engine-state lock, in the same critical section that registers the create.
    fn ensure_schema_resolved(&self, st: &EngineState, tables: &[TableRef]) -> Result<()> {
        if let Some(t) = st.first_unresolved(tables) {
            bail!("schema of '{t}' is unresolved after a change; retry later");
        }
        if let Some(t) = tables.iter().find(|t| self.resolving.is_active(t)) {
            bail!("schema of '{t}' is being resolved after a change; retry");
        }
        Ok(())
    }

    /// The create's closing check, the schema counterpart of [`Self::ensure_create_not_degraded`]:
    /// did any table this create read drift while it was reading it?
    ///
    /// A create captures every dependency's schema generation under the state lock as it registers;
    /// `retire_dependents` bumps that generation in the same critical section in which it enumerates
    /// what to retire. So a create either registered before the retirement (and is enumerated and
    /// purged by it) or after (and its captured generation is the post-drift one) — never installed,
    /// unenumerated, against a schema that has already been swapped out from under it.
    /// It also carries the **epoch** (ADR-0004), re-checked in the same lock hold: an epoch reset
    /// bumps `EngineState::epoch_gen` in the same critical section in which it enumerates the shapes
    /// it retires, so a create that registered before that enumeration — and whose backfill snapshot
    /// therefore predates the new slot's consistent point — is rolled back instead of being left
    /// serving a window of changes that no slot will ever deliver.
    pub(crate) async fn ensure_schema_unchanged(&self, gens: &SchemaGens) -> Result<()> {
        let st = self.state.lock().await;
        if st.epoch_reset_since(gens) {
            bail!("the replication epoch was reset during creation; retry");
        }
        match st.drifted_since(gens) {
            Some(t) => bail!("schema of '{t}' changed during creation; retry"),
            None => Ok(()),
        }
    }

    /// Reconcile a terminal-looking shape-stream append answer against engine state
    /// (`DsClient::append_reliable` → [`crate::ds::GoneVerdict`]).
    ///
    /// A 404/410/`stream-closed` on an append is normally the truth: the shape was purged, evicted
    /// or retired mid-flush and its batch has no reader left. But it is also what an HTTP proxy, a
    /// storage router or a failover answers about a stream that is right there — and believing that
    /// makes the sequencer advance past a committed Postgres change for a shape that stays
    /// registered and quietly stale for ever, which no client can detect or recover from.
    ///
    /// So the engine answers, and there are exactly two honest answers:
    ///
    /// * the shape is still registered on this stream **and** storage still has it (`HEAD` →
    ///   `Some`, not closed) ⇒ the terminal status was false; [`GoneVerdict::Retry`];
    /// * storage really does not have it ⇒ **retire the shape** (`Dropped` intent, close-then-delete,
    ///   deregister) so its subscribers learn through 404/`stream-closed` and re-subscribe, and only
    ///   then [`GoneVerdict::Discard`].
    ///
    /// The batch of a registered shape is therefore never abandoned without either landing it or
    /// retiring the shape. A `HEAD` that itself fails cannot distinguish the two, so it retries —
    /// bounded by the shutdown token, which discards (the process is going away; the change is
    /// re-delivered at least once after a restart and the sequencer's highwater de-duplicates it).
    ///
    /// The retirement itself is **detached**. This runs inside `append_reliable`, and an append can
    /// be awaited with the subquery-registry lock held (`subquery.rs`'s no-lane `deliver` fallback);
    /// `purge_shape` takes that same lock. An append must never wait on a lock its own caller may be
    /// holding, whatever today's lane configuration happens to make reachable.
    async fn reconcile_gone_shape_stream(&self, path: &str) -> crate::ds::GoneVerdict {
        use crate::ds::GoneVerdict;
        // Only shape streams are reconciled: the change log uses `append`/`append_checked`, which
        // never come through here, and anything else is not the engine's to retire.
        let Some(id) = path.strip_prefix("shape/") else { return GoneVerdict::Discard };
        let registered = {
            let st = self.state.lock().await;
            st.shapes.get(id).is_some_and(|rec| rec.stream_path == path)
        };
        if !registered {
            return GoneVerdict::Discard;
        }
        if self.shutdown.is_shutting_down() {
            tracing::warn!("append to {path} answered terminal during shutdown; not reconciling");
            return GoneVerdict::Discard;
        }
        match self.ds.head(path).await {
            // There, and appendable: the terminal answer was false.
            Ok(Some(head)) if !head.closed => GoneVerdict::Retry,
            // Closed or absent: storage really has retired this stream under a live shape. Retire
            // the shape for real so nothing keeps serving it.
            Ok(_) => {
                tracing::error!(
                    "shape {id}: its stream '{path}' is gone from storage while the shape was live; \
                     retiring the shape so its subscribers re-subscribe"
                );
                let (engine, id) = (self.clone(), id.to_string());
                tokio::spawn(async move {
                    let _ = engine.purge_shape(&id).await;
                });
                GoneVerdict::Discard
            }
            Err(e) => {
                tracing::warn!("shape {id}: HEAD {path} failed ({e:#}) after a terminal append; retrying the append");
                GoneVerdict::Retry
            }
        }
    }

    /// Wire [`Self::reconcile_gone_shape_stream`] into the streams client, once, at construction.
    ///
    /// This is deliberately a cycle (the client holds a closure holding the engine holding the
    /// client): the engine is a process singleton whose background tasks own clones of it anyway, so
    /// nothing is reclaimed earlier without it, and the alternative — threading a reconciler through
    /// the sequencer, the emission lanes and the subquery registry by hand — would leave every
    /// future append site to remember the rule on its own.
    ///
    /// Two consequences, both accepted rather than accidental:
    ///
    /// * the cycle means **one `Engine` is never dropped per `Engine::new`**. In the binary that is
    ///   one engine for the process lifetime; in tests and tools that construct many, it is a bounded
    ///   leak of engine state (no task, no socket — the background tasks end when their channels
    ///   close), which is why it is not worth a `Weak` indirection on the hot append path.
    /// * the slot is a `OnceLock`, so if the SAME `DsClient` is handed to two engines, the first
    ///   one's reconciler serves both. That is a test-only shape (each engine owns its client in the
    ///   binary), and the alternative — last-writer-wins — would silently repoint a live client's
    ///   reconciliation at a different engine's state, which is worse.
    pub(crate) fn install_gone_reconciler(&self) {
        let engine = self.clone();
        self.ds.set_gone_reconciler(std::sync::Arc::new(move |path: String| {
            let engine = engine.clone();
            Box::pin(async move { engine.reconcile_gone_shape_stream(&path).await })
        }));
    }

    /// Before joining a retained shape, make sure its durable stream is still THERE.
    ///
    /// The sharing fast path hands a joiner the stored record — id, stream URL — without ever
    /// touching storage, and `ensure_active` cannot tell the difference for an *active* shape: it
    /// only looks at the retention state machine. So a stream that storage lost (an operator
    /// `DELETE`, a restore from an older backup, a storage-side failure) is handed out as a live
    /// handle whose every read is 404 and whose every append is discarded.
    ///
    /// One `HEAD` per join answers it. Gone (404/410) or **closed** (a retirement already in flight)
    /// means the registry entry is stale: retire it properly — `Dropped` intent, close-then-delete,
    /// deregister (`purge_shape`) — and let the caller fall through to the create path, which mints a
    /// fresh id and a fresh stream. A transient `HEAD` failure is retried; if storage remains
    /// uncertain, creation fails rather than returning a handle the engine could not verify.
    ///
    /// Only a share that has reported **Ready** is probed. A creator registers its record and
    /// signature under the state lock and PUTs the stream after releasing it, so probing a `Pending`
    /// share would see a stream that does not exist YET and purge a perfectly good create in flight;
    /// joiners of a pending share already wait on `await_share_ready`.
    ///
    /// Deliberately NOT done on `GET /shapes/{id}`: that route is a metadata read on the hot path of
    /// every polling client, and a storage round trip per poll would be a real cost for an answer
    /// nobody acts on. Joining is where a dead handle actually gets handed out.
    async fn retire_join_target_if_stream_lost(&self, sig: &str) -> Result<()> {
        let candidate = {
            let st = self.state.lock().await;
            let Some(id) = st.feed_by_sig.get(sig).cloned() else { return Ok(()) };
            let ready = st.feed_shares.get(&id).is_some_and(|s| *s.ready.borrow() == ShareOutcome::Ready);
            if !ready {
                return Ok(());
            }
            st.shapes.get(&id).map(|r| (id, r.stream_path.clone()))
        };
        let Some((id, path)) = candidate else { return Ok(()) };
        let mut attempt = 0u32;
        let lost = loop {
            match self.ds.head(&path).await {
                Ok(Some(head)) => break head.closed,
                Ok(None) => break true,
                Err(e) => {
                    attempt += 1;
                    if attempt >= 3 || !crate::ds::is_unavailable(&e) {
                        return Err(e.context(format!(
                            "cannot verify retained shape {id}'s stream '{path}' before joining it"
                        )));
                    }
                    let backoff = std::time::Duration::from_millis(100 * u64::from(attempt));
                    tracing::warn!("join: HEAD {path} failed (attempt {attempt}), retrying in {backoff:?}: {e:#}");
                    tokio::time::sleep(backoff).await;
                }
            }
        };
        if lost {
            tracing::warn!(
                "shape {id}: its durable stream '{path}' is gone from storage; retiring the stale \
                 registry entry so this create makes a fresh shape"
            );
            let _ = self.purge_shape(&id).await;
        }
        Ok(())
    }

    /// The closing re-check every create and every join runs **after** its catalog durability wait.
    ///
    /// The checks a create takes before `send_durable` cannot see anything that happens during it,
    /// and that wait is unbounded external I/O: a `TRUNCATE`, a schema drift, an epoch reset or a
    /// native purge can retire the very shape the request is about to acknowledge, and every one of
    /// those is externally triggerable while storage is slow. So the same closing check is taken
    /// again here — degradation latch, schema generations, epoch generation — plus the one it never
    /// needed before: **is the record still registered, on the same stream?** A generation bump is
    /// not always involved (a purge removes the record without touching any generation), so the
    /// registration is checked directly rather than inferred.
    ///
    /// One implementation, six callers (plain / subquery / aggregate / circuit-aggregate creates,
    /// and both join paths) — the ordering bug this fixes existed once per copy.
    ///
    /// Failures are typed [`CreateRaced`] so the caller can redo the whole attempt; a degradation
    /// stays a typed [`Degraded`] (the HTTP layer answers 503 with the reason, and a joiner waiting
    /// on the share must get the creator's reason verbatim).
    pub(crate) async fn recheck_after_durability(&self, id: &str, stream_path: &str, gens: &SchemaGens) -> Result<()> {
        self.ensure_create_not_degraded()?;
        self.ensure_schema_unchanged(gens)
            .await
            .map_err(|e| anyhow::Error::new(CreateRaced(format!("{e:#}").trim_end_matches("; retry").to_string())))?;
        let st = self.state.lock().await;
        match st.shapes.get(id) {
            Some(rec) if rec.stream_path == stream_path => Ok(()),
            Some(rec) => Err(anyhow::Error::new(CreateRaced(format!(
                "shape '{id}' moved from stream '{stream_path}' to '{}' during its catalog durability wait",
                rec.stream_path
            )))),
            None => Err(anyhow::Error::new(CreateRaced(format!(
                "shape '{id}' was retired during its catalog durability wait"
            )))),
        }
    }

    /// Undo a create: the one implementation of "this shape was never made", shared by the explicit
    /// error paths and by [`CreateGuard`]'s cancelled path.
    ///
    /// Engine state goes first: the share signature is what a retrying client contends on, and the
    /// per-kind entry left behind for the extra moment only buffers deltas for a shape nobody can
    /// reach. The sequencer command is `RemoveShape`, not `AbortShape`: a cancellation can land
    /// after `ActivateShape` has already turned the pending buffer into live routing, and only
    /// `RemoveShape` covers both states (it drops the pending buffer, the routed/standalone/
    /// aggregate registration, and the emit counter); commands are FIFO, so one sent while
    /// `BeginShape` is still queued still lands after it.
    async fn rollback_create(&self, id: &str, table: &TableRef, stream_path: &str, registration: Registration) {
        let mut st = self.state.lock().await;
        let existed = st.shapes.remove(id).is_some();
        st.circuit_placement.remove(id);
        // The create's own subscription goes with it: a rolled-back create never happened, and
        // leaving its id claimed would refuse the client's very next attempt with a 409.
        st.forget_subscriptions(id);
        if let Some(share) = st.feed_shares.remove(id) {
            // Only if the signature still points HERE: a joiner woken by this create's failure may
            // already have registered its own replacement under the same signature.
            if st.feed_by_sig.get(&share.sig).is_some_and(|cur| cur == id) {
                st.feed_by_sig.remove(&share.sig);
            }
        }
        if registration == Registration::Sequencer
            && let Some(seq) = st.sequencer.as_ref()
        {
            let _ = seq.cmd_tx.send(SequencerCmd::RemoveShape { table: table.clone(), shape_id: id.to_string() });
        }
        drop(st);
        self.lives.lock().unwrap().remove(id);
        if existed {
            self.catalog_tx.send(CatalogEvent::Dropped { id: id.to_string() });
        }
        if registration == Registration::Registry {
            self.subqueries.lock().await.abort_create(id).await;
        }
        // Deleted, not retired: the create never returned, so the stream was never handed to a
        // subscriber and there is no one to signal with a close. The `Dropped` above is still an
        // intent that has to be closed out, so the delete records `Retired` when it lands — and when
        // it does NOT, the retirement queue finishes the job (it closes first, which is a harmless
        // extra round trip on a stream nobody ever read).
        match self.ds.delete_stream(stream_path).await {
            Ok(()) => {
                if existed {
                    self.catalog_tx.send(CatalogEvent::Retired { id: id.to_string() });
                }
            }
            Err(e) => {
                tracing::warn!("rolling back create {id}: deleting {stream_path} failed ({e:#}); queued");
                self.retirements.enqueue(stream_path, existed.then_some(id));
            }
        }
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    /// A create must not acknowledge after shutdown closes the flip propagator channel: the
    /// deferred effects returned by `finish_create` have nowhere to run, so the caller needs the
    /// normal shutdown error and rollback path.
    #[test]
    fn closed_flip_channel_rejects_finished_create_handoff() {
        let (flip_tx, flip_rx) = mpsc::unbounded_channel();
        drop(flip_rx);
        let pending_flips = std::sync::atomic::AtomicI64::new(0);
        let finished = crate::subquery::FinishedCreate {
            work: std::collections::VecDeque::new(),
            deferred: std::collections::VecDeque::from([crate::subquery::DeferredShapeWork::Full { txid: None }]),
            node_work: std::collections::VecDeque::new(),
        };

        let err = enqueue_finished_create_work(&flip_tx, &pending_flips, "s1", finished)
            .expect_err("a closed propagator cannot acknowledge deferred create work");
        assert_eq!(err.to_string(), crate::engine::sequencer::SHUTTING_DOWN);
        assert_eq!(pending_flips.load(Ordering::SeqCst), 0, "failed handoff must not hold the barrier");
    }

    /// A subquery-capable engine with no durable-streams server behind it: the rollback's stream
    /// DELETE fails and is ignored, which leaves exactly the in-memory state these tests assert on.
    async fn engine_with_subquery_tables() -> (Engine, PredicateJson) {
        let engine = Engine::new(DsClient::new("http://127.0.0.1:1"));
        let schema: Schema = serde_json::from_value(serde_json::json!({
            "tables": {
                "outer_t": { "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id" },
                "inner_t": { "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id" }
            }
        }))
        .unwrap();
        let compiled = compile_schema(&schema).unwrap();
        engine.subqueries.lock().await.set_schemas(Arc::new(compiled.clone()));
        *engine.tables_shared.write().unwrap() = compiled.clone();
        engine.state.lock().await.tables = compiled;
        let where_json = serde_json::from_value(serde_json::json!({
            "col": "gid", "in": { "table": "inner_t", "project": "gid" }
        }))
        .unwrap();
        (engine, where_json)
    }

    /// Everything `create_shape`'s subquery path registers before its first await, plus the guard
    /// it arms there.
    async fn register(engine: &Engine, id: &str, where_json: &PredicateJson) -> CreateGuard {
        let mut st = engine.state.lock().await;
        st.shapes.insert(
            id.to_string(),
            ShapeRecord {
                id: id.to_string(),
                table: "outer_t".into(),
                stream_path: format!("shape/{id}"),
                changes_only: false,
                where_json: Some(where_json.clone()),
                columns: None,
                family_key: None,
                is_subquery: true,
                aggregate: None,
                fingerprint: None,
            },
        );
        let (_ready_tx, ready) = tokio::sync::watch::channel(ShareOutcome::Pending);
        st.feed_by_sig.insert("sig".into(), id.to_string());
        st.feed_shares.insert(id.to_string(), FeedShare { sig: "sig".into(), subs: Default::default(), ready });
        st.subscribe(id, "sub-a".to_string(), crate::changelog::now_secs());
        drop(st);
        engine.lives.lock().unwrap().insert(id.to_string(), ShapeLife::active());
        CreateGuard::new(engine, id, &"outer_t".into(), &format!("shape/{id}"), Registration::Registry)
    }

    /// The detached rollback must leave nothing a later identical create could join or conflict with.
    async fn assert_rolled_back(engine: &Engine, id: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while engine.get_shape(id).await.is_some() || engine.subqueries.lock().await.touches(&"outer_t".into()) {
            assert!(std::time::Instant::now() < deadline, "the cancelled create was never rolled back");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let st = engine.state.lock().await;
        assert!(st.feed_by_sig.is_empty(), "the share signature must not outlive the create");
        assert!(st.feed_shares.is_empty());
        assert!(engine.lives.lock().unwrap().is_empty(), "the retention entry must go too");
        assert!(engine.subquery_stats().await.is_empty(), "no registry node may survive");
    }

    /// The post-durability re-check is the whole of item 1: run it on a shape that is still there,
    /// unchanged, and it passes; run it on anything that moved during the wait and it refuses with a
    /// RETRYABLE [`CreateRaced`], never a success.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_post_durability_recheck_passes_only_for_an_unchanged_registration() {
        let (engine, where_json) = engine_with_subquery_tables().await;
        let mut guard = register(&engine, "s1", &where_json).await;
        let gens = engine.state.lock().await.capture_gens(&["outer_t".into(), "inner_t".into()]);

        // Nothing moved during the wait.
        engine.recheck_after_durability("s1", "shape/s1", &gens).await.expect("an unchanged shape passes");

        // A table this create read drifted while it waited: retryable, and it names the reason.
        engine.force_schema_generation_bump(&"inner_t".into()).await;
        let e = engine.recheck_after_durability("s1", "shape/s1", &gens).await.unwrap_err();
        assert!(e.downcast_ref::<CreateRaced>().is_some(), "a lost race must be retryable: {e:#}");
        assert!(e.to_string().contains("inner_t"), "unexpected reason: {e}");

        guard.complete();
    }

    /// The case a generation bump cannot express: a native purge removes the record outright while
    /// the create is blocked on catalog durability. The generations are untouched, so ONLY the
    /// registration check catches it — and it must, or the create acknowledges a handle whose stream
    /// is already 404.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_shape_purged_during_the_wait_is_not_acknowledged() {
        let (engine, where_json) = engine_with_subquery_tables().await;
        let mut guard = register(&engine, "s1", &where_json).await;
        let gens = engine.state.lock().await.capture_gens(&["outer_t".into()]);

        engine.state.lock().await.shapes.remove("s1");
        let e = engine.recheck_after_durability("s1", "shape/s1", &gens).await.unwrap_err();
        assert!(e.downcast_ref::<CreateRaced>().is_some(), "a purge during the wait is retryable: {e:#}");
        assert!(e.to_string().contains("retired during its catalog durability wait"), "unexpected reason: {e}");

        guard.complete();
    }

    /// A record that is still there but now points at a DIFFERENT stream is not this create's shape
    /// either — the id was re-registered by something else while the wait was in flight.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_shape_whose_stream_moved_during_the_wait_is_not_acknowledged() {
        let (engine, where_json) = engine_with_subquery_tables().await;
        let mut guard = register(&engine, "s1", &where_json).await;
        let gens = engine.state.lock().await.capture_gens(&["outer_t".into()]);

        {
            let mut st = engine.state.lock().await;
            let rec = st.shapes.get_mut("s1").unwrap();
            rec.stream_path = "shape/s9".to_string();
        }
        let e = engine.recheck_after_durability("s1", "shape/s1", &gens).await.unwrap_err();
        assert!(e.downcast_ref::<CreateRaced>().is_some(), "{e:#}");
        assert!(e.to_string().contains("moved from stream"), "unexpected reason: {e}");

        guard.complete();
    }

    /// Cancelled between `begin_create` and `finish_create`: the registry holds a pending shape
    /// buffering outer deltas and a fresh node buffering inner ones. Both must go, or the next
    /// identical create conflicts on that half-seeded node until its retry budget runs out.
    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_before_install_unwinds_the_pending_registry_state() {
        let (engine, where_json) = engine_with_subquery_tables().await;
        let guard = register(&engine, "s1", &where_json).await;
        let begin = engine
            .subqueries
            .lock()
            .await
            .begin_create("s1", &"outer_t".into(), "shape/s1", &where_json, None, false)
            .unwrap();
        assert_eq!(begin.seeds.len(), 1, "one fresh node to seed");

        drop(guard);
        assert_rolled_back(&engine, "s1").await;
    }

    /// Cancelled after `finish_create` installed the shape (its phase-C replay awaits, and the HTTP
    /// response is still one await away): the rollback must go through the ordinary drop path so the
    /// installed shape, its index entry and its feed go with the engine-side state.
    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_after_install_drops_the_registered_shape() {
        let (engine, where_json) = engine_with_subquery_tables().await;
        let guard = register(&engine, "s1", &where_json).await;
        let begin = engine
            .subqueries
            .lock()
            .await
            .begin_create("s1", &"outer_t".into(), "shape/s1", &where_json, None, false)
            .unwrap();
        let seeds = begin
            .seeds
            .iter()
            .map(|(sig, _, _)| (sig.clone(), Vec::new(), crate::pg::SnapshotGate::passthrough()))
            .collect();
        engine
            .subqueries
            .lock()
            .await
            .finish_create("s1", seeds, crate::pg::SnapshotGate::passthrough(), 0, HashSet::new())
            .await
            .unwrap();
        assert!(engine.subqueries.lock().await.shapes.contains_key("s1"), "installed before the drop");

        drop(guard);
        assert_rolled_back(&engine, "s1").await;
        assert!(engine.graph().await.shapes.is_empty());
    }

    /// Cancelled in the MIDDLE of phase C: the fresh node's seed is already asserted into the
    /// membership circuit and the shape is not installed. The pending entry never left the registry,
    /// so the detached rollback finds it and unwinds the whole create — the partial seed's
    /// contributor tuples included. Without that, the leaked half-seeded node makes every later
    /// create sharing it conflict until the engine restarts.
    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_mid_phase_c_unwinds_the_partly_seeded_node() {
        let (engine, where_json) = engine_with_subquery_tables().await;
        let guard = register(&engine, "s1", &where_json).await;
        let node_id = {
            let mut reg = engine.subqueries.lock().await;
            let begin = reg.begin_create("s1", &"outer_t".into(), "shape/s1", &where_json, None, false).unwrap();
            let sig = begin.seeds[0].0.clone();
            reg.assert_seed_row_for_test(&sig, "1", Value::Int(7)).await;
            let node_id = reg.nodes[&sig].node_id;
            assert_eq!(reg.circuit_distinct(node_id), 1, "the partial seed really landed");
            node_id
        };

        drop(guard);
        assert_rolled_back(&engine, "s1").await;
        assert_eq!(
            engine.subqueries.lock().await.circuit_distinct(node_id),
            0,
            "the partial seed was retracted with the node it belonged to"
        );
    }

    // --- the sharing rendezvous carries the creator's REASON ------------------------------------

    /// Everything an in-flight shared create leaves behind for a joiner to find: the shape record,
    /// the signature a later identical create matches on, and the share entry whose outcome the
    /// joiner waits for. Returns the creator's end of that outcome channel.
    async fn pending_share(
        engine: &Engine,
        id: &str,
        where_json: &PredicateJson,
    ) -> tokio::sync::watch::Sender<ShareOutcome> {
        let sig = shape_signature(&"outer_t".into(), &Some(where_json.clone()), &None, false);
        let (tx, rx) = tokio::sync::watch::channel(ShareOutcome::Pending);
        let mut st = engine.state.lock().await;
        st.shapes.insert(
            id.to_string(),
            ShapeRecord {
                id: id.to_string(),
                table: "outer_t".into(),
                stream_path: format!("shape/{id}"),
                changes_only: false,
                where_json: Some(where_json.clone()),
                columns: None,
                family_key: None,
                is_subquery: true,
                aggregate: None,
                fingerprint: None,
            },
        );
        st.feed_by_sig.insert(sig.clone(), id.to_string());
        st.feed_shares.insert(id.to_string(), FeedShare { sig, subs: Default::default(), ready: rx });
        st.subscribe(id, "sub-creator".to_string(), crate::changelog::now_secs());
        tx
    }

    async fn refcount(engine: &Engine, id: &str) -> usize {
        engine.state.lock().await.feed_shares.get(id).map(FeedShare::refcount).unwrap_or(0)
    }

    /// Park until the joiner has actually joined (its refcount is taken) and is waiting on the
    /// creator's outcome, so the outcome below is published INTO that wait.
    async fn await_joined(engine: &Engine, id: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while refcount(engine, id).await < 2 {
            assert!(std::time::Instant::now() < deadline, "the joiner never joined the share");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    fn join(engine: &Engine, where_json: &PredicateJson) -> tokio::task::JoinHandle<Result<ShapeRecord>> {
        let engine = engine.clone();
        let where_json = where_json.clone();
        tokio::spawn(async move { engine.create_shape(&"outer_t".into(), Some(where_json), None, false, true).await })
    }

    /// A creator that refuses because the engine degraded publishes that REASON, so its joiner
    /// answers with the same typed error (503) instead of the generic "failed to initialize"
    /// (500). Two identical requests from identical clients must not disagree about why the
    /// engine said no.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_joiner_of_a_degraded_create_gets_the_typed_refusal() {
        let (engine, where_json) = engine_with_subquery_tables().await;
        let ready_tx = pending_share(&engine, "s1", &where_json).await;
        let joining = join(&engine, &where_json);
        await_joined(&engine, "s1").await;

        let _ = ready_tx.send(ShareOutcome::Degraded);
        let err = joining.await.unwrap().expect_err("the joiner must be refused");
        assert!(
            err.downcast_ref::<Degraded>().is_some(),
            "the joiner must get the creator's typed refusal, not a generic failure: {err:#}"
        );
    }

    /// A creator whose closing re-check finds its shape retired during the catalog durability wait
    /// does not give up — it redoes the create. Its joiner must reach the same conclusion, so the
    /// outcome arrives typed [`CreateRaced`] rather than as a generic initialization failure:
    /// otherwise two identical requests disagree, the creator succeeding on its next attempt while
    /// the joiner answers 500.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_joiner_of_a_raced_create_gets_the_retryable_refusal() {
        let (engine, where_json) = engine_with_subquery_tables().await;
        let ready_tx = pending_share(&engine, "s1", &where_json).await;
        let joining = join(&engine, &where_json);
        await_joined(&engine, "s1").await;

        let _ = ready_tx.send(ShareOutcome::Raced);
        let err = joining.await.unwrap().expect_err("the joiner must be refused");
        assert!(
            err.downcast_ref::<CreateRaced>().is_some(),
            "the joiner must get the creator's RETRYABLE refusal, not a generic failure: {err:#}"
        );
    }

    /// The other half: the creator finished a moment BEFORE the mark, so the outcome is `Ready` —
    /// but the reaper is about to delete the stream that handle points at. The joiner re-checks
    /// the same latch the creator's final check uses, and gives back the refcount it took (a
    /// refused join must not pin the shape).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_joiner_ready_after_the_mark_is_refused_and_gives_back_its_refcount() {
        let (engine, where_json) = engine_with_subquery_tables().await;
        let ready_tx = pending_share(&engine, "s1", &where_json).await;
        let joining = join(&engine, &where_json);
        await_joined(&engine, "s1").await;

        engine.force_degraded();
        let _ = ready_tx.send(ShareOutcome::Ready);
        let err = joining.await.unwrap().expect_err("the joiner must be refused");
        assert!(err.downcast_ref::<Degraded>().is_some(), "and typed, like the creator's: {err:#}");
        assert_eq!(refcount(&engine, "s1").await, 1, "the refused join released its subscription");
    }

    /// An ordinary creation failure stays an ordinary failure: only degradation is special-cased,
    /// so a retryable init failure must not start answering 503.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_joiner_of_a_failed_create_still_gets_the_generic_error() {
        let (engine, where_json) = engine_with_subquery_tables().await;
        let ready_tx = pending_share(&engine, "s1", &where_json).await;
        let joining = join(&engine, &where_json);
        await_joined(&engine, "s1").await;

        let _ = ready_tx.send(ShareOutcome::Failed);
        let err = joining.await.unwrap().expect_err("the joiner must be refused");
        assert!(err.downcast_ref::<Degraded>().is_none(), "not a degradation: {err:#}");
        assert!(format!("{err:#}").contains("failed to initialize"), "{err:#}");
    }
}

#[cfg(test)]
mod subscription_tests {
    use super::*;

    /// An engine with one shared shape and the given live subscriptions
    /// (`subscription id -> lease timestamp`). No durable-streams behind it: every catalog record
    /// these tests provoke is queued and retried in the background, which is exactly what the
    /// engine does in production while storage is down — and none of the behaviour under test waits
    /// on it.
    async fn engine_with_share(id: &str, subs: &[(&str, u64)]) -> Engine {
        let engine = Engine::new(DsClient::new("http://127.0.0.1:1"));
        let mut st = engine.state.lock().await;
        st.shapes.insert(
            id.to_string(),
            ShapeRecord {
                id: id.to_string(),
                table: "items".into(),
                stream_path: format!("shape/{id}"),
                changes_only: false,
                where_json: None,
                columns: None,
                family_key: None,
                is_subquery: false,
                aggregate: None,
                fingerprint: None,
            },
        );
        let (ready_tx, ready) = tokio::sync::watch::channel(ShareOutcome::Ready);
        drop(ready_tx);
        st.feed_by_sig.insert(format!("sig-{id}"), id.to_string());
        st.feed_shares.insert(id.to_string(), FeedShare { sig: format!("sig-{id}"), subs: Default::default(), ready });
        for (sub, at) in subs {
            st.subscribe(id, (*sub).to_string(), *at);
        }
        drop(st);
        engine.lives.lock().unwrap().insert(id.to_string(), ShapeLife::active());
        engine
    }

    async fn refcount(engine: &Engine, id: &str) -> usize {
        engine.state.lock().await.feed_shares.get(id).map(FeedShare::refcount).unwrap_or(0)
    }

    /// The double-delete this whole ADR exists for: a client whose success response was lost retries
    /// its `DELETE`, and the second one must change nothing. Before subscription ids, it decremented
    /// a shared counter again and stole the other subscriber's claim.
    #[tokio::test(flavor = "multi_thread")]
    async fn releasing_one_subscription_twice_releases_it_once() {
        let engine = engine_with_share("s1", &[("sub-a", 10), ("sub-b", 10)]).await;

        engine.release_subscription("s1", Some("sub-a")).await;
        engine.release_subscription("s1", Some("sub-a")).await;
        engine.release_subscription("s1", Some("sub-never-existed")).await;

        assert_eq!(refcount(&engine, "s1").await, 1, "the other subscriber keeps its claim");
        assert_eq!(
            engine.state.lock().await.subscription_owner("sub-a"),
            None,
            "the released id is free to be claimed again"
        );
        assert_eq!(engine.state.lock().await.subscription_owner("sub-b").cloned(), Some("s1".to_string()));
    }

    /// The legacy path (`DELETE` with no `subscription`) still decrements exactly once per call —
    /// and takes an engine-MINTED claim before an identified one, so a caller that never learned
    /// the protocol cannot release a subscription that did.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_anonymous_release_takes_a_minted_claim_first_and_decrements_once() {
        let engine = engine_with_share("s1", &[("~boot-1", 10), ("named", 5)]).await;

        engine.release_shape("s1").await;
        assert_eq!(refcount(&engine, "s1").await, 1);
        assert_eq!(
            engine.state.lock().await.feed_shares["s1"].subs.keys().cloned().collect::<Vec<_>>(),
            vec!["named".to_string()],
            "the un-named claim went first, even though it was the newer lease"
        );

        engine.release_shape("s1").await;
        assert_eq!(refcount(&engine, "s1").await, 0);
        // ...and it is not idempotent, which is why it is documented as legacy: there is nothing
        // left to release here, but nothing identified what the caller meant either.
        engine.release_shape("s1").await;
        assert_eq!(refcount(&engine, "s1").await, 0);
    }

    /// One subscription id names one shape. Re-using it for a different predicate is refused
    /// (409-typed), because the caller would then hold one name for two claims and could release
    /// neither of them unambiguously.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_subscription_id_held_by_another_shape_is_refused() {
        let engine = engine_with_share("s1", &[("mine", 10)]).await;
        let st = engine.state.lock().await;

        // Joining the shape that already holds it is the renewal case, not a conflict.
        ensure_subscription_free(&st, "mine", "s1").expect("renewing my own subscription is fine");

        let err = ensure_subscription_free(&st, "mine", "s2").expect_err("a foreign shape must be refused");
        let conflict = err.downcast_ref::<SubscriptionConflict>().expect("typed, so the API answers 409");
        assert_eq!(conflict.shape, "s1");
        assert!(err.to_string().contains("names one shape"), "{err}");

        // A create that is about to mint a shape passes "" as the target: any owner is a conflict.
        assert!(ensure_subscription_free(&st, "mine", "").is_err());
        assert!(ensure_subscription_free(&st, "unclaimed", "").is_ok());
    }

    /// The lease: a subscription not renewed within the idle window is released by the sweeper,
    /// exactly as an explicit `DELETE` would release it — and a renewed one is left alone. This is
    /// the only liveness signal the engine has for a native subscriber, whose reads it never sees.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unrenewed_lease_lapses_and_a_renewed_one_does_not() {
        let now = crate::changelog::now_secs();
        let engine = engine_with_share("s1", &[("stale", now - 60), ("renewed", now)]).await;
        let cfg = crate::retention::RetentionConfig {
            idle_timeout: std::time::Duration::from_secs(5),
            ..crate::retention::RetentionConfig::default()
        };

        engine.sweep_leases(&cfg).await;

        assert_eq!(
            engine.state.lock().await.feed_shares["s1"].subs.keys().cloned().collect::<Vec<_>>(),
            vec!["renewed".to_string()],
            "only the subscription that stopped saying it was there is released"
        );
        assert_eq!(
            engine.state.lock().await.subscription_owner("stale"),
            None,
            "and its id is free again — a late client simply re-subscribes"
        );

        // Renewing is the same `subscribe` call: it moves the lease and claims nothing new.
        {
            let mut st = engine.state.lock().await;
            assert!(!st.subscribe("s1", "renewed".to_string(), now + 1), "a renewal is not a new claim");
        }
        assert_eq!(refcount(&engine, "s1").await, 1);
    }

    /// The boundary belongs to the client: a one-second window must last at least one second. The
    /// lease clock is wall-clock SECONDS, so a claim renewed at `now` and swept at `now + 1` has an
    /// age of "1" that may be a millisecond of real time — lapsing on `>=` would cut every window
    /// short by up to a second, which for the second-scale windows tests and demos use is the whole
    /// window.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_lease_lasts_at_least_its_whole_window() {
        let now = crate::changelog::now_secs();
        let engine = engine_with_share("s1", &[("edge", now)]).await;
        let cfg = crate::retention::RetentionConfig {
            idle_timeout: std::time::Duration::from_secs(1),
            ..crate::retention::RetentionConfig::default()
        };

        let st = engine.state.lock().await;
        assert!(st.lapsed_subscriptions(cfg.idle_timeout, now).is_empty(), "age 0 is inside the window");
        assert!(
            st.lapsed_subscriptions(cfg.idle_timeout, now + 1).is_empty(),
            "age 1 of a 1s window may be a millisecond of real time — still inside"
        );
        assert_eq!(
            st.lapsed_subscriptions(cfg.idle_timeout, now + 2),
            vec![("s1".to_string(), "edge".to_string())],
            "a full window has certainly passed by now"
        );
    }

    /// The anonymous release picks the oldest MINTED claim, and breaks a same-second tie by the
    /// counter the engine minted them with — `~n-10` is younger than `~n-9`, which a plain string
    /// compare has backwards.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_anonymous_release_breaks_a_tie_by_mint_order() {
        let engine = engine_with_share("s1", &[("~boot-9", 10), ("~boot-10", 10)]).await;
        engine.release_shape("s1").await;
        assert_eq!(
            engine.state.lock().await.feed_shares["s1"].subs.keys().cloned().collect::<Vec<_>>(),
            vec!["~boot-10".to_string()],
            "the older claim (9) went first, not the lexicographically smaller one (10)"
        );
    }

    /// `idle_timeout == 0` turns dormancy off, and with it leases: an engine that never parks a
    /// shape has no use for the signal, and expiring subscriptions under it would only break
    /// sharing for long-lived subscribers.
    #[tokio::test(flavor = "multi_thread")]
    async fn leases_never_lapse_when_dormancy_is_disabled() {
        let engine = engine_with_share("s1", &[("ancient", 1)]).await;
        let cfg = crate::retention::RetentionConfig {
            idle_timeout: std::time::Duration::ZERO,
            ..crate::retention::RetentionConfig::default()
        };
        engine.sweep_leases(&cfg).await;
        assert_eq!(refcount(&engine, "s1").await, 1);
    }

    /// A join that is abandoned mid-wait (the client disconnected while storage was slow) gives its
    /// claim back. The compensation names the join's OWN subscription, so it can neither
    /// double-release nor take another subscriber's claim — the two hazards that made a
    /// cancellation guard impossible on an anonymous refcount.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_join_releases_its_own_subscription() {
        let engine = engine_with_share("s1", &[("held", 10), ("joining", 10)]).await;

        drop(JoinGuard::new(&engine, "s1", "joining", true));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while refcount(&engine, "s1").await > 1 {
            assert!(std::time::Instant::now() < deadline, "the cancelled join never gave its claim back");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            engine.state.lock().await.feed_shares["s1"].subs.keys().cloned().collect::<Vec<_>>(),
            vec!["held".to_string()],
            "the other subscriber is untouched"
        );
    }

    /// A cancelled RENEWAL compensates for nothing: the subscription existed before the request and
    /// must survive it failing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_renewal_keeps_the_claim_it_did_not_take() {
        let engine = engine_with_share("s1", &[("held", 10)]).await;
        drop(JoinGuard::new(&engine, "s1", "held", false));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(refcount(&engine, "s1").await, 1);
    }

    /// A purge takes the subscriptions with it: leaving the ids claimed would refuse the client's
    /// very next create with a 409 over a shape that no longer exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_purge_frees_the_ids_the_shape_held() {
        let engine = engine_with_share("s1", &[("mine", 10)]).await;
        engine.purge_shape("s1").await.unwrap();
        assert_eq!(engine.state.lock().await.subscription_owner("mine"), None);
    }
}
