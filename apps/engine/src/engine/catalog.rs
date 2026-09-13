//! Durable shape catalog: the append-only `meta/catalog` event stream, its writer
//! task, and boot-time restore/resume of shapes.

use super::*;

/// The engine's durable **shape catalog**: an append-only event stream replayed at boot so a
/// restart re-registers every shape itself instead of requiring a client re-registration storm.
/// Plain/routed shapes resume with passthrough gates (the change log replays everything after the
/// persisted offset; re-emission across the crash window is idempotent absolute upserts);
/// aggregates re-seed their fold from a fresh Postgres snapshot (their fresh gate then skips the
/// replayed history). Subquery shapes are NOT restorable without persisted inner-node state (a
/// fresh-seeded node cannot detect downtime flips, which would leave stale move-outs forever) —
/// they are dropped loudly at restore for clients to recreate.
///
/// The restore is all-or-nothing for anything that may clear by itself (ADR-0009): a storage or
/// Postgres failure part-way undoes what was installed and fails the boot, which retries. Only a
/// DEFINITIVE answer about one shape — its table moved or is gone, its stream is missing or closed,
/// it is a subquery shape — retires that shape, and the rest restore around it.
pub(crate) const CATALOG_STREAM: &str = "meta/catalog";

/// One catalog event. `Offset` checkpoints the sequencer's processed change-log position (the
/// replay start after a restart), appended at most every ~2s.
///
/// Every event carries an **`eid`** on the wire — assigned at enqueue, not here (see
/// [`CatalogWriter::enqueue`]), because it identifies the *append attempt* the writer may repeat,
/// not the value. The fold ignores an `eid` it has already applied, which is what makes the writer's
/// retry-in-place (no event is ever dropped, ADR-0007) safe for a record whose effect is not
/// naturally idempotent.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "t", rename_all = "camelCase")]
pub(crate) enum CatalogEvent {
    /// A shape was created, with the **subscription** that created it (ADR-0008) and the wall-clock
    /// second it was taken at — the lease's start, restored by the fold so a restart does not hand
    /// every subscription a fresh window.
    Created {
        rec: ShapeRecord,
        sig: Option<String>,
        subscription: String,
        at: u64,
    },
    /// A subscription joined a shared feed, or RENEWED its lease (ADR-0008): the same subscription
    /// id twice is one claim, and the second one only moves `at`. The fold keeps a SET, so a
    /// duplicate is a no-op rather than a second count.
    Joined {
        id: String,
        subscription: String,
        at: u64,
    },
    /// A subscription left a shared feed. `Left` for an id the shape does not hold is a no-op (the
    /// release is idempotent — that is the point). With retention, an empty live set keeps the
    /// shape (it goes dormant later), so `Left` never implies teardown.
    ///
    /// `lapsed` marks the retention sweeper's own release: the lease was not renewed within the
    /// idle window (ADR-0008). It changes nothing about the fold — a lapse and an explicit release
    /// are the same act — and exists so the durable record explains why a subscription no one
    /// deleted disappeared.
    Left {
        id: String,
        subscription: String,
        #[serde(default, skip_serializing_if = "is_false")]
        lapsed: bool,
    },
    /// The shape went dormant: routing state dropped, stream + record retained. `resume` is the
    /// change-log position its stream is complete up to — a `(segment, offset)` pair since
    /// ADR-0006, because the offset alone is meaningless once the log has rotated; `gate` is its
    /// original backfill-snapshot fence. Restores as dormant (an improvement over the
    /// in-memory-only lifecycle: a restart no longer forgets dormant shapes).
    Dormant {
        id: String,
        resume: LogPosition,
        gate: crate::pg::SnapshotGate,
    },
    /// A dormant shape was reactivated (replayed + re-registered).
    Reactivated {
        id: String,
    },
    Dropped {
        id: String,
    },
    /// The shape's stream retirement COMPLETED: closed, then deleted (ADR-0007). Written only after
    /// storage accepted the delete — a 404/410 counts, deletion is idempotent — so a `Dropped` with
    /// no `Retired` after it is exactly "the engine promised to remove this stream and did not".
    ///
    /// The pair is what makes retirement survive a crash: `Dropped` is the durable INTENT (always
    /// written before the retirement is attempted), `Retired` the durable COMPLETION, and the boot
    /// hands every unmatched intent to the retirement queue. Without it a stream whose delete was
    /// refused by storage would stay open forever, serving rows Postgres no longer has, with no
    /// record anywhere that it should be gone (the engine has already forgotten the shape).
    Retired {
        id: String,
    },
    /// The sequencer's checkpoint: the change-log position a restart replays from, plus the
    /// `(lsn, seq)` de-duplication highwater it had applied up to at that position (ADR-0003).
    ///
    /// The highwater has to travel WITH the position. A commit too large for one request body is
    /// appended in several chunks, so a crash can leave a prefix of a transaction applied and
    /// checkpointed while the rest is re-delivered; without the restored highwater that prefix would
    /// be applied twice, and aggregate/subquery contributor weights are not idempotent under
    /// duplicates. `None` is a checkpoint taken before anything was applied (a fresh boot).
    Offset {
        pos: LogPosition,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        highwater: Option<(u64, u64)>,
    },
    /// The change log rotated (ADR-0006): `segment` became the CURRENT segment at `at` (unix
    /// seconds). Written on the first creation of segment 0 too, so every segment's start time is
    /// in the log — segment `n` was closed when `n+1` began, which is what the retain window that
    /// governs deletion measures against. The fold's last one is the current segment.
    ChangesRotated {
        segment: u32,
        at: u64,
    },
    /// The retention sweeper deleted a rotated-out change-log segment (ADR-0006). Without it the
    /// fold's segment set would keep every segment the log ever had, so every restart would re-plan
    /// retiring streams that are long gone (harmless — a delete of an absent stream is a no-op — but
    /// it would also make the `changes_segments_retained` gauge wrong until the first sweep).
    ChangesSegmentDeleted {
        segment: u32,
    },
    /// **Audit only**: a table's schema drifted and was re-introspected (ADR-0005). The restore
    /// ignores it — every dependent shape of the table was retired by the same handler, so it is
    /// already `Dropped` in the log. It is written so the durable record explains *why* a swathe of
    /// shapes disappeared at a given point.
    SchemaChanged {
        table: TableRef,
        fingerprint: crate::schema::SchemaFingerprint,
    },
    /// The engine created (or first adopted) its replication slot: the **epoch** every shape after
    /// this point in the log belongs to (ADR-0004). The LAST one wins — a reset appends a new one
    /// after the `Dropped` records of the epoch it ended, so a fold reads "these shapes, in this
    /// epoch" straight off the log.
    SlotBound(crate::engine::epoch::SlotBinding),
}

/// `serde`'s `skip_serializing_if` wants a path, so the "omit the default" predicate is a function.
fn is_false(b: &bool) -> bool {
    !*b
}

/// Exit code when the durable catalog **refused** an event (`EX_IOERR`): storage answered, the
/// answer will not change with time, and the event cannot be written.
///
/// This is the pathological state "memory and storage disagree" — the engine is serving shapes the
/// durable record does not describe, and every second it keeps running widens the gap. There is no
/// in-process repair: the catalog is append-only and the fold IS the restart contract, so the only
/// way memory becomes consistent again is to re-fold from storage, i.e. to restart. So the process
/// exits, loudly and named, rather than continuing over a record it knows is wrong.
pub(crate) const EXIT_CATALOG_REFUSED: i32 = 74;

/// Exits the process if the catalog writer task panics.
///
/// A dead writer is the same pathological state as a refusal: nothing this process mutates from here
/// on reaches the durable record, and whatever it was appending when it died may or may not have
/// landed. Serving on would widen the gap between memory and storage, so it ends exactly like a
/// refusal — [`EXIT_CATALOG_REFUSED`], and the boot re-folds the catalog. Held as a guard local to
/// the writer task, it is dropped while the panic unwinds, before tokio reports the task failed.
struct WriterExit;

impl Drop for WriterExit {
    fn drop(&mut self) {
        if std::thread::panicking() {
            tracing::error!(
                "the durable catalog writer panicked: no catalog event can land in this process again, and \
                 the one in flight may or may not have. Exiting {EXIT_CATALOG_REFUSED}: a boot re-folds the \
                 catalog."
            );
            std::io::Write::flush(&mut std::io::stderr()).ok();
            std::process::exit(EXIT_CATALOG_REFUSED)
        }
    }
}

/// What the writer does about an append it could not complete. Pure and separately decided, so the
/// "exit the process" branch is unit-testable without exiting anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendVerdict {
    /// Transport, timeout, 5xx or 429: storage is having a moment. Retry THIS event, in place, forever —
    /// the queue is ordered and single-consumer, so everything behind it waits, which is exactly
    /// right (a later event must never reach the log before an earlier one).
    Retry,
    /// A definite refusal: a 4xx the catalog cannot recover from, or an event that would not
    /// serialize. Nothing about waiting changes the answer.
    Refused,
}

/// Classify a failed catalog append. `is_unavailable` is deliberately the ONLY forgiving side (see
/// [`crate::ds::is_unavailable`]): it forgives the transport, 5xx and 429, and nothing else.
pub(crate) fn classify_append(e: &anyhow::Error) -> AppendVerdict {
    if crate::ds::is_unavailable(e) { AppendVerdict::Retry } else { AppendVerdict::Refused }
}

/// The retry schedule for a catalog append: 100 ms before the first retry, doubling, capped at 5 s.
/// `attempt` counts from 1. Pure, so the schedule is a unit test rather than a comment.
pub(crate) fn catalog_backoff(attempt: u32) -> std::time::Duration {
    let step = attempt.saturating_sub(1).min(6);
    std::time::Duration::from_millis(100u64.saturating_mul(1u64 << step)).min(CATALOG_BACKOFF_MAX)
}

const CATALOG_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(5);

/// One queued catalog event, plus the (optional) acknowledgement its sender is waiting on.
struct CatalogSend {
    ev: CatalogEvent,
    /// Monotonic queue position. A client retry that finds its in-memory mutation already applied
    /// waits until this position has landed before acknowledging the apparent no-op.
    seq: u64,
    /// The event id the fold de-duplicates on (ADR-0008). Assigned at ENQUEUE, so every append
    /// attempt of this event — the first and every retry after a lost response — carries the same
    /// one.
    eid: String,
    /// Resolved once the append has SUCCEEDED — the durable-before-ack half of a client-facing
    /// mutation (see [`CatalogWriter::send_durable`]). `None` for engine-initiated events, which
    /// nobody is waiting on.
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

/// The catalog writer's ordered channel, plus the count of events sent but not yet appended.
///
/// The counter exists for one caller: the circuit-tier drift path exits the process, and it must
/// not do so with its own `Dropped`/`SchemaChanged` events still in the queue — a restart would
/// then restore shapes whose streams it had just deleted (see [`CatalogWriter::drain`]).
#[derive(Clone)]
pub(crate) struct CatalogWriter {
    tx: mpsc::UnboundedSender<CatalogSend>,
    in_flight: Arc<std::sync::atomic::AtomicI64>,
    /// This process's half of every `eid` it mints: a boot nonce, so two engines writing to the
    /// same catalog (a restart, or the same storage adopted by another process) can never mint the
    /// same id for different events. The other half is [`Self::next_eid`].
    eid_nonce: String,
    /// Monotonic within the process. Together with `eid_nonce` this is the whole identity of an
    /// append attempt — no clock, no hashing of the payload (two identical `Left`s ARE two
    /// different events; only a retry of the same one must be de-duplicated).
    next_eid: Arc<std::sync::atomic::AtomicU64>,
    /// Highest queue position that has landed, plus a wake-up for durability barriers.
    landed_seq: Arc<std::sync::atomic::AtomicU64>,
    landed_notify: Arc<tokio::sync::Notify>,
    /// The last `Offset` checkpoint that has actually **landed in storage** — what a restart would
    /// resume from, as opposed to what this process has processed in memory.
    ///
    /// It is the floor for change-log segment deletion (ADR-0006), and it has to be: the sequencer
    /// publishes its in-memory position the instant it crosses a segment boundary, but the
    /// checkpoint is an async append. Deleting on the in-memory position would let a sweep delete
    /// segment `n` while the durable checkpoint still says `(n, X)` — and a crash in that window
    /// leaves a boot resuming inside a stream that no longer exists.
    durable: Arc<std::sync::Mutex<Option<LogPosition>>>,
}

impl CatalogWriter {
    /// Enqueue an event. Infallible by design: a dead writer means the process is going away, and
    /// no caller has a better answer than continuing (the previous code spelled this `let _ =`).
    ///
    /// For an event a CLIENT is being told about — a create, a join, a release, an explicit purge —
    /// use [`Self::send_durable`] instead: an acknowledged mutation the durable record does not
    /// contain is a shape that vanishes at the next restart.
    pub(crate) fn send(&self, ev: CatalogEvent) {
        self.enqueue(ev, None);
    }

    /// Enqueue an event and hand back a future that resolves once it has **landed in storage**.
    ///
    /// The send happens here, synchronously, so callers keep enqueueing under the state lock and the
    /// log order still matches the state-mutation order; only the WAIT moves to the caller's own
    /// await point (after the lock is released, immediately before it answers its client).
    ///
    /// It resolves only once the append landed, or the process exits: the writer retries a transient
    /// failure forever, exits [`EXIT_CATALOG_REFUSED`] on a definite refusal, and exits the same way if
    /// its task panics ([`WriterExit`]). There is deliberately no timeout — a
    /// create while storage is down waits, because the alternative is telling a client about a shape
    /// that will not exist after a restart. The client has its own timeout; if it gives up and the
    /// record lands anyway, the shape has no subscriber and retention evicts it.
    pub(crate) fn send_durable(&self, ev: CatalogEvent) -> impl std::future::Future<Output = ()> + Send + 'static {
        let (done, wait) = tokio::sync::oneshot::channel();
        self.enqueue(ev, Some(done));
        async move {
            // An `Err` means the acknowledgement was dropped unsent: the writer task died, and
            // `WriterExit` is taking the process down with it. Whether the record landed is unknown,
            // so the caller must never be let through to tell its client it did — it waits for the
            // exit instead.
            if wait.await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Assign the event's `eid` and queue it. The id is minted HERE, once, so a retry-in-place
    /// re-appends the same identity — the fold then applies it exactly once however many copies of
    /// the record a lost response left in the log (ADR-0008).
    fn enqueue(&self, ev: CatalogEvent, done: Option<tokio::sync::oneshot::Sender<()>>) {
        let n = self.next_eid.fetch_add(1, Ordering::SeqCst);
        let eid = format!("{}-{n:x}", self.eid_nonce);
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        if self.tx.send(CatalogSend { ev, seq: n, eid, done }).is_err() {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Wait until every event enqueued before this call is durable. Used by an idempotent retry
    /// whose first request already changed memory and is still waiting on its own catalog append.
    pub(crate) async fn wait_durable(&self) {
        let target = self.next_eid.load(Ordering::SeqCst).saturating_sub(1);
        loop {
            let notified = self.landed_notify.notified();
            if self.landed_seq.load(Ordering::SeqCst) >= target {
                return;
            }
            notified.await;
        }
    }

    /// The last change-log position whose `Offset` checkpoint is durable (see [`Self::durable`]).
    pub(crate) fn durable_offset(&self) -> Option<LogPosition> {
        self.durable.lock().unwrap().clone()
    }

    /// Adopt the checkpoint the boot restored from the catalog: it is by definition already durable,
    /// and without it nothing could be deleted until this process wrote its first checkpoint.
    pub(crate) fn seed_durable_offset(&self, pos: LogPosition) {
        let mut g = self.durable.lock().unwrap();
        if g.as_ref().is_none_or(|cur| *cur < pos) {
            *g = Some(pos);
        }
    }

    /// Wait until every event sent so far has been appended (or `timeout` elapses — reported, never
    /// hung on). Returns whether the queue actually drained.
    pub(crate) async fn drain(&self, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while self.in_flight.load(Ordering::SeqCst) > 0 {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        true
    }
}

/// Spawn the single catalog writer: events are appended strictly in send order (senders enqueue
/// while holding the engine-state lock, so the log order matches the state-mutation order).
///
/// **The writer never drops an event.** A transient failure (transport, timeout, 5xx) retries THAT
/// event in place, forever, so the log keeps its order and a restart cannot under-restore; a
/// definite refusal exits [`EXIT_CATALOG_REFUSED`], because an engine whose memory and durable
/// record disagree has no honest way to continue. The retry loop is bounded in practice by the
/// shutdown grace: while it is retrying the writer registers a `catalog writer` shutdown party, so
/// a `SIGTERM` during an outage exits 70 NAMING it rather than looking like a mystery hang.
pub(crate) fn spawn_catalog_writer(ds: DsClient, shutdown: crate::shutdown::ShutdownToken) -> CatalogWriter {
    let (tx, mut rx) = mpsc::unbounded_channel::<CatalogSend>();
    let in_flight = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let counter = in_flight.clone();
    let durable: Arc<std::sync::Mutex<Option<LogPosition>>> = Arc::new(std::sync::Mutex::new(None));
    let landed = durable.clone();
    let landed_seq = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let writer_landed_seq = landed_seq.clone();
    let landed_notify = Arc::new(tokio::sync::Notify::new());
    let writer_landed_notify = landed_notify.clone();
    tokio::spawn(async move {
        let _exit = WriterExit;
        let mut ensured = false;
        // Latches the "catalog stream create failed" line to once per outage (see
        // `ensure_catalog_logged`); it outlives one event because the retry loop below runs per
        // attempt, and an outage spans many.
        let mut ensure_logged = false;
        while let Some(CatalogSend { ev, seq, eid, done }) = rx.recv().await {
            // Published only on a SUCCESSFUL append: an `Offset` whose write failed is not a
            // position a restart would resume from, and treating it as one would license deleting
            // the segment underneath it.
            let checkpoint = match &ev {
                CatalogEvent::Offset { pos, .. } => Some(pos.clone()),
                _ => None,
            };
            let json = match serde_json::to_value(&ev) {
                // The `eid` rides on the wire beside the event's own fields (the enum is
                // internally tagged, so the value is always a JSON object).
                Ok(serde_json::Value::Object(mut map)) => {
                    map.insert("eid".to_string(), serde_json::Value::String(eid.clone()));
                    serde_json::Value::Object(map)
                }
                Ok(other) => refuse(
                    &ev,
                    &anyhow::anyhow!("catalog event serialized to a non-object ({other}); it cannot carry an eid"),
                ),
                // Not a storage problem and not one waiting fixes: the engine has state it cannot
                // describe. Same verdict as a refusal, for the same reason.
                Err(e) => refuse(&ev, &anyhow::Error::new(e).context("catalog event could not be serialized")),
            };
            // Held only while an outage is in progress, so the ordinary case leaves
            // `wait_for_parties` untouched (a permanently-registered party would never let a
            // shutdown finish).
            let mut party: Option<crate::shutdown::ShutdownParty> = None;
            let mut attempt = 0u32;
            loop {
                // Inside the retry loop, not before it: the PUT that creates the catalog stream is
                // itself a storage round trip, so an engine that started while durable-streams was
                // down has not made it yet — and appending to a stream that does not exist answers
                // 404, which would otherwise read as a refusal and exit the process. Until it has
                // succeeded ONCE, every failure here is transient by construction. (After that, a
                // catalog stream that has *vanished* is a genuine refusal: the durable record has
                // been destroyed under a running engine, and continuing would quietly start a
                // second history.)
                if !ensured {
                    ensured = self::ensure_catalog(&ds, &mut ensure_logged).await;
                }
                match ds.append_json(CATALOG_STREAM, &[json.clone()]).await {
                    Ok(()) => break,
                    Err(e) => match if ensured { classify_append(&e) } else { AppendVerdict::Retry } {
                        AppendVerdict::Refused => refuse(&ev, &e),
                        AppendVerdict::Retry => {
                            attempt += 1;
                            crate::metrics::metrics().catalog_append_retries.fetch_add(1, Ordering::Relaxed);
                            // Once per outage, not once per attempt: an unreachable storage server
                            // must not turn one event into a log flood.
                            if attempt == 1 {
                                party = Some(shutdown.party("catalog writer"));
                                tracing::warn!(
                                    "catalog append failed ({e:#}); durable-streams is unavailable, so \
                                     this event is retried until it lands — every catalog mutation \
                                     behind it waits, and a client-facing create/join/release waits \
                                     with it. No event is dropped."
                                );
                            }
                            let base = catalog_backoff(attempt);
                            tokio::time::sleep(crate::replication::jitter(base, crate::replication::clock_nanos()))
                                .await;
                        }
                    },
                }
            }
            if let Some(pos) = checkpoint {
                let mut g = landed.lock().unwrap();
                if g.as_ref().is_none_or(|cur| *cur < pos) {
                    *g = Some(pos);
                }
            }
            writer_landed_seq.store(seq, Ordering::SeqCst);
            writer_landed_notify.notify_waiters();
            if attempt > 0 {
                tracing::info!("catalog append landed after {attempt} retr(ies); durable-streams is back");
                drop(party.take());
            }
            // Only now: `send_durable`'s contract is "resolved after the append SUCCEEDED".
            if let Some(done) = done {
                let _ = done.send(());
            }
            counter.fetch_sub(1, Ordering::SeqCst);
        }
    });
    CatalogWriter {
        tx,
        in_flight,
        durable,
        eid_nonce: process_nonce(),
        next_eid: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        landed_seq,
        landed_notify,
    }
}

/// A short, per-process nonce — the namespace half of both the catalog's event ids and the
/// subscription ids the engine mints (ADR-0008). Not cryptographic and not required to be: it only
/// has to differ between processes writing to the same catalog, and the pair (start time, address of
/// a fresh allocation) does that on every platform the engine runs on.
pub(crate) fn process_nonce() -> String {
    let secs =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
    let here = Box::new(0u8);
    let addr = std::ptr::addr_of!(*here) as usize as u64;
    format!("{:x}", secs ^ addr.rotate_left(17))
}

/// The refusal half of [`spawn_catalog_writer`]: name the event and what storage answered, then
/// exit. Split out (and `-> !`) so the loop above reads as "retry or die" with no third branch.
fn refuse(ev: &CatalogEvent, e: &anyhow::Error) -> ! {
    tracing::error!(
        "durable catalog REFUSED a {} event: {e:#}. Storage answered, and the answer will not change \
         — the engine is now serving state its durable record does not describe, which no restart of \
         the request and no amount of waiting can reconcile. Exiting {EXIT_CATALOG_REFUSED}: a boot \
         re-folds the catalog, which is the only way memory becomes consistent with storage again.",
        event_kind(ev)
    );
    std::io::Write::flush(&mut std::io::stderr()).ok();
    std::process::exit(EXIT_CATALOG_REFUSED)
}

/// The event's variant name, for the refusal message (`serde` tags it, but only on the way out).
fn event_kind(ev: &CatalogEvent) -> &'static str {
    match ev {
        CatalogEvent::Created { .. } => "created",
        CatalogEvent::Joined { .. } => "joined",
        CatalogEvent::Left { .. } => "left",
        CatalogEvent::Dormant { .. } => "dormant",
        CatalogEvent::Reactivated { .. } => "reactivated",
        CatalogEvent::Dropped { .. } => "dropped",
        CatalogEvent::Retired { .. } => "retired",
        CatalogEvent::Offset { .. } => "offset",
        CatalogEvent::ChangesRotated { .. } => "changesRotated",
        CatalogEvent::ChangesSegmentDeleted { .. } => "changesSegmentDeleted",
        CatalogEvent::SchemaChanged { .. } => "schemaChanged",
        CatalogEvent::SlotBound(_) => "slotBound",
    }
}

/// The durable catalog holds a record written **before** ADR-0002 (a bare `rec.table`).
///
/// A typed error so the boot can name it, and refuse outright rather than retry: nothing about
/// waiting changes it. Half-restoring a pre-qualification catalog is
/// the one outcome worse than not booting: the record's `sig` still carries the bare spelling, so an
/// identical post-cutover create would not share with it and the engine would quietly maintain two
/// streams for one table, forever. Recovery is a deliberate human act (reset the storage), not
/// something the engine gets to paper over.
#[derive(Debug)]
pub struct CatalogPredatesQualification {
    detail: String,
}

impl std::fmt::Display for CatalogPredatesQualification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "durable catalog predates ADR-0002 ({}); reset the durable-streams data directory", self.detail)
    }
}

impl std::error::Error for CatalogPredatesQualification {}

/// The durable catalog holds a change-log position written **before** ADR-0006 (a bare offset
/// string, from when the log was one un-segmented `changes` stream).
///
/// Fatal for the same reason [`CatalogPredatesQualification`] is: the value cannot be repaired, and
/// guessing would be worse than not booting. A bare offset is a byte position in a stream that no
/// longer exists under that name; adopting it as "segment 0" would resume the sequencer — or a
/// dormant shape — at an unrelated point in a different segment's byte space, silently replaying or
/// silently skipping an arbitrary span of changes. Greenfield: no such catalog can exist except
/// from a pre-cutover build, and recovery is a deliberate human act (reset the storage).
#[derive(Debug)]
pub struct CatalogPredatesSegmentation {
    detail: String,
}

impl std::fmt::Display for CatalogPredatesSegmentation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "durable catalog predates ADR-0006 ({}); reset the durable-streams data directory", self.detail)
    }
}

impl std::error::Error for CatalogPredatesSegmentation {}

/// The durable catalog holds an event written **before** ADR-0008: no `eid`, or a shape-lifecycle
/// event with no `subscription`.
///
/// Fatal for the same reason the two above are, and it is the same class of thing: the value cannot
/// be reconstructed and guessing would be worse than not booting. Without an `eid` the fold cannot
/// tell a retried append from a second event, so a lost response in the old log has already been
/// applied twice and there is no way to know which; without a `subscription` the live SET cannot be
/// rebuilt, and inventing ids would restore a refcount that no client can ever release (nothing the
/// caller holds names it). Greenfield: no such catalog can exist except from a pre-cutover build,
/// and recovery is a deliberate human act (reset the storage).
#[derive(Debug)]
pub struct CatalogPredatesSubscriptions {
    detail: String,
}

impl std::fmt::Display for CatalogPredatesSubscriptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "durable catalog predates ADR-0008 ({}); reset the durable-streams data directory", self.detail)
    }
}

impl std::error::Error for CatalogPredatesSubscriptions {}

/// A restored shape's stream passed the restore's check and was gone by the time the resume wrote to
/// it: storage lost it (or something outside the engine deleted it) in between.
///
/// Typed because the boot retries it rather than refusing (`pg::boot_disposition`). The resume
/// cannot retire the shape itself — by then its neighbours are installed, and the restore is all or
/// nothing — but the shape is not beyond the restore either: the retried attempt's check finds the
/// stream missing and retires it the ordinary way (ADR-0009). Carried as `anyhow` context, so it is
/// found with `anyhow::Error::downcast_ref` rather than by walking `chain()`.
#[derive(Debug)]
pub struct RestoreStreamVanished {
    pub shape: String,
}

impl std::fmt::Display for RestoreStreamVanished {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shape {}'s stream vanished after the restore checked it; a retried restore retires it", self.shape)
    }
}

/// Is this raw catalog event missing what ADR-0008 requires of it? `Some(detail)` names it.
///
/// Positive-checking the raw JSON rather than trusting the deserializer, exactly like
/// [`predates_segmentation`]: an event the strict deserializer cannot read is otherwise silently
/// skipped by the fold, which for a `Created` would mean a shape that simply disappears at boot.
fn predates_subscriptions(ev: &serde_json::Value) -> Option<String> {
    let kind = ev.get("t").and_then(serde_json::Value::as_str).unwrap_or("<untagged>");
    if ev.get("eid").and_then(serde_json::Value::as_str).is_none() {
        return Some(format!("a {kind} event with no eid"));
    }
    if matches!(kind, "created" | "joined" | "left")
        && ev.get("subscription").and_then(serde_json::Value::as_str).is_none()
    {
        return Some(format!("a {kind} event with no subscription"));
    }
    if matches!(kind, "created" | "joined") && ev.get("at").and_then(serde_json::Value::as_u64).is_none() {
        return Some(format!("a {kind} event with no lease timestamp"));
    }
    None
}

/// The numeric half of a shape id (`s7` -> `7`). `None` for anything that is not one — the ids the
/// engine mints always are, and a foreign spelling must not silently reset the id counter.
fn shape_id_num(id: &str) -> Option<u64> {
    id.strip_prefix('s')?.parse().ok()
}

/// Is this raw catalog event a pre-ADR-0006 change-log position? `Some(detail)` names it.
///
/// Deliberately positive-checking the OLD spelling rather than trusting the strict deserializer to
/// fail: an unparseable event is otherwise silently skipped by the fold, which for an `Offset` would
/// silently restart the sequencer from the beginning of the log.
fn predates_segmentation(ev: &serde_json::Value) -> Option<String> {
    match ev.get("t").and_then(serde_json::Value::as_str) {
        Some("offset") if ev.get("offset").is_some_and(serde_json::Value::is_string) => {
            Some(format!("bare change-log offset {}", ev["offset"]))
        }
        Some("dormant") if ev.get("resume_offset").is_some_and(serde_json::Value::is_string) => Some(format!(
            "bare dormant resume offset {} for shape {}",
            ev["resume_offset"],
            ev.get("id").and_then(serde_json::Value::as_str).unwrap_or("<unknown>")
        )),
        _ => None,
    }
}

/// Did the shape's table move while the engine was down? `Some(description)` if so.
///
/// Only Postgres-mode tables can answer: a library-mode table has no fingerprint on either side and
/// is restored as before. A record with no fingerprint of its own, over a table that HAS one, cannot
/// be vouched for and is retired — greenfield, so that is a catalog written before the field
/// existed, not a format to keep compatibility with.
fn schema_moved_while_down(rec: &ShapeRecord, compiled: &HashMap<TableRef, TableSchema>) -> Option<String> {
    let now = compiled.get(&rec.table)?.fingerprint.as_ref()?;
    match &rec.fingerprint {
        Some(then) if then.still_serves(now) => None,
        Some(then) => Some(crate::schema::describe_drift(then, now).join("; ")),
        None => Some("the record predates schema fingerprinting".to_string()),
    }
}

/// A restored record the boot will retire rather than resume, and why (ADR-0009).
struct Unrestorable {
    id: String,
    table: TableRef,
    stream_path: String,
    reason: crate::metrics::RestoreRetireReason,
    detail: String,
}

/// Is this record beyond resuming on facts the boot already holds? `Some((reason, detail))` if so.
///
/// Pure and decided before any IO, because none of these answers can change by waiting. The table
/// check comes FIRST: [`schema_moved_while_down`] has nothing to compare a missing table against and
/// answers `None`, and a resume would then fail on the absent schema — deterministically, on every
/// boot, taking the whole restore down with it. A table leaving the compiled set is ADR-0005's
/// per-table retirement seen from the boot side, never a reason to refuse the engine.
///
/// A projected (or predicate, or aggregate) column that no longer exists needs no check of its own:
/// the fingerprint lists every column, so dropping one is drift, and a Postgres-mode table always
/// carries a fingerprint (`pg::introspect_opt`).
fn unrestorable(
    rec: &ShapeRecord,
    compiled: &HashMap<TableRef, TableSchema>,
) -> Option<(crate::metrics::RestoreRetireReason, String)> {
    use crate::metrics::RestoreRetireReason as Reason;
    if !compiled.contains_key(&rec.table) {
        return Some((
            Reason::TableGone,
            "its table is no longer in the compiled set (dropped under a wildcard selector, or no longer selected by \
             ELECTRIC_CIRCUITS_PG_TABLES)"
                .to_string(),
        ));
    }
    // DDL while the engine was DOWN is seen by nothing on the live path: no `Relation` message, no
    // reconciler tick. The record's own fingerprint is the only witness — if it no longer matches
    // what boot introspected, the retained stream holds rows shaped by the old schema and can never
    // be brought up to date (ADR-0005).
    if let Some(what) = schema_moved_while_down(rec, compiled) {
        return Some((Reason::Schema, format!("its schema changed while the engine was down ({what})")));
    }
    // Subquery shapes are registry-served and their inner-node contributor state is not persisted:
    // a fresh-seeded node cannot detect flips that happened during downtime (stale move-outs would
    // persist forever), so they are dropped loudly and clients recreate them.
    if rec.is_subquery {
        return Some((Reason::Subquery, "subquery inner-node state is not persisted".to_string()));
    }
    None
}

/// Idempotently create the catalog stream, logging a failure only the FIRST time through an outage
/// — the writer retries this before every append attempt while the stream has never been created,
/// and one error line per attempt for as long as storage is down is a flood, not information.
/// `logged` is the caller's per-outage latch; it is cleared again on success.
async fn ensure_catalog(ds: &DsClient, logged: &mut bool) -> bool {
    match ds.ensure_stream(CATALOG_STREAM).await {
        Ok(()) => {
            *logged = false;
            true
        }
        Err(e) => {
            if !*logged {
                tracing::error!("catalog stream create failed: {e:#}");
                *logged = true;
            }
            false
        }
    }
}

/// One shape as the fold reconstructed it: (record, sharing signature, **live subscriptions**,
/// dormant resume state). The last `Dormant`/`Reactivated` event wins.
///
/// The subscriptions are a SET, not a count (ADR-0008): `subscription id -> the wall-clock second
/// its lease was last renewed at`. A repeated `Joined` for an id already in the set moves the
/// timestamp and nothing else; a `Left` for an id that is not in it does nothing at all. That is
/// what makes a duplicated record — a retry after a lost response, an over-eager client — harmless
/// where counter arithmetic was not.
type Restored = (
    ShapeRecord,
    Option<String>,
    std::collections::BTreeMap<String, u64>,
    Option<(LogPosition, crate::pg::SnapshotGate)>,
);

/// The durable catalog, folded — everything a boot needs before it decides what to *do* with it.
///
/// Reading and deciding are separate because the epoch check has to happen between them (ADR-0004):
/// the binding to verify against is in the log, and no shape may be resumed before the verdict is
/// in.
pub(crate) struct CatalogFold {
    recs: HashMap<String, Restored>,
    /// Shapes whose `Dropped` intent is in the log with no `Retired` completion after it: shape id →
    /// stream path. These are streams the engine promised to remove and did not — the boot hands
    /// them to the retirement queue (see [`Engine::apply_catalog`]).
    ///
    /// Bounded by the retirements actually outstanding, not by the log's length: the entry is
    /// inserted on `Dropped` and removed again on the matching `Retired`.
    pending_retire: HashMap<String, String>,
    /// The highest numeric shape id the log has EVER minted — from every `Created`, including the
    /// ones later dropped. `None` = the log created no shape.
    ///
    /// This, and not the surviving records, is the restart's id high-water mark. A dropped shape's
    /// id stays spoken for as long as anything of it survives: its `shape/sN` stream lives until the
    /// retirement lands, so re-minting `sN` would hand a brand-new shape the dead one's stream —
    /// `ensure_stream` is idempotent, so the PUT succeeds, the backfill appends to a stream holding
    /// pre-`TRUNCATE` rows, and the pending retirement then closes and deletes the LIVE shape's
    /// stream (and records `Retired` against a registered shape, whose appends are `Gone` from then
    /// on). Ids are never reused, full stop.
    max_shape_id: Option<u64>,
    /// The sequencer's change-log replay start (the last `Offset` checkpoint).
    start_pos: LogPosition,
    /// The `(lsn, seq)` de-duplication highwater recorded with that checkpoint (ADR-0003).
    start_highwater: Option<(u64, u64)>,
    /// The change log's current segment: the last `ChangesRotated`, else 0 (ADR-0006). A lower
    /// bound — a process can die between closing a segment and recording the rotation — so the
    /// boot walks forward from here (`changelog::resolve_current`).
    pub(crate) current_segment: u32,
    /// Every segment's start time (unix seconds), for the retain window that governs deletion.
    pub(crate) segment_starts: std::collections::BTreeMap<u32, u64>,
    /// The last `SlotBound`: the epoch these shapes belong to. `None` = nothing ever claimed one,
    /// which is a genuine first boot.
    pub(crate) binding: Option<crate::engine::epoch::SlotBinding>,
    /// Every `eid` this fold has already applied (ADR-0008). The writer retries a failed append in
    /// place, so a response lost after the append committed leaves the SAME event in the log twice;
    /// applying the second copy is exactly the double-count this set exists to prevent.
    ///
    /// Bounded by the catalog's length, like the fold itself — it lives only for the duration of
    /// one boot's read and is dropped with the fold.
    seen: HashSet<String>,
}

impl Default for CatalogFold {
    fn default() -> Self {
        CatalogFold {
            recs: HashMap::new(),
            pending_retire: HashMap::new(),
            max_shape_id: None,
            start_pos: LogPosition::start(),
            start_highwater: None,
            binding: None,
            current_segment: 0,
            segment_starts: std::collections::BTreeMap::new(),
            seen: HashSet::new(),
        }
    }
}

impl CatalogFold {
    /// Fold one event in, **once**: an `eid` this fold has already applied is ignored (ADR-0008).
    ///
    /// The de-duplication belongs here rather than in [`Self::apply`] because it is a property of
    /// the LOG (the writer may have appended the same event twice), not of the event's meaning.
    fn apply_once(&mut self, eid: &str, ev: CatalogEvent) {
        if !self.seen.insert(eid.to_string()) {
            tracing::debug!("catalog restore: ignoring a repeated append of event {eid}");
            return;
        }
        self.apply(ev);
    }

    /// Fold one event in. Pure (no engine, no IO), so the log's semantics — last-writer-wins for the
    /// epoch and the offset, remove-on-drop for the shapes — are unit-testable.
    fn apply(&mut self, ev: CatalogEvent) {
        match ev {
            CatalogEvent::Created { rec, sig, subscription, at } => {
                // Every create moves the id high-water mark, and nothing ever moves it back (see
                // `max_shape_id`). `max`, not "the last one wins": the engine mints ids
                // monotonically from one counter, so the log is monotonic anyway, and taking the
                // maximum means a log that somehow is not cannot lower the mark.
                if let Some(num) = shape_id_num(&rec.id) {
                    self.max_shape_id = Some(self.max_shape_id.map_or(num, |cur| cur.max(num)));
                }
                self.recs.insert(rec.id.clone(), (rec, sig, [(subscription, at)].into_iter().collect(), None));
            }
            // A join and a lease RENEWAL are the same record: insert wins for a new id, and only
            // moves the lease for one already held (ADR-0008). The restored `at` is what stops a
            // restart from handing every subscription a fresh idle window.
            CatalogEvent::Joined { id, subscription, at } => {
                if let Some(e) = self.recs.get_mut(&id) {
                    let lease = e.2.entry(subscription).or_insert(at);
                    *lease = (*lease).max(at);
                }
            }
            // Idempotent by construction: releasing an id the set does not hold is a no-op, which
            // is what makes a client's retried DELETE safe.
            CatalogEvent::Left { id, subscription, lapsed: _ } => {
                if let Some(e) = self.recs.get_mut(&id) {
                    e.2.remove(&subscription);
                }
            }
            CatalogEvent::Dormant { id, resume, gate } => {
                if let Some(e) = self.recs.get_mut(&id) {
                    e.3 = Some((resume, gate));
                }
            }
            CatalogEvent::Reactivated { id } => {
                if let Some(e) = self.recs.get_mut(&id) {
                    e.3 = None;
                }
            }
            // The record goes; the obligation to retire its stream stays until a `Retired` says it
            // was honoured. The path is taken from the record itself (the event carries only the
            // id): a `Dropped` for a record this fold never saw is a duplicate, and there is
            // nothing left to retire.
            CatalogEvent::Dropped { id } => {
                if let Some((rec, ..)) = self.recs.remove(&id) {
                    self.pending_retire.insert(id, rec.stream_path);
                }
            }
            CatalogEvent::Retired { id } => {
                self.pending_retire.remove(&id);
            }
            CatalogEvent::Offset { pos, highwater } => {
                self.start_pos = pos;
                self.start_highwater = highwater;
            }
            // The LAST rotation is the current segment; every one of them is kept, because the
            // retain window needs to know when each segment began (see the variant).
            CatalogEvent::ChangesRotated { segment, at } => {
                self.current_segment = self.current_segment.max(segment);
                self.segment_starts.insert(segment, at);
            }
            // A deleted segment leaves the set but never moves `current_segment`: the current one
            // is never deleted, so the last rotation still names it.
            CatalogEvent::ChangesSegmentDeleted { segment } => {
                self.segment_starts.remove(&segment);
            }
            // Audit only (see the variant): the shapes it explains are already `Dropped`,
            // and the boot's own introspection is the authority on the schema.
            CatalogEvent::SchemaChanged { .. } => {}
            // The LAST binding is the epoch in force. A reset appends its new one after the
            // `Dropped` records of the epoch it ended, so the two always agree.
            CatalogEvent::SlotBound(binding) => self.binding = Some(binding),
        }
    }

    /// The sequencer's restored change-log position — the segment the boot must vouch for before
    /// anything resumes (see `Engine::init_change_log`).
    pub(crate) fn start_pos(&self) -> LogPosition {
        self.start_pos.clone()
    }

    /// Every `Dropped` with no `Retired` after it: `(shape id, stream path)`, sorted by id so the
    /// boot's enqueue order is deterministic. This is the orphan-`shape/*` GC, bounded by the
    /// catalog rather than by a storage listing (durable-streams exposes no list API).
    pub(crate) fn pending_retirements(&self) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> =
            self.pending_retire.iter().map(|(id, path)| (id.clone(), path.clone())).collect();
        v.sort();
        v
    }

    /// Nothing was ever written (or everything was dropped and nothing checkpointed).
    ///
    /// Deliberately does NOT consider `pending_retire`: this asks "is there anything to INSTALL",
    /// and the boot enqueues outstanding retirements before it consults this at all.
    fn is_empty(&self) -> bool {
        self.recs.is_empty() && self.start_pos == LogPosition::start()
    }
}

/// How many restored streams the boot checks at once. Enough that a large catalog is not one round
/// trip after another, few enough not to be a burst against storage that may itself be coming up.
const RESTORE_HEAD_CONCURRENCY: usize = 16;

/// Tries each restored stream's `HEAD` gets before a transient failure fails the restore
/// (`DsClient::head_retrying`: 100, 200, 300, 400 ms apart). More than a join's: nobody is waiting on
/// a boot the way a client waits on a join, and giving up costs the whole attempt.
const RESTORE_HEAD_ATTEMPTS: u32 = 5;

/// How much of a folded catalog a boot actually installs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RestoreMode {
    /// The ordinary boot: restore the records and re-register them with the sequencer.
    Resume,
    /// The epoch broke (ADR-0004). Restore the shape RECORDS and nothing else — no resume, no
    /// sequencer registration, no retention lifecycle, and above all no teardown of its own.
    ///
    /// The records are what a reset needs in order to retire the shapes *properly* (close the
    /// stream, then delete it, and write `Dropped`), whether that reset happens immediately (the
    /// auto policy) or when an operator posts `/epoch/reset`. Nothing is resumed, so no old-epoch
    /// shape is ever maintained for an instant; nothing is destroyed either, so a refusing engine
    /// has not thrown anything away before the human said so.
    Park,
}

impl Engine {
    /// Read the durable shape catalog and fold it. No engine state is touched — see
    /// [`Self::apply_catalog`] for that half.
    pub(crate) async fn fold_catalog(&self) -> Result<CatalogFold> {
        let mut fold = CatalogFold::default();
        let mut off = "-1".to_string();
        loop {
            let (events, next, up_to_date) = self.ds.read_json(CATALOG_STREAM, &off).await?;
            for ev in events {
                // The catalog is engine-written, so `rec.table` is ALWAYS the canonical
                // `schema.name` (`ShapeRecord`'s strict deserializer enforces it). A bare one can
                // only be a catalog written before ADR-0002: refuse the boot naming the record,
                // rather than let the strict deserializer turn it into a silently skipped event.
                if let Some(raw) = ev.pointer("/rec/table").and_then(serde_json::Value::as_str)
                    && crate::table_ref::TableRef::parse(raw).is_ok_and(|t| t.as_str() != raw)
                {
                    let id = ev.pointer("/rec/id").and_then(serde_json::Value::as_str).unwrap_or("<unknown>");
                    return Err(anyhow::Error::new(CatalogPredatesQualification {
                        detail: format!("bare table name '{raw}' in shape {id}"),
                    }));
                }
                // Same stance for a pre-segmentation change-log position (ADR-0006): refuse the
                // boot naming it, rather than let the strict deserializer turn it into a silently
                // skipped event.
                if let Some(detail) = predates_segmentation(&ev) {
                    return Err(anyhow::Error::new(CatalogPredatesSegmentation { detail }));
                }
                // ...and for an event written before ADR-0008 (no `eid`, no `subscription`): the
                // fold cannot de-duplicate or rebuild the live set, and both failures are silent.
                if let Some(detail) = predates_subscriptions(&ev) {
                    return Err(anyhow::Error::new(CatalogPredatesSubscriptions { detail }));
                }
                let eid = ev["eid"].as_str().unwrap_or_default().to_string();
                let Ok(ev) = serde_json::from_value::<CatalogEvent>(ev) else { continue };
                fold.apply_once(&eid, ev);
            }
            match next {
                Some(n) if !up_to_date && n != off => off = n,
                _ => break,
            }
        }
        Ok(fold)
    }

    /// Install a folded catalog: re-register every restorable shape with the (not yet spawned)
    /// sequencer — see [`CATALOG_STREAM`] for the restore semantics per shape kind — or, in
    /// [`RestoreMode::Park`], record them and stop there.
    ///
    /// Resume runs in one order, and the order is the contract (ADR-0009): classify what the boot
    /// already knows (table gone, schema moved, subquery), `HEAD` every remaining stream, retire
    /// every record either step condemned, install the rest, resume them into a sequencer that reads
    /// nothing until all have resumed, then release it. An `Err` means nothing was installed, or
    /// everything installed was undone — never a partial registry.
    pub(crate) async fn apply_catalog(
        &self,
        fold: CatalogFold,
        compiled: &HashMap<TableRef, TableSchema>,
        mode: RestoreMode,
    ) -> Result<()> {
        // Resume starts from an engine that holds nothing. The boot gate (`Engine::ensure_booted`)
        // keeps every create, join and reactivation out until the boot resolves, and a failed attempt
        // rolls back everything it installed — so a registered shape or a running sequencer here is
        // not a state to work around: whatever put it there could have consumed or checkpointed
        // changes the restored shapes need, and resuming over it would hide that. Refused, loudly.
        if mode == RestoreMode::Resume {
            let st = self.state.lock().await;
            if st.sequencer.is_some() || !st.shapes.is_empty() {
                bail!(
                    "catalog restore found the engine already holding {} shape(s){} before it installed \
                     anything; nothing may register a shape or start the sequencer before the restore \
                     completes, so this is an engine bug — refusing to resume over it",
                    st.shapes.len(),
                    if st.sequencer.is_some() { " and a running sequencer" } else { "" }
                );
            }
        }
        // BEFORE anything else, and in both modes: a `Dropped` with no `Retired` is a shape stream a
        // previous process promised to remove and did not (its retirement was refused by storage,
        // or the process died between the two). The engine has already forgotten the shape, so
        // nothing else will ever notice — this is the only place it can be picked up. The queue
        // retries each one to completion and writes the `Retired` that closes it out.
        for (id, path) in fold.pending_retirements() {
            tracing::info!("restore: shape {id} was dropped but its stream {path} was never retired; queued");
            self.retirements.enqueue(&path, Some(&id));
        }
        // Also before anything else, and also in both modes: the id counter resumes past EVERY id
        // the log ever minted, not past the ones that survived (see `CatalogFold::max_shape_id`).
        // A catalog of nothing but creates and drops folds to `is_empty()`, so this cannot wait for
        // the branches below — that is exactly the case where re-minting would collide with a
        // `shape/*` stream whose retirement is still pending.
        if let Some(max) = fold.max_shape_id {
            let mut st = self.state.lock().await;
            st.next_shape_id = st.next_shape_id.max(max + 1);
        }
        if fold.is_empty() {
            return Ok(());
        }
        let CatalogFold { recs, start_pos, start_highwater, .. } = fold;
        tracing::info!("catalog restore: {} shape(s), change-log replay from {start_pos}", recs.len());
        // The restored checkpoint IS durable (it was read back out of the log), so it is the
        // segment-deletion floor from boot rather than from this process's first checkpoint.
        self.catalog_tx.seed_durable_offset(start_pos.clone());
        *self.seq_start.lock().unwrap() = start_pos;
        // The de-duplication highwater is restored with the position (ADR-0003), so a prefix of a
        // chunked commit that was applied and checkpointed before a crash is not applied twice when
        // Postgres re-delivers the transaction.
        *self.seq_highwater.lock().unwrap() = start_highwater;

        if mode == RestoreMode::Park {
            // The epoch broke: hold the records, touch nothing else (see `RestoreMode::Park`).
            let mut st = self.state.lock().await;
            for (id, (rec, _, _, _)) in recs {
                st.shapes.insert(id, rec); // `next_shape_id` was moved past every minted id above
                // No share entries and therefore no subscriptions: a parked epoch's shapes exist
                // only to be retired, and nothing may join or renew one.
            }
            tracing::warn!(
                "catalog restore: {} shape(s) parked over a broken epoch — none is resumed or \
                 maintained; they exist only to be retired by the reset",
                st.shapes.len()
            );
            return Ok(());
        }

        // 2. Decide, before anything is installed, which records are definitively beyond resuming.
        // Sorted by id, so the log lines, the retirement order and the resume order are the same on
        // every boot over the same catalog.
        let mut recs: Vec<(String, Restored)> = recs.into_iter().collect();
        recs.sort_by(|a, b| a.0.cmp(&b.0));
        let mut retire: Vec<Unrestorable> = Vec::new();
        let mut candidates: Vec<(String, Restored)> = Vec::with_capacity(recs.len());
        for (id, restored) in recs {
            // (`next_shape_id` was moved past every id the log ever minted, above.)
            match unrestorable(&restored.0, compiled) {
                Some((reason, detail)) => retire.push(Unrestorable {
                    table: restored.0.table.clone(),
                    stream_path: restored.0.stream_path.clone(),
                    id,
                    reason,
                    detail,
                }),
                None => candidates.push((id, restored)),
            }
        }

        // 3. Preflight: vouch for every remaining record's stream, still before installing anything.
        // A missing stream is an ordinary state rather than an impossible one — a plain or subquery
        // create enqueues its `Created` before it PUTs the stream (`lifecycle.rs`), so a process
        // killed between the two leaves a durable record with no stream behind it — and a closed
        // stream can never be appended to again. Both are definitive answers about THAT shape.
        // Anything else (transport, 5xx, 429, an unexpected status) that outlasts its retries is no
        // answer at all: the restore stops here with nothing installed, and the boot retries it or
        // refuses by name. A missing stream is never recreated — a fresh stream under the old id
        // would present a new projection as the continuation of the one its subscribers were reading.
        let heads = self.check_restored_streams(&candidates).await?;
        let mut install: Vec<(String, Restored)> = Vec::with_capacity(candidates.len());
        for ((id, restored), head) in candidates.into_iter().zip(heads) {
            let rec = &restored.0;
            let gone = match head {
                None => {
                    Some((crate::metrics::RestoreRetireReason::StreamMissing, "its stream is missing from storage"))
                }
                Some(h) if h.closed => {
                    Some((crate::metrics::RestoreRetireReason::StreamClosed, "its stream is closed to appends"))
                }
                Some(_) => None,
            };
            match gone {
                Some((reason, detail)) => retire.push(Unrestorable {
                    table: rec.table.clone(),
                    stream_path: rec.stream_path.clone(),
                    id,
                    reason,
                    detail: detail.to_string(),
                }),
                None => install.push((id, restored)),
            }
        }

        // 4. Retire the definitively dead, per shape and never whole-engine (ADR-0005): `Dropped`
        // first, then close-then-delete (ADR-0007) — clients may still be tailing these streams from
        // before the restart, and the close releases their long-poll at once with `stream-closed`.
        // None of it waits on the rest of the restore, because none of it depends on it: a shape
        // whose table is gone stays gone whether or not its neighbours resume. A storage failure
        // here is not "lost" either — `Dropped` is ordered ahead of the retirement on the writer, a
        // failed retirement goes to the background queue, and a crash before either lands leaves
        // the record for the next boot to classify again. Both steps above appended in id order,
        // but one after the other; one sort makes the whole retirement id-ordered.
        retire.sort_by(|a, b| a.id.cmp(&b.id));
        for dead in &retire {
            tracing::warn!(
                "restore: retiring shape {} on {} ({}) — {}; subscribers observe the closed stream and recreate",
                dead.id,
                dead.table,
                dead.reason.label(),
                dead.detail
            );
            self.catalog_tx.send(CatalogEvent::Dropped { id: dead.id.clone() });
            metrics().restore_retired(dead.reason);
        }
        for dead in retire {
            self.retire_shape_stream(&dead.id, &dead.stream_path).await;
        }

        // 5. Install records + shares + lifecycles. From here to the end it is all or nothing: a
        // shape that fails to resume undoes every one of these (`roll_back_restore`).
        let installed: Vec<String> = install.iter().map(|(id, _)| id.clone()).collect();
        let mut resume: Vec<ShapeRecord> = Vec::new();
        let cmd_tx = {
            let mut st = self.state.lock().await;
            for (id, (rec, sig, subs, dormant)) in install {
                st.shapes.insert(id.clone(), rec.clone());
                if let Some(sig) = sig {
                    // Restored feeds are live immediately (their streams already hold data).
                    let (ready_tx, ready_rx) = tokio::sync::watch::channel(ShareOutcome::Ready);
                    drop(ready_tx); // receivers keep observing `Ready`
                    st.feed_by_sig.insert(sig.clone(), id.clone());
                    st.feed_shares.insert(id.clone(), FeedShare { sig, subs: Default::default(), ready: ready_rx });
                    // The live set is restored WITH its lease ages (ADR-0008): a subscription that
                    // was already past the idle window when the process died is past it here too,
                    // so a restart neither forgets a subscription nor grants every one of them a
                    // fresh window to pin the shape with.
                    for (sub, at) in subs {
                        st.subscribe(&id, sub, at);
                    }
                }
                match dormant {
                    // A dormant shape restores AS dormant: record + stream retained, no routing,
                    // no replay at boot — the first touch reactivates it from its own resume
                    // offset. (Dormancy age restarts at boot; the TTL clock is conservative.)
                    Some((resume_at, gate)) => {
                        self.lives.lock().unwrap().insert(
                            id.clone(),
                            ShapeLife {
                                last_read: std::time::Instant::now(),
                                state: LifeState::Dormant { since: std::time::Instant::now(), resume: resume_at, gate },
                            },
                        );
                    }
                    None => {
                        self.lives.lock().unwrap().insert(id.clone(), ShapeLife::active());
                        resume.push(rec);
                    }
                }
            }
            // The sequencer is spawned HELD: it serves the registrations below but reads nothing
            // until every one of them is in. Reading while shapes are still arriving would consume
            // — and checkpoint past — changes that a shape registered a moment later never sees, and
            // a restore that then failed could no longer be retried from where it started. There is
            // no other sequencer to reuse: the check at the top refused one, and the boot gate keeps
            // anything from spawning one while this runs.
            let seq = self.spawn_sequencer_task(true);
            let cmd_tx = seq.cmd_tx.clone();
            st.sequencer = Some(seq);
            cmd_tx
        };

        // 6. Re-register with the sequencer. Plain/routed shapes resume without a backfill and
        // with a passthrough gate (`changes_only = true` path): everything after the restored
        // offset replays, and re-emission across the crash window is idempotent. Aggregates
        // re-seed their fold from a fresh snapshot (fresh gate skips the replayed history).
        //
        // A failure here is never a reason to drop the shape: every definitive reason was settled
        // above, so what is left is Postgres or storage having a moment (or a record the engine
        // cannot compile, which no amount of dropping would explain). The whole restore is undone
        // and the error goes to the boot, typed, for its retry-or-refuse decision.
        for rec in &resume {
            if let Err(e) = self.resume_shape(&cmd_tx, rec, compiled).await {
                tracing::error!("restore: shape {} failed to resume ({e:#}); undoing the whole restore", rec.id);
                self.roll_back_restore(&installed, &cmd_tx).await;
                // Storage confirming the stream gone is not a failure of THIS shape's resume to wait
                // out, and not a refusal either: the retried attempt's check will retire it.
                let e = if crate::ds::is_stream_gone(&e) {
                    e.context(RestoreStreamVanished { shape: rec.id.clone() })
                } else {
                    e
                };
                return Err(e.context(format!("resuming shape {} on {}", rec.id, rec.table)));
            }
        }
        // 7. Every shape is in: the sequencer may read.
        let (done, released) = tokio::sync::oneshot::channel();
        if cmd_tx.send(SequencerCmd::ReleaseReads { done }).is_err() || released.await.is_err() {
            self.roll_back_restore(&installed, &cmd_tx).await;
            bail!("the sequencer stopped before the restored shapes could be released to it");
        }
        // Restored dormant shapes need the TTL/eviction layers running. Only now, on success: with
        // no sequencer the sweep advances the durable checkpoint to the current segment, and between
        // a rolled-back attempt and its retry there IS no sequencer — a sweeper started earlier could
        // move the replay start past changes the retried shapes still need.
        self.ensure_retention_sweeper();
        Ok(())
    }

    /// `HEAD` every candidate's stream, [`RESTORE_HEAD_CONCURRENCY`] at a time, each retried in place
    /// through a transient failure ([`RESTORE_HEAD_ATTEMPTS`] tries). The answers come back in
    /// `candidates` order, so what the caller decides from them — and the order it retires in — does
    /// not depend on which response happened to arrive first.
    ///
    /// Retried in place because the alternative is expensive out of all proportion: a restore that
    /// fails is a whole boot attempt thrown away (introspection, the fold, the change log, the counts
    /// re-seed, every other check), and over a large catalog one dropped response per attempt would
    /// be enough to make no attempt succeed. After the first check that fails for good no new one is
    /// started; the ones in flight finish, and the error reported is the lowest-ordered failure — the
    /// same record however the in-flight checks interleave.
    async fn check_restored_streams(
        &self,
        candidates: &[(String, Restored)],
    ) -> Result<Vec<Option<crate::ds::StreamHead>>> {
        let mut answers: Vec<Option<Option<crate::ds::StreamHead>>> = vec![None; candidates.len()];
        let mut failed: Option<(usize, anyhow::Error)> = None;
        let mut checks = tokio::task::JoinSet::new();
        let mut next = 0usize;
        loop {
            while failed.is_none() && next < candidates.len() && checks.len() < RESTORE_HEAD_CONCURRENCY {
                let (i, ds, path) = (next, self.ds.clone(), candidates[next].1.0.stream_path.clone());
                checks.spawn(async move { (i, ds.head_retrying(&path, RESTORE_HEAD_ATTEMPTS).await) });
                next += 1;
            }
            let Some(joined) = checks.join_next().await else { break };
            let (i, answer) = joined.context("a restore stream check panicked")?;
            match answer {
                Ok(head) => answers[i] = Some(head),
                Err(e) => {
                    if failed.as_ref().is_none_or(|(first, _)| i < *first) {
                        failed = Some((i, e));
                    }
                }
            }
        }
        if let Some((i, e)) = failed {
            let (id, (rec, ..)) = &candidates[i];
            return Err(e.context(format!("checking shape {id}'s stream {} before restoring it", rec.stream_path)));
        }
        Ok(answers.into_iter().map(|a| a.expect("every stream check completed")).collect())
    }

    /// Undo everything one Resume attempt installed (ADR-0009): records, shares and the
    /// subscriptions they hold, lifecycles, circuit placements, and the sequencer's registrations.
    /// The retried boot then re-folds the catalog into an engine that holds none of it. The catalog
    /// needs no undo — installing wrote nothing to it.
    ///
    /// The held sequencer is discarded whole, with every registration in it: it never read, so it
    /// has no position worth keeping, and it captured this attempt's arrangement layer, which the
    /// retried boot replaces.
    async fn roll_back_restore(&self, ids: &[String], cmd_tx: &mpsc::UnboundedSender<SequencerCmd>) {
        let discarded = {
            let mut st = self.state.lock().await;
            for id in ids {
                if st.shapes.remove(id).is_none() {
                    continue;
                }
                // Subscriptions first: `forget_subscriptions` finds them through the share entry.
                st.forget_subscriptions(id);
                if let Some(share) = st.feed_shares.remove(id)
                    && st.feed_by_sig.get(&share.sig) == Some(id)
                {
                    st.feed_by_sig.remove(&share.sig);
                }
                st.circuit_placement.remove(id);
            }
            let mut lives = self.lives.lock().unwrap();
            for id in ids {
                lives.remove(id);
            }
            drop(lives);
            st.sequencer.take()
        };
        if discarded.is_some() {
            let (done, stopped) = tokio::sync::oneshot::channel();
            if cmd_tx.send(SequencerCmd::Discard { done }).is_ok() {
                let _ = stopped.await;
            }
        }
    }

    /// Re-register one restored shape with the sequencer (the resume half of `apply_catalog`).
    pub(crate) async fn resume_shape(
        &self,
        cmd_tx: &mpsc::UnboundedSender<SequencerCmd>,
        rec: &ShapeRecord,
        compiled: &HashMap<TableRef, TableSchema>,
    ) -> Result<()> {
        let ts = compiled.get(&rec.table).with_context(|| format!("table '{}' no longer exists", rec.table))?;
        let out_cols: Option<Arc<Vec<usize>>> = match &rec.columns {
            Some(names) => {
                let idx: Result<Vec<usize>> = names.iter().map(|n| ts.column_index(n)).collect();
                Some(Arc::new(idx?))
            }
            None => None,
        };
        let num_id: u64 = rec.id.trim_start_matches('s').parse().unwrap_or(0);
        // Circuit-served restore: re-register with the sequencer, seed=false for plain shapes
        // (the stream is already complete up to the resume offset; dynamic groups re-derive
        // from the router snapshot, which the catch-up replay has brought to the same point).
        // Aggregates re-seed from the counts snapshot (their fold is not persisted) — same
        // fresh-value semantics as the legacy aggregate resume.
        if let Some(arr) = self.arrangements.lock().unwrap().clone() {
            match &rec.aggregate {
                Some(a) if matches!(a.func, AggFn::Count) && a.col.is_none() => {
                    if let Some(gcols) = arr.counts_group_cols(&rec.table).map(|g| g.to_vec()) {
                        if let Some(constraints) = plan_circuit_agg(rec.where_json.as_ref(), ts, &gcols) {
                            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
                            cmd_tx
                                .send(SequencerCmd::CreateCircuitAgg {
                                    table: rec.table.clone(),
                                    shape_id: rec.id.clone(),
                                    stream_path: rec.stream_path.clone(),
                                    constraints,
                                    ready: ready_tx,
                                })
                                .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
                            ready_rx.await.unwrap_or_else(|_| Err(anyhow::anyhow!("sequencer dropped")))?;
                            self.state.lock().await.circuit_placement.insert(
                                rec.id.clone(),
                                CircuitPlacement { label: "counts".into(), col: None, counts: true },
                            );
                            return Ok(());
                        }
                    }
                }
                _ => {}
            }
        }
        // Compiled lazily, after the circuit branch: a circuit-served subquery record never
        // needs (and could not build) a registry-free compiled predicate.
        let pred = Arc::new(CompiledPredicate::compile_opt(rec.where_json.as_ref(), ts)?);
        let (kind, changes_only, aggregate) = match &rec.aggregate {
            Some(a) => {
                let col = a.col.as_deref().map(|c| ts.column_index(c)).transpose()?;
                (CreateKind::Aggregate { func: a.func, col }, false, Some((a.func, col)))
            }
            None => (CreateKind::Plain, true, None),
        };
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        cmd_tx
            .send(SequencerCmd::BeginShape {
                table: rec.table.clone(),
                shape_id: rec.id.clone(),
                num_id,
                stream_path: rec.stream_path.clone(),
                pred: pred.clone(),
                out_cols: out_cols.clone(),
                kind,
                ack: ack_tx,
            })
            .map_err(|_| anyhow::anyhow!("sequencer is gone"))?;
        backfill_and_activate(
            &self.ds,
            &self.pg_url,
            cmd_tx,
            ts,
            &rec.table,
            &rec.id,
            &rec.stream_path,
            &pred,
            out_cols.as_ref(),
            changes_only,
            aggregate,
            &self.shutdown_token(),
            ack_rx,
        )
        .await
    }
}

/// A minimal, faultable durable-streams stand-in for the catalog writer + retirement queue tests.
///
/// Deliberately an HTTP server rather than a trait double: the behaviour under test is entirely
/// about what the engine does with real statuses (`is_unavailable` reads the response, not a mock's
/// intent), and a double that returns pre-classified errors would test the test.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    /// Wait for a request-scoped FakeDs blocking generation to be released. `Notify` only wakes
    /// waiters; the generation is the durable test-control state, so an early release cannot be
    /// lost and cannot release a request that began in a later generation.
    async fn wait_while_blocked(
        block: &AtomicBool,
        release_generation: &AtomicU64,
        observed_generation: u64,
        released: &tokio::sync::Notify,
    ) {
        loop {
            let notified = released.notified();
            if !block.load(Ordering::SeqCst) || release_generation.load(Ordering::SeqCst) != observed_generation {
                return;
            }
            notified.await;
        }
    }

    #[derive(Default)]
    pub(crate) struct FakeDsState {
        /// Remaining `POST /meta/catalog` calls to answer 503 (transient).
        fail_appends: AtomicU32,
        /// Remaining `POST /meta/catalog` calls to COMMIT and then answer 503 — a response lost
        /// after the write landed, which is what makes the writer's retry append a second copy.
        lose_responses: AtomicU32,
        /// Remaining `DELETE` calls to answer 503.
        fail_deletes: AtomicU32,
        appends: AtomicU64,
        deletes: AtomicU64,
        closes: AtomicU64,
        block_deletes: AtomicBool,
        delete_release_generation: AtomicU64,
        delete_started: tokio::sync::Notify,
        delete_blocked: tokio::sync::Notify,
        pause_delete_after_start: AtomicBool,
        delete_after_start_paused: tokio::sync::Notify,
        continue_delete_after_start: tokio::sync::Notify,
        release_delete: tokio::sync::Notify,
        block_retired: AtomicBool,
        retired_release_generation: AtomicU64,
        retired_started: tokio::sync::Notify,
        release_retired: tokio::sync::Notify,
        /// Remaining `HEAD` calls to answer 503 (transient).
        fail_heads: AtomicU32,
        heads: AtomicU64,
        /// Stream reads. Answered 405 — which the sequencer treats as an error to back off from —
        /// except a catch-up read of a stream given a page with [`FakeDs::serve_page`].
        reads: AtomicU64,
        /// `path -> body`: answered to a non-live read from the start (`offset=-1`) as one page that
        /// is up to date. Live reads still get 405, so a sequencer never spins on it.
        pages: Mutex<std::collections::HashMap<String, String>>,
        /// Answer appends to a stream path with 404 while `HEAD` keeps reporting it present: storage
        /// (or something in front of it) answering one stream two ways.
        false_gone: Mutex<HashSet<String>>,
        /// Streams this storage does not have: `HEAD` answers 404, and so do close and delete (which
        /// the client counts as done, exactly as against the real server).
        missing: Mutex<HashSet<String>>,
        /// Streams that exist but are closed: `HEAD` answers `stream-closed: true`.
        closed: Mutex<HashSet<String>>,
        events: Mutex<Vec<serde_json::Value>>,
    }

    pub(crate) struct FakeDs {
        url: String,
        state: Arc<FakeDsState>,
    }

    impl FakeDs {
        pub(crate) async fn start() -> FakeDs {
            use axum::extract::State;
            use axum::response::IntoResponse;
            let state = Arc::new(FakeDsState::default());
            let app = axum::Router::new()
                .route(
                    "/{*path}",
                    axum::routing::put(|| async { axum::http::StatusCode::OK })
                        .get(
                            |State(st): State<Arc<FakeDsState>>,
                             axum::extract::Path(path): axum::extract::Path<String>,
                             axum::extract::RawQuery(query): axum::extract::RawQuery| async move {
                                st.reads.fetch_add(1, Ordering::SeqCst);
                                let query = query.unwrap_or_default();
                                let catch_up = query.contains("offset=-1") && !query.contains("live=");
                                match st.pages.lock().unwrap().get(&path) {
                                    Some(body) if catch_up => (
                                        axum::http::StatusCode::OK,
                                        [("stream-next-offset", "1"), ("stream-up-to-date", "true")],
                                        body.clone(),
                                    )
                                        .into_response(),
                                    _ => axum::http::StatusCode::METHOD_NOT_ALLOWED.into_response(),
                                }
                            },
                        )
                        .head(
                            |State(st): State<Arc<FakeDsState>>,
                             axum::extract::Path(path): axum::extract::Path<String>| async move {
                                st.heads.fetch_add(1, Ordering::SeqCst);
                                if st
                                    .fail_heads
                                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                                    .is_ok()
                                {
                                    return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
                                }
                                if st.missing.lock().unwrap().contains(&path) {
                                    return axum::http::StatusCode::NOT_FOUND.into_response();
                                }
                                let closed = if st.closed.lock().unwrap().contains(&path) { "true" } else { "false" };
                                (axum::http::StatusCode::OK, [("stream-next-offset", "0"), ("stream-closed", closed)])
                                    .into_response()
                            },
                        )
                        .post(
                            |State(st): State<Arc<FakeDsState>>,
                             axum::extract::Path(path): axum::extract::Path<String>,
                             body: String| async move {
                                if path != "meta/catalog" {
                                    st.closes.fetch_add(1, Ordering::SeqCst);
                                    if st.missing.lock().unwrap().contains(&path)
                                        || st.false_gone.lock().unwrap().contains(&path)
                                    {
                                        return axum::http::StatusCode::NOT_FOUND;
                                    }
                                    return axum::http::StatusCode::NO_CONTENT;
                                }
                                if st
                                    .fail_appends
                                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                                    .is_ok()
                                {
                                    return axum::http::StatusCode::SERVICE_UNAVAILABLE;
                                }
                                let is_retired = serde_json::from_str::<serde_json::Value>(&body)
                                    .ok()
                                    .and_then(|value| value.as_array().cloned())
                                    .is_some_and(|items| items.iter().any(|item| item["t"] == "retired"));
                                if is_retired && st.block_retired.load(Ordering::SeqCst) {
                                    let release_generation = st.retired_release_generation.load(Ordering::SeqCst);
                                    st.retired_started.notify_one();
                                    wait_while_blocked(
                                        &st.block_retired,
                                        &st.retired_release_generation,
                                        release_generation,
                                        &st.release_retired,
                                    )
                                    .await;
                                }
                                st.appends.fetch_add(1, Ordering::SeqCst);
                                if let Ok(serde_json::Value::Array(items)) = serde_json::from_str(&body) {
                                    st.events.lock().unwrap().extend(items);
                                }
                                // Committed, then the answer is lost: the caller cannot tell this
                                // from a write that never happened, and retries.
                                if st
                                    .lose_responses
                                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                                    .is_ok()
                                {
                                    return axum::http::StatusCode::SERVICE_UNAVAILABLE;
                                }
                                axum::http::StatusCode::NO_CONTENT
                            },
                        )
                        .delete(
                            |State(st): State<Arc<FakeDsState>>,
                             axum::extract::Path(path): axum::extract::Path<String>| async move {
                                let release_generation = st.delete_release_generation.load(Ordering::SeqCst);
                                st.deletes.fetch_add(1, Ordering::SeqCst);
                                if st.missing.lock().unwrap().contains(&path) {
                                    return axum::http::StatusCode::NOT_FOUND;
                                }
                                st.delete_started.notify_one();
                                if st.pause_delete_after_start.load(Ordering::SeqCst) {
                                    st.delete_after_start_paused.notify_one();
                                    st.continue_delete_after_start.notified().await;
                                }
                                if st.block_deletes.load(Ordering::SeqCst) {
                                    st.delete_blocked.notify_one();
                                    wait_while_blocked(
                                        &st.block_deletes,
                                        &st.delete_release_generation,
                                        release_generation,
                                        &st.release_delete,
                                    )
                                    .await;
                                }
                                if st
                                    .fail_deletes
                                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                                    .is_ok()
                                {
                                    return axum::http::StatusCode::SERVICE_UNAVAILABLE;
                                }
                                axum::http::StatusCode::NO_CONTENT
                            },
                        ),
                )
                .with_state(state.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            FakeDs { url: format!("http://{addr}"), state }
        }

        pub(crate) fn url(&self) -> &str {
            &self.url
        }

        /// Answer the next `n` catalog appends with 503.
        pub(crate) fn fail_appends(&self, n: u32) {
            self.state.fail_appends.store(n, Ordering::SeqCst);
        }

        /// Commit the next `n` catalog appends and then answer 503 (a lost success response).
        pub(crate) fn lose_responses(&self, n: u32) {
            self.state.lose_responses.store(n, Ordering::SeqCst);
        }

        /// Answer the next `n` stream deletes with 503.
        pub(crate) fn fail_deletes(&self, n: u32) {
            self.state.fail_deletes.store(n, Ordering::SeqCst);
        }

        /// Answer the next `n` stream `HEAD`s with 503.
        pub(crate) fn fail_heads(&self, n: u32) {
            self.state.fail_heads.store(n, Ordering::SeqCst);
        }

        /// This storage does not have `path` (404 to `HEAD`, close and delete).
        pub(crate) fn mark_stream_missing(&self, path: &str) {
            self.state.missing.lock().unwrap().insert(path.to_string());
        }

        /// `path` exists but is closed (`HEAD` reports `stream-closed: true`).
        pub(crate) fn mark_stream_closed(&self, path: &str) {
            self.state.closed.lock().unwrap().insert(path.to_string());
        }

        /// Serve `envelopes` as the one catch-up page of `path` (see `FakeDsState::pages`).
        pub(crate) fn serve_page(&self, path: &str, envelopes: serde_json::Value) {
            self.state.pages.lock().unwrap().insert(path.to_string(), envelopes.to_string());
        }

        /// Answer appends to `path` with 404 while `HEAD` still reports it present and open.
        pub(crate) fn answer_appends_gone(&self, path: &str) {
            self.state.false_gone.lock().unwrap().insert(path.to_string());
        }

        pub(crate) fn heads(&self) -> u64 {
            self.state.heads.load(Ordering::SeqCst)
        }

        pub(crate) fn reads(&self) -> u64 {
            self.state.reads.load(Ordering::SeqCst)
        }

        pub(crate) fn block_deletes(&self, block: bool) {
            self.state.block_deletes.store(block, Ordering::SeqCst);
        }

        pub(crate) async fn wait_delete_started(&self) {
            self.state.delete_started.notified().await;
        }

        /// Wait until a DELETE has observed the closed gate and is about to wait for its release.
        pub(crate) async fn wait_delete_blocked(&self) {
            self.state.delete_blocked.notified().await;
        }

        /// Stop a DELETE just after it has published its arrival, before it registers its blocking
        /// release waiter. This lets lifecycle tests exercise the no-lost-wakeup contract.
        pub(crate) fn pause_delete_after_start(&self, pause: bool) {
            self.state.pause_delete_after_start.store(pause, Ordering::SeqCst);
        }

        pub(crate) async fn wait_delete_after_start_paused(&self) {
            self.state.delete_after_start_paused.notified().await;
        }

        pub(crate) fn continue_delete_after_start(&self) {
            self.state.continue_delete_after_start.notify_one();
        }

        pub(crate) fn release_delete(&self) {
            self.state.delete_release_generation.fetch_add(1, Ordering::SeqCst);
            self.state.release_delete.notify_waiters();
        }

        pub(crate) fn block_retired(&self, block: bool) {
            self.state.block_retired.store(block, Ordering::SeqCst);
        }

        pub(crate) async fn wait_retired_started(&self) {
            self.state.retired_started.notified().await;
        }

        pub(crate) fn release_retired(&self) {
            self.state.retired_release_generation.fetch_add(1, Ordering::SeqCst);
            self.state.release_retired.notify_waiters();
        }

        pub(crate) fn deletes(&self) -> u64 {
            self.state.deletes.load(Ordering::SeqCst)
        }

        pub(crate) fn closes(&self) -> u64 {
            self.state.closes.load(Ordering::SeqCst)
        }

        /// Every catalog event that LANDED, in order.
        pub(crate) fn catalog_events(&self) -> Vec<serde_json::Value> {
            self.state.events.lock().unwrap().clone()
        }

        /// Just the `t` tags of the events that landed — the order-and-once assertions.
        pub(crate) fn catalog_kinds(&self) -> Vec<String> {
            self.catalog_events().iter().map(|e| e["t"].as_str().unwrap_or("?").to_string()).collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use testing::FakeDs;

    fn created_event(table: &str) -> serde_json::Value {
        serde_json::json!({
            "t": "created",
            "rec": {
                "id": "s1",
                "table": table,
                "stream_path": "shape/s1",
                "changes_only": false,
                "where_json": null,
                "columns": null,
                "family_key": null,
                "is_subquery": false,
                "aggregate": null,
                "fingerprint": null,
            },
            "sig": null,
            "subscription": "sub-a",
            "at": 100,
        })
    }

    /// The catalog is strict where API ingress is lenient: a record must already carry the
    /// canonical `schema.name`. A bare one can only be a catalog written before ADR-0002, and
    /// resolving it would leave its (bare-spelled) sharing signature unable to match an identical
    /// post-cutover create — two maintained streams for one table.
    #[test]
    fn catalog_records_must_be_canonically_spelled() {
        let ok =
            serde_json::from_value::<CatalogEvent>(created_event("public.users")).expect("a canonical record restores");
        match ok {
            CatalogEvent::Created { rec, .. } => {
                assert_eq!(rec.table.to_string(), "public.users");
                assert_eq!(rec.table.schema(), "public");
            }
            other => panic!("expected Created, got {other:?}"),
        }

        let err = serde_json::from_value::<CatalogEvent>(created_event("users"))
            .expect_err("a bare record is refused, not resolved to public.users");
        assert!(err.to_string().contains("canonical"), "{err}");

        // The same rule applies to a non-public bare-impossible spelling: `a.b.c` is not a table.
        assert!(serde_json::from_value::<CatalogEvent>(created_event("a.b.c")).is_err());
        // ...and an explicitly qualified non-public table restores untouched.
        let ok = serde_json::from_value::<CatalogEvent>(created_event("other.users")).unwrap();
        match ok {
            CatalogEvent::Created { rec, .. } => assert_eq!(rec.table.to_string(), "other.users"),
            other => panic!("expected Created, got {other:?}"),
        }
    }

    fn bound(slot: &str, sysid: &str, at: &str) -> CatalogEvent {
        CatalogEvent::SlotBound(crate::engine::epoch::SlotBinding {
            system_identifier: sysid.to_string(),
            timeline_id: 1,
            slot: slot.to_string(),
            bound_at: at.to_string(),
        })
    }

    /// Fold a sequence of events, giving each its own `eid` — the shape the writer produces when
    /// nothing was retried. [`fold_of_with_eids`] is the one that repeats them.
    fn fold_of(events: Vec<CatalogEvent>) -> CatalogFold {
        fold_of_with_eids(events.into_iter().enumerate().map(|(i, ev)| (format!("e{i}"), ev)).collect())
    }

    fn fold_of_with_eids(events: Vec<(String, CatalogEvent)>) -> CatalogFold {
        let mut fold = CatalogFold::default();
        for (eid, ev) in events {
            fold.apply_once(&eid, ev);
        }
        fold
    }

    /// The three shape-lifecycle events, with the subscription and lease timestamp ADR-0008 gives
    /// them — spelled out here so the fold tests read as "who is subscribed", not as bookkeeping.
    fn joined(id: &str, sub: &str, at: u64) -> CatalogEvent {
        CatalogEvent::Joined { id: id.to_string(), subscription: sub.to_string(), at }
    }

    fn left(id: &str, sub: &str) -> CatalogEvent {
        CatalogEvent::Left { id: id.to_string(), subscription: sub.to_string(), lapsed: false }
    }

    fn subs_of(fold: &CatalogFold, id: &str) -> Vec<String> {
        fold.recs.get(id).map(|r| r.2.keys().cloned().collect()).unwrap_or_default()
    }

    fn pos(segment: u32, offset: &str) -> LogPosition {
        LogPosition { segment, offset: offset.to_string() }
    }

    /// A catalog with no `SlotBound` anywhere is a genuine first boot — the state that licenses the
    /// engine to create a slot from nothing (ADR-0004).
    #[test]
    fn a_catalog_without_a_binding_folds_to_no_epoch() {
        let fold = fold_of(vec![CatalogEvent::Offset { pos: pos(0, "42"), highwater: None }]);
        assert!(fold.binding.is_none());
        assert_eq!(fold.start_pos, pos(0, "42"));
        // Nothing at all folds to the empty catalog, which the restore skips outright.
        assert!(CatalogFold::default().is_empty());
    }

    /// The de-duplication highwater is checkpointed WITH the position (ADR-0003), and the fold's
    /// last `Offset` wins for both together.
    ///
    /// It has to travel with the position because a commit too large for one request body is
    /// appended in several chunks: a crash can leave a prefix of a transaction applied and
    /// checkpointed while the rest is still to be re-delivered. Restoring the position without the
    /// highwater would re-apply that prefix — and aggregate/subquery contributor weights are not
    /// idempotent under duplicates.
    #[test]
    fn the_dedup_highwater_is_restored_with_the_checkpoint_it_was_taken_at() {
        let fold = fold_of(vec![
            CatalogEvent::Offset { pos: pos(0, "10"), highwater: Some((0x10, 3)) },
            CatalogEvent::Offset { pos: pos(0, "20"), highwater: Some((0x20, 7)) },
        ]);
        assert_eq!(fold.start_pos, pos(0, "20"));
        assert_eq!(fold.start_highwater, Some((0x20, 7)), "the last checkpoint's highwater, not the first");

        // A checkpoint taken before anything was applied carries none, and CLEARS an older one:
        // last-writer-wins for the pair, never a mix of a new position and a stale highwater.
        let fold = fold_of(vec![
            CatalogEvent::Offset { pos: pos(0, "20"), highwater: Some((0x20, 7)) },
            CatalogEvent::Offset { pos: pos(1, "0"), highwater: None },
        ]);
        assert_eq!(fold.start_pos, pos(1, "0"));
        assert_eq!(fold.start_highwater, None);

        // Nothing at all: no highwater, so a first boot de-duplicates from scratch.
        assert_eq!(CatalogFold::default().start_highwater, None);
    }

    /// The wire form: the highwater is omitted when absent (so a checkpoint that has applied
    /// nothing is the same bytes it always was) and round-trips when present.
    #[test]
    fn the_checkpoint_wire_form_carries_the_highwater_only_when_there_is_one() {
        let bare = serde_json::to_value(CatalogEvent::Offset { pos: pos(2, "9"), highwater: None }).unwrap();
        assert_eq!(bare.get("highwater"), None, "{bare}");
        let with = serde_json::to_value(CatalogEvent::Offset { pos: pos(2, "9"), highwater: Some((5, 6)) }).unwrap();
        assert_eq!(with["highwater"], serde_json::json!([5, 6]));
        match serde_json::from_value::<CatalogEvent>(with).unwrap() {
            CatalogEvent::Offset { pos: p, highwater } => {
                assert_eq!(p, pos(2, "9"));
                assert_eq!(highwater, Some((5, 6)));
            }
            other => panic!("expected Offset, got {other:?}"),
        }
    }

    /// The change log's segmentation folds out of the same log (ADR-0006): the last rotation is the
    /// current segment, and EVERY rotation is kept — segment `n` was closed when `n+1` began, which
    /// is the clock the retain window that governs deletion runs on.
    #[test]
    fn rotations_fold_to_the_current_segment_and_every_segments_start() {
        let fold = fold_of(vec![
            CatalogEvent::ChangesRotated { segment: 0, at: 100 },
            CatalogEvent::Offset { pos: pos(0, "9"), highwater: None },
            CatalogEvent::ChangesRotated { segment: 1, at: 200 },
            CatalogEvent::ChangesRotated { segment: 2, at: 300 },
        ]);
        assert_eq!(fold.current_segment, 2);
        assert_eq!(fold.segment_starts.get(&0), Some(&100));
        assert_eq!(fold.segment_starts.get(&1), Some(&200));
        assert_eq!(fold.segment_starts.get(&2), Some(&300));
        // A catalog that has only ever rotated has nothing to restore.
        assert_eq!(fold.start_pos, pos(0, "9"));

        // Nothing rotated = segment 0, which is also what a first boot creates.
        assert_eq!(CatalogFold::default().current_segment, 0);
    }

    /// A deleted segment leaves the folded set, so a restart neither re-plans retiring streams that
    /// are long gone nor reports a `changes_segments_retained` gauge counting them. It never moves
    /// the current segment — the current one is never deleted.
    #[test]
    fn deleted_segments_leave_the_fold() {
        let fold = fold_of(vec![
            CatalogEvent::ChangesRotated { segment: 0, at: 100 },
            CatalogEvent::ChangesRotated { segment: 1, at: 200 },
            CatalogEvent::ChangesRotated { segment: 2, at: 300 },
            CatalogEvent::ChangesSegmentDeleted { segment: 0 },
        ]);
        assert_eq!(fold.current_segment, 2);
        assert_eq!(fold.segment_starts.keys().copied().collect::<Vec<_>>(), vec![1, 2]);

        let json = serde_json::to_value(CatalogEvent::ChangesSegmentDeleted { segment: 7 }).unwrap();
        assert_eq!(json["t"], "changesSegmentDeleted");
        assert_eq!(json["segment"], 7);
    }

    /// The sequencer's checkpoint and a dormant shape's resume state carry the SEGMENT, so a
    /// restart after a rotation resumes in the right stream rather than at a byte position in a
    /// stream that no longer holds those bytes.
    #[test]
    fn checkpoints_and_dormant_resumes_carry_their_segment() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let gate = crate::pg::SnapshotGate::passthrough();
        let fold = fold_of(vec![
            created,
            CatalogEvent::Dormant { id: "s1".to_string(), resume: pos(3, "77"), gate },
            CatalogEvent::Offset { pos: pos(4, "12"), highwater: None },
        ]);
        assert_eq!(fold.start_pos, pos(4, "12"));
        assert_eq!(fold.recs.get("s1").and_then(|r| r.3.as_ref()).map(|(p, _)| p.clone()), Some(pos(3, "77")));

        // Wire form: an object, never a bare string.
        let json = serde_json::to_value(CatalogEvent::Offset { pos: pos(4, "12"), highwater: None }).unwrap();
        assert_eq!(json["t"], "offset");
        assert_eq!(json["pos"]["segment"], 4);
        assert_eq!(json["pos"]["offset"], "12");
    }

    /// A catalog written before ADR-0006 carries bare change-log offsets, which cannot be repaired:
    /// the byte position belongs to a stream that no longer exists under that name. It is named and
    /// refused at boot, never coerced to "segment 0" (which would resume at an unrelated point) and
    /// never silently skipped by the strict deserializer (which for an `Offset` would restart the
    /// sequencer from the beginning of the log).
    #[test]
    fn a_pre_segmentation_catalog_is_refused_by_name() {
        let bare_offset = serde_json::json!({ "t": "offset", "offset": "0000000000000000_0000000000000042" });
        let detail = predates_segmentation(&bare_offset).expect("a bare offset is recognised");
        assert!(detail.contains("bare change-log offset"), "{detail}");
        assert!(
            CatalogPredatesSegmentation { detail }.to_string().contains("predates ADR-0006"),
            "the operator is told which cutover the catalog is on the wrong side of"
        );

        let bare_dormant = serde_json::json!({
            "t": "dormant",
            "id": "s7",
            "resume_offset": "0000000000000000_0000000000000042",
            "gate": {},
        });
        let detail = predates_segmentation(&bare_dormant).expect("a bare dormant resume is recognised");
        assert!(detail.contains("s7"), "the refusal names the shape: {detail}");

        // The current spellings are not mistaken for the old ones.
        let now = serde_json::to_value(CatalogEvent::Offset { pos: pos(2, "9"), highwater: None }).unwrap();
        assert!(predates_segmentation(&now).is_none());
        let gate = crate::pg::SnapshotGate::passthrough();
        let now =
            serde_json::to_value(CatalogEvent::Dormant { id: "s7".to_string(), resume: pos(2, "9"), gate }).unwrap();
        assert!(predates_segmentation(&now).is_none());
        // ...and neither is an unrelated event.
        assert!(predates_segmentation(&created_event("public.users")).is_none());
    }

    /// The LAST binding is the epoch in force: a reset appends its new one after the `Dropped`
    /// records of the epoch it ended, so the fold must not stop at the first.
    #[test]
    fn the_last_slot_bound_wins() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![
            bound("s", "7300000000000000001", "2026-08-20T09:00:00.000Z"),
            created,
            CatalogEvent::Dropped { id: "s1".to_string() },
            bound("s", "7300000000000000001", "2026-08-21T11:30:00.000Z"),
        ]);
        let b = fold.binding.expect("the epoch is folded out of the log");
        assert_eq!(b.bound_at, "2026-08-21T11:30:00.000Z", "the newest binding is the epoch in force");
        assert!(fold.recs.is_empty(), "the epoch it ended took its shapes with it");
    }

    /// The binding survives everything else in the log — dropping every shape does not drop the
    /// epoch, and the epoch does not disturb the shapes.
    #[test]
    fn shapes_and_the_binding_fold_independently() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![
            bound("s", "7300000000000000001", "2026-08-21T11:30:00.000Z"),
            created,
            joined("s1", "sub-b", 100),
        ]);
        assert_eq!(
            subs_of(&fold, "s1"),
            vec!["sub-a".to_string(), "sub-b".to_string()],
            "create + join = two subscriptions"
        );
        assert_eq!(fold.binding.map(|b| b.slot), Some("s".to_string()));
    }

    /// The event is stored (and restored) as its own `t` case, alongside the shape lifecycle — a
    /// catalog is one log, and its epoch is part of it.
    #[test]
    fn slot_bound_round_trips_on_the_wire() {
        let json = serde_json::to_value(bound("electric_circuits", "73", "2026-08-21T11:30:00.000Z")).unwrap();
        assert_eq!(json["t"], "slotBound");
        assert_eq!(json["slot"], "electric_circuits");
        assert_eq!(json["system_identifier"], "73");
        assert_eq!(json["timeline_id"], 1);
        assert_eq!(json["bound_at"], "2026-08-21T11:30:00.000Z");
        match serde_json::from_value::<CatalogEvent>(json).unwrap() {
            CatalogEvent::SlotBound(b) => assert_eq!(b.system_identifier, "73"),
            other => panic!("expected SlotBound, got {other:?}"),
        }
    }

    // --- the writer never drops an event -------------------------------------------------------

    /// The classification the whole retry-or-die decision hangs on, without exiting anything: a 5xx
    /// or a dead transport is storage having a moment; a 4xx is an answer.
    #[test]
    fn a_5xx_retries_and_a_4xx_refuses() {
        let five =
            anyhow::Error::new(crate::ds::DsUnavailable { op: "POST", path: CATALOG_STREAM.to_string(), status: 503 });
        assert_eq!(classify_append(&five), AppendVerdict::Retry);
        let four = anyhow::anyhow!("POST meta/catalog -> 400 Bad Request: malformed event");
        assert_eq!(classify_append(&four), AppendVerdict::Refused);
        let serialization = anyhow::anyhow!("catalog event could not be serialized");
        assert_eq!(classify_append(&serialization), AppendVerdict::Refused);
    }

    #[test]
    fn the_catalog_schedule_climbs_to_five_seconds_and_stays() {
        let ms = |a: u32| catalog_backoff(a).as_millis();
        assert_eq!(ms(1), 100, "the first retry is immediate-ish: a create is waiting on it");
        assert_eq!(ms(2), 200);
        assert_eq!(ms(6), 3200);
        assert_eq!(ms(7), 5000, "capped");
        assert_eq!(ms(70), 5000, "and stays capped");
    }

    /// The regression this exists for: a 503 on a catalog append used to log "event lost" and drop
    /// the event, so an acknowledged shape simply was not in the log a restart folds.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refused_append_is_retried_in_place_and_lands_exactly_once() {
        let server = FakeDs::start().await;
        server.fail_appends(3);
        let w = spawn_catalog_writer(DsClient::new(server.url()), crate::shutdown::ShutdownToken::new());
        w.send(joined("s1", "sub-a", 100));
        w.send(left("s1", "sub-a"));
        assert!(w.drain(std::time::Duration::from_secs(20)).await, "the queue must drain");
        assert_eq!(
            server.catalog_kinds(),
            vec!["joined".to_string(), "left".to_string()],
            "both events land, exactly once each, in send order"
        );
    }

    /// What [`WriterExit`] relies on: a guard held across an await in a spawned task is dropped while
    /// the task's panic is still unwinding — so it sees `panicking()` and exits before tokio swallows
    /// the panic into the join handle.
    #[tokio::test]
    async fn a_task_guard_is_dropped_during_the_panic_unwind() {
        struct Probe(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0.store(std::thread::panicking(), Ordering::SeqCst);
            }
        }
        let saw_panicking = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = saw_panicking.clone();
        let task = tokio::spawn(async move {
            let _guard = Probe(probe);
            tokio::task::yield_now().await;
            panic!("writer died");
        });
        assert!(task.await.unwrap_err().is_panic());
        assert!(saw_panicking.load(Ordering::SeqCst), "the guard must observe the unwind");
    }

    /// `send_durable` is the durable-before-ack primitive: the future must not resolve while the
    /// append is still being refused.
    #[tokio::test(flavor = "multi_thread")]
    async fn send_durable_resolves_only_after_the_append_lands() {
        let server = FakeDs::start().await;
        server.fail_appends(2);
        let w = spawn_catalog_writer(DsClient::new(server.url()), crate::shutdown::ShutdownToken::new());
        let landed = w.send_durable(joined("s1", "sub-a", 100));
        tokio::pin!(landed);
        // 100 ms + 200 ms of backoff to go, so the ack cannot have happened yet.
        tokio::select! {
            _ = &mut landed => panic!("acknowledged a create whose record storage had refused"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
        }
        assert!(server.catalog_kinds().is_empty(), "nothing has landed yet");
        tokio::time::timeout(std::time::Duration::from_secs(20), landed)
            .await
            .expect("the append lands once storage recovers");
        assert_eq!(server.catalog_kinds(), vec!["joined".to_string()]);
    }

    // --- retirement: intent and completion ------------------------------------------------------

    /// A `Dropped` with no `Retired` is the durable form of "this stream must still go".
    #[test]
    fn a_drop_without_a_retirement_stays_pending() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![created, CatalogEvent::Dropped { id: "s1".to_string() }]);
        assert!(fold.recs.is_empty(), "the record is gone the moment the drop is recorded");
        assert_eq!(fold.pending_retirements(), vec![("s1".to_string(), "shape/s1".to_string())]);
    }

    /// ...and the completion clears it, so an ordinary retirement leaves the boot nothing to do.
    #[test]
    fn a_completed_retirement_leaves_nothing_pending() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![
            created,
            CatalogEvent::Dropped { id: "s1".to_string() },
            CatalogEvent::Retired { id: "s1".to_string() },
        ]);
        assert!(fold.pending_retirements().is_empty());
    }

    /// A `Dropped` for a record this fold never saw (a duplicate, or a drop from before whatever
    /// the log still holds) has no stream path to retire and must not invent one.
    #[test]
    fn a_drop_with_no_record_is_not_a_pending_retirement() {
        let fold = fold_of(vec![CatalogEvent::Dropped { id: "s404".to_string() }]);
        assert!(fold.pending_retirements().is_empty());
    }

    /// A shape id is spoken for by its `shape/*` stream, which outlives the record whenever the
    /// retirement has not landed. The fold's high-water mark therefore comes from every `Created`,
    /// not from the survivors.
    #[test]
    fn the_id_high_water_mark_survives_the_drop() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![created, CatalogEvent::Dropped { id: "s1".to_string() }]);
        assert!(fold.recs.is_empty());
        assert_eq!(fold.max_shape_id, Some(1), "s1 is still spoken for: its stream is being retired");
    }

    /// ...and it is a MAXIMUM, so nothing in the log can lower it (the engine mints monotonically,
    /// so this is belt-and-braces rather than a case that occurs).
    #[test]
    fn the_id_high_water_mark_never_goes_backwards() {
        let at = |id: &str| {
            let mut ev = created_event("public.users");
            ev["rec"]["id"] = serde_json::json!(id);
            ev["rec"]["stream_path"] = serde_json::json!(format!("shape/{id}"));
            serde_json::from_value::<CatalogEvent>(ev).unwrap()
        };
        let fold = fold_of(vec![at("s3"), CatalogEvent::Dropped { id: "s3".to_string() }, at("s1")]);
        assert_eq!(fold.max_shape_id, Some(3));
    }

    /// The end-to-end property the fold exists for: a restart must not re-mint an id whose stream is
    /// still being retired. `Created s1` + `Dropped s1` folds to `is_empty()`, so this is also the
    /// case that proves the counter is restored BEFORE that early return.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_boot_over_a_dropped_shape_mints_the_next_id_not_that_one() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![created, CatalogEvent::Dropped { id: "s1".to_string() }]);
        assert!(fold.is_empty(), "nothing to install — only an id that must not come back");
        // No durable-streams behind it: the queued retirement retries in the background and this
        // test does not depend on it (the id counter is engine state, decided before any IO).
        let engine = Engine::new(DsClient::new("http://127.0.0.1:1"));
        engine.apply_catalog(fold, &HashMap::new(), RestoreMode::Resume).await.unwrap();
        assert_eq!(engine.state.lock().await.next_shape_id, 2, "s1 is taken; the next shape is s2");
    }

    /// The same over a broken epoch: `Park` restores records to be retired, and it must not hand the
    /// reset a fresh create colliding with one of them either.
    #[tokio::test(flavor = "multi_thread")]
    async fn parking_a_broken_epoch_also_moves_the_id_counter() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![created, CatalogEvent::Offset { pos: pos(0, "10"), highwater: None }]);
        let engine = Engine::new(DsClient::new("http://127.0.0.1:1"));
        engine.apply_catalog(fold, &HashMap::new(), RestoreMode::Park).await.unwrap();
        let st = engine.state.lock().await;
        assert_eq!(st.next_shape_id, 2);
        assert_eq!(st.shapes.len(), 1, "the record is parked for the reset to retire");
    }

    // --- subscriptions: identity, idempotence, leases (ADR-0008) --------------------------------

    /// The whole of the fold's subscription semantics: a create opens the set, a join adds to it, a
    /// REPEATED join is a renewal (one member, later lease), and a `Left` removes exactly one id —
    /// a second one for the same id changing nothing.
    #[test]
    fn subscriptions_fold_as_a_set_not_a_count() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![
            created,
            joined("s1", "sub-b", 200),
            joined("s1", "sub-b", 300), // a renewal, not a second subscriber
            joined("s1", "sub-c", 400),
            left("s1", "sub-b"),
            left("s1", "sub-b"),   // the client retried its DELETE; nothing more to release
            left("s1", "sub-zzz"), // never held here at all
        ]);
        assert_eq!(subs_of(&fold, "s1"), vec!["sub-a".to_string(), "sub-c".to_string()]);

        // The renewal moved the lease, which is the only thing a repeated join may do.
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of(vec![created, joined("s1", "sub-a", 900)]);
        assert_eq!(fold.recs["s1"].2.get("sub-a"), Some(&900), "the lease is restored at its LAST renewal");
    }

    /// A lapse is recorded as an ordinary `Left` with a marker: the fold treats it identically (the
    /// subscription is released either way), and the flag is there so the durable record explains a
    /// subscription that nobody deleted.
    #[test]
    fn a_lapsed_lease_folds_exactly_like_an_explicit_release() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let lapse = CatalogEvent::Left { id: "s1".to_string(), subscription: "sub-a".to_string(), lapsed: true };
        let json = serde_json::to_value(&lapse).unwrap();
        assert_eq!(json["lapsed"], true);
        assert_eq!(json["subscription"], "sub-a");
        // An ordinary release does not carry the flag at all (the wire form stays minimal).
        assert_eq!(serde_json::to_value(left("s1", "sub-a")).unwrap().get("lapsed"), None);

        let fold = fold_of(vec![created, lapse]);
        assert!(subs_of(&fold, "s1").is_empty(), "a lapsed subscription is released like any other");
        assert!(fold.recs.contains_key("s1"), "...and the shape itself stays: retention decides its fate");
    }

    /// The regression the `eid` exists for: the SAME event appended twice (a retry after a lost
    /// response) must be applied once. Without it, the second copy of a `Left` releases a
    /// subscription the caller never released — the double-decrement that stole another
    /// subscriber's claim.
    #[test]
    fn a_repeated_append_of_one_event_is_applied_once() {
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of_with_eids(vec![
            ("e0".to_string(), created),
            ("e1".to_string(), joined("s1", "sub-b", 200)),
            ("e1".to_string(), joined("s1", "sub-b", 200)), // the writer's retry-in-place
        ]);
        assert_eq!(subs_of(&fold, "s1"), vec!["sub-a".to_string(), "sub-b".to_string()]);

        // ...and the case that used to corrupt the count: one release, appended twice.
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let fold = fold_of_with_eids(vec![
            ("e0".to_string(), created),
            ("e1".to_string(), joined("s1", "sub-b", 200)),
            ("e2".to_string(), left("s1", "sub-a")),
            ("e2".to_string(), left("s1", "sub-a")),
        ]);
        assert_eq!(subs_of(&fold, "s1"), vec!["sub-b".to_string()], "one release is one release");
    }

    /// A catalog written before ADR-0008 cannot be folded — the fold could neither de-duplicate a
    /// retried append nor rebuild the live set — so the boot refuses it by name rather than
    /// silently restoring something else.
    #[test]
    fn a_pre_subscription_catalog_is_refused_by_name() {
        let no_eid = serde_json::json!({ "t": "joined", "id": "s1", "subscription": "sub-a", "at": 1 });
        let detail = predates_subscriptions(&no_eid).expect("an event with no eid is recognised");
        assert!(detail.contains("no eid"), "{detail}");
        assert!(
            CatalogPredatesSubscriptions { detail }.to_string().contains("predates ADR-0008"),
            "the operator is told which cutover the catalog is on the wrong side of"
        );

        let no_sub = serde_json::json!({ "t": "left", "id": "s1", "eid": "e1" });
        assert!(predates_subscriptions(&no_sub).unwrap().contains("no subscription"));
        let no_at = serde_json::json!({ "t": "joined", "id": "s1", "subscription": "sub-a", "eid": "e1" });
        assert!(predates_subscriptions(&no_at).unwrap().contains("no lease timestamp"));

        // The current spellings pass, and an event that has no subscription of its own (an offset,
        // a rotation) needs only its eid.
        let now = serde_json::json!({ "t": "joined", "id": "s1", "subscription": "sub-a", "at": 1, "eid": "e1" });
        assert!(predates_subscriptions(&now).is_none());
        let offset = serde_json::json!({ "t": "offset", "pos": { "segment": 0, "offset": "1" }, "eid": "e2" });
        assert!(predates_subscriptions(&offset).is_none());
    }

    /// End to end through the real writer and a real storage response: an append that COMMITS and
    /// then loses its response is retried, so the log holds the event twice — with one `eid`, which
    /// is what makes the fold apply it once.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_lost_response_leaves_two_records_with_one_event_id() {
        let server = FakeDs::start().await;
        let w = spawn_catalog_writer(DsClient::new(server.url()), crate::shutdown::ShutdownToken::new());
        server.lose_responses(1);
        w.send(joined("s1", "sub-b", 200));
        assert!(w.drain(std::time::Duration::from_secs(20)).await, "the queue must drain");

        let events = server.catalog_events();
        assert_eq!(events.len(), 2, "the retry appended a second copy: {events:?}");
        let eids: Vec<&str> = events.iter().map(|e| e["eid"].as_str().unwrap_or("")).collect();
        assert_eq!(eids[0], eids[1], "both copies are the same event, and say so");
        assert!(!eids[0].is_empty(), "every event carries an eid");

        // Fold them exactly as a boot would: one join.
        let created = serde_json::from_value::<CatalogEvent>(created_event("public.users")).unwrap();
        let mut fold = fold_of(vec![created]);
        for ev in events {
            let eid = ev["eid"].as_str().unwrap().to_string();
            fold.apply_once(&eid, serde_json::from_value::<CatalogEvent>(ev).unwrap());
        }
        assert_eq!(subs_of(&fold, "s1"), vec!["sub-a".to_string(), "sub-b".to_string()]);
    }

    /// Two DIFFERENT events must never share an id (the dedup is per append attempt, not per
    /// payload): two identical-looking joins from two callers are two claims.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_enqueued_event_gets_its_own_id() {
        let server = FakeDs::start().await;
        let w = spawn_catalog_writer(DsClient::new(server.url()), crate::shutdown::ShutdownToken::new());
        w.send(joined("s1", "sub-b", 200));
        w.send(joined("s1", "sub-c", 200));
        assert!(w.drain(std::time::Duration::from_secs(20)).await);
        let events = server.catalog_events();
        assert_ne!(events[0]["eid"], events[1]["eid"]);
    }

    /// The restore reads the live set back out of the log, with its lease ages — the two halves the
    /// engine needs to answer "who is subscribed, and when did they last say so".
    #[tokio::test(flavor = "multi_thread")]
    async fn a_restored_shape_keeps_its_subscriptions_and_their_lease_ages() {
        let mut created = created_event("public.users");
        created["sig"] = serde_json::json!("sig-1");
        created["at"] = serde_json::json!(1000);
        let fold = fold_of(vec![
            serde_json::from_value::<CatalogEvent>(created).unwrap(),
            joined("s1", "sub-b", 2000),
            // Restored as DORMANT, which is the one restore path that installs the shape without
            // replaying it — the point here is the live set, not the resume.
            CatalogEvent::Dormant {
                id: "s1".to_string(),
                resume: pos(0, "5"),
                gate: crate::pg::SnapshotGate::passthrough(),
            },
            CatalogEvent::Offset { pos: pos(0, "10"), highwater: None },
        ]);
        // A storage server behind it: the restore vouches for the stream before installing it.
        let server = FakeDs::start().await;
        let engine = Engine::new(DsClient::new(server.url()));
        engine.apply_catalog(fold, &users_compiled(), RestoreMode::Resume).await.unwrap();
        let st = engine.state.lock().await;
        let share = st.feed_shares.get("s1").expect("the restored share entry");
        assert_eq!(share.subs.get("sub-a"), Some(&1000), "the creator's lease age survives the restart");
        assert_eq!(share.subs.get("sub-b"), Some(&2000));
        assert_eq!(share.refcount(), 2);
        assert_eq!(st.subscription_owner("sub-b"), Some(&"s1".to_string()), "the id index is restored too");
    }

    // --- restore: fail closed, retire only the definitively gone (ADR-0009) ----------------------

    /// `public.users`, compiled the way library mode compiles it — no fingerprint, so these tests
    /// exercise the table, stream and resume halves of the restore rather than the schema check.
    fn users_compiled() -> HashMap<TableRef, TableSchema> {
        let schema: Schema = serde_json::from_value(serde_json::json!({
            "tables": { "users": { "columns": { "id": {"type":"int"}, "name": {"type":"text"} }, "primaryKey": "id" } }
        }))
        .unwrap();
        compile_schema(&schema).unwrap()
    }

    fn users() -> TableRef {
        TableRef::parse("public.users").unwrap()
    }

    /// An engine whose schema holders both carry `public.users`, as `setup_postgres` leaves them
    /// before the restore — the sequencer resolves a restored shape's table through them.
    async fn restoring(engine: Engine) -> Engine {
        *engine.tables_shared.write().unwrap() = users_compiled();
        engine.state.lock().await.tables = users_compiled();
        engine
    }

    /// A plain shape `id` on `table` with its own sharing signature, created by `sub-<id>`.
    fn shape(id: &str, table: &str) -> CatalogEvent {
        let mut ev = created_event(table);
        ev["rec"]["id"] = serde_json::json!(id);
        ev["rec"]["stream_path"] = serde_json::json!(format!("shape/{id}"));
        ev["sig"] = serde_json::json!(format!("sig-{id}"));
        ev["subscription"] = serde_json::json!(format!("sub-{id}"));
        serde_json::from_value(ev).unwrap()
    }

    /// A `COUNT(*)` aggregate `id` on `table` — the shape kind whose resume re-seeds from Postgres.
    fn count_of(id: &str, table: &str) -> CatalogEvent {
        let mut ev = serde_json::to_value(shape(id, table)).unwrap();
        ev["rec"]["aggregate"] = serde_json::json!({ "func": "count", "col": null });
        serde_json::from_value(ev).unwrap()
    }

    fn dormant(id: &str) -> CatalogEvent {
        CatalogEvent::Dormant { id: id.to_string(), resume: pos(0, "5"), gate: crate::pg::SnapshotGate::passthrough() }
    }

    /// A catalog over `events` that has also checkpointed, so it has something to install.
    fn catalog(mut events: Vec<CatalogEvent>) -> CatalogFold {
        events.push(CatalogEvent::Offset { pos: pos(0, "10"), highwater: None });
        fold_of(events)
    }

    /// The catalog events that landed for one shape, by kind, in order.
    fn kinds_for(server: &FakeDs, id: &str) -> Vec<String> {
        server
            .catalog_events()
            .iter()
            .filter(|e| e["id"] == id || e["rec"]["id"] == id)
            .map(|e| e["t"].as_str().unwrap_or("?").to_string())
            .collect()
    }

    fn shape_ids(st: &EngineState) -> Vec<String> {
        let mut ids: Vec<String> = st.shapes.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// What a rolled-back (or never-started) restore must leave: no trace of any record.
    async fn assert_nothing_installed(engine: &Engine) {
        let st = engine.state.lock().await;
        assert!(st.shapes.is_empty(), "records: {:?}", shape_ids(&st));
        assert!(st.feed_shares.is_empty() && st.feed_by_sig.is_empty(), "shares survived the rollback");
        assert!(st.subs_by_id.is_empty(), "subscriptions survived the rollback: {:?}", st.subs_by_id);
        assert!(st.circuit_placement.is_empty(), "circuit placements survived the rollback");
        assert!(st.sequencer.is_none(), "the restore's sequencer survived the rollback");
        drop(st);
        assert!(engine.lives.lock().unwrap().is_empty(), "lifecycles survived the rollback");
    }

    /// A stream storage no longer has is a definitive answer about THAT shape: it is retired —
    /// `Dropped`, close-then-delete, `Retired` — and every other record restores around it, with its
    /// share, its subscriptions and its routing. Nothing recreates the missing stream.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_retires_a_shape_whose_stream_is_missing_and_restores_the_others() {
        use crate::metrics::RestoreRetireReason;
        let server = FakeDs::start().await;
        server.mark_stream_missing("shape/s2");
        let counter = &metrics().catalog_restore_retired[RestoreRetireReason::StreamMissing as usize];
        let before = counter.load(Ordering::SeqCst);
        let engine = restoring(Engine::new(DsClient::new(server.url()))).await;
        let fold = catalog(vec![
            shape("s1", "public.users"),
            shape("s2", "public.users"),
            shape("s3", "public.users"),
            dormant("s3"),
        ]);
        engine
            .apply_catalog(fold, &users_compiled(), RestoreMode::Resume)
            .await
            .expect("a missing stream retires its shape; it does not fail the restore");

        let st = engine.state.lock().await;
        assert_eq!(shape_ids(&st), ["s1", "s3"]);
        assert!(st.feed_shares.contains_key("s1") && st.feed_shares.contains_key("s3"));
        assert_eq!(st.subscription_owner("sub-s1"), Some(&"s1".to_string()));
        assert_eq!(st.subscription_owner("sub-s2"), None, "the retired shape's subscription is not restored");
        drop(st);
        assert!(engine.table_stats(&users()).await.is_some(), "the active survivor is registered with the sequencer");

        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        assert_eq!(kinds_for(&server, "s2"), ["dropped", "retired"], "intent, then completion");
        assert!(kinds_for(&server, "s1").is_empty() && kinds_for(&server, "s3").is_empty());
        assert_eq!(server.deletes(), 1, "exactly the missing stream takes the (404-tolerant) delete");
        assert!(counter.load(Ordering::SeqCst) > before, "the retirement is counted by reason");
    }

    /// A CLOSED stream is the same definitive answer: nothing can ever be appended to it again.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_retires_a_shape_whose_stream_is_closed_and_restores_the_others() {
        let server = FakeDs::start().await;
        server.mark_stream_closed("shape/s2");
        let engine = restoring(Engine::new(DsClient::new(server.url()))).await;
        let fold = catalog(vec![shape("s1", "public.users"), shape("s2", "public.users")]);
        engine.apply_catalog(fold, &users_compiled(), RestoreMode::Resume).await.expect("the closed one is retired");

        assert_eq!(shape_ids(&*engine.state.lock().await), ["s1"]);
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        assert_eq!(kinds_for(&server, "s2"), ["dropped", "retired"]);
        assert_eq!(server.deletes(), 1);
    }

    /// A record whose TABLE left the compiled set — dropped while the engine was down, or no longer
    /// selected — is ADR-0005's per-table retirement seen from the boot. Left to the resume it would
    /// fail on the missing schema on every boot and take every healthy shape down with it, so it is
    /// decided first, before its stream is even asked about.
    #[tokio::test(flavor = "multi_thread")]
    async fn restore_retires_a_shape_whose_table_is_gone_and_restores_the_others() {
        let server = FakeDs::start().await;
        let engine = restoring(Engine::new(DsClient::new(server.url()))).await;
        let fold = catalog(vec![shape("s1", "public.users"), shape("s2", "public.gone")]);
        engine
            .apply_catalog(fold, &users_compiled(), RestoreMode::Resume)
            .await
            .expect("a gone table retires its shapes, never the whole restore");

        assert_eq!(shape_ids(&*engine.state.lock().await), ["s1"]);
        assert!(engine.table_stats(&users()).await.is_some(), "the other table's shape resumed");
        assert_eq!(server.heads(), 1, "only the resumable record's stream is checked");
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        assert_eq!(kinds_for(&server, "s2"), ["dropped", "retired"]);
        assert_eq!(server.deletes(), 1);
    }

    /// A `HEAD` storage cannot answer for its whole retry budget is no answer: the restore stops
    /// before installing anything — not even the retirement the gone table already earned is
    /// recorded — and the error reaches the boot as the retryable thing it is. The retry, over the
    /// same catalog, starts clean.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_head_that_stays_unanswered_fails_the_restore_with_nothing_installed() {
        let server = FakeDs::start().await;
        let engine = restoring(Engine::new(DsClient::new(server.url()))).await;
        let events = || {
            catalog(vec![
                shape("s1", "public.users"),
                shape("s2", "public.users"),
                dormant("s2"),
                shape("s3", "public.gone"),
            ])
        };
        server.fail_heads(u32::MAX);
        let err = engine
            .apply_catalog(events(), &users_compiled(), RestoreMode::Resume)
            .await
            .expect_err("an unanswered HEAD must fail the restore, not be read as 'missing'");
        // Both checks fail; the lowest-ordered one is reported, however the two interleaved.
        assert!(format!("{err:#}").contains("shape s1's stream"), "the error names the first shape: {err:#}");
        assert!(crate::ds::is_unavailable(&err), "{err:#}");
        assert_eq!(crate::pg::boot_disposition(&err), crate::pg::BootFailure::Retryable);
        assert_nothing_installed(&engine).await;
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        assert!(server.catalog_events().is_empty(), "nothing reached the catalog: {:?}", server.catalog_kinds());
        assert_eq!(server.deletes(), 0);

        server.fail_heads(0);
        engine.apply_catalog(events(), &users_compiled(), RestoreMode::Resume).await.expect("the retry restores");
        let st = engine.state.lock().await;
        assert_eq!(shape_ids(&st), ["s1", "s2"]);
        assert_eq!(st.subscription_owner("sub-s2"), Some(&"s2".to_string()));
        assert!(st.sequencer.is_some());
        drop(st);
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        assert_eq!(kinds_for(&server, "s3"), ["dropped", "retired"]);
    }

    /// ...but ONE dropped response is not that: a transient `HEAD` failure is retried in place, so it
    /// costs a moment rather than the whole boot attempt.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_transient_head_failure_is_retried_in_place() {
        let server = FakeDs::start().await;
        let engine = restoring(Engine::new(DsClient::new(server.url()))).await;
        server.fail_heads(2);
        let fold = catalog(vec![shape("s1", "public.users"), shape("s2", "public.users")]);
        engine.apply_catalog(fold, &users_compiled(), RestoreMode::Resume).await.expect("the retries land");
        assert_eq!(shape_ids(&*engine.state.lock().await), ["s1", "s2"]);
        assert_eq!(server.heads(), 4, "two checks, two of their attempts answered 503");
    }

    /// The checks run concurrently, but nothing decided from them depends on which answer arrived
    /// first: over a catalog wider than the concurrency bound, the retirements are recorded in id
    /// order and exactly the shapes whose streams are there are installed.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_stream_checks_retire_in_id_order() {
        let server = FakeDs::start().await;
        let engine = restoring(Engine::new(DsClient::new(server.url()))).await;
        let ids: Vec<String> = (100..140).map(|n| format!("s{n}")).collect();
        // Streams missing on ...3 and ...7, and one record whose TABLE is gone in between: the
        // classification step condemns that one before any stream is checked, and it must still
        // land in id order among the others.
        let table_gone = "s125".to_string();
        let mut gone: Vec<String> = ids.iter().filter(|id| id.ends_with('3') || id.ends_with('7')).cloned().collect();
        for id in &gone {
            server.mark_stream_missing(&format!("shape/{id}"));
        }
        gone.push(table_gone.clone());
        gone.sort();
        let fold = catalog(
            ids.iter()
                .flat_map(|id| {
                    let table = if *id == table_gone { "public.gone" } else { "public.users" };
                    [shape(id, table), dormant(id)]
                })
                .collect(),
        );
        engine.apply_catalog(fold, &users_compiled(), RestoreMode::Resume).await.unwrap();

        let kept: Vec<String> = ids.iter().filter(|id| !gone.contains(id)).cloned().collect();
        assert_eq!(shape_ids(&*engine.state.lock().await), kept);
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        let dropped: Vec<String> = server
            .catalog_events()
            .iter()
            .filter(|e| e["t"] == "dropped")
            .map(|e| e["id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(dropped, gone, "retirement intents are recorded in id order, whichever step condemned them");
        assert_eq!(server.heads(), ids.len() as u64 - 1, "the gone table's record is never checked");
    }

    /// Resume starts from an engine that holds nothing — the boot gate keeps every create, join and
    /// reactivation out until the boot resolves, and a failed attempt undoes what it installed. A
    /// running sequencer, or a registered shape, is refused as the engine bug it is, before a single
    /// stream is checked: resuming over it would hide whatever it consumed or checkpointed.
    #[tokio::test(flavor = "multi_thread")]
    async fn resuming_over_an_engine_that_already_holds_state_is_refused() {
        let server = FakeDs::start().await;
        let running = restoring(Engine::new(DsClient::new(server.url()))).await;
        running.ensure_sequencer(&mut *running.state.lock().await);
        let err = running
            .apply_catalog(catalog(vec![shape("s1", "public.users")]), &users_compiled(), RestoreMode::Resume)
            .await
            .expect_err("a sequencer already running is not something to restore into");
        assert!(format!("{err:#}").contains("running sequencer"), "{err:#}");
        assert_eq!(crate::pg::boot_disposition(&err), crate::pg::BootFailure::Fatal);

        let registered = restoring(Engine::new(DsClient::new(server.url()))).await;
        let CatalogEvent::Created { rec, .. } = shape("s9", "public.users") else { unreachable!() };
        registered.state.lock().await.shapes.insert(rec.id.clone(), rec);
        let err = registered
            .apply_catalog(catalog(vec![shape("s1", "public.users")]), &users_compiled(), RestoreMode::Resume)
            .await
            .expect_err("a shape already registered is not something to restore over");
        assert!(format!("{err:#}").contains("1 shape(s)"), "{err:#}");
        assert_eq!(server.heads(), 0, "refused before any stream is checked");
    }

    /// A stream that passed the check and is gone by the time the resume writes to it: storage's
    /// answer is definitive, but the restore is past the point of retiring one shape, so it fails —
    /// and the boot retries rather than refusing, because the retry's check retires the shape. The
    /// same gone stream anywhere else in the boot stays the answer it is.
    #[test]
    fn a_stream_that_vanishes_mid_restore_is_retried_at_boot() {
        let gone = || anyhow::Error::new(crate::ds::StreamGone { path: "shape/s1".into(), status: 404 });
        let vanished = gone()
            .context("append initial aggregate")
            .context(RestoreStreamVanished { shape: "s1".into() })
            .context("resuming shape s1 on public.users")
            .context("restoring the durable shape catalog");
        assert_eq!(crate::pg::boot_disposition(&vanished), crate::pg::BootFailure::Retryable);
        assert!(format!("{vanished:#}").contains("vanished after the restore checked it"), "{vanished:#}");
        assert_eq!(crate::pg::boot_disposition(&gone().context("reading meta/catalog")), crate::pg::BootFailure::Fatal);
    }

    /// `append_retrying`'s errors stay typed: a stream storage confirms gone is a `StreamGone` the
    /// restore can recognise, and a terminal answer its `HEAD` could not confirm keeps the `HEAD`'s
    /// own unavailability, so an exhausted budget during a re-seed retries the boot instead of
    /// refusing it.
    #[tokio::test(flavor = "multi_thread")]
    async fn append_retrying_keeps_its_errors_typed() {
        let envs: Vec<crate::ds::Envelope> = vec![
            serde_json::from_value(
                serde_json::json!({ "type": "public.users", "key": "1", "headers": { "operation": "insert" } }),
            )
            .unwrap(),
        ];
        let shutdown = crate::shutdown::ShutdownToken::new();
        let budget = std::time::Duration::from_millis(300);

        let server = FakeDs::start().await;
        server.mark_stream_missing("shape/s1");
        let ds = DsClient::new(server.url());
        let err = ds.append_retrying("shape/s1", &envs, budget, &shutdown).await.expect_err("the stream is gone");
        assert!(crate::ds::is_stream_gone(&err), "{err:#}");

        server.fail_heads(u32::MAX);
        let err = ds.append_retrying("shape/s1", &envs, budget, &shutdown).await.expect_err("storage never confirms");
        assert!(crate::ds::is_unavailable(&err), "the HEAD's unavailability survives the budget: {err:#}");
        assert_eq!(crate::pg::boot_disposition(&err), crate::pg::BootFailure::Retryable);

        // An append answered "gone" for a stream its HEAD keeps finding: retried, retryable at boot,
        // and named for what it is rather than as an unreachable server.
        let inconsistent = FakeDs::start().await;
        inconsistent.answer_appends_gone("shape/s2");
        let err = DsClient::new(inconsistent.url())
            .append_retrying("shape/s2", &envs, budget, &shutdown)
            .await
            .expect_err("the contradiction never resolves");
        assert!(crate::ds::is_inconsistent(&err), "{err:#}");
        assert!(!crate::ds::is_unavailable(&err), "the server is answering, just not consistently: {err:#}");
        assert_eq!(crate::pg::boot_disposition(&err), crate::pg::BootFailure::Retryable);
        assert_eq!(crate::pg::boot_failure_name(&err), "durable-streams answers a stream inconsistently");
    }

    /// A dormant shape whose OWN stream storage lost while it slept: the touch that reactivates it
    /// finds nothing to append its replay to, and the shape is retired — `Dropped`, close-then-delete
    /// — rather than parked back to fail the same way on every touch. The runtime form of the boot's
    /// `stream_missing`, told apart from a gone change-log segment by the stream that is gone.
    #[tokio::test(flavor = "multi_thread")]
    async fn reactivating_a_shape_whose_own_stream_is_gone_retires_it() {
        let server = FakeDs::start().await;
        let engine = restoring(Engine::new(DsClient::new(server.url()))).await;
        let parked = CatalogEvent::Dormant {
            id: "s1".to_string(),
            resume: LogPosition::start(),
            gate: crate::pg::SnapshotGate::passthrough(),
        };
        engine
            .apply_catalog(catalog(vec![shape("s1", "public.users"), parked]), &users_compiled(), RestoreMode::Resume)
            .await
            .expect("the stream is there at boot");
        assert_eq!(engine.shape_lifecycle("s1").await, Some("dormant"));

        // While it is dormant, storage loses the stream; the change log holds a change to replay.
        server.mark_stream_missing("shape/s1");
        server.serve_page(
            &crate::changelog::segment_path(0),
            serde_json::json!([{ "type": "public.users", "key": "1", "value": { "id": 1, "name": "a" }, "headers": { "operation": "insert" } }]),
        );
        let err = engine.ensure_active("s1").await.expect_err("there is nothing to reactivate onto");
        assert!(format!("{err:#}").contains("reactivation failed"), "{err:#}");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while engine.get_shape("s1").await.is_some() {
            assert!(std::time::Instant::now() < deadline, "the shape was never retired");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        assert_eq!(kinds_for(&server, "s1"), ["dropped", "retired"], "retired, intent then completion");
    }

    /// `setup_postgres` runs once to success. A second call after one would put the boot phase back
    /// to `waiting` — closing the boot gate on a serving engine — and restore the catalog over the
    /// shapes it already restored; it is refused before it touches anything.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_second_setup_postgres_after_success_is_refused_untouched() {
        let engine = Engine::new_pg(DsClient::new("http://127.0.0.1:1"), "postgres://u@127.0.0.1:1/db".to_string());
        engine.health.store(HEALTH_ACTIVE, Ordering::Relaxed);
        engine.replicator_started.store(true, Ordering::SeqCst);
        let err = engine.setup_postgres(&[], "slot").await.expect_err("a completed boot is not re-run");
        assert!(format!("{err:#}").contains("already completed"), "{err:#}");
        assert!(engine.ensure_booted().is_ok(), "the refusal left the boot phase alone");
    }

    /// `POST /schema`'s engine half refuses a Postgres-mode engine outright — before it creates a
    /// segment, records a rotation, replaces the tables or spawns a sequencer.
    #[tokio::test(flavor = "multi_thread")]
    async fn define_schema_is_refused_in_postgres_mode() {
        let server = FakeDs::start().await;
        let engine = Engine::new_pg(DsClient::new(server.url()), "postgres://u@127.0.0.1:1/db".to_string());
        let schema: Schema = serde_json::from_value(serde_json::json!({
            "tables": { "users": { "columns": { "id": {"type":"int"} }, "primaryKey": "id" } }
        }))
        .unwrap();
        let err = engine.define_schema(&schema).await.expect_err("Postgres owns this engine's schema");
        assert!(err.downcast_ref::<crate::engine::SchemaIsPostgres>().is_some(), "{err:#}");
        let st = engine.state.lock().await;
        assert!(st.tables.is_empty() && st.sequencer.is_none(), "nothing was installed");
        drop(st);
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(5)).await);
        assert!(server.catalog_kinds().is_empty(), "no rotation was recorded: {:?}", server.catalog_kinds());
        assert_eq!(server.reads() + server.heads(), 0, "storage was not touched");
    }

    /// A resume that fails on something that may clear by itself — here Postgres refusing the
    /// connection an aggregate re-seeds through — used to drop that shape and retire its stream,
    /// throwing an acknowledged subscription away over a blip. Now the whole restore is undone: the
    /// shape that had already resumed leaves with its sequencer, nothing reaches the catalog (not
    /// even a checkpoint — the held sequencer never read), and the error reaches the boot typed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_transient_resume_failure_undoes_the_whole_restore() {
        let server = FakeDs::start().await;
        // Nothing listens on port 1: the aggregate's re-seed is refused at connect.
        let engine =
            restoring(Engine::new_pg(DsClient::new(server.url()), "postgres://u@127.0.0.1:1/db".to_string())).await;
        let fold = catalog(vec![
            shape("s1", "public.users"),
            shape("s2", "public.users"),
            dormant("s2"),
            count_of("s3", "public.users"),
        ]);
        let err = engine
            .apply_catalog(fold, &users_compiled(), RestoreMode::Resume)
            .await
            .expect_err("a resume failure fails the restore");
        assert!(format!("{err:#}").contains("s3"), "the error names the shape: {err:#}");
        assert_eq!(
            crate::pg::boot_disposition(&err),
            crate::pg::BootFailure::Retryable,
            "a refused Postgres connection must reach the boot typed, not as a string: {err:#}"
        );
        assert_nothing_installed(&engine).await;
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(20)).await);
        assert!(server.catalog_events().is_empty(), "nothing reached the catalog: {:?}", server.catalog_kinds());
        assert_eq!(server.deletes(), 0, "no healthy shape's stream was retired over a transient failure");

        // The retried boot restores into a clean engine (Postgres is still away, so without the
        // aggregate): a fresh sequencer, and the survivors registered with it.
        let fold = catalog(vec![shape("s1", "public.users"), shape("s2", "public.users"), dormant("s2")]);
        engine.apply_catalog(fold, &users_compiled(), RestoreMode::Resume).await.expect("the retry restores");
        assert_eq!(shape_ids(&*engine.state.lock().await), ["s1", "s2"]);
        assert!(engine.table_stats(&users()).await.is_some());
    }

    /// Park (a broken epoch) installs records only for the reset to retire, so it asks storage
    /// nothing — not even about a stream storage no longer has.
    #[tokio::test(flavor = "multi_thread")]
    async fn parking_a_broken_epoch_checks_no_streams() {
        let server = FakeDs::start().await;
        server.mark_stream_missing("shape/s1");
        let engine = Engine::new(DsClient::new(server.url()));
        engine
            .apply_catalog(catalog(vec![shape("s1", "public.users")]), &HashMap::new(), RestoreMode::Park)
            .await
            .unwrap();
        assert_eq!(server.heads(), 0);
        assert_eq!(shape_ids(&*engine.state.lock().await), ["s1"], "parked, missing stream or not");
    }

    /// The hold the restore relies on: a sequencer spawned held reads nothing until released, and
    /// one discarded while still held exits without leaving a checkpoint behind.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_held_sequencer_reads_nothing_until_released_and_a_discarded_one_writes_no_checkpoint() {
        let server = FakeDs::start().await;
        let engine = Engine::new(DsClient::new(server.url()));

        let held = engine.spawn_sequencer_task(true);
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert_eq!(server.reads(), 0, "a held sequencer must not touch the change log");
        let (done, stopped) = tokio::sync::oneshot::channel();
        held.cmd_tx.send(SequencerCmd::Discard { done }).unwrap_or_else(|_| panic!("the sequencer is running"));
        stopped.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), held.cmd_tx.closed())
            .await
            .expect("the discarded sequencer exits");
        assert!(engine.catalog_tx.drain(std::time::Duration::from_secs(5)).await);
        assert!(server.catalog_kinds().is_empty(), "no checkpoint: {:?}", server.catalog_kinds());
        assert_eq!(server.reads(), 0);

        let released = engine.spawn_sequencer_task(true);
        let (done, acked) = tokio::sync::oneshot::channel();
        released
            .cmd_tx
            .send(SequencerCmd::ReleaseReads { done })
            .unwrap_or_else(|_| panic!("the sequencer is running"));
        acked.await.unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while server.reads() == 0 {
            assert!(std::time::Instant::now() < deadline, "a released sequencer never read the change log");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn retired_round_trips_on_the_wire() {
        let json = serde_json::to_value(CatalogEvent::Retired { id: "s3".to_string() }).unwrap();
        assert_eq!(json["t"], "retired");
        assert_eq!(json["id"], "s3");
        match serde_json::from_value::<CatalogEvent>(json).unwrap() {
            CatalogEvent::Retired { id } => assert_eq!(id, "s3"),
            other => panic!("expected Retired, got {other:?}"),
        }
    }
}
