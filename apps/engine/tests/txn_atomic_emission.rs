//! **Per-transaction atomic emission survives chunking** (ADR-0003), driven against a real engine
//! (library mode) + sequencer and a fake durable-streams server that hands the change log out one
//! page at a time.
//!
//! A commit too large for one request body reaches the change log as several appends, and
//! durable-streams exposes each append atomically — so a reader long-polling the segment tail sees
//! chunk 1 on its own. Splitting the page into transactions by `(txid, lsn)` alone would make that
//! chunk look like a whole transaction and flush it to the shape streams, and a subscriber would
//! observe a fraction of a commit. The ingestor therefore marks the LAST envelope of every
//! transaction (`headers.last`), and the sequencer holds an unterminated trailing run back.
//!
//! Holding is not free of consequences, and each test here pins one of them down:
//!   1. an incomplete run is not flushed, a re-delivered prefix does not double-apply, and the
//!      completed transaction is flushed once — with publication pinned while held and released
//!      after;
//!   2. the "already held, skip it" filter applies to the held transaction ONLY: complete
//!      transactions that follow it in the same page — including ones whose `seq` restarts at 0 —
//!      are fanned out untouched (they are acknowledged, so nothing would ever re-deliver them);
//!   3. a page that completes one held run and starts another re-pins to ITS OWN page, so a
//!      catch-up over consecutive chunked commits does not freeze the checkpoint at the first one;
//!   4. progress made before a hold is checkpointed even though the hold pins the position — the
//!      de-duplication highwater moves on its own, and a crash must not re-apply what it covers.
//!
//! The same page machinery is what an UNPROCESSABLE envelope has to rewind (ADR-0010), so the last
//! four tests live here too: a change the engine cannot process parks the sequencer at the replay
//! boundary — publishing, flushing and checkpointing nothing past it, with the engine latched
//! `degraded` — while it keeps serving commands; a change for a table the engine does not compile is
//! consumed and counted instead; and only an operator's reset (`SequencerCmd::Jump`) moves the park on,
//! recording the new replay start before it reads again.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use electric_circuits_engine::changelog::LogPosition;
use electric_circuits_engine::ds::{DsClient, Envelope};
use electric_circuits_engine::engine::{Engine, EpochBroken};
use electric_circuits_engine::schema::Schema;
use electric_circuits_engine::table_ref::TableRef;

/// One recorded append: the stream path and the envelopes it carried.
type ShapeAppend = (String, Vec<Envelope>);
/// One page of the scripted change log: `next-offset` and the JSON body served at a given offset.
type Page = (String, String);

/// A durable-streams stub whose change log is a **script**: a map from the `(segment, offset)` a
/// reader asks for to the page it gets. An offset with no page parks, like a real long-poll — which is
/// what "the ingestor has not appended the next chunk yet" looks like. Keyed by segment as well as
/// offset because the log rotates (ADR-0006) and a reset moves the reader to a fresh segment whose
/// offsets start over at `-1`.
#[derive(Clone, Default)]
struct FakeLog {
    pages: Arc<Mutex<HashMap<String, Page>>>,
    /// Every POST to a `shape/*` stream — i.e. the per-transaction flushes.
    appends: Arc<Mutex<Vec<ShapeAppend>>>,
    /// Every event appended to the durable catalog (`meta/catalog`).
    catalog: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl FakeLog {
    /// Script one page of segment 0 — where the sequencer starts, and the only segment most of these
    /// tests need.
    fn serve(&self, at: &str, next: &str, envs: &[String]) {
        self.serve_segment(0, at, next, envs);
    }

    fn serve_segment(&self, segment: u32, at: &str, next: &str, envs: &[String]) {
        self.pages
            .lock()
            .unwrap()
            .insert(format!("changes/{segment}@{at}"), (next.to_string(), format!("[{}]", envs.join(","))));
    }

    fn shape_flushes(&self, path: &str) -> Vec<Vec<Envelope>> {
        self.appends.lock().unwrap().iter().filter(|(p, _)| p == path).map(|(_, e)| e.clone()).collect()
    }

    /// The `Offset` checkpoints as `(segment, offset)` — the position a restart resumes from. The
    /// segment matters after a rotation or a reset: every segment's offsets start at `-1`.
    fn checkpoint_positions(&self) -> Vec<(u32, String)> {
        self.catalog
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.get("t").and_then(|t| t.as_str()) == Some("offset"))
            .map(|e| {
                (
                    e["pos"]["segment"].as_u64().unwrap_or_default() as u32,
                    e["pos"]["offset"].as_str().unwrap_or_default().to_string(),
                )
            })
            .collect()
    }

    /// The `Offset` checkpoints the sequencer has written, as `(position offset, highwater)`.
    fn checkpoints(&self) -> Vec<(String, Option<serde_json::Value>)> {
        self.catalog
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.get("t").and_then(|t| t.as_str()) == Some("offset"))
            .map(|e| (e["pos"]["offset"].as_str().unwrap_or_default().to_string(), e.get("highwater").cloned()))
            .collect()
    }
}

/// One envelope of a scripted transaction, exactly as the ingestor stamps it.
fn env_json(txid: u32, lsn: &str, key: &str, seq: u32, last: bool) -> String {
    env_json_typed("public.t", txid, lsn, key, seq, last)
}

/// The same, for a `type` other than the one table this engine compiles.
fn env_json_typed(type_: &str, txid: u32, lsn: &str, key: &str, seq: u32, last: bool) -> String {
    let marker = if last { r#","last":true"# } else { "" };
    format!(
        r#"{{"type":"{type_}","key":"{key}","value":{{"id":"{key}"}},"headers":{{"operation":"insert","txid":"{txid}","lsn":"{lsn}","seq":{seq}{marker}}}}}"#
    )
}

async fn ds_handler(State(log): State<FakeLog>, req: Request) -> Response {
    let path = req.uri().path().trim_start_matches('/').to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    match *req.method() {
        Method::PUT | Method::DELETE => StatusCode::OK.into_response(),
        // The change log's boot walk HEADs the current segment (ADR-0006): present, never closed.
        Method::HEAD => ([("stream-next-offset", "tip")]).into_response(),
        Method::POST => {
            if req.headers().get("stream-closed").is_some() || path.starts_with("changes") {
                return StatusCode::OK.into_response();
            }
            let body = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
                Ok(b) => b,
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            };
            if path.starts_with("shape") {
                if let Ok(envs) = serde_json::from_slice::<Vec<Envelope>>(&body) {
                    log.appends.lock().unwrap().push((path, envs));
                }
            } else if path.starts_with("meta")
                && let Ok(events) = serde_json::from_slice::<Vec<serde_json::Value>>(&body)
            {
                log.catalog.lock().unwrap().extend(events);
            }
            StatusCode::OK.into_response()
        }
        Method::GET if path.starts_with("changes") => {
            let at = query.split('&').find_map(|kv| kv.strip_prefix("offset=")).unwrap_or("-1").to_string();
            let page = log.pages.lock().unwrap().get(&format!("{path}@{at}")).cloned();
            match page {
                Some((next, body)) => ([("stream-next-offset", next.as_str())], body).into_response(),
                // Nothing appended past here yet: park, like a real long-poll.
                None => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    (StatusCode::NO_CONTENT, [("stream-next-offset", at.as_str())]).into_response()
                }
            }
        }
        Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

/// A library-mode engine with one match-all shape on `public.t`, reading the scripted log.
async fn boot() -> (Engine, FakeLog, String, TableRef) {
    let state = FakeLog::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ds_url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(ds_handler).with_state(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let engine = Engine::new(DsClient::new(&ds_url));
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": { "t": { "columns": { "id": { "type": "text" } }, "primaryKey": "id" } }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    let t = TableRef::parse("t").unwrap();
    let shape = engine.create_shape(&t, None, None, false, false).await.unwrap();
    (engine, state, shape.stream_path, t)
}

async fn wait_for(mut cond: impl FnMut() -> bool, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

/// One envelope the engine cannot process: a `bogus` operation, which `apply_envelope` refuses. It
/// carries NO schema stamp, and the library-mode table has no digest either — so the ADR-0010 fence
/// reads "the schema that would decode this IS the one it was decoded under", which is exactly the
/// case that must park the sequencer rather than be stepped over.
fn unprocessable(txid: u32, lsn: &str, key: &str, seq: u32) -> String {
    format!(
        r#"{{"type":"public.t","key":"{key}","value":{{"id":"{key}"}},"headers":{{"operation":"bogus","txid":"{txid}","lsn":"{lsn}","seq":{seq},"last":true}}}}"#
    )
}

/// Wait until the engine has parked on an unprocessable envelope (ADR-0010).
async fn wait_parked(engine: &Engine) -> serde_json::Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if let Some(failure) = engine.change_log_failure_json() {
            return failure;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("the sequencer did not park on the unprocessable envelope");
}

/// Chunk 1 alone flushes nothing and pins publication; a re-delivered prefix does not double-apply;
/// the marked final chunk flushes the transaction once, whole, and releases the pin.
#[tokio::test]
async fn a_chunked_transaction_is_flushed_once_and_only_when_complete() {
    let (engine, log, stream, t) = boot().await;
    // Only chunk 1 is on the log.
    log.serve("-1", "01", &[env_json(100, "0/10", "1", 0, false), env_json(100, "0/10", "2", 1, false)]);

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(log.shape_flushes(&stream).is_empty(), "a chunk of an incomplete transaction must not be flushed");
    // Publication is pinned where the held run began: `processed` is the restart point,
    // `GET /tables/{name}/offset` and the segment-deletion floor.
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1", "pinned while held");

    // The ingestor failed on a later chunk, so Postgres re-delivered the WHOLE transaction: the
    // same seqs arrive again ahead of the rest. Still nothing, and no double-apply.
    log.serve("01", "02", &[env_json(100, "0/10", "1", 0, false), env_json(100, "0/10", "2", 1, false)]);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(log.shape_flushes(&stream).is_empty(), "a re-delivered prefix is still not a transaction");

    // The final chunk, marked.
    log.serve("02", "03", &[env_json(100, "0/10", "3", 2, false), env_json(100, "0/10", "4", 3, true)]);
    wait_for(|| !log.shape_flushes(&stream).is_empty(), "the completed transaction to be flushed").await;

    let flushes = log.shape_flushes(&stream);
    assert_eq!(flushes.len(), 1, "one transaction, one flush: {flushes:?}");
    let keys: Vec<&str> = flushes[0].iter().map(|e| e.key.as_str()).collect();
    assert_eq!(keys, vec!["1", "2", "3", "4"], "the whole transaction, in order, exactly once");

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.shape_flushes(&stream).len(), 1, "and nothing further for it");
    assert_ne!(engine.table_offset(&t).await.unwrap().offset, "-1", "the pin is released once it completes");
}

/// The de-duplication that folds a re-delivered prefix into a held run applies to the HELD
/// transaction only.
///
/// After a reconnect Postgres re-delivers the interrupted transaction, and the page can carry
/// complete transactions after it whose `seq` restarts at 0. Filtering the whole page on
/// "seq greater than the last one held" would drop those outright — and they are acknowledged, so
/// nothing would ever deliver them again. That is silent, permanent data loss.
#[tokio::test]
async fn complete_transactions_following_a_held_run_are_never_filtered_by_its_seqs() {
    let (_engine, log, stream, _t) = boot().await;
    // B is huge: its first chunk ends at seq 1001, unmarked.
    log.serve("-1", "01", &[env_json(200, "0/20", "b1", 1000, false), env_json(200, "0/20", "b2", 1001, false)]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(log.shape_flushes(&stream).is_empty());

    // B's tail, then two complete single-envelope transactions whose seqs start at 0 again.
    log.serve(
        "01",
        "02",
        &[
            env_json(200, "0/20", "b3", 1002, true),
            env_json(201, "0/21", "c1", 0, true),
            env_json(202, "0/22", "d1", 0, true),
        ],
    );
    wait_for(|| log.shape_flushes(&stream).len() >= 3, "B, C and D to be flushed").await;

    let flushes = log.shape_flushes(&stream);
    assert_eq!(flushes.len(), 3, "three transactions, three flushes: {flushes:?}");
    let per_txn: Vec<Vec<&str>> = flushes.iter().map(|f| f.iter().map(|e| e.key.as_str()).collect()).collect();
    assert_eq!(per_txn, vec![vec!["b1", "b2", "b3"], vec!["c1"], vec!["d1"]], "each once, in order");

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.shape_flushes(&stream).len(), 3, "nothing is emitted twice");
}

/// A page that completes one held run and starts another must re-pin to ITS OWN page.
///
/// Keeping the first page's pin would freeze the published position — and with it the checkpoint,
/// which only fires when the position moves — for the whole of a catch-up over consecutive chunked
/// commits, so a crash would re-apply every transaction flushed in between.
#[tokio::test]
async fn a_new_hold_after_a_completed_one_re_pins_to_its_own_page() {
    let (engine, log, stream, t) = boot().await;
    log.serve("-1", "01", &[env_json(300, "0/30", "a1", 0, false)]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1");

    // A completes here, and B's first chunk starts a NEW hold.
    log.serve("01", "02", &[env_json(300, "0/30", "a2", 1, true), env_json(301, "0/31", "b1", 0, false)]);
    wait_for(|| !log.shape_flushes(&stream).is_empty(), "A to be flushed").await;

    let flushes = log.shape_flushes(&stream);
    assert_eq!(flushes.len(), 1);
    assert_eq!(flushes[0].iter().map(|e| e.key.as_str()).collect::<Vec<_>>(), vec!["a1", "a2"]);
    // The pin followed the new hold instead of staying on page 1.
    assert_eq!(
        engine.table_offset(&t).await.unwrap().offset,
        "01",
        "the pin moved to the page the NEW held run began in"
    );
    // B is still held: nothing more is flushed.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.shape_flushes(&stream).len(), 1);
}

/// Progress made before a hold is checkpointed even though the hold pins the position.
///
/// When the pinned position happens to equal the last checkpointed one — the common case on the
/// first page — a position-only checkpoint trigger never fires, so the de-duplication highwater that
/// covers the already-applied transaction never reaches the catalog and a crash re-applies it.
/// Aggregate and subquery contributor weights are not idempotent, so that is a correctness bug, not
/// a performance one.
#[tokio::test]
async fn the_highwater_is_checkpointed_even_while_the_position_is_pinned() {
    let (engine, log, stream, t) = boot().await;
    // One complete transaction, then the first chunk of a large one — all in the very first page,
    // so the pin lands exactly on the position the sequencer started (and last checkpointed) at.
    log.serve("-1", "01", &[env_json(400, "0/40", "a1", 0, true), env_json(401, "0/41", "b1", 0, false)]);
    wait_for(|| !log.shape_flushes(&stream).is_empty(), "A to be flushed").await;
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1", "pinned at the start position");

    // The checkpoint cadence is ~2 s; the highwater has moved even though the position has not.
    wait_for(
        || log.checkpoints().iter().any(|(_, hw)| hw.is_some()),
        "a checkpoint carrying the highwater of the applied transaction",
    )
    .await;
    let (pos, hw) = log.checkpoints().into_iter().find(|(_, hw)| hw.is_some()).unwrap();
    assert_eq!(pos, "-1", "written at the pinned position");
    // A's commit LSN is 0x40 and its last seq 0 — what a restart must de-duplicate against.
    assert_eq!(hw.unwrap(), serde_json::json!([0x40, 0]));
    // ...and B, still held, was not part of it.
    assert_eq!(log.shape_flushes(&stream).len(), 1);
}

/// A change the engine cannot process is TERMINAL, not skippable (ADR-0010): the envelope stays at the
/// replay boundary, with no flush, no published position, no highwater and no checkpoint that could
/// make the missing effect vanish — and the engine says so rather than looking healthy.
#[tokio::test]
async fn an_unprocessable_envelope_parks_the_sequencer_without_progress() {
    let (engine, log, stream, t) = boot().await;
    // A complete transaction whose SECOND envelope cannot be processed. The first has already been
    // staged when the second fails, so this is also the "no partial transaction is flushed" case.
    log.serve("-1", "01", &[env_json(500, "0/50", "ok", 0, false), unprocessable(500, "0/50", "bad", 1)]);

    let failure = wait_parked(&engine).await;
    assert_eq!(failure["table"], "public.t");
    assert_eq!(failure["key"], "bad");
    assert_eq!(failure["txid"], "500");
    assert_eq!(failure["lsn"], "0/50");
    assert_eq!(failure["envelopeOffset"], 1);
    assert_eq!(failure["position"]["offset"], "-1", "the position an operator reads is the replay boundary");
    assert_eq!(failure["recovery"], "POST /epoch/reset");
    assert!(failure["error"].as_str().unwrap().contains("bogus"), "the error names the cause: {failure}");

    // Nothing of the transaction reached a subscriber, and nothing moved.
    assert!(log.shape_flushes(&stream).is_empty(), "a failed transaction must not flush its prefix");
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1", "failed work stays at the replay boundary");

    // Degraded, by name, with every shape route refusing — the state an operator (and a load
    // balancer) reads. Never auto-reset: the reason needs an operator.
    assert_eq!(engine.health_status(), "degraded");
    assert_eq!(engine.readiness_status(), "degraded");
    assert_eq!(engine.epoch_broken().map(|r| r.as_str()), Some("change_log_unprocessable"));
    assert!(engine.ensure_not_degraded().is_err(), "shape routes must refuse while parked");

    // It stays parked: not retried, and nothing further is read even though the next page is there.
    log.serve("01", "02", &[env_json(501, "0/51", "later", 0, true)]);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(log.shape_flushes(&stream).is_empty(), "a parked sequencer reads nothing further");
    assert!(
        log.checkpoints().iter().all(|(offset, hw)| offset == "-1" && hw.is_none()),
        "no checkpoint may move past the failed envelope: {:?}",
        log.checkpoints()
    );

    // Commands are still served while parked — the reset's purges and its `Jump` are how the engine
    // recovers, and both arrive as commands. `mem_bytes` round-trips through the loop, so it answering
    // at all is the proof; the purge is what the reset then does to every shape.
    let _ = engine.mem_bytes().await;
    let shape_id = stream.strip_prefix("shape/").expect("a shape stream path").to_string();
    engine.purge_shape(&shape_id).await.expect("a parked sequencer still serves a purge");

    // ...and the shutdown's final checkpoint is the rewound position, never past the envelope, so the
    // next process re-derives the same park instead of stepping over it.
    engine.shutdown_token().begin();
    wait_for(|| !log.checkpoints().is_empty(), "the final checkpoint").await;
    assert!(
        log.checkpoints().iter().all(|(offset, _)| offset == "-1"),
        "the shutdown checkpoint crossed the failed envelope: {:?}",
        log.checkpoints()
    );
}

/// If a held transaction completes on the same page as a later failure, the replay boundary is still
/// the page the HELD run began in — not the failing page. Its appends went out from here, so a replay
/// that started after them would be a replay of a different log than the one that was flushed.
#[tokio::test]
async fn a_failure_after_a_held_prefix_rewinds_to_the_held_boundary() {
    let (engine, log, stream, t) = boot().await;
    log.serve("-1", "01", &[env_json(500, "0/50", "b0", 0, false)]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(log.shape_flushes(&stream).is_empty(), "the held prefix is not flushed");
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1", "and it pins the boundary");

    // B completes here; C cannot be processed.
    log.serve("01", "02", &[env_json(500, "0/50", "b1", 1, true), unprocessable(501, "0/51", "bad", 0)]);
    wait_for(|| !log.shape_flushes(&stream).is_empty(), "B's completed transaction to flush").await;
    let failure = wait_parked(&engine).await;

    let flushes = log.shape_flushes(&stream);
    assert_eq!(flushes.len(), 1, "B once, C never: {flushes:?}");
    assert_eq!(flushes[0].iter().map(|e| e.key.as_str()).collect::<Vec<_>>(), vec!["b0", "b1"]);
    assert_eq!(failure["position"]["offset"], "-1", "the park is at the held run's page, not the failing one");
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1", "replay must include B's held prefix");
    assert!(
        log.checkpoints().iter().all(|(offset, _)| offset == "-1"),
        "a checkpoint crossed the held boundary: {:?}",
        log.checkpoints()
    );
    // B's highwater may ride along at the pinned position (that is the ordinary held-run case); C's
    // must not — its transaction was rewound.
    assert!(
        log.checkpoints().iter().all(|(_, hw)| hw.is_none() || hw.as_ref() == Some(&serde_json::json!([0x50, 1]))),
        "the failed transaction's highwater was checkpointed: {:?}",
        log.checkpoints()
    );
    engine.shutdown_token().begin();
}

/// The table half of the fence, end to end (ADR-0010): a change for a table the engine does not
/// compile is CONSUMED and counted — its dependents were retired with it, so nothing can want it —
/// while a `type` no producer could have written parks the sequencer, because that is the log itself
/// being wrong. The old code logged `ERROR change for unknown table` per envelope and stepped over
/// both.
#[tokio::test]
async fn an_uncompiled_table_is_consumed_and_an_unspellable_type_parks() {
    let (engine, log, stream, _t) = boot().await;
    let skipped = || {
        electric_circuits_engine::metrics::metrics()
            .sequencer_unknown_table_skipped
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    let before = skipped();

    // A well-formed table this engine does not compile, then an ordinary change: the first is
    // consumed and the second is fanned out, which is the proof that the skip did not park anything.
    log.serve(
        "-1",
        "01",
        &[env_json_typed("public.gone", 800, "0/80", "g1", 0, true), env_json(801, "0/81", "ok", 0, true)],
    );
    wait_for(|| !log.shape_flushes(&stream).is_empty(), "the change for the COMPILED table to be flushed").await;
    assert_eq!(log.shape_flushes(&stream)[0].iter().map(|e| e.key.as_str()).collect::<Vec<_>>(), vec!["ok"]);
    assert_eq!(skipped(), before + 1, "the uncompiled table's change is counted, not silent");
    assert!(engine.change_log_failure_json().is_none(), "consuming it must not park the sequencer");
    assert_eq!(engine.health_status(), "active");

    // A bare (non-canonical) `type`: the ingestor stamps `TableRef::to_string()` and library-mode
    // writes go through `canonicalTable`, so nothing that writes this log can produce one.
    log.serve("01", "02", &[env_json_typed("t", 802, "0/82", "bare", 0, true)]);
    let failure = wait_parked(&engine).await;
    assert_eq!(failure["table"], "t");
    assert_eq!(failure["key"], "bare");
    assert!(
        failure["error"].as_str().unwrap().contains("canonical schema.name"),
        "the failure must name what is wrong with the envelope: {failure}"
    );
    assert_eq!(engine.health_status(), "degraded");
    assert_eq!(engine.epoch_broken().map(|r| r.as_str()), Some("change_log_unprocessable"));
    // Only "ok" ever reached the shape.
    assert_eq!(log.shape_flushes(&stream).len(), 1);
    engine.shutdown_token().begin();
}

/// The recovery: a reset restarts the replay on a fresh segment (`SequencerCmd::Jump`), so nothing
/// before it is ever read again — the parked envelope included — and the sequencer reads once more.
/// The real caller is `Engine::reset_epoch`, which also retires every shape and rebinds the slot; that
/// half needs a Postgres and is covered by the epoch conformance lane.
#[tokio::test]
async fn a_jump_releases_the_park_and_never_reads_the_old_segment_again() {
    let (engine, log, stream, t) = boot().await;
    log.serve("-1", "01", &[unprocessable(600, "0/60", "bad", 0)]);
    wait_parked(&engine).await;
    // Whatever else is in the old segment stays unread.
    log.serve("01", "02", &[env_json(601, "0/61", "never", 0, true)]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(log.shape_flushes(&stream).is_empty());

    // The jump onto the segment the reset rotated to, with one post-reset change already on it.
    log.serve_segment(1, "-1", "s1a", &[env_json(700, "0/70", "after", 0, true)]);
    let taken = engine.force_sequencer_jump(LogPosition::start_of(1)).await;
    assert!(taken, "a running sequencer takes the jump — and records the new position itself");
    // The sequencer records the new start from its own task before it acknowledges, so it is the
    // FIRST durable position after the jump: a checkpoint it writes once reading resumes cannot be
    // ordered ahead of it (one task, one FIFO to the catalog writer). The park wrote none, so this is
    // the first checkpoint of the whole test.
    wait_for(|| !log.checkpoint_positions().is_empty(), "the jump's position to reach the catalog").await;
    assert_eq!(
        log.checkpoint_positions().first().cloned(),
        Some((1, "-1".to_string())),
        "the first durable position after a jump must be the new segment's start: {:?}",
        log.checkpoint_positions()
    );

    // It reads again, on the new segment, and consumes what is there.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut pos = engine.table_offset(&t).await.unwrap();
    while (pos.segment == 0 || pos.offset == "-1") && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
        pos = engine.table_offset(&t).await.unwrap();
    }
    assert_eq!(pos.segment, 1, "the park is released onto the segment the reset rotated to");
    assert_eq!(pos.offset, "s1a", "and the page waiting there is consumed");
    // Nothing from the abandoned segment is ever fanned out or checkpointed — including the envelope
    // the sequencer parked on. A reset retires every shape, so the one from the old epoch gets nothing;
    // clients re-subscribe.
    assert!(log.shape_flushes(&stream).is_empty(), "a shape from the old epoch received nothing");
    assert!(
        log.checkpoint_positions().iter().all(|(segment, _)| *segment == 1),
        "a checkpoint named a position in the abandoned segment: {:?}",
        log.checkpoint_positions()
    );
    // The jump moved the reader; it did not forgive the break. Only the reset's rebind does that, so
    // creates stay refused until an operator's `POST /epoch/reset` completes.
    let err = engine.create_shape(&t, None, None, false, false).await.expect_err("creates stay refused");
    assert!(err.downcast_ref::<EpochBroken>().is_some(), "unexpected refusal: {err:#}");
    engine.shutdown_token().begin();
}
