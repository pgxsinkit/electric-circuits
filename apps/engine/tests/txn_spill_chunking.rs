//! A transaction too large to buffer in memory still reaches the change log intact (ADR-0003),
//! driven against a fake durable-streams server that records every request body.
//!
//! The properties that make chunking safe for the sequencer, all asserted on what actually reached
//! storage rather than on the buffer's internals:
//!
//!   1. one commit becomes N appends, **all to the same (current) segment, in order** — so the
//!      chunks are contiguous on the log and the sequencer's "runs of equal `(txid, lsn)`" split
//!      sees one transaction (that it also WAITS for the whole of it is `txn_atomic_emission.rs`);
//!   2. `seq` is contiguous `0..n` **across** the chunk boundaries, every envelope carries the commit
//!      LSN and xid, and only the very last envelope carries the transaction-end marker — the
//!      `(lsn, seq)` de-duplication key stays unique and ordered, and the run has exactly one end;
//!   3. no request body exceeds the configured append budget;
//!   4. a failure part-way through leaves the earlier chunks on the log and **returns an error**,
//!      which is what stops the ingestor acknowledging the slot (`append_commit_chunked(...)?`
//!      precedes `update_applied_lsn` in `stream_loop`) — Postgres re-delivers, and the de-dup
//!      discards the chunks that did land.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use electric_circuits_engine::changelog::{ChangeLogConfig, ChangeLogWriter, ChangesState, segment_path};
use electric_circuits_engine::ds::{DsClient, Envelope, EnvelopeHeaders};
use electric_circuits_engine::replication::append_commit_chunked;
use electric_circuits_engine::txn_buffer::{TxnBuffer, TxnBufferConfig};

/// One recorded append: the stream path it went to, and the raw request body.
type Append = (String, Vec<u8>);

/// A durable-streams stub that keeps every append's path and body, and can be told to fail the
/// n-th append (the mid-commit failure case).
#[derive(Clone, Default)]
struct RecordingDs {
    appends: Arc<Mutex<Vec<Append>>>,
    /// 1-based index of the append to answer 500, or 0 for "never fail".
    fail_at: Arc<AtomicUsize>,
}

async fn ds_handler(State(ds): State<RecordingDs>, req: Request) -> Response {
    let path = req.uri().path().trim_start_matches('/').to_string();
    match *req.method() {
        Method::PUT => StatusCode::OK.into_response(),
        Method::HEAD => ([("stream-next-offset", "tip")]).into_response(),
        Method::POST => {
            if req.headers().get("stream-closed").is_some() {
                return StatusCode::NO_CONTENT.into_response();
            }
            let body = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
                Ok(b) => b,
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            };
            let n = {
                let mut g = ds.appends.lock().unwrap();
                g.push((path, body.to_vec()));
                g.len()
            };
            let fail_at = ds.fail_at.load(Ordering::Relaxed);
            if fail_at != 0 && n == fail_at {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            // A byte-positioned offset, so the writer's size accounting behaves as in production.
            ([("stream-next-offset", "0000000000000000_0000000000000100")]).into_response()
        }
        Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
        Method::DELETE => StatusCode::OK.into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn recording_ds() -> (DsClient, RecordingDs) {
    let state = RecordingDs::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().fallback(ds_handler).with_state(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (DsClient::new(format!("http://{address}")), state)
}

/// A writer that never rotates, so every chunk of the commit is expected on `changes/0`.
fn writer(ds: DsClient) -> ChangeLogWriter {
    ChangeLogWriter::new(
        ds,
        Arc::new(ChangesState::default()),
        ChangeLogConfig { segment_bytes: 0, segment_age: std::time::Duration::ZERO, retain: std::time::Duration::ZERO },
        Arc::new(|_, _| {}),
    )
}

fn env_of(key: &str, payload: usize) -> Envelope {
    Envelope {
        type_: "public.items".to_string(),
        key: key.to_string(),
        value: Some(serde_json::json!({ "id": key, "blob": "x".repeat(payload) })),
        old: None,
        headers: EnvelopeHeaders {
            operation: "insert".to_string(),
            txid: None,
            offset: None,
            lsn: None,
            seq: None,
            last: None,
            schema: None,
        },
    }
}

/// A scratch spill dir, removed on drop.
struct Scratch(std::path::PathBuf);
impl Scratch {
    fn new(tag: &str) -> Scratch {
        let p = std::env::temp_dir().join(format!("circuits-txn-it-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Scratch(p)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Fill a buffer with `n` envelopes under a cap small enough to force a spill and a budget small
/// enough to force several chunks.
fn spilled_buffer(dir: &std::path::Path, n: usize, memory_bytes: u64, append_bytes: u64) -> TxnBuffer {
    let cfg = TxnBufferConfig { memory_bytes, append_bytes, spill_dir: dir.to_path_buf() };
    let mut buf = TxnBuffer::new(3141, cfg);
    for i in 0..n {
        buf.push(env_of(&format!("k{i:04}"), 48), 64).unwrap();
    }
    buf
}

/// The whole point: a commit that spilled to disk and needed several appends still lands as one
/// contiguous, correctly stamped run on the current segment.
#[tokio::test]
async fn a_spilled_commit_lands_as_contiguous_chunks_on_the_current_segment() {
    let dir = Scratch::new("contiguous");
    let (ds, state) = recording_ds().await;
    let log = writer(ds);

    const BUDGET: u64 = 4096;
    let mut buf = spilled_buffer(&dir.0, 300, 1024, BUDGET);
    assert!(buf.spilled(), "the 1 KiB cap must have been crossed");
    let spill = buf.spill_path().expect("a spill file").to_path_buf();

    let chunks = append_commit_chunked(&log, &mut buf, "0/3A2B1C0", std::time::Instant::now()).await.unwrap();
    assert!(chunks > 1, "the append budget forced chunking (got {chunks})");

    let appends = state.appends.lock().unwrap().clone();
    assert_eq!(appends.len() as u64, chunks, "one request per chunk, nothing else");
    assert!(
        appends.iter().all(|(p, _)| p == &segment_path(0)),
        "every chunk went to the segment that was current when the commit began"
    );

    // Property 3: each request body is within the budget.
    for (i, (_, body)) in appends.iter().enumerate() {
        assert!(body.len() as u64 <= BUDGET, "chunk {i} posted {} bytes > {BUDGET}", body.len());
    }

    // Properties 1 + 2: concatenating the bodies in append order reproduces the transaction, in
    // order, with a contiguous seq, one (txid, lsn) throughout, and exactly one end marker.
    let landed: Vec<Envelope> = appends
        .iter()
        .flat_map(|(_, body)| serde_json::from_slice::<Vec<Envelope>>(body).expect("a JSON array of envelopes"))
        .collect();
    assert_eq!(landed.len(), 300);
    for (i, env) in landed.iter().enumerate() {
        assert_eq!(env.key, format!("k{i:04}"), "transaction order survived the spill and the chunking");
        assert_eq!(env.headers.seq, Some(i as u64), "seq is contiguous 0..n ACROSS chunk boundaries");
        assert_eq!(env.headers.lsn.as_deref(), Some("0/3A2B1C0"));
        assert_eq!(env.headers.txid.as_deref(), Some("3141"));
        // The transaction-end marker is on the last envelope of the last chunk and nowhere else;
        // without it the sequencer would hold this run forever, with it on any earlier envelope it
        // would flush a fragment.
        assert_eq!(env.headers.last, (i + 1 == 300).then_some(true), "envelope {i}");
    }

    // The spill file is scratch: it goes with the buffer.
    assert!(spill.exists(), "still there while the buffer lives");
    drop(buf);
    assert!(!spill.exists(), "removed when the transaction is done");
}

/// A commit that fits the budget is still exactly one append — chunking must not cost the ordinary
/// transaction an extra round-trip.
#[tokio::test]
async fn a_small_commit_is_still_a_single_append() {
    let dir = Scratch::new("small");
    let (ds, state) = recording_ds().await;
    let log = writer(ds);

    let cfg =
        TxnBufferConfig { memory_bytes: 128 * 1024 * 1024, append_bytes: 64 * 1024 * 1024, spill_dir: dir.0.clone() };
    let mut buf = TxnBuffer::new(7, cfg);
    for i in 0..25 {
        buf.push(env_of(&format!("k{i}"), 16), 32).unwrap();
    }
    assert!(!buf.spilled());

    let chunks = append_commit_chunked(&log, &mut buf, "0/1", std::time::Instant::now()).await.unwrap();
    assert_eq!(chunks, 1);
    assert_eq!(state.appends.lock().unwrap().len(), 1);
    assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 0, "and nothing touched the disk");
}

/// An empty transaction appends nothing at all (a commit carrying only untracked tables).
#[tokio::test]
async fn an_empty_commit_appends_nothing() {
    let dir = Scratch::new("empty");
    let (ds, state) = recording_ds().await;
    let log = writer(ds);
    let cfg = TxnBufferConfig { memory_bytes: 1024, append_bytes: 4096, spill_dir: dir.0.clone() };
    let mut buf = TxnBuffer::new(1, cfg);
    assert_eq!(append_commit_chunked(&log, &mut buf, "0/1", std::time::Instant::now()).await.unwrap(), 0);
    assert!(state.appends.lock().unwrap().is_empty());
}

/// A failure part-way through a chunked commit is an ERROR, not a partial success: the ingestor's
/// `?` on this call is what keeps the slot unacknowledged, so Postgres re-delivers the whole
/// transaction and the sequencer's `(lsn, seq)` de-duplication discards the chunks that landed.
#[tokio::test]
async fn a_failure_mid_commit_propagates_so_the_slot_is_never_acknowledged() {
    let dir = Scratch::new("midfail");
    let (ds, state) = recording_ds().await;
    state.fail_at.store(3, Ordering::Relaxed); // the third chunk is refused
    let log = writer(ds);

    let mut buf = spilled_buffer(&dir.0, 300, 1024, 4096);
    let err = append_commit_chunked(&log, &mut buf, "0/9", std::time::Instant::now())
        .await
        .expect_err("a refused chunk must fail the whole commit");
    assert!(format!("{err:#}").contains("append changes"), "{err:#}");

    let appends = state.appends.lock().unwrap().clone();
    assert_eq!(appends.len(), 3, "it stopped at the failing chunk instead of carrying on");
    // The chunks that did land are a well-formed, ordered prefix — exactly what the sequencer's
    // de-duplication needs to be able to discard on re-delivery.
    let landed: Vec<Envelope> = appends
        .iter()
        .flat_map(|(_, body)| serde_json::from_slice::<Vec<Envelope>>(body).unwrap_or_default())
        .collect();
    for (i, env) in landed.iter().enumerate() {
        assert_eq!(env.headers.seq, Some(i as u64));
        assert_eq!(env.headers.lsn.as_deref(), Some("0/9"));
    }

    // And the aborted transaction still cleans up after itself.
    let spill = buf.spill_path().expect("a spill file").to_path_buf();
    drop(buf);
    assert!(!spill.exists());
}
