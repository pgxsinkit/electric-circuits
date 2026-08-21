//! End-to-end regression tests for the Electric adapter's **resume** path (`GET /v1/shape` with a
//! persisted `(handle, offset)`), driven through the real HTTP surface against an in-memory ds
//! server and a library-mode engine — no Postgres.
//!
//! The bug these exist for: a client that reconnects at an offset the handle has already served
//! past needs the key set IT holds at that offset. Rebuilding at the stream's TAIL instead loses
//! every key deleted in between, and `apply_changes` then silently drops those deletes (its delete
//! arm is gated on `keys.remove(..)`) — so a row whose access was revoked while the client was away
//! is never evicted. It fails in the retain-data direction, which is why it needs a test that
//! watches a delete SURVIVE a resume rather than one that watches a fold.
//!
//! The fake ds models the one property the reconstruction depends on: a read is an exact SUFFIX
//! from the requested offset, and envelopes carry NO offset of their own (durable-streams stores
//! each JSON message verbatim and serves a byte range — it never stamps per-message offsets, and
//! neither does the engine).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use electric_circuits_engine::ds::DsClient;
use electric_circuits_engine::engine::Engine;
use electric_circuits_engine::http::router;
use electric_circuits_engine::schema::Schema;
use tower::ServiceExt;

// ---- the fake durable-streams server ------------------------------------------------------------

/// One store per server, so tests in this binary (which run concurrently) never share a change log
/// or a shape stream.
type Store = Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>;

/// How many more shape-stream reads should have an append land underneath them — the stream growing
/// while the adapter is mid-reconstruction. Drained by the handler, one per read.
type Growth = Arc<AtomicUsize>;

/// Earliest position still retained on the shape stream: a read below it is `410 Gone`, as
/// retention/compaction makes it on the real server. 0 = everything retained.
type Floor = Arc<AtomicUsize>;

#[derive(Clone)]
struct DsState {
    store: Store,
    growth: Growth,
    floor: Floor,
}

/// Offsets are opaque tokens to the engine; here they are the item index, zero-padded like the real
/// server's fixed-width tokens. `-1` is the protocol's "from the beginning" sentinel.
fn tok(n: usize) -> String {
    format!("{n:016}")
}

/// `Ok(index)`, or `Err(status)` for an offset the server would refuse: `400` for a token it cannot
/// parse, `410` for one below the earliest retained position. Modelling those is the point — coercing
/// a bad token to zero (as this fake used to) answers a client's garbage offset with a full replay
/// from the start of the stream, and hides that the adapter turned the refusal into a 500.
fn parse_offset(q: &str, floor: usize) -> Result<usize, u16> {
    let raw = q.split('&').find_map(|kv| kv.strip_prefix("offset=")).unwrap_or("-1");
    if raw == "-1" {
        return Ok(0);
    }
    if raw.len() != 16 || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(400);
    }
    match raw.parse::<usize>() {
        Ok(n) if n < floor => Err(410),
        Ok(n) => Ok(n),
        Err(_) => Err(400),
    }
}

async fn ds_handler(State(state): State<DsState>, req: Request) -> Response {
    let store = state.store.clone();
    let path = req.uri().path().trim_start_matches('/').to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let method = req.method().clone();
    match method {
        Method::PUT => {
            store.lock().unwrap().entry(path).or_default();
            StatusCode::OK.into_response()
        }
        Method::DELETE => {
            store.lock().unwrap().remove(&path);
            StatusCode::OK.into_response()
        }
        Method::POST => {
            let body = to_bytes(req.into_body(), 8 * 1024 * 1024).await.unwrap();
            let items: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
            let mut s = store.lock().unwrap();
            let stream = s.entry(path).or_default();
            stream.extend(items);
            let next = tok(stream.len());
            ([("stream-next-offset", next)], StatusCode::OK).into_response()
        }
        Method::GET => {
            // A concurrent writer: land one append on the shape stream before serving this read, so
            // the adapter's scans see a moving tail.
            if path.starts_with("shape")
                && state
                    .growth
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
            {
                let n = store.lock().unwrap().get(&path).map(Vec::len).unwrap_or(0);
                let key = format!("grow{n}");
                store.lock().unwrap().entry(path.clone()).or_default().push(serde_json::json!({
                    "type": "t",
                    "key": key,
                    "value": { "id": key },
                    "headers": { "operation": "upsert", "lsn": "0/99" },
                }));
            }
            let from = match parse_offset(&query, state.floor.load(Ordering::SeqCst)) {
                Ok(n) => n,
                Err(status) => {
                    return StatusCode::from_u16(status).unwrap().into_response();
                }
            };
            let live = query.split('&').any(|kv| kv == "live=long-poll");
            let (items, len) = {
                let s = store.lock().unwrap();
                let stream = s.get(&path).cloned().unwrap_or_default();
                let len = stream.len();
                (stream.into_iter().skip(from).collect::<Vec<_>>(), len)
            };
            if items.is_empty() {
                if live {
                    // Park like a real long-poll so the sequencer's tail read doesn't spin.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    return (
                        StatusCode::NO_CONTENT,
                        [("stream-next-offset", tok(len)), ("stream-up-to-date", "1".into())],
                    )
                        .into_response();
                }
                return (
                    [("stream-next-offset", tok(len)), ("stream-up-to-date", "1".into())],
                    "[]",
                )
                    .into_response();
            }
            (
                [("stream-next-offset", tok(len)), ("stream-up-to-date", "1".into())],
                serde_json::to_string(&items).unwrap(),
            )
                .into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

// ---- harness -------------------------------------------------------------------------------------

struct ShapeResponse {
    status: StatusCode,
    handle: Option<String>,
    offset: String,
    schema: Option<String>,
    messages: Vec<serde_json::Value>,
}

impl ShapeResponse {
    fn changes(&self) -> Vec<&serde_json::Value> {
        self.messages.iter().filter(|m| m["headers"].get("operation").is_some()).collect()
    }

    fn watermark(&self) -> Option<String> {
        self.messages
            .iter()
            .find(|m| m["headers"]["control"] == "up-to-date")
            .and_then(|m| m["headers"]["global_last_seen_lsn"].as_str().map(str::to_string))
    }
}

async fn get_shape(engine: &Engine, query: &str) -> ShapeResponse {
    let res = router(engine.clone())
        .oneshot(Request::builder().uri(format!("/v1/shape?{query}")).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let head = |n: &str| res.headers().get(n).and_then(|v| v.to_str().ok()).map(str::to_string);
    let (handle, offset, schema) =
        (head("electric-handle"), head("electric-offset").unwrap_or_default(), head("electric-schema"));
    let body = to_bytes(res.into_body(), 8 * 1024 * 1024).await.unwrap();
    // An error response's body is plain text, not a message array — keep it out of `messages`
    // rather than panicking, so a test that expected a protocol answer fails on its own assertion.
    let messages: Vec<serde_json::Value> =
        if body.is_empty() { Vec::new() } else { serde_json::from_slice(&body).unwrap_or_default() };
    ShapeResponse { status, handle, offset, schema, messages }
}

/// One committed change on the global change log, exactly as the replication ingestor stamps it.
fn append_change(store: &Store, op: &str, key: &str, lsn: &str) {
    let row = serde_json::json!({ "id": key });
    // A delete carries the full prior row (`REPLICA IDENTITY FULL`), exactly as the ingestor sends
    // it — that retraction IS the delta.
    let env = serde_json::json!({
        "type": "t",
        "key": key,
        "value": if op == "delete" { serde_json::Value::Null } else { row.clone() },
        "old": if op == "delete" { row } else { serde_json::Value::Null },
        "headers": { "operation": op, "txid": lsn, "lsn": lsn, "seq": 0 },
    });
    store.lock().unwrap().entry("changes".into()).or_default().push(env);
}

/// Poll a positioned read until it delivers at least one change (the sequencer fans out
/// asynchronously), returning the response.
async fn read_until_change(engine: &Engine, query: &str) -> ShapeResponse {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let res = get_shape(engine, query).await;
        if !res.changes().is_empty() || Instant::now() > deadline {
            return res;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn engine_with_table() -> (Engine, Store, Growth, Floor) {
    let store: Store = Arc::new(Mutex::new(HashMap::new()));
    let growth: Growth = Arc::new(AtomicUsize::new(0));
    let floor: Floor = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ds_url = format!("http://{}", listener.local_addr().unwrap());
    let served = DsState { store: store.clone(), growth: growth.clone(), floor: floor.clone() };
    tokio::spawn(async move {
        let _ = axum::serve(listener, Router::new().fallback(ds_handler).with_state(served)).await;
    });
    let engine = Engine::new(DsClient::new(&ds_url));
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": { "t": { "columns": { "id": { "type": "text" } }, "primaryKey": "id" } }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    (engine, store, growth, floor)
}

// ---- tests ---------------------------------------------------------------------------------------

#[tokio::test]
async fn resume_at_a_persisted_offset_replays_a_delete_the_handle_already_served() {
    let (engine, store, _growth, _floor) = engine_with_table().await;

    // Snapshot: empty shape, fresh handle.
    let snap = get_shape(&engine, "table=t&offset=-1").await;
    assert_eq!(snap.status, StatusCode::OK);
    let handle = snap.handle.clone().expect("snapshot mints a handle");
    assert!(snap.schema.is_some(), "the snapshot carries electric-schema");
    let persisted = snap.offset.clone();

    // Two rows arrive.
    append_change(&store, "upsert", "k1", "0/10");
    append_change(&store, "upsert", "k2", "0/20");
    let caught_up = read_until_change(&engine, &format!("table=t&handle={handle}&offset={persisted}")).await;
    assert_eq!(caught_up.changes().len(), 2, "both inserts: {:?}", caught_up.messages);
    // This is the offset the client persists.
    let persisted = caught_up.offset.clone();

    // k2's access is revoked while the client is away — and the handle serves that delete to
    // SOMETHING (a retry, another tab, a poll that never reached the client), advancing its state.
    append_change(&store, "delete", "k2", "0/30");
    let served = read_until_change(&engine, &format!("table=t&handle={handle}&offset={persisted}")).await;
    assert_eq!(served.changes().len(), 1);
    assert_eq!(served.changes()[0]["headers"]["operation"], "delete");

    // The client now reconnects at ITS persisted offset. The delete must be replayed: rebuilt at the
    // tail, k2 is absent from the key set and the delete is dropped, leaving the revoked row in
    // place forever.
    let resumed = get_shape(&engine, &format!("table=t&handle={handle}&offset={persisted}")).await;
    assert_eq!(resumed.status, StatusCode::OK);
    let changes = resumed.changes();
    assert_eq!(changes.len(), 1, "the revocation must be replayed: {:?}", resumed.messages);
    assert_eq!(changes[0]["headers"]["operation"], "delete");
    assert_eq!(changes[0]["key"], "k2");
    // ...carrying the LSN of the commit that caused it (0/30 = 48), which is what a consumer
    // positions its dedup frontier on.
    assert_eq!(changes[0]["headers"]["lsn"], "48");

    // Every non-live response repeats electric-schema — a client resuming from a persisted offset
    // has never seen a snapshot, and @electric-sql/client hard-errors without it.
    assert!(resumed.schema.is_some(), "electric-schema must ride the resume response");
}

// The reconstruction counts the stream, counts what the client has not seen, and folds the
// difference. Those counts are taken by separate reads, so an append landing between them would skew
// the subtraction — the client's window would be measured against a stream that has since grown, and
// it would be handed a key set for the wrong position. The correction settles that, and this is the
// test that the settling is real: the same revocation replay as above, with an append landing under
// every read the adapter issues while it reconstructs.
#[tokio::test]
async fn a_resume_stays_correct_while_the_stream_grows_underneath_it() {
    let (engine, store, growth, _floor) = engine_with_table().await;

    let snap = get_shape(&engine, "table=t&offset=-1").await;
    let handle = snap.handle.clone().unwrap();

    append_change(&store, "upsert", "k1", "0/10");
    append_change(&store, "upsert", "k2", "0/20");
    let caught_up = read_until_change(&engine, &format!("table=t&handle={handle}&offset={}", snap.offset)).await;
    assert_eq!(caught_up.changes().len(), 2);
    let persisted = caught_up.offset.clone();

    append_change(&store, "delete", "k2", "0/30");
    let served = read_until_change(&engine, &format!("table=t&handle={handle}&offset={persisted}")).await;
    assert_eq!(served.changes().len(), 1);

    // Now make the stream move under the reconstruction: three of its reads get an append first.
    growth.store(3, Ordering::SeqCst);
    let resumed = get_shape(&engine, &format!("table=t&handle={handle}&offset={persisted}")).await;
    assert_eq!(resumed.status, StatusCode::OK);
    assert_eq!(growth.load(Ordering::SeqCst), 0, "the appends must actually have landed");

    // The revocation still replays...
    let revocation: Vec<_> = resumed.changes().into_iter().filter(|m| m["key"] == "k2").collect();
    assert_eq!(revocation.len(), 1, "the revocation must survive a moving tail: {:?}", resumed.messages);
    assert_eq!(revocation[0]["headers"]["operation"], "delete");

    // ...and the rows that arrived DURING the reconstruction are classified against the client's real
    // position: it has never seen them, so they are inserts, not updates of rows it does not hold.
    for m in resumed.changes().into_iter().filter(|m| m["key"].as_str().unwrap().starts_with("grow")) {
        assert_eq!(m["headers"]["operation"], "insert", "concurrent arrival misclassified: {m:?}");
    }
}

#[tokio::test]
async fn up_to_date_advertises_the_fan_out_frontier_not_zero() {
    let (engine, store, _growth, _floor) = engine_with_table().await;
    let snap = get_shape(&engine, "table=t&offset=-1").await;
    let handle = snap.handle.clone().unwrap();
    assert_eq!(snap.watermark().as_deref(), Some("0"), "nothing sequenced yet");

    append_change(&store, "upsert", "k1", "0/10");
    let caught_up = read_until_change(&engine, &format!("table=t&handle={handle}&offset={}", snap.offset)).await;
    assert_eq!(caught_up.changes().len(), 1);

    // The frontier is published after the fan-out lands, so it may trail this very response by a
    // beat; it must reach the commit and never exceed it.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let res = get_shape(&engine, &format!("table=t&handle={handle}&offset={}", caught_up.offset)).await;
        let seen = res.watermark().expect("up-to-date carries a watermark");
        assert!(seen.parse::<u64>().unwrap() <= 16, "must never advertise past the fan-out: {seen}");
        if seen == "16" {
            break; // 0/10
        }
        assert!(Instant::now() < deadline, "frontier never reached the sequenced commit");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// **An offset the stream cannot place is a `must-refetch`, not a 500.** durable-streams answers a
/// malformed offset with 400 and one that has aged out of retention with 410 — the position, not
/// the read, is what failed. Electric's own answer to that is `409` carrying the `must-refetch`
/// control message, which tells the client to re-snapshot onto the same shape; a 500 tells it
/// nothing it can act on, and a client whose persisted offset outlived the stream's retention would
/// see nothing else for as long as it kept retrying.
#[tokio::test]
async fn an_offset_the_stream_cannot_place_is_answered_must_refetch() {
    let (engine, store, _growth, _floor) = engine_with_table().await;
    let snap = get_shape(&engine, "table=t&offset=-1").await;
    let handle = snap.handle.clone().expect("snapshot mints a handle");
    append_change(&store, "upsert", "k1", "0/10");
    let caught_up = read_until_change(&engine, &format!("table=t&handle={handle}&offset={}", snap.offset)).await;
    assert_eq!(caught_up.changes().len(), 1);

    // A persisted offset that is not a token this stream ever issued: truncated storage, a client
    // that constructed one, a downgraded token format.
    let res = get_shape(&engine, &format!("table=t&handle={handle}&offset=not-a-real-offset")).await;
    assert_eq!(res.status, StatusCode::CONFLICT, "expected must-refetch, got {:?}", res.messages);
    assert_eq!(res.messages.len(), 1);
    assert_eq!(res.messages[0]["headers"]["control"], "must-refetch");

    // And the shape is still serving: the refusal is about that offset, not the shape.
    let fresh = get_shape(&engine, "table=t&offset=-1").await;
    assert_eq!(fresh.status, StatusCode::OK);
    assert_eq!(fresh.changes().len(), 1, "re-snapshot returns the live row: {:?}", fresh.messages);
}

/// The other half of "cannot place this offset": a position that has aged out. durable-streams
/// answers `410 Gone` below the earliest retained position (retention/compaction), which is the
/// case a long-offline client hits — and the one where a `500` would strand it forever, since
/// retrying the same persisted offset can never start working again. Same `must-refetch` answer.
#[tokio::test]
async fn an_offset_below_the_retained_window_is_answered_must_refetch() {
    let (engine, store, _growth, floor) = engine_with_table().await;
    let snap = get_shape(&engine, "table=t&offset=-1").await;
    let handle = snap.handle.clone().expect("snapshot mints a handle");
    append_change(&store, "upsert", "k1", "0/10");
    let caught_up = read_until_change(&engine, &format!("table=t&handle={handle}&offset={}", snap.offset)).await;
    assert_eq!(caught_up.changes().len(), 1);
    let persisted = caught_up.offset.clone();

    // The client goes away long enough for its position to fall off the back of the stream.
    floor.store(9_000, Ordering::SeqCst);

    let res = get_shape(&engine, &format!("table=t&handle={handle}&offset={persisted}")).await;
    assert_eq!(res.status, StatusCode::CONFLICT, "expected must-refetch, got {:?}", res.messages);
    assert_eq!(res.messages[0]["headers"]["control"], "must-refetch");

    let fresh = get_shape(&engine, "table=t&offset=-1").await;
    assert_eq!(fresh.status, StatusCode::OK, "the recovery sentinel must remain placeable");
    assert_eq!(fresh.changes().len(), 1, "re-snapshot returns the retained current row");
}
