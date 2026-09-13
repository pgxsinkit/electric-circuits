//! A terminal-looking append answer on a LIVE shape's stream is reconciled, never taken on trust.
//!
//! `404` is what a proxy, a storage router or a failover answers just as readily as a real
//! deletion. `append_reliable` returning `false` makes the sequencer advance past the batch, so
//! believing a false one leaves a still-registered shape permanently missing a committed Postgres
//! change — invisible to every client. Both branches are covered here against a durable-streams
//! stub: a false `404` (the stream is still there) must land the batch, and a real one must retire
//! the shape rather than leave it registered and stale.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use electric_circuits_engine::ds::{DsClient, Envelope, EnvelopeHeaders};
use electric_circuits_engine::engine::Engine;
use electric_circuits_engine::schema::Schema;
use electric_circuits_engine::table_ref::TableRef;

fn tref(name: &str) -> TableRef {
    TableRef::parse(name).unwrap()
}

#[derive(Clone, Default)]
struct FakeDs {
    /// Answer the next `POST /shape/*` with a 404 the stream does not deserve, then behave.
    false_404: Arc<AtomicBool>,
    /// The stream really is gone: every method on `shape/*` answers 404, HEAD included.
    stream_lost: Arc<AtomicBool>,
    /// Successful appends to `shape/*`.
    shape_appends: Arc<AtomicUsize>,
    deleted: Arc<std::sync::Mutex<Vec<String>>>,
    closed: Arc<std::sync::Mutex<Vec<String>>>,
}

async fn ds_handler(State(ds): State<FakeDs>, request: Request) -> Response {
    let path = request.uri().path().to_string();
    let is_shape = path.starts_with("/shape/");
    // A close is a POST with `stream-closed: true` and no body (ADR-0007); record the ATTEMPT even
    // when the stream is gone, so the close-then-delete order stays observable.
    let closing = request.method() == Method::POST && request.headers().get("stream-closed").is_some();
    if closing {
        ds.closed.lock().unwrap().push(path.clone());
    }
    if is_shape && ds.stream_lost.load(Ordering::SeqCst) && request.method() != Method::DELETE {
        return StatusCode::NOT_FOUND.into_response();
    }
    match *request.method() {
        Method::PUT => StatusCode::OK.into_response(),
        Method::DELETE => {
            ds.deleted.lock().unwrap().push(path);
            StatusCode::OK.into_response()
        }
        Method::POST => {
            if closing {
                return StatusCode::NO_CONTENT.into_response();
            }
            if is_shape && ds.false_404.swap(false, Ordering::SeqCst) {
                return StatusCode::NOT_FOUND.into_response();
            }
            if is_shape {
                ds.shape_appends.fetch_add(1, Ordering::SeqCst);
            }
            StatusCode::OK.into_response()
        }
        Method::HEAD => ([("stream-next-offset", "tip")]).into_response(),
        Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

/// An engine with one live plain shape (`s1` on `shape/s1`), plus the streams client the engine
/// itself uses — the reconciler is installed on the shared client, so appending through this handle
/// is exactly what the sequencer's emission does.
async fn engine_with_one_shape() -> (Engine, DsClient, FakeDs) {
    let ds = FakeDs::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().fallback(ds_handler).with_state(ds.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let client = DsClient::new(format!("http://{address}"));
    let engine = Engine::new(client.clone());
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": { "items": { "columns": { "id": {"type":"int"} }, "primaryKey": "id" } }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    let rec = engine.create_shape(&tref("items"), None, None, false, true).await.unwrap();
    assert_eq!(rec.stream_path, "shape/s1");
    ds.shape_appends.store(0, Ordering::SeqCst); // ignore the (empty) backfill accounting
    (engine, client, ds)
}

fn envelope() -> Envelope {
    Envelope {
        type_: "public.items".to_string(),
        key: "1".to_string(),
        value: Some(serde_json::json!({ "id": 1 })),
        old: None,
        headers: EnvelopeHeaders {
            operation: "upsert".to_string(),
            txid: None,
            offset: None,
            lsn: None,
            seq: None,
            last: Some(true),
        },
    }
}

/// A one-off `404` about a stream that is still there must not lose the change: the engine's
/// reconciler HEADs the stream, finds it, and the append is retried until it lands.
#[tokio::test(flavor = "multi_thread")]
async fn a_false_404_on_a_live_shape_retries_until_the_batch_lands() {
    let (engine, client, ds) = engine_with_one_shape().await;

    ds.false_404.store(true, Ordering::SeqCst);
    let landed = client.append_reliable("shape/s1", &[envelope()]).await;

    assert!(landed, "a false 404 must not be reported as a retired stream");
    assert_eq!(ds.shape_appends.load(Ordering::SeqCst), 1, "the batch must reach the stream");
    assert!(!ds.false_404.load(Ordering::SeqCst), "the fault must have fired");
    assert!(engine.get_shape("s1").await.is_some(), "the shape must still be registered");
    assert!(ds.deleted.lock().unwrap().is_empty(), "nothing may be retired over a false 404");
}

/// The restore-path append reconciles a terminal answer the same way. Its ERROR fails the catalog
/// restore (ADR-0009), so a false `404` there would cost a whole boot attempt — one HEAD tells it
/// apart from a real one.
#[tokio::test(flavor = "multi_thread")]
async fn a_false_404_during_a_restore_append_does_not_cost_the_shape() {
    let (engine, client, ds) = engine_with_one_shape().await;
    let shutdown = engine.shutdown_token();

    ds.false_404.store(true, Ordering::SeqCst);
    client
        .append_retrying("shape/s1", &[envelope()], Duration::from_secs(5), &shutdown)
        .await
        .expect("a false 404 must not fail a restore append");

    assert_eq!(ds.shape_appends.load(Ordering::SeqCst), 1, "the seed must reach the stream");
    assert!(engine.get_shape("s1").await.is_some(), "the shape must survive");
}

/// ...and a real one still fails, typed as the stream being gone, so the restore can tell it from a
/// failure: the boot retries, and the retried restore's stream check retires the shape.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_404_during_a_restore_append_fails_so_the_caller_retires() {
    let (engine, client, ds) = engine_with_one_shape().await;
    let shutdown = engine.shutdown_token();

    ds.stream_lost.store(true, Ordering::SeqCst);
    let err = client
        .append_retrying("shape/s1", &[envelope()], Duration::from_secs(5), &shutdown)
        .await
        .expect_err("a stream storage does not have cannot be appended to");

    assert!(electric_circuits_engine::ds::is_stream_gone(&err), "unexpected error: {err:#}");
    assert_eq!(ds.shape_appends.load(Ordering::SeqCst), 0);
}

/// A `404` about a stream storage really has lost is the other branch: the batch cannot land, so
/// the shape is RETIRED (closed, deleted, deregistered) rather than left registered and stale.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_404_retires_the_shape_instead_of_leaving_it_stale() {
    let (engine, client, ds) = engine_with_one_shape().await;

    ds.stream_lost.store(true, Ordering::SeqCst);
    let landed = client.append_reliable("shape/s1", &[envelope()]).await;

    assert!(!landed, "a stream storage does not have cannot be appended to");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while engine.get_shape("s1").await.is_some() {
        assert!(std::time::Instant::now() < deadline, "the lost shape was never retired");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // The record going is the FIRST half of the retirement; the close and delete follow it (the
    // record is removed under the state lock, the stream work happens after). Polling for them
    // rather than asserting the instant the record vanishes is not leniency — asserting on the
    // earlier of two ordered events is simply the wrong moment to look.
    async fn until(what: &str, seen: &std::sync::Mutex<Vec<String>>) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !seen.lock().unwrap().iter().any(|p| p.ends_with("shape/s1")) {
            assert!(std::time::Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    until("retirement deletes the stream (ADR-0007)", &ds.deleted).await;
    until("retirement closes before deleting (ADR-0007) — the close is attempted even on a lost stream", &ds.closed)
        .await;
    assert!(engine.graph().await.shapes.is_empty(), "no shape may survive its lost stream");
}
