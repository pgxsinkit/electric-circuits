//! The fan-out barrier across subquery-shape creation.
//!
//! From registration until install, a create BUFFERS the deltas that land — outer rows on the
//! pending shape, inner rows on each fresh node — so their membership effects are not on any
//! stream yet. The engine holds the frontier barrier across that window, which makes every exit
//! path from a create load-bearing: a hold that is not released stalls the watermark for every
//! consumer of the engine, forever, and the only symptom is a `sequencedLsn` that stops moving.
//!
//! Driven with a fake durable-streams server and no Postgres, so the create fails in phase B —
//! the path that has to release the hold without ever reaching phase C.

use axum::Router;
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use electric_circuits_engine::ds::DsClient;
use electric_circuits_engine::engine::Engine;
use electric_circuits_engine::schema::Schema;
use std::time::Duration;

/// PUT/POST/DELETE succeed; every GET is an empty, up-to-date page (long-polls included, so the
/// sequencer spins harmlessly instead of parking).
async fn ds_handler(req: Request) -> Response {
    match *req.method() {
        Method::PUT | Method::POST | Method::DELETE => StatusCode::OK.into_response(),
        Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn spawn_fake_ds() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, Router::new().fallback(ds_handler)).await;
    });
    format!("http://{addr}")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_subquery_create_releases_its_barrier_hold() {
    let ds_url = spawn_fake_ds().await;
    // Library mode: no Postgres, so phase B fails at the first seeding query.
    let engine = Engine::new(DsClient::new(&ds_url));
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": {
            "outer_t": { "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id" },
            "inner_t": { "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id" }
        }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();

    let where_json = serde_json::from_value(serde_json::json!({
        "col": "gid", "in": { "table": "inner_t", "project": "gid" }
    }))
    .unwrap();
    let err = engine
        .create_shape("outer_t", Some(where_json), None, false, false)
        .await
        .expect_err("a subquery create without Postgres cannot seed");
    assert!(format!("{err:#}").contains("postgres"), "expected a seeding failure, got: {err:#}");

    assert_eq!(
        engine.pending_flips(),
        0,
        "a create that failed in phase B must give its barrier hold back — a leaked hold freezes \
         the fan-out frontier for every consumer, permanently and silently"
    );
    assert_eq!(engine.sequenced_lsn(), "0/0");
    assert_eq!(engine.flip_failures(), 0, "a failed create is not lost membership work");
}

/// **A cancelled create must give everything back.** `create_shape` is awaited straight from the
/// HTTP handler, so a client that disconnects mid-create drops the future — during the conflict
/// sleep, a Postgres round-trip, a stream append, or phase C. Nothing runs the enumerated cleanup
/// paths on that route. Two things would outlive the drop: the barrier permit, which stops the
/// watermark for every consumer of the engine with no other symptom, and the registry's pending
/// shape, whose fresh nodes keep buffering inner-table deltas forever for a create that will never
/// install.
///
/// Driven against a Postgres that accepts the connection and then says nothing, so the create is
/// genuinely parked inside phase B when the future is taken away.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_subquery_create_gives_back_its_barrier_and_its_registration() {
    let ds_url = spawn_fake_ds().await;
    // A black hole: accepts the TCP connection and never answers the startup message.
    let pg = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pg_addr = pg.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = pg.accept().await {
            held.push(sock); // keep it open, answer nothing
        }
    });

    let engine = Engine::new_pg(
        DsClient::new(&ds_url),
        format!("postgres://u:p@{pg_addr}/db?connect_timeout=60"),
    );
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": {
            "outer_t": { "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id" },
            "inner_t": { "columns": { "id": {"type":"int"}, "gid": {"type":"int"} }, "primaryKey": "id" }
        }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();

    let where_json = serde_json::from_value(serde_json::json!({
        "col": "gid", "in": { "table": "inner_t", "project": "gid" }
    }))
    .unwrap();
    // Phase A registers, phase B parks on the handshake — then the client goes away.
    let cancelled = tokio::time::timeout(
        Duration::from_millis(400),
        engine.create_shape("outer_t", Some(where_json), None, false, false),
    )
    .await;
    assert!(cancelled.is_err(), "the create must still be in phase B when it is dropped");

    // The rollback is detached (it needs the registry lock, which `drop` cannot await).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !engine.subquery_stats().await.is_empty()
        || engine.get_shape("s1").await.is_some()
        || engine.pending_flips() != 0
    {
        assert!(std::time::Instant::now() < deadline, "a cancelled create did not finish rolling back");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        engine.get_shape("s1").await.is_none(),
        "a cancelled create must remove the outer shape record as well as its subquery nodes"
    );
    assert!(
        engine.graph().await.shapes.is_empty(),
        "a cancelled create must not remain visible through graph/introspection state"
    );
    assert_eq!(
        engine.pending_flips(),
        0,
        "a cancelled create must give its barrier permit back — a leaked one freezes the fan-out \
         frontier for every consumer, permanently and silently"
    );
    assert_eq!(engine.sequenced_lsn(), "0/0");
}
