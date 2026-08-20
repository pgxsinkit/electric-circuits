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
