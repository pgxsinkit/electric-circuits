//! Integration tests for the fleet HTTP surface added to the engine router: `/v1/health` (state
//! machine + exact body + status codes + cache headers), `GET /` (200 empty), the
//! `OPTIONS /v1/shape` CORS preflight, and the liveness/readiness split (`/health` vs `/ready`).
//! The router is driven in-process via `Service::oneshot`; no Postgres or durable-streams server is
//! needed (the health phase is set at Engine construction).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use electric_circuits_engine::ds::DsClient;
use electric_circuits_engine::engine::Engine;
use electric_circuits_engine::http::router;
use tower::ServiceExt; // for `oneshot`

fn library_engine() -> Engine {
    Engine::new(DsClient::new("http://127.0.0.1:1"))
}

async fn body_string(res: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn health_active_in_library_mode() {
    let res = router(library_engine())
        .oneshot(Request::builder().uri("/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("cache-control").unwrap(), "no-cache, no-store, must-revalidate");
    assert_eq!(res.headers().get("content-type").unwrap(), "application/json");
    assert_eq!(body_string(res).await, r#"{"status":"active"}"#);
}

#[tokio::test]
async fn health_waiting_returns_202_in_pg_mode_before_setup() {
    // new_pg starts `waiting`; without setup_postgres it stays there.
    let engine = Engine::new_pg(DsClient::new("http://127.0.0.1:1"), "postgres://x/y".into());
    assert_eq!(engine.health_status(), "waiting");
    let res = router(engine).oneshot(Request::builder().uri("/v1/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    assert_eq!(body_string(res).await, r#"{"status":"waiting"}"#);
}

#[tokio::test]
async fn root_returns_200_empty() {
    let res = router(library_engine()).oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(body_string(res).await.is_empty());
}

#[tokio::test]
async fn options_shape_is_cors_preflight() {
    let res = router(library_engine())
        .oneshot(Request::builder().method("OPTIONS").uri("/v1/shape").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(res.headers().get("access-control-allow-methods").unwrap(), "GET, POST, HEAD, DELETE, OPTIONS");
}

/// `GET /ready` is the probe a load balancer gates on: 200 only when the engine is actually able
/// to serve. Library mode has nothing to wait for, so it is ready from construction.
#[tokio::test]
async fn ready_is_200_active_in_library_mode() {
    let res =
        router(library_engine()).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("cache-control").unwrap(), "no-cache, no-store, must-revalidate");
    assert_eq!(body_string(res).await, r#"{"status":"active"}"#);
}

/// Postgres mode before `setup_postgres`: NOT ready (503 `waiting`), while `/v1/health` answers its
/// fleet-parity 202 for the same phase. The two probes are deliberately different contracts.
#[tokio::test]
async fn ready_is_503_waiting_before_postgres_is_up() {
    let engine = Engine::new_pg(DsClient::new("http://127.0.0.1:1"), "postgres://x/y".into());
    assert_eq!(engine.readiness_status(), "waiting");
    let res = router(engine).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_string(res).await, r#"{"status":"waiting"}"#);
}

/// A degraded engine is live but not ready — and `/health` must NOT go with it, or a kubelet would
/// restart a pod whose problem a restart is the documented fix for only when an operator says so.
#[tokio::test]
async fn degraded_is_not_ready_but_is_still_live() {
    let engine = library_engine();
    engine.force_degraded();
    assert_eq!(engine.readiness_status(), "degraded");

    let res =
        router(engine.clone()).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_string(res).await, r#"{"status":"degraded"}"#);

    let res = router(engine).oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "liveness must not follow readiness");
    assert_eq!(body_string(res).await, "ok");
}

/// The first thing a `SIGTERM` does: `/ready` turns 503 `shutting_down` so a load balancer drains
/// the pod BEFORE anything is wound down. Liveness and `/v1/health` are untouched — the process is
/// still perfectly able to answer what it already accepted.
#[tokio::test]
async fn shutdown_makes_ready_503_before_anything_else_changes() {
    let engine = library_engine();
    engine.shutdown_token().begin();
    assert_eq!(engine.readiness_status(), "shutting_down");
    assert_eq!(engine.health_status(), "active", "shutdown must not rewrite the boot phase");

    let res =
        router(engine.clone()).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_string(res).await, r#"{"status":"shutting_down"}"#);

    let res =
        router(engine.clone()).oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = router(engine).oneshot(Request::builder().uri("/v1/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "the fleet healthcheck is unchanged by shutdown");
    assert_eq!(body_string(res).await, r#"{"status":"active"}"#);
}

/// A NEW `live=true` request during the drain is answered `503` + `Retry-After: 1`, not an empty
/// 204. Electric's client re-polls a 204 immediately, so 204 would turn the drain window into a
/// tight poll loop for every live subscriber; a 5xx is what it backs off on.
#[tokio::test]
async fn a_new_live_poll_during_the_drain_is_told_to_come_back() {
    let engine = library_engine();
    engine.shutdown_token().begin();
    let res = router(engine)
        .oneshot(
            Request::builder().uri("/v1/shape?table=items&handle=h1&offset=0_0&live=true").body(Body::empty()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(res.headers().get("retry-after").unwrap(), "1");
    assert!(body_string(res).await.contains("shutting down"));
}

/// ...and a NON-live request is not: the drain window exists precisely so requests the engine has
/// already accepted still get served.
#[tokio::test]
async fn a_non_live_request_during_the_drain_is_served_normally() {
    let engine = library_engine();
    engine.shutdown_token().begin();
    let res = router(engine)
        .oneshot(Request::builder().uri("/v1/shape?table=items&offset=-1").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "only live polls are turned away");
}

#[tokio::test]
async fn legacy_health_still_ok() {
    let res =
        router(library_engine()).oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_string(res).await, "ok");
}

/// `DELETE /table/{table}/rows` is registered and validates its input: an unknown table is a 400
/// with an `error` body (not a 404/405, which would mean the route or method is missing).
#[tokio::test]
async fn delete_table_rows_rejects_unknown_table() {
    let res = router(library_engine())
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/table/nope/rows")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"keys":[{"id":1}]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(body_string(res).await.contains("unknown table"));
}

const INTROSPECTION_ROUTES: &[&str] = &["/trace", "/graph", "/graph/node", "/state", "/state/node"];

/// `ELECTRIC_CIRCUITS_TRACE=0` (introspection off) removes the visualizer/introspection surface entirely
/// — the routes are never registered, so `/trace` can never gain a subscriber and the hot path
/// keeps its zero-subscriber fast path. Everything else keeps serving.
#[tokio::test]
async fn introspection_disabled_unregisters_viz_routes() {
    use electric_circuits_engine::http::router_with_introspection;
    for route in INTROSPECTION_ROUTES {
        let res = router_with_introspection(library_engine(), false)
            .oneshot(Request::builder().uri(*route).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND, "{route} should be unregistered");
    }
    // The rest of the surface is untouched.
    let res = router_with_introspection(library_engine(), false)
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

/// Default (`router`, introspection on): the same routes respond (200, or 400 for the two that
/// require a query param — anything but 404 proves registration).
#[tokio::test]
async fn introspection_enabled_by_default() {
    for route in INTROSPECTION_ROUTES {
        let res = router(library_engine())
            .oneshot(Request::builder().uri(*route).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_ne!(res.status(), StatusCode::NOT_FOUND, "{route} should be registered");
    }
}

/// A degraded engine has lost membership effects it cannot re-derive, so every route that would
/// answer WITH membership must refuse (503 + the typed error body) rather than serve what the
/// engine knows is wrong — while the observability surface stays up, because that is what an
/// operator needs to see the failure and decide to restart.
#[tokio::test]
async fn degraded_refuses_the_membership_routes_and_keeps_observability_up() {
    let engine = library_engine();
    engine.force_degraded();
    let call = async |method: &str, uri: &str, body: &'static str| {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        router(engine.clone()).oneshot(req).await.unwrap()
    };

    for (method, uri, body) in [
        ("POST", "/shapes", r#"{"table":"t"}"#),
        ("POST", "/aggregate", r#"{"table":"t","fn":"count"}"#),
        ("POST", "/query", r#"{"table":"t"}"#),
        ("GET", "/shapes/s1", ""),
        ("GET", "/shapes/s1/rows", ""),
        ("GET", "/shapes/s1/log", ""),
        ("GET", "/v1/shape?table=t&offset=-1", ""),
    ] {
        let res = call(method, uri, body).await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{method} {uri} must refuse");
        assert_eq!(
            body_string(res).await,
            r#"{"error":"degraded: subquery membership effects were lost; restart required"}"#,
            "{method} {uri} body"
        );
    }

    // `/v1/health` reports the state (503 + `degraded`), and the barrier endpoint still answers so
    // the held `pendingFlips` and the `flipFailures` count are readable.
    let res = call("GET", "/v1/health", "").await;
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_string(res).await, r#"{"status":"degraded"}"#);

    let res = call("GET", "/replication/lsn", "").await;
    assert_eq!(res.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_string(res).await).unwrap();
    assert_eq!(v["flipFailures"], 1);

    for uri in ["/metrics", "/memory", "/subqueries", "/graph", "/state", "/health"] {
        assert_eq!(call("GET", uri, "").await.status(), StatusCode::OK, "{uri} must stay up");
    }
}

/// Postgres mode before the boot resolves (ADR-0009): every route that would create, join,
/// reactivate, release or purge a shape — or report on one the catalog restore has not installed
/// yet — answers 503 with `Retry-After`, never the 404/400/500 a not-yet-restored registry would
/// otherwise produce. A create here would spawn a sequencer that reads and checkpoints past the
/// backlog before the restore registers its shapes, or mint an id a catalog record still owns.
/// Library mode has no boot and is unaffected.
#[tokio::test]
async fn a_booting_engine_refuses_shape_mutations_with_retry_after() {
    let engine = Engine::new_pg(DsClient::new("http://127.0.0.1:1"), "postgres://x/y".into());
    let call = async |engine: &Engine, method: &str, uri: &str, body: &'static str| {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        router(engine.clone()).oneshot(req).await.unwrap()
    };

    for (method, uri, body) in [
        ("POST", "/shapes", r#"{"table":"items"}"#),
        ("POST", "/aggregate", r#"{"table":"items","fn":"count"}"#),
        ("POST", "/query", r#"{"table":"items"}"#),
        ("GET", "/shapes/s1", ""),
        ("GET", "/shapes/s1/rows", ""),
        ("GET", "/shapes/s1/log", ""),
        ("DELETE", "/shapes/s1", ""),
        ("DELETE", "/shapes/s1?purge=true", ""),
        ("GET", "/v1/shape?table=items&offset=-1", ""),
    ] {
        let res = call(&engine, method, uri, body).await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{method} {uri} must wait for the boot");
        assert_eq!(res.headers().get("retry-after").map(|v| v.to_str().unwrap()), Some("1"), "{method} {uri}");
        assert!(body_string(res).await.contains("still booting"), "{method} {uri} names the reason");
    }
    assert!(engine.ensure_booted().is_err());

    // `POST /schema` is not a boot-time question at all in Postgres mode: the schema is Postgres's.
    // Refused with 409 whatever the boot phase, and without touching anything.
    let res = call(
        &engine,
        "POST",
        "/schema",
        r#"{"schema":{"tables":{"items":{"columns":{"id":{"type":"int"}},"primaryKey":"id"}}}}"#,
    )
    .await;
    assert_eq!(res.status(), StatusCode::CONFLICT);
    assert!(body_string(res).await.contains("Postgres mode"));

    // Library mode: no boot to wait for, so the same create gets past the gate (and fails on its own
    // terms — no such table — rather than being told to come back).
    let res = call(&library_engine(), "POST", "/shapes", r#"{"table":"items"}"#).await;
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(library_engine().ensure_booted().is_ok());
}
