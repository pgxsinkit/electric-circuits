//! Creates that must not survive: one whose future is dropped mid-flight (the HTTP client
//! disconnected), and one overtaken by a schema drift on its own table (ADR-0005). Both must give
//! back everything they registered, so the next identical create behaves as if the first never
//! happened.
//!
//! The pause point in both is the create's first await after registration — the stream PUT — held
//! open by a durable-streams stub that parks every PUT on demand. No Postgres: in library mode the
//! backfill is empty, so a plain create that is allowed to finish succeeds.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use electric_circuits_engine::ds::DsClient;
use electric_circuits_engine::engine::Engine;
use electric_circuits_engine::predicate::PredicateJson;
use electric_circuits_engine::schema::Schema;
use electric_circuits_engine::table_ref::TableRef;

/// Table refs in tests: bare names are the `public.<name>` sugar.
fn tref(name: &str) -> TableRef {
    TableRef::parse(name).unwrap()
}

#[derive(Clone, Default)]
struct FakeDs {
    /// While set, every stream PUT parks — the create under test blocks there instead of racing.
    park_puts: Arc<AtomicBool>,
    /// While set, a HEAD on a `shape/*` stream parks. That is the window a JOIN opens when it
    /// verifies the retained shape's stream still exists — the one place `create_shape` releases the
    /// engine-state lock between cloning the table schema and registering against it. Scoped to
    /// shape streams on purpose: the change log's own HEAD (`init_change_log`) must keep working, or
    /// the test's drift injection would deadlock behind its own fault.
    park_heads: Arc<AtomicBool>,
    /// `shape/*` HEADs served (so a test can wait until the create has actually reached the window).
    shape_heads: Arc<std::sync::atomic::AtomicUsize>,
    /// While set, a `shape/*` HEAD answers 404 — storage has lost the retained stream, so the join
    /// retires the stale entry and the create falls through to making a fresh shape.
    head_404: Arc<AtomicBool>,
    deleted: Arc<std::sync::Mutex<Vec<String>>>,
}

async fn ds_handler(State(ds): State<FakeDs>, request: Request) -> Response {
    match *request.method() {
        Method::PUT => {
            while ds.park_puts.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            StatusCode::OK.into_response()
        }
        Method::DELETE => {
            ds.deleted.lock().unwrap().push(request.uri().path().to_string());
            StatusCode::OK.into_response()
        }
        Method::POST => StatusCode::OK.into_response(),
        // The change log's boot walk HEADs the current segment (ADR-0006): present, never closed.
        Method::HEAD => {
            if request.uri().path().starts_with("/shape/") {
                ds.shape_heads.fetch_add(1, Ordering::SeqCst);
                while ds.park_heads.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                if ds.head_404.load(Ordering::SeqCst) {
                    return StatusCode::NOT_FOUND.into_response();
                }
            }
            ([("stream-next-offset", "tip")]).into_response()
        }
        Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn engine_on_fake_ds() -> (Engine, FakeDs) {
    let ds = FakeDs::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().fallback(ds_handler).with_state(ds.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let engine = Engine::new(DsClient::new(format!("http://{address}")));
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": {
            "parent": { "columns": { "id": {"type":"int"}, "active": {"type":"bool"} }, "primaryKey": "id" },
            "child": { "columns": { "id": {"type":"int"}, "parent_id": {"type":"int"} }, "primaryKey": "id" }
        }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    (engine, ds)
}

/// Park the create on the stream PUT, drop its future, then let the PUT through and wait for the
/// detached rollback to retire the shape record.
async fn cancel_create(engine: &Engine, ds: &FakeDs, where_: &PredicateJson) {
    ds.park_puts.store(true, Ordering::SeqCst);
    let cancelled = tokio::time::timeout(
        Duration::from_millis(300),
        engine.create_shape(&tref("child"), Some(where_.clone()), None, false, true),
    )
    .await;
    assert!(cancelled.is_err(), "the create must still be parked when its future is dropped");
    ds.park_puts.store(false, Ordering::SeqCst);
    // The rollback is detached, so poll until BOTH halves have landed: the engine-state entries and
    // the stream itself.
    let deleted = || ds.deleted.lock().unwrap().iter().any(|path| path.ends_with("shape/s1"));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while engine.get_shape("s1").await.is_some() || !deleted() {
        assert!(std::time::Instant::now() < deadline, "the cancelled create was never rolled back");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(engine.graph().await.shapes.is_empty(), "no shape may survive the rollback");
}

/// Plain shape: the second create must make a NEW shape (its own id and stream) rather than join
/// the abandoned one and fail with "creator died before completing".
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_plain_create_lets_the_same_shape_be_created_again() {
    let (engine, ds) = engine_on_fake_ds().await;
    let where_: PredicateJson =
        serde_json::from_value(serde_json::json!({"col":"parent_id","op":"eq","value":1})).unwrap();

    cancel_create(&engine, &ds, &where_).await;

    let rec = engine
        .create_shape(&tref("child"), Some(where_), None, false, true)
        .await
        .expect("the identical create must succeed once the cancelled one is rolled back");
    assert_ne!(rec.id, "s1", "the recreate must not reuse the abandoned shape");
    assert_eq!(engine.graph().await.shapes.len(), 1);
}

/// Subquery shape: the same rollback, on the path whose registration also spans the cross-table
/// registry. Library mode has no Postgres to seed the inner set from, so the recreate cannot
/// finish — but it must fail on its OWN missing Postgres, not on the abandoned create's share
/// entry, and it must never see a "subquery create conflict" from leftover registry state.
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_subquery_create_does_not_poison_the_next_one() {
    let (engine, ds) = engine_on_fake_ds().await;
    let where_: PredicateJson = serde_json::from_value(serde_json::json!({
        "col": "parent_id",
        "in": { "table": "parent", "project": "id", "where": {"col":"active","op":"eq","value":true} }
    }))
    .unwrap();

    cancel_create(&engine, &ds, &where_).await;

    let err = engine
        .create_shape(&tref("child"), Some(where_), None, false, true)
        .await
        .expect_err("library mode cannot seed a subquery shape")
        .to_string();
    assert!(err.contains("postgres"), "unexpected failure: {err}");
    assert!(engine.subquery_stats().await.is_empty(), "no registry node may survive");
}

/// A create that registered, then had its table's schema swapped underneath it while it was out at
/// Postgres, must roll that attempt back and retry — not install against a schema that is already
/// gone.
///
/// The drift is driven by bumping the table's schema generation alone (`force_schema_generation_bump`),
/// which is precisely what `retire_dependents` does in its enumeration critical section: it isolates
/// the closing check from the retirement that normally accompanies it.
#[tokio::test(flavor = "multi_thread")]
async fn a_create_overtaken_by_a_schema_drift_retries_after_rolling_back() {
    let (engine, ds) = engine_on_fake_ds().await;
    let where_: PredicateJson =
        serde_json::from_value(serde_json::json!({"col":"parent_id","op":"eq","value":1})).unwrap();

    // Park the create just past its registration.
    ds.park_puts.store(true, Ordering::SeqCst);
    let creating = tokio::spawn({
        let engine = engine.clone();
        let where_ = where_.clone();
        async move { engine.create_shape(&tref("child"), Some(where_), None, false, true).await }
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while engine.get_shape("s1").await.is_none() {
        assert!(std::time::Instant::now() < deadline, "the create never registered");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // The table drifts while the create is out at storage/Postgres.
    engine.force_schema_generation_bump(&tref("child")).await;
    ds.park_puts.store(false, Ordering::SeqCst);

    let rec = creating.await.unwrap().expect("a valid create whose first attempt raced drift must retry");
    assert_ne!(rec.id, "s1", "a rolled-back shape id is never re-minted");

    // The raced attempt leaves nothing behind; only its replacement survives.
    let deleted = || ds.deleted.lock().unwrap().iter().any(|path| path.ends_with("shape/s1"));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while engine.get_shape("s1").await.is_some() || !deleted() {
        assert!(std::time::Instant::now() < deadline, "the raced create was never rolled back");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(engine.graph().await.shapes.len(), 1, "only the retried shape may survive");
    assert!(engine.subquery_stats().await.is_empty(), "no registry node may survive");
}

/// The other half of the same guarantee, driven by a REAL retirement rather than a bare generation
/// bump: a create parked mid-flight when the drift enumerates its table is purged by that
/// enumeration and then retries itself with a fresh shape, leaving nothing from the raced attempt
/// behind.
#[tokio::test(flavor = "multi_thread")]
async fn a_real_retirement_over_a_parked_create_retries_without_leaking_the_raced_attempt() {
    let (engine, ds) = engine_on_fake_ds().await;
    let where_: PredicateJson =
        serde_json::from_value(serde_json::json!({"col":"parent_id","op":"eq","value":1})).unwrap();

    ds.park_puts.store(true, Ordering::SeqCst);
    let creating = tokio::spawn({
        let engine = engine.clone();
        let where_ = where_.clone();
        async move { engine.create_shape(&tref("child"), Some(where_), None, false, true).await }
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while engine.get_shape("s1").await.is_none() {
        assert!(std::time::Instant::now() < deadline, "the create never registered");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // The real enumerate-bump-purge sequence, under the table's resolve lock.
    engine.force_retire_dependents(&tref("child")).await;
    ds.park_puts.store(false, Ordering::SeqCst);

    let rec = creating.await.unwrap().expect("a valid create whose first attempt was retired must retry");
    assert_ne!(rec.id, "s1", "the retired id must not be re-minted");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while engine.get_shape("s1").await.is_some() {
        assert!(std::time::Instant::now() < deadline, "the shape record outlived the retirement");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ds.deleted.lock().unwrap().iter().any(|p| p.ends_with("shape/s1")), "the stream must be gone");
    assert_eq!(engine.graph().await.shapes.len(), 1, "only the retry may survive");
}

/// A create that arrives WHILE a resolution holds the table's lock is refused outright — it would
/// otherwise register after the enumeration, pass its own closing check, and be orphaned.
#[tokio::test(flavor = "multi_thread")]
async fn a_create_arriving_during_a_resolution_is_refused() {
    let (engine, _ds) = engine_on_fake_ds().await;
    let where_: PredicateJson =
        serde_json::from_value(serde_json::json!({"col":"parent_id","op":"eq","value":1})).unwrap();

    let lock = engine.force_resolve_lock(&tref("child")).await;
    let err = engine
        .create_shape(&tref("child"), Some(where_.clone()), None, false, true)
        .await
        .expect_err("a create during a resolution must be refused")
        .to_string();
    assert!(err.contains("being resolved"), "unexpected refusal: {err}");
    assert!(engine.get_shape("s1").await.is_none(), "a refused create must register nothing");

    drop(lock);
    engine
        .create_shape(&tref("child"), Some(where_), None, false, true)
        .await
        .expect("the create succeeds once the resolution has finished");
}

/// **`ts` and `gens` must come from one critical section.** A join verifies that the retained
/// shape's stream still exists, and that `HEAD` is the one point where `create_shape` releases the
/// engine-state lock between cloning the table schema and registering against it. A drift resolution
/// completing inside that window must not leave the create holding a pre-drift schema while its
/// closing check compares post-drift generations: both checks would pass and the shape would be
/// installed against a schema the table no longer has — a compiled predicate whose column indices
/// belong to a layout that is gone.
///
/// The window is driven the way reality drives it: storage has also lost the retained stream, so the
/// stale entry is retired and this create falls through to the CREATE path — the one that compiles
/// the predicate from the cloned `ts`. The drift (compiled schema swapped so the predicate's column
/// is DROPPED, generation bumped, exactly what a resolution does) lands while it is parked. With the
/// generations captured alongside `ts` the closing check sees the bump and refuses; capturing them
/// after the relock made both checks agree with each other and disagree with reality, and the create
/// returned a handle for a shape filtering on a column that no longer exists.
#[tokio::test(flavor = "multi_thread")]
async fn a_drift_inside_the_join_head_window_is_not_installed_against_the_stale_schema() {
    let (engine, ds) = engine_on_fake_ds().await;
    let where_: PredicateJson =
        serde_json::from_value(serde_json::json!({"col":"parent_id","op":"eq","value":1})).unwrap();

    // A live, Ready shape under this signature — that is what makes the next identical create take
    // the join path (and therefore the HEAD).
    engine
        .create_shape(&tref("child"), Some(where_.clone()), None, false, true)
        .await
        .expect("the first create must succeed in library mode");

    // Park the identical create inside the join's stream-liveness HEAD.
    let heads_before = ds.shape_heads.load(Ordering::SeqCst);
    ds.head_404.store(true, Ordering::SeqCst);
    ds.park_heads.store(true, Ordering::SeqCst);
    let creating = tokio::spawn({
        let engine = engine.clone();
        let where_ = where_.clone();
        async move { engine.create_shape(&tref("child"), Some(where_), None, false, true).await }
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while ds.shape_heads.load(Ordering::SeqCst) == heads_before {
        assert!(std::time::Instant::now() < deadline, "the create never reached the HEAD window");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // The drift, inside the window: new compiled schema in place, generation bumped.
    let drifted: Schema = serde_json::from_value(serde_json::json!({
        "tables": {
            "parent": { "columns": { "id": {"type":"int"}, "active": {"type":"bool"} }, "primaryKey": "id" },
            "child": { "columns": { "id": {"type":"int"} }, "primaryKey": "id" }
        }
    }))
    .unwrap();
    engine.define_schema(&drifted).await.unwrap();
    engine.force_schema_generation_bump(&tref("child")).await;
    ds.park_heads.store(false, Ordering::SeqCst);

    let err = creating
        .await
        .unwrap()
        .expect_err("the retry must validate against the current schema, not install against the old one")
        .to_string();
    assert!(err.contains("parent_id"), "unexpected refusal: {err}");

    // Nothing survives: the stale join target was retired by the HEAD, and the refused create rolled
    // itself back rather than leaving a shape behind.
    assert!(engine.graph().await.shapes.is_empty(), "no shape may survive the refusal");

    // ...and this is what the stale `ts` was hiding: against the schema the table ACTUALLY has now,
    // the definition does not even compile.
    let err = engine
        .create_shape(&tref("child"), Some(where_), None, false, true)
        .await
        .expect_err("the predicate names a column the drifted schema dropped")
        .to_string();
    assert!(err.contains("parent_id"), "unexpected failure on the current schema: {err}");
}
