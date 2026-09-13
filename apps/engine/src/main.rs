//! electric-circuits engine binary: a durable-streams client that incrementally maintains shapes
//! (key routing + stateless predicate evaluation over Z-set deltas).
//!
//! Boot configuration is resolved from the environment by [`electric_circuits_engine::config`], which maps
//! the benchmarking-fleet's `ELECTRIC_*` / `DATABASE_URL` surface onto the engine's `ELECTRIC_CIRCUITS_*`
//! internals (the latter still win, preserving the dev/test workflow). The durable-streams base URL
//! comes from `ELECTRIC_CIRCUITS_DS_URL`; the engine binds `0.0.0.0:$ELECTRIC_PORT` (default 3000 under the
//! fleet, `127.0.0.1:0` in dev). Two stdout lines are the discovery channel: `ENGINE_BINDING <url>`
//! when the port is open (before Postgres is contacted, so `GET /ready` is answerable while the boot
//! is still retrying) and `ENGINE_LISTENING <url>` once the boot has RESOLVED.
//!
//! ## Exit codes
//!
//! | code | meaning |
//! |---|---|
//! | `0` | clean exit: a graceful shutdown completed inside its grace period |
//! | `70` | the shutdown was **forced** — a second signal arrived, or the grace period elapsed with work still in flight ([`shutdown::EXIT_SHUTDOWN_FORCED`]). A catalog append being retried through an outage is a named party here |
//! | `71` | the shutdown was **incomplete**: every task finished, but the durable catalog writer did not drain ([`shutdown::EXIT_SHUTDOWN_INCOMPLETE`]) — the final checkpoint may be missing, so the next boot may replay from an earlier one |
//! | `74` | the durable catalog **refused** an event (`EX_IOERR`): storage answered, and the answer will not change — or the catalog writer task itself panicked. Memory and storage disagree and only a re-fold at boot can reconcile them, so the process exits instead of serving state its record does not describe |
//! | `75` | a counts pipeline must be rebuilt (schema drift or an epoch reset on a circuit-served table); restart to re-seed it |
//! | `78` | **boot refused** (`EX_CONFIG`): a misconfiguration retrying cannot fix — a setting the config resolver rejected (an unparseable `ELECTRIC_CIRCUITS_PG_URL`, an unusable `ELECTRIC_CIRCUITS_PG_TABLES`, an out-of-range byte budget, a missing `ELECTRIC_CIRCUITS_DS_URL`, an unwritable spill directory), or a fatal Postgres condition — bad credentials, a missing privilege, an unknown database, `wal_level` ≠ `logical`, a publication with a column list, an unreadable durable catalog |
//!
//! A **retryable** Postgres condition — connection refused, DNS, a timeout, "the database system is
//! starting up" — is not an exit at all: the boot backs off (1 s → 30 s, jittered) and tries again
//! forever, answering `GET /ready` with `503 {"status":"waiting"}` throughout. Kubernetes gates
//! traffic on readiness, so a restart buys nothing an operator would want.

use std::future::IntoFuture;
use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result};
use electric_circuits_engine::config::{self, Config};
use electric_circuits_engine::ds::DsClient;
use electric_circuits_engine::engine::Engine;
use electric_circuits_engine::shutdown::{self, ShutdownToken};
use electric_circuits_engine::{pg, statsd};

#[tokio::main]
async fn main() -> Result<()> {
    // Anchor process-uptime / boot-to-ready timing before anything else runs.
    statsd::mark_start();

    // Configuration is resolved before tracing exists (the log filter comes out of it), so a
    // refusal here writes to stderr itself — and exits 78 like every other boot refusal, rather
    // than the anyhow default of 1. An operator reading `kubectl describe` should not have to know
    // which side of `init_tracing` a bad setting was caught on.
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => refuse_boot("configuration", &e),
    };
    init_tracing(&config.log_filter);

    // Unknown ELECTRIC_* vars are accepted, never fatal — surface them once so operators can see the
    // image tolerated (and ignored) them.
    if !config.noop_vars.is_empty() {
        tracing::info!("accepted (no-op) ELECTRIC_* vars: {}", config.noop_vars.join(", "));
    }
    tracing::info!("resolved config: {}", config.redacted());

    // Publish request-path globals (instance id, stack id, /v1/shape secret) and wire up StatsD.
    config::set_globals(&config.instance_id, &config.stack_id, config.secret.as_deref());
    if let Some(target) = &config.statsd {
        statsd::init(target, &config.instance_id);
    }
    if config.prometheus_port.is_some() {
        tracing::info!(
            "ELECTRIC_PROMETHEUS_PORT is set, but the dedicated Prometheus listener is not implemented; \
             /metrics/prometheus stays on the main port"
        );
    }

    let Some(ds_url) = config.ds_url.clone() else {
        refuse_boot(
            "configuration",
            &anyhow::anyhow!("ELECTRIC_CIRCUITS_DS_URL must be set to the durable-streams server base URL"),
        )
    };

    // Large transactions spill to disk (ADR-0003), so the spill directory must exist, be private
    // and be writable NOW. A spill that fails mid-commit tears the replication connection down and
    // is retried forever against the same broken directory — an ingest stall nobody sees — so this
    // is boot-fatal, like an unusable ELECTRIC_CIRCUITS_PG_TABLES entry. (Kept out of
    // `Config::resolve`, which is a pure function of an env getter.)
    if let Err(e) = config.txn.probe().context("checking the large-transaction spill directory") {
        refuse_boot("configuration", &e);
    }

    // TEST-ONLY: surface an injected fault so a faulted run is never silent (no-op when unset).
    if electric_circuits_engine::fault::active() != electric_circuits_engine::fault::Fault::None {
        tracing::warn!("ELECTRIC_CIRCUITS_FAULT active: {:?}", electric_circuits_engine::fault::active());
    }

    // Size the shared Postgres pool (backfills, query-backs, subset queries) and publish the
    // backfill streaming budget before first use.
    electric_circuits_engine::pg::set_pool_size(config.db_pool_size);
    electric_circuits_engine::pg::set_backfill_config(config.backfill);

    // Postgres mode: data lives in Postgres, ingested via logical replication and read back for
    // backfill. Enabled by a resolved pg_url (ELECTRIC_CIRCUITS_PG_URL or DATABASE_URL).
    let engine = match &config.pg_url {
        Some(url) if !url.is_empty() => {
            let engine = Engine::new_pg(DsClient::new(ds_url.clone()), url.clone());
            // The dbsp arrangement circuit is mandatory infrastructure — always configured.
            tracing::info!("dbsp arrangements: dir {}", config.dbsp.dir.display());
            engine.set_dbsp_config(config.dbsp.clone());
            // Large transactions (ADR-0003): the ingestor's buffer spills past the memory cap and
            // appends the commit in chunks. Must be set before `setup_postgres` spawns the ingestor.
            engine.set_txn_config(config.txn.clone());
            engine
        }
        _ => {
            // Library mode: no Postgres source; the engine is `active` from construction. Shutdown
            // and readiness still apply — there is simply nothing Postgres-shaped to wait for.
            let engine = Engine::new(DsClient::new(ds_url.clone()));
            statsd::consumers_ready(engine.table_count().await as u64);
            engine
        }
    };

    // Memory probes via OpenTelemetry: register the meter provider + Prometheus exporter, publish an
    // initial sample, and start the background sampler. `_otel` is held for the process lifetime so the
    // provider (and its exporter) stays alive. Exposed at GET /metrics/prometheus and GET /memory.
    let _otel = electric_circuits_engine::mem::init_otel();
    electric_circuits_engine::mem::publish(&engine.mem_cardinalities().await);
    electric_circuits_engine::mem::spawn_sampler(engine.clone(), Duration::from_millis(500));

    // StatsD periodic samplers (no-ops when StatsD is off): system metrics + storage size.
    statsd::spawn_system_sampler(config.metrics_period);
    statsd::spawn_storage_sampler(config.storage_dir.clone());

    let shutdown = engine.shutdown_token();
    // Kept past the router so the Postgres setup and the shutdown path still have a handle.
    let engine_at_exit = engine.clone();
    let boot_engine = engine.clone();
    let app = electric_circuits_engine::http::router_with_introspection(engine, config.trace);

    let listener =
        tokio::net::TcpListener::bind(&config.bind).await.with_context(|| format!("binding {}", config.bind))?;
    let addr = listener.local_addr()?;

    // The HTTP surface comes up BEFORE Postgres, deliberately: `GET /ready` is how an orchestrator
    // learns the engine is still waiting for its database, and a probe it cannot reach is no probe
    // at all. `ENGINE_BINDING` says the port is open; readiness is a separate question.
    println!("ENGINE_BINDING http://{addr}");
    std::io::stdout().flush().ok();
    tracing::info!("electric-circuits engine listening on http://{addr}, ds={ds_url}");

    let serve_shutdown = shutdown.clone();
    let ready_drain = config.shutdown_ready_drain;
    let grace = config.shutdown_grace;
    let serve = tokio::spawn(
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { await_signal_and_begin(serve_shutdown, ready_drain, grace).await })
            .into_future(),
    );

    // Now connect to Postgres, retrying anything retryable while `/ready` reports `waiting`.
    if let Some(url) = config.pg_url.clone().filter(|u| !u.is_empty()) {
        if setup_postgres_until_ready(&boot_engine, &config).await {
            let tables = boot_engine.table_count().await;
            tracing::info!("postgres mode: {tables} table(s), slot '{}', streaming pgoutput", config.slot);
            statsd::consumers_ready(tables as u64);
            // Replication-slot gauges (engine-owned: `/metrics`, `/metrics/prometheus` AND StatsD
            // read the same ~10 s sample, taken on a POOLED connection).
            electric_circuits_engine::metrics::spawn_replication_slot_sampler(
                url,
                config.slot.clone(),
                boot_engine.shutdown_token(),
            );
            // stdout is the discovery channel (logs go to stderr). Printed only once the boot has
            // RESOLVED — a harness that sees this line can create shapes straight away, exactly as
            // before the readiness split.
            println!("ENGINE_LISTENING http://{addr}");
            std::io::stdout().flush().ok();
        }
    } else {
        println!("ENGINE_LISTENING http://{addr}");
        std::io::stdout().flush().ok();
    }

    serve.await.context("http server task")??;

    // The accept loop is closed and every in-flight request has finished (the `/v1/shape` live poll
    // joined the token, so that took milliseconds, not its 20 s window). Now let the engine's own
    // tasks reach their safe points.
    let outcome = finish_shutdown(&engine_at_exit, &shutdown, config.shutdown_grace).await;
    if outcome != shutdown::ShutdownOutcome::Complete {
        std::io::stderr().flush().ok();
        std::process::exit(outcome.exit_code());
    }
    Ok(())
}

/// Connect to Postgres and run the whole setup, retrying **only** what is worth retrying.
///
/// A fatal condition (see [`pg::classify`]) exits [`pg::EXIT_CONFIG`] straight away with a named
/// message: an operator has to act, and a crash-loop with a clear reason beats an infinite retry
/// nobody reads. A retryable one backs off on the ingestor's own schedule (1 s → 30 s, jittered)
/// and tries again for as long as it takes, with `GET /ready` reporting `waiting` throughout (the
/// HTTP surface is already up — see `main`) — the database coming up after the engine is the normal
/// case in a compose/Kubernetes start, not a failure. Returns `false` if a shutdown cut the wait
/// short.
///
/// Retrying the WHOLE setup (rather than only the connect) is deliberate and safe: `setup_postgres`
/// is idempotent — it re-introspects, re-adopts the epoch, and spawns the ingestor at most once —
/// so a connection lost half-way through introspection resumes from the top with no residue. A
/// catalog restore that fails part-way undoes everything it installed before returning (ADR-0009),
/// so the retry re-folds the catalog into a clean registry rather than on top of a partial one.
async fn setup_postgres_until_ready(engine: &Engine, config: &Config) -> bool {
    let shutdown = engine.shutdown_token();
    // Named so a forced exit can say the boot was what it was waiting for (the ingestor and the
    // sequencer do not exist yet at this point, so without this the party list would be empty).
    let _party = shutdown.party("postgres boot");
    let mut attempt: u32 = 0;
    loop {
        // Raced, not just awaited: `setup_postgres` connects, introspects, folds the durable
        // catalog and reads the change log, and any of those can be slow against a server that is
        // coming up. A signal must not have to wait for it to finish — abandoning it costs nothing,
        // because nothing is published until it returns Ok.
        let attempted = tokio::select! {
            biased;
            _ = shutdown.wait() => {
                tracing::info!("boot: shutdown requested during the Postgres setup; abandoning it");
                return false;
            }
            r = engine.setup_postgres(&config.tables, &config.slot) => r,
        };
        let err = match attempted {
            Ok(()) => return true,
            Err(e) => e,
        };
        if pg::boot_disposition(&err) == pg::BootFailure::Fatal {
            refuse_boot(pg::boot_failure_name(&err), &err);
        }
        // Back to `waiting`: the attempt left the phase wherever it failed (`starting`, if it got
        // as far as connecting), and reporting `starting` for the whole backoff would tell an
        // orchestrator the engine is making progress when it is in fact waiting to redial.
        engine.set_waiting();
        attempt = attempt.saturating_add(1);
        let wait = electric_circuits_engine::replication::jitter(
            electric_circuits_engine::replication::backoff_base(attempt.saturating_sub(1)),
            electric_circuits_engine::replication::clock_nanos(),
        );
        tracing::warn!(
            "boot: Postgres not ready ({}); attempt {attempt} failed, retrying in {wait:?}. \
             GET /ready reports `waiting` until it is. Cause: {err:#}",
            pg::boot_failure_name(&err),
        );
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            // A pod terminated while still waiting for its database stops dialling and takes the
            // ordinary shutdown path — a clean exit 0, not a kill.
            _ = shutdown.wait() => {
                tracing::info!("boot: shutdown requested while waiting for Postgres; giving up the connect loop");
                return false;
            }
        }
    }
}

/// The graceful-shutdown trigger handed to `axum::serve`.
///
/// Flipping the token is the FIRST thing that happens — before the accept loop closes, before any
/// task is told anything — so `GET /ready` answers `503 {"status":"shutting_down"}` from that
/// instant and a load balancer takes the pod out of rotation while the engine is still perfectly
/// able to serve whatever it already accepted.
///
/// A **second** signal during the grace period is an operator saying "stop waiting": the process
/// exits [`shutdown::EXIT_SHUTDOWN_FORCED`] immediately, without finishing an in-flight commit
/// (unacknowledged, so Postgres re-delivers it) and without a final checkpoint (the last lazy one
/// stands, so the next boot replays a little more).
async fn await_signal_and_begin(shutdown: ShutdownToken, ready_drain: Duration, grace: Duration) {
    let sig = shutdown::first_signal().await;
    shutdown.begin();
    // The grace is a bound on the PROCESS, not on one await point, so it is armed the instant the
    // token flips and runs on its own task. `finish_shutdown` is the happy path and normally gets
    // there first; this is what makes the bound hold when `main` is somewhere else entirely — a
    // connect to a non-routable address, a catalog fold against storage that is not up, a backfill
    // between chunks. Every one of those now joins the token as well, so this should never fire;
    // "should never fire" is exactly what a watchdog is for.
    let watchdog = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(grace).await;
        let msg = shutdown::grace_expiry_message(&watchdog.outstanding(), grace);
        tracing::error!("{msg}");
        std::io::stderr().flush().ok();
        std::process::exit(shutdown::EXIT_SHUTDOWN_FORCED);
    });
    tracing::info!(
        "{sig} received: draining. GET /ready is now 503 shutting_down; the port stays open for \
         {ready_drain:?} so a load balancer's probe sees it. Streams are left open (a restored shape \
         continues its stream) — send another signal to stop immediately."
    );
    tokio::spawn(async move {
        let sig = shutdown::first_signal().await;
        tracing::warn!(
            "second {sig} during the shutdown grace: exiting {} without finishing. Nothing is \
             corrupted — an unacknowledged commit is re-delivered and the last checkpoint stands.",
            shutdown::EXIT_SHUTDOWN_FORCED
        );
        std::io::stderr().flush().ok();
        std::process::exit(shutdown::EXIT_SHUTDOWN_FORCED);
    });
    // Keep accepting — and answering `/ready` with 503 — for the drain window. Returning here is
    // what closes the accept loop, and closing it instantly would make the readiness probe
    // connection-refuse before any load balancer had a chance to observe the 503.
    if !ready_drain.is_zero() {
        tokio::time::sleep(ready_drain).await;
    }
}

/// Wait out the engine's own tasks after the HTTP server has stopped: the ingestor finishes the
/// commit it is appending, the sequencer finishes its batch, flushes it and writes a final
/// checkpoint, and the catalog writer drains that checkpoint to storage.
///
/// The whole thing is bounded by `grace`. Falling short of it is not silent, and the outcome says
/// how: parties still outstanding are named and the outcome is [`shutdown::ShutdownOutcome::Forced`];
/// a catalog that did not drain is [`shutdown::ShutdownOutcome::CatalogIncomplete`]. `main` exits
/// with the matching code, because "exited 0" must mean "everything got to a clean point".
async fn finish_shutdown(engine: &Engine, shutdown: &ShutdownToken, grace: Duration) -> shutdown::ShutdownOutcome {
    let started = std::time::Instant::now();
    // Measured from the SIGNAL, not from here: the readiness-drain window already spent part of it.
    let left = grace.saturating_sub(shutdown.elapsed().unwrap_or_default());
    if !shutdown.wait_for_parties(left).await {
        tracing::error!(
            "shutdown grace of {grace:?} elapsed with {:?} still running; exiting {}. \
             Raise ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS if a commit of this size needs longer.",
            shutdown.outstanding(),
            shutdown::EXIT_SHUTDOWN_FORCED
        );
        return shutdown::ShutdownOutcome::Forced;
    }
    // The sequencer's final `Offset` is queued, not written: draining is what makes the checkpoint
    // durable, and with it the restart point the next boot resumes from.
    let catalog_budget = grace.saturating_sub(shutdown.elapsed().unwrap_or_default()).min(shutdown::CATALOG_DRAIN);
    if !engine.drain_catalog(catalog_budget).await {
        tracing::error!(
            "the durable catalog writer did not drain within {catalog_budget:?}; the final checkpoint may be \
             missing and the next boot will replay from the previous one. Exiting {}.",
            shutdown::EXIT_SHUTDOWN_INCOMPLETE
        );
        return shutdown::ShutdownOutcome::CatalogIncomplete;
    }
    tracing::info!("shutdown complete in {:?}", started.elapsed());
    shutdown::ShutdownOutcome::Complete
}

/// Refuse the boot: name the class of failure, print the whole error chain, exit [`pg::EXIT_CONFIG`].
///
/// Used for everything an operator has to fix before the engine can run — a setting the config
/// resolver rejected, a spill directory it cannot write, a Postgres condition [`pg::classify`]
/// called fatal. `tracing` may not be initialised yet (the log filter is itself configuration), so
/// this writes to stderr unconditionally as well as logging.
fn refuse_boot(kind: &str, e: &anyhow::Error) -> ! {
    let msg =
        format!("boot refused ({kind}): {e:#}. This will not be fixed by retrying — exiting {}.", pg::EXIT_CONFIG);
    if tracing::dispatcher::has_been_set() {
        tracing::error!("{msg}");
    } else {
        // The log filter is itself configuration, so a refusal can land before there is anywhere to
        // log to. stderr is the same stream the subscriber would have used.
        eprintln!("{msg}");
    }
    std::io::stderr().flush().ok();
    std::process::exit(pg::EXIT_CONFIG)
}

fn init_tracing(filter: &str) {
    use tracing_subscriber::{EnvFilter, fmt};
    // `filter` already reflects ELECTRIC_CIRCUITS_LOG / ELECTRIC_LOG_LEVEL precedence (see config.rs).
    let env_filter = EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(env_filter).with_writer(std::io::stderr).init();
}
