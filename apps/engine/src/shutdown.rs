//! Process shutdown: one token, flipped once, that every long-running part of the engine joins.
//!
//! `SIGTERM`/`SIGINT` must not be a kill: the ingestor may be half-way through appending a commit,
//! the sequencer may be half-way through fanning a transaction out, and the durable checkpoint that
//! decides where the next boot resumes is written lazily. A hard stop costs a replay (correct, but
//! wasteful) — and, worse, leaves a long-poll holding the pod's termination grace for its full
//! window.
//!
//! So shutdown is **cooperative and ordered**:
//!
//! 1. the token flips. `GET /ready` answers `503 {"status":"shutting_down"}` from that instant, and
//!    the server KEEPS ACCEPTING for `ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS` so a load balancer
//!    actually gets to poll it and take the pod out of rotation. A probe that connection-refuses
//!    also reads as "not ready", but only after the LB has already sent requests into a closing
//!    socket; answering 503 for one probe interval first is what makes the drain graceful rather
//!    than merely fast;
//! 2. the HTTP server stops accepting and lets its in-flight requests finish — the `/v1/shape`
//!    `live=true` long-poll and the engine's own durable-streams long-polls join this token in their
//!    selects, so they return in milliseconds instead of pinning the shutdown for their full
//!    timeout;
//! 3. each **party** (the replication ingestor, the sequencer) reaches its own safe point and
//!    finishes: the ingestor completes a commit it is APPENDING, then records its position
//!    LOCALLY — the wire acknowledgement to Postgres rides the replication client's own status
//!    interval (1 s) and its stop path does not force a final one, so a commit appended in the last
//!    second or so is re-delivered after the restart and dropped by the sequencer's `(lsn, seq)`
//!    highwater. Stopping mid-transaction, before the commit, is the same story with nothing
//!    appended at all. Either way the slot is never advanced BY the shutdown. The sequencer
//!    completes the batch it is processing, flushes it, and writes a final checkpoint;
//! 4. the catalog writer drains, and the process exits 0 — or [`EXIT_SHUTDOWN_INCOMPLETE`] when it
//!    could not, because the checkpoint the next boot resumes from may then be missing.
//!
//! The whole thing is bounded (`ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS`, default 25 s — under a
//! typical Kubernetes `terminationGracePeriodSeconds: 30`), and a **second** signal during the grace
//! period stops waiting and exits non-zero. Streams are never closed or retired on shutdown: a
//! restored shape continues its stream, exactly as after a crash.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

/// Exit code when shutdown was forced rather than completed: a second signal arrived, or the grace
/// period elapsed with a party still outstanding. Distinct from `0` (clean) and from
/// [`crate::pg::EXIT_CONFIG`] / the circuit-rebuild `75`.
pub const EXIT_SHUTDOWN_FORCED: i32 = 70;

/// Exit code when every party finished but the durable catalog writer did **not drain**: the final
/// checkpoint (and anything queued behind it) may not be in storage, so the next boot may replay from
/// an earlier one. Nothing is corrupted — a replay is de-duplicated — but it is not a clean exit
/// either, and `0` must keep meaning "everything got to a clean point".
pub const EXIT_SHUTDOWN_INCOMPLETE: i32 = 71;

/// How a graceful shutdown ended — what the process exits with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOutcome {
    /// Every party finished and the catalog drained inside the grace period.
    Complete,
    /// The grace period elapsed with a party still outstanding.
    Forced,
    /// Every party finished, but the catalog writer did not drain.
    CatalogIncomplete,
}

impl ShutdownOutcome {
    pub const fn exit_code(self) -> i32 {
        match self {
            ShutdownOutcome::Complete => 0,
            ShutdownOutcome::Forced => EXIT_SHUTDOWN_FORCED,
            ShutdownOutcome::CatalogIncomplete => EXIT_SHUTDOWN_INCOMPLETE,
        }
    }
}

/// How long the whole grace period may take, by default. Below a typical Kubernetes
/// `terminationGracePeriodSeconds: 30`, so the engine always gets to finish on its own terms rather
/// than being `SIGKILL`ed part-way.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(25);

/// How long the durable catalog writer is given to drain after the sequencer's final checkpoint.
pub const CATALOG_DRAIN: Duration = Duration::from_secs(10);

/// How long the HTTP server keeps accepting after the token flips, answering `GET /ready` with 503,
/// so a load balancer's readiness probe observes the drain before the socket closes. Two seconds
/// covers a default Kubernetes `periodSeconds: 1..2` probe; raise it to match a slower probe. Comes
/// out of the overall grace period, not on top of it.
pub const DEFAULT_READY_DRAIN: Duration = Duration::from_secs(2);

struct Inner {
    tx: tokio::sync::watch::Sender<bool>,
    /// Parties registered and not yet finished, by name — so a shutdown that runs out of grace can
    /// say **who** it was waiting for.
    outstanding: std::sync::Mutex<BTreeSet<&'static str>>,
    /// When the token flipped. The grace period is measured from HERE, not from when the HTTP
    /// server finally stopped, so the readiness-drain window comes out of the same budget.
    began_at: std::sync::Mutex<Option<std::time::Instant>>,
}

/// A cheap, cloneable handle to the process's shutdown state.
#[derive(Clone)]
pub struct ShutdownToken {
    inner: Arc<Inner>,
}

impl Default for ShutdownToken {
    fn default() -> Self {
        ShutdownToken::new()
    }
}

impl ShutdownToken {
    pub fn new() -> Self {
        ShutdownToken {
            inner: Arc::new(Inner {
                tx: tokio::sync::watch::channel(false).0,
                outstanding: std::sync::Mutex::new(BTreeSet::new()),
                began_at: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Flip the token. Idempotent; returns `true` only for the call that actually transitioned, so
    /// the caller can tell a first signal from a second one.
    pub fn begin(&self) -> bool {
        let first = !self.is_shutting_down();
        if first {
            // `send_replace`, not `send`: a `watch::Sender` with no live receiver refuses `send`
            // AND leaves the value alone, and this token normally has none (every waiter subscribes
            // on demand inside `wait`). `send_replace` always stores, then wakes whoever is there.
            *self.inner.began_at.lock().unwrap() = Some(std::time::Instant::now());
            self.inner.tx.send_replace(true);
            crate::metrics::metrics().shutdown_in_progress.store(1, std::sync::atomic::Ordering::Relaxed);
        }
        first
    }

    pub fn is_shutting_down(&self) -> bool {
        *self.inner.tx.borrow()
    }

    /// How long ago the token flipped (`None` if it has not). The grace period is measured against
    /// this, so the readiness-drain window and the tasks' wind-down share one budget.
    pub fn elapsed(&self) -> Option<Duration> {
        self.inner.began_at.lock().unwrap().map(|t| t.elapsed())
    }

    /// Resolves as soon as shutdown has begun — **immediately** if it already has, which is what
    /// makes this safe to put in a `tokio::select!` that may be entered after the flip.
    pub async fn wait(&self) {
        let mut rx = self.inner.tx.subscribe();
        // `wait_for` evaluates the current value first, so an already-flipped token returns at once.
        let _ = rx.wait_for(|flipped| *flipped).await;
    }

    /// Register a component the process must wait for before exiting. The returned guard removes it
    /// on drop, so a party that returns — or panics — never wedges the shutdown.
    pub fn party(&self, name: &'static str) -> ShutdownParty {
        self.inner.outstanding.lock().unwrap().insert(name);
        ShutdownParty { inner: self.inner.clone(), name }
    }

    /// Parties registered and not finished, right now.
    pub fn outstanding(&self) -> Vec<&'static str> {
        self.inner.outstanding.lock().unwrap().iter().copied().collect()
    }

    /// Wait until every registered party has finished, or `timeout` elapses. Returns whether they
    /// all finished. Polled rather than notified: this runs once, on the way out, and a 10 ms poll
    /// is not worth a wakeup protocol (same call as `CatalogWriter::drain`).
    pub async fn wait_for_parties(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.inner.outstanding.lock().unwrap().is_empty() {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// The message the grace watchdog logs before it forces the exit.
///
/// Split out from the watchdog itself because the watchdog's body is `sleep` then
/// `std::process::exit`, which cannot be unit-tested — but the property that matters *can* be: a
/// forced exit must NAME who it was still waiting for, or an operator is left with "it did not
/// shut down" and no next step.
pub fn grace_expiry_message(outstanding: &[&'static str], grace: Duration) -> String {
    let who = if outstanding.is_empty() {
        // No party is registered before the ingestor is spawned, so this is the shape of the boot
        // hanging on something it cannot interrupt — say that rather than printing an empty list.
        "no registered party (the boot had not finished: a connect, a catalog fold or a backfill \
         was still in flight)"
            .to_string()
    } else {
        outstanding.join(", ")
    };
    format!(
        "the {grace:?} shutdown grace elapsed with work still in flight — waiting on: {who}. \
         Exiting {EXIT_SHUTDOWN_FORCED}. Nothing is corrupted: an unacknowledged commit is \
         re-delivered and the last checkpoint stands. Raise ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS \
         if this is a commit that legitimately needs longer."
    )
}

/// Drop guard for a registered shutdown party (see [`ShutdownToken::party`]).
pub struct ShutdownParty {
    inner: Arc<Inner>,
    name: &'static str,
}

impl Drop for ShutdownParty {
    fn drop(&mut self) {
        self.inner.outstanding.lock().unwrap().remove(self.name);
    }
}

/// Resolve the grace period from an env getter. Pure, and **fallible**: an unparseable value is
/// boot-fatal rather than silently replaced, exactly like the large-transaction knobs — a grace
/// period the operator meant to set and that silently did not apply is the worst of both worlds.
pub fn resolve_grace(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Duration> {
    let secs = secs_var(&get, "ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS", DEFAULT_GRACE.as_secs())?;
    if secs == 0 {
        anyhow::bail!(
            "ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS must be > 0 (it bounds the graceful shutdown; 0 \
             would forbid finishing an in-flight commit)"
        );
    }
    Ok(Duration::from_secs(secs))
}

/// Resolve the readiness-drain window (see [`DEFAULT_READY_DRAIN`]). `0` is legal and means "stop
/// accepting at once" — correct when nothing in front of the engine polls `/ready`.
pub fn resolve_ready_drain(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Duration> {
    Ok(Duration::from_secs(secs_var(&get, "ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS", DEFAULT_READY_DRAIN.as_secs())?))
}

fn secs_var(get: impl Fn(&str) -> Option<String>, name: &str, default: u64) -> anyhow::Result<u64> {
    let Some(raw) = get(name) else { return Ok(default) };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(default);
    }
    raw.parse().map_err(|_| anyhow::anyhow!("{name} must be a whole number of seconds, got '{raw}'"))
}

/// The first of `SIGTERM` / `SIGINT`. On non-unix only `ctrl_c` exists.
pub async fn first_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("cannot install the SIGTERM handler ({e}); only SIGINT will be honoured");
                let _ = tokio::signal::ctrl_c().await;
                return "SIGINT";
            }
        };
        tokio::select! {
            _ = term.recv() => "SIGTERM",
            _ = tokio::signal::ctrl_c() => "SIGINT",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn begin_is_idempotent_and_reports_the_transition() {
        let t = ShutdownToken::new();
        assert!(!t.is_shutting_down());
        assert!(t.begin(), "the first begin is the transition");
        assert!(!t.begin(), "a second signal is not a transition");
        assert!(t.is_shutting_down());
    }

    /// The property every `select!` arm depends on: a token flipped BEFORE the wait is awaited must
    /// still resolve immediately, or a long-poll entered just after the flip would hang out the
    /// whole grace period.
    #[tokio::test]
    async fn wait_returns_immediately_when_already_flipped() {
        let t = ShutdownToken::new();
        t.begin();
        tokio::time::timeout(Duration::from_millis(50), t.wait())
            .await
            .expect("an already-flipped token must not block");
    }

    #[tokio::test]
    async fn wait_wakes_on_a_later_flip() {
        let t = ShutdownToken::new();
        let t2 = t.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            t2.begin();
        });
        tokio::time::timeout(Duration::from_secs(2), t.wait()).await.expect("the flip must wake the waiter");
    }

    #[tokio::test]
    async fn parties_gate_the_wait_and_are_named() {
        let t = ShutdownToken::new();
        let p = t.party("ingestor");
        assert_eq!(t.outstanding(), vec!["ingestor"]);
        assert!(!t.wait_for_parties(Duration::from_millis(30)).await, "an outstanding party must block");
        drop(p);
        assert!(t.outstanding().is_empty());
        assert!(t.wait_for_parties(Duration::from_millis(30)).await);
    }

    /// The forced-exit message must name the parties — and must not print an empty list when the
    /// process is stuck somewhere that registers no party at all (a boot that never finished).
    #[test]
    fn the_forced_exit_names_who_it_waited_for() {
        let m = grace_expiry_message(&["replication ingestor", "sequencer"], Duration::from_secs(25));
        assert!(m.contains("replication ingestor, sequencer"), "{m}");
        assert!(m.contains("Exiting 70"), "{m}");
        let m = grace_expiry_message(&[], Duration::from_secs(25));
        assert!(m.contains("no registered party"), "{m}");
        assert!(m.contains("the boot had not finished"), "{m}");
    }

    /// Only a completed shutdown exits 0; the two ways it can fall short are distinguishable.
    #[test]
    fn each_shutdown_outcome_has_its_own_exit_code() {
        assert_eq!(ShutdownOutcome::Complete.exit_code(), 0);
        assert_eq!(ShutdownOutcome::Forced.exit_code(), EXIT_SHUTDOWN_FORCED);
        assert_eq!(ShutdownOutcome::CatalogIncomplete.exit_code(), EXIT_SHUTDOWN_INCOMPLETE);
        assert_eq!((EXIT_SHUTDOWN_FORCED, EXIT_SHUTDOWN_INCOMPLETE), (70, 71), "operators and lanes match on these");
    }

    #[test]
    fn grace_defaults_and_refuses_nonsense() {
        assert_eq!(resolve_grace(|_| None).unwrap(), DEFAULT_GRACE);
        assert_eq!(resolve_grace(|_| Some("  ".into())).unwrap(), DEFAULT_GRACE);
        assert_eq!(resolve_grace(|_| Some("5".into())).unwrap(), Duration::from_secs(5));
        assert!(resolve_grace(|_| Some("0".into())).is_err(), "0 would forbid finishing a commit");
        assert!(resolve_grace(|_| Some("soon".into())).is_err());
        assert!(resolve_grace(|_| Some("-1".into())).is_err());
    }

    /// The drain window may legitimately be 0 (nothing in front of the engine polls `/ready`), which
    /// is the one place it differs from the grace period.
    #[test]
    fn ready_drain_defaults_and_allows_zero() {
        assert_eq!(resolve_ready_drain(|_| None).unwrap(), DEFAULT_READY_DRAIN);
        assert_eq!(resolve_ready_drain(|_| Some("0".into())).unwrap(), Duration::ZERO);
        assert_eq!(resolve_ready_drain(|_| Some("7".into())).unwrap(), Duration::from_secs(7));
        assert!(resolve_ready_drain(|_| Some("later".into())).is_err());
    }

    /// The grace period is measured from the FLIP, so the readiness-drain window and the tasks'
    /// wind-down share one budget rather than stacking.
    #[tokio::test]
    async fn elapsed_is_measured_from_the_flip() {
        let t = ShutdownToken::new();
        assert!(t.elapsed().is_none());
        t.begin();
        assert!(t.elapsed().is_some());
    }
}
