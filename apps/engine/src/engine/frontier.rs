//! The fan-out frontier: the highest commit LSN whose every effect is on the shape streams.
//!
//! A commit's effects land in two waves. Its own routed rows are appended by the sequencer and
//! flushed at the transaction boundary; the subquery flips it triggers are query-backs handed to
//! the propagator and land later, carrying that same source LSN. The Electric adapter advertises
//! this frontier as `global_last_seen_lsn`, and an Electric consumer DISCARDS every change at or
//! below the watermark it was last told — so publishing a commit whose flips have not landed makes
//! the consumer drop the move-in/move-out rows that arrive after it.
//!
//! Hence one type owning both halves of the invariant: the in-flight counter (`pendingFlips`) and
//! the published LSN. A commit becomes a *candidate* at its flush boundary and is published as soon
//! as the counter is observed at zero — either right there, or from whichever worker drains the
//! last outstanding batch. That second path is what stops a final deferred flip from freezing the
//! frontier forever on a stream that then goes quiet: nothing else would ever look again.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Owns the convergence barrier and the LSN it gates.
pub(crate) struct Frontier {
    /// Flip batches enqueued but not yet landed — propagation work AND the emission-lane appends it
    /// produces, each holding its own count until its append lands (see [`super::emission`]).
    pending: AtomicI64,
    /// Flip batches abandoned after exhausting their retries — the reason for a poisoning, counted
    /// so an operator can tell "one bad minute" from "Postgres is gone".
    failures: AtomicU64,
    /// Set before a deliberate runtime teardown. At that point abandoned in-memory work is handled
    /// by restart recovery, not an in-process degraded transition.
    shutting_down: std::sync::atomic::AtomicBool,
    state: Mutex<State>,
    degraded: tokio::sync::watch::Sender<bool>,
}

struct State {
    /// What [`Frontier::published`] returns: the last commit proven fully fanned out.
    published: String,
    /// The newest flushed commit not yet proven — waiting on the barrier.
    candidate: Option<String>,
    /// Set when subquery effects were **lost**, not merely delayed. The frontier then never moves
    /// again: a consumer told "you have seen everything through T" would silently diverge, so the
    /// engine stops making the claim and says so on `/replication/lsn` and `/v1/health` instead.
    /// Deliberately unrecoverable in-process — see `docs/ARCHITECTURE.md`.
    poisoned: bool,
}

impl Frontier {
    pub(crate) fn new() -> Frontier {
        let (degraded, _rx) = tokio::sync::watch::channel(false);
        Frontier {
            pending: AtomicI64::new(0),
            failures: AtomicU64::new(0),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
            state: Mutex::new(State { published: "0/0".to_string(), candidate: None, poisoned: false }),
            degraded,
        }
    }

    /// Take a count for work that must land before the frontier may advance.
    ///
    /// Private on purpose: pairing a `hold` with a `release` by hand is how a hold gets leaked — an
    /// early return, a `?`, or a dropped future, and the barrier never drains again. Callers take a
    /// [`Permit`] instead, which releases when it goes out of scope however it goes out of scope.
    fn hold(&self) {
        self.pending.fetch_add(1, Ordering::SeqCst);
    }

    /// Give back a count. The worker that takes the barrier to zero publishes whatever commit was
    /// waiting on it — the only chance a candidate gets if no further transaction arrives.
    fn release(&self) {
        if self.pending.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.try_publish();
        }
    }

    /// In-flight count (`pendingFlips`); part of the external convergence barrier.
    pub(crate) fn pending(&self) -> i64 {
        self.pending.load(Ordering::SeqCst)
    }

    /// This commit's own appends have been flushed. It becomes publishable once the barrier drains.
    pub(crate) fn commit_flushed(&self, lsn: &str) {
        let mut st = self.state.lock().unwrap();
        st.candidate = Some(lsn.to_string());
        drop(st);
        self.try_publish();
    }

    /// The highest commit LSN safe to advertise as delivered.
    pub(crate) fn published(&self) -> String {
        self.state.lock().unwrap().published.clone()
    }

    /// Record that subquery effects were lost and stop advancing, permanently.
    ///
    /// This is intentionally a no-op after [`Frontier::begin_shutdown`]: once runtime teardown has
    /// begun, dropped in-memory work belongs to restart recovery and is not an in-process failure.
    pub(crate) fn poison(&self) {
        if self.shutting_down.load(Ordering::SeqCst) {
            return;
        }
        self.failures.fetch_add(1, Ordering::SeqCst);
        self.state.lock().unwrap().poisoned = true;
        let _ = self.degraded.send(true);
    }

    /// How many flip batches were abandoned (each one a poisoning).
    pub(crate) fn failures(&self) -> u64 {
        self.failures.load(Ordering::SeqCst)
    }

    /// Whether the frontier has stopped advancing because effects were lost.
    pub(crate) fn poisoned(&self) -> bool {
        self.state.lock().unwrap().poisoned
    }

    pub(crate) fn subscribe_degraded(&self) -> tokio::sync::watch::Receiver<bool> {
        self.degraded.subscribe()
    }

    /// Suppress false lost-work reports while a deliberate runtime teardown cancels workers.
    pub(crate) fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    /// Promote the candidate if nothing is in flight.
    ///
    /// A [`Permit::take`] racing this read is not excluded by the state lock — it does not take that
    /// lock — and does not need to be. What makes a zero reading sound is an ordering precondition
    /// on the callers: **a permit is always taken before the commits it covers are processed**. A
    /// commit's own flip permits are taken synchronously during `process_envelope`, before that
    /// commit's boundary; a create's permit is taken before `begin_create`, before any delta can be
    /// buffered against it. So a permit appearing after the read covers only commits *later* than
    /// the candidate, and publishing the candidate stays true. A permit taken lazily — after the
    /// work it covers was already processed — would break that and is the one thing this type
    /// cannot check for you.
    fn try_publish(&self) {
        let mut st = self.state.lock().unwrap();
        if st.poisoned || self.pending.load(Ordering::SeqCst) != 0 {
            return;
        }
        let Some(candidate) = st.candidate.take() else { return };
        // Monotonic by construction (one sequencer, commit order), asserted anyway: a frontier that
        // moved backwards would have a consumer re-apply changes it had already discarded.
        if crate::pg::lsn_to_u64(&candidate) >= crate::pg::lsn_to_u64(&st.published) {
            st.published = candidate;
        }
    }
}

/// A hold on the barrier that releases itself.
///
/// Every piece of work the frontier waits on holds one for as long as its effects are not yet on
/// the streams: an enqueued emission batch, a flip batch in flight, a shape create's buffering
/// window. Completion is explicit; an armed permit that is merely dropped means its work vanished
/// with its owner, so `Drop` poisons before releasing. Cancellation-safe operations transfer the
/// permit to their asynchronous rollback and complete it only after rollback settles.
pub(crate) struct Permit {
    frontier: Arc<Frontier>,
    completed: bool,
}

impl Permit {
    /// Take a hold on `frontier`.
    pub(crate) fn take(frontier: &Arc<Frontier>) -> Permit {
        frontier.hold();
        Permit { frontier: frontier.clone(), completed: false }
    }

    /// The work this permit covered has landed, or has been fully rolled back.
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }

    /// This permit's work will never land — the worker it was handed to is gone, or its retries are
    /// spent. Poison the frontier before releasing: a barrier that drains on lost work is a
    /// watermark that advances past effects no client will ever see.
    pub(crate) fn lost(mut self) {
        self.frontier.poison();
        self.completed = true;
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        if !self.completed {
            self.frontier.poison();
        }
        self.frontier.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_quiet_commit_publishes_immediately() {
        let f = Frontier::new();
        f.commit_flushed("0/10");
        assert_eq!(f.published(), "0/10");
    }

    #[test]
    fn a_commit_with_outstanding_flips_waits_for_the_last_one() {
        let f = Arc::new(Frontier::new());
        let one = Permit::take(&f);
        let two = Permit::take(&f);
        f.commit_flushed("0/10");
        assert_eq!(f.published(), "0/0", "flips still in flight");
        one.complete();
        assert_eq!(f.published(), "0/0", "one still in flight");
        // The terminal flip publishes it — no later transaction is needed, which is the whole point.
        two.complete();
        assert_eq!(f.published(), "0/10");
    }

    /// Work is done only when its owner says so.
    #[test]
    fn an_explicitly_completed_permit_releases_cleanly() {
        let f = Arc::new(Frontier::new());
        let permit = Permit::take(&f);
        f.commit_flushed("0/10");
        permit.complete();
        assert_eq!(f.published(), "0/10");
        assert_eq!(f.pending(), 0);
    }

    #[test]
    fn a_dropped_permit_is_lost_work_and_poisons() {
        let f = Arc::new(Frontier::new());
        let permit = Permit::take(&f);
        f.commit_flushed("0/10");
        drop(permit);
        assert!(f.poisoned());
        assert_eq!(f.pending(), 0);
        assert_eq!(f.published(), "0/0");
    }

    #[test]
    fn dropping_queued_work_during_deliberate_shutdown_does_not_poison() {
        let f = Arc::new(Frontier::new());
        let permit = Permit::take(&f);
        f.begin_shutdown();
        drop(permit);
        assert!(!f.poisoned(), "runtime teardown is not an in-process loss event");
        assert_eq!(f.failures(), 0);
        assert_eq!(f.pending(), 0);
    }

    /// A permit whose work is gone is not a permit whose work is done.
    #[test]
    fn a_lost_permit_poisons_before_it_releases() {
        let f = Arc::new(Frontier::new());
        let permit = Permit::take(&f);
        f.commit_flushed("0/10");
        permit.lost();
        assert!(f.poisoned());
        assert_eq!(f.pending(), 0, "the barrier is still given back — pendingFlips means in flight");
        assert_eq!(f.published(), "0/0", "but the commit whose effects were lost is never published");
    }

    #[test]
    fn a_drained_barrier_with_nothing_waiting_publishes_nothing() {
        let f = Arc::new(Frontier::new());
        f.commit_flushed("0/10");
        Permit::take(&f).complete();
        assert_eq!(f.published(), "0/10", "no candidate to promote; stays put");
    }

    #[test]
    fn the_newest_flushed_commit_wins() {
        let f = Arc::new(Frontier::new());
        let permit = Permit::take(&f);
        f.commit_flushed("0/10");
        f.commit_flushed("0/20");
        permit.complete();
        assert_eq!(f.published(), "0/20");
    }

    #[test]
    fn poisoning_stops_the_frontier_for_good() {
        let f = Frontier::new();
        f.commit_flushed("0/10");
        f.poison();
        f.commit_flushed("0/20");
        assert_eq!(f.published(), "0/10", "lost effects: the claim is never made again");
        assert!(f.poisoned());
        assert_eq!(f.failures(), 1);
    }
}
