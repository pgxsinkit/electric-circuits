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
    state: Mutex<State>,
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
        Frontier {
            pending: AtomicI64::new(0),
            failures: AtomicU64::new(0),
            state: Mutex::new(State { published: "0/0".to_string(), candidate: None, poisoned: false }),
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
    pub(crate) fn poison(&self) {
        self.failures.fetch_add(1, Ordering::SeqCst);
        self.state.lock().unwrap().poisoned = true;
    }

    /// How many flip batches were abandoned (each one a poisoning).
    pub(crate) fn failures(&self) -> u64 {
        self.failures.load(Ordering::SeqCst)
    }

    /// Whether the frontier has stopped advancing because effects were lost.
    pub(crate) fn poisoned(&self) -> bool {
        self.state.lock().unwrap().poisoned
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
/// window. `Drop` releases, so a `?`, a panic, or a **cancelled future** (an HTTP client that
/// disconnects mid-create) cannot strand the barrier — which manual pairing could not promise.
///
/// A permit whose work was lost rather than completed is retired with [`Permit::lost`] instead.
pub(crate) struct Permit {
    frontier: Arc<Frontier>,
}

impl Permit {
    /// Take a hold on `frontier`.
    pub(crate) fn take(frontier: &Arc<Frontier>) -> Permit {
        frontier.hold();
        Permit { frontier: frontier.clone() }
    }

    /// This permit's work will never land — the worker it was handed to is gone, or its retries are
    /// spent. Poison the frontier before releasing: a barrier that drains on lost work is a
    /// watermark that advances past effects no client will ever see.
    pub(crate) fn lost(self) {
        self.frontier.poison();
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
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
        drop(one);
        assert_eq!(f.published(), "0/0", "one still in flight");
        // The terminal flip publishes it — no later transaction is needed, which is the whole point.
        drop(two);
        assert_eq!(f.published(), "0/10");
    }

    /// The leak this type exists to prevent: work that returns early, panics, or is cancelled.
    #[test]
    fn a_permit_releases_however_its_scope_ends() {
        let f = Arc::new(Frontier::new());
        {
            let _permit = Permit::take(&f);
            f.commit_flushed("0/10");
            assert_eq!(f.published(), "0/0");
        }
        assert_eq!(f.published(), "0/10", "the permit released on the way out of scope");
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
        drop(Permit::take(&f));
        assert_eq!(f.published(), "0/10", "no candidate to promote; stays put");
    }

    #[test]
    fn the_newest_flushed_commit_wins() {
        let f = Arc::new(Frontier::new());
        let permit = Permit::take(&f);
        f.commit_flushed("0/10");
        f.commit_flushed("0/20");
        drop(permit);
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
