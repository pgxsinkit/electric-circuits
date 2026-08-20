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

    /// Take a count for work that must land before the frontier may advance. Every `hold` is paired
    /// with exactly one [`Self::release`].
    pub(crate) fn hold(&self) {
        self.pending.fetch_add(1, Ordering::SeqCst);
    }

    /// Give back a count. The worker that takes the barrier to zero publishes whatever commit was
    /// waiting on it — the only chance a candidate gets if no further transaction arrives.
    pub(crate) fn release(&self) {
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

    /// Promote the candidate if nothing is in flight. The barrier is re-read **under the state
    /// lock**, so a `hold` racing a `release` cannot leave a candidate published past work that has
    /// since been enqueued: increments for a commit's own flips happen synchronously before that
    /// commit's boundary, so a zero reading proves every earlier commit's work has landed.
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
        let f = Frontier::new();
        f.hold();
        f.hold();
        f.commit_flushed("0/10");
        assert_eq!(f.published(), "0/0", "flips still in flight");
        f.release();
        assert_eq!(f.published(), "0/0", "one still in flight");
        // The terminal flip publishes it — no later transaction is needed, which is the whole point.
        f.release();
        assert_eq!(f.published(), "0/10");
    }

    #[test]
    fn a_drained_barrier_with_nothing_waiting_publishes_nothing() {
        let f = Frontier::new();
        f.commit_flushed("0/10");
        f.hold();
        f.release();
        assert_eq!(f.published(), "0/10", "no candidate to promote; stays put");
    }

    #[test]
    fn the_newest_flushed_commit_wins() {
        let f = Frontier::new();
        f.hold();
        f.commit_flushed("0/10");
        f.commit_flushed("0/20");
        f.release();
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
