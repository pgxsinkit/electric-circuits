# 0002 — The harness client retries a dead subscription's renewal at its floor cadence, logging a non-JSON body

Status: candidate (recorded 2026-09-15)
Opened: 2026-09-15 · Area: `packages/client/src/subset.ts` (the lease keeper), `apps/api/src/core.ts`
(engine error forwarding), `packages/conformance` (`conformance-native-subscription-ambiguity`,
`conformance-retention`)
Reopen trigger: the first time this log noise hides a real failure in a CI run, or the first
non-harness consumer of `@electric-circuits/client`'s lease keeper.

## The fact

- Every full conformance run — locally and in CI (run 34820964885, 2026-09-14) — prints 40–90 lines of
  `client: subscription renewal failed: TRPCClientError: Unexpected token 's', "shape crea"... is not
  valid JSON`, in bursts of ~25 at ~330 ms, inside tests that PASS. vitest attributes the first burst
  to `conformance-native-subscription-ambiguity.test.ts > adopts the fresh handle returned when a late
  renewal recreates an evicted shape`; later bursts sit inside `conformance-retention.test.ts`.
- The cadence is the lease keeper's floor: `subset.ts` renews every `max(leaseSeconds/3, 250 ms)` and
  on failure only `console.warn`s and waits for the next tick — by design "a failed renewal is not
  fatal". The tests hold shapes with short leases and retire them (eviction, purge), so the keeper
  renews a subscription on a shape that is gone, gets refused, and keeps going until the test ends.
- The body it cannot parse starts `shape crea…` — the text of the engine's `CreateRaced` answer
  (`shape create on 'public.items' lost a race during its catalog durability wait …; retry`). The
  engine itself answers JSON (`{ "error": … }` on the native routes, `{ "message": … }` on the
  Electric adapter), so a layer between the engine and the tRPC client re-emits the message as a bare
  string: `apps/api/src/core.ts:100` wraps the engine body into `Error("engine … -> 503: …")`, and the
  tRPC transport delivers something the client hands straight to `JSON.parse`. The exact hop was not
  traced; the noise is harness-only (pgxsinkit talks to the engine's native HTTP directly and maps
  every 5xx to its own 503 — this path is the `@electric-circuits/api` tRPC surface).
- Pre-dates ADR-0009/0010: identical counts in the full-suite logs from before and after them.

## The fix

- The lease keeper should distinguish a terminal answer from an outage: a renewal refused because the
  shape no longer exists (404/409, or the `CreateRaced` retry hint exhausting) ends the keeper — the
  materialization's next read finds a fresh shape anyway (ADR-0007) — instead of re-asking every
  250 ms until the interval is cleared.
- The API layer should forward engine refusals as structured tRPC errors (status + the engine's
  `error`/`message`), never as a string a client will try to parse as JSON.
- Neither changes what any test asserts; both remove the noise.

## Reopen trigger

As above. Until then this is a candidate: the tests are correct and the engine's answers are correct;
only the harness client's retry policy and the API's error shape are wrong, and both are confined to
the conformance path.
