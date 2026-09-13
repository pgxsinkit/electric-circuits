# electric-circuits-engine

The Rust engine at the center of [electric-circuits](../../README.md): a durable-streams client that
turns Postgres logical-replication changes into incrementally-maintained **shapes**, **subquery
inner-sets**, and **scalar aggregations** — one maintained stream per *distinct* definition,
ref-counted and shared across subscribers. It serves two HTTP surfaces from one process:

- the **control plane** (`/schema`, `/shapes`, `/aggregate`, `/query`, introspection), used by
  `@electric-circuits/api`;
- the **Electric-compatible `GET /v1/shape`**, so an unmodified ElectricSQL client can sync from it.

Design and execution model: [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) and
[docs/ivm-engine-internals.md](../../docs/ivm-engine-internals.md).

## Build & run

```bash
cargo build -p electric-circuits-engine          # or: pnpm engine:build (repo root)
cargo test  -p electric-circuits-engine          # or: pnpm engine:test

ELECTRIC_CIRCUITS_DS_URL=http://127.0.0.1:8791 \
ELECTRIC_CIRCUITS_PG_URL=postgres://postgres@127.0.0.1:5432/postgres \
ELECTRIC_CIRCUITS_PG_TABLES='*' \
target/debug/electric-circuits-engine
```

The engine prints two discovery lines to **stdout** (logs go to stderr), in this order:

- `ENGINE_BINDING <url>` — the HTTP port is open. It is printed *before* Postgres is contacted, on
  purpose: `GET /ready` is how an orchestrator learns the engine is still waiting for its database,
  and a probe it cannot reach is no probe at all.
- `ENGINE_LISTENING <url>` — the boot has **resolved** (in Postgres mode: connected, introspected,
  slot verified, ingestor running). A harness that sees this can create shapes straight away. In
  library mode the two are printed together.

## Environment

| Var | Default | Meaning |
|---|---|---|
| `ELECTRIC_CIRCUITS_DS_URL` | *(required)* | Durable-streams server base URL (the change log) |
| `ELECTRIC_CIRCUITS_PG_URL` | *(unset)* | Enables **Postgres mode**: ingest via logical replication, backfill by query-back. Unset = library mode (writes arrive on table streams) |
| `ELECTRIC_CIRCUITS_PG_TABLES` | *(empty)* | Comma list of tables to replicate: `schema.name`, a bare `name` (= `public.<name>`), or `schema.*` for every table with a primary key in that schema. `*` (or empty) = `public.*` — never every schema (introspect-all sets `REPLICA IDENTITY FULL`, which must not touch managed system schemas) |
| `ELECTRIC_CIRCUITS_PG_SLOT` | `electric_circuits` | Logical replication slot name |
| `ELECTRIC_CIRCUITS_PG_POLL_MS` | `50` | Replication-slot poll interval |
| `ELECTRIC_CIRCUITS_BIND` | `127.0.0.1:0` | Bind address (`:0` = ephemeral port) |
| `ELECTRIC_CIRCUITS_LOG` | `info` | `tracing` EnvFilter (e.g. `warn`, `electric_circuits_engine=debug`) |
| `ELECTRIC_CIRCUITS_TRACE` | `1` (on) | `0`/`false`/`off` unregisters the introspection surface (`/trace` SSE, `/graph`, `/graph/node`, `/state`, `/state/node` — the pipeline-visualizer backend). When on, it costs ~nothing until a client subscribes (and stays unauthenticated — see the deployment doc) |
| `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS` | `1800` | Retention: idle time (no engine-visible reads, no live subscriptions) before an active shape goes **dormant** (engine state dropped; stream + record retained). It is also the **subscription lease window** — a claim not renewed within it is released (see "Subscriptions"). `0` disables both |
| `ELECTRIC_CIRCUITS_SHAPE_DORMANT_TTL_SECS` | `604800` (7 days) | Retention: how long a shape may stay dormant before it is **evicted** (stream + record deleted). `0` disables the TTL layer |
| `ELECTRIC_CIRCUITS_MAX_SHAPES` | `10000` | Retention: total shape-count cap; over it, least-recently-read **dormant** shapes are evicted (active shapes never are). `0` = unlimited |
| `ELECTRIC_CIRCUITS_SHAPE_DISK_BUDGET_MB` | `0` (disabled) | Retention: cap on shape-stream bytes (engine-side accounting of appended bytes — resets on restart); over it, least-recently-read dormant shapes are evicted |
| `ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS` | `60` | Retention: background sweep interval (also drives change-log segment deletion) |
| `ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES` | `1073741824` (1 GiB) | Change log: rotate into a new `changes/<n+1>` once the current segment reaches this size. `0` disables the size criterion |
| `ELECTRIC_CIRCUITS_CHANGES_SEGMENT_SECS` | `86400` (1 day) | Change log: rotate once the current segment is this old. `0` disables the age criterion (both `0` = never rotate, i.e. an unbounded log) |
| `ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS` | `604800` (7 days) | Change log: how long a rotated-out segment may stay pinned by a **dormant** shape before that shape is evicted and the segment deleted. `0` = a dormant shape pins its segment forever |
| `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` | `134217728` (128 MiB) | Large transactions: in-memory bytes of ONE transaction the ingestor may buffer before it spills the rest to disk. `0` = never spill (buffer the whole transaction in RAM) |
| `ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES` | `67108864` (64 MiB) | Large transactions: byte budget for one append (one request body) when a commit is appended in chunks. Must be > 0 and ≤ the durable-streams 1 GiB body cap — a value outside that refuses the boot |
| `ELECTRIC_CIRCUITS_TXN_SPILL_DIR` | `<temp dir>/circuits-txn-spill-<uid>` | Large transactions: where a spilled transaction's temporary file is written (created 0700, files 0600). Needs room for the largest transaction the database can produce, must be writable at boot, and must not be shared between engines |
| `ELECTRIC_CIRCUITS_BACKFILL_APPEND_BYTES` | `16777216` (16 MiB) | Backfill: byte budget for one snapshot append. A shape's backfill is **streamed** from a `REPEATABLE READ` cursor and appended chunk by chunk, so engine memory per backfill is one chunk whatever the table's size. Must be > 0 and ≤ the durable-streams 1 GiB body cap — a value outside that refuses the boot |
| `ELECTRIC_CIRCUITS_BACKFILL_STATEMENT_TIMEOUT_MS` | `0` (off) | Backfill: when > 0, `SET LOCAL statement_timeout` inside the backfill transaction. A timeout fails **that** create with a clear, retryable error (`canceling statement due to statement timeout`); nothing is retired and nothing is purged, and the client may simply try again |
| `ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS` | `25` | Graceful shutdown: how long the whole drain may take before it is forced (exit `70`). Below a typical Kubernetes `terminationGracePeriodSeconds: 30`, so the engine finishes on its own terms rather than being `SIGKILL`ed part-way. Must be > 0 |
| `ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS` | `2` | Graceful shutdown: how long the HTTP port stays open after the signal, answering `GET /ready` with 503, so a load balancer's readiness probe observes the drain before the socket closes. Set it to at least your probe's `periodSeconds` × `failureThreshold`, or the socket closes while the poller still thinks the pod is ready. Comes **out of** the grace period, not on top of it; `0` = stop accepting at once. Must be < the grace |
| `ELECTRIC_CIRCUITS_SCHEMA_RECONCILE_SECS` | `60` | Schema drift: how often the engine fingerprints every tracked table against the Postgres catalog, to catch DDL that no write follows. `0` disables the reconciler (the pgoutput triggers still fire) |
| `ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS` | `true` | What to do when the replication slot can no longer be trusted (see "Replication slot and epochs"): `true` (Electric parity) retires every shape, binds a new epoch and carries on; `0`/`false`/`off`/`no` refuses instead — ingest stops, shape routes answer 503, and `POST /epoch/reset` is the operator's recovery |
| `ELECTRIC_HANDLE_TTL` | `600` | Seconds a `/v1/shape` handle may sit idle before its **handle state** is evicted and its shape subscription released (the shape + stream are retained and follow the retention lifecycle); a late request gets `409 must-refetch` and rejoins the retained shape |
| `ELECTRIC_LIVE_TIMEOUT_MS` | `20000` | Overall deadline for a `live=true` `/v1/shape` long-poll, then `204` |

### Benchmarking-fleet surface (`ELECTRIC_*`)

The engine also accepts Electric's own env surface so the `electric-circuits` image is a drop-in for
`electricsql/electric` in the [benchmarking-fleet](../../docs/fleet-conformance.md). These are resolved
in `config.rs`; the `ELECTRIC_CIRCUITS_*` vars above always **win** (dev/test behavior is unchanged). Any
unknown `ELECTRIC_*` var is accepted and logged once as "accepted (no-op)" — it never crashes boot.

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | *(unset)* | Postgres URL (tolerates `?sslmode=disable`); `ELECTRIC_CIRCUITS_PG_URL` wins |
| `ELECTRIC_PORT` | `3000` when set / under `DATABASE_URL` | Binds `0.0.0.0:<port>`; `ELECTRIC_CIRCUITS_BIND` wins |
| `ELECTRIC_LOG_LEVEL` | `info` | `error`/`warning`/`info`/`debug` → log filter; `ELECTRIC_CIRCUITS_LOG` wins |
| `ELECTRIC_REPLICATION_STREAM_ID` | *(unset)* | Slot name `electric_slot_<id>`; also the `stack_id` metric tag |
| `ELECTRIC_INSTANCE_ID` | generated UUID | Tags every StatsD metric `instance_id:<id>` |
| `ELECTRIC_STATSD_HOST` | *(unset → StatsD off)* | `host[:port]` (default port 8125) StatsD destination |
| `TELEMETRY_POLLER_PERIOD` / `ELECTRIC_SYSTEM_METRICS_POLL_INTERVAL` | `5s` | Periodic-metrics interval (ms / human duration; the latter wins) |
| `ELECTRIC_SECRET` | *(unset)* | If set, `/v1/shape` requires `secret`/`api_secret` query param (else `401`) |
| `ELECTRIC_INSECURE` | *(unset)* | Accepted; no-op when no secret |
| `ELECTRIC_STORAGE_DIR` | *(unset)* | If set + exists, `du`'d every ~60s → `electric.storage.used.bytes` |

**Backfills are streamed, never materialised.** A shape's initial rows come off a `REPEATABLE READ`
cursor (`query_raw`) and are appended to the still-**pending** shape stream in chunks bounded by
`ELECTRIC_CIRCUITS_BACKFILL_APPEND_BYTES`, so the engine holds one chunk at a time whatever the
table's size. Shape creation is already two-phase — a pending buffer, then a gated activation — so
chunking needs no protocol change: nothing reads the stream until `ActivateShape` lands, and a
failure part-way aborts the pending shape and rolls the whole creation back exactly as before. An
**aggregate** folds each chunk into its seed and drops the rows; a **subquery** inner-set node's seed
is the one thing genuinely collected in memory, because that set *is* the state the node will
maintain. The `REPEATABLE READ` bracket and the `SnapshotGate` capture are unchanged. (`GET /v1/shape`'s
*snapshot* still builds the whole body — that is what the Electric protocol asks for — but the key
set it folds for a catch-up read keeps keys only, never the rows.)

**An integer SUM is exact, and says so on the wire.** SUM/AVG accumulate integer values in `i128`, so
a `bigint` column sums without the rounding an `f64` accumulator starts doing at `2^53` — which a
single `bigint` cell can already exceed. The aggregate envelope's `value` is therefore a JSON
**number** while the total is exactly representable as one (`|v| ≤ 2^53 - 1`) and a decimal
**string** beyond that: a JSON number that large is silently rounded by every parser that decodes
into a double, and the engine will not hand back a wrong total that looks right. Float columns are
unchanged (always a number), and AVG stays a double. Exactness is for **integer** columns only:
`numeric` maps to the protocol's `float` (`pg.rs::map_pg_type`), so a `numeric` column sums in
`f64` like any other float. Clients: `AggregateValue` is `number | string | boolean | null`;
`BigInt(v)` a string sum.

**And so is every other integer on the wire.** The same encoding governs ordinary row values
(`Value::to_json`): a JSON number while `|v| ≤ 2^53 - 1`, an exact decimal **string** beyond it —
shape-stream envelopes, `POST /query`, subset pages, MIN/MAX over an int column. `Value::from_json`
takes the string form back, so it round-trips. TypeScript consumers see `number | string` for an
`int` cell (`packages/protocol`'s `Value`, the client's zod row schema); `String(v)` is always the
exact decimal.

**A composite primary key encodes its tuple unambiguously.** The envelope `key` for a single-column
key is the value's own string, unchanged. For a composite key each component is escaped (`\` →
`\\`, U+001F → `\x1f`) before the components are joined with U+001F (`schema::key_string` /
`join_key_components`; the replication decoder's `key_from_obj` uses the same helper, so the backfill
and the live path spell identical keys). Without escaping, `('x', 'y␟z')` and `('x␟y', 'z')` both
spell `x␟y␟z` — and `translate_output` de-duplicates positive rows by that string, so one legal row
disappeared from every shape.

**A subset page is ordered the way its client can reproduce.** Membership in a subset's loaded window
is decided client-side, so `ORDER BY` on a collatable text column carries an explicit `COLLATE "C"`
(code-point order), as do the keyset cursor's range comparisons — which is also how the engine's own
predicate evaluation compares text. `uuid`/`timestamptz` and friends map to the protocol's `text` but
have a collation-independent order (and refuse a collation), so they are left alone; `=`/`<>` are
never collated, keeping them index-eligible. The collation is emitted only when the Postgres type is
KNOWN, so a table declared through `POST /schema` instead of introspected keeps the database's
default ordering — the guarantee needs introspection. On a non-`C` database, `COLLATE "C"` on `<`/`>`
cannot use the column's default-collation btree index: add an expression index
`CREATE INDEX … ON t ((col COLLATE "C"))` for the columns your subsets order by (on `C`/`C.UTF-8`,
the common container default, the index already is that collation and nothing changes).

**Library mode keeps the before-image the database would have supplied.** With no
`ELECTRIC_CIRCUITS_PG_URL`, writes reach the change log through the native write API as
`(table, op, pk, row)` — a delete or update carries no prior row, and without one the retraction
half of the Z-set delta is missing, so a deleted row could never leave a shape. Two mechanisms
close that, and between them there is no gap:

- **The sequencer's per-key view** (`TableExec::library_rows`): the current row per key, stamped
  onto each envelope as its `old` before anything downstream reads it — after the de-duplication
  highwater, ahead of the pending-shape buffers, the fan-out and the aggregate folds. It is in
  memory and reported under `GET /memory`. It is **exact from boot**, not per-boot best-effort:
  library mode has no catalog checkpoint to resume from, so a starting process replays the change
  log from its origin and rebuilds the view in full before serving anything. (Postgres mode
  allocates none of this — there the invariant that the hot path holds no table copy is intact.)
- **Absolute emission** for the one reader that is not at the log's head: a shape reactivating out
  of **dormancy** replays the log from *its* resume position, where the view does not apply. Each
  old-less envelope there states the shape's membership outright — matches the predicate now →
  `upsert`, otherwise → `delete <key>` (the rule the subquery registry uses, for the same reason).
  A delete for a key the shape never held is a deliberate, tolerated no-op; on the live path the
  same rule costs a visit to every shape on the table, which is why it is reserved for envelopes
  that genuinely have no before-image.

What library mode still does **not** do is backfill: a shape or aggregate created at time T holds
only what changed after T, and a restart loses the shape catalog, so a re-created aggregate counts
from its own creation rather than from the table. (Aggregates never go dormant, so the replay path
above never applies to one.)

**`GET /v1/health`** reports the boot state machine as an exact, whitespace-free JSON body:
`{"status":"waiting"}` (202) until Postgres connects, `{"status":"starting"}` (202) through
introspection/slot/ingest spawn, then `{"status":"active"}` (200). Library mode is `active` at once.
`{"status":"degraded"}` (503) outranks all of them — see **Degraded** below.
`GET /` → 200 empty; `OPTIONS /v1/shape` → 204 with `access-control-allow-methods`.

**Degraded (fail-closed).** A subquery flip's Postgres query-back is retried (resuming the DAG walk
where it stopped, not restarting it). If the retries are exhausted the effects that batch was
carrying are lost — the inner-set node moved before the query-back ran, so nothing will re-derive
them. The engine then refuses to pretend otherwise: the batch never decrements `pendingFlips` (so
the convergence barrier `sync caught up + offsets at tail + pendingFlips == 0` now means *every
computed effect has landed, or the engine is degraded and says so*), `GET /replication/lsn` reports
a non-zero `flipFailures`, `/v1/health` turns `degraded` (503), and `POST /shapes`, `POST /aggregate`,
`POST /query`, `GET /shapes/{id}(/rows|/log)` and `GET /v1/shape` answer 503 with
`{"error":"degraded: …"}`. Every subquery shape's durable stream is retired too — closed, then
deleted — clients read durable-streams directly, past the HTTP surface, so that is the only way they
learn; the close releases a tailing long-poll at once with `stream-closed`. Observability
(`/replication/lsn`, `/metrics*`, `/memory`, `/subqueries`, `/graph`, `/state`, `/trace`, `/tables/*`,
`/health`) stays up. **Recovery is a restart**, which re-seeds every node from Postgres. A restart
*drops* every subquery shape — their inner-node state is not persisted, so the catalog restore
deliberately does not restore them — and clients recreate them with `POST /shapes`. Deleting the
streams therefore destroys nothing a restart would have kept.

**StatsD telemetry** (`statsd.rs`) is the fleet's only metrics channel — the datadog wire format
(`name:value|type|#instance_id:<id>,...`), non-blocking (bounded channel → batched ≤1432-byte UDP
datagrams), off unless `ELECTRIC_STATSD_HOST` is set. It emits a periodic system-metrics table
(`system.*`/`vm.*`, sampled with `sysinfo`) plus event metrics at the HTTP, replication, storage, and
snapshot paths. Only genuinely-measured values are emitted; anything unmeasurable on the host is
omitted, never faked. The existing `GET /metrics` (JSON) + `GET /metrics/prometheus` (OTel) are
unchanged.

## HTTP endpoints

| Route | Purpose |
|---|---|
| `GET /health` | **liveness** — `ok`/200 while the process runs, and nothing else (see "Operating") |
| `GET /ready` | **readiness** — 200 `{"status":"active"}` only when the engine can serve; 503 with `waiting`/`starting`/`degraded`/`shutting_down` otherwise |
| `POST /schema` | define the schema (library mode only; Postgres mode self-configures by introspection and answers `409`) |
| `POST /shapes` | create a shape (`table`, `where`, `columns`, `changesOnly`, `subscription`) — identical definitions share one stream. Repeating the create with the same `subscription` **renews** it and returns the same handle (see "Subscriptions") |
| `POST /aggregate` | create a live scalar aggregation (`table`, `where`, `fn`, `col`, `subscription`) |
| `GET /shapes/{id}` / `DELETE /shapes/{id}?subscription=…` | look up a shape (its retention `state` and live `subscriptions` count) / release THAT subscription — idempotent, and the shape is retained and ages through the retention lifecycle. Without `subscription` it is the legacy anonymous decrement (not retry-safe). `DELETE …?purge=true` force-drops the shape immediately (admin/debug; the visualizer's trash). Either form answers only once its catalog record is **durable**, so it blocks while storage is down and a timeout means "no answer", never "not done" — retry it (see *Durability of a create, and of a retirement*) |
| `GET /shapes/{id}/rows` | current contents of an existing shape (folds its stream; visualizer preview) |
| `GET /shapes/{id}/log` | tail of a shape's stream as-is (op/key/value/lsn) — the visualizer's feed change log |
| `POST /query` | one-shot subset query: `SELECT … ORDER BY … LIMIT/OFFSET` + snapshot LSN |
| `GET /trace` | SSE: per-envelope pipeline traces (hops + outcomes) and `shapeAdded`/`shapeDropped` lifecycle events; lossy by design, zero cost with no subscribers |
| `GET /tables` | every tracked table + its schema-drift `unresolved` flag |
| `GET /tables/{name}/offset` · `GET /tables/{name}/families` | sequencer position in the change log (`{segment, path, offset}` — compare `(segment, offset)`, never the offset alone) / routing-family stats |
| `GET /subqueries` · `GET /graph` · `GET /graph/node?sig=…` | shared-node stats, pipeline graph, one node's live index |
| `GET /replication/lsn` | ingestor LSN + sync status + `pendingFlips` / `flipFailures` (the convergence barrier) + the `epoch` object (slot binding + `state`/`reason`) + `changes` (the current change-log segment and the ingestor's tail offset in it) |
| `POST /epoch/reset` | operator recovery from a broken epoch under `ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS=false`: retire every shape, bind a new epoch, resume ingest (409 if the epoch is not broken) |
| `GET /metrics` · `POST /metrics/reset` · `GET /memory` · `GET /metrics/prometheus` | counters/histograms, memory snapshot, OTel/Prometheus exposition |
| `GET /v1/shape` | Electric protocol: snapshot (`offset=-1`), live long-poll, handles/offsets/`must-refetch` |

The `/v1/shape` adapter parses Electric's SQL `where` grammar and is validated against Electric's own
oracle/property/integration tests ([electric-conformance/](../../electric-conformance/README.md)).

**Creating a subquery shape** (`POST /shapes` with an `IN (SELECT …)` predicate) registers the
shape's dependency edges before it reads Postgres, so a membership change can reach it mid-create:
work aimed at the not-yet-installed shape is queued on the pending create, and work aimed at a
parent inner-set node this create is still seeding is queued on that node — both replayed (and, for
a node, walked on down the graph) the moment the seed and the shape are in. The create's rollback
state stays registry-owned across the whole install, so a client disconnect at any point — a
partly-installed membership seed included — is unwound exactly and the same shape is immediately
creatable again.

## Operating

### Probes

Three endpoints, three different questions. Pointing an orchestrator at the wrong one is how a pod
gets restarted for a condition a restart does not fix.

| Route | Question | Answers |
|---|---|---|
| `GET /health` | **liveness** — is the process alive? | `200 ok`, always, while the process runs |
| `GET /ready` | **readiness** — should it get traffic? | `200 {"status":"active"}`, else `503` with `waiting` / `starting` / `degraded` / `shutting_down` |
| `GET /v1/health` | fleet parity (Electric's own healthcheck) | `202` while booting, `200 active`, `503 degraded` |

`/ready` is 200 only when **every** precondition for serving holds: Postgres connected, the slot
verified against the epoch binding, the durable catalog restored, the ingestor spawned, no lost
membership effects, no broken epoch, no shutdown in progress. The HTTP surface comes up **before**
Postgres, deliberately — a readiness probe an orchestrator cannot reach is no probe at all — so an
engine still waiting for its database answers `503 waiting` rather than refusing connections.

`/health` deliberately never fails for any of that. A kubelet restarts a pod whose *liveness* probe
fails, and neither "Postgres is not up yet" nor "the epoch is broken and an operator must post
`/epoch/reset`" is fixed by a restart — the second is actively made worse by one.

### Graceful shutdown

`SIGTERM` (or `SIGINT`) drains; it is not a kill.

1. **`/ready` turns `503 {"status":"shutting_down"}` first**, before anything else changes, and the
   port stays open for `ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS` (2 s) so a load balancer's probe
   actually observes it and takes the pod out of rotation.
2. The accept loop closes; in-flight requests finish. A parked `/v1/shape` `live=true` long-poll
   returns **at once** (it joins the shutdown token in its select) instead of holding the
   termination grace for its full `ELECTRIC_LIVE_TIMEOUT_MS` window — and so does the sequencer's own
   long-poll on the change log.
3. The **ingestor** finishes the transaction it is *appending*: if it is inside the chunked append
   it runs to completion and records its position **locally**. The acknowledgement Postgres sees is
   a separate thing — the replication client sends standby feedback on its own status interval
   (1 s) and its stop path does not force a final one, so a commit appended in the last second or
   so is re-delivered after the restart and dropped by the sequencer's `(lsn, seq)` highwater
   (ADR-0003). If the ingestor is mid-transaction before the commit it simply stops, having
   appended nothing. **Shutdown never advances the slot**; at worst it costs a bounded, de-duplicated
   replay.
4. The **sequencer** finishes the batch it is processing, flushes it, and writes a final `Offset`
   checkpoint; the durable catalog writer drains it (≤ 10 s) so the next boot resumes from there
   rather than replaying since the last lazy 2 s checkpoint.
5. Exit `0`.

**Streams are never closed or retired on shutdown.** A shape's stream is left exactly as it is and
the restored shape continues it — clients see nothing, and there is no backfill storm. (Closing is
*retirement*, which means "this shape is gone; re-subscribe"; a restart is not that.) A retirement
that was still being retried when the signal landed is simply left to the next boot: its `Dropped`
record is the durable intent, and the boot re-queues it.

New `live=true` requests arriving during the drain are answered `503` + `Retry-After: 1`, not an
empty 204: an Electric client re-polls a 204 immediately, so 204 would turn the drain window into a
tight poll loop for every live subscriber. Polls already parked are unaffected — they return their
normal 204 with the offset they had.

The whole sequence is bounded by `ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS` (25 s, under a typical
Kubernetes `terminationGracePeriodSeconds: 30`). The bound is on the **process**, not on one step: a
watchdog is armed the instant the signal lands and forces the exit wherever the engine has got to,
naming whoever it was still waiting for. A **second** signal during the grace period, or the grace
elapsing with work still in flight, exits `70` immediately — nothing is corrupted either way: an
un-acknowledged commit is re-delivered (and de-duplicated) and the previous checkpoint stands.

### Exit codes

| code | meaning |
|---|---|
| `0` | a graceful shutdown completed inside its grace period |
| `70` | shutdown **forced**: a second signal, or the grace elapsed with a party still running (the log names which — including `catalog writer`, when an append is still being retried through a storage outage) |
| `71` | shutdown **incomplete**: every task finished, but the durable catalog writer did not drain in time — the final checkpoint may be missing, so the next boot may replay (de-duplicated) from an earlier one. Not corrupt, but not a clean exit either |
| `74` | the durable catalog **refused** an event (`EX_IOERR`): storage answered with something waiting will not change (a 4xx, an event that will not serialize), or the catalog writer task panicked. The engine's memory and its durable record disagree, and only a re-fold at boot reconciles them, so it exits rather than keep serving state the record does not describe. A transient failure — transport, timeout, 5xx — is NOT this: it is retried in place, forever |
| `75` | a counts pipeline must be rebuilt — schema drift, `TRUNCATE` or an epoch reset on a circuit-served table; restart re-seeds it |
| `78` | **boot refused** (`EX_CONFIG`) — see below |

### Subscriptions: identified, idempotent, leased

A subscriber **names its claim**. `POST /shapes` (and `POST /aggregate`) takes an optional
`subscription` — any non-empty string up to 128 bytes with no control characters — and every create
response carries the `subscription` it was recorded under plus `leaseSeconds`. See
`docs/adr/0008-subscriptions-are-identified-idempotent-and-leased.md`.

- **Repeat = renew.** A create naming a subscription the shape already holds returns the same handle,
  counts nothing, and moves the lease. So a create whose success response was lost can simply be
  sent again. A subscription id held by a **different** shape is `409` — one name, one shape.
- **Release is idempotent.** `DELETE /shapes/{id}?subscription=…` releases that claim; a second one
  is a no-op `200`. `DELETE /shapes/{id}` **without** the parameter is the legacy anonymous
  decrement, kept for callers that never learned their id: it is **not** retry-safe (nothing says
  which claim was meant), and it releases an engine-minted claim before an identified one so it
  cannot steal a named subscriber's. Omitting `subscription` on the create is the same trade — the
  engine mints and returns one, but that first create had no idempotency, because a repeat is
  indistinguishable from a new subscriber. A minted id is `~<nonce>-<n>`; the `~` is a **marker, not
  a reserved namespace** — it exists only for that anonymous-`DELETE` tie-break. The engine does not
  check whether it minted a given `~` id, so a returned one keeps working after the claim has lapsed,
  after it has been released, and after a restart, and a caller may name a `~` id of its own (all it
  achieves is making its own claim the expendable one).
- **A subscription is a lease.** Native reads go straight to durable-streams, so the engine never
  sees them: a claim counts as live only while it is created or renewed within
  `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS` (`leaseSeconds` in the response — renew at a fraction of it).
  The retention sweeper releases an unrenewed one exactly as an explicit `DELETE` would, and the
  shape then follows the ordinary lifecycle (idle → dormant → evicted). A client that renews late
  simply re-subscribes and may find a fresh shape. `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS=0` disables
  dormancy and, with it, leases. Watch `subscriptions_live` (gauge) and
  `subscriptions_lapsed_total` (counter); `GET /shapes/{id}` reports the per-shape count.
- **Every catalog event carries an `eid`**, assigned when it is queued, and the boot fold ignores an
  `eid` it has already applied — so the writer's retry-in-place (a response lost after the append
  committed) can never double-apply a join, a leave or anything else. A catalog written before this
  (no `eid`, no `subscription`) is boot-fatal, like the pre-ADR-0002 and pre-ADR-0006 formats.

### Durability of a create, and of a retirement

`POST /shapes` (and a join that takes a NEW claim — a lease *renewal* promises nothing that is not
already in the log, so its record is queued like any other) is acknowledged only once its record has
reached the durable catalog. The
catalog writer never drops an event: a transient failure — transport, timeout, 5xx — retries **that**
event in place, forever, at 100 ms → 5 s, and everything queued behind it waits, which is what keeps
the log's order equal to the engine's. So a create while durable-streams is down **waits** rather
than handing back a shape a restart would forget; the client's own timeout is the bound, and a record
that lands after the client gave up leaves a shape with no subscriber, which the retention sweeper
evicts.

**That wait is an interval, and the closing check is taken again after it.** Anyone who can make
storage slow can hold a create or a join open while a `TRUNCATE`, a schema drift, an epoch reset or a
`?purge=true` retires the very shape it is about to acknowledge — none of which the pre-wait checks
can see. So every `Created`/`Joined` await is followed by `recheck_after_durability`: the degradation
latch, the schema generations the create captured, the epoch generation, and "is the record still
registered, on the same stream?" (a purge moves no generation, so registration is checked directly).
A mismatch rolls the attempt back — the create's `CreateGuard`, or a join giving its provisional
refcount back — and the create is simply **redone**, up to three times, before answering 503. A handle
whose shape was retired during the wait is never returned. A join additionally `HEAD`s the retained
stream: if storage has lost it, the stale registry entry is retired and the caller gets a fresh shape
rather than a dead URL.

**A native removal waits too.** `DELETE /shapes/{id}`, with or without `?purge=true`, is acknowledged
only once its `Left`/`Dropped` has reached the durable catalog. A `200` therefore means the release or
the purge survives a restart — including under `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS=0`, where leases are
disabled and nothing would ever repair a record that went missing. The removal is **retry-safe**: a
second identical `DELETE` finds the engine state already updated, records nothing new, and waits on
the same durability barrier before answering, so it is as strong an answer as the first.

A `DELETE` that times out means only that the client stopped waiting for the answer. It does not
cancel anything: the record is owned by the catalog writer and still lands, and for a purge the
teardown that follows it — the subquery-registry removal and the stream's close-then-delete — runs in
a task the engine owns, not in the abandoned request. Retrying after a timeout is always safe, and
after storage recovers the outcome is the same either way.

Engine-internal removals are the other way round, and stay queued: drift, `TRUNCATE`, the epoch reset,
retention eviction and the `/v1/shape` adapter's release each have a completion barrier of their own,
and the lease reconverges what a crash loses — a `Left` that never landed comes back as a subscription
whose **restored lease age** is already past the idle window, so the sweeper releases it within one
sweep; a `Dropped` that never landed comes back as a shape whose subscriptions are equally stale, and
a repeated purge is a no-op.

A *definite* refusal of a record (a 4xx, an event that will not serialize) is the pathological case —
memory and storage disagree — and exits `74`. Note what that means for a storage front that accepts
the `PUT` creating `meta/catalog` but 4xx's the `POST` appending to it: the engine crash-loops on 74
rather than refusing the boot with 78. That is intentional. Until the stream has been created once,
every append failure is retried (so a misrouted PUT never exits); after it, the state is "storage
refused a record the engine has already acted on", which is an IO failure to be restarted through,
not a configuration to be rejected at boot.

Removing a shape is the mirror image (ADR-0007): the `Dropped` record is written **before** the
stream is retired and a `Retired` record only **after** storage accepted the delete, so a retirement
storage refused is retried in the background (500 ms → 5 s) and, if the process dies first, re-queued
by the next boot straight out of the catalog — which is also how an orphaned `shape/*` stream is
collected. `GET /shapes/{id}` answers 404 from the moment the record goes, pending retirement or not.

**A terminal append answer is reconciled before a live shape's batch is discarded.** `404` is what a
proxy, a storage router or a failover answers just as readily as a real deletion, and
`append_reliable` reporting "retired" makes the sequencer advance past the batch — so believing a
false one leaves a registered shape permanently missing a committed change, with nothing left that
remembers it. The engine answers instead: still registered and `HEAD` finds the stream ⇒ the status
was false, keep retrying; storage really has lost it ⇒ retire the shape (`Dropped`, close-then-delete,
deregister) so subscribers re-subscribe, and only then discard. Symmetrically, an append whose
*failure* is costly — activation's aggregate re-seed and the circuit-aggregate seed at restore, a
dormant shape's replay — uses `append_retrying` (transient retried with backoff, joined to the
shutdown token), so one 503 during a boot costs neither a live subscription nor a boot attempt. Its
budget is **30 s** (`DsClient::RESTORE_APPEND_BUDGET`): long enough to ride out a storage restart or
a failover, short enough that a boot does not hang on a dependency that is not coming back. At
restore an exhausted budget fails the whole restore and the boot backs off and retries it (see
*Boot: catalog restore* below) — the aggregate is kept, not retired.

### Boot: fatal vs retryable

A Postgres failure at boot is classified, not guessed at (`pg::classify`), and the two classes get
opposite treatment.

**Fatal → exit `78` at once, with a named message.** `wal_level` ≠ `logical` (checked explicitly
with `SHOW wal_level`, right after connecting, rather than left to surface as a slot-creation
failure — it needs a Postgres *restart* to change); authentication refused (`28000`, `28P01`);
insufficient privilege (`42501`) for `CREATE PUBLICATION`, the slot, `REPLICA IDENTITY FULL` or
`pg_control_system()`; an unknown database (`3D000`); any other SQLSTATE outside the transient
classes below. So are the engine's own refusals, which are not Postgres errors at all and which
exit `78` too: an unusable `ELECTRIC_CIRCUITS_PG_TABLES` entry, a publication with a per-table
column list, a durable catalog the engine could not read, an unwritable
`ELECTRIC_CIRCUITS_TXN_SPILL_DIR`, an out-of-range byte budget, a missing `ELECTRIC_CIRCUITS_DS_URL`
— and a **connection string the driver cannot parse**, which is refused while resolving the
configuration, before the port is bound. That last one is deliberate: a `Config::from_str` failure
carries no SQLSTATE and no server answer, so the classifier could not tell it apart from "the
database is not up yet" and would retry a typo for ever.

**Retryable → back off (1 s → 30 s, jittered) and try again, indefinitely.** Connection refused,
DNS, a timeout — a connection attempt that hangs is cut off after 10 s, so a non-routable address is
a normal retry rather than an invisible wedge — TLS, anything with no SQLSTATE at all, plus SQLSTATE
classes `08` (connection),
`40` (serialization), `53` (resources, e.g. `53300` too many connections), `55` (`55006`, the slot is
in use) and `57` (operator intervention, incl. `57P03` "the database system is starting up").
`GET /ready` reports `503 waiting` throughout and each attempt is logged. **Durable-streams** is
treated the same way: a refused connection, a timeout, a 5xx or a 429 while folding the catalog or opening
the change log backs off with `durable-streams is unreachable` rather than exiting — storage that
comes up after its engine is as ordinary as a database that does. (A *malformed* catalog, a
change-log segment that is gone and an unusable `ELECTRIC_CIRCUITS_DS_URL` stay fatal: none of them
is a transport problem. A *shape* stream that is gone is not a boot failure at all — see below.)
There is no restart in any of these cases: an orchestrator gates traffic on readiness, and
a dependency that comes up after its engine is the normal case, not a failure. A `SIGTERM` while
still waiting exits `0` — the whole boot is raced against the shutdown token, so it stops in
milliseconds even mid-connect.

### Boot: catalog restore

The restore (`docs/adr/0009-boot-restore-fails-closed-and-retires-gone-shapes.md`) never serves an
empty or partial registry over a catalog that names more. It runs in a fixed order:

1. **Classify** every record against boot introspection. A shape whose table is no longer in the
   compiled set, whose schema moved while the engine was down, or which is a subquery shape is
   condemned — per shape, never the whole engine.
2. **`HEAD`** every remaining record's stream, 16 at a time, each retried in place through a
   transient failure (5 tries, 100 → 400 ms apart). 404/410 or `stream-closed` condemns that shape
   too; a missing stream is an ordinary state (a plain or subquery create's `Created` can land
   before its stream `PUT` does, and a kill in between leaves exactly that). Anything else that
   outlasts its retries — a refused connection, a timeout, a 5xx or 429 — is not an answer: the
   restore stops with nothing installed.
3. **Retire** the condemned, in id order: `Dropped`, then close-then-delete, then `Retired`. A
   missing stream is never recreated.
4. **Install and resume** the rest as one unit, into a sequencer that reads no change until every
   shape has resumed. If any resume fails — Postgres refusing an aggregate's re-seed, storage
   exhausting a re-seed append's budget, a stream that vanished after its check — everything
   installed is undone (including that sequencer, which has read and checkpointed nothing) and the
   boot fails.

A failed restore goes through the classification above: a transient cause (and a stream that
vanished mid-restore, which the retried attempt's check then retires) retries with `/ready`
reporting `waiting`; a real refusal (a record this build cannot resume at all) exits `78` by name.
Every retirement is logged with the shape, table and reason, and counted (see *Metrics*).

**Nothing else touches shapes until the boot resolves.** In Postgres mode, for as long as the boot
phase is `waiting` or `starting`, `POST /shapes`, `POST /aggregate`, `POST /query`, `GET /shapes/{id}` (and
`/rows`, `/log`), `DELETE /shapes/{id}` and `GET /v1/shape` answer `503` with `Retry-After: 1` and
`engine is still booting …`. A create during the restore — or during a retry's backoff — would
otherwise spawn a sequencer that reads the backlog with nothing registered and checkpoints past it,
or mint a shape id a catalog record's stream still owns; and a 404 for a shape that is merely not
restored yet would tell its subscriber it is gone. `POST /schema` is refused in Postgres mode at any
phase (`409`): the schema there is introspected, and the endpoint would otherwise install tables and
start a sequencer the boot does not own. Library mode has no boot and no gate.

### Metrics

`GET /metrics` (JSON) and `GET /metrics/prometheus` (OpenTelemetry exposition) now carry the same
engine counters and gauges — the Prometheus endpoint used to export only the memory/cardinality
gauges. Alongside the existing counters, the ops-relevant ones are:

| metric | kind | meaning |
|---|---|---|
| `replication_slot_retained_wal_bytes` | gauge | `pg_current_wal_lsn() - restart_lsn` — the WAL **Postgres is holding on disk for this engine**. The number that fills the source database's volume when the engine falls behind or stops |
| `replication_confirmed_flush_lag_bytes` | gauge | `pg_current_wal_lsn() - confirmed_flush_lsn` — ingest lag, in bytes of WAL |
| `replication_slot_active` | gauge | `1` while a walsender holds the slot; `0` when it does not, and when the slot is not there at all |
| `sequencer_held_run` | gauge | `1` while the sequencer is holding an incomplete transaction (ADR-0003). A hold pins the change-log position — the restart point, the convergence barrier, the segment-deletion floor — so a hold that does not end must be visible as a level |
| `sequencer_orphan_fragments_total` | counter | incomplete transaction fragments discarded because a different transaction followed them (a reconnect re-delivering earlier commits first, or an epoch reset) |
| `backfill_chunked_appends_total` | counter | chunk appends made by backfills too large for one append (a backfill that fits in one contributes 0) |
| `shutdown_in_progress` | gauge | `1` once a graceful shutdown has begun |
| `catalog_append_retries_total` | counter | durable-catalog appends re-attempted because storage was unavailable. The writer never drops an event, so this is the visible cost of that promise: a climbing value with `shape_appends` flat means creates are waiting on storage |
| `retirements_pending` | gauge | shape streams dropped from the engine's records whose deletion storage has not yet accepted (ADR-0007). Non-zero means public stream URLs are outliving their shapes **right now**; it returns to 0 on its own, and a boot re-derives it from the catalog |
| `retirement_retries_total` | counter | retirement attempts that failed and were re-queued |
| `catalog_restore_retired_<reason>_total` | counter | shape records the boot's catalog restore retired instead of resuming (ADR-0009), one per reason: `schema`, `subquery`, `table_gone`, `stream_missing`, `stream_closed`. Prometheus exports one `engine_catalog_restore_retired_total` with a `reason` attribute. `schema`/`subquery` after a boot are expected; the other three mean something outside the engine moved |
| `sequencer_stale_schema_skipped_total` | counter | change-log envelopes consumed **without** being decoded, because a drift replaced the schema they were decoded under while the sequencer was still behind the ingestor (ADR-0010). Non-zero after a migration is expected and is bounded by the DML that migration outran; non-zero with no migrations means something is writing envelopes under a schema the engine does not have |
| `sequencer_unknown_table_skipped_total` | counter | change-log envelopes consumed without being decoded because the engine does not compile their table at all — dropped, or parked unresolved (ADR-0005), so its dependents are retired. The other half of the same rule; a `type` that is not a canonical `schema.name` is **not** counted here, because no producer can write one — it parks the sequencer |

While the sequencer is **parked** on a change it cannot process (ADR-0010), both metrics surfaces
report the envelope: `GET /metrics` adds a `changeLogFailure` object and `GET /replication/lsn` carries
the same one. It names the envelope (`table`, `key`, `txid`, `lsn`, the change-log `position`, the
`error` chain) and its `recovery`: `POST /epoch/reset`. While the engine is processing changes normally
the key is **absent** from `GET /metrics` and **`null`** on `GET /replication/lsn`.

The three replication-slot gauges are sampled every ~10 s by an **engine-owned** sampler on a
**pooled** connection (never a dedicated one), and the same sample feeds StatsD — so the numbers are
there with or without `ELECTRIC_STATSD_HOST`.

The replication client acknowledges a server keepalive's WAL end only while it is between
transactions. This releases forced/archived WAL during otherwise-idle periods without weakening the
append-before-ack rule: a keepalive observed while a transaction is buffered cannot advance the
slot, and the commit is still acknowledged only after its final durable-streams chunk lands.

## The change log

Every committed change to every tracked table rides one ordered log, which the ingestor appends to
and the sequencer consumes. That log is **segmented** (`docs/adr/0006-changes-log-segment-rotation.md`):
`changes/0`, `changes/1`, … , never a bare `changes` stream.

At a transaction boundary — after the commit's append and its acknowledgement — the ingestor checks
the current segment against `ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES` and
`ELECTRIC_CIRCUITS_CHANGES_SEGMENT_SECS`. Over either budget it creates `changes/<n+1>`, appends one
final **control envelope** naming the successor to `changes/<n>`, closes `changes/<n>` (which
releases every tailing reader at once with `stream-closed`), records the rotation in the durable
catalog, and continues in the new segment. Nothing ever appends to a closed segment: a writer told
otherwise walks forward to the open one. Control envelopes carry `type: "__circuits.control"` — the
`__circuits` schema is reserved, so `ELECTRIC_CIRCUITS_PG_TABLES` refuses to track anything in it —
and every reader drops them **by type, unconditionally**, so they never reach a table's routing or a
shape stream. (Not by position: if the close after the pointer fails, the rotation is retried at the
next commit, so a segment can carry commits after a pointer and end up with two. Readers cross only
on closed-**and**-drained, so the abandoned one is inert.)

Every position in the log is therefore a `(segment, offset)` pair — the sequencer's checkpoint, a
dormant shape's resume state, `GET /tables/{name}/offset`. Comparing offsets alone is wrong: an
offset from a later segment can be lexicographically smaller than one from an earlier segment.

Every **data** envelope on the log also carries `headers.schema`: the digest of the table schema it was
decoded under, as 16 hex characters (`docs/adr/0010-change-log-envelopes-carry-their-schema-and-processing-fails-closed.md`).
It is the fence that lets the sequencer — which runs behind the ingestor — tell an envelope its
compiled schema *describes* from one encoded before a migration. A shape stream's envelopes never carry
it (it is the engine's own bookkeeping, not part of what a subscriber reads), and neither does anything
in library mode, which has no fingerprint to digest. `GET /table/{name}/schema` reports the digest a
table is compiled under as `schemaDigest`.

**A change the engine cannot process stops ingest into shapes.** If an envelope whose stamp matches the
schema about to decode it will not decode, that is an engine bug or corrupt storage, so the sequencer
**parks** at it: the transaction is unwound (no partial flush, no count fold, the de-duplication
highwater restored), the read cursor is rewound to the replay boundary, nothing is published or
checkpointed past it — on shutdown either — and the engine latches
`epoch.reason = change_log_unprocessable`, which makes `GET /ready` and `/v1/health` report `degraded`
and every shape route answer 503. It keeps serving commands, so a purge or a reset still works. The
break is **never** reset automatically, whatever `ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS` says: recovery is
`POST /epoch/reset`, which retires every shape, rotates the change log and restarts the replay on the
fresh segment, so nothing before it is ever read again. A restart before that re-derives the same park,
by design.

**Exactly two things are consumed without being decoded**, and both say the same thing — everything that
could have wanted the change is already retired: an envelope whose schema a drift has replaced
(`sequencer_stale_schema_skipped_total`), and one for a table the engine does not compile at all —
dropped, or parked unresolved (`sequencer_unknown_table_skipped_total`). Everything else parks,
including a `type` that is not a canonical `schema.name` (no producer can write one).

The same rule governs the two replay paths, each narrowed to the one schema it holds: a pending shape's
buffered deltas at activation, and a dormant shape's reactivation replay. That second one matters most
— it reads from the shape's resume position to the head of the open segment, so it can reach an
envelope the live loop has not, and will park on. A failure there fails the reactivation (the shape
stays dormant and the read that touched it is refused) rather than reporting a shape live that is short
a change.

A rotated-out segment is **deleted** by the retention sweeper once the **durable** checkpoint (the
last position that actually reached the catalog, not the sequencer's in-memory one) is past it and
no shape resumes inside it. A dormant shape that would pin a segment for longer than
`ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS` is evicted first (the ordinary close-then-delete retirement),
which is what unpins it; a shape whose reactivation is replaying pins just as hard and is never
evicted out from under the replay. The current segment is never deleted, and neither is one the
durable checkpoint has not passed. `GET /metrics` reports `changes_rotations_total`,
`changes_segments_deleted_total` and the `changes_segments_retained` gauge.

If the process dies between closing a segment and recording the rotation, the next boot finds the
catalog's segment closed and walks forward to the open one, writing the record the crash lost. Only
the **writer** walks: a reader that finds its segment closed steps to exactly the next one, because
everything in between is changes it has not read. A closed segment with **no** successor cannot be
produced by the engine (rotation creates the successor first), so that state is refused loudly rather
than skipped past — as is a boot whose restored position, or whose recorded current segment, names a
stream storage no longer has.

### Large transactions

A transaction is only appendable once its `Commit` frame arrives (before that the commit LSN is
unknown and the transaction may still abort), so everything between `Begin` and `Commit` has to be
held somewhere — and a million-row `UPDATE` under `REPLICA IDENTITY FULL` carries old **and** new for
every row. The ingestor bounds that (`docs/adr/0003-ingest-pgoutput-v1-with-spill.md`): it buffers
`Envelope` structs (nothing is serialized on the way in, so an ordinary commit costs what it always
did), measures them as held memory, and once that reaches `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES`
serializes the whole buffer out to one temporary file under `ELECTRIC_CIRCUITS_TXN_SPILL_DIR`
(newline-delimited JSON, mode 0600 in a 0700 directory), releases the memory, and writes every
further change of that transaction straight to the file. Peak **ingestor** memory is then the cap
plus one chunk, for a transaction of any size — a bound on the ingestor, not on the engine: the
sequencer's read page, the run it holds, and its per-transaction pending appends are still bounded by
the transaction's size. **Transaction size never invalidates anything** — there is no fail-loud
branch, no shape is retired and nothing is purged for being big. The spill directory is probed at
boot; an unwritable one refuses the boot rather than failing every large commit.

At the commit the transaction is streamed back out in order, stamped with `(lsn, txid, seq)` (`seq`
contiguous `0..n` across every chunk), and appended in chunks of at most
`ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES` to the segment that was current when the commit began. The
slot is acknowledged — and `GET /replication/lsn` advanced, and the drain barrier's sentinel
released — **only after the last chunk has landed**; a failure on any chunk tears the connection down
unacknowledged, so Postgres re-delivers the whole transaction and the sequencer's `(lsn, seq)`
de-duplication discards the chunks that already landed. Rotation remains a transaction-boundary
decision, so a segment never splits a commit.

**Chunking stays invisible to subscribers.** Durable-streams exposes each append atomically, so a
reader long-polling the segment tail sees chunk 1 on its own; fanning that out would flush a fraction
of a transaction to the shape streams. The ingestor therefore marks the **last envelope of every
transaction** — `headers.last`, set on single-chunk commits too, so "no marker" always means
"incomplete" — and the sequencer holds back a trailing run that no marker terminates, carrying it
into the next read and processing the transaction only once the marker arrives. A re-delivered prefix
of the held transaction folds in by `seq`; that filter is confined to the page's leading run while it
belongs to the held transaction, because a reconnect can re-deliver earlier complete commits first
and their seqs restart from 0. If the held transaction is not what comes next at all (that reconnect,
or an epoch reset), the fragment is discarded — it will arrive again in full.

While a run is held, nothing is published past the page it began in: the restart point,
`GET /tables/{name}/offset`, the segment-deletion floor and the resume position of a shape going
dormant all stay there, so a crash (or a park) re-reads the whole transaction. A page that completes
one held run and starts another re-pins to its own page, so a catch-up over consecutive chunked
commits keeps checkpointing. The checkpoint carries the de-duplication highwater alongside the
position and is written whenever either moves, so a prefix that was applied before a crash is not
applied twice.

The spill file is scratch, not state: it is removed at commit, at abort and on connection teardown,
and a file left by a process that died mid-transaction is swept at the next boot by pid liveness.
Pids only mean something inside one pid namespace, so a spill directory must belong to exactly one
engine — give each engine its own `ELECTRIC_CIRCUITS_TXN_SPILL_DIR`. `GET /metrics` reports
`txn_spills_total`, `txn_spill_bytes` and `txn_chunked_appends_total`.

## Schema changes

The engine never keeps serving rows over a schema Postgres no longer has (`docs/adr/0005-schema-drift-retires-per-table.md`).
The compiled schema of each table carries a fingerprint — its live columns in `attnum` order with
`(name, type OID, typmod)`, plus `relreplident` and the primary key — and four things are compared
against it: the pgoutput `Relation` message Postgres re-sends after any DDL, that message's replica
identity, `TRUNCATE`, and the background reconciler (`ELECTRIC_CIRCUITS_SCHEMA_RECONCILE_SECS`, for
DDL that no write follows). (The `Relation` message cannot describe a primary key — under
`REPLICA IDENTITY FULL` every column is flagged as part of the identity — so a PK change is caught by
the reconciler, not on the wire.)

**Schema drift** (a column added, dropped, retyped or reordered; a PK change; an identity that
regressed from `FULL`, which is re-asserted first) re-introspects the table, **retires every
dependent shape** — including aggregates and any subquery shape whose predicate references it — by
closing then deleting their streams, swaps the compiled schema everywhere, and records
`schemaChanged` in the durable catalog. **`TRUNCATE`** retires the same dependents and stops there:
nothing about the schema changed, so there is nothing to re-introspect, swap or record. Clients treat
the closed stream exactly as they treat eviction: re-subscribe, and the new shape backfills through
the new schema. A create that was already in flight when a drift retired its table is refused
(`schema of <t> changed during creation; retry`) and rolled back rather than installed against a
schema that is gone.

**The sequencer's side of a drift.** It reads the log behind the ingestor, so by the time it reaches the
changes a migrating transaction was interleaved with, the compiled schema has already been swapped and
those envelopes describe a table that no longer exists. They are **consumed without being decoded** —
recognised by the schema digest each envelope carries, not by position (the swap happens
mid-transaction, so no position separates old from new) — and counted as
`sequencer_stale_schema_skipped_total`. That is correct precisely because every shape that could have
wanted them was retired by the same drift. Any other envelope that will not decode parks the sequencer
instead (see *The change log*, ADR-0010).

Granularity is per table: a migration on one table never resyncs another. The one exception is a
table with a counts pipeline (`ELECTRIC_CIRCUITS_DBSP_COUNTS`) — the circuit is built and seeded once
at boot with no runtime rebuild, so once the retirements and catalog records have landed the process
exits `75` to be restarted; boot re-seeds the circuit and the catalog restores every other table's
shapes. This applies to `TRUNCATE` as much as to drift (a truncate emits no per-row envelopes, so the
pipeline would otherwise keep its pre-truncate groups).

**Migrations applied while the engine is down** are seen by nothing on the live path. Each shape
record carries the fingerprint its table had when the shape was created, and the catalog restore
retires any shape whose table no longer matches — its retained stream holds rows shaped by the old
schema and can never be brought up to date. A table that **leaves the compiled set** retires its
shapes the same way, and only its shapes: one dropped while the engine was down under a wildcard
selector (`schema.*`, including the default `public.*`), or one removed from
`ELECTRIC_CIRCUITS_PG_TABLES`. A table dropped while an explicit `ELECTRIC_CIRCUITS_PG_TABLES` entry
still names it is different: the boot refuses (exit `78`) while preparing that table, before the
restore runs — the setting names something that does not exist, which is an operator's to fix.

**Unresolved tables.** If the drift cannot be settled — Postgres unreachable, a catalog read that
errored, or an `ALTER … REPLICA IDENTITY FULL` that could not get its `ACCESS EXCLUSIVE` lock within
5 s (the wait is bounded so one long reader cannot stall *all* ingest) — the table is parked as
**unresolved**: dependents retired, its changes dropped, and `POST /shapes` / `POST /aggregate` on it
refused with `schema of <t> is unresolved after a change; retry later`. A per-table retry task keeps
attempting the resolution (2 s → 30 s backoff) regardless of the reconciler setting, and the first
successful re-introspection un-parks it. `GET /tables` lists every tracked table with its
`unresolved` flag, and `schema_unresolved_total` counts the parkings.

**Publication requirements.** The engine needs **whole rows** on the wire: a `Relation` message that
describes fewer columns than the catalog holds can never be reconciled with the table's schema. At
boot the engine therefore refuses to start if its publication carries a per-table **column list**, and
reads `pg_publication.pubgencols` (PG18+) so that **stored generated columns** are included in the
schema fingerprint exactly when the publication publishes them. The engine's own `<slot>_pub` is
`FOR ALL TABLES`, which satisfies both.

A table that is **dropped** has its dependents retired and is untracked; a table re-created under the
same name is not synced again until the engine restarts (same as adding a table).

## Replication slot and epochs

The engine records **which slot, in which cluster, it is bound to** (`docs/adr/0004-slot-epoch-and-reset.md`).
The first time it creates — or adopts — its slot it writes a `slotBound`
(`system_identifier`, `timeline_id`, `slot`, `bound_at`) to the durable catalog; the last such record
is the current **epoch**, and every shape in that catalog belongs to it. Before *every* connection —
at boot and on each ingestor reconnect, not just at boot — the slot is verified against that binding.

A durable catalog the engine cannot **read** at boot is fatal, by design: it is not a catalog with no
epoch in it, and treating it as one would create a slot at the current WAL head and orphan every shape
already in the log. The engine refuses to start until storage is healthy.

An **epoch break** is a slot the engine can no longer vouch for: it is gone, `wal_status = 'lost'`,
it is there under a different output plugin, or `pg_control_system().system_identifier` is not the
one the binding names. There is no way to fill the resulting gap — the changes between the old slot's
`confirmed_flush_lsn` and the current WAL head simply are not available — so every shape over it is
wrong. (Two look-alikes that are **not** breaks: a slot held by another walsender, which is a second
engine on the same slot and is waited out; and a `timeline_id` that moved, which is logged and
recorded but not acted on — one primary, no promotion, per the ADR.)

- **`ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS=true`** (default, Electric parity): the engine **resets**.
  Every shape — active and dormant — is retired (stream closed, then deleted; ADR-0007), the slot is
  dropped and recreated, and a new `slotBound` is written. That record *is* the new epoch. Clients see
  closed streams and re-subscribe, exactly as for eviction or schema drift. An unattended deployment
  heals itself, at the cost of an unscheduled backfill storm.
- **`…=false`**: the engine **refuses**. Ingest does not start, `/v1/health` reports `degraded` (503),
  every shape route answers 503, and `GET /replication/lsn` names the reason
  (`epoch.state = "broken"`, `epoch.reason` one of `slot_lost` / `slot_wal_lost` /
  `system_identifier_mismatch`). Nothing is destroyed while refusing. Recovery is a deliberate act:
  `POST /epoch/reset` runs exactly the reset above and resumes ingest.

A fourth reason ends an epoch without the slot being at fault: `change_log_unprocessable` (ADR-0010,
above). It is **never** auto-reset — under either policy, and not on the ingestor's reconnect — because
a reset destroys every shape and doing that in reply to an engine bug would do it on a loop. The reset
that recovers it keeps the slot (it is healthy, and our own walsender is streaming from it) and rebinds
it, which is what ends the old epoch.

**Every reset moves the sequencer's replay start.** After retiring the shapes and force-rotating the
change log, the reset restarts the replay at the beginning of the fresh segment and records that
position durably alongside the drops — so nothing written under the epoch that ended is ever read
again, including the envelope a parked sequencer stopped on. The old segments hold changes for shapes
that no longer exist, whichever reason ended the epoch.

Reconnects (and refusals) back off exponentially with jitter, 1 s → 30 s. The schedule resets only
when a connection actually **delivered** — a `START_REPLICATION` the server rejected (the slot is held
by another walsender, say), an auth failure or a `pg_hba` rule all climb to the ceiling rather than
retrying at the floor. An operator's reset cuts the wait short rather than leaving the engine asleep. `epoch_breaks_total` and `epoch_resets_total`
count both halves. The slot NAME never changes, so the StatsD slot gauges keep reporting across a
reset. A reset while a counts pipeline is running exits `75` to be restarted, for the same reason
schema drift on a circuit-served table does: the circuit is seeded once at boot and has no runtime
rebuild across the gap.

## Shape retention lifecycle

Shapes follow a three-tier lifecycle (`src/retention.rs`) instead of delete-on-last-unsubscribe —
a deliberate divergence from upstream Electric, which keeps every retained shape actively
maintained:

- **Active** — maintained live. Unsubscribing (`DELETE /shapes/{id}?subscription=…`, a lapsed lease,
  `/v1/shape` handle expiry) does not deactivate; brief reconnects rejoin the same warm stream.
- **Dormant** — after `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS` with no reads and no **live** subscriptions
  (a subscription is live only while renewed within that same window — see "Subscriptions"): engine
  routing state is dropped, the durable stream and shape record are retained at zero engine cost.
  Any touch (rejoin, `/v1/shape` re-snapshot, rows/log read) reactivates by replaying the change
  log from the captured resume position (`(segment, offset)`, following rotation pointers across
  segments) — no Postgres backfill. A dormant shape **pins** its resume segment against deletion;
  one that would pin it for longer than `ELECTRIC_CIRCUITS_CHANGES_RETAIN_SECS` is evicted instead.
  A reactivation that finds a stream it needs gone does not park the shape again: a gone resume
  segment evicts it, and a gone **shape stream** (storage lost it while the shape slept) retires it
  outright — subscribers or not — like any shape whose stream storage confirms gone.
- **Evicted** — record deleted and the stream **retired**: closed, then deleted (see
  `docs/adr/0007-retirement-closes-before-delete.md`), so a client tailing it is released at once
  with `stream-closed` rather than blocking to the long-poll timeout. `/v1/shape` clients get
  `409 must-refetch` (the adapter turns a closed stream into that) and re-snapshot; extended-API
  clients **must** treat `stream-closed`, `404` and `410` alike: re-subscribe. (A **dormant**
  shape's stream is never closed — reactivation appends to it.)

Eviction is layered, least-recently-read first, and **dormant-only** (active shapes are never
evicted): the dormancy TTL (hygiene), the `ELECTRIC_CIRCUITS_MAX_SHAPES` count cap (engine cost bound),
and the disk budget (hard backstop). When a cap/budget is exceeded with nothing dormant to evict,
the engine logs loudly and bumps the `retention_pressure` metric instead of evicting.

Subquery and aggregate shapes never go dormant (their state is not rebuildable from a bounded
replay); once unsubscribed, the TTL layer instead evicts them straight from active after the same
total grace an ordinary shape gets (idle timeout + dormancy TTL). Lifecycle state is in-memory
today — restart recovery (persistent catalog, GH #8) will persist it.
