# 0001 — A refused shape create is not logged

Status: candidate (recorded 2026-09-11)
Opened: 2026-09-11 · Area: `apps/engine/src/http.rs` (`create_shape`, `create_aggregate`,
`impl IntoResponse for AppError`)
Reopen trigger: the next operator who has to work out from the engine's side why a control plane is
getting 4xx from `POST /shapes` — or the first request for per-request tracing on the engine's HTTP
surface.

## The fact

- `create_shape` and `create_aggregate` return `AppError` on refusal (unknown table, bad predicate,
  a table the engine was not configured to replicate, …). `IntoResponse for AppError` turns that into
  `{ "error": msg }` with the status, and **emits no log record at any level**. There is no
  request-logging layer on the router either.
- The caller (pgxsinkit's control plane) maps every engine error to a `503 sync engine unavailable`
  for its own client and, today, drops the body too (pgxsinkit backlog 0018). So at INFO the engine
  says nothing while it refuses the same shape once a second.
- Seen on the emergent dev cluster, 2026-09-11: the engine's `ELECTRIC_CIRCUITS_PG_TABLES` had
  drifted from the client registry; `public.competency_association` was untracked, so every
  `POST /shapes` for the shape on it was refused for over an hour. The engine's log for that hour
  held only the periodic `WARNING: there is no transaction in progress` lines from the metrics poll
  and nothing else — the last real entries were the successful backfills of the tracked tables. The
  refusal was only visible by diffing `GET /tables` against the registry.

## The fix

- Log every `AppError` at `warn!` (status, message, and for shape creates the table and predicate
  summary) when it is converted into a response, or add a `tower_http::TraceLayer` on the router
  that records non-2xx responses with their body. Refusals are rare and operator-actionable; they
  should never be silent.
- While there: the `WARNING: there is no transaction in progress` line every ten seconds is the
  pool's check-in `ROLLBACK` (`pg.rs`, `impl Drop for PooledClient`) fired by the slot-gauge sampler
  (`metrics.rs`, `SLOT_SAMPLE_PERIOD`), which runs a single autocommit `SELECT` and hands the client
  back. Postgres warns on every one, and tokio-postgres surfaces it at INFO. It is noise that buries
  the signal above: skip the `ROLLBACK` when the client is not in a transaction (tokio-postgres does
  not expose that directly; a cheap option is to only issue it for clients that were handed out for
  transactional use), or run the sampler on a dedicated client outside the pool.

## Reopen trigger

Any repeat of a silent refusal storm, or per-request tracing being wanted for another reason. Until
then this is a candidate: the responses themselves are correct, only the operator's view is missing.
