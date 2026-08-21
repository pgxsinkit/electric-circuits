// Helpers for driving the engine through its NATIVE surface only — `POST /shapes`, SQL against the
// system of record, and raw durable-streams reads. No Electric adapter, no `@electric-circuits/client`,
// no `headers.lsn`: this is the path a consumer that dedups on ds offsets and aligns on
// `GET /replication/lsn` actually exercises.

import pgpkg from 'pg'
import type { Row, StreamEnvelope } from '@electric-circuits/protocol'

import type { Harness } from './harness.js'

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

export async function pgQuery(h: Harness, sql: string, params: unknown[] = []): Promise<Row[]> {
  const c = new pgpkg.Client({ connectionString: h.pgUrl })
  await c.connect()
  try {
    return (await c.query(sql, params)).rows as Row[]
  } finally {
    await c.end().catch(() => {})
  }
}

export interface ShapeResp {
  shapeId: string
  streamPath: string
  streamUrl: string
}

/** `POST /shapes` as the control plane does it: the body is the shape definition, the answer a handle. */
export async function createShape(h: Harness, body: unknown, signal?: AbortSignal): Promise<ShapeResp> {
  const res = await fetch(`${h.engineUrl}/shapes`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    ...(signal ? { signal } : {}),
  })
  if (!res.ok) throw new Error(`POST /shapes -> ${res.status} ${await res.text()}`)
  return (await res.json()) as ShapeResp
}

export async function waitFor(cond: () => Promise<boolean>, what: string, timeoutMs = 20000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await cond()) return
    await sleep(50)
  }
  throw new Error(`timed out waiting for ${what}`)
}

/** Fold a shape stream (raw durable-streams reads, from the start) into its current key -> row map. */
export async function foldStream(streamUrl: string): Promise<Map<string, Row>> {
  const rows = new Map<string, Row>()
  let offset = '-1'
  for (let i = 0; i < 100; i++) {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) break
    if (!res.ok) throw new Error(`GET ${streamUrl} -> ${res.status}`)
    const body = (await res.text()).trim()
    const envs: StreamEnvelope[] = body ? (JSON.parse(body) as StreamEnvelope[]) : []
    for (const env of envs) {
      if (env.headers.operation === 'delete') rows.delete(env.key)
      else if (env.value) rows.set(env.key, env.value as Row)
    }
    const next = res.headers.get('stream-next-offset')
    const upToDate = res.headers.get('stream-up-to-date') !== null
    if (!next || next === offset) break
    offset = next
    if (upToDate) break
  }
  return rows
}

export async function streamKeys(streamUrl: string): Promise<string[]> {
  return [...(await foldStream(streamUrl)).keys()].sort((a, b) => Number(a) - Number(b))
}

/**
 * Hold `ACCESS EXCLUSIVE` on a table in an open transaction until `release()`. Any engine statement
 * that reads the table (a backfill SELECT, a membership query-back) blocks on it — a way to park the
 * engine at a chosen point of a create or a propagation without touching engine internals.
 */
export async function lockTable(h: Harness, table: string): Promise<{ release: () => Promise<void> }> {
  const c = new pgpkg.Client({ connectionString: h.pgUrl })
  await c.connect()
  await c.query('BEGIN')
  await c.query(`LOCK TABLE ${table} IN ACCESS EXCLUSIVE MODE`)
  let released = false
  return {
    release: async () => {
      if (released) return
      released = true
      await c.query('COMMIT').catch(() => {})
      await c.end().catch(() => {})
    },
  }
}

/** Backends in this database currently waiting on a heavyweight lock (the engine statement parked by `lockTable`). */
export async function lockWaiters(h: Harness): Promise<number[]> {
  const rows = await pgQuery(
    h,
    `SELECT pid FROM pg_stat_activity
      WHERE datname = current_database() AND wait_event_type = 'Lock' AND pid <> pg_backend_pid()`,
  )
  return rows.map((r) => Number(r.pid))
}

/** Backends waiting to acquire a relation lock on one specific table. */
export async function tableLockWaiters(h: Harness, table: string): Promise<number[]> {
  const rows = await pgQuery(
    h,
    `SELECT DISTINCT a.pid
       FROM pg_stat_activity AS a
       JOIN pg_locks AS l ON l.pid = a.pid AND NOT l.granted
       JOIN pg_class AS c ON c.oid = l.relation
      WHERE a.datname = current_database() AND c.relname = $1 AND a.pid <> pg_backend_pid()`,
    [table],
  )
  return rows.map((r) => Number(r.pid))
}

export async function waitForLockWaiter(h: Harness, what: string): Promise<void> {
  await waitFor(async () => (await lockWaiters(h)).length > 0, what)
}
