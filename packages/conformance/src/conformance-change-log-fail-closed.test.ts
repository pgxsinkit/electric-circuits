// The sequencer never silently skips a change it cannot process — and never mistakes a pre-drift
// envelope for one (ADR-0010).
//
// Every change-log envelope carries the digest of the schema it was decoded under. That is what lets
// the sequencer, which runs BEHIND the ingestor, tell the one legitimate reason a change will not
// decode from every other one:
//
//   1. a migration the sequencer had not caught up to (ADR-0005 retired that table's shapes and
//      swapped its compiled schema, so the envelopes still on the log describe a table that no longer
//      exists). Those are CONSUMED without being decoded, counted, and reported once — the engine
//      stays healthy and a shape created after the migration converges;
//   2. anything else: the schema the engine holds IS the one the envelope was decoded under, so the
//      failure is an engine bug or corrupt storage. The sequencer PARKS at that envelope — no shape is
//      maintained past it, nothing is published or checkpointed past it, `/ready` says `degraded` and
//      every shape route refuses — and it stays parked across restarts rather than stepping over the
//      change. Recovery is an operator's `POST /epoch/reset`, which retires every shape and restarts
//      the replay on a fresh change-log segment.
//
// Both are driven from outside the engine: SQL, an HTTP proxy in front of durable-streams that holds
// the sequencer's change-log reads, a raw append to the change log, and the engine's own HTTP surface.

import { createServer, request } from 'node:http'

import type { Row, Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { createShape, foldStream, pgQuery, sleep, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
  },
}

/** Matches every row the table ever holds here, so "the shape is gone" is never "the shape is empty". */
const matchAll = { col: 'id', op: 'gte', value: 0 }

/**
 * An ordinary HTTP proxy in front of durable-streams, for the engine process only, that can HOLD the
 * sequencer's change-log reads: the ingestor's appends (POST) pass through untouched, so the engine
 * falls behind its own log exactly as it does under load.
 *
 * The RESPONSE is what is held, not the request: the sequencer's read is a long poll, so the request
 * that will deliver the next page is already in flight before the hold begins.
 */
interface ChangeReadProxy {
  url: string
  holdChangeLogReads(hold: boolean): void
  heldReads(): number
  close(): Promise<void>
}

async function startChangeReadProxy(upstreamUrl: string): Promise<ChangeReadProxy> {
  const upstream = new URL(upstreamUrl)
  let hold = false
  let held = 0
  let waiters: Array<() => void> = []
  const release = () => {
    const pending = waiters
    waiters = []
    for (const resume of pending) resume()
  }
  const gate = () =>
    new Promise<void>((resolve) => {
      if (!hold) return resolve()
      held += 1
      waiters.push(resolve)
    })

  const server = createServer((incoming, outgoing) => {
    const target = new URL(incoming.url ?? '/', upstream)
    const isChangeRead = incoming.method === 'GET' && target.pathname.startsWith('/changes/')
    const forwarded = request(
      target,
      { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
      (response) => {
        const deliver = () => {
          outgoing.writeHead(response.statusCode ?? 502, response.headers)
          response.pipe(outgoing)
        }
        if (isChangeRead) void gate().then(deliver)
        else deliver()
      },
    )
    forwarded.on('error', (error) => {
      if (!outgoing.headersSent) outgoing.writeHead(502, { 'content-type': 'text/plain' })
      outgoing.end(String(error))
    })
    incoming.pipe(forwarded)
  })

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => resolve())
  })
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('change-read proxy did not bind TCP')

  return {
    url: `http://127.0.0.1:${address.port}`,
    holdChangeLogReads: (next) => {
      hold = next
      if (!next) release()
    },
    heldReads: () => held,
    close: async () => {
      hold = false
      release()
      await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())))
    },
  }
}

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

const pg = (sql: string, params: unknown[] = []) => pgQuery(h!, sql, params)

async function counter(name: string): Promise<number> {
  const res = await fetch(`${h!.engineUrl}/metrics`)
  if (!res.ok) throw new Error(`GET /metrics -> ${res.status}`)
  return Number(((await res.json()) as { counters: Record<string, number> }).counters[name] ?? 0)
}

interface ChangeLogFailure {
  position: { segment: number; path: string; offset: string }
  table: string
  key: string
  txid: string | null
  lsn: string | null
  envelopeOffset: number
  error: string
  recovery: string
}

/** The parked envelope as `GET /metrics` reports it (`null` while the engine is processing normally). */
async function changeLogFailure(): Promise<ChangeLogFailure | null> {
  const res = await fetch(`${h!.engineUrl}/metrics`)
  if (!res.ok) throw new Error(`GET /metrics -> ${res.status}`)
  return ((await res.json()) as { changeLogFailure?: ChangeLogFailure }).changeLogFailure ?? null
}

async function readiness(): Promise<{ code: number; status: string }> {
  const res = await fetch(`${h!.engineUrl}/ready`)
  return { code: res.status, status: ((await res.json()) as { status: string }).status }
}

async function epochReason(): Promise<string | null> {
  const res = await fetch(`${h!.engineUrl}/replication/lsn`)
  if (!res.ok) throw new Error(`GET /replication/lsn -> ${res.status}`)
  return ((await res.json()) as { epoch: { reason: string | null } }).epoch.reason
}

/** The digest of the schema a table is compiled under — what an envelope's `headers.schema` names. */
async function tableSchemaDigest(table: string): Promise<string> {
  const res = await fetch(`${h!.engineUrl}/table/${table}/schema`)
  if (!res.ok) throw new Error(`GET /table/${table}/schema -> ${res.status}`)
  const digest = ((await res.json()) as { schemaDigest: string | null }).schemaDigest
  if (!digest) throw new Error(`table ${table} has no schema digest (library mode?)`)
  return digest
}

/** The segment the ingestor is appending to — where an envelope must go to be read next. */
async function currentSegment(): Promise<number> {
  const res = await fetch(`${h!.engineUrl}/replication/lsn`)
  if (!res.ok) throw new Error(`GET /replication/lsn -> ${res.status}`)
  return ((await res.json()) as { changes: { segment: number } }).changes.segment
}

async function shapeStatus(id: string): Promise<number> {
  return (await fetch(`${h!.engineUrl}/shapes/${encodeURIComponent(id)}`)).status
}

async function createStatus(): Promise<number> {
  const res = await fetch(`${h!.engineUrl}/shapes`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ table: 'items', where: matchAll }),
  })
  await res.text()
  return res.status
}

describe('a change the sequencer cannot process (ADR-0010)', () => {
  it('skips the envelopes a migration outran, and keeps serving', async () => {
    let proxy: ChangeReadProxy | undefined
    h = await bootHarness(schema, {
      // The reconciler parked at an hour: the drift must go through the ingest path (the `Relation`
      // message), which is the case where the sequencer can be behind it.
      engineEnv: { ELECTRIC_CIRCUITS_SCHEMA_RECONCILE_SECS: '3600' },
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startChangeReadProxy(upstreamUrl)
        return proxy
      },
    })

    await pg('INSERT INTO items (id, n) VALUES (1, 1)')
    await drainEngine(h)
    const before = await createShape(h, { table: 'items', where: matchAll })
    expect((await foldStream(before.streamUrl)).has('1')).toBe(true)

    // From here the sequencer sees nothing: it is behind its own change log, as it is under load.
    proxy!.holdChangeLogReads(true)
    await pg('UPDATE items SET n = 2 WHERE id = 1')
    await pg('INSERT INTO items (id, n) VALUES (2, 2)')
    // The migration. An int -> text change is what makes the envelopes above undecodable against the
    // schema the engine will hold by the time it reads them.
    await pg('ALTER TABLE items ALTER COLUMN n TYPE text')
    await pg(`UPDATE items SET n = 'three' WHERE id = 1`)

    // The ingestor handles the drift inline, so the old shape is retired while the sequencer is still
    // parked on a read — which is exactly the state this test is about.
    await waitFor(async () => (await shapeStatus(before.shapeId)) === 404, 'the drifted shape to be retired')
    expect(proxy!.heldReads()).toBeGreaterThan(0)

    proxy!.holdChangeLogReads(false)

    // The pre-drift envelopes are consumed rather than decoded: counted, and never a failure.
    await waitFor(
      async () => (await counter('sequencer_stale_schema_skipped_total')) > 0,
      'the pre-drift envelopes to be skipped',
    )
    expect(await changeLogFailure(), 'a drift must never park the sequencer').toBeNull()
    expect(await epochReason()).toBeNull()
    expect((await readiness()).status).toBe('active')

    // ...and the engine is serving the new schema: a shape created after the migration converges with
    // what Postgres holds, live changes included. (Compared against a direct SELECT rather than the
    // harness oracle, whose typed schema still says `n` is an int.)
    const fresh = await createShape(h, { table: 'items', where: matchAll })
    await pg(`INSERT INTO items (id, n) VALUES (3, 'four')`)
    await drainEngine(h)
    await waitFor(async () => (await foldStream(fresh.streamUrl)).has('3'), 'the live insert to arrive')
    const rows = await foldStream(fresh.streamUrl)
    const oracle = (await pg('SELECT id, n FROM items ORDER BY id')) as Row[]
    expect([...rows.keys()].sort()).toEqual(oracle.map((r) => String(r.id)).sort())
    for (const row of oracle) expect(rows.get(String(row.id))).toMatchObject({ id: row.id, n: row.n })
  }, 120000)

  it('parks on an envelope it cannot process, survives a restart there, and is recovered by a reset', async () => {
    h = await bootHarness(schema)
    await pg('INSERT INTO items (id, n) VALUES (1, 1)')
    await drainEngine(h)
    const doomed = await createShape(h, { table: 'items', where: matchAll })
    expect((await foldStream(doomed.streamUrl)).has('1')).toBe(true)

    // An envelope carrying the table's CURRENT schema digest — so the engine's own fence says it
    // should be decodable — and a value the schema forbids. Appended straight to the change log: no
    // engine hook, and nothing Postgres could have produced.
    const digest = await tableSchemaDigest('items')
    const segment = await currentSegment()
    const bad = {
      type: 'public.items',
      key: '99',
      value: { id: 99, n: 'not-an-int' },
      headers: { operation: 'insert', txid: '999999', lsn: 'FF/FFFFFFF0', seq: 0, last: true, schema: digest },
    }
    const appended = await fetch(`${h.dsUrl}/changes/${segment}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify([bad]),
    })
    expect(appended.ok, `appending to changes/${segment} -> ${appended.status}`).toBe(true)

    // Fail closed: degraded by name, with the envelope named, and every shape route refusing.
    await waitFor(async () => (await readiness()).code === 503, 'the engine to report itself degraded')
    expect((await readiness()).status).toBe('degraded')
    expect(await epochReason()).toBe('change_log_unprocessable')
    expect(await createStatus()).toBe(503)
    const failure = (await changeLogFailure())!
    expect(failure.table).toBe('public.items')
    expect(failure.key).toBe('99')
    expect(failure.txid).toBe('999999')
    expect(failure.lsn).toBe('FF/FFFFFFF0')
    expect(failure.position.segment).toBe(segment)
    expect(failure.recovery).toBe('POST /epoch/reset')
    expect(failure.error).toContain('integer')
    // Nothing was destroyed while refusing: recovery is the operator's call.
    expect((await fetch(doomed.streamUrl)).status).toBe(200)

    // Nothing is checkpointed past it, so a restart re-derives the same park instead of looping or
    // stepping over the change.
    await h.restartEngine()
    await waitFor(async () => (await readiness()).code === 503, 'the restarted engine to park again')
    const again = (await changeLogFailure())!
    expect(again.key).toBe('99')
    expect(again.position.segment).toBe(segment)

    // The recovery: every shape retired, a new epoch, and the replay restarted on a fresh segment, so
    // the envelope is never read again.
    const reset = await fetch(`${h.engineUrl}/epoch/reset`, { method: 'POST' })
    expect(reset.status).toBe(200)
    await waitFor(async () => (await readiness()).code === 200, 'the engine to serve again')
    expect((await readiness()).status).toBe('active')
    expect(await changeLogFailure()).toBeNull()
    expect(await epochReason()).toBeNull()
    await waitFor(async () => (await shapeStatus(doomed.shapeId)) === 404, 'the reset to retire the old shape')

    // A shape created in the new epoch converges, live changes included...
    const fresh = await createShape(h, { table: 'items', where: matchAll })
    await pg('INSERT INTO items (id, n) VALUES (2, 2)')
    await drainEngine(h)
    await waitFor(async () => (await foldStream(fresh.streamUrl)).has('2'), 'the live insert to arrive')
    expect([...(await foldStream(fresh.streamUrl)).keys()].sort()).toEqual(['1', '2'])

    // ...and a restart after the reset is clean: the recorded replay start is past the old segment.
    await h.restartEngine()
    await sleep(250)
    expect((await readiness()).status).toBe('active')
    expect(await changeLogFailure()).toBeNull()
  }, 180000)
})
