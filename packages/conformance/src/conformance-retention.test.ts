// Shape retention lifecycle — end-to-end against the live engine (GH issue #9): releasing the last
// subscriber retains the shape; an idle unsubscribed shape goes DORMANT (engine state dropped,
// stream + record kept); any touch REACTIVATES it by replaying the table stream from the resume
// offset (no Postgres backfill) — including changes that happened while dormant; and the dormancy
// TTL eventually EVICTS it (stream + record deleted, rejoin creates a fresh shape).
//
// The engine is booted with second-scale retention knobs (see `apps/engine/README.md` for the
// production defaults: 30 min idle, 7 day TTL, 60 s sweep).

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import pgpkg from 'pg'
import type { Row, Schema, StreamEnvelope } from '@electric-circuits/protocol'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: { items: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' } },
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

let h: Harness
beforeEach(async () => {
  h = await bootHarness(schema, {
    engineEnv: {
      ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS: '1',
      ELECTRIC_CIRCUITS_SHAPE_DORMANT_TTL_SECS: '4',
      ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS: '1',
    },
  })
})
afterEach(async () => {
  await h.shutdown()
})

async function pg(sql: string, params: unknown[] = []): Promise<Row[]> {
  const c = new pgpkg.Client({ connectionString: h.pgUrl })
  await c.connect()
  try {
    return (await c.query(sql, params)).rows as Row[]
  } finally {
    await c.end().catch(() => {})
  }
}

async function postJson<T>(url: string, body: unknown): Promise<T> {
  const res = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) })
  if (!res.ok) throw new Error(`POST ${url} -> ${res.status} ${await res.text()}`)
  return (await res.json()) as T
}

interface ShapeResp {
  shapeId: string
  streamPath: string
  streamUrl: string
}

const createShape = (where: unknown) =>
  postJson<ShapeResp>(`${h.engineUrl}/shapes`, { table: 'items', where })

const release = (id: string) => fetch(`${h.engineUrl}/shapes/${id}`, { method: 'DELETE' })

/** GET /shapes/{id} — a pure metadata lookup (deliberately NOT a retention touch on the engine). */
async function shapeInfo(id: string): Promise<{ status: number; state?: string }> {
  const res = await fetch(`${h.engineUrl}/shapes/${encodeURIComponent(id)}`)
  if (!res.ok) return { status: res.status }
  const body = (await res.json()) as { state?: string }
  return { status: res.status, state: body.state }
}

async function waitFor(cond: () => Promise<boolean>, what: string, timeoutMs = 20000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await cond()) return
    await sleep(150)
  }
  throw new Error(`timed out waiting for ${what}`)
}

/** Fold a shape stream (raw durable-streams reads) into its current key -> row map. */
async function foldStream(streamUrl: string): Promise<Map<string, Row>> {
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

/** Read a stream to its tail and return the offset a live client would park a long-poll on. */
async function tailOffset(streamUrl: string): Promise<string> {
  let offset = '-1'
  for (let i = 0; i < 100; i++) {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
    if (res.status !== 200 && res.status !== 204) throw new Error(`GET ${streamUrl} -> ${res.status}`)
    await res.text() // drain
    const next = res.headers.get('stream-next-offset')
    const upToDate = res.headers.get('stream-up-to-date') !== null
    if (next && next !== offset) offset = next
    if (upToDate || !next) break
  }
  return offset
}

describe('shape retention lifecycle (active / dormant / evicted)', () => {
  it('idle unsubscribed shape goes dormant; rejoin reactivates with the changes missed while dormant', async () => {
    await pg('INSERT INTO items (id, n) VALUES (1, 10), (2, 20), (3, 5)')
    await drainEngine(h)

    const where = { col: 'n', op: 'gte', value: 10 }
    const a = await createShape(where)
    expect((await shapeInfo(a.shapeId)).state).toBe('active')

    // Releasing the last subscriber retains the shape (no teardown), and with nobody reading it
    // the idle timer (1 s here) moves it to dormant on a subsequent sweep.
    await release(a.shapeId)
    expect((await shapeInfo(a.shapeId)).status).toBe(200)
    await waitFor(async () => (await shapeInfo(a.shapeId)).state === 'dormant', 'shape to go dormant')

    // Mutate while dormant: enter (id 4), leave (id 1 drops below), delete (id 2). None of these
    // reach the retained stream yet — the shape has no engine state.
    await pg('INSERT INTO items (id, n) VALUES (4, 40)')
    await pg('UPDATE items SET n = 1 WHERE id = 1')
    await pg('DELETE FROM items WHERE id = 2')
    await drainEngine(h)

    // Rejoin: same retained shape (same id + stream), reactivated by table-stream replay before
    // the create returns — no Postgres backfill.
    const b = await createShape(where)
    expect(b.shapeId).toBe(a.shapeId)
    expect(b.streamPath).toBe(a.streamPath)
    expect((await shapeInfo(a.shapeId)).state).toBe('active')

    // The folded stream now matches Postgres: the dormant-window changes were replayed.
    const rows = await foldStream(b.streamUrl)
    const oracle = await pg('SELECT id, n FROM items WHERE n >= 10 ORDER BY id')
    expect([...rows.keys()].sort()).toEqual(oracle.map((r) => String(r.id)))
    for (const r of oracle) expect(rows.get(String(r.id))?.n).toBe(r.n)

    // And the reactivated shape is live again: a new change flows without another touch.
    await pg('INSERT INTO items (id, n) VALUES (5, 50)')
    await drainEngine(h)
    await waitFor(async () => (await foldStream(b.streamUrl)).has('5'), 'live change after reactivation')
  })

  it('a data read (rows fold) reactivates a dormant shape', async () => {
    await pg('INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)

    const a = await createShape({ col: 'n', op: 'gte', value: 10 })
    await release(a.shapeId)
    await waitFor(async () => (await shapeInfo(a.shapeId)).state === 'dormant', 'shape to go dormant')

    await pg('INSERT INTO items (id, n) VALUES (2, 20)')
    await drainEngine(h)

    // GET /shapes/{id}/rows is a touch: it reactivates (replay) and serves the current contents.
    const res = await fetch(`${h.engineUrl}/shapes/${a.shapeId}/rows`)
    expect(res.status).toBe(200)
    const body = (await res.json()) as { rows: Array<{ key: string }> }
    expect(body.rows.map((r) => r.key).sort()).toEqual(['1', '2'])
    expect((await shapeInfo(a.shapeId)).state).toBe('active')
  })

  it('DELETE ?purge=true immediately removes the native shape and eventually retires its stream', async () => {
    await pg('INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)

    const where = { col: 'n', op: 'gte', value: 10 }
    const a = await createShape(where)
    await createShape(where) // second subscription on the same shape (refcount 2)

    // A plain release leaves the shape (still one subscriber); successful native purge acknowledges
    // its durable Dropped intent, so the shape disappears immediately while process-owned stream
    // retirement completes afterwards.
    const purged = await fetch(`${h.engineUrl}/shapes/${a.shapeId}?purge=true`, { method: 'DELETE' })
    expect(purged.ok).toBe(true)
    expect((await shapeInfo(a.shapeId)).status).toBe(404)
    await waitFor(async () => {
      const stream = await fetch(a.streamUrl)
      return stream.status === 404 || stream.status === 410
    }, 'purged stream to reach a terminal state')

    // Recreation works (fresh shape, fresh stream, fresh backfill).
    const b = await createShape(where)
    expect(b.shapeId).not.toBe(a.shapeId)
    expect((await foldStream(b.streamUrl)).has('1')).toBe(true)
  })

  // Engine-initiated retirement CLOSES the shape stream before deleting it (ADR 0007): a client
  // parked on a long-poll is released immediately with `stream-closed` instead of blocking until the
  // durable-streams long-poll timeout (30 s) and then finding a deleted stream.
  it('purge closes the stream before deleting it: a waiting long-poll is released with stream-closed', async () => {
    await pg('INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)

    const a = await createShape({ col: 'n', op: 'gte', value: 10 })

    // Park a live read at the tail, the position a subscribed client tails from.
    const tail = await tailOffset(a.streamUrl)
    const started = Date.now()
    const pending = fetch(`${a.streamUrl}?offset=${encodeURIComponent(tail)}&live=long-poll`)
    await sleep(250) // let the long-poll actually block on the server before retiring the stream

    await fetch(`${h.engineUrl}/shapes/${a.shapeId}?purge=true`, { method: 'DELETE' })

    const res = await pending
    const elapsed = Date.now() - started
    await res.text() // drain
    // Only the close can free the read this fast — the long-poll timeout is 30 s.
    expect(elapsed).toBeLessThan(5000)
    expect(res.headers.get('stream-closed')).toBe('true')
    // 204 here (the close is the only thing that happened at this offset); a close that lands with
    // pending data returns 200 + the data, carrying the same header.
    expect([200, 204]).toContain(res.status)

    // Retirement is close THEN delete: the stream is gone afterwards.
    expect((await fetch(a.streamUrl)).status).toBe(404)
  })

  it('TTL eviction closes the stream before deleting it: a tailing long-poll is released with stream-closed', async () => {
    await pg('INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)

    const a = await createShape({ col: 'n', op: 'gte', value: 10 })
    await release(a.shapeId)
    await waitFor(async () => (await shapeInfo(a.shapeId)).state === 'dormant', 'shape to go dormant')

    // Tail the retained (dormant) stream — a raw durable-streams read is not an engine touch, so the
    // ~4 s dormancy TTL still evicts underneath it. Eviction is terminal, so it retires the stream.
    const tail = await tailOffset(a.streamUrl)
    const started = Date.now()
    const res = await fetch(`${a.streamUrl}?offset=${encodeURIComponent(tail)}&live=long-poll`)
    const elapsed = Date.now() - started
    await res.text() // drain
    expect(elapsed).toBeLessThan(15000) // TTL-bounded, well inside the 30 s long-poll timeout
    expect(res.headers.get('stream-closed')).toBe('true')
    expect([200, 204]).toContain(res.status)

    await waitFor(async () => (await fetch(a.streamUrl)).status === 404, 'stream deletion')
  })

  // The `/v1/shape` adapter must turn a retired stream into the answer an evicted handle already
  // gets (`409 must-refetch`) — not a 500 once the delete lands, and not a full live-timeout wait
  // (`ELECTRIC_LIVE_TIMEOUT_MS`, 20 s by default).
  it('a live /v1/shape poll is released with 409 must-refetch when the underlying shape is purged', async () => {
    await pg('INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)

    const where = 'n >= 10'
    const snap = await fetch(`${h.engineUrl}/v1/shape?${new URLSearchParams({ table: 'items', offset: '-1', where })}`)
    expect(snap.status).toBe(200)
    const handle = snap.headers.get('electric-handle') as string
    const offset = snap.headers.get('electric-offset') as string
    await snap.text() // drain

    const started = Date.now()
    const live = fetch(
      `${h.engineUrl}/v1/shape?${new URLSearchParams({ table: 'items', offset, handle, live: 'true', where })}`,
    )
    await sleep(250) // let the long-poll park on the durable-streams server

    // A handle is `<shapeId>h<seq>` over a shared engine shape; purge the shape underneath it.
    await fetch(`${h.engineUrl}/shapes/${handle.replace(/h\d+$/, '')}?purge=true`, { method: 'DELETE' })

    const res = await live
    const elapsed = Date.now() - started
    const msgs = (await res.json()) as Array<{ headers: { control?: string } }>
    expect(res.status).toBe(409)
    expect(msgs[0]?.headers.control).toBe('must-refetch')
    expect(elapsed).toBeLessThan(5000)
  })

  it('the dormancy TTL evicts: record 404s, stream is deleted, rejoin creates a fresh shape', async () => {
    await pg('INSERT INTO items (id, n) VALUES (1, 10)')
    await drainEngine(h)

    const where = { col: 'n', op: 'gte', value: 10 }
    const a = await createShape(where)
    await release(a.shapeId)

    // Dormant after ~1 s idle, evicted after a further ~4 s dormancy TTL.
    await waitFor(async () => (await shapeInfo(a.shapeId)).status === 404, 'shape eviction')
    await waitFor(async () => (await fetch(a.streamUrl)).status === 404, 'stream deletion')

    // An extended-API client recreates on 404; the fresh shape is a different object.
    const b = await createShape(where)
    expect(b.shapeId).not.toBe(a.shapeId)
    expect((await foldStream(b.streamUrl)).has('1')).toBe(true)
  })
})
