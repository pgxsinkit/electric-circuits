// Schema drift, TRUNCATE and replica-identity regression retire every dependent of the affected
// table — and ONLY of that table (ADR-0005). The engine never keeps serving data it knows is wrong:
// after `ALTER TABLE`, the rows already on a shape stream can never gain the new column, so the
// stream is retired (closed, then deleted — ADR-0007) and the client recreates against the fresh
// schema.
//
// Four triggers, one path: a pgoutput `Relation` whose fingerprint moved, a `Relation` whose replica
// identity is no longer FULL, `TRUNCATE`, and the background reconciler (DDL with no following DML,
// which produces no `Relation` message at all). Granularity is per table: a second table's shape must
// keep working across every one of them.
//
// The engine is booted with the reconciler effectively off (1 h) except in the test that exercises
// it, so the other cases genuinely go through the ingest path rather than being swept up by a tick.

import type { Row, Schema, StreamEnvelope } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'
import { createShape, foldStream, pgQuery, sleep, waitFor } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: 'int' }, n: { type: 'int' }, label: { type: 'text' } },
      primaryKey: 'id',
    },
    other: { columns: { id: { type: 'int' }, n: { type: 'int' } }, primaryKey: 'id' },
  },
}

/** Matches every row either table ever holds here — so "the shape is gone" is never "the shape is empty". */
const matchAll = { col: 'n', op: 'gte', value: 0 }

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

/** Boot with the reconciler parked at an hour unless the test is the one testing it. */
async function boot(engineEnv: Record<string, string> = {}): Promise<Harness> {
  h = await bootHarness(schema, {
    engineEnv: { ELECTRIC_CIRCUITS_SCHEMA_RECONCILE_SECS: '3600', ...engineEnv },
  })
  return h
}

const pg = (sql: string, params: unknown[] = []) => pgQuery(h!, sql, params)

async function shapeStatus(id: string): Promise<number> {
  return (await fetch(`${h!.engineUrl}/shapes/${encodeURIComponent(id)}`)).status
}

async function streamStatus(streamUrl: string): Promise<number> {
  return (await fetch(streamUrl)).status
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

/** Every envelope on a stream, in order (the live envelope's `value` is what a new column shows up in). */
async function readEnvelopes(streamUrl: string): Promise<StreamEnvelope[]> {
  const out: StreamEnvelope[] = []
  let offset = '-1'
  for (let i = 0; i < 100; i++) {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) break
    if (!res.ok) throw new Error(`GET ${streamUrl} -> ${res.status}`)
    const body = (await res.text()).trim()
    out.push(...(body ? (JSON.parse(body) as StreamEnvelope[]) : []))
    const next = res.headers.get('stream-next-offset')
    const upToDate = res.headers.get('stream-up-to-date') !== null
    if (!next || next === offset) break
    offset = next
    if (upToDate) break
  }
  return out
}

/** Retirement is asynchronous w.r.t. the drain barrier's last stage; give it a moment to land. */
async function waitRetired(shapeId: string, streamUrl: string): Promise<void> {
  await waitFor(async () => (await shapeStatus(shapeId)) === 404, `shape ${shapeId} to be dropped`)
  await waitFor(async () => (await streamStatus(streamUrl)) === 404, `stream ${streamUrl} to be deleted`)
}

/**
 * The other table's shape is untouched by any of this: its stream is still there AND still LIVE —
 * a fresh insert into `other` still arrives on it.
 */
async function expectOtherStillLive(streamUrl: string, id: number): Promise<void> {
  expect(await streamStatus(streamUrl)).toBe(200)
  await pg('INSERT INTO other (id, n) VALUES ($1, $2)', [id, id])
  await drainEngine(h!)
  await waitFor(
    async () => (await foldStream(streamUrl)).has(String(id)),
    `insert ${id} into other to reach its untouched shape`,
  )
}

describe('schema drift retires every dependent of the affected table (ADR-0005)', () => {
  it('ADD COLUMN + a write retires the table\'s shapes; a new shape carries the new column', async () => {
    await boot()
    await pg('INSERT INTO items (id, n, label) VALUES (1, 10, $1)', ['a'])
    await pg('INSERT INTO other (id, n) VALUES (1, 1)')
    await drainEngine(h!)

    const items = await createShape(h!, { table: 'items', where: matchAll })
    const other = await createShape(h!, { table: 'other', where: matchAll })
    expect(await foldStream(items.streamUrl)).toHaveProperty('size', 1)

    // ADR-0007: park a live read at the tail — the retirement must release it with `stream-closed`
    // rather than leave it blocked until the 30 s long-poll timeout.
    const tail = await tailOffset(items.streamUrl)
    const started = Date.now()
    const parked = fetch(`${items.streamUrl}?offset=${encodeURIComponent(tail)}&live=long-poll`)
    await sleep(250)

    await pg('ALTER TABLE items ADD COLUMN extra text')
    await pg('INSERT INTO items (id, n, label, extra) VALUES (2, 20, $1, $2)', ['b', 'x'])
    await drainEngine(h!)

    const released = await parked
    await released.text()
    expect(Date.now() - started).toBeLessThan(15000)
    expect(released.headers.get('stream-closed')).toBe('true')

    await waitRetired(items.shapeId, items.streamUrl)
    await expectOtherStillLive(other.streamUrl, 2)

    // A NEW shape on `items` sees the new column — in its backfill AND in the live envelope.
    const fresh = await createShape(h!, { table: 'items', where: matchAll })
    const backfilled = await foldStream(fresh.streamUrl)
    expect([...backfilled.keys()].sort()).toEqual(['1', '2'])
    expect(backfilled.get('2')).toMatchObject({ extra: 'x' })
    expect(backfilled.get('1')).toMatchObject({ extra: null })

    await pg('INSERT INTO items (id, n, label, extra) VALUES (3, 30, $1, $2)', ['c', 'y'])
    await drainEngine(h!)
    await waitFor(async () => (await foldStream(fresh.streamUrl)).has('3'), 'the live insert to arrive')
    const live = (await readEnvelopes(fresh.streamUrl)).filter((e) => e.key === '3')
    expect(live).toHaveLength(1)
    expect(live[0]!.value).toMatchObject({ id: 3, extra: 'y' })
  }, 120000)

  it('TRUNCATE retires the truncated table\'s shapes and leaves the other table alone', async () => {
    await boot()
    await pg('INSERT INTO items (id, n, label) VALUES (1, 10, $1), (2, 20, $2)', ['a', 'b'])
    await pg('INSERT INTO other (id, n) VALUES (1, 1)')
    await drainEngine(h!)

    const items = await createShape(h!, { table: 'items', where: matchAll })
    const other = await createShape(h!, { table: 'other', where: matchAll })
    expect(await foldStream(items.streamUrl)).toHaveProperty('size', 2)

    await pg('TRUNCATE items')
    await drainEngine(h!)

    await waitRetired(items.shapeId, items.streamUrl)
    await expectOtherStillLive(other.streamUrl, 2)

    // Recreating works and reflects the truncation (an empty table), not the retained rows.
    const fresh = await createShape(h!, { table: 'items', where: matchAll })
    expect(await foldStream(fresh.streamUrl)).toHaveProperty('size', 0)
  }, 120000)

  it('a replica-identity regression retires the shapes AND restores REPLICA IDENTITY FULL', async () => {
    await boot()
    await pg('INSERT INTO items (id, n, label) VALUES (1, 10, $1)', ['a'])
    await pg('INSERT INTO other (id, n) VALUES (1, 1)')
    await drainEngine(h!)

    const items = await createShape(h!, { table: 'items', where: matchAll })
    const other = await createShape(h!, { table: 'other', where: matchAll })

    await pg('ALTER TABLE items REPLICA IDENTITY DEFAULT')
    expect(await pg(`select relreplident::text as r from pg_class where oid = 'public.items'::regclass`))
      .toEqual([{ r: 'd' }])

    // The UPDATE is what makes Postgres re-send the `Relation` message carrying the regressed identity.
    await pg('UPDATE items SET n = 11 WHERE id = 1')
    await drainEngine(h!)

    await waitRetired(items.shapeId, items.streamUrl)
    await waitFor(
      async () =>
        (await pg(`select relreplident::text as r from pg_class where oid = 'public.items'::regclass`))[0]
          ?.r === 'f',
      'REPLICA IDENTITY FULL to be re-asserted',
    )
    await expectOtherStillLive(other.streamUrl, 2)

    // ...and the engine is serving the table properly again: a recreated shape sees move-outs, which
    // is exactly what a non-FULL identity would have lost.
    const fresh = await createShape(h!, { table: 'items', where: { col: 'n', op: 'gte', value: 10 } })
    expect([...(await foldStream(fresh.streamUrl)).keys()]).toEqual(['1'])
    await pg('UPDATE items SET n = 0 WHERE id = 1')
    await drainEngine(h!)
    await waitFor(async () => !(await foldStream(fresh.streamUrl)).has('1'), 'the move-out to be retracted')
  }, 120000)

  it('DDL with no following DML is caught by the reconciler', async () => {
    await boot({ ELECTRIC_CIRCUITS_SCHEMA_RECONCILE_SECS: '1' })
    await pg('INSERT INTO items (id, n, label) VALUES (1, 10, $1)', ['a'])
    await pg('INSERT INTO other (id, n) VALUES (1, 1)')
    await drainEngine(h!)

    const items = await createShape(h!, { table: 'items', where: matchAll })
    const other = await createShape(h!, { table: 'other', where: matchAll })

    // No write follows: nothing on the replication stream mentions `items` again, so only the
    // reconciler's catalog read can notice. A shape created after this would otherwise backfill
    // through the stale compiled schema and silently omit `extra`.
    await pg('ALTER TABLE items ADD COLUMN extra text')
    await waitRetired(items.shapeId, items.streamUrl)
    await expectOtherStillLive(other.streamUrl, 2)

    const fresh = await createShape(h!, { table: 'items', where: matchAll })
    expect(await foldStream(fresh.streamUrl)).toHaveProperty('size', 1)
    expect((await foldStream(fresh.streamUrl)).get('1')).toMatchObject({ extra: null })
  }, 120000)

  // DDL applied while the engine is DOWN is seen by nothing on the live path — no `Relation`
  // message, no reconciler tick. The shape record's own fingerprint is the only witness, and the
  // catalog restore compares it with what boot introspected.
  //
  // The ALTER runs just before the kill rather than between kill and start: with the reconciler
  // parked at an hour and no write after it, the live engine has no trigger that could notice, so
  // this is the same situation from the catalog's point of view — and it needs no harness surgery.
  it('a migration the engine slept through retires the stale shape at restore', async () => {
    await boot()
    await pg('INSERT INTO items (id, n, label) VALUES (1, 10, $1)', ['a'])
    await pg('INSERT INTO other (id, n) VALUES (1, 1)')
    await drainEngine(h!)

    const items = await createShape(h!, { table: 'items', where: matchAll })
    const other = await createShape(h!, { table: 'other', where: matchAll })
    expect(await foldStream(items.streamUrl)).toHaveProperty('size', 1)

    await pg('ALTER TABLE items ADD COLUMN extra text')
    await h!.restartEngine()

    // The restored engine refuses to resume a shape whose table moved underneath it.
    await waitRetired(items.shapeId, items.streamUrl)
    // The untouched table's shape restores as normal and is still live.
    expect(await shapeStatus(other.shapeId)).toBe(200)
    await expectOtherStillLive(other.streamUrl, 2)

    // ...and a new shape on `items` carries the column added while the engine was down.
    const fresh = await createShape(h!, { table: 'items', where: matchAll })
    const rows = await foldStream(fresh.streamUrl)
    expect([...rows.keys()]).toEqual(['1'])
    expect(rows.get('1')).toMatchObject({ extra: null })
    await pg('INSERT INTO items (id, n, label, extra) VALUES (2, 20, $1, $2)', ['b', 'x'])
    await drainEngine(h!)
    await waitFor(async () => (await foldStream(fresh.streamUrl)).has('2'), 'the live insert to arrive')
    expect((await foldStream(fresh.streamUrl)).get('2')).toMatchObject({ extra: 'x' })
  }, 120000)

  it('a table no longer selected at restart retires only its shapes; the other table restores', async () => {
    // The harness reads `engineEnv` at every start, so changing it here changes the restarted engine.
    const engineEnv: Record<string, string> = { ELECTRIC_CIRCUITS_SCHEMA_RECONCILE_SECS: '3600' }
    h = await bootHarness(schema, { engineEnv })
    await pg('INSERT INTO items (id, n, label) VALUES (1, 10, $1)', ['a'])
    await pg('INSERT INTO other (id, n) VALUES (1, 1)')
    await drainEngine(h!)
    const items = await createShape(h!, { table: 'items', where: matchAll })
    const other = await createShape(h!, { table: 'other', where: matchAll })

    // `items` leaves the compiled set, exactly as if it had been dropped while the engine was down.
    // Its shapes cannot resume on a table the engine no longer has; that retires them — per table
    // (ADR-0005, ADR-0009) — and must never fail the restore, which would take `other` down too.
    engineEnv.ELECTRIC_CIRCUITS_PG_TABLES = 'other'
    await h!.restartEngine()

    await waitRetired(items.shapeId, items.streamUrl)
    expect(await shapeStatus(other.shapeId)).toBe(200)
    await expectOtherStillLive(other.streamUrl, 2)
  }, 120000)

  it('DROP COLUMN retires the shapes; the new shape works without the column', async () => {
    await boot()
    await pg('INSERT INTO items (id, n, label) VALUES (1, 10, $1)', ['a'])
    await pg('INSERT INTO other (id, n) VALUES (1, 1)')
    await drainEngine(h!)

    const items = await createShape(h!, { table: 'items', where: matchAll })
    const other = await createShape(h!, { table: 'other', where: matchAll })
    expect((await foldStream(items.streamUrl)).get('1')).toMatchObject({ label: 'a' })

    await pg('ALTER TABLE items DROP COLUMN label')
    await pg('INSERT INTO items (id, n) VALUES (2, 20)')
    await drainEngine(h!)

    await waitRetired(items.shapeId, items.streamUrl)
    await expectOtherStillLive(other.streamUrl, 2)

    const fresh = await createShape(h!, { table: 'items', where: matchAll })
    const rows = await foldStream(fresh.streamUrl)
    expect([...rows.keys()].sort()).toEqual(['1', '2'])
    for (const row of rows.values()) expect(Object.keys(row as Row)).not.toContain('label')

    // A predicate over the dropped column is now a client error, not a wrong answer.
    const res = await fetch(`${h!.engineUrl}/shapes`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'items', where: { col: 'label', op: 'eq', value: 'a' } }),
    })
    expect(res.ok).toBe(false)
    await res.text()
  }, 120000)
})
