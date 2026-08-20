// Subset LSN positioning across a SUBQUERY membership move — end-to-end against the live engine.
//
// A changes-only (subset) feed whose predicate is `IN (SELECT …)` receives its membership moves from
// the subquery registry's flip path, not from the plain per-table fan-out. Those emissions are
// re-derivations from a Postgres query-back, and they must still carry the commit LSN of the change
// that CAUSED them: the client's positioning (`mergeFeedDelta`) treats a missing `lsn` as
// "always apply", so an unstamped move-in double-counts against a page that already holds the row,
// and — on the materialized `/v1/shape` path — an unstamped change is discarded outright as
// already-seen once a consumer's dedup frontier has moved off zero.
//
// Sibling of conformance-subset-positioning.test.ts, which covers the same rule for plain
// (non-subquery) feeds. See docs/ARCHITECTURE.md §7 (subset queries and client positioning) and §8
// (the adapter's watermarks).

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import pgpkg from 'pg'
import type { Predicate, Row, Schema, StreamEnvelope } from '@electric-circuits/protocol'
import { lsnToU64, mergeFeedDelta, type SubsetView } from '@electric-circuits/client'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: 'int' }, ws: { type: 'int' } }, primaryKey: 'id' },
    grants: { columns: { id: { type: 'int' }, ws: { type: 'int' } }, primaryKey: 'id' },
  },
}

/** `items.ws IN (SELECT ws FROM grants)` — membership is granted and revoked by the inner table. */
const inGrantedWorkspaces: Predicate = { col: 'ws', in: { table: 'grants', project: 'ws' } }

let h: Harness
beforeEach(async () => {
  h = await bootHarness(schema)
}, 60000)
afterEach(async () => {
  await h?.shutdown()
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
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(`POST ${url} -> ${res.status} ${await res.text()}`)
  return (await res.json()) as T
}

/** Read every envelope currently on a stream (catch-up only; mirrors the engine's non-live ds.read). */
async function readFeed(streamUrl: string): Promise<StreamEnvelope[]> {
  const out: StreamEnvelope[] = []
  let offset = '-1'
  for (let i = 0; i < 100; i++) {
    const res = await fetch(`${streamUrl}?offset=${encodeURIComponent(offset)}`)
    if (res.status === 204) break
    if (!res.ok) throw new Error(`feed read -> ${res.status}`)
    const next = res.headers.get('stream-next-offset')
    const upToDate = res.headers.get('stream-up-to-date') != null
    const text = (await res.text()).trim()
    if (text) out.push(...(JSON.parse(text) as StreamEnvelope[]))
    if (next) offset = next
    if (upToDate || !text) break
  }
  return out
}

describe('subset LSN positioning over a subquery predicate (end-to-end)', () => {
  it('stamps flip-driven move-ins with their source commit LSN, so positioning places them', async () => {
    // 1. Seed: three items in three workspaces, one of them granted. Membership = {item 1}.
    await pg('INSERT INTO items (id, ws) VALUES (1, 10), (2, 20), (3, 30)')
    await pg('INSERT INTO grants (id, ws) VALUES (1, 10)')
    await drainEngine(h)

    // 2. Open the live feed FIRST, so every later membership move is forwarded to it.
    const feed = await postJson<{ shapeId: string; streamUrl: string }>(`${h.engineUrl}/shapes`, {
      table: 'items',
      where: inGrantedWorkspaces,
      changesOnly: true,
    })

    // 3. OVERLAP grant: committed after feed-open but before the page snapshot. Item 3 moves in, so it
    //    appears BOTH on the feed and in the page — its feed copy must carry an LSN below the page's.
    await pg('INSERT INTO grants (id, ws) VALUES (2, 30)')
    await drainEngine(h)

    // 4. Page snapshot at LSN S: the engine evaluates the subquery live, so it already holds item 3.
    const page = await postJson<{ rows: Row[]; lsn: string }>(`${h.engineUrl}/query`, {
      table: 'items',
      where: inGrantedWorkspaces,
      orderBy: { col: 'id' },
      limit: 100,
    })
    const S = lsnToU64(page.lsn)!
    expect(page.rows.map((r) => Number(r.id)).sort((a, b) => a - b)).toEqual([1, 3])

    // 5. POST-snapshot grant: item 2 moves in after S, so the feed is the ONLY way the client learns it.
    await pg('INSERT INTO grants (id, ws) VALUES (3, 20)')
    await drainEngine(h)

    // 6. Every envelope must carry a commit LSN. A flip-driven emission that arrives unstamped is the
    //    regression this test exists for: `mergeFeedDelta` would always-apply it (double-counting the
    //    overlap row), and an Electric consumer would floor it to 0 and discard it.
    const envs = await readFeed(feed.streamUrl)
    expect(envs.length).toBeGreaterThan(0)
    for (const e of envs) expect(e.headers.lsn, `envelope ${e.key} missing lsn`).toBeDefined()

    const overlap = envs.filter((e) => e.key === '3')
    expect(overlap.length, 'the overlap move-in must reach the feed').toBeGreaterThan(0)
    for (const e of overlap) expect(lsnToU64(e.headers.lsn)! < S, `item 3 @${e.headers.lsn} vs S`).toBe(true)

    const late = envs.filter((e) => e.key === '2')
    expect(late.length, 'the post-snapshot move-in must reach the feed').toBeGreaterThan(0)
    expect(late.some((e) => lsnToU64(e.headers.lsn)! >= S)).toBe(true)

    // 7. Replay the feed through the REAL client merge, seeded with the page.
    const present = new Set<string>()
    const applied = new Map<string, bigint>()
    const collection = new Map<string, Row>()
    for (const r of page.rows) {
      const k = String(r.id)
      present.add(k)
      applied.set(k, S)
      collection.set(k, r)
    }
    const view: SubsetView = { snapshotLsn: S, present, applied, inView: () => true }

    let item3FeedWrites = 0
    let item2FeedWrites = 0
    for (const e of envs) {
      const action = mergeFeedDelta(view, e)
      if (e.key === '3' && action) item3FeedWrites++
      if (e.key === '2' && action) item2FeedWrites++
      if (!action) continue
      if (action.type === 'delete') collection.delete(e.key)
      else collection.set(e.key, action.value)
    }

    // 8a. No double-count: the page already holds item 3 (the snapshot saw the grant), and its feed
    //     copy sits below S — positioning drops it.
    expect(item3FeedWrites, 'the overlap move-in must not be applied twice').toBe(0)
    // 8b. The post-snapshot move-in is genuinely new, and must NOT be filtered out.
    expect(item2FeedWrites, 'the post-snapshot move-in must be applied').toBeGreaterThan(0)

    // 9. The merged view equals what Postgres says membership is.
    const oracle = await pg(
      'SELECT id, ws FROM items WHERE ws IN (SELECT ws FROM grants) ORDER BY id',
    )
    const got = [...collection.values()].sort((a, b) => Number(a.id) - Number(b.id))
    expect(got).toEqual(oracle)
  })
})
