// Durable Streams WAL recovery: this is intentionally a real-stack contract.  It crosses
// PostgreSQL logical replication, the engine, the Rust durable-streams process, and the public
// client materialization.  The named drain barrier, rather than elapsed time, fences each source
// transaction before the storage crash.

import type { Schema, ShapeDef } from '@electric-circuits/protocol'
import pgpkg from 'pg'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { bootHarness, drainEngine, type Harness, waitForConvergence } from './harness.js'

const schema: Schema = {
  tables: {
    users: {
      columns: { id: { type: 'int' }, name: { type: 'text' }, active: { type: 'bool' } },
      primaryKey: 'id',
    },
  },
}

const def: ShapeDef = { table: 'users', where: { col: 'active', op: 'eq', value: true } }
const target = { def, columns: ['id', 'name', 'active'], pk: 'id' }

/** Execute one source transaction directly against the independent PostgreSQL oracle. */
async function insertUsers(h: Harness, rows: Array<{ id: number; name: string; active: boolean }>): Promise<void> {
  const c = new pgpkg.Client({ connectionString: h.pgUrl })
  await c.connect()
  try {
    await c.query('BEGIN')
    for (const row of rows) {
      await c.query('INSERT INTO users (id, name, active) VALUES ($1, $2, $3)', [row.id, row.name, row.active])
    }
    await c.query('COMMIT')
  } catch (e) {
    await c.query('ROLLBACK').catch(() => {})
    throw e
  } finally {
    await c.end()
  }
}

describe('conformance: ds-rust WAL survives a storage crash and engine recovery', () => {
  let h: Harness

  beforeAll(async () => {
    h = await bootHarness(schema, { durableStreamsDurability: 'wal' })
  }, 60000)

  afterAll(async () => {
    await h?.shutdown()
  })

  it('retains one shape stream across SIGKILL, then converges before and after source transactions', async () => {
    const major = new pgpkg.Client({ connectionString: h.pgUrl })
    await major.connect()
    const version = Number((await major.query('SHOW server_version_num')).rows[0]!.server_version_num)
    await major.end()
    expect(Math.floor(version / 10_000), `PostgreSQL server_version_num=${version}`).toBe(18)

    // This is the real client/materializer path.  It owns a stable server-issued shape and stream;
    // neither is created again after the crash.
    const shape = await h.client.shape(def)
    const identity = { shapeId: shape.handle.shapeId, streamPath: shape.handle.streamPath }

    await insertUsers(h, [
      { id: 1, name: 'before-a', active: true },
      { id: 2, name: 'before-filtered', active: false },
    ])
    await drainEngine(h)
    let compared = await waitForConvergence(h, { shape, ...target })
    expect(compared.equal, JSON.stringify(compared)).toBe(true)
    expect(shape.currentRows().map((row) => String(row.id))).toEqual(['1'])

    // SIGKILLs ds-rust, waits for its exit, restarts the exact binary at the same endpoint/data
    // directory, and only then resolves. It is private to the harness.
    await h.crashAndRestartDurableStreams()
    await h.restartEngine()

    expect(shape.handle.shapeId).toBe(identity.shapeId)
    expect(shape.handle.streamPath).toBe(identity.streamPath)
    expect((await fetch(`${h.dsUrl}/${identity.streamPath}`, { method: 'HEAD' })).status).toBe(200)

    await insertUsers(h, [
      { id: 3, name: 'after-a', active: true },
      { id: 4, name: 'after-filtered', active: false },
    ])
    await drainEngine(h)
    compared = await waitForConvergence(h, { shape, ...target })
    expect(compared.equal, JSON.stringify(compared)).toBe(true)

    const ids = shape.currentRows().map((row) => Number(row.id)).sort((a, b) => a - b)
    // The independent PostgreSQL comparison proves no omissions or wrong values; this explicit
    // cardinality/uniqueness assertion keeps a duplicate materialization observable at the client.
    expect(ids).toEqual([1, 3])
    expect(new Set(ids).size).toBe(ids.length)

    const restored = await fetch(`${h.engineUrl}/shapes/${identity.shapeId}`)
    expect(restored.status).toBe(200)
  }, 90000)
})
