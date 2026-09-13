import type { Schema } from '@electric-circuits/protocol'
import pgpkg from 'pg'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { waitFor } from './engine-native.js'
import { bootHarness, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: 'int' }, label: { type: 'text' } },
      primaryKey: 'id',
    },
    other: {
      columns: { id: { type: 'int' }, label: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}

async function queryTable(harness: Harness, table: string): Promise<void> {
  const response = await fetch(`${harness.engineUrl}/query`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ table, limit: 1 }),
  })
  if (!response.ok) throw new Error(`POST /query -> ${response.status} ${await response.text()}`)
}

describe('Postgres pooled-connection check-in', () => {
  let harness: Harness

  beforeAll(async () => {
    harness = await bootHarness(schema, {
      engineEnv: {
        ELECTRIC_CIRCUITS_LOG: 'info',
        ELECTRIC_DB_POOL_SIZE: '1',
      },
    })
  }, 60_000)

  afterAll(async () => {
    await harness?.shutdown()
  })

  it('does not issue a rollback for a clean pooled read', async () => {
    // With one pool slot, the second query cannot begin until the first query's checkout is fully
    // returned. Its response is therefore the causal gate proving the first clean check-in and any
    // cleanup SQL it issued have completed before the assertion.
    await queryTable(harness, 'items')
    await queryTable(harness, 'items')

    expect(harness.engineStderr()).not.toContain('there is no transaction in progress')
  })

  it('rolls back an aborted transaction before the pool slot is reused', async () => {
    const control = new pgpkg.Client({ connectionString: harness.pgUrl })
    const blocker = new pgpkg.Client({ connectionString: harness.pgUrl })
    await control.connect()
    await blocker.connect()
    try {
      await control.query("INSERT INTO items (id, label) VALUES (1, 'before')")
      await control.query('ALTER TABLE items REPLICA IDENTITY DEFAULT')

      // ACCESS SHARE lets the UPDATE below commit but blocks the engine's ACCESS EXCLUSIVE repair.
      // Its five-second lock timeout aborts the repair transaction, exercising dirty check-in.
      await blocker.query('BEGIN')
      await blocker.query('SELECT * FROM items')
      await control.query("UPDATE items SET label = 'after' WHERE id = 1")

      await waitFor(
        () => harness.engineStderr().includes('could not restore REPLICA IDENTITY FULL'),
        'replica-identity repair to time out inside its explicit transaction',
        15_000,
      )
      await blocker.query('ROLLBACK')

      // The pool has one slot. This query cannot run until the failed repair checks its connection
      // back in; success proves that check-in rolled the aborted transaction back before reuse.
      await queryTable(harness, 'other')
      expect(harness.engineStderr()).not.toContain('current transaction is aborted')
      expect(harness.engineStderr()).not.toContain('there is already a transaction in progress')
    } finally {
      await blocker.query('ROLLBACK').catch(() => {})
      await blocker.end().catch(() => {})
      await control.end().catch(() => {})
    }
  }, 30_000)
})
