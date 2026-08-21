import { spawn, type ChildProcess } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { createCore, type ElectricCore, type ShapeHandle } from '@electric-circuits/api'
import { DurableStreamTestServer } from '@electric-circuits/ds-rust'
import type { Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { foldStream, waitFor } from './engine-native.js'
import { buildEngine } from './harness.js'

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: 'int' }, active: { type: 'bool' } },
      primaryKey: 'id',
    },
  },
}

const root = join(dirname(fileURLToPath(import.meta.url)), '../../..')

async function spawnLibraryEngine(dsUrl: string): Promise<{ url: string; proc: ChildProcess }> {
  buildEngine()
  const proc = spawn(join(root, 'target/debug/electric-circuits-engine'), [], {
    env: {
      ...process.env,
      DATABASE_URL: '',
      ELECTRIC_CIRCUITS_PG_URL: '',
      ELECTRIC_CIRCUITS_DS_URL: dsUrl,
      ELECTRIC_CIRCUITS_BIND: '127.0.0.1:0',
      ELECTRIC_CIRCUITS_TRACE: '0',
      ELECTRIC_CIRCUITS_LOG: 'warn',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const url = await new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('library-mode engine did not start')), 20_000)
    let stdout = ''
    proc.stdout!.on('data', (chunk: Buffer) => {
      stdout += chunk.toString()
      const match = stdout.match(/ENGINE_LISTENING (\S+)/)
      if (match) {
        clearTimeout(timer)
        resolve(match[1]!)
      }
    })
    proc.once('exit', (code) => {
      clearTimeout(timer)
      reject(new Error(`library-mode engine exited early with code ${code}`))
    })
  })
  return { url, proc }
}

describe('native library-mode writes', () => {
  let ds: DurableStreamTestServer | undefined
  let engine: ChildProcess | undefined
  let core: ElectricCore | undefined
  let shape: ShapeHandle | undefined

  afterEach(async () => {
    if (shape) await core?.dropShape(shape.shapeId).catch(() => {})
    engine?.kill('SIGKILL')
    await ds?.stop().catch(() => {})
  })

  it('removes a deleted row from a native materialized shape', async () => {
    ds = new DurableStreamTestServer({ port: 0 })
    const dsUrl = await ds.start()
    const started = await spawnLibraryEngine(dsUrl)
    engine = started.proc
    core = createCore({ dsUrl, engineUrl: started.url })
    await core.defineSchema(schema)

    shape = await core.createShape({ table: 'items' })
    await core.write({ table: 'items', op: 'insert', pk: 1, row: { id: 1, active: true } })
    await waitFor(async () => (await foldStream(shape!.streamUrl)).has('1'), 'initial native insert')

    await core.write({ table: 'items', op: 'delete', pk: 1 })
    // This later write is observed through the same public shape stream, proving the sequencer has
    // already processed the preceding delete before we inspect the materialized result.
    await core.write({ table: 'items', op: 'insert', pk: 2, row: { id: 2, active: true } })
    await waitFor(async () => (await foldStream(shape!.streamUrl)).has('2'), 'barrier native insert')

    expect([...((await foldStream(shape.streamUrl)).keys())].sort()).toEqual(['2'])
  })
})
