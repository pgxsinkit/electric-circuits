// Conformance harness (Postgres mode): Postgres is the system of record. Changes are written to
// Postgres, captured by the engine's logical-replication ingestor, fanned out to shape streams, and
// materialized by the streamdb client. The SAME Postgres answers `SELECT … WHERE pred` as the oracle.
//
// Topology:
//   Vitest worker:  per-test Postgres database (in the shared ephemeral PG) + DurableStreamTestServer
//                   + tRPC API + streamdb client + pg-backed oracle
//   child process:  electric-circuits-engine (Rust) in Postgres mode (ingestor + query-back backfill)

import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { DurableStreamTestServer, type TestServerOptions } from '@electric-circuits/ds-rust'
import { type ApiServer, createApiServer } from '@electric-circuits/api'
import { createClient, type ElectricIvmClient, type ShapeMaterialization } from '@electric-circuits/client'
import { createPgOracle, createPgTables, type Oracle } from '@electric-circuits/oracle'
import type { ChangeEvent, Row, Schema, ShapeDef } from '@electric-circuits/protocol'
import pgpkg from 'pg'

import { compareShapeSets, type CompareResult } from './compare.js'

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

function repoRoot(): string {
  let d = dirname(fileURLToPath(import.meta.url))
  for (let i = 0; i < 8; i++) {
    if (existsSync(join(d, 'Cargo.toml'))) return d
    d = dirname(d)
  }
  throw new Error('repo root (Cargo.toml) not found')
}

let engineBuilt = false
/** Build the engine binary once per process. Skipped when the vitest globalSetup already built it. */
export function buildEngine(): void {
  if (engineBuilt || process.env.ELECTRIC_CIRCUITS_ENGINE_PREBUILT === '1') return
  execFileSync('cargo', ['build', '-p', 'electric-circuits-engine'], { cwd: repoRoot(), stdio: 'inherit' })
  engineBuilt = true
}

function engineBin(): string {
  return join(repoRoot(), 'target', 'debug', 'electric-circuits-engine')
}

/** How an engine process exited: `code` for a normal exit, `signal` when it was killed. */
export interface EngineExit {
  code: number | null
  signal: NodeJS.Signals | null
}

/**
 * A raw engine child process, with no assumption that it ever becomes ready.
 *
 * `bootHarness` waits for `ENGINE_LISTENING` (the boot RESOLVED); this handle exposes the two
 * stages separately, which is what the boot-taxonomy and graceful-shutdown tests need: a binary
 * pointed at an unreachable database opens its HTTP port (`ENGINE_BINDING`, so `GET /ready` is
 * answerable) and never reports listening, and a binary with bad credentials exits 78 without
 * either.
 */
export interface RawEngine {
  proc: ChildProcess
  stderr(): string
  /** The bound base URL, once the process has printed `ENGINE_BINDING` (the port is open). */
  waitForBinding(timeoutMs?: number): Promise<string>
  /** The bound base URL, once the boot has fully resolved (`ENGINE_LISTENING`). */
  waitForListening(timeoutMs?: number): Promise<string>
  waitForExit(timeoutMs?: number): Promise<EngineExit>
  signal(sig?: NodeJS.Signals): void
}

/**
 * Spawn the engine binary with an explicit environment and hand back control of its lifecycle.
 * Nothing is awaited here — the caller decides which milestone (if any) it expects.
 */
export function spawnRawEngine(env: Record<string, string>): RawEngine {
  const proc = spawn(engineBin(), [], { env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] })
  let stderrBuf = ''
  let stdoutBuf = ''
  let exited: EngineExit | undefined
  proc.stderr!.on('data', (d: Buffer) => {
    const s = d.toString()
    stderrBuf += s
    process.stderr.write(s)
  })
  proc.stdout!.on('data', (d: Buffer) => {
    stdoutBuf += d.toString()
  })
  proc.on('exit', (code, sig) => {
    exited = { code, signal: sig }
  })

  const waitFor = (marker: string, timeoutMs: number) =>
    new Promise<string>((resolve, reject) => {
      const deadline = Date.now() + timeoutMs
      const poll = () => {
        const m = stdoutBuf.match(new RegExp(`${marker} (\\S+)`))
        if (m) return resolve(m[1]!)
        if (exited) return reject(new Error(`engine exited (code ${exited.code}) before printing ${marker}`))
        if (Date.now() > deadline) return reject(new Error(`engine did not print ${marker} within ${timeoutMs}ms`))
        setTimeout(poll, 25)
      }
      poll()
    })

  return {
    proc,
    stderr: () => stderrBuf,
    waitForBinding: (timeoutMs = 20000) => waitFor('ENGINE_BINDING', timeoutMs),
    waitForListening: (timeoutMs = 20000) => waitFor('ENGINE_LISTENING', timeoutMs),
    waitForExit: (timeoutMs = 30000) =>
      new Promise<EngineExit>((resolve, reject) => {
        if (exited) return resolve(exited)
        const timer = setTimeout(() => {
          proc.kill('SIGKILL')
          reject(new Error(`engine did not exit within ${timeoutMs}ms`))
        }, timeoutMs)
        proc.once('exit', (code, sig) => {
          clearTimeout(timer)
          resolve({ code, signal: sig })
        })
      }),
    signal: (sig: NodeJS.Signals = 'SIGTERM') => {
      proc.kill(sig)
    },
  }
}

async function spawnEngine(
  dsUrl: string,
  pgUrl: string,
  tables: string[],
  slot: string,
  fault?: string,
  extraEnv?: Record<string, string>,
): Promise<{ url: string; proc: ChildProcess; stderr: () => string; raw: RawEngine }> {
  const raw = spawnRawEngine({
    ELECTRIC_CIRCUITS_DS_URL: dsUrl,
    ELECTRIC_CIRCUITS_BIND: '127.0.0.1:0',
    ELECTRIC_CIRCUITS_LOG: process.env.ELECTRIC_CIRCUITS_LOG ?? 'warn',
    ELECTRIC_CIRCUITS_PG_URL: pgUrl,
    ELECTRIC_CIRCUITS_PG_TABLES: tables.join(','),
    ELECTRIC_CIRCUITS_PG_SLOT: slot,
    ELECTRIC_CIRCUITS_PG_POLL_MS: '25',
    ...(fault ? { ELECTRIC_CIRCUITS_FAULT: fault } : {}),
    ...(extraEnv ?? {}),
  })
  const url = await raw.waitForListening(20000).catch((e: Error) => {
    raw.proc.kill('SIGKILL') // don't leak the child if it never reports listening
    throw e
  })
  return { url, proc: raw.proc, stderr: raw.stderr, raw }
}

export interface Harness {
  dsUrl: string
  engineUrl: string
  apiUrl: string
  api: ApiServer
  client: ElectricIvmClient
  oracle: Oracle
  schema: Schema
  /** Postgres connection string for this harness's database (the system of record). */
  pgUrl: string
  /** This harness's logical replication slot (unique per database — see `bootHarness`). */
  slot: string
  /** Everything the engine has written to stderr so far — for asserting on/absence of engine log lines. */
  engineStderr(): string
  /**
   * Kill the engine (SIGKILL — simulating a crash) and boot a fresh process against the same
   * durable streams, Postgres, and slot. Returns once the new process is listening; `engineUrl`
   * is updated in place. The new engine restores its shapes from the durable catalog — nothing
   * re-registers them.
   *
   * `whileDown` runs after the old process is gone and before the new one starts — the window for
   * things only an absent engine allows (dropping its replication slot, applying DDL).
   */
  restartEngine(whileDown?: () => Promise<void>): Promise<void>
  /**
   * Send a signal to the engine process (default `SIGTERM`) WITHOUT waiting for it. The graceful
   * shutdown tests need the window between the signal and the exit — that is where `/ready` turns
   * 503 and a parked long-poll must come back.
   */
  signalEngine(sig?: NodeJS.Signals): void
  /** Wait for the current engine process to exit; resolves with its code/signal. */
  waitForEngineExit(timeoutMs?: number): Promise<EngineExit>
  /**
   * Boot a fresh engine process against the same durable streams, Postgres and slot — the second
   * half of `restartEngine`, exposed on its own for tests that stop the engine themselves (with a
   * `SIGTERM`, say) rather than killing it.
   */
  startEngine(): Promise<void>
  /**
   * Test-only Durable Streams crash/restart seam. The wrapper retains the same binary, endpoint,
   * and data directory; callers never receive its process or storage internals.
   */
  crashAndRestartDurableStreams(): Promise<void>
  shutdown(): Promise<void>
}

export interface BootOptions {
  /** Test-only durable-streams persistence mode. Recovery tests use explicit WAL on every host. */
  durableStreamsDurability?: TestServerOptions['durability']
  /** TEST-ONLY: inject an engine fault (e.g. 'drop_deletes', 'off_by_one_cmp') for negative controls. */
  fault?: string
  /** TEST-ONLY: raw DDL run (before the engine starts) INSTEAD of createPgTables — for exercising real
   * Postgres column types the coarse protocol Schema can't express (e.g. `uuid`). Must create every
   * table in `schema` with `REPLICA IDENTITY FULL`. */
  ddl?: string
  /** Extra env vars for the engine process (e.g. retention tuning: `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS`). */
  engineEnv?: Record<string, string>
  /** TEST-ONLY: runs after the tables exist and before the engine's FIRST boot — the window for
   * state the engine is supposed to find already there (e.g. an operator-created replication slot). */
  beforeEngine?: (info: { pgUrl: string; slot: string }) => Promise<void>
  /** TEST-ONLY: put an external HTTP service in front of durable-streams for the engine process.
   * The API and test client still use the real durable-streams URL, so the wrapper can inject
   * transport/status failures without replacing storage or calling engine internals. */
  wrapEngineDs?: (upstreamUrl: string) => Promise<{ url: string; close(): Promise<void> }>
}

function adminUrl(): string {
  const url = process.env.ELECTRIC_CIRCUITS_TEST_PG_URL
  if (!url) throw new Error('ELECTRIC_CIRCUITS_TEST_PG_URL not set (vitest globalSetup should boot Postgres)')
  return url
}

let dbCounter = 0
function uniqueDbName(): string {
  dbCounter += 1
  return `el_${process.pid}_${Date.now().toString(36)}_${dbCounter}`.toLowerCase()
}

export async function bootHarness(schema: Schema, opts: BootOptions = {}): Promise<Harness> {
  buildEngine()

  // 1. Create a dedicated database in the shared ephemeral Postgres (per-test isolation; slots are
  //    per-database). Create the tables (with REPLICA IDENTITY FULL) before the engine starts so its
  //    startup introspection + slot creation see them.
  const admin = new pgpkg.Client({ connectionString: adminUrl() })
  await admin.connect()
  const dbName = uniqueDbName()
  await admin.query(`CREATE DATABASE ${dbName}`)
  await admin.end()
  const pgUrl = adminUrl().replace(/\/[^/]+$/, `/${dbName}`)
  // Replication slot names are GLOBALLY unique in Postgres (not per-database), so derive a unique one.
  const slot = `slot_${dbName}`

  // Drop this harness's Postgres artifacts (slot then database). Used by both shutdown and the
  // partial-boot-failure cleanup, so a half-built harness never leaks a slot or database.
  const dropPgArtifacts = async () => {
    try {
      const c = new pgpkg.Client({ connectionString: pgUrl })
      await c.connect()
      for (let i = 0; i < 60; i++) {
        try {
          // Terminate any lingering walsender holding the slot, then drop it.
          await c.query('SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL', [slot]).catch(() => {})
          await c.query('SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)', [slot])
          break
        } catch {
          await sleep(100) // slot still marked active until PG notices the killed consumer
        }
      }
      await c.end()
    } catch {
      /* ignore */
    }
    try {
      const a = new pgpkg.Client({ connectionString: adminUrl() })
      await a.connect()
      await a.query(`DROP DATABASE IF EXISTS ${dbName} WITH (FORCE)`)
      await a.end()
    } catch {
      /* ignore */
    }
  }

  // Track resources so a failure at any step tears down everything created so far.
  let server: DurableStreamTestServer | undefined
  let proc: ChildProcess | undefined
  let api: ApiServer | undefined
  let oracle: Oracle | undefined
  let client: ElectricIvmClient | undefined
  let engineDs: { url: string; close(): Promise<void> } | undefined
  const teardown = async () => {
    await client?.close().catch(() => {})
    await api?.close().catch(() => {})
    proc?.kill('SIGKILL')
    await oracle?.close().catch(() => {})
    await engineDs?.close().catch(() => {})
    await server?.stop().catch(() => {})
    await dropPgArtifacts()
  }

  try {
    // Create the tables (with REPLICA IDENTITY FULL) before the engine starts so its startup
    // introspection + slot creation see them. `opts.ddl` overrides for real column types (e.g. uuid).
    if (opts.ddl) {
      const d = new pgpkg.Client({ connectionString: pgUrl })
      await d.connect()
      await d.query(opts.ddl)
      await d.end()
    } else {
      await createPgTables(pgUrl, schema)
    }
    // Drain-barrier sentinel: a single-row counter table the replicator decodes (but does not treat
    // as a data table). drainEngine bumps it and waits for the engine to report it (see drainEngine).
    const c = new pgpkg.Client({ connectionString: pgUrl })
    await c.connect()
    await c.query('CREATE TABLE __el_sync (id int PRIMARY KEY, n bigint NOT NULL)')
    await c.query('INSERT INTO __el_sync (id, n) VALUES (1, 0)')
    await c.end()

    await opts.beforeEngine?.({ pgUrl, slot })

    // 2. Boot durable-streams + the engine (Postgres mode) + API + client + oracle.
    server = new DurableStreamTestServer({ port: 0, durability: opts.durableStreamsDurability })
    const dsUrl = await server.start()
    engineDs = await opts.wrapEngineDs?.(dsUrl)
    const engineDsUrl = engineDs?.url ?? dsUrl
    const tables = Object.keys(schema.tables)
    let spawned = await spawnEngine(engineDsUrl, pgUrl, tables, slot, opts.fault, opts.engineEnv)
    proc = spawned.proc
    const engineUrl = spawned.url
    api = await createApiServer({ dsUrl, engineUrl })
    oracle = await createPgOracle(schema, pgUrl)
    client = createClient({ apiUrl: api.url, schema })
    // No client.defineSchema: in Postgres mode the engine self-configures from introspection.

    const h: Harness = {
      dsUrl,
      engineUrl,
      apiUrl: api.url,
      api,
      client,
      oracle,
      schema,
      pgUrl,
      slot,
      engineStderr: () => spawned.stderr(),
      signalEngine: (sig: NodeJS.Signals = 'SIGTERM') => {
        proc?.kill(sig)
      },
      waitForEngineExit: (timeoutMs = 30000) => spawned.raw.waitForExit(timeoutMs),
      startEngine: async () => {
        spawned = await spawnEngine(engineDsUrl, pgUrl, tables, slot, opts.fault, opts.engineEnv)
        proc = spawned.proc
        h.engineUrl = spawned.url
      },
      restartEngine: async (whileDown?: () => Promise<void>) => {
        spawned.raw.signal('SIGKILL')
        await spawned.raw.waitForExit()
        await whileDown?.()
        await h.startEngine()
        // NOTE: the API server keeps pointing at the dead engine; restart tests exercise the
        // engine + streams directly (the catalog restore is engine state, not API state).
      },
      crashAndRestartDurableStreams: async () => {
        if (!server) throw new Error('durable-streams server was not started')
        await server.crashAndRestart()
      },
      shutdown: teardown,
    }
    return h
  } catch (e) {
    await teardown()
    throw e
  }
}

/** Apply one change to Postgres (the system of record). The engine receives it via replication. */
export async function applyOp(h: Harness, table: string, ev: ChangeEvent): Promise<void> {
  await h.oracle.applyChange(table, ev)
}

/** A position in the segmented change log (ADR-0006): compare `(segment, offset)`, never the offset alone. */
export interface LogPosition {
  segment: number
  offset: string
}

/**
 * Is `a` at or past `b`? Segment first — an offset from a later segment can be lexicographically
 * smaller than one from an earlier segment, so comparing offsets alone is wrong. `'-1'` (the start
 * sentinel) sorts below every real offset, which is exactly what an empty target segment wants.
 */
export function positionReached(a: LogPosition, b: LogPosition): boolean {
  if (a.segment !== b.segment) return a.segment > b.segment
  return a.offset >= b.offset
}

/**
 * The tail of the change log: the CURRENT segment (which the engine reports — the log rotates, so
 * there is no un-suffixed `changes` stream to HEAD) and that segment's tail offset.
 */
export async function changesTail(dsUrl: string, engineUrl: string): Promise<LogPosition | null> {
  const segment = await engineChangesSegment(engineUrl)
  if (segment === null) return null
  const res = await fetch(`${dsUrl}/changes/${segment}`, { method: 'HEAD' })
  if (!res.ok) return null
  const off = res.headers.get('stream-next-offset')
  // A segment the ingestor has not written to yet — the usual state right after a rotation — has
  // no bytes to reach, but the sequencer must still have GOT to it, so the segment half of the
  // barrier stands. Servers differ in how they spell "empty": the Node test server reports -1, the
  // Rust server the zero offset (all-zero epoch_byte). Normalize both to the start sentinel, which
  // every position inside that segment satisfies — the alternative (waiting for the sequencer to
  // report the zero offset) waits out a whole 30 s long-poll on an empty segment.
  const empty = off === null || off === '-1' || /^0+(_0+)?$/.test(off)
  if (empty && segment === 0) return null // nothing has ever been written at all
  return { segment, offset: empty ? '-1' : off }
}

/** The segment the INGESTOR is appending to — the one a tail HEAD must address. */
export async function engineChangesSegment(engineUrl: string): Promise<number | null> {
  const res = await fetch(`${engineUrl}/replication/lsn`)
  if (!res.ok) throw new Error(`engine replication status -> ${res.status}`)
  const body = (await res.json()) as { changes?: { segment: number } }
  return body.changes?.segment ?? null
}

/** The SEQUENCER's position in the change log (the same global position for every table). */
export async function engineChangesOffset(engineUrl: string): Promise<LogPosition | null> {
  const res = await fetch(`${engineUrl}/tables/_any/offset`)
  if (res.status === 404) return null
  if (!res.ok) throw new Error(`engine change-log offset -> ${res.status}`)
  const body = (await res.json()) as { segment: number; offset: string }
  return { segment: body.segment, offset: body.offset }
}

async function engineReplicationSync(engineUrl: string): Promise<number> {
  const res = await fetch(`${engineUrl}/replication/lsn`)
  if (!res.ok) throw new Error(`engine replication status -> ${res.status}`)
  return Number(((await res.json()) as { sync: number }).sync)
}

async function enginePendingFlips(engineUrl: string): Promise<number> {
  const res = await fetch(`${engineUrl}/replication/lsn`)
  if (!res.ok) throw new Error(`engine replication status -> ${res.status}`)
  return Number(((await res.json()) as { pendingFlips?: number }).pendingFlips ?? 0)
}

/**
 * Convergence barrier (Postgres mode), in three stages:
 *  1. bump the per-database `__el_sync` sentinel counter, then wait until the engine reports having
 *     decoded-and-appended at least that value. The sentinel UPDATE commits AFTER every prior data
 *     write has committed (drainEngine runs once all applyOp() awaits have resolved), so its commit
 *     LSN is higher; the ingestor decodes in commit-LSN order, so seeing the sentinel implies every
 *     prior change is already on the stream. This is per-database, so it is robust under a shared
 *     multi-database Postgres (no dependence on server-global WAL LSNs).
 *  2. wait until the engine has processed each table stream up to its tail.
 * Without this a freshly-empty shape could read `[] == []` before the change has propagated.
 */
export async function drainEngine(h: Harness, timeoutMs = 20000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  // Stage 1: replication caught up to "now" (sentinel-based).
  const c = new pgpkg.Client({ connectionString: h.pgUrl })
  await c.connect()
  let target: number
  try {
    target = Number((await c.query('UPDATE __el_sync SET n = n + 1 WHERE id = 1 RETURNING n')).rows[0].n)
  } finally {
    await c.end().catch(() => {})
  }
  let synced = false
  while (Date.now() < deadline) {
    if ((await engineReplicationSync(h.engineUrl)) >= target) {
      synced = true
      break
    }
    await sleep(15)
  }
  // A missed barrier means propagation stalled — throw rather than let a stale/empty comparison
  // false-green. (Tests rely on drainEngine actually establishing the barrier.)
  if (!synced) {
    throw new Error(`drainEngine: replication did not reach sentinel ${target} within ${timeoutMs}ms`)
  }
  // Stage 2: the sequencer processed the single ordered change log up to its tail. The log is
  // segmented (ADR-0006), so the barrier is on `(segment, offset)`: the tail is read from the
  // segment the ingestor is currently appending to, and the sequencer must have reached that
  // segment AND that offset within it.
  {
    const tail = await changesTail(h.dsUrl, h.engineUrl)
    if (tail) {
      let reached = false
      while (Date.now() < deadline) {
        const pos = await engineChangesOffset(h.engineUrl)
        if (pos === null || positionReached(pos, tail)) {
          reached = true
          break
        }
        await sleep(20)
      }
      if (!reached) {
        throw new Error(
          `drainEngine: engine did not reach change-log tail changes/${tail.segment}@${tail.offset} within ${timeoutMs}ms`,
        )
      }
    }
  }
  // Stage 3: deferred subquery flip propagation drained. Flip query-backs run on a separate engine
  // task (off the sequencer hot path), so "log at tail" no longer implies subquery move-in/move-out
  // envelopes have been appended; the engine exposes its in-flight flip count for exactly this.
  // No new flips can be enqueued after stages 1-2 (all envelopes processed, no concurrent writers).
  let flipsDrained = false
  while (Date.now() < deadline) {
    if ((await enginePendingFlips(h.engineUrl)) === 0) {
      flipsDrained = true
      break
    }
    await sleep(15)
  }
  if (!flipsDrained) {
    throw new Error(`drainEngine: subquery flip propagation did not drain within ${timeoutMs}ms`)
  }
}

export interface ConvergenceTarget {
  shape: ShapeMaterialization
  def: ShapeDef
  columns: string[]
  pk: string
}

/** One-shot comparison of the client-materialized set against the oracle (no polling). */
export async function snapshotCompare(h: Harness, target: ConvergenceTarget): Promise<CompareResult> {
  const oracleRows: Row[] = await h.oracle.queryShape(target.def)
  const clientRows = target.shape.currentRows()
  return compareShapeSets(target.columns, target.pk, oracleRows, clientRows)
}

/** Poll until the client-materialized set equals the oracle's, or the timeout elapses. */
export async function waitForConvergence(
  h: Harness,
  target: ConvergenceTarget,
  timeoutMs = 10000,
): Promise<CompareResult> {
  const start = Date.now()
  let last: CompareResult = { equal: false, missing: [], extra: [], mismatched: [] }
  while (Date.now() - start < timeoutMs) {
    const oracleRows: Row[] = await h.oracle.queryShape(target.def)
    const clientRows = target.shape.currentRows()
    last = compareShapeSets(target.columns, target.pk, oracleRows, clientRows)
    if (last.equal) return last
    await sleep(50)
  }
  return last
}
