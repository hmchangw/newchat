// End-to-end smoke against the LIVE stack: proves thread delivery survives a
// Cassandra outage, and that starting a brand-new thread while Cassandra is
// down is refused rather than silently dropped.
//
// What this proves (docs/superpowers/sdd/2026-08-25-thread-delivery-cassandra-outage):
//   - broadcast-worker / notification-worker resolve a thread's parent from
//     the thread_rooms MongoDB doc, not history-service (which reads
//     Cassandra) — so a thread that already exists keeps delivering replies
//     while Cassandra is unreachable.
//   - message-gatekeeper refuses a reply that would START a new thread while
//     history is unreachable (errcode.Unavailable, reason
//     thread_start_unavailable) — nobody could resolve that parent, so the
//     reply would reach nobody.
//   - chat-frontend's sendMessage settles on the gatekeeper's reply instead
//     of publishing blind, so that refusal is observable to a client.
//
// Why this script does NOT import `sendMessage` (src/api/sendMessage/) or
// `requestWithAsyncResult` (src/api/_transport/asyncJob.ts), even though
// they're the real client-side implementations of exactly what's under
// test: `asyncJob.ts` imports the `@/lib/telemetry` path alias, which plain
// `node --experimental-strip-types` cannot resolve (no bundler, no
// `package.json#imports` map), and `sendMessage/index.ts` imports its
// siblings without file extensions, which Node ESM also refuses. This is a
// pre-existing gap — it equally breaks the two earlier reference scripts,
// `liveStack.smoke.mjs` and `asyncJob.smoke.mjs`, both of which import
// `asyncJob.ts` and so cannot currently load under plain Node either.
// Fixing it means adding explicit `.ts` extensions across `src/api/` (and
// probably a tsconfig change) — a module-resolution refactor tracked
// separately, not part of this thread-delivery bugfix. Until that lands,
// this script talks to NATS directly, but ONLY via `subjects.ts` (which has
// no imports of its own and loads cleanly) for every subject string — the
// wire correlation below (subscribe-before-publish, settle on the gatekeeper
// reply or a timeout) is deliberately the same dozen-line shape
// `sendMessage` itself uses, so this still asserts on the real wire
// contract, just without going through the unresolvable import chain.
// DO NOT reintroduce an import of `sendMessage` or `asyncJob.ts` here
// without first fixing that resolution gap, or this script goes dark again.
//
// Prereqs:
//   * The local stack up per docker-local/ (NATS, auth-service, room-service,
//     room-worker, message-gatekeeper, broadcast-worker, message-worker;
//     Cassandra reachable to start).
//   * Users alice/bob seeded in Mongo (`make seed`).
//   * Ability to stop/start the Cassandra container (docker compose -f
//     docker-local/compose.deps.yaml {stop,start} cassandra).
//
// Usage:
//   npm run smoke:threadoutage                  # interactive — prompts to
//                                                #   stop/start Cassandra
//   npm run smoke:threadoutage -- --assume-stopped
//                                                # unattended — the caller is
//                                                #   responsible for actually
//                                                #   stopping/starting
//                                                #   Cassandra around the run
//
// The 5s/10s event-wait timeouts below are carried over from the task's
// pseudocode and have never been checked against a live stack — treat them
// as a first-run tuning point, not a verified budget.
//
// Node 22+ --experimental-strip-types loads subjects.ts directly (no build
// step needed — it has no imports of its own).

import readline from 'node:readline'
import { connect, StringCodec, jwtAuthenticator, headers as natsHeaders } from 'nats.ws'
import { createUser } from 'nkeys.js'
import { roomCreate, userRoomEvent, msgSend, userResponse } from '../src/api/_transport/subjects.ts'
import { generateMessageID } from '../src/lib/idgen.js'
import { isDMExistsReply } from '../src/lib/constants.js'
import { v4 as uuidv4 } from 'uuid'

const AUTH_URL = process.env.AUTH_URL || 'http://localhost:8080'
const NATS_WS = process.env.NATS_WS_URL || 'ws://localhost:9222'
const SITE_ID = process.env.SITE_ID || 'site-local'
const ASSUME_STOPPED = process.argv.includes('--assume-stopped')
const sc = StringCodec()

let pass = 0
let fail = 0
function check(label, ok, detail = '') {
  const tag = ok ? 'PASS' : 'FAIL'
  console.log(`  [${tag}] ${label}${detail ? ' — ' + detail : ''}`)
  if (ok) pass++; else fail++
}

async function devLogin(account) {
  const nkey = createUser()
  const natsPublicKey = nkey.getPublicKey()
  const resp = await fetch(`${AUTH_URL}/auth`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ account, natsPublicKey }),
  })
  if (!resp.ok) throw new Error(`auth failed: ${resp.status} ${await resp.text()}`)
  const { natsJwt, user } = await resp.json()
  return { natsJwt, user, seed: nkey.getSeed() }
}

// Two-phase room.create, inlined rather than imported from
// `_transport/asyncJob.ts` (see the header comment for why): subscribe to
// the per-request response subject before publishing, read the sync reply,
// and — unless it's a DM-exists dedup, which needs no follow-up — wait for
// room-worker's AsyncJobResult on that same subject. Mirrors
// `requestWithAsyncResult`'s logic exactly, minus the OpenTelemetry spans
// this script has no use for.
async function createDMRoom(nc, account, counterpartAccount, siteId) {
  const requestId = uuidv4()
  const responseSubject = userResponse(account, requestId)
  const sub = nc.subscribe(responseSubject, { max: 1 })
  let resolveAsync
  const asyncPromise = new Promise((res) => { resolveAsync = res })
  ;(async () => {
    for await (const msg of sub) {
      resolveAsync(JSON.parse(sc.decode(msg.data)))
      return
    }
  })()

  const h = natsHeaders()
  h.set('X-Request-ID', requestId)
  const resp = await nc.request(
    roomCreate(account, siteId),
    sc.encode(JSON.stringify({ name: '', users: [counterpartAccount], orgs: [], channels: [] })),
    { timeout: 5000, headers: h }
  )
  const syncReply = JSON.parse(sc.decode(resp.data))

  if (isDMExistsReply(syncReply)) {
    sub.unsubscribe()
    return syncReply.roomId
  }
  if (syncReply.error) {
    sub.unsubscribe()
    throw new Error(syncReply.error)
  }

  const timeoutPromise = new Promise((resolve) => setTimeout(() => resolve(null), 10000))
  const asyncResult = await Promise.race([asyncPromise, timeoutPromise])
  sub.unsubscribe()
  if (!asyncResult || asyncResult.status !== 'ok') {
    throw new Error(`room create did not complete: ${JSON.stringify(asyncResult)}`)
  }
  return syncReply.roomId
}

// The same subscribe-before-publish / settle-on-reply-or-timeout shape
// `sendMessage` (src/api/sendMessage/index.ts) implements — inlined here for
// the reason in the header comment. Resolves with the gatekeeper's success
// envelope, or rejects with an Error carrying `.code`/`.reason` copied
// straight from the wire envelope (docs/client-api.md §4/§6) on a refusal,
// or on no reply within `timeoutMs`.
function sendViaGatekeeper(nc, account, roomId, siteId, payload, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const sub = nc.subscribe(userResponse(account, payload.requestId), { max: 1 })
    const timer = setTimeout(() => {
      sub.unsubscribe()
      reject(new Error('send timed out'))
    }, timeoutMs)
    ;(async () => {
      for await (const msg of sub) {
        clearTimeout(timer)
        const env = JSON.parse(sc.decode(msg.data))
        if (env.error) {
          reject(Object.assign(new Error(env.error), { code: env.code, reason: env.reason }))
        } else {
          resolve(env)
        }
        return
      }
    })()
    nc.publish(msgSend(account, roomId, siteId), sc.encode(JSON.stringify(payload)))
  })
}

function waitForEnter(promptText) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
    rl.question(promptText, () => {
      rl.close()
      resolve()
    })
  })
}

async function confirmCassandraStopped() {
  console.log('\n>>> Stop Cassandra now: docker compose -f docker-local/compose.deps.yaml stop cassandra')
  if (ASSUME_STOPPED) {
    console.log('    --assume-stopped: continuing without a prompt')
    return
  }
  await waitForEnter('    Press <Enter> once Cassandra is stopped... ')
}

async function confirmCassandraStarted() {
  console.log('\n>>> Start Cassandra now: docker compose -f docker-local/compose.deps.yaml start cassandra')
  if (ASSUME_STOPPED) {
    console.log('    --assume-stopped: continuing without a prompt')
    return
  }
  await waitForEnter('    Press <Enter> once Cassandra is back up... ')
}

async function main() {
  console.log(`Auth: ${AUTH_URL}  |  NATS-ws: ${NATS_WS}  |  site: ${SITE_ID}`)
  if (ASSUME_STOPPED) console.log('Mode: unattended (--assume-stopped)')

  // ── dev-login + connect alice and bob ───────────────────────────────────
  console.log('\n[1] dev-login + connect alice and bob')
  const aliceAuth = await devLogin('alice')
  const aliceNc = await connect({
    servers: NATS_WS,
    authenticator: jwtAuthenticator(aliceAuth.natsJwt, aliceAuth.seed),
  })
  const alice = { nc: aliceNc, account: aliceAuth.user.account }
  check('alice connected', !!aliceNc, aliceNc.getServer())

  const bobAuth = await devLogin('bob')
  const bobNc = await connect({
    servers: NATS_WS,
    authenticator: jwtAuthenticator(bobAuth.natsJwt, bobAuth.seed),
  })
  const bob = { nc: bobNc, account: bobAuth.user.account }
  check('bob connected', !!bobNc, bobNc.getServer())

  // ── create a DM room between alice and bob ──────────────────────────────
  console.log('\n[2] create a DM room (alice + bob)')
  let roomId
  try {
    roomId = await createDMRoom(aliceNc, alice.account, bob.account, SITE_ID)
  } catch (e) {
    check('DM room created', false, e.message)
    await aliceNc.drain()
    await bobNc.drain()
    process.exit(2)
  }
  check('DM room created', !!roomId, `roomId=${roomId}`)

  // ── bob captures every event on his user-room-event lane ────────────────
  // (DM thread replies fan out via subject.UserRoomEvent per member —
  // broadcast-worker's publishDMEvents — unencrypted, so no decrypt step.)
  const bobEvents = []
  const bobWaiters = []
  const bobEventSub = bobNc.subscribe(userRoomEvent(bob.account))
  ;(async () => {
    for await (const msg of bobEventSub) {
      let evt
      try {
        evt = JSON.parse(sc.decode(msg.data))
      } catch {
        continue // skip malformed messages
      }
      bobEvents.push(evt)
      for (let i = bobWaiters.length - 1; i >= 0; i--) {
        if (bobWaiters[i].predicate(evt)) {
          bobWaiters[i].resolve(evt)
          bobWaiters.splice(i, 1)
        }
      }
    }
  })()

  // Resolves with the first already-captured (or future) event matching
  // `predicate`, or `null` after `timeoutMs` — never rejects, so callers can
  // `check('...', !!result)` uniformly whether the event already arrived,
  // arrives later, or never does.
  function waitForEvent(predicate, timeoutMs) {
    const already = bobEvents.find(predicate)
    if (already) return Promise.resolve(already)
    return new Promise((resolve) => {
      const waiter = {
        predicate,
        resolve: (evt) => {
          clearTimeout(timer)
          resolve(evt)
        },
      }
      const timer = setTimeout(() => {
        const idx = bobWaiters.indexOf(waiter)
        if (idx >= 0) bobWaiters.splice(idx, 1)
        resolve(null)
      }, timeoutMs)
      bobWaiters.push(waiter)
    })
  }

  // Top-level message delivered to bob (RoomEvent{type:"new_message"}).
  function waitForRoomEvent(messageId, timeoutMs) {
    return waitForEvent((evt) => evt?.type === 'new_message' && evt?.message?.id === messageId, timeoutMs)
  }

  // Thread reply delivered to bob (RoomEvent{type:"new_thread_message"},
  // message.threadParentMessageId names the parent). Mirrors how
  // ThreadEventsContext/useRoomSubscriptions.js recognize the same event.
  function waitForThreadEvent(parentId, timeoutMs) {
    return waitForEvent(
      (evt) => evt?.type === 'new_thread_message' && evt?.message?.threadParentMessageId === parentId,
      timeoutMs
    )
  }

  // Sends a plain top-level message via sendViaGatekeeper and returns its id.
  async function sendTopLevel(sender, roomId, content) {
    const id = generateMessageID()
    await sendViaGatekeeper(sender.nc, sender.account, roomId, SITE_ID, { id, content, requestId: uuidv4() })
    return id
  }

  // Sends a thread reply via sendViaGatekeeper and returns its id.
  // threadParentMessageCreatedAt is intentionally omitted — the gatekeeper
  // resolves the authoritative value server-side and ignores any
  // client-sent one.
  async function sendThreadReply(sender, roomId, parentId, content) {
    const id = generateMessageID()
    await sendViaGatekeeper(sender.nc, sender.account, roomId, SITE_ID, {
      id,
      content,
      requestId: uuidv4(),
      threadParentMessageId: parentId,
    })
    return id
  }

  // ── Phase 1 — seed a thread while the stack is healthy, so message-worker
  // creates its thread_rooms document. This is the thread that must survive.
  console.log('\n[3] Phase 1 — seed a thread while Cassandra is healthy')
  const parentId = await sendTopLevel(alice, roomId, 'thread parent')
  await waitForRoomEvent(parentId, 5000)
  await sendThreadReply(alice, roomId, parentId, 'first reply (healthy)')
  const seeded = await waitForThreadEvent(parentId, 5000)
  check('healthy thread reply delivered', !!seeded)

  // ── Phase 2 — the outage.
  console.log('\n[4] Phase 2 — the Cassandra outage')
  await confirmCassandraStopped()

  // Assert 1: a thread that already exists still delivers. This is the fix —
  // broadcast-worker resolves the parent from thread_rooms, never Cassandra.
  await sendThreadReply(alice, roomId, parentId, 'reply during outage')
  const during = await waitForThreadEvent(parentId, 5000)
  check('existing thread still delivers during the outage', !!during,
    during ? '' : 'bob received no thread event')

  // Assert 2: starting a NEW thread is refused, not silently swallowed.
  const freshId = await sendTopLevel(alice, roomId, 'a message with no replies')
  let refusal = null
  try {
    await sendThreadReply(alice, roomId, freshId, 'first reply during outage')
  } catch (err) {
    refusal = err
  }
  check('new thread start refused with a typed reason',
    refusal?.reason === 'thread_start_unavailable',
    refusal ? `got reason=${refusal.reason}` : 'the send was accepted — it should have been refused')

  // ── Phase 3 — recovery restores thread starts.
  console.log('\n[5] Phase 3 — recovery')
  await confirmCassandraStarted()
  await sendThreadReply(alice, roomId, freshId, 'first reply after recovery')
  const recovered = await waitForThreadEvent(freshId, 10000)
  check('new thread start works again after recovery', !!recovered)

  await aliceNc.drain()
  await bobNc.drain()
  console.log(`\n=== ${pass} passed, ${fail} failed ===`)
  process.exit(fail === 0 ? 0 : 1)
}

main().catch((err) => {
  console.error('FATAL:', err)
  process.exit(2)
})
