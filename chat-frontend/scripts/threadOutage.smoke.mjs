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
// Node 22+ --experimental-strip-types loads the .ts modules in src/api/
// directly (no build step).

import readline from 'node:readline'
import { connect, StringCodec, jwtAuthenticator } from 'nats.ws'
import { createUser } from 'nkeys.js'
import { requestWithAsyncResult } from '../src/api/_transport/asyncJob.ts'
import { roomCreate, userRoomEvent } from '../src/api/_transport/subjects.ts'
import { sendMessage } from '../src/api/sendMessage/index.ts'
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

// Minimal adapter over a raw nats.ws connection matching the `Nats` shape
// `sendMessage` expects ({user, publish, subscribe}) — mirrors what
// NatsContext.jsx wires up in the real app, without the React/telemetry
// plumbing this script has no use for.
function makeNatsAdapter(nc, user) {
  return {
    user,
    publish(subject, data = {}) {
      nc.publish(subject, sc.encode(JSON.stringify(data)))
    },
    subscribe(subject, callback) {
      const sub = nc.subscribe(subject)
      ;(async () => {
        for await (const msg of sub) {
          try {
            callback(JSON.parse(sc.decode(msg.data)))
          } catch {
            // skip malformed messages, matching NatsContext.jsx's subscribe
          }
        }
      })()
      return sub
    },
  }
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
  const alice = makeNatsAdapter(aliceNc, aliceAuth.user)
  check('alice connected', !!aliceNc, aliceNc.getServer())

  const bobAuth = await devLogin('bob')
  const bobNc = await connect({
    servers: NATS_WS,
    authenticator: jwtAuthenticator(bobAuth.natsJwt, bobAuth.seed),
  })
  const bob = makeNatsAdapter(bobNc, bobAuth.user)
  check('bob connected', !!bobNc, bobNc.getServer())

  // ── create a DM room between alice and bob ──────────────────────────────
  console.log('\n[2] create a DM room (alice + bob)')
  let roomId
  try {
    const created = await requestWithAsyncResult(
      aliceNc,
      'alice',
      roomCreate('alice', SITE_ID),
      { name: '', users: ['bob'], orgs: [], channels: [] },
      { asyncTimeout: 10000, treatAsSuccess: isDMExistsReply }
    )
    roomId = created?.sync?.roomId
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
  bob.subscribe(userRoomEvent(bob.user.account), (evt) => {
    bobEvents.push(evt)
    for (let i = bobWaiters.length - 1; i >= 0; i--) {
      if (bobWaiters[i].predicate(evt)) {
        bobWaiters[i].resolve(evt)
        bobWaiters.splice(i, 1)
      }
    }
  })

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
  function waitForRoomEvent(_user, messageId, timeoutMs) {
    return waitForEvent((evt) => evt?.type === 'new_message' && evt?.message?.id === messageId, timeoutMs)
  }

  // Thread reply delivered to bob (RoomEvent{type:"new_thread_message"},
  // message.threadParentMessageId names the parent). Mirrors how
  // ThreadEventsContext/useRoomSubscriptions.js recognize the same event.
  function waitForThreadEvent(_user, parentId, timeoutMs) {
    return waitForEvent(
      (evt) => evt?.type === 'new_thread_message' && evt?.message?.threadParentMessageId === parentId,
      timeoutMs
    )
  }

  // Sends a plain top-level message through the real sendMessage() client
  // path and returns its message id.
  async function sendTopLevel(sender, roomId, content) {
    const id = generateMessageID()
    await sendMessage(sender, {
      roomId,
      siteId: SITE_ID,
      payload: { id, content, requestId: uuidv4() },
    })
    return id
  }

  // Sends a thread reply through the real sendMessage() client path and
  // returns its message id. threadParentMessageCreatedAt is intentionally
  // omitted — the gatekeeper resolves the authoritative value server-side
  // and ignores any client-sent one.
  async function sendThreadReply(sender, roomId, parentId, content) {
    const id = generateMessageID()
    await sendMessage(sender, {
      roomId,
      siteId: SITE_ID,
      payload: { id, content, requestId: uuidv4(), threadParentMessageId: parentId },
    })
    return id
  }

  // ── Phase 1 — seed a thread while the stack is healthy, so message-worker
  // creates its thread_rooms document. This is the thread that must survive.
  console.log('\n[3] Phase 1 — seed a thread while Cassandra is healthy')
  const parentId = await sendTopLevel(alice, roomId, 'thread parent')
  await waitForRoomEvent(bob, parentId, 5000)
  await sendThreadReply(alice, roomId, parentId, 'first reply (healthy)')
  const seeded = await waitForThreadEvent(bob, parentId, 5000)
  check('healthy thread reply delivered', !!seeded)

  // ── Phase 2 — the outage.
  console.log('\n[4] Phase 2 — the Cassandra outage')
  await confirmCassandraStopped()

  // Assert 1: a thread that already exists still delivers. This is the fix —
  // broadcast-worker resolves the parent from thread_rooms, never Cassandra.
  await sendThreadReply(alice, roomId, parentId, 'reply during outage')
  const during = await waitForThreadEvent(bob, parentId, 5000)
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
  const recovered = await waitForThreadEvent(bob, freshId, 10000)
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
