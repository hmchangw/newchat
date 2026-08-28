// End-to-end smoke against the LIVE stack: auth-service + room-service + room-worker.
// Verifies that what the unit suite mocks actually happens on the wire:
//   - POST /auth (dev mode) returns a NATS JWT
//   - the frontend's requestWithAsyncResult creates a real channel via
//     chat.user.{a}.request.room.{site}.create
//   - room-worker materializes the room and emits AsyncJobResult
//   - chat.user.{a}.request.room.{r}.{s}.member.list?enrich=true returns
//     the seeded individuals + the requester as owner
//
// Prereqs:
//   * NATS running with WebSocket on ws://localhost:9222
//   * auth-service in DEV_MODE on http://localhost:8080
//   * room-service + room-worker connected to NATS + Mongo
//   * Users alice/bob/charlie seeded in Mongo
//
// Usage: npm run smoke:livestack (Node 22+ --experimental-strip-types
// loads the .ts modules in api/_transport/ directly).

import { connect, StringCodec, jwtAuthenticator } from 'nats.ws'
import { createUser } from 'nkeys.js'
import { requestWithAsyncResult } from '../src/api/_transport/asyncJob.ts'
import { roomCreate, memberList } from '../src/api/_transport/subjects.ts'
import { isDMExistsReply } from '../src/lib/constants.js'

const AUTH_URL = process.env.AUTH_URL || 'http://localhost:8080'
const NATS_WS = process.env.NATS_WS_URL || 'ws://localhost:9222'
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

async function main() {
  console.log(`Auth: ${AUTH_URL}  |  NATS-ws: ${NATS_WS}`)

  // ── 1. dev-login as alice ──────────────────────────────────────────────────
  console.log('\n[1] dev-login as alice')
  const { natsJwt, user, seed } = await devLogin('alice')
  check('auth returned a JWT', !!natsJwt && natsJwt.length > 50)
  check('auth returned user.account=alice', user?.account === 'alice', `id=${user?.id}`)

  // ── 2. open a NATS WS connection authenticated by the JWT ──────────────────
  console.log('\n[2] connect NATS WebSocket with returned JWT')
  const nc = await connect({
    servers: NATS_WS,
    authenticator: jwtAuthenticator(natsJwt, seed),
    // Replies ride the client's own user namespace, already granted as
    // `chat.user.{{tag(account)}}.>`. Used verbatim: auth-service returns the
    // JWT tag value itself. Inlined rather than imported from
    // src/api/_transport/inbox.js — this script runs outside the bundler.
    inboxPrefix: `chat.user.${user.account}`,
  })
  check('connected', !!nc, nc.getServer())

  // ── 3. create a channel — real room-service + room-worker ──────────────────
  console.log('\n[3] create a channel via the frontend helper, live stack')
  const channelName = `e2e-${Date.now()}`
  let createResult, createErr
  try {
    createResult = await requestWithAsyncResult(
      nc,
      'alice',
      roomCreate('alice', 'site-local'),
      { name: channelName, users: ['bob', 'charlie'], orgs: [], channels: [] },
      { asyncTimeout: 10000 }
    )
  } catch (e) { createErr = e }
  if (createErr) {
    check('create succeeded', false, createErr.message)
  } else {
    check('sync.status = accepted', createResult.sync?.status === 'accepted', `roomId=${createResult.sync?.roomId}`)
    check('sync.roomType = channel', createResult.sync?.roomType === 'channel')
    check('async result is ok', createResult.async?.status === 'ok',
      `op=${createResult.async?.operation} requestId=${createResult.async?.requestId?.slice(0, 8)}…`)
    check('async.roomId matches sync.roomId', createResult.async?.roomId === createResult.sync?.roomId)
  }

  // ── 4. member.list?enrich on the new room ──────────────────────────────────
  console.log('\n[4] member.list?enrich on the new room')
  if (createResult?.sync?.roomId) {
    const roomId = createResult.sync.roomId
    const listSubject = memberList('alice', roomId, 'site-local')
    let listResult
    try {
      const resp = await nc.request(listSubject, sc.encode(JSON.stringify({ enrich: true })), { timeout: 5000 })
      listResult = JSON.parse(sc.decode(resp.data))
    } catch (e) {
      listResult = { error: e.message }
    }

    check('member.list responded', !listResult.error, listResult.error)
    if (!listResult.error) {
      const members = listResult.members ?? []
      const individuals = members.filter((m) => m.member?.type === 'individual')
      const accounts = individuals.map((m) => m.member?.account).sort()
      check('individuals include alice + bob + charlie',
        accounts.join(',') === 'alice,bob,charlie',
        accounts.join(','))
      const alice = individuals.find((m) => m.member?.account === 'alice')
      check('alice is owner', alice?.member?.isOwner === true)
      const bob = individuals.find((m) => m.member?.account === 'bob')
      check('bob is member (not owner)', bob?.member?.isOwner !== true)
      check('display names enriched (engName populated)',
        !!alice?.member?.engName && !!bob?.member?.engName,
        `${alice?.member?.engName}, ${bob?.member?.engName}`)
    }
  } else {
    check('member.list', false, 'skipped: no roomId from create')
  }

  // ── 5. DM-exists dedup: create the same DM twice ───────────────────────────
  console.log('\n[5] DM dedup: second create with same counterpart returns existing roomId')
  let dm1Result, dm2Result
  try {
    dm1Result = await requestWithAsyncResult(
      nc, 'alice',
      roomCreate('alice', 'site-local'),
      { name: '', users: ['bob'], orgs: [], channels: [] },
      { asyncTimeout: 10000 }
    )
  } catch (e) { dm1Result = { error: e.message } }
  check('first DM accepted', dm1Result?.sync?.status === 'accepted', `roomId=${dm1Result?.sync?.roomId}`)

  try {
    dm2Result = await requestWithAsyncResult(
      nc, 'alice',
      roomCreate('alice', 'site-local'),
      { name: '', users: ['bob'], orgs: [], channels: [] },
      { asyncTimeout: 10000, treatAsSuccess: isDMExistsReply }
    )
  } catch (e) { dm2Result = { error: e.message } }
  const sameRoomId = dm2Result?.sync?.roomId === dm1Result?.sync?.roomId
  check('second DM reply carries existing roomId',
    sameRoomId,
    `first=${dm1Result?.sync?.roomId} second=${dm2Result?.sync?.roomId}`)
  check('second DM reply is the "dm already exists" shape',
    dm2Result?.sync?.error === 'dm already exists',
    dm2Result?.sync?.error)
  check('second DM async is null (no follow-up)', dm2Result?.async === null)

  // ── 6. validation: empty create rejected ──────────────────────────────────
  console.log('\n[6] empty payload rejected with classification error')
  let empty
  try {
    await requestWithAsyncResult(
      nc, 'alice',
      roomCreate('alice', 'site-local'),
      { name: '', users: [], orgs: [], channels: [] },
      { asyncTimeout: 3000 }
    )
  } catch (e) { empty = e }
  check('threw with empty-create error', !!empty, empty?.message)

  await nc.drain()
  console.log(`\n=== ${pass} passed, ${fail} failed ===`)
  process.exit(fail === 0 ? 0 : 1)
}

main().catch((err) => {
  console.error('FATAL:', err)
  process.exit(2)
})
