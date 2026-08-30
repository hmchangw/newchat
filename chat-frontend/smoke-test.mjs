// Programmatic smoke test for chat-frontend integration
// Tests: auth -> NATS WebSocket connect -> send message -> receive message -> request/reply
//
// Run with `npm run smoke` (uses Node's --experimental-strip-types for the
// .ts subject builders in api/_transport). Requires a live local stack
// (auth-service on :8080, NATS WS on :4223).
import { connect, StringCodec, jwtAuthenticator } from 'nats.ws'
import { createUser } from 'nkeys.js'
// Reach into `api/_transport/` directly — smoke tests operate at the wire
// layer (raw subjects + connections) so the public api/ barrel doesn't
// add value here. Production code stays behind the barrel.
import { roomEvent, roomsList, userRoomEvent } from './src/api/_transport/subjects.ts'

const sc = StringCodec()
const AUTH_URL = 'http://localhost:8080'
const NATS_URL = 'ws://localhost:4223'

async function authenticate(account) {
  const nkey = createUser()
  const natsPublicKey = nkey.getPublicKey()

  const resp = await fetch(`${AUTH_URL}/auth`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ account, natsPublicKey }),
  })

  if (!resp.ok) throw new Error(`Auth failed: ${resp.status}`)
  const data = await resp.json()
  return { nkey, natsPublicKey, jwt: data.natsJwt, user: data.user }
}

async function connectNats(auth) {
  const nc = await connect({
    servers: NATS_URL,
    authenticator: jwtAuthenticator(auth.jwt, auth.nkey.getSeed()),
    // Replies ride the client's own user namespace, already granted as
    // `chat.user.{{tag(account)}}.>`. Used verbatim: auth-service returns the
    // JWT tag value itself, so there is nothing to normalise here.
    inboxPrefix: `chat.user.${auth.user.account}`,
  })
  return nc
}

async function main() {
  console.log('=== Chat Frontend Smoke Test ===\n')

  console.log('1. Authenticating alice...')
  const aliceAuth = await authenticate('alice')
  console.log(`   ✓ Got JWT for ${aliceAuth.user.account} (${aliceAuth.user.email})`)

  console.log('2. Connecting alice to NATS WebSocket...')
  const aliceNc = await connectNats(aliceAuth)
  console.log('   ✓ Connected')

  console.log('3. Authenticating bob...')
  const bobAuth = await authenticate('bob')
  console.log(`   ✓ Got JWT for ${bobAuth.user.account} (${bobAuth.user.email})`)

  console.log('4. Connecting bob to NATS WebSocket...')
  const bobNc = await connectNats(bobAuth)
  console.log('   ✓ Connected')

  console.log('5. Alice subscribes to room events...')
  const roomId = 'test-room-' + Date.now()
  const received = []
  const sub = aliceNc.subscribe(roomEvent(roomId))
  ;(async () => {
    for await (const msg of sub) {
      received.push(JSON.parse(sc.decode(msg.data)))
    }
  })()
  console.log(`   ✓ Subscribed to ${roomEvent(roomId)}`)

  console.log('6. Bob publishes a message to the room...')
  const testEvent = {
    type: 'new_message',
    roomId,
    message: {
      id: 'msg-' + Date.now(),
      content: 'Hello from bob!',
      sender: { account: 'bob', engName: 'bob' },
      createdAt: new Date().toISOString(),
    },
    timestamp: Date.now(),
  }
  bobNc.publish(roomEvent(roomId), sc.encode(JSON.stringify(testEvent)))
  console.log('   ✓ Published')

  console.log('7. Waiting for alice to receive the message...')
  await new Promise(r => setTimeout(r, 500))

  if (received.length > 0) {
    const msg = received[0]
    console.log(`   ✓ Alice received: "${msg.message.content}" from ${msg.message.sender.account}`)
  } else {
    console.log('   ✗ Alice did NOT receive the message')
    process.exitCode = 1
  }

  console.log('7b. Bob subscribes to his DM event stream...')
  const bobDmReceived = []
  const bobDmSub = bobNc.subscribe(userRoomEvent('bob'))
  ;(async () => {
    for await (const msg of bobDmSub) {
      bobDmReceived.push(JSON.parse(sc.decode(msg.data)))
    }
  })()
  console.log(`   ✓ Subscribed to ${userRoomEvent('bob')}`)

  console.log('7c. Alice publishes a DM event directly to bob...')
  const dmMsgId = 'dm-msg-' + Date.now()
  const nowIso = new Date().toISOString()
  const dmEvent = {
    type: 'new_message',
    roomId: 'dm-' + Date.now(),
    roomType: 'dm',
    hasMention: false,
    lastMsgAt: nowIso,
    lastMsgId: dmMsgId,
    message: {
      id: dmMsgId,
      content: 'Direct hello from alice',
      sender: { account: 'alice', engName: 'alice' },
      createdAt: nowIso,
    },
    timestamp: Date.now(),
  }
  aliceNc.publish(userRoomEvent('bob'), sc.encode(JSON.stringify(dmEvent)))
  console.log('   ✓ Published DM event')

  console.log('7d. Waiting for bob to receive the DM event...')
  await new Promise((r) => setTimeout(r, 500))
  if (bobDmReceived.length > 0 && bobDmReceived[0].message.content === 'Direct hello from alice') {
    console.log(`   ✓ Bob received DM: "${bobDmReceived[0].message.content}"`)
  } else {
    console.log('   ✗ Bob did NOT receive the DM event')
    process.exitCode = 1
  }
  bobDmSub.unsubscribe()

  console.log('8. Testing request/reply pattern...')
  const respSub = aliceNc.subscribe(roomsList('alice'))
  ;(async () => {
    for await (const msg of respSub) {
      const reply = { rooms: [{ id: roomId, name: 'test-room', type: 'channel', userCount: 2 }] }
      msg.respond(sc.encode(JSON.stringify(reply)))
      respSub.unsubscribe()
    }
  })()

  const resp = await aliceNc.request(
    roomsList('alice'),
    sc.encode(JSON.stringify({})),
    { timeout: 3000 }
  )
  const rooms = JSON.parse(sc.decode(resp.data))
  if (rooms.rooms && rooms.rooms.length > 0) {
    console.log(`   ✓ Got ${rooms.rooms.length} room(s): "${rooms.rooms[0].name}"`)
  } else {
    console.log('   ✗ Request/reply failed')
    process.exitCode = 1
  }

  console.log('\n9. Cleaning up...')
  sub.unsubscribe()
  await aliceNc.drain()
  await bobNc.drain()
  console.log('   ✓ Connections drained')

  console.log('\n=== Smoke Test Complete ===')
}

main().catch(err => {
  console.error('FAILED:', err.message)
  process.exitCode = 1
})
