import { render, screen, act } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { ThreadEventsProvider, useThreadEvents } from './ThreadEventsContext'

const request = vi.fn()
const publish = vi.fn()
// Records the order of transport calls so a test can assert that the thread
// subscription opens BEFORE history is fetched.
const callOrder = []
const unsubscribe = vi.fn()
let threadEventHandler = null
const threadHandlers = new Map()
// sendMessage subscribes on the msg.send response subject
// (chat.user.{account}.response.{requestId}) before publishing. Tests that
// need to resolve/reject a send call responseHandlers.get(subject)(reply).
const responseHandlers = new Map()
const subscribe = vi.fn((subj, handler) => {
  callOrder.push(`subscribe:${subj}`)
  if (subj.includes('.thread.')) {
    threadEventHandler = handler
    threadHandlers.set(subj, handler)
  } else if (subj.includes('.response.')) {
    responseHandlers.set(subj, handler)
  }
  return { unsubscribe }
})
let currentUser = { account: 'alice', siteId: 's1' }
vi.mock('../NatsContext/NatsContext', () => ({
  useNats: () => ({ user: currentUser, request, publish, subscribe }),
}))
vi.mock('@/lib/idgen', () => ({ generateMessageID: () => 'OPT-000000000000000000' }))
vi.mock('uuid', () => ({ v4: () => 'req-uuid' }))

const roomDispatch = vi.fn()
let registeredThreadReplyHandler = null
const registerThreadReplyHandler = vi.fn((handler) => {
  registeredThreadReplyHandler = handler
  return () => {
    if (registeredThreadReplyHandler === handler) registeredThreadReplyHandler = null
  }
})
let registeredThreadMessageMutationHandler = null
const registerThreadMessageMutationHandler = vi.fn((handler) => {
  registeredThreadMessageMutationHandler = handler
  return () => {
    if (registeredThreadMessageMutationHandler === handler) registeredThreadMessageMutationHandler = null
  }
})
const decrypt = vi.fn(async () => null)
const ensureKey = vi.fn(async () => false)
vi.mock('@/context/RoomKeysContext', () => ({
  useRoomKeys: () => ({ decrypt, ensureKey }),
}))
vi.mock('../RoomEventsContext/RoomEventsContext', () => ({
  useRoomDispatch: () => roomDispatch,
  useRegisterThreadReplyHandler: () => registerThreadReplyHandler,
  useRegisterThreadMessageMutationHandler: () => registerThreadMessageMutationHandler,
}))

function Probe() {
  const t = useThreadEvents()
  return (
    <div>
      <span>active:{t.activeParent?.messageId ?? 'none'}</span>
      <span>count:{t.messages.length}</span>
      <span>firstContent:{t.messages[0]?.content ?? 'none'}</span>
      <span>firstDeleted:{String(Boolean(t.messages[0]?.deleted))}</span>
      <span>firstEditedAt:{t.messages[0]?.editedAt ?? 'none'}</span>
      <span>loaded:{String(t.hasLoadedHistory)}</span>
      <span>loading:{String(t.historyLoading)}</span>
      <span>error:{t.historyError ?? 'none'}</span>
      <button type="button" onClick={() => t.openThread({ roomId: 'r1', siteId: 's1', messageId: 'p1', createdAtMs: 1000, crossSite: true })}>open</button>
      <button type="button" onClick={() => t.openThread({ roomId: 'r1', siteId: 's1', messageId: 'p2', createdAtMs: 2000, crossSite: true })}>open-p2</button>
      <button type="button" onClick={() => t.openThread({ roomId: 'r2', siteId: 's1', messageId: 'p3', createdAtMs: 3000, crossSite: false })}>open-local</button>
      <button type="button" onClick={() => t.closeThread()}>close</button>
      <button type="button" onClick={() => t.sendReply('hi', {})}>send</button>
      <button type="button" onClick={() => t.sendReply('q-hi', { quotedParentMessageId: 'q-id' })}>send-quote</button>
      <button type="button" onClick={() => t.retryReply('OPT-000000000000000000')}>retry</button>
      <button type="button" onClick={() => t.dismissReply('OPT-000000000000000000')}>dismiss</button>
    </div>
  )
}

const setup = () =>
  render(<ThreadEventsProvider><Probe /></ThreadEventsProvider>)

// Simulates the gatekeeper's reply on the msg.send response subject for the
// most recently subscribed send (requestId is mocked constant, so the
// subject is always chat.user.alice.response.req-uuid).
const respondToSend = (reply) => {
  responseHandlers.get('chat.user.alice.response.req-uuid')?.(reply)
}

describe('ThreadEventsContext', () => {
  beforeEach(() => {
    // sendReply/retryReply drive sendMessage's internal setTimeout
    // (default 10s). Fake timers keep any un-settled send from leaving a
    // real timer pending past the test — a real one can fire during a
    // later test file's run under vitest's shared worker pool.
    vi.useFakeTimers({ shouldAdvanceTime: true })
    request.mockReset()
    publish.mockReset()
    registerThreadReplyHandler.mockClear()
    registeredThreadReplyHandler = null
    subscribe.mockClear()
    unsubscribe.mockClear()
    callOrder.length = 0
    threadEventHandler = null
    responseHandlers.clear()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('openThread sets activeParent and fires msg.thread RPC; on success dispatches HISTORY_LOADED', async () => {
    request.mockResolvedValueOnce({ messages: [{ id: 'r1' }, { id: 'r2' }], hasNext: false, nextCursor: null })
    setup()
    expect(screen.getByText('active:none')).toBeInTheDocument()
    await act(async () => {
      screen.getByText('open').click()
    })
    expect(request).toHaveBeenCalledWith(
      'chat.user.alice.request.room.r1.s1.msg.thread',
      { threadMessageId: 'p1', limit: 50 }
    )
    expect(screen.getByText('active:p1')).toBeInTheDocument()
    expect(screen.getByText('count:2')).toBeInTheDocument()
    expect(screen.getByText('loaded:true')).toBeInTheDocument()
    expect(screen.getByText('loading:false')).toBeInTheDocument()
  })

  it('openThread RPC failure dispatches HISTORY_FAILED', async () => {
    request.mockRejectedValueOnce(new Error('boom'))
    setup()
    await act(async () => { screen.getByText('open').click() })
    expect(screen.getByText('error:boom')).toBeInTheDocument()
    expect(screen.getByText('loading:false')).toBeInTheDocument()
  })

  it('opening the same parent twice short-circuits (no second RPC)', async () => {
    request.mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    setup()
    await act(async () => { screen.getByText('open').click() })
    request.mockClear()
    await act(async () => { screen.getByText('open').click() })
    expect(request).not.toHaveBeenCalled()
  })

  it('closeThread resets state', async () => {
    request.mockResolvedValue({ messages: [{ id: 'r1' }], hasNext: false, nextCursor: null })
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('close').click() })
    expect(screen.getByText('active:none')).toBeInTheDocument()
    expect(screen.getByText('count:0')).toBeInTheDocument()
  })

  it('sendReply optimistically appends and publishes msg.send with thread parent fields', async () => {
    request.mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    // publish is sync void — default no-op is fine.
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('send').click() })
    expect(screen.getByText('count:1')).toBeInTheDocument()
    expect(publish).toHaveBeenCalledWith(
      'chat.user.alice.room.r1.s1.msg.send',
      {
        id: 'OPT-000000000000000000',
        content: 'hi',
        requestId: 'req-uuid',
        threadParentMessageId: 'p1',
        threadParentMessageCreatedAt: 1000,
      }
    )
  })

  it('sendReply with quotedParentMessageId carries the field in the payload', async () => {
    request.mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('send-quote').click() })
    const call = publish.mock.calls[0]
    expect(call[1].quotedParentMessageId).toBe('q-id')
  })

  it('sendReply publish failure (publish throws) tags _status=failed on the optimistic row', async () => {
    request.mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    publish.mockImplementation(() => { throw new Error('Not connected') })
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('send').click() })
    // Probe doesn't expose per-row _status, but count:1 is enough — the
    // reducer test verifies _status='failed' precisely.
    expect(screen.getByText('count:1')).toBeInTheDocument()
  })

  it('dismissReply removes the row', async () => {
    request.mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    publish.mockImplementation(() => { throw new Error('Not connected') })
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('send').click() })
    await act(async () => { screen.getByText('dismiss').click() })
    expect(screen.getByText('count:0')).toBeInTheDocument()
  })
})

describe('ThreadEventsContext — cross-dispatch OWN_THREAD_REPLY_SENT', () => {
  beforeEach(() => {
    // See the fake-timers comment in the describe block above — same reason.
    vi.useFakeTimers({ shouldAdvanceTime: true })
    request.mockReset()
    publish.mockReset()
    roomDispatch.mockClear()
    responseHandlers.clear()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('on successful sendReply, dispatches OWN_THREAD_REPLY_SENT to RoomEventsContext once the gatekeeper confirms', async () => {
    request.mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('send').click() })
    // Not yet — OWN_THREAD_REPLY_SENT only fires once the gatekeeper confirms.
    expect(roomDispatch).not.toHaveBeenCalled()
    await act(async () => { respondToSend({ id: 'OPT-000000000000000000' }) })
    expect(roomDispatch).toHaveBeenCalledWith({
      type: 'OWN_THREAD_REPLY_SENT',
      roomId: 'r1',
      parentId: 'p1',
      replyId: 'OPT-000000000000000000',
    })
  })

  it('does NOT dispatch when publish throws synchronously', async () => {
    request.mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    publish.mockImplementation(() => { throw new Error('Not connected') })
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('send').click() })
    expect(roomDispatch).not.toHaveBeenCalled()
  })

  it('does NOT dispatch when the gatekeeper refuses the send', async () => {
    request.mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('send').click() })
    await act(async () => { respondToSend({ error: 'nope', code: 'unavailable', reason: 'thread_start_unavailable' }) })
    expect(roomDispatch).not.toHaveBeenCalled()
  })

  it('retryReply does NOT re-dispatch OWN_THREAD_REPLY_SENT (the original send already counted)', async () => {
    request.mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    // First send fails synchronously so retryReply has something to retry.
    publish.mockImplementationOnce(() => { throw new Error('Not connected') })
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('send').click() })
    // The initial send failed — no roomDispatch fired (covered by the test above).
    expect(roomDispatch).not.toHaveBeenCalled()
    // Now succeed on retry.
    publish.mockImplementation(() => {})
    await act(async () => { screen.getByText('retry').click() })
    await act(async () => { respondToSend({ id: 'OPT-000000000000000000' }) })
    // Even though the retry succeeded, the tcount should not be bumped by the
    // retry path — the room reducer assumes one increment per logical send,
    // and the initial sendReply already owned that responsibility (it just
    // happened to fail; the next successful send is a continuation, not a new
    // reply).
    expect(roomDispatch).not.toHaveBeenCalled()
  })
})

describe('ThreadEventsContext — live THREAD_REPLY_RECEIVED bridge', () => {
  beforeEach(() => {
    request.mockReset()
    publish.mockReset()
    registerThreadReplyHandler.mockClear()
    registeredThreadReplyHandler = null
  })

  it('registers a handler on mount and unregisters on unmount', () => {
    const { unmount } = setup()
    expect(registerThreadReplyHandler).toHaveBeenCalledTimes(1)
    expect(typeof registeredThreadReplyHandler).toBe('function')
    unmount()
    expect(registeredThreadReplyHandler).toBe(null)
  })

  it('appends an inbound thread reply when the open thread matches', async () => {
    request.mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      registeredThreadReplyHandler({
        parentMessageId: 'p1',
        roomId: 'r1',
        siteId: 's1',
        message: { id: 'live-1', content: 'from B', threadParentMessageId: 'p1' },
      })
    })
    expect(screen.getByText('count:1')).toBeInTheDocument()
  })

  it('ignores inbound thread replies when no thread is open', async () => {
    setup()
    expect(screen.getByText('active:none')).toBeInTheDocument()
    await act(async () => {
      registeredThreadReplyHandler({
        parentMessageId: 'p1',
        roomId: 'r1',
        siteId: 's1',
        message: { id: 'live-1', threadParentMessageId: 'p1' },
      })
    })
    expect(screen.getByText('count:0')).toBeInTheDocument()
  })
})

describe('ThreadEventsContext — live thread-message mutation bridge', () => {
  beforeEach(() => {
    request.mockReset()
    publish.mockReset()
    registerThreadMessageMutationHandler.mockClear()
    registeredThreadMessageMutationHandler = null
  })

  it('registers a mutation handler on mount and unregisters on unmount', () => {
    const { unmount } = setup()
    expect(registerThreadMessageMutationHandler).toHaveBeenCalledTimes(1)
    expect(typeof registeredThreadMessageMutationHandler).toBe('function')
    unmount()
    expect(registeredThreadMessageMutationHandler).toBe(null)
  })

  it("applies an inbound 'edited' mutation to the open thread message", async () => {
    request.mockResolvedValue({
      messages: [{ id: 'r1', content: 'old', sender: { account: 'bob' } }],
      hasNext: false,
      nextCursor: null,
    })
    setup()
    await act(async () => { screen.getByText('open').click() })
    expect(screen.getByText('firstContent:old')).toBeInTheDocument()
    await act(async () => {
      registeredThreadMessageMutationHandler({
        kind: 'edited',
        messageId: 'r1',
        content: 'edited!',
        editedAt: '2026-05-19T10:00:00Z',
      })
    })
    expect(screen.getByText('firstContent:edited!')).toBeInTheDocument()
    expect(screen.getByText('firstEditedAt:2026-05-19T10:00:00Z')).toBeInTheDocument()
  })

  it("applies an inbound 'deleted' mutation to the open thread message", async () => {
    request.mockResolvedValue({
      messages: [{ id: 'r1', content: 'x', sender: { account: 'bob' } }],
      hasNext: false,
      nextCursor: null,
    })
    setup()
    await act(async () => { screen.getByText('open').click() })
    expect(screen.getByText('firstDeleted:false')).toBeInTheDocument()
    await act(async () => {
      registeredThreadMessageMutationHandler({ kind: 'deleted', messageId: 'r1' })
    })
    expect(screen.getByText('firstDeleted:true')).toBeInTheDocument()
  })
})

describe('ThreadEventsContext thread-view subscription', () => {
  beforeEach(() => {
    request.mockReset()
    subscribe.mockClear()
    unsubscribe.mockClear()
    callOrder.length = 0
    threadEventHandler = null
    threadHandlers.clear()
    currentUser = { account: 'alice', siteId: 's1' }
    decrypt.mockReset().mockResolvedValue(null)
    ensureKey.mockReset().mockResolvedValue(false)
    request.mockImplementation(() => {
      callOrder.push('request')
      return Promise.resolve({ messages: [], hasNext: false, nextCursor: null })
    })
  })

  it('subscribes to the thread subject while the panel is open', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    expect(subscribe).toHaveBeenCalledWith('chat.room.r1.thread.p1.event', expect.any(Function))
  })

  it('routes a same-site room to the local thread subject', async () => {
    setup()
    await act(async () => { screen.getByText('open-local').click() })
    expect(subscribe).toHaveBeenCalledWith('chat.local.room.r2.thread.p3.event', expect.any(Function))
  })

  // A reply landing between the fetch and the subscribe would be lost, which is
  // the very gap this feature closes.
  it('subscribes before fetching history', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    expect(callOrder.indexOf('subscribe:chat.room.r1.thread.p1.event')).toBeLessThan(callOrder.indexOf('request'))
  })

  it('unsubscribes when the panel closes', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('close').click() })
    expect(unsubscribe).toHaveBeenCalledTimes(1)
  })

  it('swaps the subscription when the panel switches parents', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('open-p2').click() })
    expect(unsubscribe).toHaveBeenCalledTimes(1)
    expect(subscribe).toHaveBeenLastCalledWith('chat.room.r1.thread.p2.event', expect.any(Function))
  })

  it('unsubscribes on unmount', async () => {
    const { unmount } = setup()
    await act(async () => { screen.getByText('open').click() })
    unmount()
    expect(unsubscribe).toHaveBeenCalledTimes(1)
  })

  it('appends a reply arriving on the thread subject', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      threadEventHandler({ type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'from a viewer lane' } })
    })
    expect(screen.getByText('count:1')).toBeInTheDocument()
    expect(screen.getByText('firstContent:from a viewer lane')).toBeInTheDocument()
  })

  // A follower with the panel open receives both lanes; the reducer's id guard
  // must keep that to one rendered reply.
  it('renders a reply delivered on both lanes once', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    const evt = { type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'dup' } }
    await act(async () => {
      registeredThreadReplyHandler({ parentMessageId: 'p1', message: evt.message })
      threadEventHandler(evt)
    })
    expect(screen.getByText('count:1')).toBeInTheDocument()
  })

  it('applies an edit arriving on the thread subject', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      threadEventHandler({ type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'before' } })
    })
    await act(async () => {
      threadEventHandler({ type: 'message_edited', messageId: 'm9', newContent: 'after', editedAt: '2026-08-22T10:00:00Z' })
    })
    expect(screen.getByText('firstContent:after')).toBeInTheDocument()
    expect(screen.getByText('firstEditedAt:2026-08-22T10:00:00Z')).toBeInTheDocument()
  })

  // Matches the room lane: an encrypted edit carries encryptedNewContent, and
  // blanking to '' would wipe the rendered reply.
  it('ignores an edit whose newContent is not a plaintext string', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      threadEventHandler({ type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'keep me' } })
    })
    await act(async () => {
      threadEventHandler({ type: 'message_edited', messageId: 'm9', encryptedNewContent: { version: 3 } })
    })
    expect(screen.getByText('firstContent:keep me')).toBeInTheDocument()
  })

  it('normalizes a non-string editedAt to an ISO string', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      threadEventHandler({ type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'before' } })
    })
    await act(async () => {
      threadEventHandler({ type: 'message_edited', messageId: 'm9', newContent: 'after', editedAt: 1756000000000 })
    })
    expect(screen.getByText('firstEditedAt:2025-08-24T01:46:40.000Z')).toBeInTheDocument()
  })

  // Channel rooms are encrypted, so without a decrypt step every reply on this
  // lane would arrive as an empty envelope and be dropped.
  it('decrypts an encrypted reply arriving on the thread subject', async () => {
    decrypt.mockResolvedValue(JSON.stringify({ id: 'm9', content: 'sealed then opened' }))
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      await threadEventHandler({
        type: 'new_thread_message',
        roomId: 'r1',
        encryptedMessage: { version: 3, nonce: 'bm9uY2U=', ciphertext: 'Y2lwaGVy' },
      })
    })
    expect(screen.getByText('firstContent:sealed then opened')).toBeInTheDocument()
  })

  // The NATS subscribe loop does not await its callback, so a plaintext delete
  // can finalize before a prior encrypted create resolves. Without
  // serialization the delete lands on an absent id, is dropped, and the create
  // then appends the reply as live — a deleted message rendering until reload.
  it('applies a fast plaintext delete after a slow encrypted create, not before', async () => {
    let releaseDecrypt
    const decryptGate = new Promise((resolve) => { releaseDecrypt = resolve })
    decrypt.mockImplementation(async () => {
      await decryptGate
      return JSON.stringify({ id: 'm9', content: 'slow' })
    })
    setup()
    await act(async () => { screen.getByText('open').click() })

    // Both events are handed to the callback before the create's decrypt resolves.
    let created, deleted
    await act(async () => {
      created = threadEventHandler({
        type: 'new_thread_message',
        roomId: 'r1',
        encryptedMessage: { version: 3, nonce: 'bm9uY2U=', ciphertext: 'Y2lwaGVy' },
      })
      deleted = threadEventHandler({ type: 'message_deleted', messageId: 'm9' })
      releaseDecrypt()
      await Promise.all([created, deleted])
    })

    expect(screen.getByText('count:1')).toBeInTheDocument()
    expect(screen.getByText('firstDeleted:true')).toBeInTheDocument()
  })

  // Edits ride the room namespace sealed too, so the lane must open them.
  it('decrypts an encrypted edit arriving on the thread subject', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      await threadEventHandler({ type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'before' } })
    })
    decrypt.mockResolvedValue('after')
    await act(async () => {
      await threadEventHandler({
        type: 'message_edited',
        roomId: 'r1',
        messageId: 'm9',
        encryptedNewContent: { version: 3, nonce: 'bm9uY2U=', ciphertext: 'Y2lwaGVy' },
        editedAt: '2026-08-22T10:00:00Z',
      })
    })
    expect(screen.getByText('firstContent:after')).toBeInTheDocument()
  })

  it('applies a delete arriving on the thread subject', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      threadEventHandler({ type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'doomed' } })
    })
    await act(async () => {
      threadEventHandler({ type: 'message_deleted', messageId: 'm9' })
    })
    expect(screen.getByText('firstDeleted:true')).toBeInTheDocument()
  })
})

describe('ThreadEventsContext thread-lane robustness', () => {
  beforeEach(() => {
    request.mockReset().mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    subscribe.mockClear()
    unsubscribe.mockClear()
    threadEventHandler = null
    threadHandlers.clear()
    decrypt.mockReset().mockResolvedValue(null)
    ensureKey.mockReset().mockResolvedValue(false)
  })

  // decryptRoomEvent returns the event unchanged when the room key has not
  // arrived, so the reply has no plaintext body. Dropping it leaves the panel
  // silently missing a message; the room timeline shows a placeholder instead.
  it('renders a placeholder when the room key has not arrived', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      await threadEventHandler({
        type: 'new_thread_message',
        roomId: 'r1',
        lastMsgId: 'm9',
        lastMsgAt: '2026-08-22T10:00:00Z',
        timestamp: 1756000000000,
        encryptedMessage: { version: 3, nonce: 'bm9uY2U=', ciphertext: 'Y2lwaGVy' },
      })
    })
    expect(screen.getByText('count:1')).toBeInTheDocument()
    expect(screen.getByText('firstContent:[encrypted message]')).toBeInTheDocument()
  })

  it('drops an undecryptable reply that carries no id to fall back on', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      await threadEventHandler({
        type: 'new_thread_message',
        roomId: 'r1',
        encryptedMessage: { version: 3, nonce: 'bm9uY2U=', ciphertext: 'Y2lwaGVy' },
      })
    })
    expect(screen.getByText('count:0')).toBeInTheDocument()
  })

  // Arrival order is not causal order: a redelivered older edit must not
  // overwrite a newer one already applied.
  it('ignores an edit older than the one already applied', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      await threadEventHandler({ type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'v0' } })
    })
    await act(async () => {
      await threadEventHandler({
        type: 'message_edited', messageId: 'm9', newContent: 'v2', editedAt: '2026-08-22T10:00:02Z',
      })
    })
    await act(async () => {
      await threadEventHandler({
        type: 'message_edited', messageId: 'm9', newContent: 'v1', editedAt: '2026-08-22T10:00:01Z',
      })
    })
    expect(screen.getByText('firstContent:v2')).toBeInTheDocument()
  })

  it('still applies a strictly newer edit', async () => {
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => {
      await threadEventHandler({ type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'v0' } })
    })
    for (const [content, at] of [['v1', '2026-08-22T10:00:01Z'], ['v2', '2026-08-22T10:00:02Z']]) {
      await act(async () => {
        await threadEventHandler({ type: 'message_edited', messageId: 'm9', newContent: content, editedAt: at })
      })
    }
    expect(screen.getByText('firstContent:v2')).toBeInTheDocument()
  })

  // A chain shared across threads lets an event stalled on a key fetch for one
  // thread delay the thread the user just opened.
  it('does not let a stalled event on one thread block the next', async () => {
    let releaseStalled
    const gate = new Promise((resolve) => { releaseStalled = resolve })
    decrypt.mockImplementation(async () => { await gate; return null })

    setup()
    await act(async () => { screen.getByText('open').click() })
    const stalledHandler = threadHandlers.get('chat.room.r1.thread.p1.event')
    let stalled
    await act(async () => {
      stalled = stalledHandler({
        type: 'new_thread_message', roomId: 'r1',
        encryptedMessage: { version: 3, nonce: 'bm9uY2U=', ciphertext: 'Y2lwaGVy' },
      })
      await Promise.resolve()
    })

    await act(async () => { screen.getByText('open-p2').click() })
    await act(async () => {
      await threadHandlers.get('chat.room.r1.thread.p2.event')({
        type: 'new_thread_message', roomId: 'r1', message: { id: 'm-new', content: 'not blocked' },
      })
    })
    expect(screen.getByText('firstContent:not blocked')).toBeInTheDocument()

    await act(async () => { releaseStalled(); await stalled })
  })
})

describe('ThreadEventsContext logout lifecycle', () => {
  beforeEach(() => {
    request.mockReset().mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    subscribe.mockClear()
    unsubscribe.mockClear()
    threadEventHandler = null
    threadHandlers.clear()
    currentUser = { account: 'alice', siteId: 's1' }
    decrypt.mockReset().mockResolvedValue(null)
    ensureKey.mockReset().mockResolvedValue(false)
  })

  // The provider stays mounted across a logout, so unmount cleanup never runs.
  // Leaving the subscription open keeps a previous session's lane live.
  it('closes the thread subscription on logout', async () => {
    const { rerender } = setup()
    await act(async () => { screen.getByText('open').click() })
    expect(subscribe).toHaveBeenCalledTimes(1)

    currentUser = null
    await act(async () => {
      rerender(<ThreadEventsProvider><Probe /></ThreadEventsProvider>)
    })
    expect(unsubscribe).toHaveBeenCalledTimes(1)
  })

  // Generation must advance too, or work already queued for the old session
  // still lands after the reset.
  it('invalidates in-flight work queued before logout', async () => {
    let releaseDecrypt
    const gate = new Promise((resolve) => { releaseDecrypt = resolve })
    decrypt.mockImplementation(async () => { await gate; return JSON.stringify({ id: 'm9', content: 'stale session' }) })

    const { rerender } = setup()
    await act(async () => { screen.getByText('open').click() })
    let inFlight
    await act(async () => {
      inFlight = threadEventHandler({
        type: 'new_thread_message', roomId: 'r1',
        encryptedMessage: { version: 3, nonce: 'bm9uY2U=', ciphertext: 'Y2lwaGVy' },
      })
      await Promise.resolve()
    })

    currentUser = null
    await act(async () => {
      rerender(<ThreadEventsProvider><Probe /></ThreadEventsProvider>)
      releaseDecrypt()
      await inFlight
    })
    expect(screen.getByText('count:0')).toBeInTheDocument()
  })
})

describe('ThreadEventsContext thread-lane failure isolation', () => {
  beforeEach(() => {
    request.mockReset().mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    subscribe.mockClear()
    unsubscribe.mockClear()
    threadEventHandler = null
    threadHandlers.clear()
    currentUser = { account: 'alice', siteId: 's1' }
    decrypt.mockReset().mockResolvedValue(null)
    ensureKey.mockReset().mockResolvedValue(false)
  })

  // A malformed event must not reject the chain: the subscribe loop ignores the
  // returned promise, so the rejection would surface as an unhandled rejection
  // and the event would vanish with nothing logged.
  it('settles and logs when an event throws, and keeps the lane alive', async () => {
    const onError = vi.spyOn(console, 'error').mockImplementation(() => {})
    setup()
    await act(async () => { screen.getByText('open').click() })

    // A non-string, unparseable editedAt makes normalizeEditedAt throw.
    await act(async () => {
      await expect(
        threadEventHandler({ type: 'message_edited', messageId: 'm1', newContent: 'x', editedAt: {} })
      ).resolves.toBeUndefined()
    })
    expect(onError).toHaveBeenCalled()

    await act(async () => {
      await threadEventHandler({ type: 'new_thread_message', roomId: 'r1', message: { id: 'm9', content: 'still working' } })
    })
    expect(screen.getByText('firstContent:still working')).toBeInTheDocument()
    onError.mockRestore()
  })
})
