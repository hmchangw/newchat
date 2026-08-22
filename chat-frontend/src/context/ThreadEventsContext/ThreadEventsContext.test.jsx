import { render, screen, act } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { ThreadEventsProvider, useThreadEvents } from './ThreadEventsContext'

const request = vi.fn()
const publish = vi.fn()
// Records the order of transport calls so a test can assert that the thread
// subscription opens BEFORE history is fetched.
const callOrder = []
const unsubscribe = vi.fn()
let threadEventHandler = null
const subscribe = vi.fn((subj, handler) => {
  callOrder.push(`subscribe:${subj}`)
  if (subj.includes('.thread.')) threadEventHandler = handler
  return { unsubscribe }
})
vi.mock('../NatsContext/NatsContext', () => ({
  useNats: () => ({
    user: { account: 'alice', siteId: 's1' },
    request, publish, subscribe,
  }),
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

describe('ThreadEventsContext', () => {
  beforeEach(() => {
    request.mockReset()
    publish.mockReset()
    registerThreadReplyHandler.mockClear()
    registeredThreadReplyHandler = null
    subscribe.mockClear()
    unsubscribe.mockClear()
    callOrder.length = 0
    threadEventHandler = null
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

  it('sendReply publish failure (sync throw) tags _status=failed on the optimistic row', async () => {
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
    request.mockReset()
    publish.mockReset()
    roomDispatch.mockClear()
  })

  it('on successful sendReply, dispatches OWN_THREAD_REPLY_SENT to RoomEventsContext', async () => {
    request.mockResolvedValue({ messages: [], hasNext: false, nextCursor: null })
    // publish is sync void — default no-op is success.
    setup()
    await act(async () => { screen.getByText('open').click() })
    await act(async () => { screen.getByText('send').click() })
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
