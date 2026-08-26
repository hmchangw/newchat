import { useEffect } from 'react'
import { render, screen, fireEvent, act } from '@testing-library/react'
import { describe, it, expect, vi, beforeAll } from 'vitest'

// jsdom doesn't implement scrollIntoView; both ThreadMessageArea and
// MessageList call it on bottomRef. Stub at the prototype.
beforeAll(() => {
  if (!window.HTMLElement.prototype.scrollIntoView) {
    window.HTMLElement.prototype.scrollIntoView = vi.fn()
  }
})
import { NatsContext } from '@/context/NatsContext'
import { RoomEventsProvider, useRoomDispatch } from '@/context/RoomEventsContext'
import { ThreadEventsProvider } from '@/context/ThreadEventsContext'
import MainApp from './MainApp'

// Real ThreadEventsContext + RoomEventsContext; only NATS is mocked at the
// bottom layer with a fake provider. This exercises the click-to-mount chain
// the same way the browser does: MessageRow hover -> Thread button click ->
// ChatPage.handleThread -> openThread -> reducer -> activeParent -> MainApp
// re-render -> ThreadRightBar mount.
//
// If this test passes but the user reports the panel doesn't appear, the
// remaining suspects are CSS clipping or browser-specific (nginx caching).

// RoomEventsProvider calls useRoomKeys() internally. Stub it so this test
// doesn't need a real RoomKeysProvider (which would try to connect to NATS).
vi.mock('@/context/RoomKeysContext', () => ({
  useRoomKeys: () => ({ decrypt: async () => null, hasKey: () => false }),
}))

// RoomEventsProvider also calls useDegraded() to report history-load
// outcomes. Stub it so this test doesn't need a real DegradedProvider.
const degradedMock = {
  historyDegraded: false,
  noteHistoryFailure: () => {},
  noteHistorySuccess: () => {},
}
vi.mock('@/context/DegradedContext', () => ({
  useDegraded: () => degradedMock,
}))

vi.mock('./AppHeader/AppHeader', () => ({
  default: ({ onSelectRoom }) => (
    <header>
      <button type="button" onClick={() => onSelectRoom({ id: 'r1', siteId: 's1', name: 'general', type: 'channel' })}>
        pick-r1
      </button>
    </header>
  ),
}))
vi.mock('./Sidebar/Sidebar', () => ({
  default: ({ onSelectRoom }) => (
    <aside>
      <button type="button" onClick={() => onSelectRoom({ id: 'r1', siteId: 's1', name: 'general', type: 'channel' })}>
        side-pick-r1
      </button>
    </aside>
  ),
}))
vi.mock('./ChatPage/RoomMembersBadge/RoomMembersBadge', () => ({ default: () => null }))
vi.mock('./ChatPage/InRoomSearch/InRoomSearch', () => ({ default: () => null }))
vi.mock('./ChatPage/ManageMembersDialog/ManageMembersDialog', () => ({ default: () => null }))
vi.mock('../shared/MessageList/MessageRow/MessageActions/MessageActionMenu/MessageActionMenu', () => ({ default: () => null }))
// Bypass the full RoomMessageArea/RoomMessageInput pipeline; we just want
// a button that simulates a Thread click from a MessageRow.
vi.mock('./ChatPage/RoomMessageInput/RoomMessageInput', () => ({ default: () => null }))
vi.mock('./ChatPage/RoomMessageArea/RoomMessageArea', () => ({
  default: ({ onThread }) => (
    <div>
      <button
        type="button"
        onClick={() =>
          onThread?.({
            id: 'm1',
            createdAt: '2026-05-13T10:00:00Z',
            sender: { account: 'alice', engName: 'Alice' },
            content: 'hello',
          })
        }
      >
        click-thread
      </button>
    </div>
  ),
}))
// Real ThreadMessageArea + ThreadMessageInput — we want to catch any
// runtime error those would throw when the panel mounts. (If the panel
// mounts but one of these throws, jsdom will surface the error and the
// test will fail with a useful stack.)

function FakeNatsProvider({ children }) {
  const value = {
    connected: true,
    user: { account: 'alice', engName: 'Alice', siteId: 's1' },
    error: null,
    connect: vi.fn(),
    request: vi.fn(() => new Promise(() => {})), // never resolves; we only care about state
    requestWithAsyncResult: vi.fn(() => new Promise(() => {})),
    publish: vi.fn(),
    subscribe: vi.fn(() => ({ unsubscribe: vi.fn() })),
    disconnect: vi.fn(),
  }
  return <NatsContext.Provider value={value}>{children}</NatsContext.Provider>
}

function SeedRoomSummaries({ children }) {
  const dispatch = useRoomDispatch()
  useEffect(() => {
    dispatch({
      type: 'ROOMS_LOADED',
      rooms: [
        { id: 'r1', name: 'general', type: 'channel', siteId: 's1', userCount: 5, lastMsgAt: '2026-05-13T10:00:00Z' },
      ],
    })
  }, [dispatch])
  return children
}

describe('MainApp — integration: Thread button → ThreadRightBar mount (REAL providers)', () => {
  it('clicking a Thread button on a message mounts the thread panel', async () => {
    const { container } = render(
      <FakeNatsProvider>
        <RoomEventsProvider>
          <ThreadEventsProvider>
            <SeedRoomSummaries>
              <MainApp />
            </SeedRoomSummaries>
          </ThreadEventsProvider>
        </RoomEventsProvider>
      </FakeNatsProvider>
    )

    // Before any room is picked there's no thread panel.
    expect(container.querySelector('aside.thread-rightbar')).toBeNull()

    // Pick room r1 so ChatPage gets a selectedRoom.
    fireEvent.click(screen.getByText('pick-r1'))

    // Click the simulated Thread button surfaced by the mocked RoomMessageArea.
    await act(async () => {
      fireEvent.click(screen.getByText('click-thread'))
    })

    // ThreadRightBar should now be in the DOM, with the real ThreadMessageArea
    // and ThreadMessageInput mounted inside it. Lazy-loaded so we await the
    // chunk. If a real renderer throws, jsdom surfaces the error and this
    // test fails with a stack trace.
    expect(await screen.findByRole('button', { name: /close thread/i })).toBeInTheDocument()
    expect(container.querySelector('aside.thread-rightbar')).not.toBeNull()
  })
})
