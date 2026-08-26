import { describe, it, expect } from 'vitest'
import { threadEventsReducer, initialState } from './reducer'

const parent = { roomId: 'r1', siteId: 's1', messageId: 'p1', createdAtMs: 1000 }

describe('threadEventsReducer — OPEN_THREAD', () => {
  it('sets activeParent and flags loading', () => {
    const out = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    expect(out.activeParent).toEqual(parent)
    expect(out.historyLoading).toBe(true)
    expect(out.messages).toEqual([])
    expect(out.hasLoadedHistory).toBe(false)
    expect(out.historyError).toBe(null)
  })

  it('short-circuits when the same parent is already active', () => {
    const seed = { ...initialState, activeParent: parent, messages: [{ id: 'r1' }] }
    const out = threadEventsReducer(seed, { type: 'OPEN_THREAD', parent })
    expect(out).toBe(seed)
  })

  it('switches to a different parent and clears prior state', () => {
    const seed = { ...initialState, activeParent: parent, messages: [{ id: 'old' }], hasLoadedHistory: true }
    const next = { ...parent, messageId: 'p2' }
    const out = threadEventsReducer(seed, { type: 'OPEN_THREAD', parent: next })
    expect(out.activeParent).toEqual(next)
    expect(out.messages).toEqual([])
    expect(out.hasLoadedHistory).toBe(false)
    expect(out.historyLoading).toBe(true)
  })
})

describe('threadEventsReducer — CLOSE_THREAD', () => {
  it('resets to initialState', () => {
    const seed = { ...initialState, activeParent: parent, messages: [{ id: 'x' }] }
    expect(threadEventsReducer(seed, { type: 'CLOSE_THREAD' })).toEqual(initialState)
  })
})

describe('threadEventsReducer — HISTORY_LOADING', () => {
  it('sets historyLoading=true when dispatched for the active parent', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const cleared = { ...open, historyLoading: false }
    const out = threadEventsReducer(cleared, { type: 'HISTORY_LOADING', parentId: 'p1' })
    expect(out.historyLoading).toBe(true)
  })

  it('is ignored for a non-active parent', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const cleared = { ...open, historyLoading: false }
    const out = threadEventsReducer(cleared, { type: 'HISTORY_LOADING', parentId: 'other' })
    expect(out).toBe(cleared)
  })
})

describe('threadEventsReducer — HISTORY_LOADED', () => {
  const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })

  it('hydrates messages from the response', () => {
    const out = threadEventsReducer(open, {
      type: 'HISTORY_LOADED',
      parentId: 'p1',
      resp: { messages: [{ id: 'r1' }, { id: 'r2' }], hasNext: false, nextCursor: null },
    })
    expect(out.messages).toEqual([{ id: 'r1' }, { id: 'r2' }])
    expect(out.hasLoadedHistory).toBe(true)
    expect(out.historyLoading).toBe(false)
    expect(out.historyError).toBe(null)
    expect(out.hasNext).toBe(false)
    expect(out.nextCursor).toBe(null)
  })

  it('ignores results for a non-active parent', () => {
    const out = threadEventsReducer(open, {
      type: 'HISTORY_LOADED',
      parentId: 'other',
      resp: { messages: [{ id: 'r1' }], hasNext: false, nextCursor: null },
    })
    expect(out).toBe(open)
  })

  it('preserves any optimistic _local rows when merging history', () => {
    const seeded = { ...open, messages: [{ id: 'opt', _local: true, content: 'mine' }] }
    const out = threadEventsReducer(seeded, {
      type: 'HISTORY_LOADED',
      parentId: 'p1',
      resp: { messages: [{ id: 'r-from-server' }], hasNext: false, nextCursor: null },
    })
    const ids = out.messages.map((m) => m.id)
    expect(ids).toContain('opt')
    expect(ids).toContain('r-from-server')
  })
})

describe('threadEventsReducer — HISTORY_FAILED', () => {
  it('sets historyError, clears historyLoading', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const out = threadEventsReducer(open, { type: 'HISTORY_FAILED', parentId: 'p1', error: 'nope' })
    expect(out.historyError).toBe('nope')
    expect(out.historyLoading).toBe(false)
  })

  it('ignores failures for a non-active parent', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const out = threadEventsReducer(open, { type: 'HISTORY_FAILED', parentId: 'other', error: 'x' })
    expect(out).toBe(open)
  })
})

describe('threadEventsReducer — REPLY_SENT_LOCAL', () => {
  it('appends an optimistic message with _local: true', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const out = threadEventsReducer(open, {
      type: 'REPLY_SENT_LOCAL',
      message: { id: 'opt', content: 'hi', _local: true },
    })
    expect(out.messages).toEqual([{ id: 'opt', content: 'hi', _local: true }])
  })

  it('dedupes by id (no double-append)', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const once = threadEventsReducer(open, { type: 'REPLY_SENT_LOCAL', message: { id: 'opt', _local: true } })
    const twice = threadEventsReducer(once, { type: 'REPLY_SENT_LOCAL', message: { id: 'opt', _local: true } })
    expect(twice.messages).toHaveLength(1)
  })
})

describe('threadEventsReducer — THREAD_REPLY_RECEIVED', () => {
  it('appends an inbound reply when the open thread matches the parentId', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const out = threadEventsReducer(open, {
      type: 'THREAD_REPLY_RECEIVED',
      parentId: parent.messageId,
      message: { id: 'live-1', content: 'from B', threadParentMessageId: parent.messageId },
    })
    expect(out.messages.map((m) => m.id)).toEqual(['live-1'])
  })

  it('is a no-op when no thread is open (closed panel)', () => {
    const out = threadEventsReducer(initialState, {
      type: 'THREAD_REPLY_RECEIVED',
      parentId: parent.messageId,
      message: { id: 'live-1', threadParentMessageId: parent.messageId },
    })
    expect(out).toBe(initialState)
  })

  it('is a no-op when the open thread is on a different parent', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const out = threadEventsReducer(open, {
      type: 'THREAD_REPLY_RECEIVED',
      parentId: 'some-other-parent',
      message: { id: 'live-1', threadParentMessageId: 'some-other-parent' },
    })
    expect(out).toBe(open)
  })

  it('dedupes by message id (sender echo after REPLY_SENT_LOCAL)', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const local = threadEventsReducer(open, {
      type: 'REPLY_SENT_LOCAL',
      message: { id: 'opt-1', content: 'mine', _local: true },
    })
    // Server echo arrives with the same ID.
    const echoed = threadEventsReducer(local, {
      type: 'THREAD_REPLY_RECEIVED',
      parentId: parent.messageId,
      message: { id: 'opt-1', threadParentMessageId: parent.messageId },
    })
    expect(echoed).toBe(local)
  })
})

describe('threadEventsReducer — REPLY_SEND_FAILED / REPLY_RETRIED / REPLY_DISMISSED', () => {
  const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
  const sent = threadEventsReducer(open, {
    type: 'REPLY_SENT_LOCAL',
    message: { id: 'opt', _local: true, content: 'x' },
  })

  it('REPLY_SEND_FAILED marks _status: "failed" on the matching id', () => {
    const out = threadEventsReducer(sent, { type: 'REPLY_SEND_FAILED', messageId: 'opt', error: 'nope' })
    expect(out.messages[0]._status).toBe('failed')
  })

  it('REPLY_SEND_FAILED stores the curated error text as _error on the matching id', () => {
    const out = threadEventsReducer(sent, {
      type: 'REPLY_SEND_FAILED',
      messageId: 'opt',
      error: "Message history is unavailable — you can't start a new thread right now. Try again shortly.",
    })
    expect(out.messages[0]._error).toBe(
      "Message history is unavailable — you can't start a new thread right now. Try again shortly."
    )
  })

  // The point of the whole change: a refused send must never take the user's
  // text with it. Everything else here is bookkeeping around this one line.
  it('REPLY_SEND_FAILED keeps the row and its draft content intact', () => {
    const out = threadEventsReducer(sent, {
      type: 'REPLY_SEND_FAILED',
      messageId: 'opt',
      error: "Message history is unavailable — you can't start a new thread right now. Try again shortly.",
    })
    expect(out.messages).toHaveLength(1)
    expect(out.messages[0].id).toBe('opt')
    expect(out.messages[0].content).toBe('x')
    expect(out.messages[0]._local).toBe(true)
  })

  it('REPLY_RETRIED clears _status on the matching id', () => {
    const failed = threadEventsReducer(sent, { type: 'REPLY_SEND_FAILED', messageId: 'opt', error: 'nope' })
    const out = threadEventsReducer(failed, { type: 'REPLY_RETRIED', messageId: 'opt' })
    expect(out.messages[0]._status).toBeUndefined()
  })

  it('REPLY_RETRIED clears _error along with _status', () => {
    const failed = threadEventsReducer(sent, { type: 'REPLY_SEND_FAILED', messageId: 'opt', error: 'nope' })
    const out = threadEventsReducer(failed, { type: 'REPLY_RETRIED', messageId: 'opt' })
    expect(out.messages[0]._error).toBeUndefined()
  })

  it('REPLY_DISMISSED removes the row', () => {
    const failed = threadEventsReducer(sent, { type: 'REPLY_SEND_FAILED', messageId: 'opt', error: 'nope' })
    const out = threadEventsReducer(failed, { type: 'REPLY_DISMISSED', messageId: 'opt' })
    expect(out.messages).toEqual([])
  })
})

describe('threadEventsReducer — RESET', () => {
  it('returns to initialState', () => {
    const seed = { ...initialState, activeParent: parent, messages: [{ id: 'x' }] }
    expect(threadEventsReducer(seed, { type: 'RESET' })).toEqual(initialState)
  })
})

describe('threadEventsReducer — REPLY_EDITED_LOCAL', () => {
  it('updates content + editedAt on the matching message', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const seeded = { ...open, messages: [{ id: 'r1', content: 'old' }, { id: 'r2', content: 'other' }] }
    const out = threadEventsReducer(seeded, {
      type: 'REPLY_EDITED_LOCAL', messageId: 'r1', content: 'new', editedAt: '2026-05-13T12:00:00Z',
    })
    expect(out.messages[0]).toEqual({ id: 'r1', content: 'new', editedAt: '2026-05-13T12:00:00Z' })
    expect(out.messages[1]).toEqual({ id: 'r2', content: 'other' })
  })

  it('is a no-op when the messageId is not buffered', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const out = threadEventsReducer(open, {
      type: 'REPLY_EDITED_LOCAL', messageId: 'unknown', content: 'x', editedAt: 't',
    })
    expect(out).toBe(open)
  })
})

describe('threadEventsReducer — REPLY_DELETED_LOCAL', () => {
  it('flags the matching reply as deleted', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const seeded = { ...open, messages: [{ id: 'r1', content: 'bye' }] }
    const out = threadEventsReducer(seeded, { type: 'REPLY_DELETED_LOCAL', messageId: 'r1' })
    expect(out.messages[0]).toEqual({ id: 'r1', content: 'bye', deleted: true })
  })

  it('is a no-op when messageId is not buffered', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const out = threadEventsReducer(open, { type: 'REPLY_DELETED_LOCAL', messageId: 'r1' })
    expect(out).toBe(open)
  })
})

describe('threadEventsReducer — REPLY_EDITED / REPLY_DELETED live broadcast', () => {
  it('REPLY_EDITED updates the matching reply', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const seeded = { ...open, messages: [{ id: 'r1', content: 'old' }] }
    const out = threadEventsReducer(seeded, {
      type: 'REPLY_EDITED', messageId: 'r1', content: 'new', editedAt: '2026-05-19T10:00:00Z',
    })
    expect(out.messages[0]).toEqual({
      id: 'r1', content: 'new', editedAt: '2026-05-19T10:00:00Z',
    })
  })

  it('REPLY_DELETED flags the matching reply as deleted', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const seeded = { ...open, messages: [{ id: 'r1', content: 'x' }] }
    const out = threadEventsReducer(seeded, { type: 'REPLY_DELETED', messageId: 'r1' })
    expect(out.messages[0]).toEqual({ id: 'r1', content: 'x', deleted: true })
  })

  it('both are no-ops when messageId is not buffered', () => {
    const open = threadEventsReducer(initialState, { type: 'OPEN_THREAD', parent })
    const e = threadEventsReducer(open, {
      type: 'REPLY_EDITED', messageId: 'unknown', content: 'n', editedAt: 't',
    })
    expect(e).toBe(open)
    const d = threadEventsReducer(open, { type: 'REPLY_DELETED', messageId: 'unknown' })
    expect(d).toBe(open)
  })
})

// A follower with the panel open receives every mutation on both lanes; the
// reducer must return the identical state object so React bails out.
describe('threadEventsReducer duplicate-lane delivery', () => {
  const opened = threadEventsReducer(
    threadEventsReducer(initialState, {
      type: 'OPEN_THREAD',
      parent: { roomId: 'r1', siteId: 's1', messageId: 'p1', createdAtMs: 1000 },
    }),
    { type: 'THREAD_REPLY_RECEIVED', parentId: 'p1', message: { id: 'm1', content: 'hi' } }
  )

  it('REPLY_EDITED applied twice returns the identical state the second time', () => {
    const edit = { type: 'REPLY_EDITED', messageId: 'm1', content: 'edited', editedAt: '2026-08-22T10:00:00Z' }
    const once = threadEventsReducer(opened, edit)
    expect(once).not.toBe(opened)
    expect(threadEventsReducer(once, edit)).toBe(once)
  })

  it('REPLY_DELETED applied twice returns the identical state the second time', () => {
    const del = { type: 'REPLY_DELETED', messageId: 'm1' }
    const once = threadEventsReducer(opened, del)
    expect(once).not.toBe(opened)
    expect(threadEventsReducer(once, del)).toBe(once)
  })
})

// The view lane may render a placeholder when the room key has not arrived while
// the per-subscriber lane carries the real body under the same id. Deduping on
// id alone lets whichever lands first win.
describe('threadEventsReducer placeholder reconciliation', () => {
  const opened = threadEventsReducer(initialState, {
    type: 'OPEN_THREAD',
    parent: { roomId: 'r1', siteId: 's1', messageId: 'p1', createdAtMs: 1000 },
  })
  const placeholder = { id: 'm1', content: '[encrypted message]', encrypted: true }
  const real = { id: 'm1', content: 'the real body' }
  const recv = (message) => ({ type: 'THREAD_REPLY_RECEIVED', parentId: 'p1', message })

  it('replaces a placeholder with the real body', () => {
    const withPlaceholder = threadEventsReducer(opened, recv(placeholder))
    const resolved = threadEventsReducer(withPlaceholder, recv(real))
    expect(resolved.messages).toHaveLength(1)
    expect(resolved.messages[0].content).toBe('the real body')
    expect(resolved.messages[0].encrypted).toBeUndefined()
  })

  it('does not let a placeholder overwrite a body already rendered', () => {
    const withReal = threadEventsReducer(opened, recv(real))
    const after = threadEventsReducer(withReal, recv(placeholder))
    expect(after).toBe(withReal)
  })

  it('still drops a plain duplicate', () => {
    const once = threadEventsReducer(opened, recv(real))
    expect(threadEventsReducer(once, recv(real))).toBe(once)
  })
})

// Go marshals time.Time as RFC3339Nano with trailing zeros stripped, so the wire
// carries both "...00Z" and "...00.5Z". Lexicographically "." (0x2E) sorts before
// "Z" (0x5A), so a string compare calls the sub-second edit older and drops it.
describe('threadEventsReducer edit ordering across timestamp precisions', () => {
  const opened = threadEventsReducer(
    threadEventsReducer(initialState, {
      type: 'OPEN_THREAD',
      parent: { roomId: 'r1', siteId: 's1', messageId: 'p1', createdAtMs: 1000 },
    }),
    { type: 'THREAD_REPLY_RECEIVED', parentId: 'p1', message: { id: 'm1', content: 'v0' } }
  )
  const edit = (content, editedAt) => ({ type: 'REPLY_EDITED', messageId: 'm1', content, editedAt })

  it('applies a sub-second edit that follows a whole-second one', () => {
    const first = threadEventsReducer(opened, edit('v1', '2026-08-22T10:00:00Z'))
    const second = threadEventsReducer(first, edit('v2', '2026-08-22T10:00:00.5Z'))
    expect(second.messages[0].content).toBe('v2')
  })

  it('still rejects a whole-second edit that precedes a sub-second one', () => {
    const first = threadEventsReducer(opened, edit('v2', '2026-08-22T10:00:00.5Z'))
    const stale = threadEventsReducer(first, edit('v1', '2026-08-22T10:00:00Z'))
    expect(stale).toBe(first)
  })
})

// A placeholder can be mutated while it stands in for a body the key had not
// arrived for. Replacing it wholesale with the real body loses those mutations.
describe('threadEventsReducer placeholder replacement preserves mutations', () => {
  const opened = threadEventsReducer(initialState, {
    type: 'OPEN_THREAD',
    parent: { roomId: 'r1', siteId: 's1', messageId: 'p1', createdAtMs: 1000 },
  })
  const placeholder = { id: 'm1', content: '[encrypted message]', encrypted: true }
  const real = { id: 'm1', content: 'the real body' }
  const recv = (message) => ({ type: 'THREAD_REPLY_RECEIVED', parentId: 'p1', message })

  it('keeps a delete applied while the placeholder stood', () => {
    let st = threadEventsReducer(opened, recv(placeholder))
    st = threadEventsReducer(st, { type: 'REPLY_DELETED', messageId: 'm1' })
    st = threadEventsReducer(st, recv(real))
    expect(st.messages[0].deleted).toBe(true)
  })

  it('keeps an edit applied while the placeholder stood', () => {
    let st = threadEventsReducer(opened, recv(placeholder))
    st = threadEventsReducer(st, {
      type: 'REPLY_EDITED', messageId: 'm1', content: 'edited body', editedAt: '2026-08-22T10:00:00Z',
    })
    st = threadEventsReducer(st, recv(real))
    expect(st.messages[0].content).toBe('edited body')
    expect(st.messages[0].editedAt).toBe('2026-08-22T10:00:00Z')
  })

  it('takes the real body when nothing was applied to the placeholder', () => {
    const st = threadEventsReducer(threadEventsReducer(opened, recv(placeholder)), recv(real))
    expect(st.messages[0].content).toBe('the real body')
    expect(st.messages[0].encrypted).toBeUndefined()
  })
})
