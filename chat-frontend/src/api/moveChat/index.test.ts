import { describe, it, expect, vi } from 'vitest'
import { moveChat } from './index'
import type { Nats } from '../types'

function fakeNats(request: unknown) {
  return { user: { account: 'alice', siteId: 'site-A' }, request } as unknown as Nats
}

describe('moveChat (real path)', () => {
  it('requests the room-scoped chat.move subject with sectionId + afterRoomId', async () => {
    const request = vi.fn().mockResolvedValue({ status: 'ok', sectionId: 's1', sectionOrder: 3.5 })
    await moveChat(fakeNats(request), {
      roomId: 'r1',
      siteId: 'site-B',
      sectionId: 's1',
      afterRoomId: 'r0',
    })
    expect(request).toHaveBeenCalledWith('chat.user.alice.request.room.r1.site-B.chat.move', {
      sectionId: 's1',
      afterRoomId: 'r0',
    })
  })

  it('sends sectionId null to remove from section', async () => {
    const request = vi.fn().mockResolvedValue({ status: 'ok' })
    await moveChat(fakeNats(request), { roomId: 'r1', siteId: 'site-A', sectionId: null })
    expect(request).toHaveBeenCalledWith('chat.user.alice.request.room.r1.site-A.chat.move', {
      sectionId: null,
      afterRoomId: undefined,
    })
  })
})
