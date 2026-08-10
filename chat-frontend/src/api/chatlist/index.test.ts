import { describe, it, expect, vi } from 'vitest'
import {
  getChatlist,
  createChatlistSection,
  renameChatlistSection,
  deleteChatlistSection,
  reorderChatlistSections,
  setChatlistSectionSortMode,
} from './index'
import type { Nats } from '../types'

// CHATLIST_MOCK is false by default in the test env, so these exercise the
// real NATS-subject path — the op→subject+payload mapping.
function fakeNats(request: unknown) {
  return { user: { account: 'alice', siteId: 'site-A' }, request } as unknown as Nats
}

describe('chatlist api ops (real path)', () => {
  it('getChatlist requests the chatlist.get subject with no body', async () => {
    const request = vi.fn().mockResolvedValue({ sectionOrder: [], sections: [], lastUpdatedAt: 0 })
    await getChatlist(fakeNats(request))
    expect(request).toHaveBeenCalledWith('chat.user.alice.request.user.site-A.chatlist.get')
  })

  it('createChatlistSection maps name + sortMode', async () => {
    const request = vi.fn().mockResolvedValue({})
    await createChatlistSection(fakeNats(request), { name: 'Work', sortMode: 'custom' })
    expect(request).toHaveBeenCalledWith(
      'chat.user.alice.request.user.site-A.chatlist.section.create',
      { name: 'Work', sortMode: 'custom' },
    )
  })

  it('renameChatlistSection maps sectionId + name', async () => {
    const request = vi.fn().mockResolvedValue({})
    await renameChatlistSection(fakeNats(request), { sectionId: 's1', name: 'Renamed' })
    expect(request).toHaveBeenCalledWith(
      'chat.user.alice.request.user.site-A.chatlist.section.rename',
      { sectionId: 's1', name: 'Renamed' },
    )
  })

  it('deleteChatlistSection maps sectionId', async () => {
    const request = vi.fn().mockResolvedValue({})
    await deleteChatlistSection(fakeNats(request), 's1')
    expect(request).toHaveBeenCalledWith(
      'chat.user.alice.request.user.site-A.chatlist.section.delete',
      { sectionId: 's1' },
    )
  })

  it('reorderChatlistSections maps the full order permutation', async () => {
    const request = vi.fn().mockResolvedValue({})
    await reorderChatlistSections(fakeNats(request), ['favorites', 'work', 'chats'])
    expect(request).toHaveBeenCalledWith(
      'chat.user.alice.request.user.site-A.chatlist.section.reorder',
      { sectionOrder: ['favorites', 'work', 'chats'] },
    )
  })

  it('setChatlistSectionSortMode maps sectionId + sortMode', async () => {
    const request = vi.fn().mockResolvedValue({})
    await setChatlistSectionSortMode(fakeNats(request), { sectionId: 's1', sortMode: 'mostRecent' })
    expect(request).toHaveBeenCalledWith(
      'chat.user.alice.request.user.site-A.chatlist.section.setsortmode',
      { sectionId: 's1', sortMode: 'mostRecent' },
    )
  })
})
