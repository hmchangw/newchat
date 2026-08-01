// Move a chat into a custom chatlist section (or remove it). Room-scoped
// synchronous RPC; membership + order live on the subscription, mirroring
// favorite.toggle. Emits subscription.update (action "section_moved").
//
// Guards on CHATLIST_MOCK: mock → in-memory store; live → the real subject.

import { CHATLIST_MOCK } from '@/lib/runtimeConfig'
import { chatMove } from '../_transport/subjects'
import { moveChatMock } from '../_transport/chatlistMock'
import type { Nats } from '../types'

export interface MoveChatArgs {
  roomId: string
  /** Room's origin site (like the other room-scoped RPCs). */
  siteId: string
  /** Custom section to move into; `null` removes it (falls back to a derived
   *  built-in). A built-in id is rejected by the server. */
  sectionId: string | null
  /** Optional: place just after this room in the section; omit to append. */
  afterRoomId?: string
}

/** Mirrors the chat.move reply. `sectionId`/`sectionOrder` omitted on remove. */
export interface MoveChatResponse {
  status: 'ok'
  sectionId?: string
  sectionOrder?: number
}

export function moveChat(
  { user, request }: Nats,
  { roomId, siteId, sectionId, afterRoomId }: MoveChatArgs,
): Promise<MoveChatResponse> {
  if (CHATLIST_MOCK) return Promise.resolve(moveChatMock(roomId, sectionId, afterRoomId))
  return request<MoveChatResponse>(chatMove(user.account, roomId, siteId), { sectionId, afterRoomId })
}
