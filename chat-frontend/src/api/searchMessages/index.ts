import { searchMessages as searchMessagesSubject } from '../_transport/subjects'
import type { Nats } from '../types'

export interface SearchMessagesArgs {
  searchText: string
  /** Limit hits to these rooms; empty/omitted searches everywhere. */
  roomIds?: string[]
  size: number
}

/** Mirrors pkg/model.SearchMessage (the search.messages reply projection). */
export interface SearchMessageHit {
  messageId: string
  roomId: string
  siteId: string
  userAccount: string
  content: string
  createdAt: string
  editedAt?: string
  updatedAt?: string
  threadParentMessageId?: string
  threadParentMessageCreatedAt?: string
}

export interface SearchMessagesResponse {
  messages: SearchMessageHit[]
  total: number
}

/** Full-text search across messages. Optionally scope to a room subset. */
export async function searchMessages(
  { user, request }: Nats,
  { searchText, roomIds, size }: SearchMessagesArgs,
): Promise<SearchMessagesResponse> {
  const payload: { query: string; roomIds?: string[]; size: number } = {
    query: searchText,
    size,
  }
  if (roomIds) payload.roomIds = roomIds
  return request<SearchMessagesResponse>(searchMessagesSubject(user.account), payload)
}
