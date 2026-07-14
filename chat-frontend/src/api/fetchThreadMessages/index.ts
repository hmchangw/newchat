import { msgThread } from '../_transport/subjects'
import { normalizeHistoricalMessages } from '../_transport/normalizeMessage'
import type { Nats, Message, HistoryMessage } from '../types'

export interface FetchThreadMessagesArgs {
  roomId: string
  siteId: string
  /** Parent message id — the root of the reply chain. */
  threadMessageId: string
  limit?: number
}

export interface FetchThreadMessagesResponse {
  /** Normalised broadcast shape. */
  messages: Message[]
  nextCursor?: string
  hasNext: boolean
  /**
   * UTC milliseconds since Unix epoch. Present only when every thread
   * subscriber has read — mirrors LoadHistoryResponse.minUserLastSeenAt.
   */
  minUserLastSeenAt?: number
}

interface WireResponse {
  messages?: HistoryMessage[]
  nextCursor?: string
  hasNext?: boolean
  minUserLastSeenAt?: number
}

/**
 * Load the reply chain for a thread (parent + replies).
 * Normalises cassandra shape into broadcast shape.
 */
export async function fetchThreadMessages(
  { user, request }: Nats,
  args: FetchThreadMessagesArgs,
): Promise<FetchThreadMessagesResponse> {
  const { roomId, siteId, threadMessageId, limit = 50 } = args
  const resp = await request<WireResponse>(
    msgThread(user.account, roomId, siteId),
    { threadMessageId, limit },
  )
  // history-service returns thread messages newest-first (DESC); the thread
  // panel renders ASC (oldest on top). Mirrors `fetchMessageHistory`'s reverse.
  const asc = [...(resp.messages ?? [])].reverse()
  return {
    messages: normalizeHistoricalMessages(asc),
    nextCursor: resp.nextCursor,
    hasNext: resp.hasNext ?? false,
    minUserLastSeenAt: resp.minUserLastSeenAt,
  }
}
