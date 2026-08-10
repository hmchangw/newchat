// Subscribe to chatlist.update events — the caller-fanned section-definition
// sync. Payload is a ChatlistUpdateEvent (full-state replace, LWW by
// chatlist.lastUpdatedAt). Guards on CHATLIST_MOCK: mock → the in-memory
// emitter; live → the real subject.

import { CHATLIST_MOCK } from '@/lib/runtimeConfig'
import { chatlistUpdate } from '../_transport/subjects'
import { onChatlistUpdateMock } from '../_transport/chatlistMock'
import type { Nats, NatsSubscription, ChatlistUpdateEvent } from '../types'

export type ChatlistUpdateCallback = (event: ChatlistUpdateEvent) => void

export function subscribeToChatlistUpdates(
  { user, subscribe }: Nats,
  callback: ChatlistUpdateCallback,
): NatsSubscription {
  if (CHATLIST_MOCK) {
    const unsub = onChatlistUpdateMock(callback)
    return { unsubscribe: unsub }
  }
  return subscribe(chatlistUpdate(user.account), callback as (event: unknown) => void)
}
