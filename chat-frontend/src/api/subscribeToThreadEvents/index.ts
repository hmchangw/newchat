import { roomThreadEvent } from '../_transport/subjects'
import type { Nats, NatsSubscription, SubscriptionCallback } from '../types'

/** Subscribe to one thread's event stream for as long as its panel is open.
 *  `crossSite` selects the global (`chat.room.…`) vs local
 *  (`chat.local.room.…`) namespace, resolved from the room the same way
 *  `subscribeToRoomEvents` resolves it. */
export function subscribeToThreadEvents(
  { subscribe }: Pick<Nats, 'subscribe'>,
  { roomId, parentMessageId, crossSite }: { roomId: string; parentMessageId: string; crossSite: boolean },
  callback: SubscriptionCallback,
): NatsSubscription {
  return subscribe(roomThreadEvent(roomId, parentMessageId, crossSite), callback)
}
