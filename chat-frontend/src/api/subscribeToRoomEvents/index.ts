import { roomEvent } from '../_transport/subjects'
import type { Nats, NatsSubscription, SubscriptionCallback } from '../types'

/** Subscribe to a single channel's event stream. `crossSite` selects the
 *  global (`chat.room.…`) vs local (`chat.local.room.…`) namespace —
 *  callers must resolve it from the room object, defaulting to `true`
 *  (global) when the flag is missing. */
export function subscribeToRoomEvents(
  { subscribe }: Pick<Nats, 'subscribe'>,
  { roomId, crossSite }: { roomId: string; crossSite: boolean },
  callback: SubscriptionCallback,
): NatsSubscription {
  return subscribe(roomEvent(roomId, crossSite), callback)
}
