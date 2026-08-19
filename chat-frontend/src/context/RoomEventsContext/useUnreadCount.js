import { useEffect, useMemo, useState } from 'react'
import { selectUnreadRoomIds } from './selectUnread'

/** Track whether the window is in front of the user. The badge counts the
 *  active room while hidden (nobody is reading it) and suppresses it while
 *  visible — see selectUnreadRoomIds. */
function useDocumentVisible() {
  const [visible, setVisible] = useState(() => document.visibilityState !== 'hidden')
  useEffect(() => {
    const onChange = () => setVisible(document.visibilityState !== 'hidden')
    document.addEventListener('visibilitychange', onChange)
    return () => document.removeEventListener('visibilitychange', onChange)
  }, [])
  return visible
}

/**
 * App-wide unread total, folded from subscription state the client already
 * holds — no RPC. Every input (`lastMsgAt`, `lastSeenAt`, `muted`,
 * `threadUnread`) arrives on the bucket bootstrap and is kept current by the
 * live event stream, so the count moves at event speed rather than at
 * request/reply speed.
 *
 * Uncapped: the server's `subscription.count` is uncapped too (BADGE_COUNT_CAP
 * applies to the push path). UnreadBadge owns its own render cap.
 *
 * @param {object} state RoomEventsContext state
 * @returns {number} count of unread rooms
 */
export function useUnreadCount(state) {
  const visible = useDocumentVisible()
  return useMemo(
    () => selectUnreadRoomIds(state, visible).length,
    [state, visible],
  )
}
