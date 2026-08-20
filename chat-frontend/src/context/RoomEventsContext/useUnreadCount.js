import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
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

/** How often to check the fold against the server. Deliberately NOT gated on
 *  visibility: a taskbar badge is looked at while the app is in the
 *  background, so a focus-only check would run exactly when it stops
 *  mattering. */
export const RECONCILE_INTERVAL_MS = 5 * 60 * 1000

/** Consecutive disagreements before a resync. One is expected noise: between a
 *  message landing in the visibly-active room and its trailing markRoomRead
 *  committing, the client suppresses that room and the server still counts it.
 *  Two in a row, minutes apart, is real drift. */
const MISMATCHES_BEFORE_RESYNC = 2

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
 * Drift is possible — core NATS is at-most-once, and a thread read on another
 * device produces no client event at all — so `reconcile` provides a backstop:
 * ask the server periodically, and on repeated disagreement re-pull the
 * authoritative state. The server corrects the STATE, never the number, so the
 * badge keeps exactly one source.
 *
 * @param {object} state RoomEventsContext state
 * @param {{ getCount: () => Promise<{count: number}>, resync: () => Promise<unknown> }} [reconcile]
 *   omit to fold with no backstop (tests, and any caller without a connection)
 * @returns {number} count of unread rooms
 */
export function useUnreadCount(state, reconcile) {
  const visible = useDocumentVisible()
  const count = useMemo(
    () => selectUnreadRoomIds(state, visible).length,
    [state, visible],
  )

  // Read inside the interval callback rather than captured, so the timer does
  // not have to be torn down and rebuilt on every count change.
  const countRef = useRef(count)
  countRef.current = count
  const reconcileRef = useRef(reconcile)
  reconcileRef.current = reconcile
  const mismatchesRef = useRef(0)
  const inFlightRef = useRef(false)

  const enabled = Boolean(reconcile?.getCount)
  const check = useCallback(() => {
    const r = reconcileRef.current
    if (!r?.getCount) return
    // A single foregrounding fires visibilitychange AND focus; without this
    // guard that one transient scores two mismatches and instantly spends the
    // whole tolerance. Also keeps a slow RPC from overlapping the next tick.
    if (inFlightRef.current) return
    inFlightRef.current = true
    r.getCount()
      .then((resp) => {
        if (!reconcileRef.current) return
        if ((resp?.count ?? 0) === countRef.current) {
          mismatchesRef.current = 0
          return
        }
        mismatchesRef.current += 1
        if (mismatchesRef.current < MISMATCHES_BEFORE_RESYNC) return
        mismatchesRef.current = 0
        r.resync?.()
      })
      // Keep the folded value and try again next tick. Zeroing the badge on a
      // transient RPC failure would read as "all caught up".
      .catch(() => {})
      .finally(() => { inFlightRef.current = false })
  }, [])

  useEffect(() => {
    if (!enabled) return undefined
    // No check on mount: the bucket bootstrap IS an authoritative pull, so
    // reconciling against it immediately would only duplicate it.
    const timer = setInterval(check, RECONCILE_INTERVAL_MS)
    const onVisibilityChange = () => {
      if (document.visibilityState !== 'hidden') check()
    }
    document.addEventListener('visibilitychange', onVisibilityChange)
    window.addEventListener('focus', check)
    return () => {
      clearInterval(timer)
      document.removeEventListener('visibilitychange', onVisibilityChange)
      window.removeEventListener('focus', check)
    }
  }, [enabled, check])

  return count
}
