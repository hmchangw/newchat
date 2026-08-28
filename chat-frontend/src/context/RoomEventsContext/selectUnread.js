/**
 * Which rooms count toward the unread badge.
 *
 * Mirrors user-service's `unreadRooms()` (the `subscription.count` rule) so the
 * locally folded number and the server's agree: a room counts ONCE when it is
 * not muted and either its newest message is past the user's read position or
 * it carries an unread followed thread.
 *
 * The one deliberate deviation is `activeRoomId` — the server has no concept of
 * an open window. The two converge because opening a room marks it read; see
 * the design note in docs/superpowers/specs/2026-08-19-client-side-unread-badge-design.md.
 */

/** A room event at `at` is unread relative to `lastSeenAt`. Never read (no
 *  lastSeenAt) counts as unread as long as the room has content at all. */
function isAfter(at, lastSeenAt) {
  if (!at) return false
  if (!lastSeenAt) return true
  return Date.parse(at) > Date.parse(lastSeenAt)
}

/**
 * The ONE per-room unread rule, shared by the badge fold below and the
 * sidebar row's bold state (stamped as `room.hasUnread` by
 * useSidebarSections): not muted, and either the room's newest user message
 * is past the member's read position or it carries an unread followed
 * thread. `reading` suppresses the MAIN FEED only — sitting in a room does
 * not read its threads.
 *
 * @param {object} room summary carrying `lastMsgAt` (user-activity semantics)
 * @param {object|undefined} sub the member's subscription record, if any
 * @param {boolean} reading whether the member is looking at this room right now
 */
export function isRoomUnread(room, sub, reading) {
  if (sub?.muted) return false
  const unreadByMessage = !reading && isAfter(room.lastMsgAt, sub?.lastSeenAt)
  const unreadByThread = (sub?.threadUnread?.length ?? 0) > 0
  return unreadByMessage || unreadByThread
}

/**
 * @param {object} state RoomEventsContext state (summaries + subscriptions + activeRoomId)
 * @param {boolean} isVisible whether the window is in front of the user — when
 *   false there is no room being read, so the active room is NOT suppressed
 * @returns {string[]} unread room IDs, in `summaries` order
 */
export function selectUnreadRoomIds(state, isVisible) {
  const ids = []
  for (const room of state.summaries) {
    const reading = isVisible && room.id === state.activeRoomId
    if (isRoomUnread(room, state.subscriptions[room.id], reading)) ids.push(room.id)
  }
  return ids
}
