import { appendBounded, mergeById, MAX_CACHED } from '@/lib/messageBuffer'

export { MAX_CACHED }

export const BUFFER_MODE = {
  LIVE: 'live',
  HISTORICAL: 'historical',
}

export const initialState = {
  summaries: [],
  roomState: {},
  activeRoomId: null,
  roomsError: null,
  favoriteIds: new Set(),
  appIds: new Set(),
  channelDmIds: new Set(),
  /**
   * Per-roomId map of the FULL model.Subscription record for every room
   * the current user is subscribed to (sourced from the three user-service
   * bucket RPCs + the live `subscription.update` event stream).
   *
   * Components read their per-room subscription via `useSubscription(roomId)`
   * — gives them roles, alert, lastSeenAt, hasMention, hrInfo, etc. without
   * a per-component fetch. The sidebar enrichment in `useSidebarSections`
   * also pulls `name` + `hrInfo` from here.
   *
   * Shape: { [roomId]: Subscription } where Subscription mirrors
   * pkg/model.Subscription (see chat-frontend/src/api/types.ts).
   */
  subscriptions: {},
}

/**
 * Aggregate the per-room unread state into a single header badge value.
 *
 * Pure derivation over `state.summaries` — the reducer already keeps
 * `unreadCount` / `hasMention` per room current (incremented by
 * `MESSAGE_RECEIVED` for non-active rooms, zeroed by `SET_ACTIVE_ROOM`),
 * so the badge stays live without any extra fetch or subscription.
 */
export function selectUnreadTotal(summaries) {
  let total = 0
  let hasMention = false
  for (const s of summaries) {
    total += s.unreadCount ?? 0
    if (s.hasMention) hasMention = true
  }
  return { total, hasMention }
}

function sortByLastMsgDesc(summaries) {
  return [...summaries].sort((a, b) => {
    const at = a.lastMsgAt ? new Date(a.lastMsgAt).getTime() : 0
    const bt = b.lastMsgAt ? new Date(b.lastMsgAt).getTime() : 0
    return bt - at
  })
}

function toSummary(room) {
  return {
    id: room.id,
    name: room.name,
    // Per-user friendly name (DM display fallback). RoomEventsContext sets
    // this from the inbound subscription.update event; rooms loaded via the
    // initial rooms.list don't carry it today (server returns Room, not
    // Subscription), so it'll be undefined on first paint — roomDisplayName
    // falls back to a placeholder until subscription.update lands.
    subscriptionName: room.subscriptionName,
    type: room.type,
    siteId: room.siteId,
    userCount: room.userCount,
    lastMsgAt: room.lastMsgAt ?? null,
    unreadCount: 0,
    hasMention: false,
    mentionAll: false,
  }
}

/**
 * Apply the server's per-user subscription record onto a summary.
 *
 * Three call sites need this exact merge:
 *   - `ROOM_ADDED` (when a subscription arrived ahead of the async
 *     getRoom() that triggered the dispatch)
 *   - `BUCKETS_LOADED` (cold-start: every summary already exists, we
 *     fold in the freshly-fetched subscription records)
 *   - `SUBSCRIPTION_UPSERTED` (live delta: server says "this changed")
 *
 * **Field presence is honored.** Each field is only touched on the
 * summary if it's actually present in the subscription payload. This
 * matters because `SUBSCRIPTION_UPSERTED` events can carry partial
 * deltas (e.g. role-update only emits roles + the constants); we
 * must not clobber the summary's other fields with `undefined`.
 *
 * Semantics for `hasMention`: server-canonical when present. If the
 * sub has `hasMention: false`, we clear; if `true`, we set; if the
 * field is absent (partial event), we leave the summary's value
 * alone. Live mentions via `MESSAGE_RECEIVED` re-OR `hasMention` back
 * to true on the next event regardless.
 */
function mergeSubscriptionIntoSummary(summary, sub) {
  if (!sub) return summary
  const next = { ...summary }
  if ('hasMention' in sub) next.hasMention = !!sub.hasMention
  if (sub.name) next.subscriptionName = sub.name
  return next
}

function emptyRoomState() {
  return {
    messages: [],
    hasLoadedHistory: false,
    historyError: null,
    unreadCount: 0,
    hasMention: false,
    mentionAll: false,
    lastMsgAt: null,
    lastMsgId: null,
    bufferMode: BUFFER_MODE.LIVE,
    pendingLiveMessages: [],
    focusMessageId: null,
  }
}

export function roomEventsReducer(state, action) {
  switch (action.type) {
    case 'ROOMS_LOADED': {
      const summaries = sortByLastMsgDesc(action.rooms.map(toSummary))
      return { ...state, summaries, roomsError: null }
    }
    case 'ROOM_ADDED': {
      if (state.summaries.some((r) => r.id === action.room.id)) return state
      const roomId = action.room.id
      // A SUBSCRIPTION_UPSERTED commonly fires BEFORE this ROOM_ADDED
      // (useRoomSubscriptions dispatches the subscription synchronously
      // and awaits getRoom() before ROOM_ADDED). Merge the existing
      // subscription record into the new summary so hasMention /
      // subscriptionName survive — otherwise toSummary's zero-init
      // would clobber them and the badge / DM name would silently drop.
      const summary = mergeSubscriptionIntoSummary(
        toSummary(action.room),
        state.subscriptions[roomId],
      )
      const summaries = sortByLastMsgDesc([...state.summaries, summary])
      let appIds = state.appIds
      let channelDmIds = state.channelDmIds
      if (action.room.type === 'botDM') {
        if (!appIds.has(roomId)) {
          appIds = new Set(appIds)
          appIds.add(roomId)
        }
      } else if (!channelDmIds.has(roomId)) {
        channelDmIds = new Set(channelDmIds)
        channelDmIds.add(roomId)
      }
      return { ...state, summaries, appIds, channelDmIds }
    }
    case 'ROOM_REMOVED': {
      const summaries = state.summaries.filter((r) => r.id !== action.roomId)
      const { [action.roomId]: _removed, ...rest } = state.roomState
      let favoriteIds = state.favoriteIds
      let appIds = state.appIds
      let channelDmIds = state.channelDmIds
      let subscriptions = state.subscriptions
      if (favoriteIds.has(action.roomId)) {
        favoriteIds = new Set(favoriteIds)
        favoriteIds.delete(action.roomId)
      }
      if (appIds.has(action.roomId)) {
        appIds = new Set(appIds)
        appIds.delete(action.roomId)
      }
      if (channelDmIds.has(action.roomId)) {
        channelDmIds = new Set(channelDmIds)
        channelDmIds.delete(action.roomId)
      }
      if (subscriptions[action.roomId]) {
        const { [action.roomId]: _drop, ...restSubs } = subscriptions
        subscriptions = restSubs
      }
      return {
        ...state,
        summaries,
        roomState: rest,
        favoriteIds,
        appIds,
        channelDmIds,
        subscriptions,
      }
    }
    case 'BUCKETS_LOADED': {
      // Seed every summary from the freshly-fetched subscription
      // records — server-canonical hasMention / subscriptionName for
      // the cold-start path. See `mergeSubscriptionIntoSummary`.
      const subs = action.subscriptions ?? {}
      const summaries = state.summaries.map((s) =>
        subs[s.id] ? mergeSubscriptionIntoSummary(s, subs[s.id]) : s
      )
      return {
        ...state,
        summaries,
        favoriteIds: new Set(action.favoriteIds),
        appIds: new Set(action.appIds),
        channelDmIds: new Set(action.channelDmIds),
        subscriptions: subs,
      }
    }
    case 'SUBSCRIPTION_UPSERTED': {
      // Upsert a single subscription record (live delta from
      // `subscription.update` events). Spreads the new fields on top
      // of the prior record so a partial event (role-update only
      // carrying `roles`, mark-read only carrying `hasMention`, …)
      // doesn't lose lastSeenAt / alert / hrInfo / etc. The full
      // sender-of-truth is what room-worker emits; if it sends the
      // full record we just replace, if partial we merge.
      const sub = action.subscription
      if (!sub?.roomId) return state
      const prevSub = state.subscriptions[sub.roomId]
      const merged = prevSub ? { ...prevSub, ...sub } : sub
      const subscriptions = { ...state.subscriptions, [sub.roomId]: merged }
      const summaries = state.summaries.map((s) =>
        // Pass the INCOMING delta (not the merged record) so summary
        // updates only touch fields the event actually carried — see
        // `mergeSubscriptionIntoSummary`'s presence-aware writes.
        s.id === sub.roomId ? mergeSubscriptionIntoSummary(s, sub) : s
      )
      return { ...state, summaries, subscriptions }
    }
    case 'ROOM_METADATA_UPDATED': {
      const existing = state.summaries.find((r) => r.id === action.roomId)
      if (!existing) return state
      if (
        existing.name === action.name &&
        existing.userCount === action.userCount &&
        existing.lastMsgAt === action.lastMsgAt
      ) {
        return state
      }
      const summaries = sortByLastMsgDesc(
        state.summaries.map((r) =>
          r.id === action.roomId
            ? { ...r, name: action.name, userCount: action.userCount, lastMsgAt: action.lastMsgAt }
            : r
        )
      )
      return { ...state, summaries }
    }
    case 'MESSAGE_RECEIVED': {
      const evt = action.event
      // Normalize the message payload across the two possible broadcast-worker
      // modes: plaintext (evt.message populated) and encrypted-only (only
      // evt.encryptedMessage populated; .message is dropped via Go's
      // json:omitempty). Until client-side crypto lands we can't decrypt,
      // but silently swallowing the event leaves the room visually frozen —
      // synthesize a "[encrypted message]" placeholder from the top-level
      // lastMsgId/lastMsgAt instead so the user sees something happened.
      // The `encrypted: true` marker lets the UI render it differently if
      // it wants to (italics, lock icon, etc.); the default message renderer
      // just shows the placeholder text.
      let msg = evt.message
      if ((!msg || !msg.id) && evt.encryptedMessage) {
        if (!evt.lastMsgId) return state
        msg = {
          id: evt.lastMsgId,
          roomId: evt.roomId,
          content: '[encrypted message]',
          createdAt: evt.lastMsgAt ?? new Date(evt.timestamp ?? Date.now()).toISOString(),
          encrypted: true,
        }
      }
      if (!msg || !msg.id) return state
      // Thread replies are written to thread tables only, but broadcast-worker
      // publishes them on the main subject too. Filter them here so they don't
      // flicker into the main feed.
      if (msg.threadParentMessageId) {
        return state
      }
      const roomId = evt.roomId
      const prev = state.roomState[roomId] ?? emptyRoomState()
      const isActive = state.activeRoomId === roomId
      if (prev.bufferMode === BUFFER_MODE.HISTORICAL) {
        if (
          prev.messages.some((m) => m.id === msg.id) ||
          prev.pendingLiveMessages.some((m) => m.id === msg.id)
        ) {
          return state
        }
        const pendingLiveMessages = [...prev.pendingLiveMessages, msg]
        const nextRoomState = {
          ...prev,
          pendingLiveMessages,
          lastMsgAt: evt.lastMsgAt ?? msg.createdAt ?? prev.lastMsgAt,
          lastMsgId: evt.lastMsgId ?? prev.lastMsgId,
          unreadCount: isActive ? prev.unreadCount : prev.unreadCount + 1,
          hasMention: isActive ? false : prev.hasMention || !!evt.hasMention,
          mentionAll: isActive ? false : prev.mentionAll || !!evt.mentionAll,
        }
        const summaries = state.summaries.some((r) => r.id === roomId)
          ? sortByLastMsgDesc(
              state.summaries.map((r) =>
                r.id === roomId
                  ? {
                      ...r,
                      lastMsgAt: nextRoomState.lastMsgAt ?? r.lastMsgAt,
                      unreadCount: nextRoomState.unreadCount,
                      // OR with the summary's existing mention so a
                      // BUCKETS_LOADED / SUBSCRIPTION_UPSERTED seed
                      // isn't clobbered by a subsequent non-mention
                      // message. Active-room clears unconditionally.
                      hasMention: isActive ? false : (r.hasMention || nextRoomState.hasMention),
                      mentionAll: isActive ? false : (r.mentionAll || nextRoomState.mentionAll),
                    }
                  : r
              )
            )
          : state.summaries
        return {
          ...state,
          summaries,
          roomState: { ...state.roomState, [roomId]: nextRoomState },
        }
      }
      if (prev.messages.some((m) => m.id === msg.id)) return state
      const messages = appendBounded(prev.messages, msg)
      const nextRoomState = {
        ...prev,
        messages,
        lastMsgAt: evt.lastMsgAt ?? msg.createdAt ?? prev.lastMsgAt,
        lastMsgId: evt.lastMsgId ?? prev.lastMsgId,
        unreadCount: isActive ? prev.unreadCount : prev.unreadCount + 1,
        hasMention: isActive ? false : prev.hasMention || !!evt.hasMention,
        mentionAll: isActive ? false : prev.mentionAll || !!evt.mentionAll,
      }
      const summaries = state.summaries.some((r) => r.id === roomId)
        ? sortByLastMsgDesc(
            state.summaries.map((r) =>
              r.id === roomId
                ? {
                    ...r,
                    lastMsgAt: nextRoomState.lastMsgAt ?? r.lastMsgAt,
                    unreadCount: nextRoomState.unreadCount,
                    // See historical-mode branch above for the OR rationale.
                    hasMention: isActive ? false : (r.hasMention || nextRoomState.hasMention),
                    mentionAll: isActive ? false : (r.mentionAll || nextRoomState.mentionAll),
                  }
                : r
            )
          )
        : state.summaries
      return {
        ...state,
        summaries,
        roomState: { ...state.roomState, [roomId]: nextRoomState },
      }
    }
    case 'HISTORY_LOADED': {
      const prev = state.roomState[action.roomId] ?? emptyRoomState()
      const merged = mergeById(prev.messages, action.messages)
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: {
            ...prev,
            messages: merged,
            hasLoadedHistory: true,
            historyError: null,
          },
        },
      }
    }
    case 'HISTORY_FAILED': {
      const prev = state.roomState[action.roomId] ?? emptyRoomState()
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: { ...prev, historyError: action.error },
        },
      }
    }
    case 'REPLACE_ROOM_BUFFER': {
      const prev = state.roomState[action.roomId] ?? emptyRoomState()
      const messages = action.messages ?? []
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: {
            ...prev,
            messages,
            hasLoadedHistory: true,
            historyError: null,
            bufferMode: BUFFER_MODE.HISTORICAL,
            focusMessageId: action.focusMessageId ?? null,
            pendingLiveMessages: [],
          },
        },
      }
    }
    case 'FOCUS_CLEARED': {
      // Drop the focusMessageId after MessageList has consumed it for the
      // scroll-into-view + flash-jump animation. Without this, switching
      // rooms and back replays the flash, AND clicking the same quoted
      // message twice no-ops (the focusMessageId effect deps don't change
      // between the two clicks).
      const prev = state.roomState[action.roomId]
      if (!prev || prev.focusMessageId == null) return state
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: { ...prev, focusMessageId: null },
        },
      }
    }
    case 'RESET_TO_LIVE_TAIL': {
      const prev = state.roomState[action.roomId]
      if (!prev) {
        return {
          ...state,
          roomState: {
            ...state.roomState,
            [action.roomId]: emptyRoomState(),
          },
        }
      }
      const existingIds = new Set(prev.messages.map((m) => m.id))
      const newPending = (prev.pendingLiveMessages ?? []).filter(
        (m) => !existingIds.has(m.id)
      )
      const merged = [...prev.messages, ...newPending]
      const bounded =
        merged.length > MAX_CACHED ? merged.slice(merged.length - MAX_CACHED) : merged
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: {
            ...prev,
            messages: bounded,
            pendingLiveMessages: [],
            focusMessageId: null,
            bufferMode: BUFFER_MODE.LIVE,
          },
        },
      }
    }
    case 'SET_ACTIVE_ROOM': {
      const roomId = action.roomId
      if (roomId === state.activeRoomId) return state
      if (roomId === null) {
        return { ...state, activeRoomId: null }
      }
      const prev = state.roomState[roomId] ?? emptyRoomState()
      const nextRoomState = { ...prev, unreadCount: 0, hasMention: false, mentionAll: false }
      const summaries = state.summaries.map((r) =>
        r.id === roomId ? { ...r, unreadCount: 0, hasMention: false, mentionAll: false } : r
      )
      // Also clear the per-room subscription's hasMention so a cold
      // reload (which reseeds summary.hasMention from
      // state.subscriptions via BUCKETS_LOADED's merge) doesn't
      // resurrect the badge before the server's subscription.update
      // mark-read event lands. RoomEventsContext fires the
      // markRoomRead RPC alongside this dispatch — server is the
      // eventual source of truth; this just keeps the local view
      // consistent in the interim.
      let subscriptions = state.subscriptions
      const existingSub = subscriptions[roomId]
      if (existingSub?.hasMention) {
        subscriptions = { ...subscriptions, [roomId]: { ...existingSub, hasMention: false } }
      }
      return {
        ...state,
        activeRoomId: roomId,
        summaries,
        roomState: { ...state.roomState, [roomId]: nextRoomState },
        subscriptions,
      }
    }
    case 'RESET': {
      return initialState
    }
    case 'ROOMS_FAILED': {
      return { ...state, roomsError: action.error }
    }
    case 'MESSAGE_SENT_LOCAL': {
      // Optimistic append for the local user's own send. Dedupes by id so a
      // later MESSAGE_RECEIVED for the same message is a no-op (appendBounded
      // already handles this — the optimistic row stays put). The shape
      // mirrors a real broadcast message but carries `_local: true` so any
      // UI affordance can distinguish pending-server-confirm rows.
      const msg = action.message
      if (!msg || !msg.id) return state
      const roomId = action.roomId
      if (!roomId) return state
      const prev = state.roomState[roomId] ?? emptyRoomState()
      if (prev.messages.some((m) => m.id === msg.id)) return state
      const messages = appendBounded(prev.messages, msg)
      return {
        ...state,
        roomState: { ...state.roomState, [roomId]: { ...prev, messages } },
      }
    }
    case 'MESSAGE_EDITED_LOCAL': {
      const prev = state.roomState[action.roomId]
      if (!prev) return state
      const idx = prev.messages.findIndex((m) => m.id === action.messageId)
      if (idx < 0) return state
      const updatedMsg = { ...prev.messages[idx], content: action.content, editedAt: action.editedAt }
      const messages = [...prev.messages.slice(0, idx), updatedMsg, ...prev.messages.slice(idx + 1)]
      return {
        ...state,
        roomState: { ...state.roomState, [action.roomId]: { ...prev, messages } },
      }
    }
    case 'MESSAGE_DELETED_LOCAL': {
      const prev = state.roomState[action.roomId]
      if (!prev) return state
      const idx = prev.messages.findIndex((m) => m.id === action.messageId)
      if (idx < 0) return state
      const updatedMsg = { ...prev.messages[idx], deleted: true }
      const messages = [...prev.messages.slice(0, idx), updatedMsg, ...prev.messages.slice(idx + 1)]
      return {
        ...state,
        roomState: { ...state.roomState, [action.roomId]: { ...prev, messages } },
      }
    }
    case 'OWN_THREAD_REPLY_SENT': {
      const prev = state.roomState[action.roomId]
      if (!prev) return state
      const idx = prev.messages.findIndex((m) => m.id === action.parentId)
      if (idx < 0) return state
      const tcount = (prev.messages[idx].tcount ?? 0) + 1
      const updatedMsg = { ...prev.messages[idx], tcount }
      const messages = [...prev.messages.slice(0, idx), updatedMsg, ...prev.messages.slice(idx + 1)]
      return {
        ...state,
        roomState: { ...state.roomState, [action.roomId]: { ...prev, messages } },
      }
    }
    default:
      return state
  }
}
