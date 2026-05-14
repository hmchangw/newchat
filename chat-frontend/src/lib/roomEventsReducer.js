export const MAX_CACHED = 200

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
  // Per-roomId subscription metadata sourced from the user-service RPCs.
  // Shape: { [roomId]: { name?: string, hrInfo?: { engName, name } } }.
  // Merged into rooms at read time by useSidebarSections so display logic
  // (roomDisplayName) can use subscription.Name / HRInfo without changing
  // the underlying summary structure.
  subscriptionData: {},
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

function appendBounded(messages, msg) {
  if (messages.some((m) => m.id === msg.id)) return messages
  const next = [...messages, msg]
  if (next.length > MAX_CACHED) {
    return next.slice(next.length - MAX_CACHED)
  }
  return next
}

export function roomEventsReducer(state, action) {
  switch (action.type) {
    case 'ROOMS_LOADED': {
      const summaries = sortByLastMsgDesc(action.rooms.map(toSummary))
      return { ...state, summaries, roomsError: null }
    }
    case 'ROOM_ADDED': {
      if (state.summaries.some((r) => r.id === action.room.id)) return state
      const summaries = sortByLastMsgDesc([...state.summaries, toSummary(action.room)])
      const roomId = action.room.id
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
      let subscriptionData = state.subscriptionData
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
      if (subscriptionData[action.roomId]) {
        const { [action.roomId]: _drop, ...restData } = subscriptionData
        subscriptionData = restData
      }
      return {
        ...state,
        summaries,
        roomState: rest,
        favoriteIds,
        appIds,
        channelDmIds,
        subscriptionData,
      }
    }
    case 'BUCKETS_LOADED': {
      return {
        ...state,
        favoriteIds: new Set(action.favoriteIds),
        appIds: new Set(action.appIds),
        channelDmIds: new Set(action.channelDmIds),
        subscriptionData: action.subscriptionData ?? {},
      }
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
                      hasMention: nextRoomState.hasMention,
                      mentionAll: nextRoomState.mentionAll,
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
                    hasMention: nextRoomState.hasMention,
                    mentionAll: nextRoomState.mentionAll,
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
      const existingIds = new Set(prev.messages.map((m) => m.id))
      const merged = [
        ...action.messages.filter((m) => !existingIds.has(m.id)),
        ...prev.messages,
      ]
      const bounded = merged.length > MAX_CACHED ? merged.slice(merged.length - MAX_CACHED) : merged
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: {
            ...prev,
            messages: bounded,
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
      return {
        ...state,
        activeRoomId: roomId,
        summaries,
        roomState: { ...state.roomState, [roomId]: nextRoomState },
      }
    }
    case 'RESET': {
      return initialState
    }
    case 'ROOMS_FAILED': {
      return { ...state, roomsError: action.error }
    }
    default:
      return state
  }
}
