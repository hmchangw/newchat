import { appendBounded, mergeById, MAX_CACHED } from '@/lib/messageBuffer'
import { defaultChatlistState, sortByLastMsgDesc } from '@/lib/chatlist'
import { previewSnippet, previewText } from '@/lib/previewText'
import { participantDisplayName, messageSenderName } from '@/lib/participantName'

export { MAX_CACHED }

// Returns a new array with the matching row patched, or null if no match.
// Null signals "skip" so callers can fall back to `prev` and avoid touching
// untouched arrays.
function patchMessageById(list, messageId, patch) {
  if (!list || list.length === 0) return null
  const idx = list.findIndex((m) => m.id === messageId)
  if (idx < 0) return null
  return [...list.slice(0, idx), { ...list[idx], ...patch }, ...list.slice(idx + 1)]
}

// Apply a single reaction toggle to a message's reactions map, keyed by
// shortcode with a list of {account, displayName}. Add is idempotent per
// account; remove drops the account and the shortcode key when it empties.
// Returns a new reactions object (never mutates).
function applyReaction(reactions, { shortcode, action, account, displayName }) {
  const next = { ...(reactions || {}) }
  const list = next[shortcode] ? [...next[shortcode]] : []
  const idx = list.findIndex((u) => u.account === account)
  if (action === 'removed') {
    if (idx < 0) return next
    list.splice(idx, 1)
  } else {
    if (idx >= 0) return next
    list.push({ account, displayName })
  }
  if (list.length === 0) delete next[shortcode]
  else next[shortcode] = list
  return next
}

// Like patchMessageById but computes the reaction patch per-matched-message.
function patchReactionById(list, messageId, delta) {
  if (!list || list.length === 0) return null
  const idx = list.findIndex((m) => m.id === messageId)
  if (idx < 0) return null
  const msg = list[idx]
  const reactions = applyReaction(msg.reactions, delta)
  return [...list.slice(0, idx), { ...msg, reactions }, ...list.slice(idx + 1)]
}

export const BUFFER_MODE = {
  LIVE: 'live',
  HISTORICAL: 'historical',
}

export const initialState = {
  summaries: [],
  roomState: {},
  activeRoomId: null,
  roomsError: null,
  /**
   * Monotonic counter bumped on every accepted MESSAGE_RECEIVED (any
   * room). Pure trigger, not derived data — `useUnreadCount` keys a
   * debounced `subscription.count` refetch off it so the badge tracks
   * incoming messages without re-deriving from `summaries`.
   */
  msgRecvSeq: 0,
  /**
   * Monotonic counter bumped after a `markRoomRead` RPC resolves (the
   * server-side `lastSeenAt` write has committed). `useUnreadCount`
   * refetches on it so the badge is pulled AFTER the read lands rather
   * than racing it.
   */
  readSeq: 0,
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
  /**
   * Per-roomId sidebar preview: the room's most recent eligible message,
   * flattened to a single line at write time. An absent key means there is
   * nothing to show — the row still renders at full height with a blank
   * snippet line (the row's height is a CSS property, not a consequence of
   * this map).
   *
   * Shape: { [roomId]: { messageId, senderName, text } }
   */
  previews: {},
  /**
   * The per-user chatlist section-definition overlay (ChatlistState) — the
   * section names, display order, and sortMode. Membership is NOT here; it
   * rides each subscription's sectionId/sectionOrder. Seeded by
   * CHATLIST_LOADED (chatlist.get) and replaced wholesale by CHATLIST_UPDATED
   * (chatlist.update event, last-write-wins on lastUpdatedAt). Defaults to the
   * four derived built-ins for a never-customized user.
   */
  chatlist: defaultChatlistState(),
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

// Build a stored preview from a live message. Returns null when there is
// nothing to store, so callers leave the map untouched rather than writing an
// empty sentinel.
function previewFromMessage(msg) {
  if (!msg || !msg.id) return null
  return {
    messageId: msg.id,
    senderName: messageSenderName(msg),
    text: previewSnippet(msg.content, msg.mentions, msg.attachments),
  }
}

// Two stored previews are interchangeable when all three rendered fields match.
// Used to keep the previews reference stable on a same-content write (e.g. the
// server echo of a message this client already stored optimistically) — a fresh
// object would invalidate useSidebarSections' memo for every room in the sidebar.
function samePreview(a, b) {
  return !!a && !!b && a.messageId === b.messageId && a.senderName === b.senderName && a.text === b.text
}

// Build a stored preview from a wire PreviewMessage (subscription.list rows
// and the refreshed preview on edit/delete events).
function previewFromWire(previewMessage) {
  if (!previewMessage || !previewMessage.messageId) return null
  return {
    messageId: previewMessage.messageId,
    senderName: participantDisplayName(previewMessage.sender),
    text: previewSnippet(previewMessage.content, previewMessage.mentions, previewMessage.attachments),
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
    // Older-message pagination. `hasMoreOlder` starts true (unknown ⇒
    // assume there may be older history) and is set from the fetched
    // page's fullness once history loads. `loadingOlder` guards the
    // in-flight older-page fetch and drives the top spinner.
    hasMoreOlder: true,
    loadingOlder: false,
  }
}

export function roomEventsReducer(state, action) {
  switch (action.type) {
    case 'ROOMS_LOADED': {
      // Enrich every fresh summary with the matching subscription record
      // if one is already in state. Without this, a BUCKETS_LOADED that
      // landed BEFORE ROOMS_LOADED (parallel-fetched in the bootstrap)
      // would have its server-canonical hasMention / subscriptionName
      // wiped out by toSummary's zero-init when ROOMS_LOADED arrives.
      const summaries = sortByLastMsgDesc(
        action.rooms.map((r) =>
          mergeSubscriptionIntoSummary(toSummary(r), state.subscriptions[r.id])
        )
      )
      return { ...state, summaries, roomsError: null }
    }
    case 'ROOM_ADDED': {
      const roomId = action.room.id
      const existingIdx = state.summaries.findIndex((r) => r.id === roomId)
      // The summary may already exist if ROOMS_LOADED ran first (the
      // bootstrap fan-out + a live `subscription.update added` can race).
      // In that case we still need the bucket-Set maintenance below —
      // skipping it would leave the room in `summaries` but in NO
      // bucket Set, so `useSidebarSections`'s strict partition would
      // silently drop it (until the all-empty fallback kicks in).
      let summaries = state.summaries
      if (existingIdx === -1) {
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
        summaries = sortByLastMsgDesc([...state.summaries, summary])
      }
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
      // Object-identity short-circuit so a true no-op (room already in
      // summaries AND in the right bucket) doesn't trigger a re-render.
      if (
        summaries === state.summaries &&
        appIds === state.appIds &&
        channelDmIds === state.channelDmIds
      ) {
        return state
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
      let previews = state.previews
      if (previews[action.roomId]) {
        const { [action.roomId]: _dropPreview, ...restPreviews } = previews
        previews = restPreviews
      }
      return {
        ...state,
        summaries,
        roomState: rest,
        favoriteIds,
        appIds,
        channelDmIds,
        subscriptions,
        previews,
      }
    }
    case 'BUCKETS_LOADED': {
      const subs = action.subscriptions ?? {}
      let summaries
      if (action.rooms) {
        // Cold-start path: rooms are derived from the three subscription
        // RPCs (the real user-service embeds room metadata inline). Build
        // summaries from scratch and merge in the per-room subscription
        // for server-canonical hasMention / subscriptionName.
        summaries = sortByLastMsgDesc(
          action.rooms.map((r) =>
            mergeSubscriptionIntoSummary(toSummary(r), subs[r.id])
          )
        )
      } else {
        // Partial-update path (e.g. tests, future bucket-refresh): no
        // rooms supplied, just enrich existing summaries with new sub
        // metadata.
        summaries = state.summaries.map((s) =>
          subs[s.id] ? mergeSubscriptionIntoSummary(s, subs[s.id]) : s
        )
      }
      // Seed from the room metadata user-service embeds on each list row.
      // Starts from the existing map so the partial-update path (no rooms
      // supplied) doesn't wipe previews already in hand.
      const previews = { ...state.previews }
      for (const [roomId, sub] of Object.entries(subs)) {
        const preview = previewFromWire(sub?.room?.previewMessage)
        if (preview) previews[roomId] = preview
      }
      return {
        ...state,
        summaries,
        favoriteIds: new Set(action.favoriteIds),
        appIds: new Set(action.appIds),
        channelDmIds: new Set(action.channelDmIds),
        subscriptions: subs,
        previews,
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
    case 'SUBSCRIPTION_SECTION_MOVED': {
      // A chat's chatlist section membership/order changed (subscription.update
      // action "section_moved"). Unlike SUBSCRIPTION_UPSERTED's spread-merge,
      // set BOTH fields explicitly — a remove clears them (a spread of an
      // omitempty-absent field would leave the chat stuck in its old section).
      const { roomId } = action
      if (!roomId) return state
      const prev = state.subscriptions[roomId]
      if (!prev) return state
      if (prev.sectionId === action.sectionId && prev.sectionOrder === action.sectionOrder) {
        return state
      }
      const next = { ...prev, sectionId: action.sectionId, sectionOrder: action.sectionOrder }
      return { ...state, subscriptions: { ...state.subscriptions, [roomId]: next } }
    }
    case 'CHATLIST_LOADED': {
      // Initial section-definition overlay (chatlist.get). Replaces the
      // built-in default wholesale.
      if (!action.chatlist) return state
      return { ...state, chatlist: action.chatlist }
    }
    case 'CHATLIST_UPDATED': {
      // Live chatlist.update — full-state replace, last-write-wins on the
      // overlay's high-water mark. A stale event (older lastUpdatedAt) is
      // dropped.
      const incoming = action.chatlist
      if (!incoming) return state
      if (state.chatlist && incoming.lastUpdatedAt < state.chatlist.lastUpdatedAt) return state
      return { ...state, chatlist: incoming }
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
      // Thread reply: bump parent's tcount (dedupe via threadReplyIds to skip
      // sender's own echo), don't insert into main feed.
      if (msg.threadParentMessageId) {
        const tRoomId = evt.roomId
        const tPrev = state.roomState[tRoomId]
        // No room buffer means no parent in state to update — skip silently;
        // the count will be authoritative when history is fetched later.
        if (!tPrev) return state
        const parentIdx = tPrev.messages.findIndex(
          (m) => m.id === msg.threadParentMessageId
        )
        if (parentIdx < 0) return state
        const parent = tPrev.messages[parentIdx]
        const seenReplies = parent.threadReplyIds ?? new Set()
        if (seenReplies.has(msg.id)) return state
        const nextSeen = new Set(seenReplies)
        nextSeen.add(msg.id)
        const updatedParent = {
          ...parent,
          tcount: (parent.tcount ?? 0) + 1,
          threadReplyIds: nextSeen,
        }
        const messages = [
          ...tPrev.messages.slice(0, parentIdx),
          updatedParent,
          ...tPrev.messages.slice(parentIdx + 1),
        ]
        return {
          ...state,
          roomState: { ...state.roomState, [tRoomId]: { ...tPrev, messages } },
        }
      }
      const roomId = evt.roomId
      // Thread replies returned above, so anything reaching here belongs in
      // the room timeline and is a preview candidate. Computed once and
      // applied at every return point below.
      const nextPreview = previewFromMessage(msg)
      const previews =
        !nextPreview || samePreview(state.previews[roomId], nextPreview)
          ? state.previews
          : { ...state.previews, [roomId]: nextPreview }
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
          msgRecvSeq: state.msgRecvSeq + 1,
          previews,
        }
      }
      // Replace optimistic createdAt (client clock) with server's — keeping
      // it stale breaks message-worker's IF EXISTS stamp for thread replies.
      const existingIdx = prev.messages.findIndex((m) => m.id === msg.id)
      if (existingIdx >= 0) {
        const existing = prev.messages[existingIdx]
        const replaced = { ...existing, ...msg }
        if (existing._local) replaced._local = existing._local
        if (existing._status) replaced._status = existing._status
        const mergedMessages = [
          ...prev.messages.slice(0, existingIdx),
          replaced,
          ...prev.messages.slice(existingIdx + 1),
        ]
        return {
          ...state,
          roomState: { ...state.roomState, [roomId]: { ...prev, messages: mergedMessages } },
          previews,
        }
      }
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
        msgRecvSeq: state.msgRecvSeq + 1,
        previews,
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
            // Whether a full first page came back (⇒ older pages may follow).
            // Absent action field leaves the prior value untouched.
            hasMoreOlder: action.hasMoreOlder ?? prev.hasMoreOlder,
            loadingOlder: false,
          },
        },
      }
    }
    case 'HISTORY_OLDER_LOADING': {
      const prev = state.roomState[action.roomId] ?? emptyRoomState()
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: { ...prev, loadingOlder: true, historyError: null },
        },
      }
    }
    case 'HISTORY_OLDER_LOADED': {
      const prev = state.roomState[action.roomId] ?? emptyRoomState()
      // Prepend the older block ahead of the current buffer, deduping any id
      // already present. Unlike the live tail, older pages are NOT trimmed to
      // MAX_CACHED from the front — trimming the front would discard exactly
      // the messages the user paginated up to see. The buffer is allowed to
      // grow while the user is actively browsing older history.
      const existingIds = new Set(prev.messages.map((m) => m.id))
      const older = (action.messages ?? []).filter((m) => !existingIds.has(m.id))
      const messages = older.length ? [...older, ...prev.messages] : prev.messages
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: {
            ...prev,
            messages,
            loadingOlder: false,
            hasMoreOlder: !!action.hasMoreOlder,
            historyError: null,
          },
        },
      }
    }
    case 'HISTORY_OLDER_FAILED': {
      const prev = state.roomState[action.roomId] ?? emptyRoomState()
      // Only stop the spinner. Leave hasMoreOlder untouched so the next
      // scroll-to-top retries, and don't hoist the failure into the
      // room-wide historyError banner — a failed older page shouldn't blank
      // the messages the user is already reading.
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: { ...prev, loadingOlder: false },
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
            // A jumped-to window sits in the middle of history — older
            // messages exist above it, so re-enable upward pagination.
            hasMoreOlder: true,
            loadingOlder: false,
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
    case 'ROOM_READ_SYNCED': {
      return { ...state, readSeq: state.readSeq + 1 }
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
      // A thread reply doesn't appear in the room timeline, so it isn't the
      // room's preview either — matching the server, which omits
      // previewMessage for hidden thread replies.
      const nextPreview = msg.threadParentMessageId ? null : previewFromMessage(msg)
      const previews =
        !nextPreview || samePreview(state.previews[roomId], nextPreview)
          ? state.previews
          : { ...state.previews, [roomId]: nextPreview }
      return {
        ...state,
        roomState: { ...state.roomState, [roomId]: { ...prev, messages } },
        previews,
      }
    }
    case 'MESSAGE_EDITED_LOCAL': {
      const prev = state.roomState[action.roomId]
      if (!prev) return state
      const idx = prev.messages.findIndex((m) => m.id === action.messageId)
      if (idx < 0) return state
      const updatedMsg = { ...prev.messages[idx], content: action.content, editedAt: action.editedAt }
      const messages = [...prev.messages.slice(0, idx), updatedMsg, ...prev.messages.slice(idx + 1)]
      // Only the message currently on display affects the preview. The
      // action carries no mentions, so mention tokens flatten literally
      // here; the server's message_edited event follows with the
      // authoritative preview a moment later.
      const cur = state.previews[action.roomId]
      const previews =
        cur && cur.messageId === action.messageId
          ? { ...state.previews, [action.roomId]: { ...cur, text: previewText(action.content) } }
          : state.previews
      return {
        ...state,
        roomState: { ...state.roomState, [action.roomId]: { ...prev, messages } },
        previews,
      }
    }
    case 'MESSAGE_DELETED_LOCAL': {
      const prev = state.roomState[action.roomId]
      if (!prev) return state
      const idx = prev.messages.findIndex((m) => m.id === action.messageId)
      if (idx < 0) return state
      const updatedMsg = { ...prev.messages[idx], deleted: true }
      const messages = [...prev.messages.slice(0, idx), updatedMsg, ...prev.messages.slice(idx + 1)]
      // Preview deliberately untouched: the client can't reproduce the
      // server's walk-back to an earlier eligible message. The authoritative
      // message_deleted event corrects it.
      return {
        ...state,
        roomState: { ...state.roomState, [action.roomId]: { ...prev, messages } },
      }
    }
    case 'MESSAGE_EDITED': {
      // Live broadcast `message_edited`. Mirrors `_LOCAL` but must also patch
      // pendingLiveMessages — when the user is in historical mode, recent
      // messages live there until RESET_TO_LIVE_TAIL merges them back. An
      // edit arriving while in that mode would otherwise be lost on merge.
      const prev = state.roomState[action.roomId]
      if (!prev) return state
      const patch = { content: action.content, editedAt: action.editedAt }
      const messages = patchMessageById(prev.messages, action.messageId, patch)
      const pendingLiveMessages = patchMessageById(prev.pendingLiveMessages, action.messageId, patch)
      if (!messages && !pendingLiveMessages) return state
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: {
            ...prev,
            messages: messages ?? prev.messages,
            pendingLiveMessages: pendingLiveMessages ?? prev.pendingLiveMessages,
          },
        },
      }
    }
    case 'MESSAGE_DELETED': {
      // Live broadcast `message_deleted`. See MESSAGE_EDITED for the
      // pendingLiveMessages rationale.
      const prev = state.roomState[action.roomId]
      if (!prev) return state
      const patch = { deleted: true }
      const messages = patchMessageById(prev.messages, action.messageId, patch)
      const pendingLiveMessages = patchMessageById(prev.pendingLiveMessages, action.messageId, patch)
      if (!messages && !pendingLiveMessages) return state
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: {
            ...prev,
            messages: messages ?? prev.messages,
            pendingLiveMessages: pendingLiveMessages ?? prev.pendingLiveMessages,
          },
        },
      }
    }
    case 'ROOM_PREVIEW_UPDATED': {
      // The room's refreshed preview after an edit or delete, computed
      // server-side. Its own action rather than a field on MESSAGE_EDITED /
      // MESSAGE_DELETED because both of those bail when the room has no
      // message buffer — which is the normal case for a sidebar row.
      const { roomId, previewMessage, deletedMessageId } = action
      if (!roomId) return state
      const next = previewFromWire(previewMessage)
      if (next) return { ...state, previews: { ...state.previews, [roomId]: next } }
      // No preview on the event. For a delete that means nothing eligible is
      // left — but only when the deleted message is the one on display. Any
      // other id is contradictory input (the server would have echoed the
      // unchanged preview), so leave what's there.
      const cur = state.previews[roomId]
      if (!deletedMessageId || !cur || cur.messageId !== deletedMessageId) return state
      const { [roomId]: _cleared, ...rest } = state.previews
      return { ...state, previews: rest }
    }
    case 'MESSAGE_REACTED': {
      // Live `message_reacted` toggle. Mirrors MESSAGE_EDITED's dual-buffer
      // patch so a reaction arriving while in historical mode isn't lost when
      // pendingLiveMessages merges back.
      const prev = state.roomState[action.roomId]
      if (!prev) return state
      const delta = {
        shortcode: action.shortcode,
        action: action.action,
        account: action.account,
        displayName: action.displayName,
      }
      const messages = patchReactionById(prev.messages, action.messageId, delta)
      const pendingLiveMessages = patchReactionById(prev.pendingLiveMessages, action.messageId, delta)
      if (!messages && !pendingLiveMessages) return state
      return {
        ...state,
        roomState: {
          ...state.roomState,
          [action.roomId]: {
            ...prev,
            messages: messages ?? prev.messages,
            pendingLiveMessages: pendingLiveMessages ?? prev.pendingLiveMessages,
          },
        },
      }
    }
    case 'OWN_THREAD_REPLY_SENT': {
      // Optimistic tcount bump; inbound echo dedupes off threadReplyIds.
      const prev = state.roomState[action.roomId]
      if (!prev) return state
      const idx = prev.messages.findIndex((m) => m.id === action.parentId)
      if (idx < 0) return state
      const parent = prev.messages[idx]
      const seen = parent.threadReplyIds ?? new Set()
      // Idempotency: don't re-bump if this replyId was already counted.
      if (action.replyId && seen.has(action.replyId)) return state
      const nextSeen =
        action.replyId && !seen.has(action.replyId)
          ? (() => { const s = new Set(seen); s.add(action.replyId); return s })()
          : seen
      const tcount = (parent.tcount ?? 0) + 1
      const updatedMsg = { ...parent, tcount, threadReplyIds: nextSeen }
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
