import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useReducer,
  useRef,
  type Dispatch,
  type ReactNode,
} from 'react'
import { useNats } from '@/context/NatsContext'
import { useRoomKeys } from '@/context/RoomKeysContext'
import { BUFFER_MODE, initialState, roomEventsReducer } from './reducer'
import { useRoomSubscriptions } from './useRoomSubscriptions'
import { useUnreadCount as useUnreadCountFold } from './useUnreadCount'
import {
  fetchMessageHistory,
  fetchSurroundingMessages,
  markRoomRead,
  createChatlistSection,
  renameChatlistSection,
  deleteChatlistSection,
  reorderChatlistSections,
  setChatlistSectionSortMode,
  moveChat,
} from '@/api'
import { deriveSidebarSections } from '@/lib/chatlist'
import type {
  ChatlistState,
  ChatlistSortMode,
  DMSubscription,
  Message,
  Nats,
  Room,
  SubscriptionHRInfo,
} from '@/api/types'

/** Per-room buffer state — owned by the reducer's `roomState` map. */
interface RoomBufferState {
  messages: Message[]
  hasLoadedHistory: boolean
  historyError: string | null
  unreadCount: number
  hasMention: boolean
  mentionAll: boolean
  lastMsgAt: string | null
  lastMsgId: string | null
  bufferMode: 'live' | 'historical'
  pendingLiveMessages: Message[]
  focusMessageId: string | null
  /** True while more older messages may exist above the buffer. */
  hasMoreOlder: boolean
  /** True while an older-page fetch is in flight (drives the top spinner). */
  loadingOlder: boolean
}

/** Page size for both the initial history load and each older page. A
 *  returned page of exactly this size means older messages may follow. */
const HISTORY_PAGE_SIZE = 50

/** One room's stored sidebar preview. Flattened and name-resolved at write
 *  time so the render layer never branches on whether it came from a wire
 *  PreviewMessage or a live Message. */
interface RoomPreview {
  /** The previewed message's id — guards the delete-clears rule in the
   *  reducer's ROOM_PREVIEW_UPDATED case. */
  messageId: string
  senderName: string
  /** Single-line, flattened, capped at PREVIEW_MAX_LENGTH. */
  text: string
  /** RFC3339, from the previewed message's createdAt. Powers
   *  ROOM_PREVIEW_UPDATED's recency guard: broadcast-worker processes
   *  canonical messages concurrently, so an edit/delete's server-resolved
   *  preview can arrive after a genuinely newer message's live preview —
   *  a strictly older incoming createdAt is rejected rather than regressing
   *  the sidebar. Optional because older code paths / test fixtures may not
   *  set it; a missing timestamp on either side of the comparison is
   *  treated as "accept the write". */
  createdAt?: string
  /** True when this preview was built from a live message the client
   *  couldn't decrypt — the reducer's "[encrypted message]" placeholder
   *  branch. Guards ROOM_PREVIEW_UPDATED from overwriting the placeholder
   *  with the server's plaintext previewMessage body for the SAME message
   *  (history-service / broadcast-worker relay that body unencrypted). A
   *  wire preview for a DIFFERENT, newer message still overwrites normally.
   *  Known residual: after a reload there's no placeholder in state to
   *  compare against, so bootstrap still shows the server's plaintext —
   *  fully closing that gap needs a backend change, not a client one. */
  encrypted?: boolean
}

/** Sidebar summary — derived from `model.Room` + the user's
 *  Subscription. Only the fields the sidebar / chat header read. */
interface RoomSummary {
  id: string
  name: string
  subscriptionName?: string
  type: Room['type']
  siteId: string
  userCount: number
  lastMsgAt: string | null
  unreadCount: number
  hasMention: boolean
  mentionAll: boolean
  /** DM-room counterpart's HRInfo — added by useSidebarSections enrich
   *  for DM rooms whose subscription carried it. Plain channels stay
   *  undefined here. */
  hrInfo?: SubscriptionHRInfo
  /** Joined in by useSidebarSections from state.previews. Undefined when the
   *  room has no preview — the row still renders at full height with a blank
   *  snippet line. */
  preview?: RoomPreview
}

/** Top-level state shape returned by `roomEventsReducer`. */
interface RoomEventsState {
  summaries: RoomSummary[]
  roomState: Record<string, RoomBufferState>
  activeRoomId: string | null
  roomsError: string | null
  /** Monotonic counter bumped on every accepted MESSAGE_RECEIVED.
   *  Drives the unread badge's debounced refetch (see useUnreadCount). */
  msgRecvSeq: number
  /** Monotonic counter bumped after a markRoomRead RPC resolves.
   *  Drives the unread badge's post-read refetch (see useUnreadCount). */
  readSeq: number
  favoriteIds: Set<string>
  appIds: Set<string>
  channelDmIds: Set<string>
  /** Keyed by roomId. Typed `DMSubscription` (Subscription ∪ hrInfo?)
   *  so consumers reading the map for either channel or DM rooms see
   *  hrInfo as optional without narrowing. */
  subscriptions: Record<string, DMSubscription>
  /** Keyed by roomId. Absent key = no preview to show. Deliberately NOT a
   *  field on RoomSummary: summaries are rebuilt from `Room` records, which
   *  mirror pkg/model.Room — and the backend hangs previewMessage off
   *  SubscriptionRoom instead, so a summary field would break the mirror. */
  previews: Record<string, RoomPreview>
  /** Chatlist section-definition overlay (names, order, sortMode). Seeded by
   *  CHATLIST_LOADED, replaced by CHATLIST_UPDATED (LWW). Membership rides the
   *  subscriptions; this is O(sections). */
  chatlist: ChatlistState
}

/** Payload for the thread-reply hook ThreadEventsProvider registers. */
export interface ThreadReplyEvent {
  parentMessageId: string
  roomId: string
  siteId: string
  message: Message
}

type ThreadReplyHandler = (evt: ThreadReplyEvent) => void

/** Payload for the thread-message-mutation hook (live edit + delete events). */
export type ThreadMessageMutation =
  | { kind: 'edited'; messageId: string; content?: string; editedAt: string }
  | { kind: 'deleted'; messageId: string }

type ThreadMessageMutationHandler = (mut: ThreadMessageMutation) => void

/** Surface exposed via React context. Components consume via
 *  `useRoomEvents` / `useRoomSummaries` / `useSubscription` / etc. */
interface RoomEventsContextValue {
  state: RoomEventsState
  dispatch: Dispatch<{ type: string; [k: string]: unknown }>
  loadHistory: (roomId: string) => Promise<unknown> | void
  loadOlderHistory: (roomId: string) => Promise<unknown> | void
  setActiveRoom: (roomId: string | null) => void
  jumpToMessage: (roomId: string, messageId: string) => Promise<void> | void
  resetToLiveTail: (roomId: string) => void
  /** Register a thread-reply event handler; returns an unsubscribe fn. */
  registerThreadReplyHandler: (h: ThreadReplyHandler) => () => void
  /** Register a handler for thread-message edit/delete; returns an unsubscribe fn. */
  registerThreadMessageMutationHandler: (h: ThreadMessageMutationHandler) => () => void
}

const RoomEventsContext = createContext<RoomEventsContextValue | null>(null)

export function RoomEventsProvider({ children }: { children: ReactNode }) {
  // `useNats()` returns `never` to TS because NatsContext.jsx does
  // `createContext(null)` without annotations. Cast here so downstream
  // callbacks see the proper Nats interface — safe because the
  // provider only renders inside the `connected` gate at App.jsx,
  // where the NATS handshake has populated user/request/etc.
  const nats = useNats() as unknown as Nats
  const { user } = nats
  const { decrypt, ensureKey, seedKeys } = useRoomKeys()
  const [state, dispatch] = useReducer(roomEventsReducer, initialState) as unknown as [
    RoomEventsState,
    Dispatch<{ type: string; [k: string]: unknown }>,
  ]

  const inflightHistory = useRef(new Map<string, Promise<unknown>>())
  const inflightOlder = useRef(new Map<string, Promise<unknown>>())
  const stateRef = useRef(state)
  stateRef.current = state

  // Single-slot for ThreadEventsProvider's live-reply handler.
  const threadReplyHandlerRef = useRef<ThreadReplyHandler | null>(null)
  const registerThreadReplyHandler = useCallback((h: ThreadReplyHandler) => {
    threadReplyHandlerRef.current = h
    return () => {
      if (threadReplyHandlerRef.current === h) threadReplyHandlerRef.current = null
    }
  }, [])

  // Single-slot for ThreadEventsProvider's live edit/delete handler.
  const threadMessageMutationHandlerRef = useRef<ThreadMessageMutationHandler | null>(null)
  const registerThreadMessageMutationHandler = useCallback((h: ThreadMessageMutationHandler) => {
    threadMessageMutationHandlerRef.current = h
    return () => {
      if (threadMessageMutationHandlerRef.current === h) threadMessageMutationHandlerRef.current = null
    }
  }, [])

  // useRoomSubscriptions reads `.current` on the ref slots when room-channel
  // events arrive, fanning them to ThreadEvents.
  const { currentGeneration } = useRoomSubscriptions(
    nats,
    dispatch,
    stateRef,
    threadReplyHandlerRef,
    threadMessageMutationHandlerRef,
    decrypt,
    ensureKey,
    seedKeys,
  )

  const loadHistory = useCallback(
    async (roomId: string) => {
      if (!user || !roomId) return
      const prev = stateRef.current.roomState[roomId]
      if (prev?.hasLoadedHistory) return
      if (inflightHistory.current.has(roomId)) return inflightHistory.current.get(roomId)

      // Route to the room's home site: a cross-site room's history lives on
      // another site, so use the summary's siteId (mirrors loadOlderHistory /
      // markRoomRead / loadSurrounding); fall back to the user's home site when
      // no summary is loaded yet.
      const summary = stateRef.current.summaries.find((r) => r.id === roomId)
      const siteId = summary?.siteId ?? user.siteId

      const gen = currentGeneration()
      const promise = (async () => {
        try {
          const resp = await fetchMessageHistory(nats, { roomId, siteId, limit: HISTORY_PAGE_SIZE })
          // history-service ships newest-first; the UI reads chronological.
          // Normalisation to the broadcast `Message` shape now happens inside
          // the api op.
          const asc = [...(resp.messages ?? [])].reverse()
          // A full page back ⇒ there may be older messages to paginate to.
          const hasMoreOlder = (resp.messages?.length ?? 0) >= HISTORY_PAGE_SIZE
          if (currentGeneration() === gen) dispatch({ type: 'HISTORY_LOADED', roomId, messages: asc, hasMoreOlder })
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err)
          if (currentGeneration() === gen) dispatch({ type: 'HISTORY_FAILED', roomId, error: message })
          throw err
        } finally {
          inflightHistory.current.delete(roomId)
        }
      })()
      inflightHistory.current.set(roomId, promise)
      return promise
    },
    [user, nats, currentGeneration],
  )

  // Fetch the page of messages immediately older than the buffer's current
  // top, prepending them. Triggered by MessageList when the user scrolls near
  // the top. No-ops until the initial history has loaded, when nothing older
  // remains, or when a fetch is already in flight (coalesced via inflightOlder).
  const loadOlderHistory = useCallback(
    async (roomId: string) => {
      if (!user || !roomId) return
      const rs = stateRef.current.roomState[roomId]
      if (!rs || !rs.hasLoadedHistory) return
      if (rs.loadingOlder || !rs.hasMoreOlder) return
      if (inflightOlder.current.has(roomId)) return inflightOlder.current.get(roomId)
      const oldest = rs.messages[0]
      if (!oldest) return
      // `before` is an exclusive cursor (history-service: created_at < before),
      // so the oldest loaded message's own timestamp fetches strictly older rows.
      const before = new Date(oldest.createdAt).getTime()
      if (!Number.isFinite(before)) return
      const summary = stateRef.current.summaries.find((r) => r.id === roomId)
      const siteId = summary?.siteId ?? user.siteId

      const gen = currentGeneration()
      dispatch({ type: 'HISTORY_OLDER_LOADING', roomId })
      const promise = (async () => {
        try {
          const resp = await fetchMessageHistory(nats, { roomId, siteId, limit: HISTORY_PAGE_SIZE, before })
          const asc = [...(resp.messages ?? [])].reverse()
          const hasMoreOlder = (resp.messages?.length ?? 0) >= HISTORY_PAGE_SIZE
          if (currentGeneration() === gen) {
            dispatch({ type: 'HISTORY_OLDER_LOADED', roomId, messages: asc, hasMoreOlder })
          }
        } catch (err) {
          if (currentGeneration() === gen) dispatch({ type: 'HISTORY_OLDER_FAILED', roomId })
          throw err
        } finally {
          inflightOlder.current.delete(roomId)
        }
      })()
      inflightOlder.current.set(roomId, promise)
      return promise
    },
    [user, nats, currentGeneration],
  )

  const setActiveRoom = useCallback(
    (roomId: string | null) => {
      dispatch({ type: 'SET_ACTIVE_ROOM', roomId })
      // Mark-read when the user opens a room. siteId comes from the
      // summary if we have one (cross-site DMs etc.); otherwise fall
      // back to the user's home site. Bump readSeq once the RPC resolves
      // (lastSeenAt committed) so the unread badge re-pulls AFTER the
      // read instead of racing it.
      if (roomId) {
        const summary = stateRef.current.summaries.find((r) => r.id === roomId)
        const siteId = summary?.siteId ?? user.siteId
        markRoomRead(nats, { roomId, siteId }).then((ok) => {
          if (ok) dispatch({ type: 'ROOM_READ_SYNCED' })
        })
      }
    },
    [nats, user],
  )

  const jumpToMessage = useCallback(
    async (roomId: string, messageId: string) => {
      if (!user || !roomId || !messageId) return
      const summary = stateRef.current.summaries.find((r) => r.id === roomId)
      const siteId = summary?.siteId ?? user.siteId
      const gen = currentGeneration()
      try {
        const resp = await fetchSurroundingMessages(nats, { roomId, siteId, messageId })
        if (currentGeneration() !== gen) return
        dispatch({
          type: 'REPLACE_ROOM_BUFFER',
          roomId,
          messages: resp.messages,
          focusMessageId: messageId,
        })
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err)
        if (currentGeneration() === gen) {
          dispatch({ type: 'HISTORY_FAILED', roomId, error: message })
        }
        throw err
      }
    },
    [user, nats, currentGeneration],
  )

  const resetToLiveTail = useCallback((roomId: string) => {
    if (!roomId) return
    dispatch({ type: 'RESET_TO_LIVE_TAIL', roomId })
  }, [])

  const value = useMemo<RoomEventsContextValue>(
    () => ({
      state,
      dispatch,
      loadHistory,
      loadOlderHistory,
      setActiveRoom,
      jumpToMessage,
      resetToLiveTail,
      registerThreadReplyHandler,
      registerThreadMessageMutationHandler,
    }),
    [
      state,
      dispatch,
      loadHistory,
      loadOlderHistory,
      setActiveRoom,
      jumpToMessage,
      resetToLiveTail,
      registerThreadReplyHandler,
      registerThreadMessageMutationHandler,
    ],
  )

  return <RoomEventsContext.Provider value={value}>{children}</RoomEventsContext.Provider>
}

function useRoomEventsInternal(): RoomEventsContextValue {
  const ctx = useContext(RoomEventsContext)
  if (!ctx) throw new Error('RoomEvents hooks must be used inside RoomEventsProvider')
  return ctx
}

export function useRoomEvents(roomId: string | null | undefined) {
  const { state, dispatch, loadHistory, loadOlderHistory, jumpToMessage, resetToLiveTail } =
    useRoomEventsInternal()
  const room = roomId ? state.roomState[roomId] : undefined
  const load = useCallback(() => (roomId ? loadHistory(roomId) : undefined), [loadHistory, roomId])
  const loadOlder = useCallback(
    () => (roomId ? loadOlderHistory(roomId) : undefined),
    [loadOlderHistory, roomId],
  )
  const jump = useCallback(
    (messageId: string) => (roomId ? jumpToMessage(roomId, messageId) : undefined),
    [jumpToMessage, roomId],
  )
  const reset = useCallback(() => (roomId ? resetToLiveTail(roomId) : undefined), [resetToLiveTail, roomId])
  return useMemo(
    () => ({
      messages: room?.messages ?? [],
      hasLoadedHistory: !!room?.hasLoadedHistory,
      historyError: room?.historyError ?? null,
      loadHistory: load,
      loadOlder,
      hasMoreOlder: room?.hasMoreOlder ?? true,
      loadingOlder: !!room?.loadingOlder,
      bufferMode: room?.bufferMode ?? BUFFER_MODE.LIVE,
      pendingCount: room?.pendingLiveMessages?.length ?? 0,
      focusMessageId: room?.focusMessageId ?? null,
      jumpToMessage: jump,
      resetToLiveTail: reset,
      dispatch,
    }),
    [room, load, loadOlder, jump, reset, dispatch],
  )
}

export function useRoomSummaries() {
  const { state, setActiveRoom, jumpToMessage } = useRoomEventsInternal()
  return {
    summaries: state.summaries,
    setActiveRoom,
    jumpToMessage,
    error: state.roomsError,
  }
}

/**
 * App-wide unread total for the header badge.
 *
 * Folded from local subscription state — no RPC. The client already holds
 * every input the server's `subscription.count` computes over, and receives
 * every delta live, so recomputing it server-side per message was redundant.
 */
export function useUnreadCount(): number {
  const { state } = useRoomEventsInternal()
  return useUnreadCountFold(state)
}

export function useRoomDispatch(): RoomEventsContextValue['dispatch'] {
  const ctx = useContext(RoomEventsContext)
  if (!ctx) throw new Error('useRoomDispatch must be used inside RoomEventsProvider')
  return ctx.dispatch
}

/** Register a handler for live thread-reply events; returns an unsubscribe fn. */
export function useRegisterThreadReplyHandler(): RoomEventsContextValue['registerThreadReplyHandler'] {
  const ctx = useContext(RoomEventsContext)
  if (!ctx) throw new Error('useRegisterThreadReplyHandler must be used inside RoomEventsProvider')
  return ctx.registerThreadReplyHandler
}

/** Register a handler for live thread-message edits/deletes; returns an unsubscribe fn. */
export function useRegisterThreadMessageMutationHandler(): RoomEventsContextValue['registerThreadMessageMutationHandler'] {
  const ctx = useContext(RoomEventsContext)
  if (!ctx) throw new Error('useRegisterThreadMessageMutationHandler must be used inside RoomEventsProvider')
  return ctx.registerThreadMessageMutationHandler
}

/** Section descriptor returned by `useSidebarSections`. `key` is the section
 *  id (a built-in id or a custom-section id — the collapse + React key). */
export interface SidebarSection {
  key: string
  title: string
  builtIn: boolean
  sortMode: ChatlistSortMode
  rooms: RoomSummary[]
}

/**
 * Derive the grouped sidebar (Custom Sections v2 read model): Favorites ·
 * Apps · Teams · each custom section · Chats, all derived from the per-room
 * subscription flags (favorite / bot / sectionId) joined with the section
 * definitions overlay (`state.chatlist`). Orphan-tolerant (a sectionId
 * pointing at a deleted section renders in Chats). The pure grouping + sort
 * lives in `lib/chatlist.deriveSidebarSections`; here we only join state and
 * enrich each room with its subscription's display name + hrInfo.
 */
export function useSidebarSections(): SidebarSection[] {
  const { state } = useRoomEventsInternal()
  const { summaries, subscriptions, chatlist, previews } = state
  return useMemo(() => {
    const enrich = (room: RoomSummary): RoomSummary => {
      const sub = subscriptions[room.id]
      const preview = previews[room.id]
      if (!sub && !preview) return room
      return {
        ...room,
        subscriptionName: sub?.name ?? room.subscriptionName,
        hrInfo: sub?.hrInfo ?? room.hrInfo,
        preview,
      }
    }
    const sections = deriveSidebarSections(summaries, subscriptions, chatlist) as SidebarSection[]
    return sections.map((s) => ({ ...s, rooms: s.rooms.map(enrich) }))
  }, [summaries, subscriptions, chatlist, previews])
}

/** Raw overlay section order (the full list the backend stores — built-ins +
 *  customs). The reorder RPC requires its argument to be a PERMUTATION of this
 *  full list, so the reorder UI moves a section within it rather than
 *  reconstructing an order from the derived (built-in-normalised) sections. */
export function useChatlistSectionOrder(): string[] {
  const { state } = useRoomEventsInternal()
  return state.chatlist?.sectionOrder ?? []
}

/** Imperative chatlist-section actions for the sidebar UI. Each wraps its api
 *  op and applies the returned overlay locally (CHATLIST_UPDATED) — the op's
 *  own event fans to the user's OTHER devices, so the caller must apply its
 *  own result. `moveChatTo` relies on the section_moved event to update state. */
export function useChatlistActions() {
  const nats = useNats()
  const dispatch = useRoomDispatch()
  return useMemo(() => {
    const apply = (chatlist: ChatlistState) => dispatch({ type: 'CHATLIST_UPDATED', chatlist })
    return {
      createSection: async (name: string, sortMode?: ChatlistSortMode) =>
        apply(await createChatlistSection(nats, { name, sortMode })),
      renameSection: async (sectionId: string, name: string) =>
        apply(await renameChatlistSection(nats, { sectionId, name })),
      deleteSection: async (sectionId: string) => apply(await deleteChatlistSection(nats, sectionId)),
      reorderSections: async (sectionOrder: string[]) =>
        apply(await reorderChatlistSections(nats, sectionOrder)),
      setSortMode: async (sectionId: string, sortMode: ChatlistSortMode) =>
        apply(await setChatlistSectionSortMode(nats, { sectionId, sortMode })),
      moveChatTo: async (
        roomId: string,
        siteId: string,
        sectionId: string | null,
        afterRoomId?: string,
        beforeRoomId?: string,
      ) => {
        await moveChat(nats, { roomId, siteId, sectionId, afterRoomId, beforeRoomId })
      },
    }
  }, [nats, dispatch])
}

/**
 * Read the current user's Subscription record for a single room.
 *
 * Returns the full `model.Subscription` (roles, alert, hasMention,
 * lastSeenAt, hrInfo, …) so components don't have to re-fetch via
 * `member.list` or re-derive permission from message events. Returns
 * `undefined` until the bucket bootstrap completes or a
 * `subscription.update added` event lands.
 *
 * Re-render scope: every consumer re-renders on ANY RoomEventsContext
 * state change — this hook just reads through the context value.
 * Memoise downstream (`useMemo(() => sub.roles.includes('owner'),
 * [sub])`) if you only care about a derived bit. A future perf pass
 * could split this into a dedicated SubscriptionsContext or wire it
 * through `useSyncExternalStore` selectors — out of scope today.
 */
export function useSubscription(roomId: string | null | undefined): DMSubscription | undefined {
  const { state } = useRoomEventsInternal()
  return roomId ? state.subscriptions[roomId] : undefined
}

export type { RoomEventsState, RoomSummary, RoomPreview, RoomBufferState, RoomEventsContextValue }
