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
import { useUnreadCount as useUnreadCountQuery } from './useUnreadCount'
import { fetchMessageHistory, fetchSurroundingMessages, markRoomRead } from '@/api'
import type { DMSubscription, Message, Nats, Room, SubscriptionHRInfo } from '@/api/types'

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
  const { decrypt, ensureKey } = useRoomKeys()
  const [state, dispatch] = useReducer(roomEventsReducer, initialState) as unknown as [
    RoomEventsState,
    Dispatch<{ type: string; [k: string]: unknown }>,
  ]

  const inflightHistory = useRef(new Map<string, Promise<unknown>>())
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
  )

  const loadHistory = useCallback(
    async (roomId: string) => {
      if (!user || !roomId) return
      const prev = stateRef.current.roomState[roomId]
      if (prev?.hasLoadedHistory) return
      if (inflightHistory.current.has(roomId)) return inflightHistory.current.get(roomId)

      const gen = currentGeneration()
      const promise = (async () => {
        try {
          const resp = await fetchMessageHistory(nats, { roomId, siteId: user.siteId, limit: 50 })
          // history-service ships newest-first; the UI reads chronological.
          // Normalisation to the broadcast `Message` shape now happens inside
          // the api op.
          const asc = [...(resp.messages ?? [])].reverse()
          if (currentGeneration() === gen) dispatch({ type: 'HISTORY_LOADED', roomId, messages: asc })
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
  const { state, dispatch, loadHistory, jumpToMessage, resetToLiveTail } = useRoomEventsInternal()
  const room = roomId ? state.roomState[roomId] : undefined
  const load = useCallback(() => (roomId ? loadHistory(roomId) : undefined), [loadHistory, roomId])
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
      bufferMode: room?.bufferMode ?? BUFFER_MODE.LIVE,
      pendingCount: room?.pendingLiveMessages?.length ?? 0,
      focusMessageId: room?.focusMessageId ?? null,
      jumpToMessage: jump,
      resetToLiveTail: reset,
      dispatch,
    }),
    [room, load, jump, reset, dispatch],
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
 * Sourced from the `subscription.count` RPC (via `useUnreadCountQuery`),
 * not derived from `state.summaries`. Re-fetches whenever the active
 * room changes — opening/reading a room is when the server-side total
 * moves.
 */
export function useUnreadCount(): number {
  const nats = useNats() as unknown as Nats
  const { state } = useRoomEventsInternal()
  return useUnreadCountQuery(nats, state.readSeq, state.msgRecvSeq)
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

/** Section descriptor returned by `useSidebarSections`. */
export interface SidebarSection {
  key: 'favorite' | 'apps' | 'channelDm'
  title: string
  rooms: RoomSummary[]
  /** Disabled-section banner. Rendered in place of room items when the
   *  section is expanded — used today for the Favorite section while the
   *  end-to-end favorite path is unimplemented (`pkg/model.Subscription`
   *  has no Favorite field yet). */
  note?: string
}

/**
 * Partition the room summaries into the three sidebar buckets:
 * Favorite, Apps, Channels and DMs. Bucket membership comes from the
 * `BUCKETS_LOADED` dispatch (fetched by useRoomSubscriptions on login)
 * + the per-type ROOM_ADDED / ROOM_REMOVED maintenance the reducer
 * applies.
 *
 * Returns an ordered array of `{key, title, rooms}` sections so the
 * sidebar can render headers + rows without re-deriving the split.
 *
 * Per-room subscription metadata (subscription.name + hrInfo for DMs)
 * is merged onto each room here so `roomDisplayName` resolves the user's
 * preferred name for channels and the counterpart's hrInfo for DMs
 * (only present on DMSubscription records) without the underlying
 * summary structure carrying those fields directly.
 */
export function useSidebarSections(): SidebarSection[] {
  const { state } = useRoomEventsInternal()
  const { summaries, favoriteIds, appIds, channelDmIds, subscriptions } = state
  return useMemo(() => {
    const enrich = (room: RoomSummary): RoomSummary => {
      const sub = subscriptions[room.id]
      if (!sub) return room
      return {
        ...room,
        subscriptionName: sub.name ?? room.subscriptionName,
        hrInfo: sub.hrInfo ?? room.hrInfo,
      }
    }
    const favorite: RoomSummary[] = []
    const apps: RoomSummary[] = []
    const channelDm: RoomSummary[] = []
    // Cold-start safety: if all three bucket Sets are empty, the
    // subscription.list RPCs either failed or haven't landed yet.
    // subscription.list is independent and may have populated `summaries`
    // already; strict partitioning would drop every one of those
    // rooms and render an empty sidebar. Fall back to a flat list
    // under Channels and DMs so the user can still reach their rooms;
    // the next BUCKETS_LOADED dispatch re-partitions normally.
    const allBucketsEmpty =
      favoriteIds.size === 0 && appIds.size === 0 && channelDmIds.size === 0
    for (const room of summaries) {
      if (favoriteIds.has(room.id)) favorite.push(enrich(room))
      else if (appIds.has(room.id)) apps.push(enrich(room))
      else if (channelDmIds.has(room.id)) channelDm.push(enrich(room))
      else if (allBucketsEmpty) channelDm.push(enrich(room))
    }
    return [
      { key: 'favorite',  title: 'Favorite',          rooms: favorite },
      { key: 'apps',      title: 'Apps',              rooms: apps },
      { key: 'channelDm', title: 'Channels and DMs',  rooms: channelDm },
    ]
  }, [summaries, favoriteIds, appIds, channelDmIds, subscriptions])
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

export type { RoomEventsState, RoomSummary, RoomBufferState, RoomEventsContextValue }
