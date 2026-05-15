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
import { BUFFER_MODE, initialState, roomEventsReducer, selectUnreadTotal } from './reducer'
import { useRoomSubscriptions } from './useRoomSubscriptions'
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
  favoriteIds: Set<string>
  appIds: Set<string>
  channelDmIds: Set<string>
  /** Keyed by roomId. Typed `DMSubscription` (Subscription ∪ hrInfo?)
   *  so consumers reading the map for either channel or DM rooms see
   *  hrInfo as optional without narrowing. */
  subscriptions: Record<string, DMSubscription>
}

/** Surface exposed via React context. Components consume via
 *  `useRoomEvents` / `useRoomSummaries` / `useSubscription` / etc. */
interface RoomEventsContextValue {
  state: RoomEventsState
  dispatch: Dispatch<{ type: string; [k: string]: unknown }>
  loadHistory: (roomId: string) => Promise<unknown> | void
  setActiveRoom: (roomId: string | null) => void
  jumpToMessage: (roomId: string, messageId: string) => Promise<void> | void
  resetToLiveTail: (roomId: string) => void
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
  const [state, dispatch] = useReducer(roomEventsReducer, initialState) as unknown as [
    RoomEventsState,
    Dispatch<{ type: string; [k: string]: unknown }>,
  ]

  const inflightHistory = useRef(new Map<string, Promise<unknown>>())
  const stateRef = useRef(state)
  stateRef.current = state

  // The hook owns the generation counter that gates stale dispatches
  // from this provider's async callbacks below. It also reads
  // `stateRef.current.activeRoomId` / `.summaries` from inside the
  // long-lived subscription callbacks to fire `markRoomRead` when
  // messages arrive in the currently-active room.
  const { currentGeneration } = useRoomSubscriptions(nats, dispatch, stateRef)

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
      // Mark-read fire-and-forget when the user opens a room. siteId
      // comes from the summary if we have one (cross-site DMs etc.);
      // otherwise fall back to the user's home site.
      if (roomId) {
        const summary = stateRef.current.summaries.find((r) => r.id === roomId)
        const siteId = summary?.siteId ?? user.siteId
        markRoomRead(nats, { roomId, siteId })
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
    () => ({ state, dispatch, loadHistory, setActiveRoom, jumpToMessage, resetToLiveTail }),
    [state, dispatch, loadHistory, setActiveRoom, jumpToMessage, resetToLiveTail],
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
 * Aggregate unread badge state for the whole app.
 *
 * Returns `{ total, hasMention }` derived from `state.summaries`. Updates
 * live: every `MESSAGE_RECEIVED` (count up for non-active rooms) and
 * `SET_ACTIVE_ROOM` (count down — room visited) already mutates
 * `summaries`, which re-renders this consumer. No fetch, no polling.
 */
export function useUnreadTotal(): { total: number; hasMention: boolean } {
  const { state } = useRoomEventsInternal()
  return useMemo(() => selectUnreadTotal(state.summaries), [state.summaries])
}

export function useRoomDispatch(): RoomEventsContextValue['dispatch'] {
  const ctx = useContext(RoomEventsContext)
  if (!ctx) throw new Error('useRoomDispatch must be used inside RoomEventsProvider')
  return ctx.dispatch
}

/** Section descriptor returned by `useSidebarSections`. */
export interface SidebarSection {
  key: 'favorite' | 'apps' | 'channelDm'
  title: string
  rooms: RoomSummary[]
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
    for (const room of summaries) {
      if (favoriteIds.has(room.id)) favorite.push(enrich(room))
      else if (appIds.has(room.id)) apps.push(enrich(room))
      else if (channelDmIds.has(room.id)) channelDm.push(enrich(room))
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
