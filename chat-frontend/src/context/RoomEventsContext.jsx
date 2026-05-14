import { createContext, useCallback, useContext, useEffect, useMemo, useReducer, useRef } from 'react'
import { useNats } from './NatsContext'
import { BUFFER_MODE, initialState, roomEventsReducer } from '../lib/roomEventsReducer'
import {
  msgHistory,
  msgSurrounding,
  roomEvent,
  roomsGet,
  roomsList,
  subscriptionUpdate,
  roomMetadataUpdate,
  userRoomEvent,
  userSubscriptionGetCurrent,
  userSubscriptionGetApps,
  userSubscriptionGetRooms,
} from '../lib/subjects'

const RoomEventsContext = createContext(null)

export function RoomEventsProvider({ children }) {
  const { user, request, subscribe } = useNats()
  const [state, dispatch] = useReducer(roomEventsReducer, initialState)
  const inflightHistory = useRef(new Map())
  const stateRef = useRef(state)
  stateRef.current = state

  const channelSubs = useRef(new Map())
  const cancelledRef = useRef(false)
  const generationRef = useRef(0)

  useEffect(() => {
    if (!user) return
    cancelledRef.current = false
    const generation = ++generationRef.current

    const safeDispatch = (action) => {
      if (cancelledRef.current) return
      dispatch(action)
    }

    const dmSub = subscribe(userRoomEvent(user.account), (evt) => {
      if (evt?.type === 'new_message') {
        safeDispatch({ type: 'MESSAGE_RECEIVED', event: evt })
      }
    })

    const openChannelSub = (roomId) => {
      if (channelSubs.current.has(roomId)) return
      const subj = roomEvent(roomId)
      const sub = subscribe(subj, (evt) => {
        if (evt?.type === 'new_message') {
          const hasMention = (evt.mentions ?? []).some(
            (p) => p.account === user.account
          )
          safeDispatch({ type: 'MESSAGE_RECEIVED', event: { ...evt, hasMention } })
        }
      })
      channelSubs.current.set(roomId, sub)
    }

    const closeChannelSub = (roomId) => {
      const sub = channelSubs.current.get(roomId)
      if (sub) {
        sub.unsubscribe()
        channelSubs.current.delete(roomId)
      }
    }

    const subUpdate = subscribe(subscriptionUpdate(user.account), (evt) => {
      if (cancelledRef.current) return
      if (evt.action === 'added' && evt.subscription?.roomId) {
        request(roomsGet(user.account, evt.subscription.roomId), {})
          .then((room) => {
            if (cancelledRef.current || !room) return
            // DM rooms have no canonical Room.Name server-side — the friendly
            // text lives on the user's Subscription. Stash that here so the
            // sidebar + header can fall back to it via roomDisplayName(room).
            const merged = evt.subscription?.name
              ? { ...room, subscriptionName: evt.subscription.name }
              : room
            safeDispatch({ type: 'ROOM_ADDED', room: merged })
            if (room.type === 'channel') openChannelSub(room.id)
          })
          .catch(() => {})
      } else if (evt.action === 'removed') {
        const roomId = evt.subscription?.roomId
        if (!roomId) return
        closeChannelSub(roomId)
        safeDispatch({ type: 'ROOM_REMOVED', roomId })
      }
    })

    const metaUpdate = subscribe(roomMetadataUpdate(user.account), (evt) => {
      safeDispatch({
        type: 'ROOM_METADATA_UPDATED',
        roomId: evt.roomId,
        name: evt.name,
        userCount: evt.userCount,
        lastMsgAt: evt.lastMsgAt,
      })
    })

    request(roomsList(user.account), {})
      .then((resp) => {
        if (cancelledRef.current) return
        const rooms = resp.rooms ?? []
        safeDispatch({ type: 'ROOMS_LOADED', rooms })
        for (const r of rooms) {
          if (r.type === 'channel') openChannelSub(r.id)
        }
      })
      .catch((err) => {
        if (!cancelledRef.current) safeDispatch({ type: 'ROOMS_FAILED', error: err.message })
      })

    Promise.all([
      request(userSubscriptionGetCurrent(user.account, user.siteId), { favorite: true }),
      request(userSubscriptionGetApps(user.account, user.siteId), {}),
      request(userSubscriptionGetRooms(user.account, user.siteId), {}),
    ])
      .then(([favResp, appResp, roomResp]) => {
        if (cancelledRef.current) return
        const subscriptionData = {}
        const collect = (resp) => {
          for (const s of resp?.subscriptions ?? []) {
            if (!s?.roomId) continue
            // Later sources overwrite earlier ones, but the three responses
            // describe the same Subscription record so collisions are benign.
            subscriptionData[s.roomId] = { name: s.name, hrInfo: s.hrInfo }
          }
        }
        collect(favResp)
        collect(appResp)
        collect(roomResp)
        safeDispatch({
          type: 'BUCKETS_LOADED',
          favoriteIds: (favResp?.subscriptions ?? []).map((s) => s.roomId),
          appIds: (appResp?.subscriptions ?? []).map((s) => s.roomId),
          channelDmIds: (roomResp?.subscriptions ?? []).map((s) => s.roomId),
          subscriptionData,
        })
      })
      .catch((err) => {
        // Bucket RPC failure is non-fatal — sidebar grouping degrades but rooms
        // still render via the existing roomsList path.
        console.warn('sidebar bucket bootstrap failed:', err.message)
      })

    return () => {
      cancelledRef.current = true
      dmSub.unsubscribe()
      subUpdate.unsubscribe()
      metaUpdate.unsubscribe()
      for (const sub of channelSubs.current.values()) sub.unsubscribe()
      channelSubs.current.clear()
      dispatch({ type: 'RESET' })   // RESET runs even when cancelled — it's the cleanup itself
    }
  }, [user, subscribe, request])

  const loadHistory = useCallback(
    async (roomId) => {
      if (!user || !roomId) return
      const prev = stateRef.current.roomState[roomId]
      if (prev?.hasLoadedHistory) return
      if (inflightHistory.current.has(roomId)) return inflightHistory.current.get(roomId)

      const gen = generationRef.current
      const promise = (async () => {
        try {
          const resp = await request(msgHistory(user.account, roomId, user.siteId), { limit: 50 })
          const asc = [...(resp.messages ?? [])].reverse()
          if (generationRef.current === gen) dispatch({ type: 'HISTORY_LOADED', roomId, messages: asc })
        } catch (err) {
          if (generationRef.current === gen) dispatch({ type: 'HISTORY_FAILED', roomId, error: err.message })
          throw err
        } finally {
          inflightHistory.current.delete(roomId)
        }
      })()
      inflightHistory.current.set(roomId, promise)
      return promise
    },
    [user, request]
  )

  const setActiveRoom = useCallback((roomId) => {
    dispatch({ type: 'SET_ACTIVE_ROOM', roomId })
  }, [])

  const jumpToMessage = useCallback(
    async (roomId, messageId) => {
      if (!user || !roomId || !messageId) return
      const summary = stateRef.current.summaries.find((r) => r.id === roomId)
      const siteId = summary?.siteId ?? user.siteId
      const gen = generationRef.current
      try {
        const resp = await request(msgSurrounding(user.account, roomId, siteId), { messageId })
        if (generationRef.current !== gen) return
        const messages = resp.messages ?? []
        dispatch({
          type: 'REPLACE_ROOM_BUFFER',
          roomId,
          messages,
          focusMessageId: messageId,
        })
      } catch (err) {
        if (generationRef.current === gen) {
          dispatch({ type: 'HISTORY_FAILED', roomId, error: err.message })
        }
        throw err
      }
    },
    [user, request]
  )

  const resetToLiveTail = useCallback((roomId) => {
    if (!roomId) return
    dispatch({ type: 'RESET_TO_LIVE_TAIL', roomId })
  }, [])

  const value = useMemo(
    () => ({ state, loadHistory, setActiveRoom, jumpToMessage, resetToLiveTail }),
    [state, loadHistory, setActiveRoom, jumpToMessage, resetToLiveTail]
  )

  return <RoomEventsContext.Provider value={value}>{children}</RoomEventsContext.Provider>
}

function useRoomEventsInternal() {
  const ctx = useContext(RoomEventsContext)
  if (!ctx) throw new Error('RoomEvents hooks must be used inside RoomEventsProvider')
  return ctx
}

export function useRoomEvents(roomId) {
  const { state, loadHistory, jumpToMessage, resetToLiveTail } = useRoomEventsInternal()
  const room = state.roomState[roomId]
  const load = useCallback(() => loadHistory(roomId), [loadHistory, roomId])
  const jump = useCallback(
    (messageId) => jumpToMessage(roomId, messageId),
    [jumpToMessage, roomId]
  )
  const reset = useCallback(() => resetToLiveTail(roomId), [resetToLiveTail, roomId])
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
    }),
    [room, load, jump, reset]
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

export function useSidebarSections() {
  const { state } = useRoomEventsInternal()
  const { summaries, favoriteIds, appIds, channelDmIds, subscriptionData } = state
  return useMemo(() => {
    // Merge per-room subscription metadata (name + hrInfo) from the
    // user-service RPCs onto each summary so roomDisplayName can render
    // subscription.Name for channels/botDM/discussion and HRInfo for DMs.
    const enrich = (room) => {
      const data = subscriptionData[room.id]
      if (!data) return room
      return {
        ...room,
        subscriptionName: data.name ?? room.subscriptionName,
        hrInfo: data.hrInfo ?? room.hrInfo,
      }
    }
    const favorite = []
    const apps = []
    const channelDm = []
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
  }, [summaries, favoriteIds, appIds, channelDmIds, subscriptionData])
}
