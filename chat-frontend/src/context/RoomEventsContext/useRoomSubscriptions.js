import { useEffect, useMemo, useRef } from 'react'
import {
  fetchSidebarBuckets,
  markRoomRead,
  subscribeToRoomEvents,
  subscribeToRoomMetadataUpdates,
  subscribeToSubscriptionUpdates,
  subscribeToUserRoomEvents,
} from '@/api'

/** Trailing-debounce window for the active-room mark-read RPC. 500ms
 *  collapses a burst of "10 msg/sec" room chatter into ONE RPC at the
 *  trailing edge. Long enough that bursts coalesce; short enough that
 *  the server's lastSeenAt for the active user stays current. */
const MARK_READ_DEBOUNCE_MS = 500

/**
 * Owns every backend subscription + the initial-room-list fetch that
 * keeps RoomEventsContext.state in sync with the server, AND owns the
 * connection-cycle generation counter that the provider's async
 * callbacks consult to drop late dispatches.
 *
 * The effect runs once per real login cycle — depending on `user`
 * only. The `nats` context value is captured via a ref that the hook
 * keeps current on every render. Why: NatsContext's memoised value
 * flips identity on every `connected` / `error` change (e.g. a
 * transient disconnect notice on line 67 of NatsContext.jsx), and
 * including `nats` in the dep array would tear down all four
 * subscriptions + dispatch RESET on every flicker. The user identity
 * is stable for the session (only `connectToNats` writes it), so
 * gating on `[user]` rebuilds subs only when login actually changes.
 *
 * Behaviour:
 *   - On `user` flip from null to truthy: open four subscriptions
 *     (DM events, per-channel events, subscription.update,
 *     room.metadata.update) and fire listRooms() once.
 *   - On `user` flip back to null (logout): tear every subscription
 *     down, dispatch RESET, bump cancellation.
 *
 * Dispatch guard: `safeDispatch` + cancellationRef stop a late
 * resolving promise from writing to a torn-down reducer.
 *
 * Returns `{ currentGeneration }` — a stable getter the provider's
 * `loadHistory` / `jumpToMessage` use to detect "I started in
 * generation N, but generation is N+1 by the time I resolved — drop
 * this dispatch." Keeps the generation ref encapsulated in the hook
 * instead of threading it across the module boundary.
 *
 * The `stateRef` parameter is the provider's `useRef(state)` mirror —
 * the hook reads `stateRef.current.activeRoomId` + `summaries` from
 * inside long-lived subscription callbacks to decide whether to fire
 * a `markRoomRead` RPC on incoming messages.
 *
 * @param {(input: { roomId: string; version: number; nonceB64: string; ciphertextB64: string }) => Promise<string | null>} [decrypt]
 *   Room-message decryption function from RoomKeysContext. Defaults to a
 *   no-op that always returns null (pass-through: encrypted events reach
 *   the reducer's placeholder branch unchanged).
 * @param {(roomId: string, version: number, siteId: string) => Promise<boolean>} [ensureKey]
 *   On-demand key fetch from RoomKeysContext. Called once when the initial
 *   decrypt returns null; if it resolves true, decrypt is retried once.
 *   Defaults to a no-op that always returns false (no retry).
 * @param {(entries: { roomId: string; version: number; privateKey: string }[]) => void} [seedKeys]
 *   Bulk key-seed from RoomKeysContext. Called once after BUCKETS_LOADED with
 *   the room keys `subscription.list` delivered inline, so the first message
 *   in each encrypted room decrypts without a placeholder or an on-demand
 *   fetch. Defaults to a no-op.
 */
export function useRoomSubscriptions(
  nats,
  dispatch,
  stateRef,
  threadReplyHandlerRef,
  threadMessageMutationHandlerRef,
  decrypt = async () => null,
  ensureKey = async () => false,
  seedKeys = () => {},
) {
  const { user } = nats
  // Keep a live ref to `nats` so long-lived subscription callbacks see the
  // latest connection without forcing the effect to re-run.
  const natsRef = useRef(nats)
  natsRef.current = nats

  // Keep a live ref to `decrypt` so subscription callbacks always use
  // the latest version without restarting the effect.
  const decryptRef = useRef(decrypt)
  decryptRef.current = decrypt

  // Keep a live ref to `ensureKey` for the same reason.
  const ensureKeyRef = useRef(ensureKey)
  ensureKeyRef.current = ensureKey

  // Keep a live ref to `seedKeys` for the same reason.
  const seedKeysRef = useRef(seedKeys)
  seedKeysRef.current = seedKeys

  // Bumped on every login (re)cycle so the provider's async fetch
  // callbacks can detect stale-generation dispatches.
  const generationRef = useRef(0)

  // Channel subscriptions live in a ref so subscriptionUpdate's
  // "added" branch (which opens them) and the cleanup (which closes
  // them) can both reach the same map without re-creating the effect.
  const channelSubs = useRef(new Map())
  const cancelledRef = useRef(false)

  // Trailing-edge debounce for the per-active-room mark-read RPC.
  // A chatty room (10+ msg/sec) would otherwise generate one
  // `message.read` RPC per inbound message; with this debounce a
  // burst coalesces to a single trailing call after the room goes
  // quiet for MARK_READ_DEBOUNCE_MS. The setActiveRoom path
  // (provider-side) stays immediate — that's the explicit user
  // action and not coalescable.
  const pendingMarkReadRef = useRef(null)
  const markReadTimeoutRef = useRef(null)

  useEffect(() => {
    if (!user) return
    cancelledRef.current = false
    generationRef.current += 1

    // Capture the nats value at effect-run time for the one-shot
    // listRooms() below. Long-lived callbacks read natsRef.current
    // directly so they pick up a fresh nc after a reconnect.
    const liveNats = natsRef.current

    const safeDispatch = (action) => {
      if (cancelledRef.current) return
      dispatch(action)
    }

    // Schedule a trailing `message.read` for the active room with a
    // 500ms debounce. A burst of N messages in a chatty room produces
    // ONE RPC at the end of the burst instead of N. If the user
    // switches rooms before the timer fires, the active-room check at
    // fire time skips the stale entry.
    // NOTE: own messages are NOT skipped. Unread is derived server-side
    // as lastMsgAt > lastSeenAt; sending advances Room.lastMsgAt but not
    // the sender's lastSeenAt, so an own message in the active room must
    // still mark the room read or the badge counts the room you're in.
    const scheduleMarkActiveRead = (evtRoomId) => {
      if (!evtRoomId) return
      if (stateRef.current.activeRoomId !== evtRoomId) return
      const summary = stateRef.current.summaries.find((r) => r.id === evtRoomId)
      const siteId = summary?.siteId ?? user.siteId
      // Clear any prior pending timer FIRST, then write the new pending
      // entry. Defensive ordering: if future code ever introduces async
      // work between these two lines, the prior timer can't race with
      // the new pending entry it was never meant to operate on.
      if (markReadTimeoutRef.current) clearTimeout(markReadTimeoutRef.current)
      pendingMarkReadRef.current = { roomId: evtRoomId, siteId }
      markReadTimeoutRef.current = setTimeout(() => {
        markReadTimeoutRef.current = null
        const pending = pendingMarkReadRef.current
        pendingMarkReadRef.current = null
        if (!pending) return
        // Re-check: only fire if the pending room is still the active
        // room. Mid-burst room switch would otherwise misfire a
        // mark-read for a room the user has already left.
        if (cancelledRef.current) return
        if (stateRef.current.activeRoomId !== pending.roomId) return
        markRoomRead(natsRef.current, pending).then((ok) => {
          if (ok) safeDispatch({ type: 'ROOM_READ_SYNCED' })
        })
      }, MARK_READ_DEBOUNCE_MS)
    }

    // Fan an edit/delete mutation into ThreadEvents (so the open thread, if any,
    // updates the message too). Room reducer dispatch happens separately below.
    const fanThreadMutation = (mut) => {
      const handler = threadMessageMutationHandlerRef?.current
      if (!handler) return
      try {
        handler(mut)
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn('thread-mutation handler threw:', err?.message ?? err, mut)
      }
    }

    // Translate a wire-level event to room+thread dispatches for edit/delete.
    const handleMutationEvent = (evt) => {
      if (evt?.type === 'message_edited' && evt.messageId) {
        const { messageId, newContent, editedAt } = evt
        // Drop edits without a plaintext body. Encrypted channel rooms emit
        // `encryptedNewContent` instead; blanking the existing content to ''
        // would silently wipe the message until decryption is implemented.
        if (typeof newContent !== 'string') return true
        const editedAtIso =
          typeof editedAt === 'string' ? editedAt : new Date(editedAt ?? Date.now()).toISOString()
        safeDispatch({
          type: 'MESSAGE_EDITED',
          roomId: evt.roomId,
          messageId,
          content: newContent,
          editedAt: editedAtIso,
        })
        fanThreadMutation({ kind: 'edited', messageId, content: newContent, editedAt: editedAtIso })
        return true
      }
      if (evt?.type === 'message_deleted' && evt.messageId) {
        const { messageId } = evt
        safeDispatch({ type: 'MESSAGE_DELETED', roomId: evt.roomId, messageId })
        fanThreadMutation({ kind: 'deleted', messageId })
        return true
      }
      if (evt?.type === 'message_reacted' && evt.messageId && evt.shortcode) {
        // ReactRoomEvent: {messageId, shortcode, action: added|removed, actor}.
        safeDispatch({
          type: 'MESSAGE_REACTED',
          roomId: evt.roomId,
          messageId: evt.messageId,
          shortcode: evt.shortcode,
          action: evt.action,
          account: evt.actor?.account,
          displayName: evt.actor?.engName || evt.actor?.account,
        })
        return true
      }
      return false
    }

    // Fan thread-reply events to ThreadEvents; no-op if no consumer is registered.
    const fanThreadReply = (evt) => {
      const msg = evt?.message
      if (!msg?.threadParentMessageId) return
      const handler = threadReplyHandlerRef?.current
      if (!handler) return
      try {
        handler({
          parentMessageId: msg.threadParentMessageId,
          roomId: evt.roomId,
          siteId: evt.siteId,
          message: msg,
        })
      } catch (err) {
        // Don't let a handler exception break the subscription callback.
        // eslint-disable-next-line no-console
        console.warn(
          'thread-reply handler threw:',
          err?.message ?? err,
          { roomId: evt.roomId, parentMessageId: msg.threadParentMessageId },
        )
      }
    }

    // Per-room dispatch chains. Each entry is a Promise representing the
    // most recent in-flight work for that room. New events for the same
    // room chain off it via .then(fn, fn) so they observe the same order
    // they arrived in even when some are encrypted (await deriveAesKey +
    // GCM.open) and others are plaintext (synchronous). Without this,
    // a plaintext mutation event can finalize before a prior encrypted
    // new_message resolves, scrambling the message-list order.
    const dispatchChains = new Map()
    const enqueueByRoom = (roomId, work) => {
      if (!roomId) {
        work()
        return
      }
      const prev = dispatchChains.get(roomId) ?? Promise.resolve()
      const next = prev.then(work, work)
      dispatchChains.set(roomId, next)
    }

    // Decrypt encrypted fields on an event, then call finalize(decoded).
    // Handles two cases:
    //   1. encryptedMessage (new_message with no plaintext body yet)
    //   2. encryptedNewContent (edit events in encrypted rooms)
    // When the initial decrypt returns null, ensureKey is called once; if it
    // resolves true, decrypt is retried. On persistent null the event falls
    // through unchanged and the reducer's placeholder branch handles it.
    const decryptAndDispatch = async (evt, finalize) => {
      let decoded = evt
      try {
        // Handle encrypted full-message events.
        if (decoded.encryptedMessage && !decoded.message) {
          const enc = decoded.encryptedMessage
          if (typeof enc.version === 'number' && enc.nonce && enc.ciphertext) {
            let plaintext = await decryptRef.current({
              roomId: decoded.roomId,
              version: enc.version,
              nonceB64: enc.nonce,
              ciphertextB64: enc.ciphertext,
            })
            if (plaintext == null && decoded.roomId) {
              const siteId = decoded.siteId ?? natsRef.current.user?.siteId
              if (siteId) {
                const ok = await ensureKeyRef.current(decoded.roomId, enc.version, siteId)
                if (ok) {
                  plaintext = await decryptRef.current({
                    roomId: decoded.roomId,
                    version: enc.version,
                    nonceB64: enc.nonce,
                    ciphertextB64: enc.ciphertext,
                  })
                }
              }
            }
            if (plaintext != null) {
              const msg = JSON.parse(plaintext)
              decoded = { ...decoded, message: msg, encryptedMessage: undefined }
            }
          }
        }
        // Handle encrypted message edits (flattened edit event).
        if (decoded.type === 'message_edited' && decoded.encryptedNewContent && !decoded.newContent) {
          const enc = decoded.encryptedNewContent
          if (typeof enc.version === 'number' && enc.nonce && enc.ciphertext) {
            let plaintext = await decryptRef.current({
              roomId: decoded.roomId,
              version: enc.version,
              nonceB64: enc.nonce,
              ciphertextB64: enc.ciphertext,
            })
            if (plaintext == null && decoded.roomId) {
              const siteId = decoded.siteId ?? natsRef.current.user?.siteId
              if (siteId) {
                const ok = await ensureKeyRef.current(decoded.roomId, enc.version, siteId)
                if (ok) {
                  plaintext = await decryptRef.current({
                    roomId: decoded.roomId,
                    version: enc.version,
                    nonceB64: enc.nonce,
                    ciphertextB64: enc.ciphertext,
                  })
                }
              }
            }
            if (plaintext != null) {
              decoded = { ...decoded, newContent: plaintext, encryptedNewContent: undefined }
            }
          }
        }
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn('decryptAndDispatch failed; forwarding original event', err)
      }
      finalize(decoded)
    }

    const dmSub = subscribeToUserRoomEvents(liveNats, (evt) => {
      enqueueByRoom(evt?.roomId, () => {
        if (evt?.type === 'new_message') {
          return decryptAndDispatch(evt, (decoded) => {
            safeDispatch({ type: 'MESSAGE_RECEIVED', event: decoded })
            fanThreadReply(decoded)
            // Thread replies don't advance the main-feed lastSeenAt.
            if (!decoded.message?.threadParentMessageId) {
              scheduleMarkActiveRead(decoded.roomId)
            }
          })
        }
        handleMutationEvent(evt)
      })
    })

    const openChannelSub = (roomId) => {
      if (channelSubs.current.has(roomId)) return
      const sub = subscribeToRoomEvents(natsRef.current, { roomId }, (evt) => {
        enqueueByRoom(evt?.roomId ?? roomId, () => {
          if (evt?.type === 'new_message') {
            return decryptAndDispatch(evt, (decoded) => {
              const hasMention = (decoded.mentions ?? []).some(
                (p) => p.account === user.account
              )
              const normalized = { ...decoded, hasMention }
              safeDispatch({ type: 'MESSAGE_RECEIVED', event: normalized })
              fanThreadReply(normalized)
              // See dm path above — skip main-feed mark-read for thread replies.
              if (!decoded.message?.threadParentMessageId) {
                scheduleMarkActiveRead(decoded.roomId ?? roomId)
              }
            })
          }
          if (evt?.type === 'message_edited') {
            return decryptAndDispatch(evt, (decoded) => {
              handleMutationEvent(decoded)
            })
          }
          handleMutationEvent(evt)
        })
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

    const subUpdate = subscribeToSubscriptionUpdates(liveNats, (evt) => {
      if (cancelledRef.current) return
      if (evt.action === 'added' && evt.subscription?.roomId) {
        // Store the full subscription record FIRST so any consumer that
        // wakes up on the ROOM_ADDED dispatch already sees fresh roles /
        // hasMention / alert state. The full payload is what room-worker
        // emits on `subscription.update`.
        safeDispatch({ type: 'SUBSCRIPTION_UPSERTED', subscription: evt.subscription })
        // Build the sidebar room straight from the subscription record — it
        // carries roomId, roomType, siteId, and the per-user friendly name.
        // Room-level metadata (userCount, lastMsgAt) is absent here and lands
        // via subsequent ROOM_METADATA_UPDATED / MESSAGE_RECEIVED events.
        const sub = evt.subscription
        const room = {
          id: sub.roomId,
          type: sub.roomType,
          siteId: sub.siteId,
          name: sub.name,
          subscriptionName: sub.name,
        }
        safeDispatch({ type: 'ROOM_ADDED', room })
        if (sub.roomType === 'channel') openChannelSub(sub.roomId)
      } else if (evt.action === 'removed') {
        const roomId = evt.subscription?.roomId
        if (!roomId) return
        closeChannelSub(roomId)
        safeDispatch({ type: 'ROOM_REMOVED', roomId })
      } else if (evt.subscription?.roomId) {
        // Catch-all for any other action that carries a subscription
        // payload. Today the backend emits `role_updated` (room-worker
        // handler.go:197); future actions (mute, favorite, mark-read)
        // will flow through the same branch once the backend wires
        // them. The reducer's SUBSCRIPTION_UPSERTED partial-merges so
        // a payload missing fields (e.g. only `roles`) won't drop
        // lastSeenAt / hasMention / alert from the prior record.
        safeDispatch({ type: 'SUBSCRIPTION_UPSERTED', subscription: evt.subscription })
      }
    })

    const metaUpdate = subscribeToRoomMetadataUpdates(liveNats, (evt) => {
      safeDispatch({
        type: 'ROOM_METADATA_UPDATED',
        roomId: evt.roomId,
        name: evt.name,
        userCount: evt.userCount,
        lastMsgAt: evt.lastMsgAt,
      })
    })

    // Bootstrap the sidebar via the three user-service subscription
    // RPCs (favorites / apps / channel+dm). Each reply embeds the room
    // metadata inline, so `buckets.rooms` is the canonical full list —
    // no separate `rooms.list` RPC is needed. Per-bucket failures
    // degrade to empty (fetchSidebarBuckets uses Promise.allSettled);
    // a total failure leaves the sidebar empty.
    fetchSidebarBuckets(liveNats)
      .then((buckets) => {
        if (cancelledRef.current) return
        safeDispatch({ type: 'BUCKETS_LOADED', ...buckets })
        // Seed room keys delivered inline on subscription.list so the first
        // message in each encrypted room decrypts immediately — no placeholder,
        // no on-demand key.get RPC. Rooms without a key (plaintext DMs, or no
        // key provisioned) simply aren't seeded.
        const keyEntries = []
        for (const sub of Object.values(buckets.subscriptions ?? {})) {
          const room = sub?.room
          if (room?.privateKey && typeof room.keyVersion === 'number') {
            keyEntries.push({ roomId: sub.roomId, version: room.keyVersion, privateKey: room.privateKey })
          }
        }
        if (keyEntries.length > 0) seedKeysRef.current(keyEntries)
        for (const r of buckets.rooms) {
          if (r.type === 'channel') openChannelSub(r.id)
        }
      })
      .catch((err) => {
        // eslint-disable-next-line no-console
        console.warn('sidebar bucket bootstrap failed:', err?.message ?? err)
      })

    return () => {
      cancelledRef.current = true
      dmSub.unsubscribe()
      subUpdate.unsubscribe()
      metaUpdate.unsubscribe()
      for (const sub of channelSubs.current.values()) sub.unsubscribe()
      channelSubs.current.clear()
      dispatchChains.clear()
      // Cancel any in-flight mark-read trailing timer so it doesn't
      // fire after teardown (would `markRoomRead` against a dead nc).
      if (markReadTimeoutRef.current) {
        clearTimeout(markReadTimeoutRef.current)
        markReadTimeoutRef.current = null
      }
      pendingMarkReadRef.current = null
      // RESET runs even when cancelled — it IS the cleanup.
      dispatch({ type: 'RESET' })
    }
    // `stateRef` is consumed only via `.current` — stable across renders
    // by construction; not in the dep array.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user, dispatch])

  // Memoised so the provider's downstream useMemo + useCallback that
  // depend on this value don't churn on every render.
  return useMemo(() => ({
    currentGeneration: () => generationRef.current,
  }), [])
}
