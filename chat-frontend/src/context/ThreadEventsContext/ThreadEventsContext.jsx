import { createContext, useCallback, useContext, useEffect, useReducer, useRef } from 'react'
import { v4 as uuidv4 } from 'uuid'
import { useNats } from '../NatsContext/NatsContext'
import {
  useRoomDispatch,
  useRegisterThreadReplyHandler,
  useRegisterThreadMessageMutationHandler,
} from '../RoomEventsContext/RoomEventsContext'
import { generateMessageID } from '@/lib/idgen'
import { useRoomKeys } from '@/context/RoomKeysContext'
import { fetchThreadMessages, sendMessage, subscribeToThreadEvents } from '@/api'
import { decryptRoomEvent } from '@/lib/decryptRoomEvent'
import { threadEventsReducer, initialState } from './reducer'

/** The body of a reply, or a placeholder when the room key has not arrived.
 *  Mirrors the room timeline's `[encrypted message]` fallback: silently
 *  dropping it leaves the panel missing a message with no explanation. */
function threadReplyMessage(evt) {
  const msg = evt.message
  if (msg?.id || !evt.encryptedMessage || !evt.lastMsgId) return msg
  return {
    id: evt.lastMsgId,
    roomId: evt.roomId,
    content: '[encrypted message]',
    createdAt: evt.lastMsgAt ?? new Date(evt.timestamp ?? Date.now()).toISOString(),
    encrypted: true,
  }
}

/** Mirrors the room lane's coercion so both lanes hand the reducer an ISO
 *  string regardless of what the wire carried. */
function normalizeEditedAt(editedAt) {
  return typeof editedAt === 'string' ? editedAt : new Date(editedAt ?? Date.now()).toISOString()
}

const ThreadEventsContext = createContext(null)

export function ThreadEventsProvider({ children }) {
  const nats = useNats()
  const { user } = nats
  const roomDispatch = useRoomDispatch()
  const { decrypt, ensureKey } = useRoomKeys()
  const registerThreadReplyHandler = useRegisterThreadReplyHandler()
  const registerThreadMessageMutationHandler = useRegisterThreadMessageMutationHandler()
  const [state, dispatch] = useReducer(threadEventsReducer, initialState)
  const generationRef = useRef(0)
  const threadSubRef = useRef(null)
  const decryptRef = useRef(decrypt)
  decryptRef.current = decrypt
  const ensureKeyRef = useRef(ensureKey)
  ensureKeyRef.current = ensureKey
  const stateRef = useRef(state)
  stateRef.current = state

  // Reset on logout.
  useEffect(() => {
    if (!user) dispatch({ type: 'RESET' })
  }, [user])

  // The three reducer actions are authored here once. Both the room-lane
  // bridges below and the thread-lane subscription in openThread feed them, so
  // a reply cannot mean two different things depending on which lane won.
  const applyReply = useCallback((parentId, message) => {
    dispatch({ type: 'THREAD_REPLY_RECEIVED', parentId, message })
  }, [])

  const applyMutation = useCallback((mut) => {
    if (mut.kind === 'edited') {
      // Drop edits without a plaintext body, matching the room lane: an
      // encrypted edit carries encryptedNewContent, and blanking to '' would
      // wipe the rendered message.
      if (typeof mut.content !== 'string') return
      dispatch({
        type: 'REPLY_EDITED',
        messageId: mut.messageId,
        content: mut.content,
        editedAt: mut.editedAt,
      })
    } else if (mut.kind === 'deleted') {
      dispatch({ type: 'REPLY_DELETED', messageId: mut.messageId })
    }
  }, [])

  // Bridge live room-channel thread replies → THREAD_REPLY_RECEIVED.
  useEffect(() => {
    const unsubscribe = registerThreadReplyHandler((evt) => {
      applyReply(evt.parentMessageId, evt.message)
    })
    return unsubscribe
  }, [registerThreadReplyHandler, applyReply])

  // Bridge live edit/delete mutations → REPLY_EDITED / REPLY_DELETED.
  useEffect(() => {
    const unsubscribe = registerThreadMessageMutationHandler(applyMutation)
    return unsubscribe
  }, [registerThreadMessageMutationHandler, applyMutation])

  // Drops the open panel's thread subscription. NATS reaps the interest, which
  // is what removes this client from the thread's delivery set.
  const closeThreadSub = useCallback(() => {
    threadSubRef.current?.unsubscribe()
    threadSubRef.current = null
  }, [])

  useEffect(() => closeThreadSub, [closeThreadSub])

  const openThread = useCallback(
    (parent) => {
      // Short-circuit if it's already the same parent (mirrors reducer guard).
      if (stateRef.current.activeParent?.messageId === parent.messageId) return
      const myGen = ++generationRef.current
      dispatch({ type: 'OPEN_THREAD', parent })
      closeThreadSub()
      if (!user) return
      // Subscribe before fetching: a reply landing in the gap would be lost,
      // and closing that gap is the whole point of the thread lane.
      let chain = Promise.resolve()
      threadSubRef.current = subscribeToThreadEvents(
        nats,
        { roomId: parent.roomId, parentMessageId: parent.messageId, crossSite: parent.crossSite },
        (raw) => {
          // Serialize within this thread: the NATS subscribe loop does not await
          // the callback, so without a chain a plaintext delete finalizes before
          // a prior encrypted create and lands on a message not yet in the list.
          // The chain is per subscription, so a decrypt stalled on a key fetch
          // cannot delay the thread the user opens next.
          const work = async () => {
            // Checked before decrypting too: a stale event should not hold the
            // chain for the length of a key fetch it will be discarded after.
            if (myGen !== generationRef.current) return
            // Channel rooms are encrypted, so this lane must decrypt exactly as
            // the room lane does or every reply arrives as an empty envelope.
            const evt = await decryptRoomEvent(raw, {
              decrypt: decryptRef.current,
              ensureKey: ensureKeyRef.current,
              fallbackSiteId: parent.siteId,
            })
            if (myGen !== generationRef.current) return
            if (evt?.type === 'new_thread_message') {
              applyReply(parent.messageId, threadReplyMessage(evt))
            } else if (evt?.type === 'message_edited') {
              applyMutation({
                kind: 'edited',
                messageId: evt.messageId,
                content: evt.newContent,
                editedAt: normalizeEditedAt(evt.editedAt),
              })
            } else if (evt?.type === 'message_deleted') {
              applyMutation({ kind: 'deleted', messageId: evt.messageId })
            }
          }
          chain = chain.then(work, work)
          return chain
        }
      )
      fetchThreadMessages(nats, {
        roomId: parent.roomId,
        siteId: parent.siteId,
        threadMessageId: parent.messageId,
        limit: 50,
      })
        .then((resp) => {
          if (myGen !== generationRef.current) return
          // The api op already normalises into broadcast shape (id/content);
          // hand it straight to the reducer.
          dispatch({ type: 'HISTORY_LOADED', parentId: parent.messageId, resp })
        })
        .catch((err) => {
          if (myGen !== generationRef.current) return
          dispatch({
            type: 'HISTORY_FAILED',
            parentId: parent.messageId,
            error: err?.message ?? String(err),
          })
        })
    },
    [user, nats, closeThreadSub, applyReply, applyMutation]
  )

  const closeThread = useCallback(() => {
    generationRef.current++
    closeThreadSub()
    dispatch({ type: 'CLOSE_THREAD' })
  }, [closeThreadSub])

  // publishReply is synchronous — `publish` is sync void and throws if not
  // connected. Callers wrap in try/catch.
  const publishReply = useCallback(
    (id, content, opts) => {
      const parent = stateRef.current.activeParent
      if (!parent || !user) throw new Error('no active thread')
      const payload = {
        id,
        content,
        requestId: uuidv4(),
        threadParentMessageId: parent.messageId,
        threadParentMessageCreatedAt: parent.createdAtMs,
      }
      if (opts?.quotedParentMessageId) payload.quotedParentMessageId = opts.quotedParentMessageId
      sendMessage(nats, { roomId: parent.roomId, siteId: parent.siteId, payload })
    },
    [user, nats]
  )

  const sendReply = useCallback(
    (content, opts) => {
      const parent = stateRef.current.activeParent
      if (!parent || !user || !content || !content.trim()) return
      const id = generateMessageID()
      const optimistic = {
        id,
        content: content.trim(),
        createdAt: new Date().toISOString(),
        sender: { account: user.account },
        threadParentMessageId: parent.messageId,
        threadParentMessageCreatedAt: new Date(parent.createdAtMs).toISOString(),
        _local: true,
      }
      if (opts?.quotedParentMessageId) {
        // Mirror the server's cassandra.QuotedParentMessage shape so the
        // optimistic snapshot renders identically to what later broadcasts
        // / history loads will carry (messageId / sender.engName / msg).
        const sn = opts.quotedSnapshot?.senderName
        optimistic.quotedParentMessage = {
          messageId: opts.quotedParentMessageId,
          sender: { engName: sn, account: sn },
          msg: opts.quotedSnapshot?.content,
        }
      }
      dispatch({ type: 'REPLY_SENT_LOCAL', message: optimistic })
      try {
        publishReply(id, content.trim(), opts)
        if (parent) {
          // replyId lets the room reducer dedupe the inbound echo on tcount.
          roomDispatch({
            type: 'OWN_THREAD_REPLY_SENT',
            roomId: parent.roomId,
            parentId: parent.messageId,
            replyId: id,
          })
        }
      } catch (err) {
        dispatch({ type: 'REPLY_SEND_FAILED', messageId: id, error: err?.message ?? String(err) })
      }
    },
    [user, publishReply, roomDispatch]
  )

  const retryReply = useCallback(
    (messageId) => {
      const row = stateRef.current.messages.find((m) => m.id === messageId)
      if (!row) return
      dispatch({ type: 'REPLY_RETRIED', messageId })
      try {
        publishReply(
          messageId,
          row.content,
          row.quotedParentMessage
            ? { quotedParentMessageId: row.quotedParentMessage.messageId ?? row.quotedParentMessage.id }
            : undefined
        )
        // Intentionally NOT dispatching OWN_THREAD_REPLY_SENT here. A retry is
        // the continuation of a logical send, not a new one; double-bumping the
        // parent's tcount would inflate the badge across repeated failures and
        // recoveries. The optimistic count stays at 0 until the next
        // msg.thread reload reconciles from the authoritative server state.
      } catch (err) {
        dispatch({ type: 'REPLY_SEND_FAILED', messageId, error: err?.message ?? String(err) })
      }
    },
    [publishReply]
  )

  const dismissReply = useCallback((messageId) => {
    dispatch({ type: 'REPLY_DISMISSED', messageId })
  }, [])

  const value = {
    activeParent: state.activeParent,
    messages: state.messages,
    hasLoadedHistory: state.hasLoadedHistory,
    historyLoading: state.historyLoading,
    historyError: state.historyError,
    openThread,
    closeThread,
    sendReply,
    retryReply,
    dismissReply,
    dispatch,
  }

  return <ThreadEventsContext.Provider value={value}>{children}</ThreadEventsContext.Provider>
}

export function useThreadEvents() {
  const ctx = useContext(ThreadEventsContext)
  if (!ctx) throw new Error('useThreadEvents must be used inside ThreadEventsProvider')
  return ctx
}
