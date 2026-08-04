import { useEffect, useRef, useState } from 'react'
import { useNats } from '@/context/NatsContext'
import { useRoomEvents } from '@/context/RoomEventsContext'
import { BUFFER_MODE } from '@/context/RoomEventsContext/reducer'
import { editMessage, deleteMessage } from '@/api'
import MessageList from '@/components/shared/MessageList/MessageList'
import DeleteConfirmDialog from '@/components/shared/DeleteConfirmDialog/DeleteConfirmDialog'
import TextInputDialog from '@/components/shared/TextInputDialog/TextInputDialog'
import './style.css'

export default function RoomMessageArea({ room, onThread, onReply }) {
  const nats = useNats()
  const { user } = nats
  const {
    messages,
    hasLoadedHistory,
    historyError,
    loadHistory,
    loadOlder,
    hasMoreOlder,
    loadingOlder,
    bufferMode,
    pendingCount,
    focusMessageId,
    resetToLiveTail,
    jumpToMessage,
    dispatch,
  } = useRoomEvents(room?.id ?? null)
  const bottomRef = useRef(null)
  const [editingMessage, setEditingMessage] = useState(null)
  const [pendingDelete, setPendingDelete] = useState(null)

  useEffect(() => { setEditingMessage(null); setPendingDelete(null) }, [room?.id])

  useEffect(() => {
    if (!room) return
    loadHistory().catch(() => {})
  }, [room, loadHistory])

  // Auto-scroll to the bottom on a NEW message (the last id changes) or the
  // initial load. Keyed on the last message's id — NOT the whole `messages`
  // array — so prepending an older page (which changes the FIRST id, not the
  // last) doesn't yank the user back down to the live tail while they read.
  const lastMsgId = messages.length ? messages[messages.length - 1].id : null
  useEffect(() => {
    if (bufferMode === BUFFER_MODE.HISTORICAL) return
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [lastMsgId, bufferMode])

  useEffect(() => {
    if (bufferMode === BUFFER_MODE.LIVE) {
      bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
    }
  }, [bufferMode])

  const handleEdit = (msg) => setEditingMessage(msg)
  const handleEditCancel = () => setEditingMessage(null)
  const handleEditSave = (newContent) => {
    if (!editingMessage) return
    // Server: EditMessageRequest{ MessageID, NewMsg }. No createdAt, no requestId.
    editMessage(nats, {
      roomId: room.id,
      siteId: user.siteId,
      payload: { messageId: editingMessage.id, newMsg: newContent },
    })
    dispatch({
      type: 'MESSAGE_EDITED_LOCAL',
      roomId: room.id,
      messageId: editingMessage.id,
      content: newContent,
      editedAt: new Date().toISOString(),
    })
    setEditingMessage(null)
  }

  const handleDelete = (msg) => setPendingDelete(msg)
  const handleDeleteCancel = () => setPendingDelete(null)
  const handleDeleteConfirm = () => {
    if (!pendingDelete) return
    // Server: DeleteMessageRequest{ MessageID }.
    deleteMessage(nats, {
      roomId: room.id,
      siteId: user.siteId,
      payload: { messageId: pendingDelete.id },
    })
    dispatch({
      type: 'MESSAGE_DELETED_LOCAL',
      roomId: room.id,
      messageId: pendingDelete.id,
    })
    setPendingDelete(null)
  }

  if (!room) {
    return (
      <div className="message-area">
        <div className="message-area-empty">Select a room to start chatting</div>
      </div>
    )
  }

  return (
    <div className="message-area">
      <MessageList
        messages={messages}
        room={room}
        hasLoadedHistory={hasLoadedHistory}
        historyError={historyError}
        context="main"
        focusMessageId={focusMessageId}
        currentUserAccount={user?.account}
        onThread={onThread}
        onReply={onReply}
        onEdit={handleEdit}
        onDelete={handleDelete}
        onJumpToMessage={(msgId) => jumpToMessage?.(msgId)?.catch?.(() => {})}
        onFocusConsumed={() => dispatch({ type: 'FOCUS_CLEARED', roomId: room.id })}
        onLoadOlder={() => loadOlder?.()?.catch?.(() => {})}
        hasMoreOlder={hasMoreOlder}
        loadingOlder={loadingOlder}
        bottomRef={bottomRef}
      />
      {bufferMode === BUFFER_MODE.HISTORICAL && pendingCount > 0 && (
        <div className="jump-latest-pill">
          <button type="button" onClick={() => resetToLiveTail()}>
            Jump to latest ({pendingCount} new)
          </button>
        </div>
      )}
      {pendingDelete && (
        <DeleteConfirmDialog onConfirm={handleDeleteConfirm} onCancel={handleDeleteCancel} />
      )}
      {editingMessage && (
        <TextInputDialog
          title="Edit message"
          initialValue={editingMessage.content || editingMessage.msg || ''}
          confirmLabel="Save"
          onSave={handleEditSave}
          onCancel={handleEditCancel}
        />
      )}
    </div>
  )
}
