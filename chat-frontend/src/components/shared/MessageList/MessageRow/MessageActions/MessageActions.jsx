import { useDegraded } from '@/context/DegradedContext'
import MessageActionMenu from './MessageActionMenu/MessageActionMenu'
import './style.css'

export default function MessageActions({
  message, room, context = 'main', isOwn,
  onThread, onReply, onEdit, onDelete,
}) {
  // Thread: only opens new threads from the main feed. Inside the thread
  // panel (parent OR replies) it's hidden — you're already in a thread.
  // Quote: always available — any visible message can be quoted in a reply,
  // including the thread parent (you reply to it inside the thread input).
  // Edit / Delete: own-only.
  const showThread = context === 'main'
  const showReply = true
  const showEdit = !!isOwn
  const showDelete = !!isOwn

  const { historyDegraded } = useDegraded()
  // The gatekeeper only refuses a thread-start in a channel room with no
  // existing thread (see message-gatekeeper/handler.go: meta.Type !=
  // model.RoomTypeChannel is never refused) — a message with no replies has
  // no thread_rooms document there, so a reply can't be delivered while
  // history is down. DM/botDM/discussion rooms and tcount > 0 (thread
  // already exists, resolves from Mongo) are never refused — leave them alone.
  const threadStartBlocked = historyDegraded && room?.type === 'channel' && !(message.tcount > 0)

  // If nothing would render and the kebab is hidden too (others' message),
  // skip the toolbar entirely so we don't paint an empty floating bar.
  const hasAnyButton = showThread || showReply || showEdit || showDelete
  const hasKebab = !!isOwn
  if (!hasAnyButton && !hasKebab) return null

  return (
    <div className="message-actions" role="toolbar" aria-label="Message actions">
      {showThread && (
        <button
          type="button"
          className="message-action message-action-thread"
          aria-label="Reply in thread"
          title={threadStartBlocked ? 'Threads are temporarily unavailable' : undefined}
          disabled={threadStartBlocked}
          onClick={() => onThread?.(message)}
        >
          Thread
        </button>
      )}
      {showReply && (
        <button
          type="button"
          className="message-action message-action-reply"
          aria-label="Quote this message"
          title="Quote this message"
          onClick={() => onReply?.(message)}
        >
          Quote
        </button>
      )}
      {showEdit && (
        <button
          type="button"
          className="message-action message-action-edit"
          aria-label="Edit message"
          title="Edit message"
          onClick={() => onEdit?.(message)}
        >
          Edit
        </button>
      )}
      {showDelete && (
        <button
          type="button"
          className="message-action message-action-delete"
          aria-label="Delete message"
          title="Delete message"
          onClick={() => onDelete?.(message)}
        >
          Delete
        </button>
      )}
      {/* Read-receipt kebab — only renders on own messages (handled inside
          MessageActionMenu). When rendered, it sits as the last button in
          the toolbar so the whole group reveals/dismisses together. */}
      <MessageActionMenu message={message} room={room} />
    </div>
  )
}
