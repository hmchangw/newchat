import MessageActions from './MessageActions/MessageActions'
import MessageContent from '@/components/shared/MessageContent/MessageContent'
import MessageAttachments from '@/components/shared/MessageAttachments/MessageAttachments'
import MessageReactions from '@/components/shared/MessageReactions/MessageReactions'
import QuotedBlock from '@/components/shared/QuotedBlock/QuotedBlock'
import useHoverWithDelay from '@/hooks/useHoverWithDelay'
import { useNats } from '@/context/NatsContext'
import { useSubscription } from '@/context/RoomEventsContext'
import { redactInaccessibleQuoteSnapshot } from '@/lib/redactQuote'
import { messageSenderName } from '@/lib/participantName'
import './style.css'

function formatDateTime(dateStr) {
  const d = new Date(dateStr)
  return d.toLocaleString([], {
    year: 'numeric',
    month: 'numeric',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function pinnedLabel(msg) {
  const by = msg.pinnedBy
  const byName = by?.engName || by?.account
  return byName ? `Pinned by ${byName}` : 'Pinned'
}

function senderInitial(msg) {
  const name = messageSenderName(msg)
  return (name.charAt(0) || '?').toUpperCase()
}

function messageContent(msg) {
  return msg.content || msg.msg || ''
}

export default function MessageRow({
  message,
  room,
  context,
  isOwn,
  onThread,
  onReply,
  onEdit,
  onDelete,
  onJumpToMessage,
  onRetry,
  onDismiss,
}) {
  // Hover state driven from JS, NOT CSS :hover — see useHoverWithDelay for
  // why. Attach the same `handlers` to both the bubble-wrap (trigger) and
  // the floating menu (so the menu stays open while the cursor travels
  // between them).
  const { hovered, handlers } = useHoverWithDelay(200)

  // Mirror history-service's quote redaction client-side: the live broadcast
  // path doesn't gate quote snapshots against the reader's access window, so
  // a quote of a message older than historySharedSince would otherwise leak.
  const { user } = useNats()
  const subscription = useSubscription(room?.id)
  const quoteSnapshot = redactInaccessibleQuoteSnapshot(
    message.quotedParentMessage,
    subscription?.historySharedSince,
  )

  // Deleted messages are removed from the feed entirely. (Also filtered at
  // MessageList — this is defense-in-depth.)
  if (message.deleted) return null

  const rowClasses = ['message-row']
  if (isOwn) rowClasses.push('message-row-own')

  return (
    <div
      className={rowClasses.join(' ')}
      data-message-id={message.id}
      tabIndex={0}
    >
      {!isOwn && (
        <div className="message-row-avatar" aria-hidden="true">
          {senderInitial(message)}
        </div>
      )}
      <div className="message-row-body">
        <div className="message-header">
          <span className="message-sender">{messageSenderName(message)}</span>
          <span className="message-time">{formatDateTime(message.createdAt)}</span>
          {message.editedAt && <span className="message-edited"> (edited)</span>}
          {message.pinnedAt && (
            <span
              className="message-pinned"
              role="img"
              aria-label={pinnedLabel(message)}
              title={pinnedLabel(message)}
            >
              📌
            </span>
          )}
        </div>
        {/* QuotedBlock sits OUTSIDE message-bubble-wrap so hovering the
            quote doesn't trigger the action toolbar. CSS pulls it flush
            against the bubble's top so it still looks attached. */}
        {quoteSnapshot && (
          <QuotedBlock
            variant="bubble"
            snapshot={quoteSnapshot}
            onClick={onJumpToMessage}
          />
        )}
        <div className="message-bubble-wrap" {...handlers}>
          <div className="message-bubble">
            <MessageContent
              content={messageContent(message)}
              mentions={message.mentions}
              selfAccount={subscription?.u?.account}
            />
          </div>
          {hovered && (
            <div className="message-actions-host" {...handlers}>
              <MessageActions
                message={message}
                room={room}
                context={context}
                isOwn={isOwn}
                onThread={onThread}
                onReply={onReply}
                onEdit={onEdit}
                onDelete={onDelete}
              />
            </div>
          )}
        </div>
        {message.attachments?.length > 0 && (
          <MessageAttachments attachments={message.attachments} baseUrl={user?.baseUrl} />
        )}
        <MessageReactions reactions={message.reactions} selfAccount={subscription?.u?.account} />
        {message.tcount > 0 && context !== 'thread' && context !== 'thread-parent' && (
          <button
            type="button"
            className="message-reply-badge"
            onClick={() => onThread?.(message)}
          >
            💬 {message.tcount} {message.tcount === 1 ? 'reply' : 'replies'}
          </button>
        )}
        {message._status === 'failed' && (
          <div className="message-row-failed">
            <span className="message-row-failed-label">Failed to send.</span>
            <button type="button" aria-label="Retry sending message" onClick={() => onRetry?.(message.id)}>⟳</button>
            <button type="button" aria-label="Dismiss failed message" onClick={() => onDismiss?.(message.id)}>✕</button>
          </div>
        )}
      </div>
    </div>
  )
}
