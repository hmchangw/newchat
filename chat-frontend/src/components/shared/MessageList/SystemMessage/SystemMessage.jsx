import './style.css'

export default function SystemMessage({ message }) {
  return (
    <div className="system-message-row" data-message-id={message.id}>
      <span className="system-message-text">{message.content}</span>
      <time className="system-message-time" dateTime={message.createdAt}>
        {new Date(message.createdAt).toLocaleString([], {
          year: 'numeric',
          month: 'numeric',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
        })}
      </time>
    </div>
  )
}
