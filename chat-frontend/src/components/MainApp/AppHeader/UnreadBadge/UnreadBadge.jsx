import { useUnreadTotal } from '@/context/RoomEventsContext'
import './style.css'

/**
 * Header pill showing the app-wide unread total from `useUnreadTotal`.
 *
 * Renders nothing when there's nothing unread; caps the display at
 * `99+`; switches to the mention-accent variant when any room has a
 * mention. Purely derived — no fetch, updates live with reducer state.
 */
export default function UnreadBadge() {
  const { total, hasMention } = useUnreadTotal()
  if (total <= 0) return null

  const label = `${total} unread message${total === 1 ? '' : 's'}`
  const className = `unread-badge${hasMention ? ' unread-badge--mention' : ''}`

  return (
    <span className={className} aria-label={label} title={label}>
      {total > 99 ? '99+' : total}
    </span>
  )
}
