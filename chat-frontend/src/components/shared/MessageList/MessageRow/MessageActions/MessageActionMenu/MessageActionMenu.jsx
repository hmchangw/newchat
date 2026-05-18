import { useCallback, useEffect, useRef, useState } from 'react'
import { useNats } from '@/context/NatsContext'
import { fetchReadReceipt, listRoomMembers } from '@/api'
import './style.css'

function formatReaderName(r) {
  const eng = r.engName || r.account || ''
  return r.chineseName ? `${eng} ${r.chineseName}`.trim() : eng
}

export default function MessageActionMenu({ message, room }) {
  const nats = useNats()
  const { user } = nats
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [readers, setReaders] = useState(null)
  // Recipient count sourced from listRoomMembers when the kebab opens.
  // Falls back to room.userCount when the RPC fails or hasn't resolved —
  // room.userCount can be stale (0) after a cold-start because the
  // subscription bucket reply doesn't carry it.
  const [recipientCount, setRecipientCount] = useState(null)
  const [tooltipOpen, setTooltipOpen] = useState(false)
  const rootRef = useRef(null)
  const mountedRef = useRef(true)

  useEffect(() => () => { mountedRef.current = false }, [])

  const close = useCallback(() => {
    setOpen(false)
    setTooltipOpen(false)
    setLoading(false)
    setError(null)
    setReaders(null)
    setRecipientCount(null)
  }, [])

  useEffect(() => {
    if (!open) return
    const onMouseDown = (e) => {
      if (rootRef.current && !rootRef.current.contains(e.target)) close()
    }
    const onKeyDown = (e) => { if (e.key === 'Escape') close() }
    document.addEventListener('mousedown', onMouseDown)
    document.addEventListener('keydown', onKeyDown)
    return () => {
      document.removeEventListener('mousedown', onMouseDown)
      document.removeEventListener('keydown', onKeyDown)
    }
  }, [open, close])

  const isOwnMessage = !!user && message?.sender?.account === user.account

  const handleKebabClick = () => {
    if (open) { close(); return }
    setOpen(true)
    setLoading(true)
    setError(null)
    setReaders(null)
    setRecipientCount(null)
    setTooltipOpen(false)
    const siteId = room?.siteId ?? user.siteId
    // History-loaded messages (pkg/model/cassandra.Message) serialize their
    // id as `messageId`; the api layer's normalizeHistoricalMessage already
    // remaps these to `id`, but the fallback keeps the menu working if any
    // pre-normalization path is ever introduced (e.g. quoted-parent snapshots).
    const messageId = message.id ?? message.messageId
    // member.list is authoritative for the recipient count. room.userCount
    // can be 0 after a cold-start re-login because the subscription bucket
    // reply doesn't carry it. Mirror RoomMembersBadge's pattern: fetch the
    // members on demand. Failure is non-blocking — the read-receipt side
    // still renders and Y degrades to room.userCount - 1.
    const memberCountP = Promise.resolve(
      listRoomMembers(nats, { roomId: room.id, siteId }),
    )
      .then((resp) => (Array.isArray(resp?.members) ? resp.members.length : null))
      .catch(() => null)
    Promise.all([
      fetchReadReceipt(nats, { roomId: room.id, siteId, messageId }),
      memberCountP,
    ])
      .then(([receipt, memberCount]) => {
        if (!mountedRef.current) return
        setReaders(receipt?.readers ?? [])
        if (memberCount != null) {
          setRecipientCount(Math.max(0, memberCount - 1))
        }
        setLoading(false)
      })
      .catch((err) => {
        if (!mountedRef.current) return
        setError(err?.message || 'Failed to load read receipts')
        setLoading(false)
      })
  }

  if (!isOwnMessage || !room?.id) return null

  const X = readers?.length ?? 0
  const Y = recipientCount ?? Math.max(0, (room?.userCount ?? 1) - 1)
  const hasReaders = readers != null && X > 0

  return (
    <div className="message-action-menu" ref={rootRef}>
      <button
        type="button"
        className="message-action-kebab"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-label="Message actions"
        onClick={handleKebabClick}
      >
        ⋮
      </button>
      {open && (
        <div className="message-action-popover" role="menu">
          {loading && <div className="read-receipt-row read-receipt-loading">Loading…</div>}
          {error && <div className="read-receipt-row read-receipt-error">{error}</div>}
          {!loading && !error && readers != null && (
            hasReaders ? (
              <button
                type="button"
                role="menuitem"
                className="read-receipt-row"
                onMouseEnter={() => setTooltipOpen(true)}
                onMouseLeave={() => setTooltipOpen(false)}
                onFocus={() => setTooltipOpen(true)}
                onBlur={() => setTooltipOpen(false)}
              >
                Read by {X} of {Y}
                {tooltipOpen && (
                  <ul className="read-receipt-tooltip" role="tooltip">
                    {readers.map((r) => (
                      <li key={r.userId}>{formatReaderName(r)}</li>
                    ))}
                  </ul>
                )}
              </button>
            ) : (
              <div
                role="menuitem"
                aria-disabled="true"
                className="read-receipt-row read-receipt-empty"
              >
                Read by {X} of {Y}
              </div>
            )
          )}
        </div>
      )}
    </div>
  )
}
