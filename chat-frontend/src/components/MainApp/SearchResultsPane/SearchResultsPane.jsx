import { useEffect, useState } from 'react'
import { useNats } from '@/context/NatsContext'
import { searchRooms, searchMessages } from '@/api'
import { roomFromSearchHit, searchRoomPrefix } from '@/lib/roomFormat'

export default function SearchResultsPane({
  query,
  onClose,
  onSelectRoom,
  onJumpToMessage,
}) {
  const nats = useNats()
  const { user } = nats
  const [activeTab, setActiveTab] = useState('rooms')
  const [roomResults, setRoomResults] = useState([])
  const [roomTotal, setRoomTotal] = useState(0)
  const [msgResults, setMsgResults] = useState([])
  const [msgTotal, setMsgTotal] = useState(0)
  const [roomsLoading, setRoomsLoading] = useState(false)
  const [msgsLoading, setMsgsLoading] = useState(false)
  const [roomsError, setRoomsError] = useState(null)
  const [msgsError, setMsgsError] = useState(null)

  // Fire rooms + messages searches in parallel; errors are surfaced per tab
  // so an outage doesn't render as an empty state.
  useEffect(() => {
    if (!query || !user) return
    let cancelled = false
    setRoomsLoading(true)
    setMsgsLoading(true)
    setRoomsError(null)
    setMsgsError(null)

    searchRooms(nats, { searchText: query, roomType: 'all', size: 50 })
      .then((resp) => {
        if (cancelled) return
        const rooms = resp.rooms ?? []
        setRoomResults(rooms)
        setRoomTotal(rooms.length)
      })
      .catch((err) => {
        if (!cancelled) setRoomsError(err?.message || 'Search failed')
      })
      .finally(() => {
        if (!cancelled) setRoomsLoading(false)
      })

    searchMessages(nats, { searchText: query, size: 50 })
      .then((resp) => {
        if (cancelled) return
        setMsgResults(resp.messages ?? [])
        setMsgTotal(resp.total ?? 0)
      })
      .catch((err) => {
        if (!cancelled) setMsgsError(err?.message || 'Search failed')
      })
      .finally(() => {
        if (!cancelled) setMsgsLoading(false)
      })

    return () => {
      cancelled = true
    }
  }, [query, user, nats])

  const handleRoomClick = (hit) => {
    onSelectRoom(roomFromSearchHit(hit))
    onClose()
  }

  const handleMessageClick = (hit) => {
    onJumpToMessage(hit.roomId, hit.messageId)
    onClose()
  }

  return (
    <div className="search-results-pane">
      <div className="search-results-header">
        <h2>Search Results: "{query}"</h2>
        <button className="search-results-close" onClick={onClose}>
          ✕
        </button>
      </div>

      <div className="search-results-tabs">
        <button
          className={`tab ${activeTab === 'rooms' ? 'active' : ''}`}
          onClick={() => setActiveTab('rooms')}
          role="tab"
          aria-label="Rooms"
        >
          Rooms ({roomTotal})
        </button>
        <button
          className={`tab ${activeTab === 'messages' ? 'active' : ''}`}
          onClick={() => setActiveTab('messages')}
          role="tab"
          aria-label="Messages"
        >
          Messages ({msgTotal})
        </button>
      </div>

      <div className="search-results-content">
        {activeTab === 'rooms' && (
          <div className="room-results">
            {roomsLoading && <div className="loading">Loading rooms...</div>}
            {!roomsLoading && roomsError && (
              <div className="search-error">Room search failed: {roomsError}</div>
            )}
            {!roomsLoading && !roomsError && roomResults.length === 0 && (
              <div className="empty">No rooms found</div>
            )}
            {roomResults.map((hit) => (
              <button
                type="button"
                key={hit.roomId}
                className="result-item"
                onClick={() => handleRoomClick(hit)}
              >
                <span className="result-type">
                  {searchRoomPrefix(hit.roomType)}
                </span>
                <span className="result-name">{hit.name}</span>
              </button>
            ))}
          </div>
        )}

        {activeTab === 'messages' && (
          <div className="message-results">
            {msgsLoading && <div className="loading">Loading messages...</div>}
            {!msgsLoading && msgsError && (
              <div className="search-error">Message search failed: {msgsError}</div>
            )}
            {!msgsLoading && !msgsError && msgResults.length === 0 && (
              <div className="empty">No messages found</div>
            )}
            {msgResults.map((hit) => (
              <button
                type="button"
                key={hit.messageId}
                className="result-item"
                onClick={() => handleMessageClick(hit)}
              >
                <div className="msg-content">{hit.content}</div>
                <div className="msg-meta">
                  {hit.userAccount} · {new Date(hit.createdAt).toLocaleString()}
                </div>
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
