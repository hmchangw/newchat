import { useRef, useEffect, useState, useCallback } from 'react'
import { useNats } from '@/context/NatsContext'
import { searchMessages } from '@/api'
import './style.css'

// Submit-on-Enter (no search-as-you-type). The header's global search bar
// is the spotlight typeahead surface; this in-room panel is for deliberate
// message lookups against the messages index where each query hits ES.
// Debouncing per-keystroke would issue a request for every prefix and
// burn ES roundtrips with no UX win — Teams uses the same explicit-submit
// pattern.

export default function InRoomSearch({ roomId, onClose, onJumpToMessage }) {
  const nats = useNats()
  const inputRef = useRef(null)
  const [query, setQuery] = useState('')
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
  const [submitted, setSubmitted] = useState(false)

  useEffect(() => {
    inputRef.current?.focus()
  }, [])

  const runSearch = useCallback(async () => {
    const q = query.trim()
    if (q.length < 2) return
    setLoading(true)
    setSubmitted(true)
    try {
      const resp = await searchMessages(nats, { searchText: q, roomIds: [roomId], size: 50 })
      setResults(resp.messages ?? [])
    } catch {
      setResults([])
    } finally {
      setLoading(false)
    }
  }, [query, roomId, nats])

  const handleKeyDown = (e) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      runSearch()
    }
  }

  const handleChange = (e) => {
    setQuery(e.target.value)
    // Clearing the box should reset the visible state so the panel
    // doesn't keep showing stale results from a prior search.
    if (e.target.value === '') {
      setSubmitted(false)
      setResults([])
    }
  }

  const handleClick = (hit) => {
    onJumpToMessage(hit.messageId)
    onClose()
  }

  return (
    <aside className="in-room-search" aria-label="Search messages in this room">
      <div className="in-room-search-header">
        <span className="in-room-search-title">Search in room</span>
        <button
          type="button"
          className="in-room-search-close"
          onClick={onClose}
          aria-label="Close search"
        >
          ×
        </button>
      </div>
      <div className="in-room-search-input-wrap">
        <input
          ref={inputRef}
          type="text"
          className="in-room-search-input"
          value={query}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          placeholder="Type and press Enter to search…"
          aria-label="Search messages in room"
        />
      </div>
      {submitted && (
        <div className="in-room-search-meta">
          {loading ? 'Searching…' : `${results.length} result${results.length === 1 ? '' : 's'}`}
        </div>
      )}
      <div className="in-room-search-results" role="listbox">
        {!loading && submitted && results.length === 0 && (
          <div className="in-room-search-empty">No matches</div>
        )}
        {results.map((hit) => (
          <div
            key={hit.messageId}
            className="in-room-search-result"
            role="option"
            aria-selected="false"
            onClick={() => handleClick(hit)}
          >
            <div className="in-room-search-result-content">{hit.content}</div>
            {hit.userAccount && (
              <div className="in-room-search-result-meta">
                {hit.userAccount}
                {hit.createdAt ? ` · ${new Date(hit.createdAt).toLocaleString()}` : ''}
              </div>
            )}
          </div>
        ))}
      </div>
    </aside>
  )
}
