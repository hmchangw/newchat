import { useState, useRef, useEffect, useCallback } from 'react'
import { useNats } from '@/context/NatsContext'
import { searchRooms } from '@/api'
import { useDebouncedSearch } from '@/hooks/useDebouncedSearch'
import { roomFromSearchHit, searchRoomPrefix } from '@/lib/roomFormat'
import './style.css'

export default function SearchBar({ onSelectRoom, onEnterSearch }) {
  const nats = useNats()
  const [activeIdx, setActiveIdx] = useState(0)
  const inputRef = useRef(null)

  const fetcher = useCallback(
    async (q) => {
      const resp = await searchRooms(nats, { searchText: q, roomType: 'all', size: 8 })
      setActiveIdx(0)
      return resp.rooms ?? []
    },
    [nats]
  )

  const { query, results, onChange, reset } = useDebouncedSearch({
    delay: 250,
    minLen: 2,
    fetcher,
  })

  // Ctrl+K / Cmd+K global shortcut
  useEffect(() => {
    const handler = (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
        e.preventDefault()
        inputRef.current?.focus()
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  const handleChange = (e) => {
    onChange(e.target.value)
  }

  const handleKeyDown = (e) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setActiveIdx((i) => Math.min(i + 1, results.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setActiveIdx((i) => Math.max(i - 1, 0))
    } else if (e.key === 'Enter') {
      e.preventDefault()
      if (query.length >= 2) onEnterSearch(query)
    } else if (e.key === 'Escape') {
      reset()
      inputRef.current?.blur()
    }
  }

  const handleClick = (hit) => {
    onSelectRoom(roomFromSearchHit(hit))
    reset()
  }

  return (
    <div className="search-bar-wrap">
      <input
        ref={inputRef}
        type="text"
        className="search-bar"
        value={query}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        placeholder="Search..."
        aria-label="Search rooms and messages"
      />
      {query.length >= 2 && results.length > 0 && (
        <div className="search-dropdown" role="listbox">
          {results.map((hit, idx) => (
            <div
              key={hit.roomId}
              className={`search-result ${idx === activeIdx ? 'active' : ''}`}
              onClick={() => handleClick(hit)}
              role="option"
              aria-selected={idx === activeIdx}
            >
              <div className="result-type">
                {searchRoomPrefix(hit.roomType)}
              </div>
              <div className="result-name">{hit.name}</div>
            </div>
          ))}
          <div className="search-footer">
            <span>↑↓ navigate · Enter see all · Esc close</span>
            <span>{results.length} rooms</span>
          </div>
        </div>
      )}
    </div>
  )
}
