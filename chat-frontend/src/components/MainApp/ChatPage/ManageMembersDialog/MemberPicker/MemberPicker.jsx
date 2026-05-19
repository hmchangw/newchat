import { useState, useCallback, forwardRef, useImperativeHandle, useRef } from 'react'
import { useNats } from '@/context/NatsContext'
import { useDebouncedSearch } from '@/hooks/useDebouncedSearch'
import { searchRooms } from '@/api'
import './style.css'

// Split a typed string into individual entries on commas, trim each segment,
// drop empties. The free-text contract for users/orgs/channels: typing
// "alice, bob, charlie" commits three chips, not one. Single-value entry
// still works because parseCommaList("alice") returns ["alice"].
function parseCommaList(text) {
  return text
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
}

// Module-scope handlers for the picker fields: stable references so
// EntityField props don't churn on every parent render. Only the channel
// `parseEntries` stays inline (below) because it captures `user.siteId`.
const parseStringEntries = (text) => parseCommaList(text)
const identity = (e) => e

const renderChannelResult = (r) => (
  <div className="picker-result-line">
    <strong>{r.name}</strong>
    <span className="picker-result-sub"> — {r.siteId}</span>
  </div>
)
const channelRefFromResult = (r) => ({ roomId: r.roomId, siteId: r.siteId })
const channelEntryKey = (c) => `${c.siteId}/${c.roomId}`
const channelEntryLabel = (c) => `${c.roomId} (${c.siteId})`
const channelResultKey = (r) => `${r.siteId}/${r.roomId}`

// Merge a parsed batch of entries into the current list, de-duplicating
// against existing chips via entryKey. Returns the new array (same identity
// as `current` when nothing was added, so callers can detect no-op).
function mergeBatch(current, parsed, entryKey) {
  if (parsed.length === 0) return current
  const seen = new Set(current.map(entryKey))
  const added = []
  for (const entry of parsed) {
    const key = entryKey(entry)
    if (seen.has(key)) continue
    seen.add(key)
    added.push(entry)
  }
  if (added.length === 0) return current
  return [...current, ...added]
}

// Collects three entity lists for the room-service create + add-members
// payloads: users (string[]), orgs (string[]), channels (ChannelRef[]).
// Each field is a chip input that accepts comma-separated values — typing
// "alice, bob" + Enter commits two chips. The Channels field also has
// search.rooms typeahead for cross-site picks; users + orgs are free-text
// -only until the server lands the corresponding search endpoints.
//
// Imperative API (via forwarded ref):
//   flushAndGetEntries() — captures any typed-but-uncommitted text in each
//   field as chips (comma-splitting first), propagates via onChange, clears
//   the inputs, and returns the merged arrays synchronously so callers can
//   use them for an in-flight submit without waiting for the next render.
const MemberPicker = forwardRef(function MemberPicker(
  {
    users,
    orgs,
    channels,
    onUsersChange,
    onOrgsChange,
    onChannelsChange,
    disabled,
  },
  ref
) {
  const nats = useNats()
  const { user } = nats

  const channelFetcher = useCallback(
    async (q) => {
      const resp = await searchRooms(nats, { searchText: q, roomType: 'all', size: 8 })
      return resp.rooms ?? []
    },
    [nats]
  )

  // Channel parseEntries captures user.siteId so it stays inline; users/orgs
  // use module-scope `parseStringEntries` for identity stability.
  const parseChannelEntries = useCallback(
    (text) => parseCommaList(text).map((id) => ({ roomId: id, siteId: user.siteId })),
    [user.siteId]
  )

  const userFieldRef = useRef(null)
  const orgFieldRef = useRef(null)
  const channelFieldRef = useRef(null)

  useImperativeHandle(
    ref,
    () => ({
      flushAndGetEntries: () => ({
        users: userFieldRef.current?.flushAndMerge(users) ?? users,
        orgs: orgFieldRef.current?.flushAndMerge(orgs) ?? orgs,
        channels: channelFieldRef.current?.flushAndMerge(channels) ?? channels,
      }),
    }),
    [users, orgs, channels]
  )

  return (
    <div className="member-picker">
      <EntityField
        ref={userFieldRef}
        id="picker-users"
        label="Users"
        placeholder="Comma-separated, e.g. alice, bob"
        entries={users}
        onChange={onUsersChange}
        parseEntries={parseStringEntries}
        entryKey={identity}
        entryLabel={identity}
        disabled={disabled}
      />
      <EntityField
        ref={orgFieldRef}
        id="picker-orgs"
        label="Orgs"
        placeholder="Comma-separated, e.g. eng-org, ops-org"
        entries={orgs}
        onChange={onOrgsChange}
        parseEntries={parseStringEntries}
        entryKey={identity}
        entryLabel={identity}
        disabled={disabled}
      />
      <EntityField
        ref={channelFieldRef}
        id="picker-channels"
        label="Channels"
        placeholder="Search a channel — or comma-separated local-site room ids"
        entries={channels}
        onChange={onChannelsChange}
        // Free-text commit assumes typed roomIds are local to the user's
        // site; cross-site picks must come from the search dropdown so the
        // siteId is sourced from server-known room metadata, not guessed.
        parseEntries={parseChannelEntries}
        entryKey={channelEntryKey}
        entryLabel={channelEntryLabel}
        searchFetcher={channelFetcher}
        renderResult={renderChannelResult}
        entryFromResult={channelRefFromResult}
        resultKey={channelResultKey}
        disabled={disabled}
      />
    </div>
  )
})

export default MemberPicker

const EntityField = forwardRef(function EntityField(
  {
    id,
    label,
    placeholder,
    entries,
    onChange,
    parseEntries,
    entryKey,
    entryLabel,
    searchFetcher,
    renderResult,
    entryFromResult,
    resultKey,
    disabled,
  },
  ref
) {
  const [activeIdx, setActiveIdx] = useState(0)

  const { query, results, onChange: setQuery, reset } = useDebouncedSearch({
    delay: 250,
    minLen: 2,
    fetcher: searchFetcher,
  })
  const hasDropdown = !disabled && !!searchFetcher && query.length >= 2 && results.length > 0

  // Commit one entry (used by dropdown picks). Multi-add via comma-list goes
  // through commitParsedText which builds the merged list in one onChange so
  // we don't fight stale `entries` from props between successive calls.
  const commitSingle = (entry) => {
    if (entry == null) return
    const key = entryKey(entry)
    if (entries.some((e) => entryKey(e) === key)) return
    onChange([...entries, entry])
    reset()
    setActiveIdx(0)
  }

  const commitParsedText = (text) => {
    const parsed = parseEntries(text)
    const next = mergeBatch(entries, parsed, entryKey)
    if (next !== entries) onChange(next)
    reset()
    setActiveIdx(0)
  }

  // Imperative API for the parent picker's flushAndGetEntries: take whatever
  // text is in the input right now, comma-parse it, merge into the supplied
  // currentEntries (the parent's latest props value, which may include this
  // field's chips), and return the new array. Clears the input even on
  // dedup/empty so a subsequent submit doesn't pick up stale text.
  useImperativeHandle(
    ref,
    () => ({
      flushAndMerge: (currentEntries) => {
        const trimmed = query.trim()
        reset()
        setActiveIdx(0)
        if (!trimmed) return currentEntries
        const parsed = parseEntries(trimmed)
        const next = mergeBatch(currentEntries, parsed, entryKey)
        if (next !== currentEntries) onChange(next)
        return next
      },
    }),
    [query, reset, parseEntries, entryKey, onChange]
  )

  const handleKeyDown = (e) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      if (hasDropdown && results[activeIdx]) {
        commitSingle(entryFromResult(results[activeIdx]))
      } else if (query.trim()) {
        commitParsedText(query)
      }
    } else if (e.key === 'ArrowDown' && hasDropdown) {
      e.preventDefault()
      setActiveIdx((i) => Math.min(i + 1, results.length - 1))
    } else if (e.key === 'ArrowUp' && hasDropdown) {
      e.preventDefault()
      setActiveIdx((i) => Math.max(i - 1, 0))
    } else if (e.key === 'Escape') {
      reset()
      setActiveIdx(0)
    }
  }

  return (
    <div className="member-picker-field">
      <label htmlFor={id}>{label}</label>
      {entries.length > 0 && (
        <div className="member-picker-chips">
          {entries.map((entry, idx) => (
            <span key={entryKey(entry)} className="member-picker-chip">
              {entryLabel(entry)}
              <button
                type="button"
                aria-label={`Remove ${entryLabel(entry)}`}
                className="member-picker-chip-remove"
                onClick={() => onChange(entries.filter((_, i) => i !== idx))}
                disabled={disabled}
              >
                ×
              </button>
            </span>
          ))}
        </div>
      )}
      <input
        id={id}
        type="text"
        value={query}
        onChange={(e) => {
          setQuery(e.target.value)
          setActiveIdx(0)
        }}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        disabled={disabled}
      />
      {hasDropdown && (
        <div className="member-picker-dropdown" role="listbox">
          {results.map((r, idx) => (
            <div
              key={resultKey(r)}
              role="option"
              aria-selected={idx === activeIdx}
              className={`member-picker-result${idx === activeIdx ? ' active' : ''}`}
              onClick={() => {
                if (disabled) return
                commitSingle(entryFromResult(r))
              }}
            >
              {renderResult(r)}
            </div>
          ))}
        </div>
      )}
    </div>
  )
})
