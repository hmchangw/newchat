import { useEffect, useRef, useState } from 'react'
import { validateSectionName } from '@/lib/chatlist'

const INVALID_COPY = 'Use 1–50 letters, digits, spaces or - _ . / ( ), no double spaces.'

/**
 * Create or rename a custom chatlist section. `mode` picks the title/verb;
 * `initialName` pre-fills for rename. `onSubmit(name)` runs the RPC (the
 * caller handles errors/close on resolve); this dialog only pre-flights the
 * name locally so the user gets instant feedback before the round-trip.
 */
export default function ChatlistSectionDialog({ mode = 'create', initialName = '', onSubmit, onClose }) {
  const [name, setName] = useState(initialName)
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const inputRef = useRef(null)

  useEffect(() => {
    inputRef.current?.focus()
    inputRef.current?.select()
  }, [])

  const submit = async (e) => {
    e.preventDefault()
    const v = validateSectionName(name)
    if (!v.ok) {
      setError(INVALID_COPY)
      return
    }
    setBusy(true)
    setError(null)
    try {
      await onSubmit(v.name)
      onClose()
    } catch (err) {
      setError(err?.reason === 'chatlist_duplicate_name' ? 'A section with that name already exists.' : (err?.message ?? 'Failed'))
      setBusy(false)
    }
  }

  const title = mode === 'rename' ? 'Rename section' : 'New section'
  const verb = mode === 'rename' ? 'Rename' : 'Create'

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div className="dialog" role="dialog" aria-label={title} onClick={(e) => e.stopPropagation()}>
        <h2>{title}</h2>
        <form onSubmit={submit}>
          <input
            ref={inputRef}
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="Section name"
            maxLength={50}
            aria-label="Section name"
          />
          {error && <div className="dialog-error">{error}</div>}
          <div className="dialog-actions">
            <button type="button" className="dialog-cancel" onClick={onClose} disabled={busy}>
              Cancel
            </button>
            <button type="submit" className="btn btn-primary" disabled={busy}>
              {busy ? '…' : verb}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
