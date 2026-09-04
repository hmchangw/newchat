import { useEffect, useState } from 'react'
import Modal from '@/components/shared/Modal'
import { listRoomMembers, setRoomOnDuty } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import './style.css'

// Turns duty on for one channel. The owner is picked from the room's own members
// because room-service validates the designated owner against exactly that roster
// — offering any other account would only earn a 400.
export default function SetOnDutyDialog({ authToken, room, onClose, onDone }) {
  const [members, setMembers] = useState([])
  const [loaded, setLoaded] = useState(false)
  const [owner, setOwner] = useState('')
  const [filter, setFilter] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState(null)
  const handleAdminError = useHandleAdminError()

  useEffect(() => {
    let cancelled = false
    listRoomMembers(authToken, room.id)
      .then((found) => {
        if (cancelled) return
        setMembers(found)
        setLoaded(true)
      })
      .catch((err) => {
        if (cancelled) return
        const message = handleAdminError(err)
        if (message !== null) setError(message)
      })
    return () => {
      cancelled = true
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authToken, room.id])

  // Filtering is local: the roster is already loaded and bounded by room-service's
  // MAX_ROOM_SIZE, so a query per keystroke would buy nothing.
  const needle = filter.trim().toLowerCase()
  const matches = members.filter((m) => m.account.toLowerCase().includes(needle))

  const handleConfirm = async () => {
    setSubmitting(true)
    setError(null)
    try {
      await setRoomOnDuty(authToken, room.id, { onDuty: true, ownerAccount: owner })
      onDone()
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) setError(message)
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Modal onClose={onClose} labelledBy="set-onduty-title">
      <h2 id="set-onduty-title">Set {room.name} on duty?</h2>
      <p>
        Only owners will be able to change the roster, and members will be able to connect from
        outside the company network. The account you pick becomes the room&apos;s sole owner —
        every other member is reset to plain member.
      </p>

      {loaded && members.length === 0 && (
        <p className="onduty-owner-empty">This room has no members to promote.</p>
      )}

      {members.length > 0 && (
        <fieldset className="onduty-owner">
          <legend>Owner account</legend>
          <input
            type="text"
            className="onduty-owner-filter"
            aria-label="Filter members"
            placeholder="Filter members…"
            autoComplete="off"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            disabled={submitting}
          />
          {owner && (
            // Stated separately from the list: a filter that hides the chosen row
            // must not leave confirm pointing at an account nobody can see, and
            // this action resets every other member to plain member.
            <p className="onduty-owner-chosen">
              Owner: <strong>{owner}</strong>
            </p>
          )}
          {matches.length === 0 && (
            <p className="onduty-owner-empty">No member matches “{filter.trim()}”.</p>
          )}
          <div className="onduty-owner-list" role="radiogroup" aria-label="Owner account">
            {matches.map((member) => (
              <label key={member.account} className="onduty-owner-option">
                <input
                  type="radio"
                  name="onduty-owner"
                  value={member.account}
                  checked={owner === member.account}
                  onChange={() => setOwner(member.account)}
                  disabled={submitting}
                />
                <span>{member.account}</span>
                {member.isBot && <span className="status-badge onduty-owner-bot">bot</span>}
              </label>
            ))}
          </div>
        </fieldset>
      )}

      {error && <div className="dialog-error">{error}</div>}

      <div className="dialog-actions">
        <button type="button" className="dialog-cancel" onClick={onClose} disabled={submitting}>
          Cancel
        </button>
        <button type="button" onClick={handleConfirm} disabled={submitting || !owner}>
          {submitting ? 'Setting…' : 'Set onduty'}
        </button>
      </div>
    </Modal>
  )
}
