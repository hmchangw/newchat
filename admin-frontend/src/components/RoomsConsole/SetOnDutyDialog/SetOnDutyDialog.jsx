import { useEffect, useState } from 'react'
import Modal from '@/components/shared/Modal'
import { listRoomMembers, setRoomOnDuty } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import './style.css'

// Turns duty on for one channel. The owner is picked from the room's own members
// because room-service validates the designated owner against exactly that roster
// — offering any other account would only earn a 400. A radio group is what makes
// "exactly one owner" structural: the browser enforces it, not our state handling.
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
  const matches = needle
    ? members.filter((m) => m.account.toLowerCase().includes(needle))
    : members
  // A chosen owner stays on screen even once the filter stops matching them —
  // otherwise confirm would submit an account the admin can no longer see, and
  // this action demotes every other member of the room.
  const pinned = owner && !matches.some((m) => m.account === owner)
  const visible = pinned ? [...members.filter((m) => m.account === owner), ...matches] : matches
  // Bots are shown but not selectable: room-service would accept one as owner
  // (CheckMembership only looks for a subscription), so this is the console's
  // guard against handing a room to an account nobody reads.
  const botsOnly = members.length > 0 && members.every((m) => m.isBot)

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

      {botsOnly && (
        <p className="onduty-owner-empty">
          This room has only bot members, which cannot be made owner.
        </p>
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
          {matches.length === 0 && (
            <p className="onduty-owner-empty">No member matches “{filter.trim()}”.</p>
          )}
          <div className="onduty-owner-list" role="radiogroup" aria-label="Owner account">
            {visible.map((member, i) => (
              <label
                key={member.account}
                className={`onduty-owner-option ${pinned && i === 0 ? 'is-pinned' : ''} ${
                  member.isBot ? 'is-disabled' : ''
                }`}
              >
                <input
                  type="radio"
                  name="onduty-owner"
                  value={member.account}
                  checked={owner === member.account}
                  // Guarded rather than relying on `disabled` alone: that only stops
                  // a browser from dispatching the event, it does not make the rule true.
                  onChange={() => !member.isBot && setOwner(member.account)}
                  disabled={submitting || member.isBot}
                />
                <span>{member.account}</span>
                {member.isBot && <span className="onduty-owner-bot">bot</span>}
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
