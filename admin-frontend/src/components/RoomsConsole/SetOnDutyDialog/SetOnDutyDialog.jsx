import { useEffect, useState } from 'react'
import Modal from '@/components/shared/Modal'
import AccountPicker from '@/components/shared/AccountPicker'
import { listRoomMembers, setRoomOnDuty } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'

// Mirrors AccountPicker's own SEARCH_LIMIT for the listUsers-backed path.
const MAX_SUGGESTIONS = 10

// Turns duty on for one channel. The owner is picked from the room's own members
// because room-service validates the designated owner against exactly that roster
// — offering any other account would only earn a 400.
export default function SetOnDutyDialog({ authToken, room, onClose, onDone }) {
  const [members, setMembers] = useState([])
  const [owner, setOwner] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState(null)
  const handleAdminError = useHandleAdminError()

  useEffect(() => {
    let cancelled = false
    listRoomMembers(authToken, room.id)
      .then((found) => {
        if (!cancelled) setMembers(found)
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

  // Filters the already-loaded roster rather than querying: the list is bounded,
  // so a round trip per keystroke would buy nothing. Capped at the same count the
  // server-backed picker returns — a big channel would otherwise render a
  // thousand-option listbox on a single common letter.
  const searchMembers = async (q) => {
    const needle = q.toLowerCase()
    return members.filter((m) => m.account.toLowerCase().includes(needle)).slice(0, MAX_SUGGESTIONS)
  }

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
        outside the company network. The account below becomes the room&apos;s sole owner — every
        other member is reset to plain member.
      </p>
      <AccountPicker
        id="set-onduty-owner"
        label="Owner account"
        authToken={authToken}
        value={owner}
        onChange={setOwner}
        disabled={submitting}
        searchAccounts={searchMembers}
      />
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
