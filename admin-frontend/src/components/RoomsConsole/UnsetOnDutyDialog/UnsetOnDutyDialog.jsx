import { useState } from 'react'
import Modal from '@/components/shared/Modal'
import { setRoomOnDuty } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'

// Turns duty off for one channel. No owner is sent, so existing roles are left
// exactly as they are — only the two visibility flags clear.
export default function UnsetOnDutyDialog({ authToken, room, onClose, onDone }) {
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState(null)
  const handleAdminError = useHandleAdminError()

  const handleConfirm = async () => {
    setSubmitting(true)
    setError(null)
    try {
      await setRoomOnDuty(authToken, room.id, { onDuty: false })
      onDone()
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) setError(message)
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Modal onClose={onClose} labelledBy="unset-onduty-title">
      <h2 id="unset-onduty-title">Take {room.name} off duty?</h2>
      <p>
        External access will be revoked — members will no longer be able to connect from outside
        the company network, and any member will be able to change the roster again. Owner roles
        are left as they are.
      </p>
      {error && <div className="dialog-error">{error}</div>}
      <div className="dialog-actions">
        <button type="button" className="dialog-cancel" onClick={onClose} disabled={submitting}>
          Cancel
        </button>
        <button type="button" onClick={handleConfirm} disabled={submitting}>
          {submitting ? 'Unsetting…' : 'Unset onduty'}
        </button>
      </div>
    </Modal>
  )
}
