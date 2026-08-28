import { useState } from 'react'
import Modal from '@/components/shared/Modal'
import { resyncUser } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'

// Double-confirm wrapper around POST /users/:account/resync. Re-delivery only —
// the server writes nothing — and the alert follows the create-notice rule
// (spec R9): any direct-sync miss alerts; hrSyncFailed picks the severity.
export default function ResyncUserDialog({ authToken, user, onClose }) {
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState(null)
  const [syncResult, setSyncResult] = useState(null)
  const handleAdminError = useHandleAdminError()

  const handleConfirm = async () => {
    setSubmitting(true)
    setError(null)
    try {
      const res = await resyncUser(authToken, user.account)
      if (res.syncFailures.length > 0) {
        setSyncResult(res)
      } else {
        onClose()
      }
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) setError(message)
    } finally {
      setSubmitting(false)
    }
  }

  if (syncResult) {
    return (
      <Modal onClose={onClose} labelledBy="resync-user-title">
        <h2 id="resync-user-title">Resync incomplete</h2>
        <p className="dialog-error" role="alert">
          {syncResult.hrSyncFailed ? (
            <>
              This user did not sync to: {syncResult.syncFailures.join(', ')}. Resync again once
              those sites are reachable.
            </>
          ) : (
            <>
              Only this user&apos;s identity reached: {syncResult.syncFailures.join(', ')} —
              roles and status did not sync. Resync again to deliver them.
            </>
          )}
        </p>
        <div className="dialog-actions">
          <button type="button" onClick={onClose}>
            Done
          </button>
        </div>
      </Modal>
    )
  }

  return (
    <Modal onClose={onClose} labelledBy="resync-user-title">
      <h2 id="resync-user-title">Resync {user.account}?</h2>
      <p>
        Re-sends this user&apos;s current state ({user.account}) to every site on both sync
        lanes. Nothing is modified on this site.
      </p>
      {error && <div className="dialog-error">{error}</div>}
      <div className="dialog-actions">
        <button type="button" className="dialog-cancel" onClick={onClose} disabled={submitting}>
          Cancel
        </button>
        <button type="button" onClick={handleConfirm} disabled={submitting}>
          {submitting ? 'Resyncing…' : 'Resync'}
        </button>
      </div>
    </Modal>
  )
}
