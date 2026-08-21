import { useState } from 'react'
import Modal from '@/components/shared/Modal'
import { resyncUser } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'

// Double-confirm wrapper around POST /users/:account/resync. Re-delivery only —
// the server writes nothing — so the only alarming outcome is the create-notice
// rule (spec R5): both lanes missed at least one site.
export default function ResyncUserDialog({ authToken, user, onClose }) {
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState(null)
  // Set only when BOTH lanes missed at least one site — either lane alone
  // fully covers a site, so anything less closes silently as a success.
  const [failedSites, setFailedSites] = useState(null)
  const handleAdminError = useHandleAdminError()

  const handleConfirm = async () => {
    setSubmitting(true)
    setError(null)
    try {
      const res = await resyncUser(authToken, user.account)
      if (res.hrSyncFailed && res.syncFailures.length > 0) {
        setFailedSites(res.syncFailures)
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

  if (failedSites) {
    return (
      <Modal onClose={onClose} labelledBy="resync-user-title">
        <h2 id="resync-user-title">Resync incomplete</h2>
        <p className="dialog-error" role="alert">
          Both the direct sync and the durable identity sync missed: {failedSites.join(', ')}.
          Retry once those sites are reachable.
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
