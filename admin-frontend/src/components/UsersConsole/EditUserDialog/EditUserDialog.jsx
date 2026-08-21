import { useState } from 'react'
import Modal from '@/components/shared/Modal'
import { updateUser } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import { ROLE_OPTIONS } from '../roleOptions'

function sameRoles(a, b) {
  if (a.length !== b.length) return false
  const sortedA = [...a].sort()
  const sortedB = [...b].sort()
  return sortedA.every((role, i) => role === sortedB[i])
}

// Only changed fields go into the PATCH; deactivating requires a second Save to confirm.
export default function EditUserDialog({ authToken, user, onClose, onUpdated }) {
  const [engName, setEngName] = useState(user.engName)
  const [chineseName, setChineseName] = useState(user.chineseName)
  const [roles, setRoles] = useState(user.roles)
  const [deactivated, setDeactivated] = useState(!user.active)
  const [confirmingDeactivate, setConfirmingDeactivate] = useState(false)
  const [error, setError] = useState(null)
  const [submitting, setSubmitting] = useState(false)
  // Set only when the patch committed here but some replication did not land — swaps the
  // dialog to a notice the admin has to acknowledge, instead of closing silently.
  const [syncFailures, setSyncFailures] = useState(null)
  const handleAdminError = useHandleAdminError()

  const toggleRole = (role) => {
    setConfirmingDeactivate(false)
    setRoles((prev) => (prev.includes(role) ? prev.filter((r) => r !== role) : [...prev, role]))
  }

  const buildPatch = () => {
    const patch = {}
    const trimmedEngName = engName.trim()
    const trimmedChineseName = chineseName.trim()
    if (trimmedEngName !== user.engName) patch.engName = trimmedEngName
    if (trimmedChineseName !== user.chineseName) patch.chineseName = trimmedChineseName
    if (!sameRoles(roles, user.roles)) patch.roles = roles
    if (deactivated !== !user.active) patch.active = !deactivated
    return patch
  }

  const submitPatch = async (patch) => {
    setSubmitting(true)
    setError(null)
    try {
      const res = await updateUser(authToken, user.account, patch)
      if (res.syncFailures.length > 0) {
        setSyncFailures(res.syncFailures)
      } else {
        onUpdated()
      }
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) setError(message)
    } finally {
      setSubmitting(false)
    }
  }

  const handleSubmit = (e) => {
    e.preventDefault()
    const patch = buildPatch()
    if (Object.keys(patch).length === 0) {
      onClose()
      return
    }
    if (patch.active === false && !confirmingDeactivate) {
      setConfirmingDeactivate(true)
      return
    }
    submitPatch(patch)
  }

  // The patch landed locally, so every exit from the notice (Close, Esc, backdrop) is onUpdated —
  // the parent has to refresh its list, not just drop the dialog.
  if (syncFailures) {
    return (
      <Modal onClose={onUpdated} labelledBy="edit-user-title">
        <h2 id="edit-user-title">Saved on this site</h2>
        <p className="dialog-error" role="alert">
          Cross-site sync failed for: {syncFailures.join(', ')}. Use Resync on this user once
          those sites are reachable.
        </p>
        <div className="dialog-actions">
          <button type="button" onClick={onUpdated}>
            Close
          </button>
        </div>
      </Modal>
    )
  }

  return (
    <Modal onClose={onClose} labelledBy="edit-user-title">
      <h2 id="edit-user-title">Edit {user.account}</h2>
      <form onSubmit={handleSubmit}>
        <label htmlFor="edit-user-engname">English name</label>
        <input
          id="edit-user-engname"
          value={engName}
          onChange={(e) => {
            setConfirmingDeactivate(false)
            setEngName(e.target.value)
          }}
          disabled={submitting}
        />

        <label htmlFor="edit-user-chinesename">Chinese name</label>
        <input
          id="edit-user-chinesename"
          value={chineseName}
          onChange={(e) => {
            setConfirmingDeactivate(false)
            setChineseName(e.target.value)
          }}
          disabled={submitting}
        />

        <fieldset>
          <legend>Roles</legend>
          {ROLE_OPTIONS.map((role) => (
            <label key={role} className="dialog-checkbox">
              <input
                type="checkbox"
                checked={roles.includes(role)}
                onChange={() => toggleRole(role)}
                disabled={submitting}
              />
              {role}
            </label>
          ))}
        </fieldset>

        <label className="dialog-checkbox">
          <input
            type="checkbox"
            checked={deactivated}
            onChange={(e) => {
              setConfirmingDeactivate(false)
              setDeactivated(e.target.checked)
            }}
            disabled={submitting}
          />
          Deactivated
        </label>

        {confirmingDeactivate && (
          <div className="dialog-error">
            This deactivates the account and blocks sign-in. Click Save again to confirm.
          </div>
        )}

        {error && <div className="dialog-error">{error}</div>}

        <div className="dialog-actions">
          <button type="button" className="dialog-cancel" onClick={onClose} disabled={submitting}>
            Cancel
          </button>
          <button type="submit" disabled={submitting}>
            {submitting ? 'Saving…' : 'Save'}
          </button>
        </div>
      </form>
    </Modal>
  )
}
