import { useCallback, useEffect, useState } from 'react'
import Modal from '@/components/shared/Modal'
import { listSessions, revokeAllSessions, revokeSession } from '@/api'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import './style.css'

// Lazy-loaded dialog opened from a row's "Sessions" action. Fetches the
// user's active sessions on mount; every revoke action re-fetches since
// revokeSession/revokeAllSessions return no body.
export default function SessionsDialog({ authToken, user, onClose }) {
  const [sessions, setSessions] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const handleAdminError = useHandleAdminError()

  const refresh = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const result = await listSessions(authToken, user.account)
      setSessions(result)
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) setError(message)
    } finally {
      setLoading(false)
    }
  }, [authToken, user.account, handleAdminError])

  useEffect(() => {
    refresh()
  }, [refresh])

  const handleRevoke = async (sessionId) => {
    setBusy(true)
    setError(null)
    try {
      await revokeSession(authToken, user.account, sessionId)
      await refresh()
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) setError(message)
    } finally {
      setBusy(false)
    }
  }

  const handleRevokeAll = async () => {
    setBusy(true)
    setError(null)
    try {
      await revokeAllSessions(authToken, user.account)
      await refresh()
    } catch (err) {
      const message = handleAdminError(err)
      if (message !== null) setError(message)
    } finally {
      setBusy(false)
    }
  }

  return (
    <Modal onClose={onClose} labelledBy="sessions-title">
      <h2 id="sessions-title">Sessions for {user.account}</h2>

      {error && <div className="dialog-error">{error}</div>}

      {loading ? (
        <p>Loading…</p>
      ) : sessions.length === 0 ? (
        <p>No active sessions.</p>
      ) : (
        <ul className="sessions-list">
          {sessions.map((s) => (
            <li key={s.id}>
              <span>{new Date(s.issuedAt).toLocaleString()}</span>
              <button
                type="button"
                className="btn btn-ghost"
                onClick={() => handleRevoke(s.id)}
                disabled={busy}
              >
                Revoke
              </button>
            </li>
          ))}
        </ul>
      )}

      <div className="dialog-actions">
        <button type="button" className="dialog-cancel" onClick={onClose}>
          Close
        </button>
        <button
          type="button"
          className="btn-danger"
          onClick={handleRevokeAll}
          disabled={busy || sessions.length === 0}
        >
          Revoke all
        </button>
      </div>
    </Modal>
  )
}
