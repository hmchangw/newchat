import { useEffect, useRef, useState } from 'react'
import { uploadClientVersion } from '@/api'
import { useAuth } from '@/context/AuthContext'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import './style.css'

// Publishes a client update artifact pair. Validation of the files themselves
// lives in client-update-service, so this form deliberately does not second-guess
// extensions — the server's message is what the admin sees.
export default function UpdatesPage() {
  const { session } = useAuth()
  const handleAdminError = useHandleAdminError()
  const [configFile, setConfigFile] = useState(null)
  const [executeFile, setExecuteFile] = useState(null)
  const [busy, setBusy] = useState(false)
  const [percent, setPercent] = useState(0)
  const [error, setError] = useState('')
  const [done, setDone] = useState('')
  const abortRef = useRef(null)

  // An upload runs for minutes. Leaving the console must not leave it consuming
  // the browser's, admin-service's and client-update-service's connections, nor
  // let a second upload start alongside the first on the way back.
  useEffect(() => () => abortRef.current?.abort(), [])

  const ready = Boolean(configFile && executeFile) && !busy

  const handleUpload = async () => {
    setBusy(true)
    setError('')
    setDone('')
    setPercent(0)
    const controller = new AbortController()
    abortRef.current = controller
    try {
      await uploadClientVersion(
        session.authToken,
        configFile,
        executeFile,
        setPercent,
        controller.signal,
      )
      setDone(`Uploaded ${configFile.name} and ${executeFile.name}.`)
    } catch (err) {
      // An upload runs for minutes, so a session can expire mid-flight: the shared
      // hook logs out on invalid_token rather than leaving a stale banner on a dead form.
      const message = handleAdminError(err)
      if (message !== null) setError(message)
    } finally {
      abortRef.current = null
      setBusy(false)
    }
  }

  return (
    <section className="updates-page">
      <h2>Client updates</h2>
      <p className="updates-page-hint">
        Upload the update descriptor and its executable. An upload replaces any
        artifact already stored under the same file name.
      </p>

      <div className="updates-page-field">
        <label htmlFor="configFile">Config file (.yaml or .yml)</label>
        <input
          id="configFile"
          type="file"
          disabled={busy}
          onChange={(e) => setConfigFile(e.target.files?.[0] ?? null)}
        />
      </div>

      <div className="updates-page-field">
        <label htmlFor="executeFile">Executable</label>
        <input
          id="executeFile"
          type="file"
          disabled={busy}
          onChange={(e) => setExecuteFile(e.target.files?.[0] ?? null)}
        />
      </div>

      <button type="button" className="updates-page-submit" disabled={!ready} onClick={handleUpload}>
        {busy ? `Uploading… ${percent}%` : 'Upload'}
      </button>

      {done && (
        <p className="updates-page-ok" role="status">
          {done}
        </p>
      )}
      {error && (
        <p className="updates-page-error" role="alert">
          {error}
        </p>
      )}
    </section>
  )
}
