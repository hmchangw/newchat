import { useState } from 'react'
import { uploadClientVersion, formatAsyncJobError } from '@/api'
import { useAuth } from '@/context/AuthContext'
import './style.css'

// Publishes a client update artifact pair. Validation of the files themselves
// lives in client-update-service, so this form deliberately does not second-guess
// extensions — the server's message is what the admin sees.
export default function UpdatesPage() {
  const { session } = useAuth()
  const [configFile, setConfigFile] = useState(null)
  const [executeFile, setExecuteFile] = useState(null)
  const [busy, setBusy] = useState(false)
  const [percent, setPercent] = useState(0)
  const [error, setError] = useState('')
  const [done, setDone] = useState('')

  const ready = Boolean(configFile && executeFile) && !busy

  const handleUpload = async () => {
    setBusy(true)
    setError('')
    setDone('')
    setPercent(0)
    try {
      await uploadClientVersion(session.authToken, configFile, executeFile, setPercent)
      setDone(`Uploaded ${configFile.name} and ${executeFile.name}.`)
    } catch (err) {
      setError(formatAsyncJobError(err))
    } finally {
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
        <label htmlFor="configFile">Config file (.yaml)</label>
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
