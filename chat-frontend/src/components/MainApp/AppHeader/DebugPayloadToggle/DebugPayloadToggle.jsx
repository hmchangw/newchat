import { useDebug } from '@/context/DebugContext'
import './style.css'

// Opts the request into full request/reply payload capture via the
// X-Debug-Payload header. Separate from the X-Debug rung; the backend only
// honors it when its own DEBUG_LOG_PAYLOADS flag is enabled.
export default function DebugPayloadToggle() {
  const { payload, setPayload } = useDebug()
  return (
    <label
      className={`debug-payload-toggle${payload ? ' debug-payload-toggle--on' : ''}`}
      title="Capture full request/reply payloads (X-Debug-Payload)"
    >
      <input
        type="checkbox"
        className="debug-payload-toggle__control"
        aria-label="Capture debug payloads"
        checked={payload}
        onChange={(e) => setPayload(e.target.checked)}
      />
      <span className="debug-payload-toggle__label">Payload</span>
    </label>
  )
}
