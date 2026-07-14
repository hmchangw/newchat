import { createContext, useCallback, useContext, useMemo, useState } from 'react'

const LEVEL_KEY = 'debug'
const PAYLOAD_KEY = 'debugPayload'

// Ordered debug verbosity levels. 'off' sends no X-Debug header; the others
// are sent verbatim as the header value so the backend can scale diagnostics.
export const DEBUG_LEVELS = ['off', 'flow', 'debug', 'trace']

const DebugContext = createContext(null)

function normalizeLevel(value) {
  if (value === '1') return 'debug' // legacy boolean-on value
  return DEBUG_LEVELS.includes(value) ? value : 'off'
}

function readLevel() {
  try {
    return normalizeLevel(localStorage.getItem(LEVEL_KEY))
  } catch {
    // localStorage unavailable; treat as off
    return 'off'
  }
}

function readPayload() {
  try {
    return localStorage.getItem(PAYLOAD_KEY) === '1'
  } catch {
    return false
  }
}

// Persists a stored value, or removes the key when value is falsy.
function persist(key, value) {
  try {
    if (value) localStorage.setItem(key, value)
    else localStorage.removeItem(key)
  } catch {
    // tolerate private mode / quota errors
  }
}

export function DebugProvider({ children }) {
  const [level, setLevelState] = useState(readLevel)
  const [payload, setPayloadState] = useState(readPayload)

  const setLevel = useCallback((next) => {
    const normalized = normalizeLevel(next)
    persist(LEVEL_KEY, normalized === 'off' ? '' : normalized)
    setLevelState(normalized)
  }, [])

  // Payload capture (X-Debug-Payload) is a separate capability from the rung —
  // the backend gates it behind its own DEBUG_LOG_PAYLOADS flag — so it is
  // tracked independently of `level`.
  const setPayload = useCallback((next) => {
    const on = Boolean(next)
    persist(PAYLOAD_KEY, on ? '1' : '')
    setPayloadState(on)
  }, [])

  const value = useMemo(
    () => ({ level, setLevel, payload, setPayload }),
    [level, setLevel, payload, setPayload],
  )

  return <DebugContext.Provider value={value}>{children}</DebugContext.Provider>
}

export function useDebug() {
  const ctx = useContext(DebugContext)
  if (ctx === null) {
    throw new Error('useDebug must be used within a DebugProvider')
  }
  return ctx
}
