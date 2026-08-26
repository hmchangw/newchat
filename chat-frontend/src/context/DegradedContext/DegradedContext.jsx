import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'

/** How long a degraded flag survives without corroboration. A stuck flag would
 *  keep threads disabled long after recovery, and the next history load or send
 *  re-arms it if the outage is still on. */
const DEGRADED_TTL_MS = 60_000

/** Outage-class signals. These mirror the categories errcode.IsTransient treats
 *  as retryable infrastructure, so the client and the server agree on what an
 *  outage is. A terminal error (not_found, forbidden) is a settled answer, not
 *  an outage, and must not disable anything. */
function isOutageSignal(err) {
  return err?.code === 'unavailable'
    || err?.code === 'internal'
    || err?.reason === 'thread_start_unavailable'
}

const DegradedContext = createContext(null)

export function DegradedProvider({ children }) {
  const [historyDegraded, setHistoryDegraded] = useState(false)
  const timer = useRef(null)

  const clearTimer = useCallback(() => {
    if (timer.current) {
      clearTimeout(timer.current)
      timer.current = null
    }
  }, [])

  const noteHistoryFailure = useCallback((err) => {
    if (!isOutageSignal(err)) return
    setHistoryDegraded(true)
    clearTimer()
    timer.current = setTimeout(() => {
      timer.current = null
      setHistoryDegraded(false)
    }, DEGRADED_TTL_MS)
  }, [clearTimer])

  const noteHistorySuccess = useCallback(() => {
    clearTimer()
    setHistoryDegraded(false)
  }, [clearTimer])

  useEffect(() => clearTimer, [clearTimer])

  const value = useMemo(
    () => ({ historyDegraded, noteHistoryFailure, noteHistorySuccess }),
    [historyDegraded, noteHistoryFailure, noteHistorySuccess],
  )
  return <DegradedContext.Provider value={value}>{children}</DegradedContext.Provider>
}

export function useDegraded() {
  const ctx = useContext(DegradedContext)
  if (!ctx) throw new Error('useDegraded must be used within a DegradedProvider')
  return ctx
}
