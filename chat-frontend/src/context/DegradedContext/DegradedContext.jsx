import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'

/** How long a degraded flag survives without corroboration. A stuck flag would
 *  keep threads disabled long after recovery, and the next history load or send
 *  re-arms it if the outage is still on. */
const DEGRADED_TTL_MS = 60_000

/** The errcode categories that are a settled answer: retrying gets the same
 *  result, so none of them is an outage. Everything else is (see below). */
const TERMINAL_ERROR_CODES = new Set([
  'bad_request', 'unauthenticated', 'forbidden', 'not_found', 'conflict', 'too_many_requests',
])

/** Outage-class signals, mirroring `errcode.IsTransient` in BOTH directions:
 *  a classified terminal category is not an outage, and anything unclassified
 *  is (the server's `IsTransient` returns true for every non-*errcode.Error).
 *
 *  The unclassified half is load-bearing, not theoretical. `requestSync` times
 *  out at 5s and only attaches `.code` when the reply carried an errcode
 *  envelope, so the two failure modes that never produce one are exactly the
 *  ones this feature exists for: Cassandra up but not answering (cassutil's
 *  10s query timeout outlives our 5s, so nats.ws throws `TIMEOUT`) and
 *  history-service down or crash-looping (`503`, no responders). Requiring a
 *  `.code` silently skipped both.
 *
 *  Direction of the failure is deliberate: a wrong arm costs 60s of disabled
 *  thread-start buttons, cleared early by the next successful load or send; a
 *  missed arm costs a silently dropped thread reply, which is the bug this
 *  whole change exists to prevent. */
function isOutageSignal(err) {
  if (!err) return false
  if (err.reason === 'thread_start_unavailable') return true
  return !TERMINAL_ERROR_CODES.has(err.code)
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
