import { createContext, useCallback, useContext, useMemo, useState } from 'react'

const STORAGE_KEY = 'admin.session'

const AuthContext = createContext(null)

// Only {authToken, account, siteId} is exposed via `session`; the rest stays in sessionStorage only.
function toExposedSession(bundle) {
  const { authToken, account, siteId } = bundle
  return { authToken, account, siteId }
}

function readStoredSession() {
  let raw = null
  try {
    raw = sessionStorage.getItem(STORAGE_KEY)
  } catch {
    return null
  }
  if (!raw) return null
  try {
    const bundle = JSON.parse(raw)
    if (!bundle || typeof bundle !== 'object') throw new Error('malformed session')
    return toExposedSession(bundle)
  } catch {
    try {
      sessionStorage.removeItem(STORAGE_KEY)
    } catch {
      // tolerate storage unavailable
    }
    return null
  }
}

function persistBundle(bundle) {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(bundle))
  } catch {
    // tolerate private mode / quota errors
  }
}

function clearStoredSession() {
  try {
    sessionStorage.removeItem(STORAGE_KEY)
  } catch {
    // tolerate storage unavailable
  }
}

export function AuthProvider({ children }) {
  const [session, setSession] = useState(() => readStoredSession())

  const login = useCallback((bundle) => {
    persistBundle(bundle)
    setSession(toExposedSession(bundle))
  }, [])

  const logout = useCallback(() => {
    clearStoredSession()
    setSession(null)
  }, [])

  const value = useMemo(() => ({ session, login, logout }), [session, login, logout])

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (ctx === null) {
    throw new Error('useAuth must be used within an AuthProvider')
  }
  return ctx
}
