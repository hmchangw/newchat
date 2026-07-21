import { useEffect, useState, useCallback } from 'react'
import { NatsProvider, useNats } from '@/context/NatsContext'
import { RoomKeysProvider } from '@/context/RoomKeysContext'
import { RoomEventsProvider } from '@/context/RoomEventsContext'
import { ThreadEventsProvider } from '@/context/ThreadEventsContext'
import LoginPage from '@/pages/LoginPage'
import MainApp from '@/components/MainApp/MainApp'
import OidcCallback from '@/pages/OidcCallback'
import BotLoginPage from '@/pages/BotLoginPage'
import ErrorBoundary from '@/components/shared/ErrorBoundary'
import { BOT_LOGIN_ENABLED } from '@/lib/runtimeConfig'

function AppContent() {
  const { connected } = useNats()
  const [pathname, setPathname] = useState(
    typeof window !== 'undefined' ? window.location.pathname : '/'
  )

  useEffect(() => {
    const onPop = () => setPathname(window.location.pathname)
    window.addEventListener('popstate', onPop)
    return () => window.removeEventListener('popstate', onPop)
  }, [])

  // OidcCallback calls onDone() after it finishes; we use that to refresh
  // our local view of window.location.pathname, since history.replaceState
  // doesn't fire popstate.
  const handleOidcDone = useCallback(() => {
    setPathname(window.location.pathname)
  }, [])

  // Backend enforces the flag regardless — this effect is UI-only so the app
  // doesn't advertise a disabled surface. Clearing the URL (rather than doing
  // it inline during render) keeps render side-effect-free so React
  // StrictMode's double-render doesn't double-fire history mutations; a
  // refresh won't keep re-landing here once the URL is cleared.
  useEffect(() => {
    if (!connected && pathname === '/dev-login' && !BOT_LOGIN_ENABLED) {
      window.history.replaceState({}, '', '/')
      setPathname('/')
    }
  }, [connected, pathname])

  if (pathname === '/oidc-callback') {
    return <OidcCallback onDone={handleOidcDone} />
  }

  // Bot/admin password login — independent of DEV_MODE (bots never SSO).
  // Gated on !connected so that once login succeeds (connected flips true)
  // the user falls through to MainApp even while the URL is still /dev-login.
  if (!connected && pathname === '/dev-login' && BOT_LOGIN_ENABLED) {
    return <BotLoginPage />
  }

  if (!connected) {
    return <LoginPage />
  }

  return (
    <RoomKeysProvider>
      <RoomEventsProvider>
        <ThreadEventsProvider>
          <MainApp />
        </ThreadEventsProvider>
      </RoomEventsProvider>
    </RoomKeysProvider>
  )
}

export default function App() {
  // The boundary wraps NatsProvider so an error inside the provider's
  // initial render (e.g. a malformed runtime config) also caught.
  return (
    <ErrorBoundary>
      <NatsProvider>
        <AppContent />
      </NatsProvider>
    </ErrorBoundary>
  )
}
