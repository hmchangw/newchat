import { lazy, Suspense, useState } from 'react'
import { useAuth } from '@/context/AuthContext'
import { permissionsEnabled } from '@/lib/runtimeConfig'
import LazyFallback from '@/components/shared/LazyFallback'
import './style.css'

const SECTIONS = [
  { key: 'users', label: 'Users', Component: lazy(() => import('@/components/UsersConsole')) },
  { key: 'audit', label: 'Audit', Component: lazy(() => import('@/components/AuditView')) },
  {
    key: 'permissions',
    label: 'Permissions',
    Component: lazy(() => import('@/components/PermissionsView')),
  },
  {
    key: 'updates',
    label: 'Updates',
    Component: lazy(() => import('@/components/UpdatesConsole')),
  },
]

// Top-level authed layout: nav to switch Users/Audit, header with signed-in account + logout.
export default function AppShell() {
  const { session, logout } = useAuth()
  const [section, setSection] = useState('users')

  const handleLogout = () => {
    logout()
  }

  // Permissions is deploy-gated: the section exists only where the runtime
  // config enables it (PERMISSIONS_ENABLED, rendered by nginx from env).
  const sections = SECTIONS.filter((s) => s.key !== 'permissions' || permissionsEnabled())
  const { Component } = sections.find((s) => s.key === section) ?? sections[0]

  return (
    <div className="app-shell">
      <nav className="app-shell-nav">
        <div className="app-shell-nav-links">
          {sections.map((s) => (
            <button
              key={s.key}
              type="button"
              className={`app-shell-nav-link ${section === s.key ? 'is-active' : ''}`}
              onClick={() => setSection(s.key)}
            >
              {s.label}
            </button>
          ))}
        </div>

        <div className="app-shell-account">
          <span className="app-shell-account-name">{session.account}</span>
          <button type="button" className="btn btn-ghost" onClick={handleLogout}>
            Log out
          </button>
        </div>
      </nav>

      <main className="app-shell-main">
        <Suspense fallback={<LazyFallback variant="inline" />}>
          <Component />
        </Suspense>
      </main>
    </div>
  )
}
