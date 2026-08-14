import { lazy, Suspense, useState } from 'react'
import { useAuth } from '@/context/AuthContext'
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
]

// Top-level authed layout: nav to switch Users/Audit, header with signed-in account + logout.
export default function AppShell() {
  const { session, logout } = useAuth()
  const [section, setSection] = useState('users')

  const handleLogout = () => {
    logout()
  }

  const { Component } = SECTIONS.find((s) => s.key === section) ?? SECTIONS[0]

  return (
    <div className="app-shell">
      <nav className="app-shell-nav">
        <div className="app-shell-nav-links">
          {SECTIONS.map((s) => (
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
