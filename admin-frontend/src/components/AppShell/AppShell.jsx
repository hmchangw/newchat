import { lazy, Suspense, useState } from 'react'
import { useAuth } from '@/context/AuthContext'
import LazyFallback from '@/components/shared/LazyFallback'
import './style.css'

const UsersPage = lazy(() => import('@/components/UsersConsole'))
const AuditView = lazy(() => import('@/components/AuditView'))

const SECTIONS = [
  { key: 'users', label: 'Users' },
  { key: 'audit', label: 'Audit' },
]

// Top-level authed layout: nav to switch Users/Audit, header with signed-in account + logout.
export default function AppShell() {
  const { session, logout } = useAuth()
  const [section, setSection] = useState('users')

  const handleLogout = () => {
    logout()
  }

  const renderSection = () => {
    if (section === 'users') return <UsersPage />
    return <AuditView />
  }

  const content = (
    <Suspense fallback={<LazyFallback variant="inline" />}>
      {renderSection()}
    </Suspense>
  )

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

      <main className="app-shell-main">{content}</main>
    </div>
  )
}
