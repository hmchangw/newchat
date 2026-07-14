import { lazy, Suspense, useCallback, useEffect, useState } from 'react'
import { AsyncJobError, listUsers } from '@/api'
import { useAuth } from '@/context/AuthContext'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import { useLatestRequest } from '@/hooks/useLatestRequest'
import LazyFallback from '@/components/shared/LazyFallback'
import Pager from '@/components/shared/Pager'
import { useDebouncedSearch } from '@/hooks/useDebouncedSearch'
import UserTable from '../UserTable'
import './style.css'

const CreateUserForm = lazy(() => import('../CreateUserForm'))
const EditUserDialog = lazy(() => import('../EditUserDialog'))
const SetPasswordDialog = lazy(() => import('../SetPasswordDialog'))
const SessionsDialog = lazy(() => import('../SessionsDialog'))

// Matches admin-service's parsePaging default limit (handler.go).
const PAGE_SIZE = 20

// Settings → Users console. Owns the list query, the table, and which per-user dialog is open;
// child dialogs report mutations back up here since none of the mutating api/ calls return the record.
export default function UsersPage() {
  const { session } = useAuth()
  const authToken = session?.authToken
  const handleAdminError = useHandleAdminError()

  const [users, setUsers] = useState([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [notAuthorized, setNotAuthorized] = useState(false)
  const [page, setPage] = useState(1)

  const [showCreate, setShowCreate] = useState(false)
  const [editingUser, setEditingUser] = useState(null)
  const [passwordUser, setPasswordUser] = useState(null)
  const [sessionsUser, setSessionsUser] = useState(null)

  const { begin, isCurrent } = useLatestRequest()

  const fetchUsers = useCallback(
    async (params, pageArg) => {
      const token = begin()
      setLoading(true)
      setError(null)
      try {
        const result = await listUsers(authToken, { ...params, page: pageArg, limit: PAGE_SIZE })
        if (!isCurrent(token)) return // superseded by a newer request
        setUsers(result.users)
        setTotal(result.total)
        setNotAuthorized(false)
      } catch (err) {
        if (!isCurrent(token)) return
        if (err instanceof AsyncJobError && err.reason === 'not_admin') {
          setNotAuthorized(true)
        } else {
          const message = handleAdminError(err)
          if (message !== null) setError(message)
        }
      } finally {
        if (isCurrent(token)) setLoading(false)
      }
    },
    [authToken, handleAdminError, begin, isCurrent],
  )

  useEffect(() => {
    setPage(1)
    fetchUsers({}, 1)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authToken])

  const { query, setQuery } = useDebouncedSearch({
    onSearch: (q) => {
      setPage(1)
      fetchUsers(q ? { q } : {}, 1)
    },
  })

  const refresh = () => fetchUsers(query ? { q: query } : {}, page)

  const goToPage = (nextPage) => {
    setPage(nextPage)
    fetchUsers(query ? { q: query } : {}, nextPage)
  }

  if (notAuthorized) {
    return (
      <div className="users-page users-page-not-authorized">
        <p>You are not authorized to manage users.</p>
      </div>
    )
  }

  return (
    <div className="users-page">
      <div className="users-page-header">
        <input
          type="search"
          className="users-search-input"
          placeholder="Search users…"
          aria-label="Search users"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <span className="users-page-total">{total} users</span>
        <button type="button" className="btn btn-primary" onClick={() => setShowCreate(true)}>
          New user
        </button>
      </div>

      {error && <div className="dialog-error">{error}</div>}

      <UserTable
        users={users}
        loading={loading}
        onEdit={setEditingUser}
        onSetPassword={setPasswordUser}
        onSessions={setSessionsUser}
      />

      <Pager
        page={page}
        limit={PAGE_SIZE}
        total={total}
        onPrev={() => goToPage(page - 1)}
        onNext={() => goToPage(page + 1)}
      />

      {showCreate && (
        <Suspense fallback={<LazyFallback variant="dialog" />}>
          <CreateUserForm
            authToken={authToken}
            onClose={() => setShowCreate(false)}
            onCreated={() => {
              setShowCreate(false)
              refresh()
            }}
          />
        </Suspense>
      )}

      {editingUser && (
        <Suspense fallback={<LazyFallback variant="dialog" />}>
          <EditUserDialog
            authToken={authToken}
            user={editingUser}
            onClose={() => setEditingUser(null)}
            onUpdated={() => {
              setEditingUser(null)
              refresh()
            }}
          />
        </Suspense>
      )}

      {passwordUser && (
        <Suspense fallback={<LazyFallback variant="dialog" />}>
          <SetPasswordDialog
            authToken={authToken}
            user={passwordUser}
            onClose={() => setPasswordUser(null)}
            onUpdated={() => {
              setPasswordUser(null)
              refresh()
            }}
          />
        </Suspense>
      )}

      {sessionsUser && (
        <Suspense fallback={<LazyFallback variant="dialog" />}>
          <SessionsDialog
            authToken={authToken}
            user={sessionsUser}
            onClose={() => setSessionsUser(null)}
          />
        </Suspense>
      )}
    </div>
  )
}
