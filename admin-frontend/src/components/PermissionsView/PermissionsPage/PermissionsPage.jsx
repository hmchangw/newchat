import { lazy, Suspense, useCallback, useEffect, useRef, useState } from 'react'
import { AsyncJobError, listPermissions } from '@/api'
import { useAuth } from '@/context/AuthContext'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import { useLatestRequest } from '@/hooks/useLatestRequest'
import { useDebouncedSearch } from '@/hooks/useDebouncedSearch'
import LazyFallback from '@/components/shared/LazyFallback'
import Pager from '@/components/shared/Pager'
import PermissionsTable from '../PermissionsTable'
import { PERMISSION_KEY } from '../permissionKey'
import './style.css'

const CreatePermissionsDialog = lazy(() => import('../CreatePermissionsDialog'))

// Matches admin-service's parsePaging default limit (handler.go).
const PAGE_SIZE = 20

const DEFAULT_FILTERS = { subjectAccount: '', permission: '' }

// Only include a filter param when the caller actually set it — mirrors AuditView's
// buildFilterParams so the wire call stays `{}` (all rows, newest-first) at rest.
function buildFilterParams(filters) {
  const params = {}
  if (filters.subjectAccount) params.subjectAccount = filters.subjectAccount
  if (filters.permission) params.permission = filters.permission
  return params
}

// Settings → Permissions console: list-first, mirroring UsersConsole/UsersPage. Unfiltered shows
// page 1 of every row for the site (admin-service now supports both filters being optional).
// "Create" opens the grant/revoke dialog; a successful submit refreshes this list immediately.
export default function PermissionsPage() {
  const { session } = useAuth()
  const authToken = session?.authToken
  const handleAdminError = useHandleAdminError()

  const [entries, setEntries] = useState([])
  const [total, setTotal] = useState(0)
  const [currentlyGranted, setCurrentlyGranted] = useState(undefined)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [notAuthorized, setNotAuthorized] = useState(false)
  const [filters, setFilters] = useState(DEFAULT_FILTERS)
  const [page, setPage] = useState(1)
  const [showCreate, setShowCreate] = useState(false)

  const { begin, isCurrent } = useLatestRequest()
  // Tracks the filter key already reflected by the most recent fetch (whether triggered by the
  // debounce or by goToPage), so a debounced onSearch firing after a manual goToPage — with the
  // same filters it already picked up — doesn't clobber the page the user just navigated to.
  const lastFetchedFilterKeyRef = useRef(JSON.stringify(DEFAULT_FILTERS))

  const fetchPermissions = useCallback(
    async (params, pageArg) => {
      const token = begin()
      setLoading(true)
      setError(null)
      try {
        const result = await listPermissions(authToken, { ...params, page: pageArg, limit: PAGE_SIZE })
        if (!isCurrent(token)) return // superseded by a newer request
        setEntries(result.entries)
        setTotal(result.total)
        setCurrentlyGranted(result.currentlyGranted)
        setNotAuthorized(false)
      } catch (err) {
        if (!isCurrent(token)) return
        setEntries([])
        setTotal(0)
        setCurrentlyGranted(undefined)
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
    fetchPermissions({}, 1)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authToken])

  const { setQuery: setDebouncedFilters } = useDebouncedSearch({
    onSearch: (serialized) => {
      if (serialized === lastFetchedFilterKeyRef.current) return
      lastFetchedFilterKeyRef.current = serialized
      const parsed = serialized ? JSON.parse(serialized) : DEFAULT_FILTERS
      setPage(1)
      fetchPermissions(buildFilterParams(parsed), 1)
    },
  })

  const updateFilter = (key, value) => {
    const next = { ...filters, [key]: value }
    setFilters(next)
    setDebouncedFilters(JSON.stringify(next))
  }

  const refresh = () => fetchPermissions(buildFilterParams(filters), page)

  const goToPage = (nextPage) => {
    lastFetchedFilterKeyRef.current = JSON.stringify(filters)
    setPage(nextPage)
    fetchPermissions(buildFilterParams(filters), nextPage)
  }

  if (notAuthorized) {
    return (
      <div className="permissions-page permissions-page-not-authorized">
        <p>You are not authorized to manage permissions.</p>
      </div>
    )
  }

  return (
    <div className="permissions-page">
      <div className="permissions-page-header">
        <input
          type="text"
          className="permissions-filter-input"
          aria-label="Filter by subject account"
          placeholder="Subject account"
          value={filters.subjectAccount}
          onChange={(e) => updateFilter('subjectAccount', e.target.value)}
        />
        <select
          className="permissions-filter-select"
          aria-label="Filter by permission"
          value={filters.permission}
          onChange={(e) => updateFilter('permission', e.target.value)}
        >
          <option value="">All permissions</option>
          <option value={PERMISSION_KEY}>{PERMISSION_KEY}</option>
        </select>
        <span className="permissions-page-total">{total} entries</span>
        <button type="button" className="btn btn-primary" onClick={() => setShowCreate(true)}>
          Create
        </button>
      </div>

      {error && <div className="dialog-error">{error}</div>}

      {currentlyGranted === true && (
        <span className="permissions-badge is-granted">Currently granted</span>
      )}
      {currentlyGranted === false && (
        <span className="permissions-badge is-not-granted">Not granted</span>
      )}

      <PermissionsTable entries={entries} loading={loading} />

      <Pager
        page={page}
        limit={PAGE_SIZE}
        total={total}
        onPrev={() => goToPage(page - 1)}
        onNext={() => goToPage(page + 1)}
      />

      {showCreate && (
        <Suspense fallback={<LazyFallback variant="dialog" />}>
          <CreatePermissionsDialog
            authToken={authToken}
            onClose={() => setShowCreate(false)}
            onCreated={refresh}
          />
        </Suspense>
      )}
    </div>
  )
}
