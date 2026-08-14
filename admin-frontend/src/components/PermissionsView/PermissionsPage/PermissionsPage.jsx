import { lazy, Suspense, useState } from 'react'
import { listPermissions } from '@/api'
import { useAuth } from '@/context/AuthContext'
import { usePagedAdminList } from '@/hooks/usePagedAdminList'
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
  const [showCreate, setShowCreate] = useState(false)

  const {
    data,
    entries,
    total,
    page,
    filters,
    loading,
    error,
    notAuthorized,
    updateFilter,
    goToPage,
    refresh,
  } = usePagedAdminList({
    authToken,
    fetcher: (token, f, paging) => listPermissions(token, { ...buildFilterParams(f), ...paging }),
    defaultFilters: DEFAULT_FILTERS,
    pageSize: PAGE_SIZE,
  })

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

      {data?.currentlyGranted === true && (
        <span className="status-badge is-granted">Currently granted</span>
      )}
      {data?.currentlyGranted === false && (
        <span className="status-badge is-not-granted">Not granted</span>
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
