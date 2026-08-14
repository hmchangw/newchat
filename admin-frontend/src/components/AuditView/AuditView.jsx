import { listAudit } from '@/api'
import { useAuth } from '@/context/AuthContext'
import { usePagedAdminList } from '@/hooks/usePagedAdminList'
import Pager from '@/components/shared/Pager'
import './style.css'

// Matches admin-service's parsePaging default limit (handler.go).
const PAGE_SIZE = 20

const DEFAULT_FILTERS = { action: '', targetAccount: '' }

// Only include a filter param when the caller actually typed something —
// mirrors listUsers' `q` omission so the wire call stays `{}` at rest.
function buildFilterParams(filters) {
  const params = {}
  if (filters.action) params.action = filters.action
  if (filters.targetAccount) params.targetAccount = filters.targetAccount
  return params
}

// Settings → Audit console. Filter inputs update immediately; the query itself is debounced
// (both fields serialized into one value) — see usePagedAdminList for the paging shell.
export default function AuditView() {
  const { session } = useAuth()
  const { entries, total, page, filters, loading, error, notAuthorized, updateFilter, goToPage } =
    usePagedAdminList({
      authToken: session?.authToken,
      fetcher: (token, f, paging) => listAudit(token, { ...buildFilterParams(f), ...paging }),
      defaultFilters: DEFAULT_FILTERS,
      pageSize: PAGE_SIZE,
    })

  if (notAuthorized) {
    return (
      <div className="audit-view audit-view-not-authorized">
        <p>You are not authorized to view the audit log.</p>
      </div>
    )
  }

  return (
    <div className="audit-view">
      <div className="audit-view-header">
        <input
          type="text"
          className="audit-filter-input"
          aria-label="Filter by action"
          placeholder="Action"
          value={filters.action}
          onChange={(e) => updateFilter('action', e.target.value)}
        />
        <input
          type="text"
          className="audit-filter-input"
          aria-label="Filter by target account"
          placeholder="Target account"
          value={filters.targetAccount}
          onChange={(e) => updateFilter('targetAccount', e.target.value)}
        />
        <span className="audit-view-total">{total} entries</span>
      </div>

      {error && <div className="dialog-error">{error}</div>}

      {loading ? (
        <div className="audit-table-status">Loading…</div>
      ) : entries.length === 0 ? (
        <div className="audit-table-status">No audit entries found.</div>
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>Actor</th>
              <th>Action</th>
              <th>Target</th>
              <th>Time</th>
            </tr>
          </thead>
          <tbody>
            {entries.map((entry) => (
              <tr key={entry.id}>
                <td>{entry.actorAccount}</td>
                <td>{entry.action}</td>
                <td>{entry.targetAccount ?? entry.targetUserId}</td>
                <td>{new Date(entry.timestamp).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <Pager
        page={page}
        limit={PAGE_SIZE}
        total={total}
        onPrev={() => goToPage(page - 1)}
        onNext={() => goToPage(page + 1)}
      />
    </div>
  )
}
