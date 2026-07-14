import { useCallback, useEffect, useRef, useState } from 'react'
import { AsyncJobError, listAudit } from '@/api'
import { useAuth } from '@/context/AuthContext'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import { useLatestRequest } from '@/hooks/useLatestRequest'
import { useDebouncedSearch } from '@/hooks/useDebouncedSearch'
import Pager from '@/components/shared/Pager'
import './style.css'

// Matches admin-service's parsePaging default limit (handler.go).
const PAGE_SIZE = 20

// Only include a filter param when the caller actually typed something —
// mirrors listUsers' `q` omission so the wire call stays `{}` at rest.
function buildFilterParams(filters) {
  const params = {}
  if (filters.action) params.action = filters.action
  if (filters.targetAccount) params.targetAccount = filters.targetAccount
  return params
}

// Settings → Audit console. Filter inputs update immediately; the query itself is debounced
// (both fields serialized into one value). A generation counter drops stale in-flight responses.
export default function AuditView() {
  const { session } = useAuth()
  const authToken = session?.authToken
  const handleAdminError = useHandleAdminError()

  const [entries, setEntries] = useState([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [notAuthorized, setNotAuthorized] = useState(false)
  const [filters, setFilters] = useState({ action: '', targetAccount: '' })
  const [page, setPage] = useState(1)

  const { begin, isCurrent } = useLatestRequest()
  // Tracks the filter key already reflected by the most recent fetch (whether
  // triggered by the debounce or by goToPage), so a debounced onSearch firing
  // after a manual goToPage — with the same filters it already picked up —
  // doesn't clobber the page the user just navigated to.
  const lastFetchedFilterKeyRef = useRef(JSON.stringify({ action: '', targetAccount: '' }))

  const fetchAudit = useCallback(
    async (params, pageArg) => {
      const token = begin()
      setLoading(true)
      setError(null)
      try {
        const result = await listAudit(authToken, { ...params, page: pageArg, limit: PAGE_SIZE })
        if (!isCurrent(token)) return // superseded by a newer request
        setEntries(result.entries)
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
    fetchAudit({}, 1)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authToken])

  const { setQuery: setDebouncedFilters } = useDebouncedSearch({
    onSearch: (serialized) => {
      // If a manual goToPage already fetched with this exact filter set (e.g. the
      // user paginated before this debounce fired), skip the redundant page-1 reset.
      if (serialized === lastFetchedFilterKeyRef.current) return
      lastFetchedFilterKeyRef.current = serialized
      const parsed = serialized ? JSON.parse(serialized) : { action: '', targetAccount: '' }
      setPage(1)
      fetchAudit(buildFilterParams(parsed), 1)
    },
  })

  const updateFilter = (key, value) => {
    const next = { ...filters, [key]: value }
    setFilters(next)
    setDebouncedFilters(JSON.stringify(next))
  }

  const goToPage = (nextPage) => {
    lastFetchedFilterKeyRef.current = JSON.stringify(filters)
    setPage(nextPage)
    fetchAudit(buildFilterParams(filters), nextPage)
  }

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
        <table className="audit-table">
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
