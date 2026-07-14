import './style.css'

// Prev/Next pager shared by UsersPage and AuditView. `page` is 1-based to match
// admin-service's parsePaging (default page=1).
export default function Pager({ page, limit, total, onPrev, onNext }) {
  const hasPrev = page > 1
  const hasNext = page * limit < total

  return (
    <div className="pager">
      <button type="button" className="btn btn-ghost" onClick={onPrev} disabled={!hasPrev}>
        Prev
      </button>
      <span className="pager-status">
        Page {page} · {total} total
      </span>
      <button type="button" className="btn btn-ghost" onClick={onNext} disabled={!hasNext}>
        Next
      </button>
    </div>
  )
}
