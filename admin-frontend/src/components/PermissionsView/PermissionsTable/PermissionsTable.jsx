import './style.css'

// Presentational — no state, no api/ imports, mirrors UsersConsole/UserTable. Unlike the old
// single-subject ledger table, the list can now span multiple subjects/permissions at once
// (unfiltered or permission-only queries), so both columns are always shown.
export default function PermissionsTable({ entries, loading }) {
  if (loading) {
    return <div className="permissions-table-status">Loading…</div>
  }
  if (entries.length === 0) {
    return <div className="permissions-table-status">No permission records found.</div>
  }

  return (
    <table className="data-table permissions-table">
      <thead>
        <tr>
          <th>Subject</th>
          <th>Permission</th>
          <th>Change</th>
          <th>Effective from</th>
          <th>Expires at</th>
          <th>Applicant</th>
          <th>Approver</th>
          <th>Reason</th>
          <th>Recorded by</th>
          <th>Recorded at</th>
        </tr>
      </thead>
      <tbody>
        {entries.map((entry) => (
          <tr key={entry.id}>
            <td>{entry.subjectAccount}</td>
            <td>{entry.permission}</td>
            <td>{entry.granted ? 'Grant' : 'Revoke'}</td>
            {/* Civil dates ("YYYY-MM-DD") render verbatim — no Date parsing, matching the
                fixed-UTC+8 write-time rule (design doc §8): the UI performs no timezone math. */}
            <td>{entry.effectiveFrom ?? '—'}</td>
            <td>{entry.expiresAt ?? '—'}</td>
            <td>{entry.applicantAccount}</td>
            <td>{entry.approverAccount}</td>
            <td>{entry.reason}</td>
            <td>{entry.recordedBy}</td>
            <td>{new Date(entry.recordedAt).toLocaleString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
