import './style.css'

// Presentational — no state, no api/ imports. All actions bubble up via
// callbacks so UsersPage owns which dialog (if any) is open.
export default function UserTable({
  users,
  loading,
  ownSiteId,
  onEdit,
  onSetPassword,
  onSessions,
  onResync,
}) {
  if (loading) {
    return <div className="users-table-status">Loading…</div>
  }
  if (users.length === 0) {
    return <div className="users-table-status">No users found.</div>
  }

  return (
    <div className="users-table-wrap">
      <table className="users-table">
        <thead>
          <tr>
            <th>Account</th>
            <th>Name</th>
            <th>Site</th>
            <th>Roles</th>
            <th>Status</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {users.map((user) => (
            <tr key={user.id}>
              <td>{user.account}</td>
              <td>{[user.engName, user.chineseName].filter(Boolean).join(' / ') || '—'}</td>
              <td>{user.siteId}</td>
              <td>{user.roles.join(', ')}</td>
              <td>
                <span
                  className={`users-status-badge ${
                    user.active ? 'is-active' : 'is-inactive'
                  }`}
                >
                  {user.active ? 'Active' : 'Deactivated'}
                </span>
              </td>
              <td className="users-table-actions">
                {user.siteId === ownSiteId ? (
                  <>
                    <button type="button" className="btn btn-ghost" onClick={() => onEdit(user)}>
                      Edit
                    </button>
                    <button
                      type="button"
                      className="btn btn-ghost"
                      onClick={() => onSetPassword(user)}
                    >
                      Set password
                    </button>
                    <button type="button" className="btn btn-ghost" onClick={() => onSessions(user)}>
                      Sessions
                    </button>
                    <button type="button" className="btn btn-ghost" onClick={() => onResync(user)}>
                      Resync
                    </button>
                  </>
                ) : (
                  // Cross-site replica: managed at its home site, read-only here.
                  <span>—</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
