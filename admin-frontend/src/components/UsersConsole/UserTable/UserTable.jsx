import './style.css'

// Presentational — no state, no api/ imports. All actions bubble up via
// callbacks so UsersPage owns which dialog (if any) is open.
export default function UserTable({ users, loading, onEdit, onSetPassword, onSessions }) {
  if (loading) {
    return <div className="users-table-status">Loading…</div>
  }
  if (users.length === 0) {
    return <div className="users-table-status">No users found.</div>
  }

  return (
    <table className="users-table">
      <thead>
        <tr>
          <th>Account</th>
          <th>Name</th>
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
            <td>{user.roles.join(', ')}</td>
            <td>
              <span
                className={`users-status-badge ${
                  user.deactivated ? 'is-deactivated' : 'is-active'
                }`}
              >
                {user.deactivated ? 'Deactivated' : 'Active'}
              </span>
            </td>
            <td className="users-table-actions">
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
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
