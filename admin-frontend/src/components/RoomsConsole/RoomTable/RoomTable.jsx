import { ondutyMinMembers } from '@/lib/runtimeConfig'
import './style.css'

// Presentational — no state, no api/ imports. All actions bubble up via
// callbacks so RoomsPage owns which dialog (if any) is open.
export default function RoomTable({ rooms, loading, onSetOnDuty, onUnsetOnDuty }) {
  if (loading) {
    return <div className="rooms-table-status">Loading…</div>
  }
  if (rooms.length === 0) {
    return <div className="rooms-table-status">No rooms found.</div>
  }

  // Both server-side rules the console can know up front: room-service restricts
  // channels only, and refuses the off→on transition below this floor. A room it
  // would reject gets no button rather than a guaranteed error.
  const minMembers = ondutyMinMembers()
  const canSet = (room) => room.type === 'channel' && room.userCount >= minMembers

  return (
    <div className="rooms-table-wrap">
      <table className="rooms-table">
        <thead>
          <tr>
            <th>_id</th>
            <th>Name</th>
            <th>Type</th>
            <th>Members</th>
            <th>Status</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          {rooms.map((room) => (
            <tr key={room.id}>
              <td className="rooms-table-id">{room.id}</td>
              <td>{room.name}</td>
              <td>{room.type}</td>
              <td>{room.userCount}</td>
              <td>
                {room.restricted && <span className="rooms-status-badge">onduty</span>}
              </td>
              <td className="rooms-table-actions">
                {room.type === 'channel' && room.restricted && (
                  <button
                    type="button"
                    className="btn btn-ghost"
                    onClick={() => onUnsetOnDuty(room)}
                  >
                    unset onduty
                  </button>
                )}
                {!room.restricted && canSet(room) && (
                  <button type="button" className="btn btn-ghost" onClick={() => onSetOnDuty(room)}>
                    set onduty
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
