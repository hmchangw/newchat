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

  // room-service refuses the off→on transition below this floor, so a room it
  // would reject gets no button rather than a guaranteed error.
  const minMembers = ondutyMinMembers()

  // The toggle writes `restricted` and `externalAccess` together, but they are
  // stored independently. A half-set room is not on duty, so it gets the "set"
  // action — re-running the toggle is what brings it fully on duty.
  const isOnDuty = (room) => room.restricted && room.externalAccess

  return (
    <div className="rooms-table-wrap">
      <table className="data-table rooms-table">
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
                {isOnDuty(room) && <span className="status-badge is-onduty">onduty</span>}
              </td>
              <td className="rooms-table-actions">
                {/* Channels only — room-service rejects a DM with non_channel_operation. */}
                {room.type === 'channel' &&
                  (isOnDuty(room) ? (
                    <button
                      type="button"
                      className="btn btn-ghost"
                      onClick={() => onUnsetOnDuty(room)}
                    >
                      unset onduty
                    </button>
                  ) : (
                    room.userCount >= minMembers && (
                      <button
                        type="button"
                        className="btn btn-ghost"
                        onClick={() => onSetOnDuty(room)}
                      >
                        set onduty
                      </button>
                    )
                  ))}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
