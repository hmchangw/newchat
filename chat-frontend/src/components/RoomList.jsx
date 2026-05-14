import { useState } from 'react'
import { useRoomSummaries, useSidebarSections } from '../context/RoomEventsContext'
import { roomPrefix, roomDisplayName } from '../lib/roomFormat'

function mentionBadge(summary) {
  if (summary.mentionAll) return <span className="room-badge-mention-all">!</span>
  if (summary.hasMention) return <span className="room-badge-mention">@</span>
  return null
}

function RoomItem({ room, isSelected, onSelectRoom }) {
  const unread = room.unreadCount > 0
  const classes = ['room-item']
  if (isSelected) classes.push('room-item-selected')
  if (unread) classes.push('room-item-unread')
  return (
    <div className={classes.join(' ')} onClick={() => onSelectRoom(room)}>
      <span className="room-name">
        {roomPrefix(room.type)}{roomDisplayName(room)}
      </span>
      {mentionBadge(room)}
      <span className="room-meta">{room.userCount}</span>
      {unread && <span className="room-badge-unread">{room.unreadCount}</span>}
    </div>
  )
}

export default function RoomList({ selectedRoomId, onSelectRoom }) {
  const { error } = useRoomSummaries()
  const sections = useSidebarSections()
  const [collapsed, setCollapsed] = useState({})

  const toggle = (key) => setCollapsed((c) => ({ ...c, [key]: !c[key] }))

  return (
    <div className="room-list">
      <div className="room-list-header">Rooms</div>
      {error && <div className="room-list-error">{error}</div>}
      <div className="room-list-items">
        {sections.map((section) => {
          if (section.rooms.length === 0) return null
          const isCollapsed = !!collapsed[section.key]
          const sectionClasses = ['room-list-section']
          if (isCollapsed) sectionClasses.push('room-list-section-collapsed')
          return (
            <div key={section.key} className={sectionClasses.join(' ')}>
              <div
                className="room-list-section-header"
                onClick={() => toggle(section.key)}
              >
                {section.title}
              </div>
              {!isCollapsed &&
                section.rooms.map((room) => (
                  <RoomItem
                    key={room.id}
                    room={room}
                    isSelected={room.id === selectedRoomId}
                    onSelectRoom={onSelectRoom}
                  />
                ))}
            </div>
          )
        })}
      </div>
    </div>
  )
}
