import { useRef, useState } from 'react'
import {
  useRoomSummaries,
  useSidebarSections,
  useChatlistActions,
  useChatlistSectionOrder,
} from '@/context/RoomEventsContext'
import { roomPrefix, roomDisplayName } from '@/lib/roomFormat'
import { BUILTIN_CHATS, isBuiltinSectionId } from '@/lib/chatlist'
import ChatlistSectionDialog from '../ChatlistSectionDialog/ChatlistSectionDialog'
import './style.css'

function mentionBadge(summary) {
  if (summary.mentionAll) return <span className="room-badge-mention-all">!</span>
  if (summary.hasMention) return <span className="room-badge-mention">@</span>
  return null
}

function RoomItem({ room, isSelected, onSelectRoom, onDragStartRoom, onDropOnRoom }) {
  const unread = room.unreadCount > 0
  const classes = ['room-item']
  if (isSelected) classes.push('room-item-selected')
  if (unread) classes.push('room-item-unread')
  return (
    <div
      className={classes.join(' ')}
      onClick={() => onSelectRoom(room)}
      draggable
      onDragStart={(e) => onDragStartRoom(e, room)}
      onDragOver={(e) => e.preventDefault()}
      onDrop={(e) => {
        e.stopPropagation()
        onDropOnRoom(room)
      }}
    >
      <span className="room-drag-handle" aria-hidden="true">⋮⋮</span>
      <div className="room-item-body">
        <div className="room-item-title">
          <span className="room-name">
            {roomPrefix(room.type)}{roomDisplayName(room)}
          </span>
          {mentionBadge(room)}
          <span className="room-meta">{room.userCount}</span>
          {unread && <span className="room-badge-unread">{room.unreadCount}</span>}
        </div>
        {/* Always rendered, even with no preview — the row's height must not
            depend on whether this line has content, or the sidebar reflows
            as previews arrive during bootstrap. */}
        <div className="room-preview">
          {room.preview && room.type !== 'dm' && room.type !== 'botDM' && (
            <span className="room-preview-sender">{room.preview.senderName}: </span>
          )}
          {room.preview?.text}
        </div>
      </div>
    </div>
  )
}

export default function RoomList({ selectedRoomId, onSelectRoom }) {
  const { error } = useRoomSummaries()
  const sections = useSidebarSections()
  const rawSectionOrder = useChatlistSectionOrder()
  const { createSection, renameSection, deleteSection, reorderSections, setSortMode, moveChatTo } =
    useChatlistActions()
  const [collapsed, setCollapsed] = useState({})
  const [dialog, setDialog] = useState(null) // {mode:'create'|'rename', sectionId?, initialName?}
  const [dragOverKey, setDragOverKey] = useState(null)
  const dragRoomRef = useRef(null)
  const dragSectionRef = useRef(null) // key of a custom section being reordered

  const toggle = (key) => setCollapsed((c) => ({ ...c, [key]: !c[key] }))

  // A section is a valid drop target only if a chat can actually move there:
  // a custom section (move into) or Chats (remove from its section). The other
  // built-ins (Favorites/Apps/Teams) are derived, not user-assignable.
  const isDropTarget = (section) => !isBuiltinSectionId(section.key) || section.key === BUILTIN_CHATS

  // anchor: {} = append; {after: roomId} = place after; {before: roomId} = place
  // before it (move-to-top when before = the section's current first room).
  const dropInto = (section, anchor = {}) => {
    const dragged = dragRoomRef.current
    dragRoomRef.current = null
    setDragOverKey(null)
    if (!dragged || !isDropTarget(section)) return
    const targetSectionId = section.key === BUILTIN_CHATS ? null : section.key
    moveChatTo(dragged.id, dragged.siteId, targetSectionId, anchor.after, anchor.before)
  }

  const onDragStartRoom = (e, room) => {
    dragRoomRef.current = { id: room.id, siteId: room.siteId }
    e.dataTransfer.effectAllowed = 'move'
    e.dataTransfer.setData('text/plain', room.id)
  }

  const onDragStartSection = (e, sectionKey) => {
    dragSectionRef.current = sectionKey
    e.dataTransfer.effectAllowed = 'move'
    e.dataTransfer.setData('text/plain', sectionKey)
  }

  // Reorder custom sections: drop the dragged section header onto another custom
  // section -> the dragged one takes that slot. The reorder RPC requires its
  // argument to be a PERMUTATION of the current full sectionOrder (built-ins +
  // every custom, each exactly once — else chatlist_invalid_order). So move the
  // dragged section within the RAW stored order rather than reconstructing one.
  const reorderSectionTo = (targetKey) => {
    const dragged = dragSectionRef.current
    dragSectionRef.current = null
    setDragOverKey(null)
    if (!dragged || dragged === targetKey) return
    if (!rawSectionOrder.includes(dragged) || !rawSectionOrder.includes(targetKey)) return
    const next = rawSectionOrder.filter((k) => k !== dragged)
    next.splice(next.indexOf(targetKey), 0, dragged) // dragged goes just before the target
    reorderSections(next)
  }

  return (
    <div className="room-list">
      <div className="room-list-header">
        <span>Rooms</span>
        <button
          type="button"
          className="btn-icon room-list-add-section"
          aria-label="New section"
          title="New section"
          onClick={() => setDialog({ mode: 'create' })}
        >
          ＋
        </button>
      </div>
      {error && <div className="room-list-error">{error}</div>}
      <div className="room-list-items">
        {sections.map((section) => {
          const isCollapsed = !!collapsed[section.key]
          const custom = !section.builtIn
          const sectionClasses = ['room-list-section']
          if (isCollapsed) sectionClasses.push('room-list-section-collapsed')
          if (dragOverKey === section.key) sectionClasses.push('room-list-section-dragover')
          return (
            <div
              key={section.key}
              className={sectionClasses.join(' ')}
              onDragOver={(e) => {
                if (!isDropTarget(section)) return
                e.preventDefault()
                setDragOverKey(section.key)
              }}
              onDragLeave={() => setDragOverKey((k) => (k === section.key ? null : k))}
              onDrop={() => dropInto(section, {})}
            >
              <div
                className="room-list-section-header"
                draggable={custom}
                onDragStart={(e) => custom && onDragStartSection(e, section.key)}
                onDragOver={(e) => {
                  // Allow drop for a section reorder (a section is being dragged) OR
                  // a chat move-to-top (a room is being dragged onto a droppable section).
                  if (dragSectionRef.current || isDropTarget(section)) e.preventDefault()
                }}
                onDrop={(e) => {
                  e.stopPropagation()
                  if (dragSectionRef.current) {
                    // Section dropped on this header -> reorder it into this slot.
                    reorderSectionTo(section.key)
                  } else {
                    // Room dropped on the header -> move to the TOP of the section
                    // (before its current first room). Empty section -> append.
                    dropInto(section, { before: section.rooms[0]?.id })
                  }
                }}
              >
                <span className="room-list-section-title" onClick={() => toggle(section.key)}>
                  <span className="room-list-section-chevron" aria-hidden="true">▾</span>
                  {section.title}
                </span>
                <span className="room-list-section-controls">
                  <button
                    type="button"
                    className="btn-icon"
                    aria-label={`Sort ${section.title}`}
                    title={section.sortMode === 'custom' ? 'Sort: manual' : 'Sort: recent'}
                    onClick={() =>
                      setSortMode(section.key, section.sortMode === 'custom' ? 'mostRecent' : 'custom')
                    }
                  >
                    {section.sortMode === 'custom' ? '⇅' : '🕘'}
                  </button>
                  {custom && (
                    <>
                      <button
                        type="button"
                        className="btn-icon"
                        aria-label={`Rename ${section.title}`}
                        title="Rename"
                        onClick={() =>
                          setDialog({ mode: 'rename', sectionId: section.key, initialName: section.title })
                        }
                      >
                        ✎
                      </button>
                      <button
                        type="button"
                        className="btn-icon"
                        aria-label={`Delete ${section.title}`}
                        title="Delete"
                        onClick={() => deleteSection(section.key)}
                      >
                        🗑
                      </button>
                    </>
                  )}
                </span>
              </div>
              {!isCollapsed && section.rooms.length === 0 && (
                <div className="room-list-section-empty">No rooms</div>
              )}
              {!isCollapsed &&
                section.rooms.map((room) => (
                  <RoomItem
                    key={room.id}
                    room={room}
                    isSelected={room.id === selectedRoomId}
                    onSelectRoom={onSelectRoom}
                    onDragStartRoom={onDragStartRoom}
                    onDropOnRoom={(target) => dropInto(section, { after: target.id })}
                  />
                ))}
            </div>
          )
        })}
      </div>
      {dialog && (
        <ChatlistSectionDialog
          mode={dialog.mode}
          initialName={dialog.initialName ?? ''}
          onSubmit={(name) =>
            dialog.mode === 'rename' ? renameSection(dialog.sectionId, name) : createSection(name)
          }
          onClose={() => setDialog(null)}
        />
      )}
    </div>
  )
}
