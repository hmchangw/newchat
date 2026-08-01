import { lazy, Suspense, useRef, useState } from 'react'
import {
  useRoomSummaries,
  useSidebarSections,
  useChatlistActions,
} from '@/context/RoomEventsContext'
import { roomPrefix, roomDisplayName } from '@/lib/roomFormat'
import { BUILTIN_CHATS, isBuiltinSectionId } from '@/lib/chatlist'
import LazyFallback from '@/components/shared/LazyFallback'
import './style.css'

const ChatlistSectionDialog = lazy(() => import('../ChatlistSectionDialog/ChatlistSectionDialog'))

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
  const { createSection, renameSection, deleteSection, setSortMode, moveChatTo } = useChatlistActions()
  const [collapsed, setCollapsed] = useState({})
  const [dialog, setDialog] = useState(null) // {mode:'create'|'rename', sectionId?, initialName?}
  const [dragOverKey, setDragOverKey] = useState(null)
  const dragRoomRef = useRef(null)

  const toggle = (key) => setCollapsed((c) => ({ ...c, [key]: !c[key] }))

  // A section is a valid drop target only if a chat can actually move there:
  // a custom section (move into) or Chats (remove from its section). The other
  // built-ins (Favorites/Apps/Teams) are derived, not user-assignable.
  const isDropTarget = (section) => !isBuiltinSectionId(section.key) || section.key === BUILTIN_CHATS

  const dropInto = (section, afterRoomId) => {
    const dragged = dragRoomRef.current
    dragRoomRef.current = null
    setDragOverKey(null)
    if (!dragged || !isDropTarget(section)) return
    const targetSectionId = section.key === BUILTIN_CHATS ? null : section.key
    moveChatTo(dragged.id, dragged.siteId, targetSectionId, afterRoomId)
  }

  const onDragStartRoom = (e, room) => {
    dragRoomRef.current = { id: room.id, siteId: room.siteId }
    e.dataTransfer.effectAllowed = 'move'
    e.dataTransfer.setData('text/plain', room.id)
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
              onDrop={() => dropInto(section, undefined)}
            >
              <div className="room-list-section-header">
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
                    onDropOnRoom={(target) => dropInto(section, target.id)}
                  />
                ))}
            </div>
          )
        })}
      </div>
      {dialog && (
        <Suspense fallback={<LazyFallback variant="dialog" />}>
          <ChatlistSectionDialog
            mode={dialog.mode}
            initialName={dialog.initialName ?? ''}
            onSubmit={(name) =>
              dialog.mode === 'rename' ? renameSection(dialog.sectionId, name) : createSection(name)
            }
            onClose={() => setDialog(null)}
          />
        </Suspense>
      )}
    </div>
  )
}
