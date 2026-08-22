import { Suspense, lazy, useEffect, useRef, useState } from 'react'
import { useRoomSummaries } from '@/context/RoomEventsContext'
import { useThreadEvents } from '@/context/ThreadEventsContext'
import RoomMessageArea from './RoomMessageArea/RoomMessageArea'
import RoomMessageInput from './RoomMessageInput/RoomMessageInput'
import RoomMembersBadge from './RoomMembersBadge/RoomMembersBadge'
import LazyFallback from '@/components/shared/LazyFallback'
import { roomPrefix, roomDisplayName } from '@/lib/roomFormat'
import './style.css'

// Gated panels: only render after the user opens them. Splitting these
// keeps the member-roster + search-results trees out of the initial
// chat bundle.
const ManageMembersDialog = lazy(() => import('./ManageMembersDialog/ManageMembersDialog'))
const InRoomSearch = lazy(() => import('./InRoomSearch/InRoomSearch'))

export default function ChatPage({ selectedRoom, onSelectRoom }) {
  const { jumpToMessage } = useRoomSummaries()
  const { openThread, closeThread, activeParent } = useThreadEvents()
  const [showMembers, setShowMembers] = useState(false)
  const [inRoomSearchOpen, setInRoomSearchOpen] = useState(false)
  // Bumped each time ManageMembersDialog closes so RoomMembersBadge refetches
  // its count immediately, without waiting for the next room switch. Mirrors
  // the pattern in upstream MessageArea (pre-refactor).
  const [membersRefreshKey, setMembersRefreshKey] = useState(0)
  const [quotedTarget, setQuotedTarget] = useState(null)
  const triggerRef = useRef(null)

  // When the selected room changes, close room-scoped overlays.
  useEffect(() => {
    setShowMembers(false)
    setInRoomSearchOpen(false)
  }, [selectedRoom?.id])

  // Clear quoted target on room change.
  useEffect(() => {
    setQuotedTarget(null)
  }, [selectedRoom?.id])

  // Close thread when the user switches rooms.
  useEffect(() => {
    if (activeParent && activeParent.roomId !== selectedRoom?.id) {
      closeThread()
    }
  }, [selectedRoom?.id, activeParent, closeThread])

  // Restore focus to the trigger element when the thread panel closes.
  useEffect(() => {
    if (!activeParent && triggerRef.current) {
      triggerRef.current.focus?.()
      triggerRef.current = null
    }
  }, [activeParent])

  // Ctrl/Cmd-F opens the in-room side panel; Esc closes it.
  // Dep on `selectedRoom?.id` rather than the object — a parent re-render
  // that hands us an equivalent-but-new room reference would otherwise
  // rebind the global listener on every render.
  useEffect(() => {
    if (!selectedRoom) return
    const handler = (e) => {
      if ((e.ctrlKey || e.metaKey) && (e.key === 'f' || e.key === 'F')) {
        e.preventDefault()
        if (activeParent) closeThread()
        setInRoomSearchOpen(true)
      } else if (e.key === 'Escape') {
        setInRoomSearchOpen(false)
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [selectedRoom?.id, activeParent, closeThread])

  const handleInRoomJump = (msgId) => {
    if (selectedRoom && jumpToMessage) {
      jumpToMessage(selectedRoom.id, msgId)?.catch?.(() => {})
    }
  }

  const handleThread = (msg) => {
    if (!selectedRoom || !msg) return
    triggerRef.current = document.activeElement
    setInRoomSearchOpen(false)
    openThread({
      roomId: selectedRoom.id,
      siteId: selectedRoom.siteId,
      crossSite: selectedRoom.crossSite,
      messageId: msg.id,
      createdAtMs: new Date(msg.createdAt).getTime(),
    })
  }

  const handleReply = (msg) => {
    setQuotedTarget({
      id: msg.id,
      senderName: msg.sender?.engName || msg.sender?.account || msg.userAccount || 'Unknown',
      content: msg.content || msg.msg || '',
    })
  }

  return (
    <main className="chat-page">
      {selectedRoom && (
        <header className="chat-room-header">
          <span className="chat-room-name">
            {roomPrefix(selectedRoom.type)}{roomDisplayName(selectedRoom)}
          </span>
          {/* Spacer pushes the members badge to the right edge. */}
          <div className="chat-room-header-spacer" />
          <RoomMembersBadge
            room={selectedRoom}
            onOpen={() => setShowMembers(true)}
            refreshKey={membersRefreshKey}
          />
        </header>
      )}
      <div className="chat-page-body">
        <div className="chat-main-content">
          <RoomMessageArea
            room={selectedRoom}
            onThread={handleThread}
            onReply={handleReply}
          />
          <RoomMessageInput room={selectedRoom} quotedTarget={quotedTarget} onClearQuote={() => setQuotedTarget(null)} />
        </div>
        {inRoomSearchOpen && selectedRoom && (
          <Suspense fallback={<LazyFallback />}>
            <InRoomSearch
              roomId={selectedRoom.id}
              onClose={() => setInRoomSearchOpen(false)}
              onJumpToMessage={handleInRoomJump}
            />
          </Suspense>
        )}
      </div>
      {showMembers && selectedRoom && (
        <Suspense fallback={<LazyFallback variant="dialog" />}>
          <ManageMembersDialog
            room={selectedRoom}
            onClose={() => {
              setShowMembers(false)
              setMembersRefreshKey((k) => k + 1)
            }}
          />
        </Suspense>
      )}
    </main>
  )
}
