import { useNats } from '@/context/NatsContext'
import SearchBar from './SearchBar/SearchBar'
import ThemeToggle from './ThemeToggle/ThemeToggle'
import DebugLevelSelect from './DebugLevelSelect/DebugLevelSelect'
import DebugPayloadToggle from './DebugPayloadToggle/DebugPayloadToggle'
import UnreadBadge from './UnreadBadge'
import './style.css'

export default function AppHeader({ onSelectRoom, onEnterSearch }) {
  const { user, disconnect } = useNats()

  return (
    <header className="app-header">
      <span className="chat-header-title">
        Chat
        <sup className="chat-header-badge">
          <UnreadBadge />
        </sup>
      </span>
      <div className="app-header-search">
        <SearchBar onSelectRoom={onSelectRoom} onEnterSearch={onEnterSearch} />
      </div>
      <span className="app-header-user">
        {user?.account} · {user?.siteId}
      </span>
      <DebugLevelSelect />
      <DebugPayloadToggle />
      <ThemeToggle />
      <button type="button" className="app-header-logout" onClick={disconnect}>
        Logout
      </button>
    </header>
  )
}
