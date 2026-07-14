import { useDebug, DEBUG_LEVELS } from '@/context/DebugContext'
import './style.css'

// Lets the user pick how much diagnostic detail the backend should emit.
// 'off' sends no X-Debug header; the other levels are sent as the value.
export default function DebugLevelSelect() {
  const { level, setLevel } = useDebug()
  const active = level !== 'off'
  return (
    <label
      className={`debug-level-select${active ? ' debug-level-select--on' : ''}`}
      title="X-Debug header level"
    >
      <span className="debug-level-select__label">Debug</span>
      <select
        className="debug-level-select__control"
        aria-label="Debug header level"
        value={level}
        onChange={(e) => setLevel(e.target.value)}
      >
        {DEBUG_LEVELS.map((lvl) => (
          <option key={lvl} value={lvl}>{lvl}</option>
        ))}
      </select>
    </label>
  )
}
