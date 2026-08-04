import './style.css'

// A shortcode that contains any non-ASCII codepoint is a literal unicode emoji
// (render as-is); a purely ASCII shortcode is a name (e.g. acme_party) shown as
// :name:. Resolving named/custom shortcodes to glyphs or images is a separate
// emoji-picker / emoji.list follow-up.
function hasGlyph(shortcode) {
  for (let i = 0; i < shortcode.length; i += 1) {
    if (shortcode.charCodeAt(i) > 127) return true
  }
  return false
}

function label(shortcode) {
  return hasGlyph(shortcode) ? shortcode : `:${shortcode}:`
}

/**
 * Render a message's reaction chips: one per shortcode with a reactor count and
 * a title listing the reactors. Chips the current user reacted to are marked.
 *
 * @param {object} props
 * @param {Record<string, { account: string, displayName?: string }[]>} [props.reactions]
 * @param {string} [props.selfAccount]
 */
export default function MessageReactions({ reactions, selfAccount }) {
  const entries = reactions
    ? Object.entries(reactions).filter(([, users]) => Array.isArray(users) && users.length > 0)
    : []
  if (entries.length === 0) return null
  const self = selfAccount ? String(selfAccount).toLowerCase() : null

  return (
    <ul className="message-reactions">
      {entries.map(([shortcode, users]) => {
        const mine = self && users.some((u) => u.account?.toLowerCase() === self)
        const title = users.map((u) => u.displayName || u.account).join(', ')
        return (
          <li
            key={shortcode}
            className={`reaction-chip${mine ? ' reaction-chip-mine' : ''}`}
            title={title}
          >
            <span className="reaction-emoji">{label(shortcode)}</span>
            <span className="reaction-count">{users.length}</span>
          </li>
        )
      })}
    </ul>
  )
}
