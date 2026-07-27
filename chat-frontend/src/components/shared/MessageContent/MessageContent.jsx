import { useMemo } from 'react'
import { parseMessageContent } from '@/lib/messageContent'
import './style.css'

/**
 * Render a message body with inline @mentions and links highlighted.
 * Plain-text segments render verbatim (React escapes them); newlines are
 * preserved by the caller's `white-space: pre-wrap`.
 *
 * @param {object} props
 * @param {string} props.content
 * @param {{ account?: string, engName?: string }[]} [props.mentions]
 * @param {string} [props.selfAccount] current user's account — self-mentions
 *   get an extra class so the reader's own name stands out.
 */
export default function MessageContent({ content, mentions, selfAccount }) {
  const segments = useMemo(() => parseMessageContent(content, mentions), [content, mentions])
  const self = selfAccount ? String(selfAccount).toLowerCase() : null

  return (
    <>
      {segments.map((seg, i) => {
        if (seg.type === 'mention') {
          const classes = ['mention']
          if (seg.all) classes.push('mention-all')
          if (!seg.all && self && seg.account === self) classes.push('mention-self')
          return (
            <span key={i} className={classes.join(' ')} data-account={seg.account}>
              @{seg.display}
            </span>
          )
        }
        if (seg.type === 'link') {
          return (
            <a
              key={i}
              className="message-link"
              href={seg.href}
              target="_blank"
              rel="noopener noreferrer"
            >
              {seg.text}
            </a>
          )
        }
        return <span key={i}>{seg.text}</span>
      })}
    </>
  )
}
