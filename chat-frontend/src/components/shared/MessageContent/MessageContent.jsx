import { useMemo } from 'react'
import { parseMessageContent } from '@/lib/messageContent'
import './style.css'

/**
 * Render a message body with inline @mentions, links, and a markdown subset
 * (bold / italic / strikethrough / inline code / fenced code blocks)
 * highlighted. Plain-text runs render verbatim (React escapes them); newlines
 * are preserved by the caller's `white-space: pre-wrap`.
 *
 * @param {object} props
 * @param {string} props.content
 * @param {{ account?: string, engName?: string }[]} [props.mentions]
 * @param {string} [props.selfAccount] current user's account — self-mentions
 *   get an extra class so the reader's own name stands out.
 */
export default function MessageContent({ content, mentions, selfAccount }) {
  const nodes = useMemo(() => parseMessageContent(content, mentions), [content, mentions])
  const self = selfAccount ? String(selfAccount).toLowerCase() : null
  return <>{renderNodes(nodes, self)}</>
}

function renderNodes(nodes, self, keyPrefix = '') {
  return nodes.map((node, i) => renderNode(node, `${keyPrefix}${i}`, self))
}

function renderNode(node, key, self) {
  switch (node.type) {
    case 'mention': {
      const classes = ['mention']
      if (node.all) classes.push('mention-all')
      if (!node.all && self && node.account === self) classes.push('mention-self')
      return (
        <span key={key} className={classes.join(' ')} data-account={node.account}>
          @{node.display}
        </span>
      )
    }
    case 'link':
      return (
        <a key={key} className="message-link" href={node.href} target="_blank" rel="noopener noreferrer">
          {node.text}
        </a>
      )
    case 'code':
      return (
        <code key={key} className="message-code">
          {node.text}
        </code>
      )
    case 'codeblock':
      return (
        <pre key={key} className="message-codeblock">
          <code>{node.text}</code>
        </pre>
      )
    case 'strong':
      return <strong key={key}>{renderNodes(node.children, self, `${key}-`)}</strong>
    case 'em':
      return <em key={key}>{renderNodes(node.children, self, `${key}-`)}</em>
    case 'del':
      return <del key={key}>{renderNodes(node.children, self, `${key}-`)}</del>
    default:
      return <span key={key}>{node.text}</span>
  }
}
