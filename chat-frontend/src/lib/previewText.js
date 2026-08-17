// Flatten a message body into a single-line plain-text snippet for the sidebar
// room list. Reuses the message-content tokenizer so the snippet and the
// rendered message agree on what the body says. No React, no I/O.

import { parseMessageContent } from './messageContent'
import { attachmentKind } from './attachment'

// Hard cap on the returned string. CSS ellipsis truncates visually but leaves
// the whole string in the DOM, so without this a multi-thousand-character
// message sits in the sidebar for as long as it's the room's latest.
export const PREVIEW_MAX_LENGTH = 140

// Headroom applied BEFORE tokenizing: previewText runs on the reducer hot
// path for every message in every room (not just the ones being rendered),
// so parseMessageContent — recursive, O(body length) — must never see a
// full multi-KB body just to produce a 140-char snippet. The margin has to
// be generous rather than exactly PREVIEW_MAX_LENGTH because markup near the
// boundary can both shrink (**bold** -> bold) and grow (@alice -> @Alice
// Chen) once parsed, so the raw prefix needs slack on both sides to still
// resolve correctly.
const PARSE_SLICE_MARGIN = 500
const PARSE_SLICE_LENGTH = PREVIEW_MAX_LENGTH + PARSE_SLICE_MARGIN

const KIND_LABEL = { image: 'Photo', audio: 'Audio', video: 'Video' }

/**
 * Flatten a message body to a single line of plain text.
 *
 * @param {string | null | undefined} content
 * @param {{ account?: string, engName?: string }[]} [mentions]
 * @returns {string} '' when there is nothing to show
 */
export function previewText(content, mentions = []) {
  if (!content) return ''
  const bounded = safeSlice(content, PARSE_SLICE_LENGTH)
  const flat = flattenNodes(parseMessageContent(bounded, mentions))
  const collapsed = flat.replace(/\s+/g, ' ').trim()
  return safeSlice(collapsed, PREVIEW_MAX_LENGTH)
}

// Slice to at most maxLength UTF-16 code units without splitting a surrogate
// pair. A plain slice(0, N) can land between a high surrogate (0xD800-
// 0xDBFF) and its low surrogate, leaving a lone surrogate that renders as
// U+FFFD — drop the whole pair instead of half of it.
function safeSlice(str, maxLength) {
  if (str.length <= maxLength) return str
  let end = maxLength
  const code = str.charCodeAt(end - 1)
  if (code >= 0xd800 && code <= 0xdbff) end -= 1
  return str.slice(0, end)
}

/**
 * Label for a message whose body is empty but which carries attachments.
 *
 * @param {import('../api/types').Attachment[] | null | undefined} attachments
 * @returns {string} '' when there are none
 */
export function attachmentFallbackText(attachments) {
  const first = attachments?.[0]
  if (!first) return ''
  return KIND_LABEL[attachmentKind(first)] ?? (first.title || 'File')
}

/**
 * The sidebar snippet for a message: its flattened body, or an attachment
 * label when the body is empty. Single entry point for both preview writers.
 *
 * @param {string | null | undefined} content
 * @param {{ account?: string, engName?: string }[]} [mentions]
 * @param {import('../api/types').Attachment[] | null | undefined} [attachments]
 * @returns {string}
 */
export function previewSnippet(content, mentions, attachments) {
  return previewText(content, mentions) || attachmentFallbackText(attachments)
}

function flattenNodes(nodes) {
  let out = ''
  for (const node of nodes) out += flattenNode(node)
  return out
}

function flattenNode(node) {
  switch (node.type) {
    case 'mention':
      return `@${node.display}`
    // link/code/codeblock all carry their visible text on `.text`; a link's
    // href is deliberately never emitted separately.
    case 'link':
    case 'code':
    case 'codeblock':
      return node.text
    case 'strong':
    case 'em':
    case 'del':
      return flattenNodes(node.children)
    default:
      return node.text ?? ''
  }
}
