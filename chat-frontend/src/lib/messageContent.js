// Pure tokenizer that splits a message body into inline segments for
// rendering: plain text, @mentions, and links. No React, no I/O.
//
// The mention grammar mirrors the server's pkg/mention parser exactly so the
// client highlights precisely what the backend treated as a mention:
//   - `@` must sit at the start of the string or immediately after whitespace
//     (so `bob@example.com` is NOT a mention),
//   - the account token is `[0-9a-zA-Z_-]+(.[0-9a-zA-Z_-]+)*`,
//   - `@all` (any case) is the group mention,
//   - individual `@account` tokens are highlighted only when the server
//     resolved them into `mentions[]` (unknown accounts render as plain text,
//     matching pkg/mention.ResolveFromParsed dropping unknowns).

const MENTION_RE = /(^|\s)@([0-9a-zA-Z_-]+(?:\.[0-9a-zA-Z_-]+)*)/g
const URL_RE = /https?:\/\/[^\s<]+/g
// Trailing characters that are almost always sentence punctuation, not part of
// the URL (e.g. "see https://x.io." → the dot is text).
const URL_TRAILING_PUNCT = /[),.;!?]+$/

/**
 * @typedef {{ type: 'text', text: string }
 *   | { type: 'mention', account: string, all: boolean, display: string }
 *   | { type: 'link', href: string, text: string }} Segment
 */

function mentionDisplay(participant) {
  return participant?.engName || participant?.account || ''
}

/**
 * Split `content` into ordered inline segments.
 *
 * @param {string | null | undefined} content
 * @param {{ account?: string, engName?: string }[]} [mentions]
 * @returns {Segment[]}
 */
export function parseMessageContent(content, mentions = []) {
  if (!content) return []

  const byAccount = new Map()
  for (const p of mentions ?? []) {
    if (p?.account) byAccount.set(p.account.toLowerCase(), p)
  }

  // Collect non-overlapping token spans (mentions + links), then emit the
  // text between them.
  const spans = []

  MENTION_RE.lastIndex = 0
  let m
  while ((m = MENTION_RE.exec(content)) !== null) {
    const lead = m[1] // '' at string start, or the single whitespace char
    const account = m[2].toLowerCase()
    const atStart = m.index + lead.length // index of '@'
    const end = atStart + 1 + m[2].length // '@' + token
    const isAll = account === 'all'
    if (isAll) {
      spans.push({ start: atStart, end, seg: { type: 'mention', account: 'all', all: true, display: 'all' } })
    } else if (byAccount.has(account)) {
      spans.push({
        start: atStart,
        end,
        seg: { type: 'mention', account, all: false, display: mentionDisplay(byAccount.get(account)) },
      })
    }
  }

  URL_RE.lastIndex = 0
  while ((m = URL_RE.exec(content)) !== null) {
    const href = m[0].replace(URL_TRAILING_PUNCT, '')
    if (!href) continue
    spans.push({ start: m.index, end: m.index + href.length, seg: { type: 'link', href, text: href } })
  }

  // Earliest start first; on a tie prefer the longer span.
  spans.sort((a, b) => a.start - b.start || b.end - a.end)

  const segments = []
  let cursor = 0
  for (const span of spans) {
    if (span.start < cursor) continue // overlaps an already-emitted span; skip
    if (span.start > cursor) segments.push({ type: 'text', text: content.slice(cursor, span.start) })
    segments.push(span.seg)
    cursor = span.end
  }
  if (cursor < content.length) segments.push({ type: 'text', text: content.slice(cursor) })
  return segments
}
