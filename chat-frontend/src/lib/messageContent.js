// Pure tokenizer that turns a message body into a tree of render nodes:
// plain text, @mentions, links, and a small markdown subset (bold, italic,
// strikethrough, inline code, fenced code blocks). No React, no I/O.
//
// Node shapes:
//   { type: 'text', text }
//   { type: 'mention', account, all, display }
//   { type: 'link', href, text }
//   { type: 'code', text }                 // inline code — literal
//   { type: 'codeblock', text }            // fenced block — literal
//   { type: 'strong' | 'em' | 'del', children: Node[] }
//
// Markdown flavor is CommonMark-ish: **bold**/__bold__, *italic*/_italic_,
// ~~strike~~, `code`, ```fenced```. Emphasis is guarded against intraword
// matches (foo_bar, 2*3) and code spans/blocks are literal (no nested parsing).
// The mention grammar mirrors the server's pkg/mention parser: `@` only at
// start-or-after-whitespace, `@all` is the group mention, and individual
// `@account` tokens are highlighted only when resolved into mentions[].

const MENTION_RE = /(^|\s)@([0-9a-zA-Z_-]+(?:\.[0-9a-zA-Z_-]+)*)/g
const URL_RE = /https?:\/\/[^\s<]+/
const URL_TRAILING_PUNCT = /[),.;!?]+$/
const FENCE_RE = /```[^\n]*\n?([\s\S]*?)```/g

// Inline markdown constructs, checked at each step. `order` breaks ties when two
// constructs start at the same index (lower wins). Emphasis uses lookarounds so
// intraword _ / * (foo_bar, 2*3) and the inner star of ** aren't matched.
const INLINE_CONSTRUCTS = [
  { type: 'code', order: 0, literal: true, re: /`([^`]+?)`/ },
  { type: 'strong', order: 1, re: /\*\*([^*]+?)\*\*/ },
  { type: 'strong', order: 1, re: /(?<![\w])__([^_]+?)__(?![\w])/ },
  { type: 'del', order: 2, re: /~~([^~]+?)~~/ },
  { type: 'em', order: 3, re: /(?<![\w*])\*([^*]+?)\*(?![\w*])/ },
  { type: 'em', order: 3, re: /(?<![\w])_([^_]+?)_(?![\w])/ },
]

function mentionDisplay(participant) {
  return participant?.engName || participant?.account || ''
}

/**
 * @param {string | null | undefined} content
 * @param {{ account?: string, engName?: string }[]} [mentions]
 * @returns {object[]} render-node tree
 */
export function parseMessageContent(content, mentions = []) {
  if (!content) return []
  const byAccount = new Map()
  for (const p of mentions ?? []) {
    if (p?.account) byAccount.set(p.account.toLowerCase(), p)
  }
  return parseBlocks(content, byAccount)
}

// Pull fenced code blocks out first (literal), inline-parse everything else.
function parseBlocks(text, byAccount) {
  const nodes = []
  let last = 0
  FENCE_RE.lastIndex = 0
  let m
  while ((m = FENCE_RE.exec(text)) !== null) {
    if (m.index > last) nodes.push(...parseInline(text.slice(last, m.index), byAccount))
    nodes.push({ type: 'codeblock', text: m[1].replace(/\n$/, '') })
    last = m.index + m[0].length
  }
  if (last < text.length) nodes.push(...parseInline(text.slice(last), byAccount))
  return nodes
}

// Recursively parse a run of inline text. Finds the earliest construct
// (markdown span, link, or mention), emits the plain text before it, the
// construct node, then recurses on the remainder.
function parseInline(text, byAccount) {
  if (!text) return []
  const hit = earliestConstruct(text, byAccount)
  if (!hit) return [{ type: 'text', text }]
  const out = []
  if (hit.start > 0) out.push({ type: 'text', text: text.slice(0, hit.start) })
  out.push(hit.node)
  return [...out, ...parseInline(text.slice(hit.end), byAccount)]
}

function earliestConstruct(text, byAccount) {
  const candidates = []

  for (const c of INLINE_CONSTRUCTS) {
    const mm = c.re.exec(text)
    if (!mm) continue
    candidates.push({
      start: mm.index,
      end: mm.index + mm[0].length,
      order: c.order,
      build: () =>
        c.literal
          ? { type: c.type, text: mm[1] }
          : { type: c.type, children: parseInline(mm[1], byAccount) },
    })
  }

  const link = URL_RE.exec(text)
  if (link) {
    const href = link[0].replace(URL_TRAILING_PUNCT, '')
    if (href) {
      candidates.push({
        start: link.index,
        end: link.index + href.length,
        order: 4,
        build: () => ({ type: 'link', href, text: href }),
      })
    }
  }

  const men = firstResolvedMention(text, byAccount)
  if (men) candidates.push({ ...men, order: 5, build: () => men.node })

  if (candidates.length === 0) return null
  candidates.sort((a, b) => a.start - b.start || a.order - b.order)
  const best = candidates[0]
  return { start: best.start, end: best.end, node: best.build() }
}

// First @mention token that resolves (either @all or a known account).
function firstResolvedMention(text, byAccount) {
  MENTION_RE.lastIndex = 0
  let m
  while ((m = MENTION_RE.exec(text)) !== null) {
    const lead = m[1]
    const account = m[2].toLowerCase()
    const atStart = m.index + lead.length
    const end = atStart + 1 + m[2].length
    if (account === 'all') {
      return { start: atStart, end, node: { type: 'mention', account: 'all', all: true, display: 'all' } }
    }
    if (byAccount.has(account)) {
      return {
        start: atStart,
        end,
        node: { type: 'mention', account, all: false, display: mentionDisplay(byAccount.get(account)) },
      }
    }
  }
  return null
}
