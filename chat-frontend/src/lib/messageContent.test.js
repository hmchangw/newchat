import { describe, it, expect } from 'vitest'
import { parseMessageContent } from './messageContent'

describe('parseMessageContent', () => {
  it('returns an empty array for empty / nullish content', () => {
    expect(parseMessageContent('', [])).toEqual([])
    expect(parseMessageContent(null, [])).toEqual([])
    expect(parseMessageContent(undefined)).toEqual([])
  })

  it('returns a single text segment when there are no inline tokens', () => {
    expect(parseMessageContent('just plain text', [])).toEqual([
      { type: 'text', text: 'just plain text' },
    ])
  })

  it('highlights a resolved @mention and resolves its display name from mentions[]', () => {
    const segs = parseMessageContent('hi @alice', [
      { account: 'alice', engName: 'Alice Lee' },
    ])
    expect(segs).toEqual([
      { type: 'text', text: 'hi ' },
      { type: 'mention', account: 'alice', all: false, display: 'Alice Lee' },
    ])
  })

  it('matches mentions case-insensitively but keeps the resolved display name', () => {
    const segs = parseMessageContent('ping @Alice now', [
      { account: 'alice', engName: 'Alice Lee' },
    ])
    expect(segs).toEqual([
      { type: 'text', text: 'ping ' },
      { type: 'mention', account: 'alice', all: false, display: 'Alice Lee' },
      { type: 'text', text: ' now' },
    ])
  })

  it('falls back to the account when the mention has no engName', () => {
    const segs = parseMessageContent('@bob', [{ account: 'bob' }])
    expect(segs).toEqual([{ type: 'mention', account: 'bob', all: false, display: 'bob' }])
  })

  it('always highlights @all as a group mention (case-insensitive), even with no mentions[]', () => {
    expect(parseMessageContent('heads up @all', [])).toEqual([
      { type: 'text', text: 'heads up ' },
      { type: 'mention', account: 'all', all: true, display: 'all' },
    ])
    expect(parseMessageContent('@All team', [])).toEqual([
      { type: 'mention', account: 'all', all: true, display: 'all' },
      { type: 'text', text: ' team' },
    ])
  })

  it('does NOT highlight an @token that is not a resolved mention', () => {
    // Server only puts resolved users in mentions[]; an unresolved @token is
    // plain text, mirroring pkg/mention.ResolveFromParsed dropping unknowns.
    expect(parseMessageContent('@ghost was here', [])).toEqual([
      { type: 'text', text: '@ghost was here' },
    ])
  })

  it('does NOT treat an email address as a mention (needs start-or-whitespace before @)', () => {
    expect(parseMessageContent('mail bob@example.com', [{ account: 'example', engName: 'X' }])).toEqual([
      { type: 'text', text: 'mail bob@example.com' },
    ])
  })

  it('linkifies http(s) URLs', () => {
    expect(parseMessageContent('see https://example.com/x for more', [])).toEqual([
      { type: 'text', text: 'see ' },
      { type: 'link', href: 'https://example.com/x', text: 'https://example.com/x' },
      { type: 'text', text: ' for more' },
    ])
  })

  it('trims trailing sentence punctuation off a URL', () => {
    expect(parseMessageContent('open https://example.com.', [])).toEqual([
      { type: 'text', text: 'open ' },
      { type: 'link', href: 'https://example.com', text: 'https://example.com' },
      { type: 'text', text: '.' },
    ])
  })

  it('handles multiple mentions and a link in one message', () => {
    const segs = parseMessageContent('@alice @all https://x.io done', [
      { account: 'alice', engName: 'Alice' },
    ])
    expect(segs).toEqual([
      { type: 'mention', account: 'alice', all: false, display: 'Alice' },
      { type: 'text', text: ' ' },
      { type: 'mention', account: 'all', all: true, display: 'all' },
      { type: 'text', text: ' ' },
      { type: 'link', href: 'https://x.io', text: 'https://x.io' },
      { type: 'text', text: ' done' },
    ])
  })

  it('preserves newlines in text segments', () => {
    expect(parseMessageContent('line1\nline2', [])).toEqual([
      { type: 'text', text: 'line1\nline2' },
    ])
  })
})

describe('parseMessageContent — markdown', () => {
  it('parses bold, italic and strikethrough into wrapper nodes', () => {
    expect(parseMessageContent('**b**', [])).toEqual([
      { type: 'strong', children: [{ type: 'text', text: 'b' }] },
    ])
    expect(parseMessageContent('*i*', [])).toEqual([
      { type: 'em', children: [{ type: 'text', text: 'i' }] },
    ])
    expect(parseMessageContent('~~s~~', [])).toEqual([
      { type: 'del', children: [{ type: 'text', text: 's' }] },
    ])
  })

  it('parses inline code as a literal (no inner parsing)', () => {
    expect(parseMessageContent('`@alice *x*`', [{ account: 'alice' }])).toEqual([
      { type: 'code', text: '@alice *x*' },
    ])
  })

  it('parses a fenced code block literally', () => {
    expect(parseMessageContent('```\nx = 1\n```', [])).toEqual([
      { type: 'codeblock', text: 'x = 1' },
    ])
  })

  it('strips the info string off a fenced block', () => {
    expect(parseMessageContent('```js\nconst a = 1\n```', [])).toEqual([
      { type: 'codeblock', text: 'const a = 1' },
    ])
  })

  it('resolves mentions nested inside emphasis', () => {
    expect(parseMessageContent('**@alice**', [{ account: 'alice', engName: 'Alice' }])).toEqual([
      {
        type: 'strong',
        children: [{ type: 'mention', account: 'alice', all: false, display: 'Alice' }],
      },
    ])
  })

  it('does not let underscores in a URL break the link', () => {
    expect(parseMessageContent('see https://x.io/a_b_c done', [])).toEqual([
      { type: 'text', text: 'see ' },
      { type: 'link', href: 'https://x.io/a_b_c', text: 'https://x.io/a_b_c' },
      { type: 'text', text: ' done' },
    ])
  })

  it('does not italicize intraword underscores or asterisks', () => {
    expect(parseMessageContent('foo_bar_baz', [])).toEqual([{ type: 'text', text: 'foo_bar_baz' }])
    expect(parseMessageContent('2*3*4', [])).toEqual([{ type: 'text', text: '2*3*4' }])
  })

  it('mixes markdown with plain text and links', () => {
    expect(parseMessageContent('see **bold** at https://x.io', [])).toEqual([
      { type: 'text', text: 'see ' },
      { type: 'strong', children: [{ type: 'text', text: 'bold' }] },
      { type: 'text', text: ' at ' },
      { type: 'link', href: 'https://x.io', text: 'https://x.io' },
    ])
  })

  it('supports underscore/double-underscore emphasis at word boundaries', () => {
    expect(parseMessageContent('a _i_ b', [])).toEqual([
      { type: 'text', text: 'a ' },
      { type: 'em', children: [{ type: 'text', text: 'i' }] },
      { type: 'text', text: ' b' },
    ])
    expect(parseMessageContent('__b__', [])).toEqual([
      { type: 'strong', children: [{ type: 'text', text: 'b' }] },
    ])
  })
})
