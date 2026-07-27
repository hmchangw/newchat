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
