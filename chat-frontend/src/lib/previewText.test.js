import { describe, it, expect } from 'vitest'
import { previewText, attachmentFallbackText, previewSnippet, PREVIEW_MAX_LENGTH } from './previewText'

describe('previewText', () => {
  it('returns plain text unchanged', () => {
    expect(previewText('hello there')).toBe('hello there')
  })

  it('returns empty string for empty, null, and undefined input', () => {
    expect(previewText('')).toBe('')
    expect(previewText(null)).toBe('')
    expect(previewText(undefined)).toBe('')
  })

  it('drops markdown emphasis markers but keeps the text', () => {
    expect(previewText('**bold** and *italic* and ~~struck~~')).toBe('bold and italic and struck')
  })

  it('flattens nested emphasis', () => {
    expect(previewText('**bold with _inner italic_ inside**')).toBe('bold with inner italic inside')
  })

  it('leaves the outer markers literal when emphasis nests with the same marker', () => {
    // The tokenizer's strong pattern forbids `*` inside its captured group, so
    // `**a *b* c**` never matches as strong — the outer asterisks stay literal.
    expect(previewText('**bold with *inner italic* inside**')).toBe('**bold with inner italic inside**')
  })

  it('drops inline code backticks', () => {
    expect(previewText('run `npm test` now')).toBe('run npm test now')
  })

  it('keeps fenced code block contents and collapses their newlines', () => {
    expect(previewText('see:\n```\nline one\nline two\n```')).toBe('see: line one line two')
  })

  it('leaves an unterminated fence as literal text', () => {
    expect(previewText('oops ```never closed')).toBe('oops ```never closed')
  })

  it('renders a resolved mention as @ plus the display name', () => {
    const mentions = [{ account: 'alice', engName: 'Alice Chen' }]
    expect(previewText('hey @alice ping', mentions)).toBe('hey @Alice Chen ping')
  })

  it('renders @all as @all', () => {
    expect(previewText('@all standup now')).toBe('@all standup now')
  })

  it('leaves an unresolved mention as literal text', () => {
    expect(previewText('hey @nobody there', [])).toBe('hey @nobody there')
  })

  it('renders a link as its visible label, never a separate href', () => {
    expect(previewText('see https://example.com/x now')).toBe('see https://example.com/x now')
  })

  it('collapses newlines and runs of whitespace to single spaces, then trims', () => {
    expect(previewText('  line one\n\n  line   two \t three  ')).toBe('line one line two three')
  })

  it('caps the result at PREVIEW_MAX_LENGTH characters', () => {
    const long = 'x'.repeat(500)
    const out = previewText(long)
    expect(out).toHaveLength(PREVIEW_MAX_LENGTH)
    expect(PREVIEW_MAX_LENGTH).toBe(140)
  })

  it('does not split a surrogate pair straddling the PREVIEW_MAX_LENGTH boundary', () => {
    // 139 plain chars + a 2-code-unit emoji puts the emoji's code units at
    // indices 139/140 — a naive slice(0, 140) keeps only the high surrogate,
    // leaving a lone surrogate that renders as U+FFFD.
    const content = 'a'.repeat(139) + '😀' + 'b'.repeat(20)
    const out = previewText(content)
    expect(out).toHaveLength(139)
    expect(out).toBe('a'.repeat(139))
    // No lone surrogate anywhere in the output.
    expect(/[\uD800-\uDFFF]/.test(out)).toBe(false)
  })

  it('applies the full cap to a markdown-heavy body that shrinks when flattened', () => {
    // `**x**` flattens 5:1, so a bounded pre-parse would yield fewer than the
    // cap here even though the full body has more than enough content.
    expect(previewText('**x**'.repeat(150))).toHaveLength(PREVIEW_MAX_LENGTH)
  })
})

describe('attachmentFallbackText', () => {
  it('returns empty string for no attachments', () => {
    expect(attachmentFallbackText(undefined)).toBe('')
    expect(attachmentFallbackText([])).toBe('')
  })

  it('labels an image Photo', () => {
    expect(attachmentFallbackText([{ imageUrl: '/a.png' }])).toBe('Photo')
  })

  it('labels audio and video', () => {
    expect(attachmentFallbackText([{ audioUrl: '/a.mp3' }])).toBe('Audio')
    expect(attachmentFallbackText([{ videoUrl: '/a.mp4' }])).toBe('Video')
  })

  it('uses a generic file attachment title', () => {
    expect(attachmentFallbackText([{ title: 'report.pdf', fileType: 'application/pdf' }])).toBe('report.pdf')
  })

  it('falls back to File for an untitled file attachment', () => {
    expect(attachmentFallbackText([{ title: '', fileType: 'application/pdf' }])).toBe('File')
  })

  it('classifies by the first attachment only', () => {
    expect(attachmentFallbackText([{ imageUrl: '/a.png' }, { title: 'b.pdf' }])).toBe('Photo')
  })
})

describe('previewSnippet', () => {
  it('prefers text over the attachment fallback', () => {
    expect(previewSnippet('look at this', [], [{ imageUrl: '/a.png' }])).toBe('look at this')
  })

  it('falls back to the attachment label when the text is empty', () => {
    expect(previewSnippet('', [], [{ imageUrl: '/a.png' }])).toBe('Photo')
  })

  it('returns empty string when there is neither text nor attachments', () => {
    expect(previewSnippet('', [], [])).toBe('')
  })
})
