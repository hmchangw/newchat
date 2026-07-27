import { describe, it, expect } from 'vitest'
import { attachmentKind, attachmentSize, formatFileSize, mediaUrl } from './attachment'

describe('attachmentKind', () => {
  it('classifies by the present media URL', () => {
    expect(attachmentKind({ imageUrl: 'x' })).toBe('image')
    expect(attachmentKind({ audioUrl: 'x' })).toBe('audio')
    expect(attachmentKind({ videoUrl: 'x' })).toBe('video')
  })

  it('falls back to the fileType MIME family', () => {
    expect(attachmentKind({ fileType: 'image/png' })).toBe('image')
    expect(attachmentKind({ fileType: 'audio/mpeg' })).toBe('audio')
    expect(attachmentKind({ fileType: 'video/mp4' })).toBe('video')
    expect(attachmentKind({ fileType: 'application/pdf' })).toBe('file')
  })

  it('defaults to file when nothing matches', () => {
    expect(attachmentKind({})).toBe('file')
    expect(attachmentKind(null)).toBe('file')
  })
})

describe('attachmentSize', () => {
  it('returns the size for the matching media family', () => {
    expect(attachmentSize({ imageUrl: 'x', imageSize: 10 })).toBe(10)
    expect(attachmentSize({ audioUrl: 'x', audioSize: 20 })).toBe(20)
    expect(attachmentSize({ videoUrl: 'x', videoSize: 30 })).toBe(30)
  })

  it('returns 0 when no size is present', () => {
    expect(attachmentSize({ fileType: 'application/pdf' })).toBe(0)
  })
})

describe('formatFileSize', () => {
  it('formats bytes into human units', () => {
    expect(formatFileSize(0)).toBe('')
    expect(formatFileSize(512)).toBe('512 B')
    expect(formatFileSize(1024)).toBe('1.0 KB')
    expect(formatFileSize(1536)).toBe('1.5 KB')
    expect(formatFileSize(1048576)).toBe('1.0 MB')
    expect(formatFileSize(5 * 1048576)).toBe('5.0 MB')
  })
})

describe('mediaUrl', () => {
  it('joins the gateway base with a relative titleLink', () => {
    expect(mediaUrl('https://site.example.com', 'api/v1/file/rooms/r1/file/f1')).toBe(
      'https://site.example.com/api/v1/file/rooms/r1/file/f1',
    )
  })

  it('normalizes a trailing slash on the base and a leading slash on the path', () => {
    expect(mediaUrl('https://site.example.com/', '/api/v1/file/x')).toBe(
      'https://site.example.com/api/v1/file/x',
    )
  })

  it('returns an empty string when base or path is missing', () => {
    expect(mediaUrl('', 'api/v1/file/x')).toBe('')
    expect(mediaUrl('https://site.example.com', '')).toBe('')
  })

  it('passes an already-absolute titleLink through unchanged', () => {
    expect(mediaUrl('https://site.example.com', 'https://cdn.example.com/f')).toBe(
      'https://cdn.example.com/f',
    )
  })
})
