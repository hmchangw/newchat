// Pure helpers for rendering message attachments. No React, no I/O.

/**
 * @typedef {'image' | 'audio' | 'video' | 'file'} AttachmentKind
 */

/**
 * Classify an attachment. Prefers the presence of a media-specific URL (set by
 * upload-service for the matching family), then falls back to the fileType MIME
 * family, then to a generic file.
 *
 * @param {import('../api/types').Attachment | null | undefined} att
 * @returns {AttachmentKind}
 */
export function attachmentKind(att) {
  if (!att) return 'file'
  if (att.imageUrl) return 'image'
  if (att.audioUrl) return 'audio'
  if (att.videoUrl) return 'video'
  const ft = att.fileType || ''
  if (ft.startsWith('image/')) return 'image'
  if (ft.startsWith('audio/')) return 'audio'
  if (ft.startsWith('video/')) return 'video'
  return 'file'
}

/**
 * Byte size for the attachment's media family (0 when unknown).
 * @param {import('../api/types').Attachment | null | undefined} att
 * @returns {number}
 */
export function attachmentSize(att) {
  if (!att) return 0
  return att.imageSize || att.audioSize || att.videoSize || 0
}

/**
 * Human-readable byte size (e.g. "1.5 KB"). Empty string for 0/unknown so the
 * caller can omit it entirely.
 * @param {number} bytes
 * @returns {string}
 */
export function formatFileSize(bytes) {
  if (!bytes || bytes <= 0) return ''
  if (bytes < 1024) return `${bytes} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let value = bytes / 1024
  let unit = 0
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024
    unit += 1
  }
  return `${value.toFixed(1)} ${units[unit]}`
}

/**
 * Build the absolute media URL from the gateway base and a relative titleLink.
 * `titleLink` is relative to the media gateway; an already-absolute URL passes
 * through. Returns '' when either input is missing so callers can skip the link.
 *
 * @param {string | null | undefined} baseUrl
 * @param {string | null | undefined} relativePath
 * @returns {string}
 */
export function mediaUrl(baseUrl, relativePath) {
  if (!baseUrl || !relativePath) return ''
  if (/^https?:\/\//i.test(relativePath)) return relativePath
  return `${baseUrl.replace(/\/+$/, '')}/${relativePath.replace(/^\/+/, '')}`
}
