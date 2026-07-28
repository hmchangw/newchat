import { attachmentKind, attachmentSize, formatFileSize, mediaUrl } from '@/lib/attachment'
import './style.css'

const KIND_ICON = { image: '🖼️', audio: '🎵', video: '🎬', file: '📎' }
const KIND_LABEL = { image: 'Image', audio: 'Audio', video: 'Video', file: 'File' }

/**
 * Render a message's attachments: images inline (with a blurred-preview
 * fallback), everything else as a download chip. `baseUrl` is the media gateway
 * origin; image/download URLs are built from it + the attachment's relative
 * titleLink. Those URLs hit the protected file endpoint — establishing the
 * download session (the ssoToken cookie the backend's setCookie route issues
 * for header-less <img>/<a> requests) is a follow-up; until then the inline
 * image degrades to the embedded preview on load failure.
 *
 * @param {object} props
 * @param {import('@/api/types').Attachment[]} [props.attachments]
 * @param {string} [props.baseUrl]
 */
export default function MessageAttachments({ attachments, baseUrl }) {
  if (!attachments?.length) return null
  return (
    <div className="message-attachments">
      {attachments.map((att, i) => (
        <AttachmentItem key={att.id || i} att={att} baseUrl={baseUrl} />
      ))}
    </div>
  )
}

function AttachmentItem({ att, baseUrl }) {
  const kind = attachmentKind(att)
  const href = mediaUrl(baseUrl, att.titleLink)
  const preview = att.imagePreview ? `data:image/jpeg;base64,${att.imagePreview}` : undefined

  if (kind === 'image') {
    // Prefer a local object URL (an optimistic just-sent image) so the sender
    // sees it instantly; otherwise load from the gateway URL, falling back to
    // the embedded blurred preview if that fails (auth/network) or is absent.
    const src = att.localUrl || href || preview
    return (
      <a
        className="attachment attachment-image"
        href={href || undefined}
        target="_blank"
        rel="noopener noreferrer"
        title={att.title}
      >
        <img
          className="attachment-image-img"
          src={src}
          alt={att.title || 'image attachment'}
          loading="lazy"
          width={att.imageDimensions?.width || undefined}
          height={att.imageDimensions?.height || undefined}
          onError={(e) => {
            if (preview && e.currentTarget.src !== preview) e.currentTarget.src = preview
          }}
        />
      </a>
    )
  }

  const sizeText = formatFileSize(attachmentSize(att))
  const sub = [KIND_LABEL[kind], sizeText].filter(Boolean).join(' · ')
  return (
    <a
      className={`attachment attachment-file attachment-${kind}`}
      href={href || undefined}
      target="_blank"
      rel="noopener noreferrer"
      download={att.title || undefined}
    >
      <span className="attachment-icon" aria-hidden="true">
        {KIND_ICON[kind]}
      </span>
      <span className="attachment-meta">
        <span className="attachment-title">{att.title || 'attachment'}</span>
        {sub && <span className="attachment-sub">{sub}</span>}
      </span>
    </a>
  )
}
