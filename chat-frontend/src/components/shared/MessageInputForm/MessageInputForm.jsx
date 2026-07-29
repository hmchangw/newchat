import { useRef } from 'react'
import QuotedBlock from '../QuotedBlock/QuotedBlock'
import './style.css'

export default function MessageInputForm({
  value,
  onChange,
  onSubmit,
  placeholder,
  disabled,
  quotedTarget,
  onClearQuote,
  inputRef,
  // Image attach (optional — only the room composer wires these).
  onPickImage,
  pendingImage,
  onClearImage,
}) {
  const fileInputRef = useRef(null)

  const hasImage = !!pendingImage
  const canSubmit = !disabled && ((value && value.trim().length > 0) || hasImage)

  const handleSubmit = (e) => {
    e?.preventDefault?.()
    if (!canSubmit) return
    onSubmit()
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSubmit(e)
    }
  }

  const handleFileChange = (e) => {
    const file = e.target.files?.[0]
    if (file) onPickImage?.(file)
    // Reset so picking the same file again re-fires change.
    e.target.value = ''
  }

  return (
    <form className="message-input-form" onSubmit={handleSubmit}>
      {quotedTarget && (
        <QuotedBlock variant="chip" snapshot={quotedTarget} onClear={onClearQuote} />
      )}
      {pendingImage && (
        <div className="message-input-attachment">
          <img className="message-input-attachment-thumb" src={pendingImage.url} alt={pendingImage.name} />
          <span className="message-input-attachment-name">{pendingImage.name}</span>
          <button
            type="button"
            className="message-input-attachment-remove"
            aria-label="Remove image"
            onClick={onClearImage}
          >
            ✕
          </button>
        </div>
      )}
      <div className="message-input-row">
        {onPickImage && (
          <>
            <button
              type="button"
              className="message-input-attach-btn"
              aria-label="Attach image"
              disabled={disabled}
              onClick={() => fileInputRef.current?.click()}
            >
              🖼️
            </button>
            <input
              ref={fileInputRef}
              type="file"
              accept="image/*"
              className="message-input-file"
              hidden
              onChange={handleFileChange}
            />
          </>
        )}
        <input
          ref={inputRef}
          type="text"
          value={value ?? ''}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          disabled={disabled}
        />
        <button type="submit" disabled={!canSubmit}>
          Send
        </button>
      </div>
    </form>
  )
}
