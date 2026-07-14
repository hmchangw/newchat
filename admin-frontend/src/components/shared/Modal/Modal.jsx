import { useEffect } from 'react'

/** Reusable base dialog. Overlay click or Esc closes it (Esc captured at window level,
 * capture phase + stopPropagation, so the dialog claims it over any ancestor handler). */
export default function Modal({ onClose, children, labelledBy }) {
  useEffect(() => {
    const handler = (e) => {
      if (e.key === 'Escape') {
        e.preventDefault()
        e.stopPropagation()
        onClose?.()
      }
    }
    window.addEventListener('keydown', handler, true)
    return () => window.removeEventListener('keydown', handler, true)
  }, [onClose])

  return (
    <div
      className="dialog-overlay"
      onMouseDown={(e) => {
        // Backdrop click → close. Only when the actual overlay (not a child)
        // is the mousedown target.
        if (e.target === e.currentTarget) onClose?.()
      }}
    >
      <div
        className="dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby={labelledBy}
        onMouseDown={(e) => e.stopPropagation()}
      >
        {children}
      </div>
    </div>
  )
}
