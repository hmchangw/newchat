import './style.css'

/** Suspense fallback for lazy-loaded surfaces: `variant="dialog"` overlays the dialog chrome,
 * `variant="inline"` (default) fills the parent panel. `label` is optional; aria-busy is always set. */
export default function LazyFallback({ variant = 'inline', label }) {
  if (variant === 'dialog') {
    return (
      <div className="dialog-overlay" aria-busy="true" aria-live="polite">
        <div className="lazy-fallback-spinner" />
        {label && <span className="lazy-fallback-label">{label}</span>}
      </div>
    )
  }
  return (
    <div className="lazy-fallback-inline" aria-busy="true" aria-live="polite">
      <div className="lazy-fallback-spinner" />
      {label && <span className="lazy-fallback-label">{label}</span>}
    </div>
  )
}
