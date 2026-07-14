import { Component } from 'react'
import './style.css'

/** Catches render errors in the subtree so a stray bug doesn't blank the whole admin session.
 * Does NOT catch event-handler/async/effect errors (React's own rules) — those need a window listener. */
export default class ErrorBoundary extends Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, info) {
    // In prod we'd ship this to an error tracker (Sentry or similar) instead.
    // eslint-disable-next-line no-console
    console.error('[ErrorBoundary] render error:', error, info?.componentStack)
  }

  /** Soft recovery: clears the error and re-renders children, preserving in-memory state. */
  handleReset = () => {
    this.setState({ error: null })
  }

  handleReload = () => {
    window.location.reload()
  }

  render() {
    if (!this.state.error) return this.props.children
    if (this.props.fallback) {
      return typeof this.props.fallback === 'function'
        ? this.props.fallback({
            error: this.state.error,
            reset: this.handleReset,
            reload: this.handleReload,
          })
        : this.props.fallback
    }
    return (
      <div className="error-boundary" role="alert">
        <div className="error-boundary-card">
          <h1 className="error-boundary-title">Something went wrong</h1>
          <p className="error-boundary-message">
            The app hit an unexpected error. Try Again keeps your session;
            Reload starts fresh.
          </p>
          {import.meta.env.DEV && this.state.error?.message && (
            <pre className="error-boundary-detail">{this.state.error.message}</pre>
          )}
          <div className="error-boundary-actions">
            <button
              type="button"
              className="btn btn-primary"
              onClick={this.handleReset}
              autoFocus
            >
              Try Again
            </button>
            <button type="button" className="btn" onClick={this.handleReload}>
              Reload
            </button>
          </div>
        </div>
      </div>
    )
  }
}
