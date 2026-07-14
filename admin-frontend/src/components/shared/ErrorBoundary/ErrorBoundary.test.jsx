import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import ErrorBoundary from './ErrorBoundary'

function Boom({ message = 'kaboom' }) {
  throw new Error(message)
}

describe('ErrorBoundary', () => {
  // The boundary deliberately logs the caught error. Silence that so the
  // test output stays clean — we still assert via getByText below.
  let consoleSpy
  beforeEach(() => {
    consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
  })
  afterEach(() => {
    consoleSpy.mockRestore()
  })

  it('renders children unchanged when there is no error', () => {
    render(
      <ErrorBoundary>
        <div>Happy path</div>
      </ErrorBoundary>
    )
    expect(screen.getByText('Happy path')).toBeInTheDocument()
  })

  it('shows the default fallback UI when a child throws', () => {
    render(
      <ErrorBoundary>
        <Boom message="something broke" />
      </ErrorBoundary>
    )
    expect(screen.getByRole('alert')).toBeInTheDocument()
    expect(screen.getByText(/Something went wrong/i)).toBeInTheDocument()
    expect(screen.getByText('something broke')).toBeInTheDocument()
  })

  it('renders a custom fallback element when provided', () => {
    render(
      <ErrorBoundary fallback={<div data-testid="custom">My fallback</div>}>
        <Boom />
      </ErrorBoundary>
    )
    expect(screen.getByTestId('custom')).toHaveTextContent('My fallback')
  })

  it('passes error + reset + reload to a function fallback', () => {
    const fallback = vi.fn(({ error }) => <div>err: {error.message}</div>)
    render(
      <ErrorBoundary fallback={fallback}>
        <Boom message="oh no" />
      </ErrorBoundary>
    )
    expect(fallback).toHaveBeenCalledWith(
      expect.objectContaining({
        error: expect.any(Error),
        reset: expect.any(Function),
        reload: expect.any(Function),
      })
    )
    expect(screen.getByText('err: oh no')).toBeInTheDocument()
  })

  it('Try Again button clears the error and remounts children', () => {
    // Stateful child: throws the first render, succeeds on the second.
    let shouldThrow = true
    function Flaky() {
      if (shouldThrow) throw new Error('first-render-bug')
      return <div>recovered</div>
    }
    render(
      <ErrorBoundary>
        <Flaky />
      </ErrorBoundary>
    )
    expect(screen.getByRole('alert')).toBeInTheDocument()
    // Flip the source of the bug, then click Try Again.
    shouldThrow = false
    fireEvent.click(screen.getByRole('button', { name: /Try Again/i }))
    expect(screen.getByText('recovered')).toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })

  it('Reload button calls window.location.reload()', () => {
    // jsdom's `window.location` is non-configurable, so we can't replace
    // `.reload` directly. Stub the whole `location` object instead.
    const reload = vi.fn()
    const originalLocation = window.location
    delete window.location
    window.location = { ...originalLocation, reload }
    try {
      render(
        <ErrorBoundary>
          <Boom />
        </ErrorBoundary>
      )
      fireEvent.click(screen.getByRole('button', { name: /Reload/i }))
      expect(reload).toHaveBeenCalledTimes(1)
    } finally {
      window.location = originalLocation
    }
  })

  it('logs the error to console.error with the component stack', () => {
    render(
      <ErrorBoundary>
        <Boom message="logme" />
      </ErrorBoundary>
    )
    expect(consoleSpy).toHaveBeenCalled()
    const firstCall = consoleSpy.mock.calls.find((c) =>
      String(c[0]).includes('[ErrorBoundary]')
    )
    expect(firstCall).toBeTruthy()
  })
})
