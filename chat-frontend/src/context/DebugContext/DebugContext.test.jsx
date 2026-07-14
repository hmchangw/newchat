import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import { DebugProvider, useDebug, DEBUG_LEVELS } from './DebugContext'

function Probe() {
  const { level, setLevel, payload, setPayload } = useDebug()
  return (
    <>
      <span data-testid="level">{level}</span>
      <span data-testid="payload">{String(payload)}</span>
      <button data-testid="set-trace" onClick={() => setLevel('trace')}>trace</button>
      <button data-testid="set-flow" onClick={() => setLevel('flow')}>flow</button>
      <button data-testid="set-off" onClick={() => setLevel('off')}>off</button>
      <button data-testid="set-bogus" onClick={() => setLevel('bogus')}>bogus</button>
      <button data-testid="payload-on" onClick={() => setPayload(true)}>payload on</button>
      <button data-testid="payload-off" onClick={() => setPayload(false)}>payload off</button>
    </>
  )
}

beforeEach(() => {
  localStorage.clear()
})

afterEach(() => {
  localStorage.clear()
  vi.restoreAllMocks()
})

describe('DEBUG_LEVELS', () => {
  it('lists off/flow/debug/trace in increasing verbosity', () => {
    expect(DEBUG_LEVELS).toEqual(['off', 'flow', 'debug', 'trace'])
  })
})

describe('DebugProvider initial state', () => {
  it('defaults to "off" and payload disabled when localStorage is empty', () => {
    render(<DebugProvider><Probe /></DebugProvider>)
    expect(screen.getByTestId('level').textContent).toBe('off')
    expect(screen.getByTestId('payload').textContent).toBe('false')
  })

  it('reads a stored level verbatim', () => {
    localStorage.setItem('debug', 'trace')
    render(<DebugProvider><Probe /></DebugProvider>)
    expect(screen.getByTestId('level').textContent).toBe('trace')
  })

  it('reads stored payload capture as enabled', () => {
    localStorage.setItem('debugPayload', '1')
    render(<DebugProvider><Probe /></DebugProvider>)
    expect(screen.getByTestId('payload').textContent).toBe('true')
  })

  it('maps the legacy "1" level value to "debug"', () => {
    localStorage.setItem('debug', '1')
    render(<DebugProvider><Probe /></DebugProvider>)
    expect(screen.getByTestId('level').textContent).toBe('debug')
  })

  it('treats an unknown stored level as "off"', () => {
    localStorage.setItem('debug', 'verbose')
    render(<DebugProvider><Probe /></DebugProvider>)
    expect(screen.getByTestId('level').textContent).toBe('off')
  })

  it('does not crash when localStorage.getItem throws on initial read', () => {
    const original = Storage.prototype.getItem
    Storage.prototype.getItem = vi.fn(() => {
      throw new Error('localStorage unavailable')
    })
    try {
      render(<DebugProvider><Probe /></DebugProvider>)
      expect(screen.getByTestId('level').textContent).toBe('off')
      expect(screen.getByTestId('payload').textContent).toBe('false')
    } finally {
      Storage.prototype.getItem = original
    }
  })
})

describe('DebugProvider level mutations', () => {
  it('setLevel persists a non-off level', () => {
    render(<DebugProvider><Probe /></DebugProvider>)
    act(() => { screen.getByTestId('set-trace').click() })
    expect(screen.getByTestId('level').textContent).toBe('trace')
    expect(localStorage.getItem('debug')).toBe('trace')

    act(() => { screen.getByTestId('set-flow').click() })
    expect(screen.getByTestId('level').textContent).toBe('flow')
    expect(localStorage.getItem('debug')).toBe('flow')
  })

  it('setLevel("off") clears the stored key', () => {
    localStorage.setItem('debug', 'trace')
    render(<DebugProvider><Probe /></DebugProvider>)
    act(() => { screen.getByTestId('set-off').click() })
    expect(screen.getByTestId('level').textContent).toBe('off')
    expect(localStorage.getItem('debug')).toBeNull()
  })

  it('normalizes an invalid setLevel argument to "off"', () => {
    localStorage.setItem('debug', 'trace')
    render(<DebugProvider><Probe /></DebugProvider>)
    act(() => { screen.getByTestId('set-bogus').click() })
    expect(screen.getByTestId('level').textContent).toBe('off')
    expect(localStorage.getItem('debug')).toBeNull()
  })

  it('does not crash when localStorage.setItem throws', () => {
    const original = Storage.prototype.setItem
    Storage.prototype.setItem = vi.fn(() => {
      throw new Error('quota exceeded')
    })
    try {
      render(<DebugProvider><Probe /></DebugProvider>)
      act(() => { screen.getByTestId('set-trace').click() })
      expect(screen.getByTestId('level').textContent).toBe('trace')
    } finally {
      Storage.prototype.setItem = original
    }
  })
})

describe('DebugProvider payload-capture mutations', () => {
  it('setPayload(true) enables and persists, setPayload(false) clears the key', () => {
    render(<DebugProvider><Probe /></DebugProvider>)
    act(() => { screen.getByTestId('payload-on').click() })
    expect(screen.getByTestId('payload').textContent).toBe('true')
    expect(localStorage.getItem('debugPayload')).toBe('1')

    act(() => { screen.getByTestId('payload-off').click() })
    expect(screen.getByTestId('payload').textContent).toBe('false')
    expect(localStorage.getItem('debugPayload')).toBeNull()
  })

  it('payload capture is independent of the level', () => {
    render(<DebugProvider><Probe /></DebugProvider>)
    act(() => { screen.getByTestId('payload-on').click() })
    expect(screen.getByTestId('level').textContent).toBe('off')
    expect(screen.getByTestId('payload').textContent).toBe('true')
  })
})

describe('useDebug guard', () => {
  it('throws when used outside a DebugProvider', () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {})
    function Bare() {
      useDebug()
      return null
    }
    expect(() => render(<Bare />)).toThrow(/useDebug must be used within a DebugProvider/)
    spy.mockRestore()
  })
})
