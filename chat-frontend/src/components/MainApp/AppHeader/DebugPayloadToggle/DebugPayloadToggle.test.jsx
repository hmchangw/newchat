import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { DebugProvider } from '@/context/DebugContext'
import DebugPayloadToggle from './DebugPayloadToggle'

beforeEach(() => {
  localStorage.clear()
})

afterEach(() => {
  localStorage.clear()
})

describe('DebugPayloadToggle', () => {
  it('renders an unchecked checkbox by default', () => {
    render(
      <DebugProvider>
        <DebugPayloadToggle />
      </DebugProvider>,
    )
    const box = screen.getByRole('checkbox', { name: /capture debug payloads/i })
    expect(box).toBeInTheDocument()
    expect(box).not.toBeChecked()
  })

  it('reflects the stored payload-capture state', () => {
    localStorage.setItem('debugPayload', '1')
    render(
      <DebugProvider>
        <DebugPayloadToggle />
      </DebugProvider>,
    )
    expect(screen.getByRole('checkbox', { name: /capture debug payloads/i })).toBeChecked()
  })

  it('toggling persists the state', () => {
    render(
      <DebugProvider>
        <DebugPayloadToggle />
      </DebugProvider>,
    )
    const box = screen.getByRole('checkbox', { name: /capture debug payloads/i })
    fireEvent.click(box)
    expect(box).toBeChecked()
    expect(localStorage.getItem('debugPayload')).toBe('1')

    fireEvent.click(box)
    expect(box).not.toBeChecked()
    expect(localStorage.getItem('debugPayload')).toBeNull()
  })
})
