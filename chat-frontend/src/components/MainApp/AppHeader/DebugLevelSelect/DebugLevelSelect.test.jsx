import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { DebugProvider } from '@/context/DebugContext'
import DebugLevelSelect from './DebugLevelSelect'

beforeEach(() => {
  localStorage.clear()
})

afterEach(() => {
  localStorage.clear()
})

describe('DebugLevelSelect', () => {
  it('offers off/flow/debug/trace and defaults to off', () => {
    render(
      <DebugProvider>
        <DebugLevelSelect />
      </DebugProvider>,
    )
    const select = screen.getByRole('combobox', { name: /debug header level/i })
    const options = Array.from(select.options).map((o) => o.value)
    expect(options).toEqual(['off', 'flow', 'debug', 'trace'])
    expect(select.value).toBe('off')
  })

  it('reflects the stored level', () => {
    localStorage.setItem('debug', 'trace')
    render(
      <DebugProvider>
        <DebugLevelSelect />
      </DebugProvider>,
    )
    expect(screen.getByRole('combobox', { name: /debug header level/i }).value).toBe('trace')
  })

  it('changing the selection updates and persists the level', () => {
    render(
      <DebugProvider>
        <DebugLevelSelect />
      </DebugProvider>,
    )
    const select = screen.getByRole('combobox', { name: /debug header level/i })
    fireEvent.change(select, { target: { value: 'debug' } })
    expect(select.value).toBe('debug')
    expect(localStorage.getItem('debug')).toBe('debug')

    fireEvent.change(select, { target: { value: 'off' } })
    expect(select.value).toBe('off')
    expect(localStorage.getItem('debug')).toBeNull()
  })
})
