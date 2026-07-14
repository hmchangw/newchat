import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import Pager from './Pager'

describe('Pager', () => {
  it('disables Prev on the first page and enables Next when more rows remain', () => {
    render(<Pager page={1} limit={20} total={50} onPrev={vi.fn()} onNext={vi.fn()} />)
    expect(screen.getByRole('button', { name: /prev/i })).toBeDisabled()
    expect(screen.getByRole('button', { name: /next/i })).toBeEnabled()
  })

  it('disables Next once the current page covers all of total', () => {
    // page 3 * limit 20 = 60 >= total 50 — no more rows to page into.
    render(<Pager page={3} limit={20} total={50} onPrev={vi.fn()} onNext={vi.fn()} />)
    expect(screen.getByRole('button', { name: /next/i })).toBeDisabled()
    expect(screen.getByRole('button', { name: /prev/i })).toBeEnabled()
  })

  it('shows the current page number and total row count', () => {
    render(<Pager page={2} limit={20} total={50} onPrev={vi.fn()} onNext={vi.fn()} />)
    expect(screen.getByText(/page 2/i)).toBeInTheDocument()
    expect(screen.getByText(/50 total/i)).toBeInTheDocument()
  })

  it('invokes onPrev/onNext when the buttons are clicked', () => {
    const onPrev = vi.fn()
    const onNext = vi.fn()
    render(<Pager page={2} limit={20} total={50} onPrev={onPrev} onNext={onNext} />)
    fireEvent.click(screen.getByRole('button', { name: /prev/i }))
    fireEvent.click(screen.getByRole('button', { name: /next/i }))
    expect(onPrev).toHaveBeenCalledTimes(1)
    expect(onNext).toHaveBeenCalledTimes(1)
  })
})
