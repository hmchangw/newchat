import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import MessageAttachments from './MessageAttachments'

const BASE = 'https://site.example.com'

describe('MessageAttachments', () => {
  it('renders nothing when there are no attachments', () => {
    const { container } = render(<MessageAttachments attachments={[]} baseUrl={BASE} />)
    expect(container.firstChild).toBeNull()
    const { container: c2 } = render(<MessageAttachments baseUrl={BASE} />)
    expect(c2.firstChild).toBeNull()
  })

  it('renders an image attachment as an <img> pointing at the absolute gateway URL', () => {
    const att = {
      id: 'f1',
      title: 'photo.png',
      type: 'file',
      titleLink: 'api/v1/file/rooms/r1/file/f1',
      imageUrl: 'api/v1/file/rooms/r1/file/f1',
      fileType: 'image/png',
    }
    render(<MessageAttachments attachments={[att]} baseUrl={BASE} />)
    const img = screen.getByRole('img', { name: 'photo.png' })
    expect(img).toHaveAttribute('src', `${BASE}/api/v1/file/rooms/r1/file/f1`)
  })

  it('falls back to the base64 imagePreview when no gateway base is available', () => {
    const att = {
      id: 'f2',
      title: 'pic.jpg',
      type: 'file',
      titleLink: 'api/v1/file/rooms/r1/file/f2',
      fileType: 'image/jpeg',
      imagePreview: 'QUJD',
    }
    render(<MessageAttachments attachments={[att]} baseUrl="" />)
    expect(screen.getByRole('img', { name: 'pic.jpg' })).toHaveAttribute(
      'src',
      'data:image/jpeg;base64,QUJD',
    )
  })

  it('falls back to the blurred preview when the full image fails to load', () => {
    const att = {
      id: 'f5',
      title: 'photo.png',
      type: 'file',
      titleLink: 'api/v1/file/rooms/r1/file/f5',
      imageUrl: 'api/v1/file/rooms/r1/file/f5',
      fileType: 'image/png',
      imagePreview: 'QUJD',
    }
    render(<MessageAttachments attachments={[att]} baseUrl={BASE} />)
    const img = screen.getByRole('img', { name: 'photo.png' })
    expect(img).toHaveAttribute('src', `${BASE}/api/v1/file/rooms/r1/file/f5`)
    fireEvent.error(img)
    expect(img).toHaveAttribute('src', 'data:image/jpeg;base64,QUJD')
  })

  it('renders a non-image attachment as a download chip with title, size and link', () => {
    const att = {
      id: 'f3',
      title: 'report.pdf',
      type: 'file',
      titleLink: 'api/v1/file/rooms/r1/file/f3',
      fileType: 'application/pdf',
    }
    render(<MessageAttachments attachments={[att]} baseUrl={BASE} />)
    const link = screen.getByRole('link', { name: /report\.pdf/ })
    expect(link).toHaveAttribute('href', `${BASE}/api/v1/file/rooms/r1/file/f3`)
    expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    expect(screen.getByText('report.pdf')).toBeInTheDocument()
  })

  it('shows a human-readable size for a sized file', () => {
    const att = {
      id: 'f4',
      title: 'clip.mp4',
      type: 'file',
      titleLink: 'api/v1/file/rooms/r1/file/f4',
      videoUrl: 'api/v1/file/rooms/r1/file/f4',
      videoSize: 5 * 1048576,
      fileType: 'video/mp4',
    }
    render(<MessageAttachments attachments={[att]} baseUrl={BASE} />)
    expect(screen.getByText(/5\.0 MB/)).toBeInTheDocument()
  })
})
