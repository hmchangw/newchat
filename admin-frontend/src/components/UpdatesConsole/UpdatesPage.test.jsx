import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'

vi.mock('@/context/AuthContext', () => ({ useAuth: vi.fn() }))
vi.mock('@/api', async (importOriginal) => {
  const actual = await importOriginal()
  return { ...actual, uploadClientVersion: vi.fn() }
})

import UpdatesPage from './UpdatesPage'
import { useAuth } from '@/context/AuthContext'
import { uploadClientVersion, AsyncJobError } from '@/api'

const yaml = () => new File(['version: 1'], 'app.yaml', { type: 'text/yaml' })
const exe = () => new File(['MZ'], 'app.exe', { type: 'application/octet-stream' })

// fireEvent.change cannot set an <input type="file"> value directly; assigning a
// FileList-like array to target.files is the standard workaround.
function selectFile(input, file) {
  fireEvent.change(input, { target: { files: [file] } })
}

beforeEach(() => {
  vi.clearAllMocks()
  useAuth.mockReturnValue({
    session: { authToken: 'tok', account: 'root', siteId: 'site-1' },
    logout: vi.fn(),
  })
})

describe('UpdatesPage', () => {
  it('disables upload until both files are chosen', () => {
    render(<UpdatesPage />)

    const button = screen.getByRole('button', { name: /upload/i })
    expect(button).toBeDisabled()

    selectFile(screen.getByLabelText(/config file/i), yaml())
    expect(button).toBeDisabled()

    selectFile(screen.getByLabelText(/executable/i), exe())
    expect(button).toBeEnabled()
  })

  it('uploads both files and shows a success message', async () => {
    uploadClientVersion.mockResolvedValue(undefined)
    render(<UpdatesPage />)

    selectFile(screen.getByLabelText(/config file/i), yaml())
    selectFile(screen.getByLabelText(/executable/i), exe())
    fireEvent.click(screen.getByRole('button', { name: /upload/i }))

    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent(/uploaded/i))
    expect(uploadClientVersion).toHaveBeenCalledTimes(1)
    const [token, cfg, bin] = uploadClientVersion.mock.calls[0]
    expect(token).toBe('tok')
    expect(cfg.name).toBe('app.yaml')
    expect(bin.name).toBe('app.exe')
  })

  it('shows the server error message when the upload is rejected', async () => {
    uploadClientVersion.mockRejectedValue(
      new AsyncJobError('configFile must be a .yaml or .yml file', { code: 'bad_request' }),
    )
    render(<UpdatesPage />)

    selectFile(screen.getByLabelText(/config file/i), yaml())
    selectFile(screen.getByLabelText(/executable/i), exe())
    fireEvent.click(screen.getByRole('button', { name: /upload/i }))

    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent(/\.yaml or \.yml/i))
  })

  it('disables the button while an upload is in flight', async () => {
    let release
    uploadClientVersion.mockImplementation(() => new Promise((r) => { release = r }))
    render(<UpdatesPage />)

    selectFile(screen.getByLabelText(/config file/i), yaml())
    selectFile(screen.getByLabelText(/executable/i), exe())
    fireEvent.click(screen.getByRole('button', { name: /upload/i }))

    await waitFor(() =>
      expect(screen.getByRole('button', { name: /upload/i })).toBeDisabled(),
    )
    release()
    await waitFor(() =>
      expect(screen.getByRole('button', { name: /upload/i })).toBeEnabled(),
    )
  })

  it('passes a progress callback through to the client', async () => {
    uploadClientVersion.mockImplementation((_t, _c, _e, onProgress) => {
      onProgress(42)
      return Promise.resolve()
    })
    render(<UpdatesPage />)

    selectFile(screen.getByLabelText(/config file/i), yaml())
    selectFile(screen.getByLabelText(/executable/i), exe())
    fireEvent.click(screen.getByRole('button', { name: /upload/i }))

    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent(/uploaded/i))
    expect(uploadClientVersion).toHaveBeenCalledWith(
      'tok', expect.any(File), expect.any(File), expect.any(Function),
    )
  })
})
