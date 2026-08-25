import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import RoomMessageInput from './RoomMessageInput'

const publish = vi.fn()
const subscribe = vi.fn(() => ({ unsubscribe: vi.fn() }))
const dispatch = vi.fn()
vi.mock('@/context/NatsContext', () => ({
  useNats: () => ({ user: { account: 'alice', siteId: 's1', baseUrl: 'https://media.test' }, publish, subscribe }),
}))
vi.mock('@/context/RoomEventsContext', () => ({
  useRoomDispatch: () => dispatch,
}))
vi.mock('@/lib/idgen', () => ({ generateMessageID: () => '12345678901234567890' }))
vi.mock('uuid', () => ({ v4: () => 'req-uuid' }))
// Keep the real sendMessage (→ publish) but stub the HTTP uploadImage.
vi.mock('@/api', async (orig) => ({ ...(await orig()), uploadImage: vi.fn() }))
import { uploadImage } from '@/api'

const room = { id: 'r1', name: 'general', type: 'channel' }

function pickFile(container, file) {
  const fileInput = container.querySelector('input[type="file"]')
  fireEvent.change(fileInput, { target: { files: [file] } })
}

describe('RoomMessageInput', () => {
  beforeEach(() => {
    publish.mockClear()
    subscribe.mockClear()
    dispatch.mockClear()
    vi.mocked(uploadImage).mockReset()
    vi.stubGlobal('URL', {
      ...URL,
      createObjectURL: vi.fn(() => 'blob:preview'),
      revokeObjectURL: vi.fn(),
    })
  })

  it('renders the form disabled when no room is selected', () => {
    render(<RoomMessageInput room={null} />)
    expect(screen.getByPlaceholderText(/select a room/i)).toBeDisabled()
  })

  it('publishes msg.send on submit with the correct payload', () => {
    render(<RoomMessageInput room={room} />)
    const input = screen.getByPlaceholderText(/general/i)
    fireEvent.change(input, { target: { value: 'hello' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(publish).toHaveBeenCalledWith(
      'chat.user.alice.room.r1.s1.msg.send',
      { id: '12345678901234567890', content: 'hello', requestId: 'req-uuid' }
    )
  })

  it('clears the text after publish', () => {
    render(<RoomMessageInput room={room} />)
    const input = screen.getByPlaceholderText(/general/i)
    fireEvent.change(input, { target: { value: 'hello' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(input).toHaveValue('')
  })

  it('does not publish when text is empty or whitespace', () => {
    render(<RoomMessageInput room={room} />)
    const input = screen.getByPlaceholderText(/general/i)
    fireEvent.change(input, { target: { value: '   ' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(publish).not.toHaveBeenCalled()
  })

  it('forwards quotedTarget and onClearQuote to MessageInputForm', () => {
    const onClearQuote = vi.fn()
    render(
      <RoomMessageInput
        room={room}
        quotedTarget={{ id: 'q', senderName: 'bob', content: 'orig' }}
        onClearQuote={onClearQuote}
      />
    )
    fireEvent.click(screen.getByRole('button', { name: /clear quoted message/i }))
    expect(onClearQuote).toHaveBeenCalled()
  })

  it('includes quotedParentMessageId in the publish payload when quotedTarget is set', () => {
    render(
      <RoomMessageInput
        room={room}
        quotedTarget={{ id: 'q123', senderName: 'bob', content: 'orig' }}
        onClearQuote={() => {}}
      />
    )
    const input = screen.getByPlaceholderText(/general/i)
    fireEvent.change(input, { target: { value: 'reply' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(publish).toHaveBeenCalledWith(
      'chat.user.alice.room.r1.s1.msg.send',
      { id: '12345678901234567890', content: 'reply', requestId: 'req-uuid', quotedParentMessageId: 'q123' }
    )
  })

  it('dispatches an optimistic MESSAGE_SENT_LOCAL alongside the publish', () => {
    render(<RoomMessageInput room={room} />)
    const input = screen.getByPlaceholderText(/general/i)
    fireEvent.change(input, { target: { value: 'hi' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(dispatch).toHaveBeenCalledWith(expect.objectContaining({
      type: 'MESSAGE_SENT_LOCAL',
      roomId: 'r1',
      message: expect.objectContaining({
        id: '12345678901234567890',
        content: 'hi',
        sender: expect.objectContaining({ account: 'alice' }),
        _local: true,
      }),
    }))
  })

  it('uploads a picked image and sends it as a base64 attachment (empty text allowed)', async () => {
    const att = { id: 'f1', title: 'p.png', type: 'file', titleLink: 'api/v1/file/rooms/r1/file/f1', fileType: 'image/png' }
    vi.mocked(uploadImage).mockResolvedValue(att)
    const { container } = render(<RoomMessageInput room={room} />)
    pickFile(container, new File(['x'], 'p.png', { type: 'image/png' }))
    // Preview chip shows the picked image name.
    expect(await screen.findByText('p.png')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: /^Send$/ }))

    await waitFor(() => expect(publish).toHaveBeenCalled())
    expect(uploadImage).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ roomId: 'r1', file: expect.any(File) }),
    )
    const [, payload] = publish.mock.calls[0]
    expect(payload.attachments).toHaveLength(1)
    expect(typeof payload.attachments[0]).toBe('string')
    // Optimistic message carries the attachment with a local preview URL.
    const optimistic = dispatch.mock.calls.find((c) => c[0]?.type === 'MESSAGE_SENT_LOCAL')[0].message
    expect(optimistic.attachments[0]).toMatchObject({ id: 'f1', localUrl: expect.any(String) })
  })

  it('does not send when the image upload fails', async () => {
    vi.mocked(uploadImage).mockRejectedValue(new Error('413'))
    const { container } = render(<RoomMessageInput room={room} />)
    pickFile(container, new File(['x'], 'p.png', { type: 'image/png' }))
    fireEvent.click(screen.getByRole('button', { name: /^Send$/ }))
    await waitFor(() => expect(uploadImage).toHaveBeenCalled())
    expect(publish).not.toHaveBeenCalled()
    expect(dispatch).not.toHaveBeenCalled()
  })

  it('optimistic message carries a server-shaped quotedParentMessage snapshot', () => {
    render(
      <RoomMessageInput
        room={room}
        quotedTarget={{ id: 'q123', senderName: 'bob', content: 'orig' }}
        onClearQuote={() => {}}
      />
    )
    const input = screen.getByPlaceholderText(/general/i)
    fireEvent.change(input, { target: { value: 'reply' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    const call = dispatch.mock.calls.find((c) => c[0]?.type === 'MESSAGE_SENT_LOCAL')
    expect(call).toBeDefined()
    expect(call[0].message.quotedParentMessage).toEqual({
      messageId: 'q123',
      sender: { engName: 'bob', account: 'bob' },
      msg: 'orig',
    })
  })

  it('surfaces a gatekeeper refusal from the response subject', async () => {
    subscribe.mockImplementation((subject, cb) => {
      queueMicrotask(() => cb({ error: 'too big', code: 'bad_request', reason: 'content_too_large' }))
      return { unsubscribe: vi.fn() }
    })
    render(<RoomMessageInput room={room} />)
    const input = screen.getByPlaceholderText(/general/i)
    fireEvent.change(input, { target: { value: 'hello' } })
    fireEvent.keyDown(input, { key: 'Enter' })
    expect(await screen.findByText('too big')).toBeInTheDocument()
  })
})
