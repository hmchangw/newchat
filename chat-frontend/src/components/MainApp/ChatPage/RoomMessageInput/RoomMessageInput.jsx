import { useEffect, useRef, useState } from 'react'
import { v4 as uuidv4 } from 'uuid'
import { useNats } from '@/context/NatsContext'
import { useRoomDispatch } from '@/context/RoomEventsContext'
import { sendMessage, uploadImage, formatAsyncJobError } from '@/api'
import { encodeAttachment } from '@/lib/attachment'
import { generateMessageID } from '@/lib/idgen'
import { roomPrefix, roomDisplayName } from '@/lib/roomFormat'
import MessageInputForm from '@/components/shared/MessageInputForm/MessageInputForm'

export default function RoomMessageInput({ room, quotedTarget, onClearQuote }) {
  const nats = useNats()
  const { user } = nats
  const dispatch = useRoomDispatch()
  const [text, setText] = useState('')
  // { file, url } — url is a local object URL for the compose-time preview.
  const [pendingImage, setPendingImage] = useState(null)
  const [sending, setSending] = useState(false)
  const [sendError, setSendError] = useState(null)
  const inputRef = useRef(null)

  useEffect(() => {
    if (quotedTarget?.id) {
      inputRef.current?.focus()
    }
  }, [quotedTarget?.id])

  // Revoke the preview object URL when it's replaced or the input unmounts.
  useEffect(() => {
    const url = pendingImage?.url
    return () => {
      if (url) URL.revokeObjectURL(url)
    }
  }, [pendingImage?.url])

  const placeholder = room
    ? `Message ${roomPrefix(room.type)}${roomDisplayName(room)}`
    : 'Select a room...'
  const disabled = !room || !user || sending

  const pickImage = (file) => {
    if (!file || !file.type?.startsWith('image/')) return
    setPendingImage({ file, url: URL.createObjectURL(file) })
  }
  const clearImage = () => setPendingImage(null)

  const handleSubmit = async () => {
    const content = text.trim()
    if (!room || !user || sending) return
    if (!content && !pendingImage) return

    const id = generateMessageID()
    const payload = { id, content, requestId: uuidv4() }
    const optimistic = {
      id,
      content,
      createdAt: new Date().toISOString(),
      sender: { account: user.account, engName: user.engName },
      _local: true,
    }
    if (quotedTarget?.id) {
      payload.quotedParentMessageId = quotedTarget.id
      optimistic.quotedParentMessage = {
        messageId: quotedTarget.id,
        sender: { engName: quotedTarget.senderName, account: quotedTarget.senderName },
        msg: quotedTarget.content,
      }
    }

    // Upload the image (if any) BEFORE optimistic insert — we need the server
    // Attachment to send, and a failed upload must not leave a dangling row.
    if (pendingImage) {
      setSending(true)
      let attachment
      try {
        attachment = await uploadImage(nats, { roomId: room.id, file: pendingImage.file })
      } catch (err) {
        setSending(false)
        // eslint-disable-next-line no-console
        console.warn('image upload failed:', err?.message ?? err)
        return
      }
      setSending(false)
      payload.attachments = [encodeAttachment(attachment)]
      // Show the actual image immediately via a local object URL (a fresh one,
      // owned by the message — the preview URL is revoked when we clear the
      // composer below). The server echo carries the same Attachment and
      // renders from the gateway URL.
      optimistic.attachments = [{ ...attachment, localUrl: URL.createObjectURL(pendingImage.file) }]
    }

    dispatch({ type: 'MESSAGE_SENT_LOCAL', roomId: room.id, message: optimistic })
    setSendError(null)
    sendMessage(nats, { roomId: room.id, siteId: user.siteId, payload }).catch((err) => {
      setSendError(formatAsyncJobError(err))
    })
    setText('')
    setPendingImage(null) // effect revokes the preview object URL
    onClearQuote?.()
  }

  return (
    <>
      {sendError && <div className="dialog-error">{sendError}</div>}
      <MessageInputForm
        inputRef={inputRef}
        value={text}
        onChange={setText}
        onSubmit={handleSubmit}
        placeholder={placeholder}
        disabled={disabled}
        quotedTarget={quotedTarget}
        onClearQuote={onClearQuote}
        onPickImage={pickImage}
        pendingImage={pendingImage ? { name: pendingImage.file.name, url: pendingImage.url } : null}
        onClearImage={clearImage}
      />
    </>
  )
}
