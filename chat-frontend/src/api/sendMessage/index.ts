import { msgSend } from '../_transport/subjects'
import type { Nats } from '../types'

export interface SendMessagePayload {
  id: string
  content: string
  requestId: string
  quotedParentMessageId?: string
  threadParentMessageId?: string
  threadParentMessageCreatedAt?: number
  /** Base64-encoded Attachment JSON blobs (see lib/attachment.encodeAttachment).
   *  Max 1 today; content may be empty when attachments are present. */
  attachments?: string[]
}

export interface SendMessageArgs {
  roomId: string
  siteId: string
  payload: SendMessagePayload
}

/** Submit a new message into a room. Fire-and-forget. */
export function sendMessage(
  { user, publish }: Nats,
  { roomId, siteId, payload }: SendMessageArgs,
): void {
  publish(msgSend(user.account, roomId, siteId), payload)
}
