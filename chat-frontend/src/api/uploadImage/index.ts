import type { Attachment, Nats } from '../types'
import { getAccessToken } from '../auth/oidcClient'

export interface UploadImageArgs {
  roomId: string
  file: File
}

interface UploadFileReply {
  success?: boolean
  attachments?: Attachment[]
}

/**
 * Upload one image for a room and return its render-ready Attachment.
 *
 * Uses the protected upload endpoint (`POST {baseUrl}/api/v1/file/rooms/:roomId/
 * upload/file`), authenticated with the SSO access token in the `ssoToken`
 * header. The returned Attachment is what the caller base64-encodes into a
 * `msg.send` `attachments` entry. Rejects if there's no media base URL, no
 * access token (e.g. dev-mode login), or the server rejects the file.
 */
export async function uploadImage({ user }: Nats, { roomId, file }: UploadImageArgs): Promise<Attachment> {
  const baseUrl = user?.baseUrl
  if (!baseUrl) throw new Error('no media base URL for upload')
  const token = await getAccessToken()
  if (!token) throw new Error('not authenticated for upload')

  const form = new FormData()
  form.append('file', file)

  const url = `${baseUrl.replace(/\/+$/, '')}/api/v1/file/rooms/${encodeURIComponent(roomId)}/upload/file`
  const resp = await fetch(url, { method: 'POST', headers: { ssoToken: token }, body: form })
  if (!resp.ok) throw new Error(`image upload failed: ${resp.status}`)

  const data = (await resp.json()) as UploadFileReply
  const att = data.attachments?.[0]
  if (!att) throw new Error('image upload returned no attachment')
  return att
}
