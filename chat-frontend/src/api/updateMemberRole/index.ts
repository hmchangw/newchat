import { memberRoleUpdate } from '../_transport/subjects'
import type { Nats, AsyncJobOptions, AsyncJobResult } from '../types'

/** 'member' is the legacy spelling of 'user'; the server still accepts it. */
export type MemberRole = 'owner' | 'user' | 'member'

export interface UpdateMemberRoleArgs {
  roomId: string
  siteId: string
  account: string
  newRole: MemberRole
}

/** Promote / demote a member's role in a channel. Two-phase. */
export async function updateMemberRole(
  { user, requestWithAsyncResult }: Nats,
  args: UpdateMemberRoleArgs,
  opts?: AsyncJobOptions,
): Promise<AsyncJobResult> {
  const { roomId, siteId, account, newRole } = args
  const payload = { roomId, account, newRole }
  return requestWithAsyncResult(memberRoleUpdate(user.account, roomId, siteId), payload, opts)
}
