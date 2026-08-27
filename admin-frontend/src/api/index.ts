// Public surface of the api/ layer. `_transport/` is internal — components must
// import from this barrel, never reach into `@/api/_transport/...` directly.

export {
  AsyncJobError,
  envelopeErrorFromBody,
  formatAsyncJobError,
  parseHttpEnvelopeError,
} from './_transport/httpEnvelope'

export { botLogin, changePassword } from './auth/botAuth'
export type { Bundle } from './auth/botAuth'

export {
  createPermissions,
  createUser,
  getUser,
  listAudit,
  listPermissions,
  listRoomMembers,
  listRooms,
  listSessions,
  listUsers,
  resyncPermissions,
  resyncUser,
  revokeAllSessions,
  revokeSession,
  setPassword,
  setRoomOnDuty,
  updateUser,
  uploadClientVersion,
} from './admin'
export type {
  AdminRoom,
  AdminRoomMember,
  AdminSession,
  AdminUser,
  AuditEntry,
  AuditFilter,
  CreatePermissionsRequest,
  CreatePermissionsResponse,
  CreateUserInput,
  ListPermissionsParams,
  ListPermissionsResponse,
  ListRoomsParams,
  ListUsersParams,
  PermissionGrantView,
  ResyncPermissionsRequest,
  ResyncPermissionsResponse,
  SetPasswordInput,
  SetRoomOnDutyInput,
  UpdateUserPatch,
} from './admin'
