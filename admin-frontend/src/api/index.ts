// Public surface of the api/ layer. `_transport/` is internal — components must
// import from this barrel, never reach into `@/api/_transport/...` directly.

export {
  AsyncJobError,
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
  listSessions,
  listUsers,
  resyncPermissions,
  resyncUser,
  revokeAllSessions,
  revokeSession,
  setPassword,
  updateUser,
} from './admin'
export type {
  AdminSession,
  AdminUser,
  AuditEntry,
  AuditFilter,
  CreatePermissionsRequest,
  CreatePermissionsResponse,
  CreateUserInput,
  ListPermissionsParams,
  ListPermissionsResponse,
  ListUsersParams,
  PermissionGrantView,
  ResyncPermissionsRequest,
  ResyncPermissionsResponse,
  SetPasswordInput,
  UpdateUserPatch,
} from './admin'
