// Runtime config: window.__APP_CONFIG__ in prod (nginx envsubst), import.meta.env.VITE_*
// in `vite dev`, literal defaults as last resort. Mirrors chat-frontend's read chain.

const runtimeConfig = () => (typeof window !== 'undefined' && window.__APP_CONFIG__) || {}

export const ADMIN_SERVICE_URL =
  runtimeConfig().ADMIN_SERVICE_URL ||
  import.meta.env.VITE_ADMIN_SERVICE_URL ||
  'http://localhost:8082'

// envsubst renders strings, so only the literal 'true' enables. Deliberately
// case-sensitive, unlike chat-frontend's boolConfig.
const flagEnabled = (value) => value === 'true'

// Deploy gates read at call time (not module load) so the nginx-rendered config.js
// and tests can both flip them. Take the VITE_ fallback as a value: Vite statically
// replaces only literal `import.meta.env.VITE_X`, never a computed key.
export const permissionsEnabled = () =>
  flagEnabled(runtimeConfig().PERMISSIONS_ENABLED ?? import.meta.env.VITE_PERMISSIONS_ENABLED)

export const updatesEnabled = () =>
  flagEnabled(runtimeConfig().UPDATES_ENABLED ?? import.meta.env.VITE_UPDATES_ENABLED)

// Minimum member count before a room may be put on duty. Mirrors room-service's
// RESTRICTED_ROOM_MIN_MEMBERS, which is the real enforcement — this only decides
// whether the console offers the action. A junk or unrendered value falls back to 5.
const ONDUTY_MIN_MEMBERS_DEFAULT = 5

export const ondutyMinMembers = () => {
  const raw = runtimeConfig().ROOM_ONDUTY_MIN_MEMBERS ?? import.meta.env.VITE_ROOM_ONDUTY_MIN_MEMBERS
  const parsed = Number(raw)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : ONDUTY_MIN_MEMBERS_DEFAULT
}
