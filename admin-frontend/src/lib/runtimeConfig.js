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
