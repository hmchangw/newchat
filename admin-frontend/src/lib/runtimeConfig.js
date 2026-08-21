// Runtime config: window.__APP_CONFIG__ in prod (nginx envsubst), import.meta.env.VITE_*
// in `vite dev`, literal defaults as last resort. Mirrors chat-frontend's read chain.

const runtime = (typeof window !== 'undefined' && window.__APP_CONFIG__) || {}

export const ADMIN_SERVICE_URL =
  runtime.ADMIN_SERVICE_URL || import.meta.env.VITE_ADMIN_SERVICE_URL || 'http://localhost:8082'

// Read at call time (not module load) so the nginx-rendered config.js and tests
// can both flip it. envsubst renders strings, so only the string 'true' enables.
export const permissionsEnabled = () => {
  const runtimeNow = (typeof window !== 'undefined' && window.__APP_CONFIG__) || {}
  return (runtimeNow.PERMISSIONS_ENABLED ?? import.meta.env.VITE_PERMISSIONS_ENABLED) === 'true'
}
