// Runtime config: window.__APP_CONFIG__ in prod (nginx envsubst), import.meta.env.VITE_*
// in `vite dev`, literal defaults as last resort. Mirrors chat-frontend's read chain.

const runtime = (typeof window !== 'undefined' && window.__APP_CONFIG__) || {}

export const PORTAL_URL =
  runtime.PORTAL_URL || import.meta.env.VITE_PORTAL_URL || 'http://localhost:8085'

export const ADMIN_SERVICE_URL =
  runtime.ADMIN_SERVICE_URL || import.meta.env.VITE_ADMIN_SERVICE_URL || 'http://localhost:8082'
