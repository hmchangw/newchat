import { useCallback } from 'react'
import { AsyncJobError, formatAsyncJobError } from '@/api'
import { useAuth } from '@/context/AuthContext'

/** On `reason === 'invalid_token'` (expired/revoked session), logs out and returns `null` instead
 * of a banner message; otherwise returns `formatAsyncJobError(err)`. Callers needing `not_admin`
 * handling must still check that reason themselves before calling this helper. */
export function useHandleAdminError() {
  const { logout } = useAuth()

  return useCallback(
    (err) => {
      if (err instanceof AsyncJobError && err.reason === 'invalid_token') {
        logout()
        return null
      }
      return formatAsyncJobError(err)
    },
    [logout],
  )
}
