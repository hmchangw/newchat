import { useCallback, useRef } from 'react'

// Guards against out-of-order async responses. begin() stamps a monotonically
// increasing token for a request; isCurrent(token) returns false once a newer
// request has begun, so a slow response can be dropped instead of clobbering
// fresher state. Both functions are stable across renders.
export function useLatestRequest() {
  const ref = useRef(0)
  const begin = useCallback(() => ++ref.current, [])
  const isCurrent = useCallback((token) => token === ref.current, [])
  return { begin, isCurrent }
}
