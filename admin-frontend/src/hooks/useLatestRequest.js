import { useCallback, useRef } from 'react'

// Guards against out-of-order async responses. begin() stamps a monotonically
// increasing sequence number for a request; isCurrent(seq) returns false once a
// newer request has begun, so a slow response can be dropped instead of
// clobbering fresher state. Both functions are stable across renders.
//
// The parameter is named `seq`, not `token`: it is a plain counter, and the
// timing-attack linters key off the operand's name, so calling it `token` made
// this ordinary comparison look like a secret check.
export function useLatestRequest() {
  const ref = useRef(0)
  const begin = useCallback(() => ++ref.current, [])
  const isCurrent = useCallback((seq) => seq === ref.current, [])
  return { begin, isCurrent }
}
