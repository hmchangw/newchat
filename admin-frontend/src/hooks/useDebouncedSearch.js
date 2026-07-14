import { useEffect, useRef, useState } from 'react'

/** Debounced search box: `delay` ms after the last change, calls `onSearch(query)`.
 * Does NOT fire on initial mount — callers issue their own unfiltered fetch first. */
export function useDebouncedSearch({ delay = 300, onSearch } = {}) {
  const [query, setQuery] = useState('')
  const timerRef = useRef(null)
  const onSearchRef = useRef(onSearch)
  const isFirstRef = useRef(true)
  onSearchRef.current = onSearch

  useEffect(() => {
    if (isFirstRef.current) {
      isFirstRef.current = false
      return undefined
    }
    clearTimeout(timerRef.current)
    timerRef.current = setTimeout(() => {
      onSearchRef.current?.(query)
    }, delay)
    return () => clearTimeout(timerRef.current)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query, delay])

  return { query, setQuery }
}
