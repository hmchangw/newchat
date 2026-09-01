import { useCallback, useEffect, useRef, useState } from 'react'
import { AsyncJobError } from '@/api'
import { useDebouncedSearch } from './useDebouncedSearch'
import { useHandleAdminError } from './useHandleAdminError'
import { useLatestRequest } from './useLatestRequest'

/** Shell shared by the paged admin consoles (Audit, Permissions, Rooms): filter state, page
 * state, stale-response guard, and the `not_admin` branch. Filtering is optional — a console
 * with no search box passes `defaultFilters: {}` and simply never triggers the debounce.
 *
 * `fetcher(authToken, filters, { page, limit })` keeps the page-specific query building local to
 * the caller and returns the raw response; it is read through a ref, so an inline arrow is fine.
 * The whole response is also exposed as `data` for fields beyond `entries`/`total`. */
export function usePagedAdminList({ authToken, fetcher, defaultFilters, pageSize }) {
  const handleAdminError = useHandleAdminError()
  const { begin, isCurrent } = useLatestRequest()
  const fetcherRef = useRef(fetcher)
  fetcherRef.current = fetcher

  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [notAuthorized, setNotAuthorized] = useState(false)
  const [filters, setFilters] = useState(defaultFilters)
  const [page, setPage] = useState(1)

  // Tracks the filter key already reflected by the most recent fetch (whether triggered by the
  // debounce or by goToPage), so a debounced onSearch firing after a manual goToPage — with the
  // same filters it already picked up — doesn't clobber the page the user just navigated to.
  const lastFetchedFilterKeyRef = useRef(JSON.stringify(defaultFilters))

  const fetchPage = useCallback(
    async (nextFilters, nextPage) => {
      const token = begin()
      setLoading(true)
      setError(null)
      try {
        const result = await fetcherRef.current(authToken, nextFilters, {
          page: nextPage,
          limit: pageSize,
        })
        if (!isCurrent(token)) return // superseded by a newer request
        setData(result)
        setNotAuthorized(false)
      } catch (err) {
        if (!isCurrent(token)) return
        // Deliberate: clearing stale rows makes the console show empty + the error banner,
        // rather than rows that silently no longer match the filters/page that just failed.
        setData(null)
        if (err instanceof AsyncJobError && err.reason === 'not_admin') {
          setNotAuthorized(true)
        } else {
          const message = handleAdminError(err)
          if (message !== null) setError(message)
        }
      } finally {
        if (isCurrent(token)) setLoading(false)
      }
    },
    [authToken, pageSize, handleAdminError, begin, isCurrent],
  )

  useEffect(() => {
    setPage(1)
    fetchPage(defaultFilters, 1)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authToken])

  const { setQuery: setDebouncedFilters } = useDebouncedSearch({
    onSearch: (serialized) => {
      if (serialized === lastFetchedFilterKeyRef.current) return
      lastFetchedFilterKeyRef.current = serialized
      setPage(1)
      fetchPage(serialized ? JSON.parse(serialized) : defaultFilters, 1)
    },
  })

  return {
    data,
    entries: data?.entries ?? [],
    total: data?.total ?? 0,
    page,
    filters,
    loading,
    error,
    notAuthorized,
    updateFilter: (key, value) => {
      const next = { ...filters, [key]: value }
      setFilters(next)
      setDebouncedFilters(JSON.stringify(next))
    },
    goToPage: (nextPage) => {
      lastFetchedFilterKeyRef.current = JSON.stringify(filters)
      setPage(nextPage)
      fetchPage(filters, nextPage)
    },
    refresh: () => fetchPage(filters, page),
  }
}
