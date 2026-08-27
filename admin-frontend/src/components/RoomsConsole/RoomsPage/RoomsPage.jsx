import { lazy, Suspense, useCallback, useEffect, useState } from 'react'
import { AsyncJobError, listRooms } from '@/api'
import { useAuth } from '@/context/AuthContext'
import { useHandleAdminError } from '@/hooks/useHandleAdminError'
import { useLatestRequest } from '@/hooks/useLatestRequest'
import LazyFallback from '@/components/shared/LazyFallback'
import Pager from '@/components/shared/Pager'
import RoomTable from '../RoomTable'
import './style.css'

const SetOnDutyDialog = lazy(() => import('../SetOnDutyDialog'))
const UnsetOnDutyDialog = lazy(() => import('../UnsetOnDutyDialog'))

// Matches admin-service's parsePaging default limit (handler.go).
const PAGE_SIZE = 20

// Settings → Rooms console. Owns the list query and which duty dialog is open;
// the toggle returns only {status:"ok"}, so a landed change is picked up by
// refetching rather than by patching the row in place.
export default function RoomsPage() {
  const { session } = useAuth()
  const authToken = session?.authToken
  const handleAdminError = useHandleAdminError()

  const [rooms, setRooms] = useState([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [notAuthorized, setNotAuthorized] = useState(false)
  const [page, setPage] = useState(1)

  const [onDutyTarget, setOnDutyTarget] = useState(null)
  const [offDutyTarget, setOffDutyTarget] = useState(null)

  const { begin, isCurrent } = useLatestRequest()

  const fetchRooms = useCallback(
    async (pageArg) => {
      const token = begin()
      setLoading(true)
      setError(null)
      try {
        const result = await listRooms(authToken, { page: pageArg, limit: PAGE_SIZE })
        if (!isCurrent(token)) return // superseded by a newer request
        setRooms(result.rooms)
        setTotal(result.total)
        setNotAuthorized(false)
      } catch (err) {
        if (!isCurrent(token)) return
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
    [authToken, handleAdminError, begin, isCurrent],
  )

  useEffect(() => {
    setPage(1)
    fetchRooms(1)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [authToken])

  const goToPage = (nextPage) => {
    setPage(nextPage)
    fetchRooms(nextPage)
  }

  if (notAuthorized) {
    return (
      <div className="rooms-page rooms-page-not-authorized">
        <p>You are not authorized to manage rooms.</p>
      </div>
    )
  }

  return (
    <div className="rooms-page">
      <div className="rooms-page-header">
        <span className="rooms-page-total">{total} rooms</span>
      </div>

      {error && <div className="dialog-error">{error}</div>}

      <RoomTable
        rooms={rooms}
        loading={loading}
        onSetOnDuty={setOnDutyTarget}
        onUnsetOnDuty={setOffDutyTarget}
      />

      <Pager
        page={page}
        limit={PAGE_SIZE}
        total={total}
        onPrev={() => goToPage(page - 1)}
        onNext={() => goToPage(page + 1)}
      />

      {onDutyTarget && (
        <Suspense fallback={<LazyFallback variant="dialog" />}>
          <SetOnDutyDialog
            authToken={authToken}
            room={onDutyTarget}
            onClose={() => setOnDutyTarget(null)}
            onDone={() => {
              setOnDutyTarget(null)
              fetchRooms(page)
            }}
          />
        </Suspense>
      )}

      {offDutyTarget && (
        <Suspense fallback={<LazyFallback variant="dialog" />}>
          <UnsetOnDutyDialog
            authToken={authToken}
            room={offDutyTarget}
            onClose={() => setOffDutyTarget(null)}
            onDone={() => {
              setOffDutyTarget(null)
              fetchRooms(page)
            }}
          />
        </Suspense>
      )}
    </div>
  )
}
