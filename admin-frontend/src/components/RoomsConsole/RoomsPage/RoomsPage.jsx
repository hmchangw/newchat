import { lazy, Suspense, useState } from 'react'
import { listRooms } from '@/api'
import { useAuth } from '@/context/AuthContext'
import { ondutyMinMembers } from '@/lib/runtimeConfig'
import { usePagedAdminList } from '@/hooks/usePagedAdminList'
import LazyFallback from '@/components/shared/LazyFallback'
import Pager from '@/components/shared/Pager'
import RoomTable from '../RoomTable'
import './style.css'

const SetOnDutyDialog = lazy(() => import('../SetOnDutyDialog'))
const UnsetOnDutyDialog = lazy(() => import('../UnsetOnDutyDialog'))

// Matches admin-service's parsePaging default limit (handler.go).
const PAGE_SIZE = 20

// The listing takes no filters — unlike Audit and Permissions, this console has no
// search box, so the hook's debounce path is never exercised.
const NO_FILTERS = {}

// Settings → Rooms console. Owns which duty dialog is open; the toggle returns only
// {status:"ok"}, so a landed change is picked up by refetching rather than by
// patching the row in place. Paging shell comes from usePagedAdminList.
export default function RoomsPage() {
  const { session } = useAuth()
  const authToken = session?.authToken

  const { data, total, page, loading, error, notAuthorized, goToPage, refresh } =
    usePagedAdminList({
      authToken,
      fetcher: (token, _filters, paging) => listRooms(token, paging),
      defaultFilters: NO_FILTERS,
      pageSize: PAGE_SIZE,
    })
  const rooms = data?.rooms ?? []

  const [onDutyTarget, setOnDutyTarget] = useState(null)
  const [offDutyTarget, setOffDutyTarget] = useState(null)

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
        // room-service refuses the off→on transition below this floor, so a room
        // it would reject gets no button rather than a guaranteed error.
        minMembers={ondutyMinMembers()}
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
              refresh()
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
              refresh()
            }}
          />
        </Suspense>
      )}
    </div>
  )
}
