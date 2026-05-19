import { searchRooms as searchRoomsSubject } from '../_transport/subjects'
import type { Nats, RoomType } from '../types'

export interface SearchRoomsArgs {
  searchText: string
  /** Server-accepted roomType filter; mirrors search-service's
   *  query_rooms.go. `'all'` (default) returns every accessible room;
   *  the type-specific values narrow. The server rejects any other value. */
  roomType: 'all' | 'channel' | 'dm'
  size: number
}

/** Mirrors pkg/model.SearchRoom (the search.rooms reply projection). */
export interface SearchRoomHit {
  roomId: string
  name: string
  roomType?: RoomType
  siteId: string
}

export interface SearchRoomsResponse {
  rooms: SearchRoomHit[]
}

/**
 * Search rooms the caller is a member of (or all rooms if roomType='all').
 * Mirrors search-service's `search.rooms` handler — hits sorted by relevance.
 */
export async function searchRooms(
  { user, request }: Nats,
  { searchText, roomType, size }: SearchRoomsArgs,
): Promise<SearchRoomsResponse> {
  return request<SearchRoomsResponse>(searchRoomsSubject(user.account), {
    query: searchText,
    roomType,
    size,
  })
}
