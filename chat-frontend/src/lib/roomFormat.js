export function roomPrefix(type) {
  return type === 'dm' || type === 'botDM' ? '@ ' : '# '
}

// Resolves the text label for a room in the sidebar / chat header.
//
// For channel / botDM / discussion: prefer the user's subscription Name
// (server stores it per-subscription so each user can rename a room without
// affecting others); fall back to the canonical Room.Name, then to the
// room id as a last-resort identifier.
//
// For dm: compose from the counterpart's HRInfo — engName + " " + name,
// collapsed to just `name` when the two are identical. HRInfo lives on the
// Subscription for dm rooms and is sourced from the user-service
// subscription RPCs. When no hrInfo is available yet, render a "(DM)"
// placeholder so the sidebar row stays identifiable.
export function roomDisplayName(room) {
  if (!room) return ''
  if (room.type === 'dm') {
    const eng = room.hrInfo?.engName
    const name = room.hrInfo?.name
    if (eng && name) return eng === name ? name : `${eng} ${name}`
    return '(DM)'
  }
  if (room.subscriptionName) return room.subscriptionName
  if (room.name) return room.name
  if (room.type === 'botDM') return '(DM)'
  return room.id ?? ''
}

export function roomFromSearchHit(hit) {
  return {
    id: hit.roomId,
    name: hit.roomName,
    type: hit.roomType,
    siteId: hit.siteId,
  }
}

// Backend uses model.RoomType strings ("channel" / "dm" / ...). Same logic as
// roomPrefix above but without the trailing space — search rows place the
// prefix in its own span/div so spacing is handled by CSS, not the string.
export function searchRoomPrefix(roomType) {
  return roomType === 'dm' || roomType === 'botDM' ? '@' : '#'
}
