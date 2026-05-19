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
// For dm: compose from the counterpart's hrInfo — engName + " " + name,
// collapsed to just `name` when the two are identical. The hrInfo field
// lives on `DMSubscription` (pkg/model.DMSubscription wraps Subscription
// with a `*SubscriptionHRInfo` pointer); for channels/botDMs/discussions
// the field is absent and we render a "(DM)" placeholder so the sidebar
// row stays identifiable until the DM subscription loads.
export function roomDisplayName(room) {
  if (!room) return ''
  if (room.type === 'dm') {
    if (!room.hrInfo) return '(DM)'
    const { engName, name } = room.hrInfo
    return engName === name ? name : `${engName} ${name}`
  }
  if (room.subscriptionName) return room.subscriptionName
  if (room.name) return room.name
  if (room.type === 'botDM') return '(DM)'
  return room.id ?? ''
}

export function roomFromSearchHit(hit) {
  return {
    id: hit.roomId,
    name: hit.name,
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
