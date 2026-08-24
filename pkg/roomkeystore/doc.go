// Package roomkeystore stores room encryption secrets as an "encKey"
// sub-document embedded in the room's document in the MongoDB rooms collection.
//
// # Versioning
//
// Set assigns version 0 to a fresh key. Rotate increments the version and demotes
// the current key into the encKey.prev* slot, stamping it with a grace-period
// expiry (encKey.prevExpiresAt). GetByVersion serves either the current key or
// the previous key while its grace window is still open, enabling decrypt of
// messages encrypted under a recently-rotated key.
//
// # Lifecycle
//
// A room key is a field of its room, so Set returns ErrRoomNotFound when the room
// does not exist yet. Delete unsets the encKey sub-document.
//
// # Concurrency
//
// Rotate runs as a single aggregation-pipeline update against one document, so
// the current-to-previous transition and version bump are atomic; no concurrent
// reader observes a partially-rotated key. SetIfAbsent is likewise a single
// conditional update whose post-image lets racing callers converge on one v0 key,
// which plain Set (last-write-wins) cannot do. Set and Get are not otherwise
// coordinated; readers see Set's write atomically once the update completes.
//
// # Federation
//
// Site-local only. A room exists on its origin site, so the broadcast pipeline
// that needs the key runs on that same site and reads from the origin's local
// rooms collection. There is no cross-site key replication; inbox-worker on
// remote sites replicates subscription/room metadata but never room keys.
package roomkeystore
