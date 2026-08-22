// Package cachekeys is the single registry of every Valkey keyspace in the
// system: the builders that construct keys and the patterns that classify
// them back to the cache that owns them.
//
// Both live here for one reason — a cache whose keys are built from a string
// literal at the call site cannot be attributed in a per-cache memory
// breakdown, because nothing enumerates its pattern. Routing every key
// through a builder makes registration a precondition of using the cache at
// all, the same way pkg/outbox.Publish rejects an event type that is in no
// consumer filter set rather than letting it sit unconsumed.
//
// Adding a cache means adding a Keyspace to the registry and a builder beside
// it. The package tests then enforce that its pattern overlaps no existing
// one and that its builder's output classifies back to its own name.
//
// The key strings here are live production keys. Changing one orphans every
// cached entry under the old spelling on deploy — a silent, self-healing (via
// TTL) but total cache flush for that cache. Treat them as a wire format.
package cachekeys

import "strings"

// Unclassified is the bucket Classify returns for a key matching no
// registered keyspace. A memory exporter should surface it as its own series:
// it is how a cache added without a registry entry becomes visible.
const Unclassified = "unclassified"

// Keyspace describes one cache's key layout as a literal Prefix, an opaque
// variable segment (a room ID, an account, …), and a literal Suffix.
//
// A keyspace with Variable=false is a single fixed key and matches Prefix
// exactly. Name is the metric label and need not be unique — several
// keyspaces may report under one logical cache.
type Keyspace struct {
	Name     string
	Prefix   string
	Suffix   string
	Variable bool
	// Sample is a representative key, used by the tests to prove the pattern
	// overlaps no other keyspace and that the builder still matches it.
	Sample string
}

// Glob returns the SCAN MATCH pattern for k, for ad-hoc inspection with
// valkey-cli. Classify does not use it — matching is done on Prefix/Suffix so
// that a key containing glob metacharacters cannot be misclassified.
func (k Keyspace) Glob() string {
	if !k.Variable {
		return k.Prefix
	}
	return k.Prefix + "*" + k.Suffix
}

// Matches reports whether key belongs to this keyspace. The length test
// rejects a key too short to hold both affixes, so a Prefix and Suffix that
// overlap in a short key cannot produce a false positive.
func (k Keyspace) Matches(key string) bool {
	if !k.Variable {
		return key == k.Prefix
	}
	return len(key) >= len(k.Prefix)+len(k.Suffix) &&
		strings.HasPrefix(key, k.Prefix) &&
		strings.HasSuffix(key, k.Suffix)
}

// build renders a key for the variable segment id.
func (k Keyspace) build(id string) string {
	return k.Prefix + id + k.Suffix
}

// The registry. Every Valkey keyspace in the system appears exactly once.
// Builders below are the only supported way to construct these keys.
var (
	roomMeta = Keyspace{
		Name: "roommeta", Prefix: "room:{", Suffix: "}:meta", Variable: true,
		Sample: "room:{r1}:meta",
	}
	// roomSubs is deliberately un-hash-tagged, unlike roomMeta. The two are
	// therefore in different cluster slots for the same room. Preserved as-is:
	// this package documents the keyspace, it does not migrate it.
	//
	// The "v3" segment is pkg/roomsubcache's Member-wire-shape schema version:
	// bump it whenever a Member field changes such that an old cached entry
	// would silently decode with a zero-valued new field forever (Valkey has
	// no schema check), so stale-shape entries miss and repopulate from Mongo.
	// It lives here because it is part of the literal pattern Classify matches
	// on. Bumping it strands the previous version's keys in the unclassified
	// bucket until their TTL expires — expected, and short-lived.
	roomSubs = Keyspace{
		Name: "roomsubs", Prefix: "room:v3:", Suffix: ":subs", Variable: true,
		Sample: "room:v3:r1:subs",
	}
	presenceConns = Keyspace{
		Name: "presence.conns", Prefix: "presence:{", Suffix: "}:conns", Variable: true,
		Sample: "presence:{alice}:conns",
	}
	presenceManual = Keyspace{
		Name: "presence.manual", Prefix: "presence:{", Suffix: "}:manual", Variable: true,
		Sample: "presence:{alice}:manual",
	}
	presenceStatus = Keyspace{
		Name: "presence.status", Prefix: "presence:{", Suffix: "}:status", Variable: true,
		Sample: "presence:{alice}:status",
	}
	presenceAzure = Keyspace{
		Name: "presence.azure", Prefix: "presence:{", Suffix: "}:azure", Variable: true,
		Sample: "presence:{alice}:azure",
	}
	// The three fixed presence keys share one label — they are singletons whose
	// individual sizes are not worth separate series.
	presenceSweep       = Keyspace{Name: "presence.index", Prefix: "presence:sweep", Sample: "presence:sweep"}
	presenceInCallIndex = Keyspace{Name: "presence.index", Prefix: "presence:status:index:azure", Sample: "presence:status:index:azure"}
	presenceIDMap       = Keyspace{Name: "presence.index", Prefix: "presence:idmap:azure", Sample: "presence:idmap:azure"}

	botRateLimitCaller = Keyspace{
		Name: "botratelimit", Prefix: "botrl:caller:", Variable: true,
		Sample: "botrl:caller:u1",
	}
	botIdempotency = Keyspace{
		Name: "botidempotency", Prefix: "idem:", Variable: true,
		Sample: "idem:op1",
	}
	searchRestrictedRooms = Keyspace{
		Name: "searchrestrictedrooms", Prefix: "searchservice:restrictedrooms:", Variable: true,
		Sample: "searchservice:restrictedrooms:alice",
	}
)

// registry is the classification order. Fixed keyspaces precede variable ones
// so a singleton can never be swallowed by a broader pattern; the package
// tests additionally prove no two patterns overlap at all.
var registry = []Keyspace{
	presenceSweep,
	presenceInCallIndex,
	presenceIDMap,
	roomMeta,
	roomSubs,
	presenceConns,
	presenceManual,
	presenceStatus,
	presenceAzure,
	botRateLimitCaller,
	botIdempotency,
	searchRestrictedRooms,
}

// Keyspaces returns every registered keyspace. The returned slice is a copy,
// so callers cannot mutate the registry.
func Keyspaces() []Keyspace {
	out := make([]Keyspace, len(registry))
	copy(out, registry)
	return out
}

// Classify returns the Name of the first keyspace matching key, or
// Unclassified. It is linear in the registry size and is called once per key
// during a keyspace scan; the registry is small enough that indexing it would
// be premature.
func Classify(key string) string {
	for _, ks := range registry {
		if ks.Matches(key) {
			return ks.Name
		}
	}
	return Unclassified
}

// RoomMeta is the L2 key for a room's cached metadata (pkg/roommetacache).
// The {roomID} hash tag colocates it with the room's encryption key.
func RoomMeta(roomID string) string { return roomMeta.build(roomID) }

// RoomSubs is the key for a room's cached member list (pkg/roomsubcache).
func RoomSubs(roomID string) string { return roomSubs.build(roomID) }

// PresenceConns is the key for an account's live connection set.
func PresenceConns(account string) string { return presenceConns.build(account) }

// PresenceManual is the key for an account's manually-set presence status.
func PresenceManual(account string) string { return presenceManual.build(account) }

// PresenceStatus is the key for an account's computed effective status.
func PresenceStatus(account string) string { return presenceStatus.build(account) }

// PresenceAzure is the key for an account's Azure-sourced presence state.
func PresenceAzure(account string) string { return presenceAzure.build(account) }

// PresenceSweep is the fixed key for the presence sweep bookkeeping entry.
func PresenceSweep() string { return presenceSweep.Prefix }

// PresenceInCallIndex is the fixed key for the Azure in-call account index.
func PresenceInCallIndex() string { return presenceInCallIndex.Prefix }

// PresenceIDMap is the fixed key for the Azure-to-account id map.
func PresenceIDMap() string { return presenceIDMap.Prefix }

// BotRateLimitCaller is the per-caller fixed-window rate-limit counter key.
func BotRateLimitCaller(userID string) string { return botRateLimitCaller.build(userID) }

// BotIdempotency is the key for a bot operation's idempotency sentinel.
func BotIdempotency(opID string) string { return botIdempotency.build(opID) }

// SearchRestrictedRooms is the key for an account's cached restricted-rooms map.
func SearchRestrictedRooms(account string) string { return searchRestrictedRooms.build(account) }
