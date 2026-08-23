package cachekeys

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuilders_ExactStrings pins every builder to the literal it replaced.
// These strings are live production keys — a change here orphans cached
// entries on deploy, so the expected values must never be "fixed" to match
// an implementation change.
func TestBuilders_ExactStrings(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"room meta", RoomMeta("r123"), "room:{r123}:meta"},
		{"room subs", RoomSubs("r123"), "room:v3:r123:subs"},
		{"presence conns", PresenceConns("alice"), "presence:{alice}:conns"},
		{"presence manual", PresenceManual("alice"), "presence:{alice}:manual"},
		{"presence status", PresenceStatus("alice"), "presence:{alice}:status"},
		{"presence azure", PresenceAzure("alice"), "presence:{alice}:azure"},
		{"presence sweep", PresenceSweep(), "presence:sweep"},
		{"presence in-call index", PresenceInCallIndex(), "presence:status:index:azure"},
		{"presence id map", PresenceIDMap(), "presence:idmap:azure"},
		{"bot rate limit", BotRateLimitCaller("u1"), "botrl:caller:u1"},
		{"bot idempotency", BotIdempotency("op1"), "idem:op1"},
		{"search restricted rooms", SearchRestrictedRooms("alice"), "searchservice:restrictedrooms:alice"},
		{"badge set", BadgeSet("alice"), "badge:{alice}"},
		{"badge fresh marker", BadgeFresh("alice"), "badge:fresh:{alice}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.got)
		})
	}
}

// TestBuilders_EmptyArgument documents that builders do not validate their
// argument: an empty ID yields a structurally valid but meaningless key.
// Callers are responsible for rejecting empty IDs upstream.
func TestBuilders_EmptyArgument(t *testing.T) {
	assert.Equal(t, "room:{}:meta", RoomMeta(""))
	assert.Equal(t, "room:v3::subs", RoomSubs(""))
	assert.Equal(t, "idem:", BotIdempotency(""))
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{"room meta", RoomMeta("r123"), "roommeta"},
		{"room subs", RoomSubs("r123"), "roomsubs"},
		{"presence conns", PresenceConns("alice"), "presence.conns"},
		{"presence manual", PresenceManual("alice"), "presence.manual"},
		{"presence status", PresenceStatus("alice"), "presence.status"},
		{"presence azure", PresenceAzure("alice"), "presence.azure"},
		{"presence sweep", PresenceSweep(), "presence.index"},
		{"presence in-call index", PresenceInCallIndex(), "presence.index"},
		{"presence id map", PresenceIDMap(), "presence.index"},
		{"bot rate limit", BotRateLimitCaller("u1"), "botratelimit"},
		{"bot idempotency", BotIdempotency("op1"), "botidempotency"},
		{"search restricted", SearchRestrictedRooms("alice"), "searchrestrictedrooms"},
		{"badge set", BadgeSet("alice"), "badge"},
		{"badge fresh marker", BadgeFresh("alice"), "badge.fresh"},
		{"badge-like but unknown suffix", "badge:{alice}:nope", Unclassified},
		{"unknown prefix", "somethingnew:abc", Unclassified},
		{"empty key", "", Unclassified},
		{"room-like but unknown suffix", "room:r1:members", Unclassified},
		{"presence-like but unknown suffix", "presence:{alice}:nope", Unclassified},
		{"prefix only, no variable segment", "room:{", Unclassified},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Classify(tc.key))
		})
	}
}

// TestClassify_HashTaggedAndUntaggedRoomKeysDoNotCollide guards the one real
// ambiguity in the keyspace: roommetacache hash-tags its room ID and
// roomsubcache does not, so both live under a "room:" prefix.
func TestClassify_HashTaggedAndUntaggedRoomKeysDoNotCollide(t *testing.T) {
	assert.Equal(t, "roommeta", Classify("room:{r1}:meta"))
	assert.Equal(t, "roomsubs", Classify("room:v3:r1:subs"))
	assert.NotEqual(t, Classify(RoomMeta("r1")), Classify(RoomSubs("r1")))
}

// TestClassify_BadgeSetAndMarkerDoNotCollide guards the second real ambiguity
// in the keyspace: the badge freshness marker nests under the badge set's own
// "badge:" prefix, so a marker key must not be attributed to the set.
func TestClassify_BadgeSetAndMarkerDoNotCollide(t *testing.T) {
	assert.Equal(t, "badge", Classify("badge:{alice}"))
	assert.Equal(t, "badge.fresh", Classify("badge:fresh:{alice}"))
	assert.NotEqual(t, Classify(BadgeSet("alice")), Classify(BadgeFresh("alice")))
}

// TestKeyspaces_SampleMatchesExactlyOne is the invariant that keeps Classify
// unambiguous as keyspaces are added: no sample key may match two keyspaces.
// A new keyspace whose pattern overlaps an existing one fails here.
func TestKeyspaces_SampleMatchesExactlyOne(t *testing.T) {
	spaces := Keyspaces()
	require.NotEmpty(t, spaces)

	for _, ks := range spaces {
		require.NotEmpty(t, ks.Sample, "keyspace %q must carry a Sample key", ks.Name)
		var matched []string
		for _, other := range spaces {
			if other.Matches(ks.Sample) {
				matched = append(matched, other.Name+" ("+other.Glob()+")")
			}
		}
		assert.Len(t, matched, 1,
			"sample %q of keyspace %q matched %v; patterns must not overlap",
			ks.Sample, ks.Name, matched)
	}
}

// TestKeyspaces_SampleClassifiesToOwnName guards against a builder and its
// keyspace pattern drifting apart — the failure that would silently move a
// cache into the unclassified bucket.
func TestKeyspaces_SampleClassifiesToOwnName(t *testing.T) {
	for _, ks := range Keyspaces() {
		t.Run(ks.Name+" "+ks.Glob(), func(t *testing.T) {
			assert.Equal(t, ks.Name, Classify(ks.Sample))
		})
	}
}

func TestKeyspaces_WellFormed(t *testing.T) {
	for _, ks := range Keyspaces() {
		t.Run(ks.Glob(), func(t *testing.T) {
			assert.NotEmpty(t, ks.Name, "Name is the metric label and must be set")
			assert.NotEmpty(t, ks.Prefix, "Prefix must be set")
			assert.NotEqual(t, Unclassified, ks.Name, "Name must not shadow the unclassified bucket")
			if !ks.Variable {
				assert.Empty(t, ks.Suffix, "a fixed keyspace has no Suffix")
			}
		})
	}
}

// TestKeyspaces_ReturnsCopy ensures a caller cannot mutate the registry.
func TestKeyspaces_ReturnsCopy(t *testing.T) {
	first := Keyspaces()
	require.NotEmpty(t, first)
	original := first[0].Name
	first[0].Name = "mutated"

	assert.Equal(t, original, Keyspaces()[0].Name)
}

func TestKeyspace_Glob(t *testing.T) {
	tests := []struct {
		name string
		ks   Keyspace
		want string
	}{
		{"variable", Keyspace{Prefix: "room:{", Suffix: "}:meta", Variable: true}, "room:{*}:meta"},
		{"fixed", Keyspace{Prefix: "presence:sweep"}, "presence:sweep"},
		{"variable with empty suffix", Keyspace{Prefix: "idem:", Variable: true}, "idem:*"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.ks.Glob())
		})
	}
}

func TestKeyspace_Matches(t *testing.T) {
	variable := Keyspace{Name: "v", Prefix: "room:{", Suffix: "}:meta", Variable: true}
	fixed := Keyspace{Name: "f", Prefix: "presence:sweep"}

	tests := []struct {
		name string
		ks   Keyspace
		key  string
		want bool
	}{
		{"variable matches", variable, "room:{r1}:meta", true},
		{"variable matches empty segment", variable, "room:{}:meta", true},
		{"variable rejects wrong suffix", variable, "room:{r1}:subs", false},
		{"variable rejects wrong prefix", variable, "rooms:{r1}:meta", false},
		{"variable rejects prefix/suffix overlap", variable, "room:{:meta", false},
		{"variable rejects short key", variable, "room:{", false},
		{"fixed matches exactly", fixed, "presence:sweep", true},
		{"fixed rejects extension", fixed, "presence:sweeper", false},
		{"fixed rejects empty", fixed, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.ks.Matches(tc.key))
		})
	}
}
