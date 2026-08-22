package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

// A cancelled client context must short-circuit the cross-site fan-out: each
// site RPC takes ~5s, so firing them for a request nobody awaits is pure waste.
// In-flight calls still fail fast via the ctx passed to GetRoomsInfo.
func TestEnrichCrossSite_ContextCancelled_SkipsRPC(t *testing.T) {
	svc, _, _, _, rooms, _, _ := newSvc(t)
	subs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "b", RoomID: "r2", SiteID: "site-b"}},
	}
	idxBySite := map[string][]int{"site-b": {0}}

	c := ctx("alice", "site-a")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	c.SetContext(cancelled)

	rooms.EXPECT().GetRoomsInfo(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	svc.enrichCrossSite(c, "alice", subs, idxBySite)

	assert.Nil(t, subs[0].Room, "cancelled fan-out leaves the sub without a room object")
}

// key32 builds a valid 32-byte room secret whose bytes are all b.
func key32(b byte) []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = b
	}
	return k
}

func TestEnrichWithRoomInfo_LocalAndCrossSite(t *testing.T) {
	svc, _, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	localMsg := time.UnixMilli(150).UTC()
	newer := int64(200)
	subs := []model.EnrichedSubscription{
		// LOCAL: enriched from the $lookup baseline (RoomName/UserCount/LastMsg*) + key.
		{Subscription: model.Subscription{ID: "a", RoomID: "r1", SiteID: "site-a", Name: "eng-sub", LastSeenAt: &seen,
			Alert: true, HasMention: true},
			RoomName: "Eng", UserCount: 7, AppCount: 2, LastMsgAt: &localMsg, LastMsgID: "m-7"},
		// CROSS-SITE: enriched via the room-service RPC.
		{Subscription: model.Subscription{ID: "b", RoomID: "r2", SiteID: "site-b", LastSeenAt: &seen}},
	}
	// LOCAL path: one key read for the local rooms; NO GetRoomsInfo for site-a.
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", []string{"r2"}).
		Return([]model.RoomInfo{{RoomID: "r2", Found: true, Name: "Ops", UserCount: 3, LastMsgAt: &newer, LastMsgID: "m-3"}}, nil)

	svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)

	assert.Equal(t, "eng-sub", subs[0].Name, "subscription name must survive enrichment")
	assert.True(t, subs[0].Alert, "stored alert preserved")
	assert.True(t, subs[0].HasMention, "stored hasMention preserved")
	require.NotNil(t, subs[0].Room)
	assert.Equal(t, "Eng", subs[0].Room.Name, "local room name comes from the baseline")
	assert.Equal(t, 7, subs[0].Room.UserCount) // baseline value (no RPC for local)
	assert.Equal(t, 2, subs[0].Room.AppCount)
	assert.Equal(t, "m-7", subs[0].Room.LastMsgID)
	require.NotNil(t, subs[0].Room.LastMsgAt)
	assert.Equal(t, localMsg, *subs[0].Room.LastMsgAt, "local baseline *time.Time passes through unconverted")

	require.NotNil(t, subs[1].Room)
	assert.Equal(t, "Ops", subs[1].Room.Name)
	assert.False(t, subs[1].Alert, "no stored alert ⇒ stays false; never set from room data")
	assert.Equal(t, 3, subs[1].Room.UserCount) // cross-site sub gets room fields via RPC
	assert.Equal(t, "m-3", subs[1].Room.LastMsgID)
	require.NotNil(t, subs[1].Room.LastMsgAt, "cross-site RPC epoch millis are converted to a timestamp")
	assert.Equal(t, time.UnixMilli(newer).UTC(), *subs[1].Room.LastMsgAt)
}

// TestEnrichWithRoomInfo_LocalKeyMaterial pins that a LOCAL sub whose room has a
// key gets base64 PrivateKey + KeyVersion from the $lookup baseline, with NO RPC.
func TestEnrichWithRoomInfo_LocalKeyMaterial(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	subs := []model.EnrichedSubscription{
		// LOCAL sub carrying the room key in its $lookup baseline (current slot).
		{Subscription: model.Subscription{ID: "a", RoomID: "r1", SiteID: "site-a"},
			RoomName: "Eng", UserCount: 5, RoomKeyPriv: key32(0xAB), RoomKeyVer: 4},
	}
	// No GetRoomsInfo expectation: an all-local input must never hit the RPC.

	svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)

	require.NotNil(t, subs[0].Room)
	assert.Equal(t, "Eng", subs[0].Room.Name)
	assert.Equal(t, 5, subs[0].Room.UserCount)
	require.NotNil(t, subs[0].Room.PrivateKey)
	// base64 of 32 bytes of 0xAB.
	assert.Equal(t, "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=", *subs[0].Room.PrivateKey)
	require.NotNil(t, subs[0].Room.KeyVersion)
	assert.Equal(t, 4, *subs[0].Room.KeyVersion)
}

// TestEnrichWithRoomInfo_LocalNoKey pins that a LOCAL sub whose room has no key
// still gets a baseline room object with no key material.
func TestEnrichWithRoomInfo_LocalNoKey(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	subs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "a", RoomID: "r1", SiteID: "site-a"},
			RoomName: "Eng", UserCount: 5, LastMsgID: "m-base"},
	}

	svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)

	require.NotNil(t, subs[0].Room, "local room must still be built from the baseline")
	assert.Equal(t, "Eng", subs[0].Room.Name)
	assert.Equal(t, 5, subs[0].Room.UserCount)
	assert.Equal(t, "m-base", subs[0].Room.LastMsgID)
	assert.Nil(t, subs[0].Room.PrivateKey, "no key ⇒ no PrivateKey")
	assert.Nil(t, subs[0].Room.KeyVersion)
}

// TestEnrichWithRoomInfo_LocalInvalidKeyLength pins that a baseline key whose
// secret isn't 32 bytes is treated as absent — the local room object is still
// built, just with no key material.
func TestEnrichWithRoomInfo_LocalInvalidKeyLength(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	subs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "a", RoomID: "r1", SiteID: "site-a"},
			RoomName: "Eng", UserCount: 5, RoomKeyPriv: []byte("short"), RoomKeyVer: 2},
	}

	svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)

	require.NotNil(t, subs[0].Room, "invalid-length key still yields a baseline room object")
	assert.Equal(t, "Eng", subs[0].Room.Name)
	assert.Equal(t, 5, subs[0].Room.UserCount)
	assert.Nil(t, subs[0].Room.PrivateKey)
	assert.Nil(t, subs[0].Room.KeyVersion)
}

// TestEnrichWithRoomInfo_AllRoomTypesKeyed pins the Option-A contract: a LOCAL
// sub of ANY room type whose room carries a 32-byte key gets base64 PrivateKey +
// KeyVersion from the $lookup baseline — keying is not gated by room type.
func TestEnrichWithRoomInfo_AllRoomTypesKeyed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		roomType model.RoomType
	}{
		{"channel", model.RoomTypeChannel},
		{"dm", model.RoomTypeDM},
		{"botDM", model.RoomTypeBotDM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, _, _, _, _, _ := newSvc(t)
			subs := []model.EnrichedSubscription{
				{Subscription: model.Subscription{ID: "a", RoomID: "r1", SiteID: "site-a", RoomType: tc.roomType},
					RoomName: "room", RoomKeyPriv: key32(0xAB), RoomKeyVer: 4},
			}

			svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)

			require.NotNil(t, subs[0].Room)
			require.NotNil(t, subs[0].Room.PrivateKey, "every room type returns its key")
			assert.Equal(t, "q6urq6urq6urq6urq6urq6urq6urq6urq6urq6urq6s=", *subs[0].Room.PrivateKey)
			require.NotNil(t, subs[0].Room.KeyVersion)
			assert.Equal(t, 4, *subs[0].Room.KeyVersion)
		})
	}
}

// TestEnrichWithRoomInfo_CrossSiteRPCZeroFields pins that a found cross-site
// room's RPC entry is authoritative for the room object even when fields are
// zero; the internal baseline stays on the flattened sub fields only.
func TestEnrichWithRoomInfo_CrossSiteRPCZeroFields(t *testing.T) {
	svc, _, _, _, rooms, _, _ := newSvc(t)
	subs := []model.EnrichedSubscription{{Subscription: model.Subscription{ID: "a", RoomID: "r2", SiteID: "site-b"}, UserCount: 5, LastMsgID: "m-base"}}
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", []string{"r2"}).
		Return([]model.RoomInfo{{RoomID: "r2", Found: true, Name: "Ops"}}, nil)
	svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)
	require.NotNil(t, subs[0].Room)
	assert.Equal(t, "Ops", subs[0].Room.Name)
	assert.Equal(t, 5, subs[0].UserCount, "internal baseline untouched")
	assert.Equal(t, "m-base", subs[0].LastMsgID, "internal baseline untouched")
}

// TestEnrichWithRoomInfo_CrossSiteNotFoundNoRoom pins that a cross-site room the
// RPC reports as not-found yields NO room object — there is no local baseline to
// fall back to for a remote room.
func TestEnrichWithRoomInfo_CrossSiteNotFoundNoRoom(t *testing.T) {
	svc, _, _, _, rooms, _, _ := newSvc(t)
	subs := []model.EnrichedSubscription{{Subscription: model.Subscription{ID: "a", RoomID: "r2", SiteID: "site-b"}, UserCount: 5, LastMsgID: "m-base"}}
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", []string{"r2"}).
		Return([]model.RoomInfo{{RoomID: "r2", Found: false}}, nil)
	svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)
	assert.Len(t, subs, 1)
	assert.False(t, subs[0].Alert)
	assert.Nil(t, subs[0].Room, "not-found cross-site room ⇒ no room object (no local baseline)")
}

// TestEnrichWithRoomInfo_LocalDeletedRoomNoRoom pins that a LOCAL sub whose room is
// soft-deleted (baseline name "Del-...") is kept but gets NO room object.
func TestEnrichWithRoomInfo_LocalDeletedRoomNoRoom(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	subs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "a", RoomID: "r1", SiteID: "site-a", Name: "team"},
			RoomName: "Del-Team", UserCount: 5, RoomKeyPriv: key32(0xAB), RoomKeyVer: 1},
	}
	svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)
	assert.Nil(t, subs[0].Room, "soft-deleted local room ⇒ no room object")
	assert.Equal(t, "team", subs[0].Name, "the subscription itself is kept")
}

// TestEnrichWithRoomInfo_CrossSiteDeletedRoomDroppedFromList pins that with
// dropDeleted=true (the list/count paths) a cross-site sub whose room-service entry
// is soft-deleted (name "Del-...") is DROPPED from the returned slice — matching the
// in-query exclusion of locally-deleted rooms — while a healthy sibling on the same
// site and a local sub survive (order preserved).
func TestEnrichWithRoomInfo_CrossSiteDeletedRoomDroppedFromList(t *testing.T) {
	svc, _, _, _, rooms, _, _ := newSvc(t)
	subs := []model.EnrichedSubscription{
		// LOCAL healthy sub — survives (no RPC).
		{Subscription: model.Subscription{ID: "loc", RoomID: "r1", SiteID: "site-a"}, RoomName: "Eng"},
		// CROSS-SITE soft-deleted — dropped.
		{Subscription: model.Subscription{ID: "del", RoomID: "r2", SiteID: "site-b"}},
		// CROSS-SITE healthy on the same site — survives.
		{Subscription: model.Subscription{ID: "ok", RoomID: "r3", SiteID: "site-b"}},
	}
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", []string{"r2", "r3"}).
		Return([]model.RoomInfo{
			{RoomID: "r2", Found: true, Name: "Del-Ops"},
			{RoomID: "r3", Found: true, Name: "Ops"},
		}, nil)

	got := svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)

	require.Len(t, got, 2, "the cross-site Del- sub is dropped from a list")
	assert.Equal(t, "loc", got[0].ID, "local sub survives, order preserved")
	assert.Equal(t, "ok", got[1].ID, "healthy cross-site sub survives, order preserved")
	require.NotNil(t, got[1].Room, "healthy cross-site sub keeps its room")
	assert.Equal(t, "Ops", got[1].Room.Name)
}

// TestEnrichWithRoomInfo_CrossSiteDeletedRoomKeptRoomlessInLookup pins that with
// dropDeleted=false (the single-item getDM/getByRoomID paths) a cross-site sub whose
// room is soft-deleted is KEPT with NO room object — exactly how a LOCAL Del- sub is
// kept room-nulled in those lookups (never dropped).
func TestEnrichWithRoomInfo_CrossSiteDeletedRoomKeptRoomlessInLookup(t *testing.T) {
	svc, _, _, _, rooms, _, _ := newSvc(t)
	subs := []model.EnrichedSubscription{{Subscription: model.Subscription{ID: "del", RoomID: "r2", SiteID: "site-b"}}}
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", []string{"r2"}).
		Return([]model.RoomInfo{{RoomID: "r2", Found: true, Name: "Del-Ops"}}, nil)

	got := svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, false, false)

	require.Len(t, got, 1, "single-item lookup keeps the Del- sub")
	assert.Equal(t, "del", got[0].ID)
	assert.Nil(t, got[0].Room, "soft-deleted cross-site room ⇒ no room object")
}

// TestEnrichWithRoomInfo_CrossSiteRPCFailDegradesSiteKeepsOthers pins per-site
// degradation: a failed site RPC leaves that site's subs without a room object,
// while sibling sites are still enriched. The local sub is built from the baseline.
func TestEnrichWithRoomInfo_CrossSiteRPCFailDegradesSiteKeepsOthers(t *testing.T) {
	svc, _, _, _, rooms, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := int64(200)
	subs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "loc", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, RoomName: "Eng"},
		{Subscription: model.Subscription{ID: "b", RoomID: "r2", SiteID: "site-b", LastSeenAt: &seen, Alert: true}},
		{Subscription: model.Subscription{ID: "c", RoomID: "r3", SiteID: "site-c", LastSeenAt: &seen}},
	}
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", []string{"r2"}).Return(nil, errors.New("down"))
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-c", []string{"r3"}).
		Return([]model.RoomInfo{{RoomID: "r3", Found: true, Name: "Ops", LastMsgAt: &newer}}, nil)

	svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)

	require.NotNil(t, subs[0].Room, "local sub built from baseline")
	assert.Equal(t, "Eng", subs[0].Room.Name)
	assert.Nil(t, subs[1].Room, "degraded cross-site sub gets no room object")
	assert.True(t, subs[1].Alert, "stored alert preserved")
	require.NotNil(t, subs[2].Room)
	assert.Equal(t, "Ops", subs[2].Room.Name) // site-c still enriched
}

func TestEnrichWithRoomInfo_Empty(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	// No GetRoomsInfo / GetMany expectations: empty input must short-circuit before any call.
	svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", nil, true, false)
	svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", []model.EnrichedSubscription{}, true, false)
}

// TestEnrichWithRoomInfo_LocalNeverRecomputesFlags pins that local enrichment
// leaves alert/hasMention alone — they are stored subscription state, never
// derived from room timestamps.
func TestEnrichWithRoomInfo_LocalNeverRecomputesFlags(t *testing.T) {
	svc, _, _, _, _, _, _ := newSvc(t)
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(999).UTC()
	mentionAt := time.UnixMilli(999).UTC()
	subs := []model.EnrichedSubscription{
		{Subscription: model.Subscription{ID: "a", RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen, Alert: false, HasMention: false},
			RoomName: "Eng", LastMsgAt: &newer, LastMentionAllAt: &mentionAt},
	}
	svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)
	assert.False(t, subs[0].Alert, "room lastMsgAt newer than lastSeen must NOT flip stored alert")
	assert.False(t, subs[0].HasMention, "room lastMentionAllAt newer than lastSeen must NOT flip stored hasMention")
}

func TestUnread(t *testing.T) {
	seen := time.UnixMilli(100).UTC()
	older := int64(50)
	newer := int64(200)
	cases := []struct {
		name     string
		lastSeen *time.Time
		ms       *int64
		want     bool
	}{
		{"nil ms is never unread", &seen, nil, false},
		{"nil lastSeen with msg is unread", nil, &newer, true},
		{"msg newer than lastSeen is unread", &seen, &newer, true},
		{"msg older than lastSeen is read", &seen, &older, false},
		{"msg equal to lastSeen is read", &seen, ptrInt64(seen.UTC().UnixMilli()), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, unread(tc.lastSeen, tc.ms))
		})
	}
}

func ptrInt64(v int64) *int64 { return &v }

// hasUnread is computed at read time for LOCAL subs: the room's lastMsgAt
// (baseline) is newer than the subscription's lastSeenAt. A deleted/absent room
// yields no room object and so is never unread.
func TestEnrichWithRoomInfo_ComputesHasUnread_Local(t *testing.T) {
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(200).UTC()
	older := time.UnixMilli(50).UTC()
	cases := []struct {
		name string
		sub  model.EnrichedSubscription
		want bool
	}{
		{"room msg newer than lastSeen is unread",
			model.EnrichedSubscription{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, RoomName: "Eng", LastMsgAt: &newer}, true},
		{"room msg older than lastSeen is read",
			model.EnrichedSubscription{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, RoomName: "Eng", LastMsgAt: &older}, false},
		{"never seen but room has a msg is unread",
			model.EnrichedSubscription{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a"}, RoomName: "Eng", LastMsgAt: &newer}, true},
		{"room has no msg is read",
			model.EnrichedSubscription{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, RoomName: "Eng"}, false},
		{"soft-deleted room (no room object) is read",
			model.EnrichedSubscription{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, RoomName: "Del-Eng", LastMsgAt: &newer}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, _, _, _, _, _ := newSvc(t)
			subs := []model.EnrichedSubscription{tc.sub}
			svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)
			assert.Equal(t, tc.want, subs[0].HasUnread)
		})
	}
}

// hasUnread is computed at read time for CROSS-SITE subs from the room-service
// RPC's lastMsgAt vs lastSeenAt; a not-found room yields no room object and is
// never unread.
func TestApplyRoomInfo_ComputesHasUnread_CrossSite(t *testing.T) {
	seen := time.UnixMilli(100).UTC()
	cases := []struct {
		name     string
		info     model.RoomInfo
		lastSeen *time.Time
		want     bool
	}{
		{"rpc msg newer than lastSeen is unread", model.RoomInfo{RoomID: "r2", Found: true, Name: "Ops", LastMsgAt: ptrInt64(200)}, &seen, true},
		{"rpc msg older than lastSeen is read", model.RoomInfo{RoomID: "r2", Found: true, Name: "Ops", LastMsgAt: ptrInt64(50)}, &seen, false},
		{"never seen but rpc has a msg is unread", model.RoomInfo{RoomID: "r2", Found: true, Name: "Ops", LastMsgAt: ptrInt64(200)}, nil, true},
		{"not found (no room object) is read", model.RoomInfo{RoomID: "r2", Found: false}, &seen, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := model.Subscription{RoomID: "r2", SiteID: "site-b", LastSeenAt: tc.lastSeen}
			applyRoomInfo(&sub, &tc.info)
			assert.Equal(t, tc.want, sub.HasUnread)
		})
	}
}

// hasGroupMention is computed at read time for LOCAL subs from the room's
// lastMentionAllAt (an @all mention) vs lastSeenAt — the @all-mention parallel of
// hasUnread; a soft-deleted room (no room object) is never a group mention.
func TestEnrichWithRoomInfo_ComputesHasGroupMention_Local(t *testing.T) {
	seen := time.UnixMilli(100).UTC()
	newer := time.UnixMilli(200).UTC()
	older := time.UnixMilli(50).UTC()
	cases := []struct {
		name string
		sub  model.EnrichedSubscription
		want bool
	}{
		{"@all mention newer than lastSeen is a group mention",
			model.EnrichedSubscription{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, RoomName: "Eng", LastMentionAllAt: &newer}, true},
		{"@all mention older than lastSeen is read",
			model.EnrichedSubscription{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, RoomName: "Eng", LastMentionAllAt: &older}, false},
		{"never seen but room has an @all mention",
			model.EnrichedSubscription{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a"}, RoomName: "Eng", LastMentionAllAt: &newer}, true},
		{"no @all mention is read",
			model.EnrichedSubscription{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, RoomName: "Eng"}, false},
		{"soft-deleted room (no room object) is read",
			model.EnrichedSubscription{Subscription: model.Subscription{RoomID: "r1", SiteID: "site-a", LastSeenAt: &seen}, RoomName: "Del-Eng", LastMentionAllAt: &newer}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, _, _, _, _, _, _ := newSvc(t)
			subs := []model.EnrichedSubscription{tc.sub}
			svc.enrichWithRoomInfoAndLastMsg(ctx("alice", "site-a"), "alice", subs, true, false)
			assert.Equal(t, tc.want, subs[0].HasGroupMention)
		})
	}
}

// hasGroupMention is computed for CROSS-SITE subs from the RPC's lastMentionAllAt
// vs lastSeenAt; a not-found room is never a group mention.
func TestApplyRoomInfo_ComputesHasGroupMention_CrossSite(t *testing.T) {
	seen := time.UnixMilli(100).UTC()
	cases := []struct {
		name     string
		info     model.RoomInfo
		lastSeen *time.Time
		want     bool
	}{
		{"rpc @all mention newer than lastSeen is a group mention", model.RoomInfo{RoomID: "r2", Found: true, Name: "Ops", LastMentionAllAt: ptrInt64(200)}, &seen, true},
		{"rpc @all mention older than lastSeen is read", model.RoomInfo{RoomID: "r2", Found: true, Name: "Ops", LastMentionAllAt: ptrInt64(50)}, &seen, false},
		{"never seen but rpc has an @all mention", model.RoomInfo{RoomID: "r2", Found: true, Name: "Ops", LastMentionAllAt: ptrInt64(200)}, nil, true},
		{"not found (no room object) is read", model.RoomInfo{RoomID: "r2", Found: false}, &seen, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := model.Subscription{RoomID: "r2", SiteID: "site-b", LastSeenAt: tc.lastSeen}
			applyRoomInfo(&sub, &tc.info)
			assert.Equal(t, tc.want, sub.HasGroupMention)
		})
	}
}

// Bots are key-holders, so a bot/pseudo principal listing its own subscriptions
// keeps the room-key material in the enriched bootstrap, exactly like a human.
func TestEnrichWithRoomInfo_BotRequester_KeepsKeyMaterial(t *testing.T) {
	secret := make([]byte, roomKeySecretLen)
	for i := range secret {
		secret[i] = 0x42
	}
	mkSubs := func() []model.EnrichedSubscription {
		return []model.EnrichedSubscription{{
			Subscription: model.Subscription{
				ID: "s1", RoomID: "r1", SiteID: "site-a",
				RoomType: model.RoomTypeChannel,
			},
			RoomName:    "general",
			RoomKeyPriv: append([]byte(nil), secret...),
			RoomKeyVer:  3,
		}}
	}
	svc := &UserService{siteID: "site-a"}

	for _, requester := range []string{"p_hook", "weather.bot", "alice"} {
		t.Run(requester+" keeps the key", func(t *testing.T) {
			got := svc.enrichWithRoomInfoAndLastMsg(ctx(requester, "site-a"), requester, mkSubs(), false, false)
			require.Len(t, got, 1)
			require.NotNil(t, got[0].Room)
			require.NotNil(t, got[0].Room.PrivateKey, "key material is no longer stripped for bots")
			assert.NotEmpty(t, *got[0].Room.PrivateKey)
			require.NotNil(t, got[0].Room.KeyVersion)
		})
	}
}

// enrichFixture builds n local subscriptions on one site, each with a room whose
// LastMsgAt makes it hint-eligible.
func enrichFixture(n int, site string) ([]model.EnrichedSubscription, map[string][]int) {
	last := time.Now().UTC()
	subs := make([]model.EnrichedSubscription, n)
	idx := make([]int, n)
	for i := range subs {
		id := fmt.Sprintf("r%d", i)
		subs[i] = model.EnrichedSubscription{
			Subscription: model.Subscription{ID: fmt.Sprintf("s%d", i), RoomID: id, SiteID: site},
		}
		subs[i].Room = &model.SubscriptionRoom{SiteID: site, LastMsgAt: &last}
		idx[i] = i
	}
	return subs, map[string][]int{site: idx}
}

// history-service hard-rejects rooms.get above 100 ids AND above 100 hints, so an
// unchunked 250-room page would come back with no previews at all.
func TestEnrichLastMessage_ChunksBeyondBatchCap(t *testing.T) {
	svc, _, history := newSvcRawHistory(t)
	subs, idxBySite := enrichFixture(250, "site-a")

	var mu sync.Mutex
	var batchSizes []int
	history.EXPECT().RoomsGet(gomock.Any(), "site-a", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, ids []string, hints map[string]model.RoomTimeHint) (map[string]model.PreviewMessage, error) {
			assert.LessOrEqual(t, len(ids), 100, "history-service rejects batches over 100 ids")
			assert.LessOrEqual(t, len(hints), 100, "history-service rejects over 100 hints too")
			assert.Len(t, hints, len(ids), "each chunk carries hints for exactly its own rooms")
			mu.Lock()
			batchSizes = append(batchSizes, len(ids))
			mu.Unlock()

			out := make(map[string]model.PreviewMessage, len(ids))
			for _, id := range ids {
				out[id] = model.PreviewMessage{MessageID: id + "-msg"}
			}
			return out, nil
		}).Times(3)

	svc.enrichLastMessage(context.Background(), "alice", subs, idxBySite)

	sort.Ints(batchSizes)
	assert.Equal(t, []int{50, 100, 100}, batchSizes)
	for i := range subs {
		require.NotNil(t, subs[i].Room, "row %d lost its room", i)
		assert.NotNil(t, subs[i].Room.PreviewMessage, "row %d lost its preview", i)
	}
}

func TestEnrichLastMessage_OneFailedChunkDegradesOnlyItsRooms(t *testing.T) {
	svc, _, history := newSvcRawHistory(t)
	subs, idxBySite := enrichFixture(250, "site-a")

	history.EXPECT().RoomsGet(gomock.Any(), "site-a", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, ids []string, _ map[string]model.RoomTimeHint) (map[string]model.PreviewMessage, error) {
			if len(ids) == 50 {
				return nil, errors.New("chunk boom")
			}
			out := make(map[string]model.PreviewMessage, len(ids))
			for _, id := range ids {
				out[id] = model.PreviewMessage{MessageID: id + "-msg"}
			}
			return out, nil
		}).Times(3)

	svc.enrichLastMessage(context.Background(), "alice", subs, idxBySite)

	var withPreview int
	for i := range subs {
		if subs[i].Room.PreviewMessage != nil {
			withPreview++
		}
	}
	assert.Equal(t, 200, withPreview, "a failed chunk must cost only its own 50 rooms, not the whole site")
}

func TestEnrichCrossSite_ChunksBeyondBatchCap(t *testing.T) {
	svc, _, _, _, rooms, _, _ := newSvc(t)
	subs, idxBySite := enrichFixture(250, "site-b")
	for i := range subs {
		subs[i].Room = nil // cross-site rows have no local room doc
	}

	var mu sync.Mutex
	var batchSizes []int
	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, ids []string) ([]model.RoomInfo, error) {
			assert.LessOrEqual(t, len(ids), 100)
			mu.Lock()
			batchSizes = append(batchSizes, len(ids))
			mu.Unlock()

			out := make([]model.RoomInfo, 0, len(ids))
			for _, id := range ids {
				out = append(out, model.RoomInfo{RoomID: id, Found: true, Name: "room-" + id})
			}
			return out, nil
		}).Times(3)

	dropped := svc.enrichCrossSite(context.Background(), "alice", subs, idxBySite)

	sort.Ints(batchSizes)
	assert.Equal(t, []int{50, 100, 100}, batchSizes)
	assert.Empty(t, dropped)
	for i := range subs {
		assert.NotNil(t, subs[i].Room, "row %d must be enriched from its chunk", i)
	}
}

func TestEnrichCrossSite_OneFailedChunkDegradesOnlyItsRooms(t *testing.T) {
	svc, _, _, _, rooms, _, _ := newSvc(t)
	subs, idxBySite := enrichFixture(250, "site-b")
	for i := range subs {
		subs[i].Room = nil
	}

	rooms.EXPECT().GetRoomsInfo(gomock.Any(), "site-b", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, ids []string) ([]model.RoomInfo, error) {
			if len(ids) == 50 {
				return nil, errors.New("chunk boom")
			}
			out := make([]model.RoomInfo, 0, len(ids))
			for _, id := range ids {
				out = append(out, model.RoomInfo{RoomID: id, Found: true, Name: "room-" + id})
			}
			return out, nil
		}).Times(3)

	svc.enrichCrossSite(context.Background(), "alice", subs, idxBySite)

	var enriched int
	for i := range subs {
		if subs[i].Room != nil {
			enriched++
		}
	}
	assert.Equal(t, 200, enriched, "a failed chunk must not blank the whole site")
}

// A room count cannot bound reply bytes: previews carry untruncated message
// bodies, so a full chunk can overflow the transport. It must be split, not lost.
func TestEnrichLastMessage_SplitsOnResponseTooLarge(t *testing.T) {
	svc, _, history := newSvcRawHistory(t)
	subs, idxBySite := enrichFixture(100, "site-a")

	var mu sync.Mutex
	var attempted []int
	history.EXPECT().RoomsGet(gomock.Any(), "site-a", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, ids []string, _ map[string]model.RoomTimeHint) (map[string]model.PreviewMessage, error) {
			mu.Lock()
			attempted = append(attempted, len(ids))
			mu.Unlock()
			// The full batch is refused; halves fit.
			if len(ids) > 50 {
				return nil, errcode.Internal("reply exceeds max_payload",
					errcode.WithReason(errcode.ResponseTooLarge))
			}
			out := make(map[string]model.PreviewMessage, len(ids))
			for _, id := range ids {
				out[id] = model.PreviewMessage{MessageID: id + "-msg"}
			}
			return out, nil
		}).AnyTimes()

	svc.enrichLastMessage(context.Background(), "alice", subs, idxBySite)

	sort.Ints(attempted)
	assert.Equal(t, []int{50, 50, 100}, attempted, "the refused batch must be halved and retried")
	for i := range subs {
		assert.NotNil(t, subs[i].Room.PreviewMessage, "row %d must survive the split", i)
	}
}

// Splitting applies only to the payload refusal; other failures must not be
// retried, or a shedding downstream would see the request multiply.
func TestEnrichLastMessage_DoesNotSplitOtherErrors(t *testing.T) {
	svc, _, history := newSvcRawHistory(t)
	subs, idxBySite := enrichFixture(100, "site-a")

	history.EXPECT().RoomsGet(gomock.Any(), "site-a", gomock.Any(), gomock.Any()).
		Return(nil, errcode.Unavailable("shed")).Times(1)

	svc.enrichLastMessage(context.Background(), "alice", subs, idxBySite)

	for i := range subs {
		assert.Nil(t, subs[i].Room.PreviewMessage, "row %d degrades without a retry", i)
	}
}

// A half that still overflows degrades alone rather than taking the page with it.
func TestEnrichLastMessage_KeepsTheHalfThatFits(t *testing.T) {
	svc, _, history := newSvcRawHistory(t)
	subs, idxBySite := enrichFixture(100, "site-a")

	history.EXPECT().RoomsGet(gomock.Any(), "site-a", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, ids []string, _ map[string]model.RoomTimeHint) (map[string]model.PreviewMessage, error) {
			// Only the first half ever fits; the second keeps refusing to one room.
			if len(ids) > 50 || ids[0] != "r0" {
				return nil, errcode.Internal("too large", errcode.WithReason(errcode.ResponseTooLarge))
			}
			out := make(map[string]model.PreviewMessage, len(ids))
			for _, id := range ids {
				out[id] = model.PreviewMessage{MessageID: id + "-msg"}
			}
			return out, nil
		}).AnyTimes()

	svc.enrichLastMessage(context.Background(), "alice", subs, idxBySite)

	var withPreview int
	for i := range subs {
		if subs[i].Room.PreviewMessage != nil {
			withPreview++
		}
	}
	assert.Equal(t, 50, withPreview, "the half that fits must survive")
}

// The fan-out semaphores send before spawning their receiver, so a capacity of
// zero is an unbuffered channel that blocks forever. A directly-constructed
// UserService must not be able to hang the enrichment.
func TestFanout_NormalisesNonPositive(t *testing.T) {
	for _, n := range []int{0, -1} {
		assert.Equal(t, defaultSiteFanout, (&UserService{maxFanout: n}).fanout())
	}
	assert.Equal(t, 3, (&UserService{maxFanout: 3}).fanout())
}

func TestEnrichLastMessage_UnsetFanoutDoesNotDeadlock(t *testing.T) {
	_, _, history := newSvcRawHistory(t)
	// maxFanout deliberately left at zero, as the badge/thread fixtures build it.
	svc := &UserService{siteID: "site-a", history: history, roomBatchChunk: 100}
	subs, idxBySite := enrichFixture(10, "site-a")

	history.EXPECT().RoomsGet(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]model.PreviewMessage{}, nil).AnyTimes()

	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.enrichLastMessage(context.Background(), "alice", subs, idxBySite)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("enrichment deadlocked on an unbuffered semaphore")
	}
}

// A page whose previews would blow the byte budget degrades the overflow instead
// of assembling it: message bodies are capped at 20 KB each, but 400 of them are
// not, and the budget is the only thing standing between a max-size page and the
// pod's memory.
func TestEnrichLastMessage_StopsAtPreviewByteBudget(t *testing.T) {
	svc, _, history := newSvcRawHistory(t)
	svc.previewBudget = 40 * 1024 // fits two 20 KB previews, not four
	subs, idxBySite := enrichFixture(4, "site-a")
	svc.roomBatchChunk = 1 // one room per RPC, so the budget bites between chunks

	big := strings.Repeat("x", 20*1024)
	history.EXPECT().RoomsGet(gomock.Any(), "site-a", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, ids []string, _ map[string]model.RoomTimeHint) (map[string]model.PreviewMessage, error) {
			out := make(map[string]model.PreviewMessage, len(ids))
			for _, id := range ids {
				out[id] = model.PreviewMessage{MessageID: id + "-msg", Content: big}
			}
			return out, nil
		}).AnyTimes()

	svc.enrichLastMessage(context.Background(), "alice", subs, idxBySite)

	var attached int
	for i := range subs {
		if subs[i].Room.PreviewMessage != nil {
			attached++
		}
	}
	assert.Greater(t, attached, 0, "the budget must not degrade the whole page")
	assert.Less(t, attached, 4, "previews past the budget must be dropped")
}

// On a payload-capped transport the page's own reply shares the ceiling history
// just hit, so splitting spends RPCs on data that can never be delivered.
func TestEnrichLastMessage_NoSplitOnPayloadCappedTransport(t *testing.T) {
	svc, _, history := newSvcRawHistory(t)
	subs, idxBySite := enrichFixture(100, "site-a")

	history.EXPECT().RoomsGet(gomock.Any(), "site-a", gomock.Any(), gomock.Any()).
		Return(nil, errcode.Internal("too large", errcode.WithReason(errcode.ResponseTooLarge))).Times(1)

	svc.enrichLastMessage(payloadCapped(context.Background()), "alice", subs, idxBySite)

	for i := range subs {
		assert.Nil(t, subs[i].Room.PreviewMessage, "row %d degrades without a split", i)
	}
}

// Unbounded halving is exponential in RPCs; past the depth cap the chunk degrades.
func TestEnrichLastMessage_BoundsSplitDepth(t *testing.T) {
	svc, _, history := newSvcRawHistory(t)
	subs, idxBySite := enrichFixture(100, "site-a")

	var mu sync.Mutex
	var calls int
	history.EXPECT().RoomsGet(gomock.Any(), "site-a", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ []string, _ map[string]model.RoomTimeHint) (map[string]model.PreviewMessage, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			// Never satisfiable, so only the cap can stop the recursion.
			return nil, errcode.Internal("too large", errcode.WithReason(errcode.ResponseTooLarge))
		}).AnyTimes()

	svc.enrichLastMessage(context.Background(), "alice", subs, idxBySite)

	assert.LessOrEqual(t, calls, 31, "a chunk must not exceed 2^(maxSplitDepth+1)-1 RPCs")
	for i := range subs {
		assert.Nil(t, subs[i].Room.PreviewMessage, "row %d degrades once the cap is hit", i)
	}
}
