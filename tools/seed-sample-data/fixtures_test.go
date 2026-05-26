package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

func TestSeedBaseTime_IsFixed(t *testing.T) {
	want, err := time.Parse(time.RFC3339, "2026-05-01T09:00:00Z")
	assert.NoError(t, err)
	assert.Equal(t, want, seedBaseTime)
}

func TestSiteIDs_AreLocalAndRemote(t *testing.T) {
	assert.Equal(t, "site-local", siteLocal)
	assert.Equal(t, "site-remote", siteRemote)
}

func TestBuildUsers_ReturnsExpectedRoster(t *testing.T) {
	users := BuildUsers()

	require.Len(t, users, 10)

	wantIDs := []string{
		"u-alice", "u-bob", "u-carol", "u-dave", "u-eve",
		"u-frank", "u-grace", "u-heidi", "u-ivan", "u-judy",
	}
	gotIDs := make([]string, len(users))
	for i, u := range users {
		gotIDs[i] = u.ID
	}
	assert.Equal(t, wantIDs, gotIDs)
}

func TestBuildUsers_AccountMatchesIDSuffix(t *testing.T) {
	for _, u := range BuildUsers() {
		assert.Equal(t, "u-"+u.Account, u.ID,
			"user %q has account %q but id %q — must match `u-<account>`", u.Account, u.Account, u.ID)
	}
}

func TestBuildUsers_SiteDistribution(t *testing.T) {
	local, remote := 0, 0
	for _, u := range BuildUsers() {
		switch u.SiteID {
		case siteLocal:
			local++
		case siteRemote:
			remote++
		default:
			t.Fatalf("unexpected siteId %q on user %s", u.SiteID, u.ID)
		}
	}
	assert.Equal(t, 8, local, "8 local users (alice..heidi)")
	assert.Equal(t, 2, remote, "2 remote users (ivan, judy)")
}

func TestBuildUsers_RequiredFieldsPopulated(t *testing.T) {
	for _, u := range BuildUsers() {
		assert.NotEmpty(t, u.ID, "id")
		assert.NotEmpty(t, u.Account, "account for %s", u.ID)
		assert.NotEmpty(t, u.SiteID, "siteId for %s", u.ID)
		assert.NotEmpty(t, u.SectID, "sectId for %s", u.ID)
		assert.NotEmpty(t, u.SectName, "sectName for %s", u.ID)
		assert.NotEmpty(t, u.DeptID, "deptId for %s", u.ID)
		assert.NotEmpty(t, u.DeptName, "deptName for %s", u.ID)
		assert.NotEmpty(t, u.EngName, "engName for %s", u.ID)
		assert.NotEmpty(t, u.ChineseName, "chineseName for %s", u.ID)
		assert.NotEmpty(t, u.EmployeeID, "employeeId for %s", u.ID)
	}
}

func TestBuildRooms_ReturnsSixRooms(t *testing.T) {
	rooms := BuildRooms()
	require.Len(t, rooms, 6)

	byID := make(map[string]model.Room, len(rooms))
	for _, r := range rooms {
		byID[r.ID] = r
	}

	for _, id := range []string{"r-general", "r-eng", "r-design", "r-remote-announce"} {
		_, ok := byID[id]
		assert.True(t, ok, "missing room %q", id)
	}
	_, ok := byID[idgen.BuildDMRoomID("u-alice", "u-bob")]
	assert.True(t, ok, "missing alice-bob DM room")
	_, ok = byID[idgen.BuildDMRoomID("u-carol", "u-eve")]
	assert.True(t, ok, "missing carol-eve DM room")
}

func TestBuildRooms_TypesAndSites(t *testing.T) {
	want := map[string]struct {
		typ    model.RoomType
		siteID string
	}{
		"r-general":                             {model.RoomTypeChannel, siteLocal},
		"r-eng":                                 {model.RoomTypeChannel, siteLocal},
		"r-design":                              {model.RoomTypeChannel, siteLocal},
		idgen.BuildDMRoomID("u-alice", "u-bob"): {model.RoomTypeDM, siteLocal},
		idgen.BuildDMRoomID("u-carol", "u-eve"): {model.RoomTypeDM, siteLocal},
		"r-remote-announce":                     {model.RoomTypeChannel, siteRemote},
	}
	for _, r := range BuildRooms() {
		w, ok := want[r.ID]
		require.True(t, ok, "unexpected room id %q", r.ID)
		assert.Equal(t, w.typ, r.Type, "room %s type", r.ID)
		assert.Equal(t, w.siteID, r.SiteID, "room %s siteId", r.ID)
	}
}

func TestBuildRooms_EngIsRestricted(t *testing.T) {
	for _, r := range BuildRooms() {
		if r.ID == "r-eng" {
			assert.True(t, r.Restricted, "r-eng must be restricted for search-cache coverage")
			return
		}
	}
	t.Fatal("r-eng not found")
}

func TestBuildRooms_UserCountMatchesMemberLists(t *testing.T) {
	for _, r := range BuildRooms() {
		assert.Equal(t, len(r.UIDs), r.UserCount, "room %s userCount must match len(UIDs)", r.ID)
		assert.Equal(t, len(r.UIDs), len(r.Accounts), "room %s UIDs/Accounts must be same length", r.ID)
	}
}

func TestBuildRoomMembers_ChannelsOnly(t *testing.T) {
	members := BuildRoomMembers()
	assert.Len(t, members, 19)

	dmAB := idgen.BuildDMRoomID("u-alice", "u-bob")
	dmCE := idgen.BuildDMRoomID("u-carol", "u-eve")
	for _, m := range members {
		assert.NotEqual(t, dmAB, m.RoomID, "alice-bob DM must not appear in room_members")
		assert.NotEqual(t, dmCE, m.RoomID, "carol-eve DM must not appear in room_members")
	}
}

func TestBuildRoomMembers_StableIDFormat(t *testing.T) {
	for _, m := range BuildRoomMembers() {
		want := m.RoomID + ":" + m.Member.ID
		assert.Equal(t, want, m.ID, "RoomMember.ID must be `<roomID>:<userID>`")
		assert.Equal(t, model.RoomMemberIndividual, m.Member.Type)
		assert.NotEmpty(t, m.Member.Account)
		assert.Equal(t, seedBaseTime, m.Ts)
	}
}

func TestBuildRoomMembers_OneEntryPerChannelUser(t *testing.T) {
	got := make(map[string]int)
	for _, m := range BuildRoomMembers() {
		got[m.RoomID]++
	}
	assert.Equal(t, 9, got["r-general"])
	assert.Equal(t, 4, got["r-eng"])
	assert.Equal(t, 3, got["r-design"])
	assert.Equal(t, 3, got["r-remote-announce"])
}

func TestBuildSubscriptions_Count(t *testing.T) {
	assert.Len(t, BuildSubscriptions(), 23)
}

func TestBuildSubscriptions_StableID(t *testing.T) {
	for _, s := range BuildSubscriptions() {
		want := "sub:" + s.User.ID + ":" + s.RoomID
		assert.Equal(t, want, s.ID, "Subscription.ID must be `sub:<userID>:<roomID>`")
	}
}

func TestBuildSubscriptions_OwnerRoles(t *testing.T) {
	owners := map[string]string{
		"r-general":         "u-alice",
		"r-eng":             "u-alice",
		"r-design":          "u-frank",
		"r-remote-announce": "u-ivan",
	}
	got := map[string]string{}
	for _, s := range BuildSubscriptions() {
		for _, role := range s.Roles {
			if role == model.RoleOwner {
				got[s.RoomID] = s.User.ID
			}
		}
	}
	for roomID, wantUser := range owners {
		assert.Equal(t, wantUser, got[roomID], "owner of %s", roomID)
	}
}

func TestBuildSubscriptions_FieldsPopulated(t *testing.T) {
	for _, s := range BuildSubscriptions() {
		assert.NotEmpty(t, s.User.ID)
		assert.NotEmpty(t, s.User.Account)
		assert.NotEmpty(t, s.RoomID)
		assert.NotEmpty(t, s.SiteID)
		assert.NotEmpty(t, s.RoomType)
		assert.NotEmpty(t, s.Roles, "subscription %s has empty roles", s.ID)
		assert.True(t, s.IsSubscribed, "IsSubscribed should be true for seeded subs")
		assert.False(t, s.JoinedAt.IsZero())
	}
}

func TestBuildSubscriptions_DMSubscriptionsHaveCounterpartName(t *testing.T) {
	dmAB := idgen.BuildDMRoomID("u-alice", "u-bob")
	gotForAB := 0
	for _, s := range BuildSubscriptions() {
		if s.RoomID != dmAB {
			continue
		}
		gotForAB++
		if s.User.Account == "alice" {
			assert.Equal(t, "bob", s.Name, "alice's DM sub Name = counterpart account")
		}
		if s.User.Account == "bob" {
			assert.Equal(t, "alice", s.Name, "bob's DM sub Name = counterpart account")
		}
	}
	assert.Equal(t, 2, gotForAB, "DM room must have exactly 2 subscriptions")
}

func TestBuildMessages_TotalCount(t *testing.T) {
	assert.Len(t, BuildMessages(), 23)
}

func TestBuildMessages_MonotonicTimestampsPerRoom(t *testing.T) {
	byRoom := map[string][]time.Time{}
	for _, m := range BuildMessages() {
		byRoom[m.RoomID] = append(byRoom[m.RoomID], m.CreatedAt)
	}
	for room, ts := range byRoom {
		for i := 1; i < len(ts); i++ {
			assert.True(t, ts[i].After(ts[i-1]),
				"room %s: timestamps must be strictly increasing (idx %d %v vs %v)", room, i, ts[i-1], ts[i])
		}
	}
}

func TestBuildMessages_DeterministicIDs(t *testing.T) {
	first := BuildMessages()
	second := BuildMessages()
	require.Equal(t, len(first), len(second))
	for i := range first {
		assert.Equal(t, first[i].ID, second[i].ID, "message IDs must be deterministic across calls")
	}
}

func TestBuildMessages_IDsAreValidMessageIDs(t *testing.T) {
	for _, m := range BuildMessages() {
		assert.True(t, idgen.IsValidMessageID(m.ID), "id %q for message in room %s not a valid message id", m.ID, m.RoomID)
	}
}

func TestBuildMessages_AuthorIsRoomMember(t *testing.T) {
	rooms := roomsByID()
	for _, m := range BuildMessages() {
		r, ok := rooms[m.RoomID]
		require.True(t, ok, "message references unknown room %s", m.RoomID)
		found := false
		for _, account := range r.Accounts {
			if account == m.UserAccount {
				found = true
				break
			}
		}
		assert.True(t, found, "message author %s is not a member of room %s", m.UserAccount, m.RoomID)
	}
}

func TestBuildThreadRooms_OneEntry(t *testing.T) {
	trs := BuildThreadRooms()
	require.Len(t, trs, 1)

	tr := trs[0]
	assert.Equal(t, "tr-uuidv7-debate", tr.ID)
	assert.Equal(t, "r-eng", tr.RoomID)
	assert.Equal(t, siteLocal, tr.SiteID)
	assert.NotEmpty(t, tr.ParentMessageID, "must reference the parent message ID")
	assert.NotEmpty(t, tr.LastMsgID)
	assert.False(t, tr.LastMsgAt.IsZero())
	assert.ElementsMatch(t, []string{"bob", "carol"}, tr.ReplyAccounts)
}

func TestBuildThreadRooms_ParentMessageExistsInBuildMessages(t *testing.T) {
	tr := BuildThreadRooms()[0]
	for _, m := range BuildMessages() {
		if m.ID == tr.ParentMessageID {
			return
		}
	}
	t.Fatalf("thread parent message ID %q is not in BuildMessages()", tr.ParentMessageID)
}

func TestBuildThreadSubscriptions_BobAndCarol(t *testing.T) {
	subs := BuildThreadSubscriptions()
	require.Len(t, subs, 2)
	got := []string{subs[0].UserAccount, subs[1].UserAccount}
	assert.ElementsMatch(t, []string{"bob", "carol"}, got)

	for _, s := range subs {
		assert.Equal(t, "tr-uuidv7-debate", s.ThreadRoomID)
		assert.Equal(t, "r-eng", s.RoomID)
		assert.Equal(t, siteLocal, s.SiteID)
		assert.NotEmpty(t, s.ParentMessageID)
		assert.False(t, s.CreatedAt.IsZero())
	}
}

func TestBuildRoomsWithLastMsg_PopulatesLastMessageFields(t *testing.T) {
	rooms := BuildRoomsWithLastMsg()
	require.Len(t, rooms, 6)

	latest := map[string]model.Message{}
	for _, m := range BuildMessages() {
		l, ok := latest[m.RoomID]
		if !ok || m.CreatedAt.After(l.CreatedAt) {
			latest[m.RoomID] = m
		}
	}
	for _, r := range rooms {
		want, ok := latest[r.ID]
		require.True(t, ok, "no messages found for seeded room %s", r.ID)
		assert.Equal(t, want.ID, r.LastMsgID, "room %s LastMsgID", r.ID)
		require.NotNil(t, r.LastMsgAt, "room %s LastMsgAt should be set", r.ID)
		assert.True(t, r.LastMsgAt.Equal(want.CreatedAt), "room %s LastMsgAt", r.ID)
	}
}

func TestConsistency_AllSubscriptionsHaveValidRoom(t *testing.T) {
	rooms := roomsByID()
	for _, s := range BuildSubscriptions() {
		_, ok := rooms[s.RoomID]
		assert.True(t, ok, "subscription %s references unknown room %s", s.ID, s.RoomID)
	}
}

func TestConsistency_AllRoomMembersHaveValidUserAndRoom(t *testing.T) {
	users := usersByAccount()
	rooms := roomsByID()
	for _, m := range BuildRoomMembers() {
		_, ok := rooms[m.RoomID]
		assert.True(t, ok, "room_member %s references unknown room", m.ID)
		_, ok = users[m.Member.Account]
		assert.True(t, ok, "room_member %s references unknown account %s", m.ID, m.Member.Account)
	}
}

func TestConsistency_AllMessageAuthorsAreUsers(t *testing.T) {
	users := usersByAccount()
	for _, m := range BuildMessages() {
		_, ok := users[m.UserAccount]
		assert.True(t, ok, "message %s in %s authored by unknown account %s", m.ID, m.RoomID, m.UserAccount)
	}
}

func TestBuildRoomKeys_OneKeyPerRoom(t *testing.T) {
	keys := BuildRoomKeys()
	require.Len(t, keys, 6, "one key per seeded room")
	seen := map[string]bool{}
	for _, k := range keys {
		assert.False(t, seen[k.RoomID], "duplicate room key for %s", k.RoomID)
		seen[k.RoomID] = true
		assert.Len(t, k.KeyPair.PrivateKey, 32, "AES-256 key must be 32 bytes")
	}
}

func TestBuildRoomKeys_DeterministicAcrossCalls(t *testing.T) {
	a := BuildRoomKeys()
	b := BuildRoomKeys()
	require.Equal(t, len(a), len(b))
	for i := range a {
		assert.Equal(t, a[i].RoomID, b[i].RoomID)
		assert.Equal(t, a[i].KeyPair.PrivateKey, b[i].KeyPair.PrivateKey, "room %s key must be stable across calls", a[i].RoomID)
	}
}

func TestBuildRestrictedCache_OneEntryPerEngMember(t *testing.T) {
	entries := BuildRestrictedCache()
	require.Len(t, entries, 4, "alice + bob + carol + ivan are r-eng members")
	wantJoinMs := seedBaseTime.UnixMilli()
	accounts := make([]string, len(entries))
	for i, e := range entries {
		accounts[i] = e.Account
		assert.Contains(t, e.Rooms, "r-eng", "%s cache must list r-eng", e.Account)
		assert.Equal(t, wantJoinMs, e.Rooms["r-eng"], "%s r-eng join ts", e.Account)
	}
	assert.ElementsMatch(t, []string{"alice", "bob", "carol", "ivan"}, accounts)
}
