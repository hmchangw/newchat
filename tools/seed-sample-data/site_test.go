package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestUserSiteByAccount_MapsSeededUsersToHomeSites(t *testing.T) {
	got := userSiteByAccount()

	assert.Equal(t, "site-local", got["alice"])
	assert.Equal(t, "site-remote", got["ivan"])
	assert.Equal(t, "site-remote", got["judy"])
	assert.Len(t, got, len(BuildUsers()))
}

func TestRoomSiteByID_MapsSeededRoomsToHomeSites(t *testing.T) {
	got := roomSiteByID()

	assert.Equal(t, "site-local", got["r-general"])
	assert.Equal(t, "site-remote", got["r-remote-announce"])
	assert.Len(t, got, len(BuildRooms()))
}

// Fixture selection helpers. These only *find* a seeded row; they deliberately
// contain no routing logic, so a test built on them still has to call the
// production accessor to learn a row's home site.

func findRoom(t *testing.T, id string) model.Room {
	t.Helper()
	rooms := BuildRooms()
	for i := range rooms {
		if rooms[i].ID == id {
			return rooms[i]
		}
	}
	t.Fatalf("no seeded room with id %q", id)
	return model.Room{}
}

func findMessage(t *testing.T, roomID, author string) model.Message {
	t.Helper()
	msgs := BuildMessages()
	for i := range msgs {
		if msgs[i].RoomID == roomID && msgs[i].UserAccount == author {
			return msgs[i]
		}
	}
	t.Fatalf("no seeded message in room %q by %q", roomID, author)
	return model.Message{}
}

func findMember(t *testing.T, roomID, account string) model.RoomMember {
	t.Helper()
	members := BuildRoomMembers()
	for i := range members {
		if members[i].RoomID == roomID && members[i].Member.Account == account {
			return members[i]
		}
	}
	t.Fatalf("no seeded room_member for %q in room %q", account, roomID)
	return model.RoomMember{}
}

func findSubscription(t *testing.T, account, roomID string) model.Subscription {
	t.Helper()
	subs := BuildSubscriptions()
	for i := range subs {
		if subs[i].User.Account == account && subs[i].RoomID == roomID {
			return subs[i]
		}
	}
	t.Fatalf("no seeded subscription for %q in room %q", account, roomID)
	return model.Subscription{}
}

func findRoomKey(t *testing.T, roomID string) RoomKeyEntry {
	t.Helper()
	for _, k := range BuildRoomKeys() {
		if k.RoomID == roomID {
			return k
		}
	}
	t.Fatalf("no seeded room key for room %q", roomID)
	return RoomKeyEntry{}
}

func findRestrictedCacheEntry(t *testing.T, account string) RestrictedCacheEntry {
	t.Helper()
	for _, e := range BuildRestrictedCache() {
		if e.Account == account {
			return e
		}
	}
	t.Fatalf("no seeded restricted-rooms cache entry for %q", account)
	return RestrictedCacheEntry{}
}

// --- Rule 1: room-owned rows route by the ROOM's home site. ---
//
// Each case below is chosen so that routing by anything else (the author's or
// the member's home site, or a SiteID field carried on the row) produces a
// different answer — otherwise the test could not tell the rules apart.

func TestRoomHomeSite(t *testing.T) {
	tests := []struct {
		name   string
		roomID string
		want   string
	}{
		{name: "local channel is homed locally", roomID: "r-general", want: "site-local"},
		{name: "restricted local channel is homed locally", roomID: "r-eng", want: "site-local"},
		{name: "remote channel is homed remotely", roomID: "r-remote-announce", want: "site-remote"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, roomHomeSite(findRoom(t, tt.roomID)))
		})
	}
}

func TestMessageHomeSite(t *testing.T) {
	tests := []struct {
		name    string
		roomID  string
		author  string
		want    string
		because string
	}{
		{
			name: "message by a local author in a local room", roomID: "r-general", author: "alice",
			want: "site-local",
		},
		{
			name: "message by a REMOTE author in a local room follows the room", roomID: "r-eng", author: "ivan",
			want: "site-local", because: "ivan is homed remotely; the message still belongs to r-eng's site",
		},
		{
			name: "message by a LOCAL author in a remote room follows the room", roomID: "r-remote-announce", author: "alice",
			want: "site-remote", because: "alice is homed locally; the message still belongs to r-remote-announce's site",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, messageHomeSite(findMessage(t, tt.roomID, tt.author)), tt.because)
		})
	}
}

func TestMemberHomeSite(t *testing.T) {
	tests := []struct {
		name    string
		roomID  string
		account string
		want    string
		because string
	}{
		{
			name: "local member of a local room", roomID: "r-general", account: "alice",
			want: "site-local",
		},
		{
			name: "REMOTE member of a local room follows the room", roomID: "r-general", account: "ivan",
			want: "site-local", because: "ivan is homed remotely; his r-general membership row belongs to site-local",
		},
		{
			name: "LOCAL member of a remote room follows the room", roomID: "r-remote-announce", account: "alice",
			want: "site-remote", because: "alice is homed locally; her r-remote-announce membership row belongs to site-remote",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, memberHomeSite(findMember(t, tt.roomID, tt.account)), tt.because)
		})
	}
}

func TestThreadRoomHomeSite(t *testing.T) {
	seeded := BuildThreadRooms()
	require.Len(t, seeded, 1, "one seeded thread room, in r-eng")

	tests := []struct {
		name    string
		row     model.ThreadRoom
		want    string
		because string
	}{
		{
			name: "seeded thread room follows its parent room", row: seeded[0],
			want: "site-local",
		},
		{
			// Synthetic because no seeded thread room lives in a remote room:
			// without this case the accessor could read tr.SiteID instead of
			// looking the RoomID up, and no fixture would tell the difference.
			name: "thread room in a remote room follows the room, not its own SiteID field",
			row:  model.ThreadRoom{RoomID: "r-remote-announce", SiteID: siteLocal},
			want: "site-remote", because: "RoomID decides, not the SiteID carried on the row",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, threadRoomHomeSite(tt.row), tt.because)
		})
	}
}

func TestRoomKeyHomeSite(t *testing.T) {
	tests := []struct {
		name   string
		roomID string
		want   string
	}{
		{name: "key for a local room", roomID: "r-general", want: "site-local"},
		{name: "key for a remote room", roomID: "r-remote-announce", want: "site-remote"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, roomKeyHomeSite(findRoomKey(t, tt.roomID)))
		})
	}
}

// --- Rule 2: subscriber-owned rows route by the SUBSCRIBER's home site. ---
//
// This is the invariant that makes the two-site dataset usable. Get it
// backwards and ivan's rows land in site-local's database, so his chat list
// renders empty with no error anywhere.

func TestSubscriptionHomeSite_RoutesBySubscriberNotRoom(t *testing.T) {
	tests := []struct {
		name    string
		account string
		roomID  string
		want    string
		because string
	}{
		{
			name: "local subscriber of a local room", account: "alice", roomID: "r-general",
			want: "site-local",
		},
		{
			name: "REMOTE subscriber of a LOCAL room follows the subscriber", account: "ivan", roomID: "r-general",
			want: "site-remote", because: "ivan is homed remotely, so his r-general row lives in site-remote's DB",
		},
		{
			name: "LOCAL subscriber of a REMOTE room follows the subscriber", account: "alice", roomID: "r-remote-announce",
			want: "site-local", because: "alice is homed locally, so her r-remote-announce row lives in site-local's DB",
		},
		{
			name: "remote subscriber of a remote room", account: "judy", roomID: "r-remote-announce",
			want: "site-remote",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub := findSubscription(t, tt.account, tt.roomID)

			assert.Equal(t, tt.want, subscriptionHomeSite(sub), tt.because)
			assert.Equal(t, findRoom(t, tt.roomID).SiteID, sub.SiteID,
				"the row always carries the ROOM's siteId, whichever DB it lives in")
		})
	}
}

func TestThreadSubscriptionHomeSite_RoutesBySubscriberNotRoom(t *testing.T) {
	seeded := BuildThreadSubscriptions()
	require.NotEmpty(t, seeded, "bob and carol are subscribed to the seeded thread")

	tests := []struct {
		name    string
		row     model.ThreadSubscription
		want    string
		because string
	}{
		{
			name: "seeded local subscriber", row: seeded[0],
			want: "site-local",
		},
		{
			// Synthetic: every seeded thread subscriber is local and the only
			// seeded thread is in a local room, so no fixture pair can tell
			// subscriber-routing from room-routing here.
			name: "REMOTE subscriber of a thread in a LOCAL room follows the subscriber",
			row:  model.ThreadSubscription{UserAccount: "ivan", RoomID: "r-eng", SiteID: siteLocal},
			want: "site-remote", because: "ivan is homed remotely; the thread's room and SiteID are both site-local",
		},
		{
			name: "LOCAL subscriber of a thread in a REMOTE room follows the subscriber",
			row:  model.ThreadSubscription{UserAccount: "alice", RoomID: "r-remote-announce", SiteID: siteRemote},
			want: "site-local", because: "alice is homed locally; the thread's room and SiteID are both site-remote",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, threadSubscriptionHomeSite(tt.row), tt.because)
		})
	}
}

func TestRestrictedCacheHomeSite_RoutesBySubscriberNotRoom(t *testing.T) {
	tests := []struct {
		name    string
		account string
		want    string
		because string
	}{
		{name: "local member of the restricted room", account: "alice", want: "site-local"},
		{
			name: "REMOTE member of the LOCAL restricted room follows the member", account: "ivan",
			want: "site-remote", because: "r-eng is homed at site-local, but ivan's cache entry lives in site-remote's Valkey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := findRestrictedCacheEntry(t, tt.account)

			assert.Equal(t, tt.want, restrictedCacheHomeSite(entry), tt.because)
			assert.Contains(t, entry.Rooms, "r-eng", "the entry still names the room it caches")
		})
	}
}

func TestFilterBySite(t *testing.T) {
	type row struct {
		id   string
		site string
	}
	homeSite := func(r row) string { return r.site }
	rows := []row{
		{id: "a", site: "site-local"},
		{id: "b", site: "site-remote"},
		{id: "c", site: "site-local"},
	}

	tests := []struct {
		name    string
		site    string
		wantIDs []string
	}{
		{name: "local site keeps only local rows", site: "site-local", wantIDs: []string{"a", "c"}},
		{name: "remote site keeps only remote rows", site: "site-remote", wantIDs: []string{"b"}},
		{name: "unknown site keeps nothing", site: "site-nope", wantIDs: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterBySite(rows, tt.site, homeSite)

			gotIDs := make([]string, 0, len(got))
			for _, r := range got {
				gotIDs = append(gotIDs, r.id)
			}
			assert.Equal(t, tt.wantIDs, gotIDs)
		})
	}
}

func TestFilterBySite_EmptyInput(t *testing.T) {
	got := filterBySite([]model.Room{}, "site-local", roomHomeSite)

	assert.Empty(t, got)
}

// The invariant that makes the two-site dataset usable, checked end-to-end
// through the production accessor the seeder actually passes to filterBySite:
// a subscription row lives in the DB of the SUBSCRIBER's home site, but keeps
// the ROOM's siteId. Routing these by the room instead would put ivan's rows
// in site-local's database and render an empty chat list for him.
func TestFilterBySite_SubscriptionsRouteBySubscriberNotRoom(t *testing.T) {
	all := BuildSubscriptions()
	remote := filterBySite(all, "site-remote", subscriptionHomeSite)

	require.NotEmpty(t, remote, "ivan and judy have subscriptions")
	for _, s := range remote {
		assert.Contains(t, []string{"ivan", "judy"}, s.User.Account)
	}

	// ivan is a member of r-general, which is homed at site-local. His row must
	// land in the remote set while still carrying the room's siteId.
	var ivanGeneral *model.Subscription
	for i := range remote {
		if remote[i].User.Account == "ivan" && remote[i].RoomID == "r-general" {
			ivanGeneral = &remote[i]
			break
		}
	}
	require.NotNil(t, ivanGeneral, "ivan's r-general subscription belongs in the remote set")
	assert.Equal(t, "site-local", ivanGeneral.SiteID,
		"row keeps the room's siteId even though it lives in the remote site's DB")

	// The mirror image: alice is homed locally but subscribes to the remote
	// room, so her row must NOT be in the remote set.
	local := filterBySite(all, "site-local", subscriptionHomeSite)
	var aliceAnnounce *model.Subscription
	for i := range local {
		if local[i].User.Account == "alice" && local[i].RoomID == "r-remote-announce" {
			aliceAnnounce = &local[i]
			break
		}
	}
	require.NotNil(t, aliceAnnounce, "alice's r-remote-announce subscription belongs in the local set")
	assert.Equal(t, "site-remote", aliceAnnounce.SiteID,
		"row keeps the room's siteId even though it lives in the local site's DB")
}

func TestFilterBySite_RoomsRouteByRoomHomeSite(t *testing.T) {
	roomSite := roomSiteByID()

	local := filterBySite(BuildRooms(), "site-local", roomHomeSite)
	remote := filterBySite(BuildRooms(), "site-remote", roomHomeSite)

	assert.Len(t, remote, 1, "only r-remote-announce is homed remotely")
	assert.Equal(t, "r-remote-announce", remote[0].ID)
	assert.Len(t, local, len(BuildRooms())-1)
	assert.Equal(t, "site-remote", roomSite["r-remote-announce"])
}

func TestFilterBySite_EverySubscriptionLandsAtExactlyOneSite(t *testing.T) {
	all := BuildSubscriptions()
	local := filterBySite(all, "site-local", subscriptionHomeSite)
	remote := filterBySite(all, "site-remote", subscriptionHomeSite)

	assert.Len(t, append(local, remote...), len(all),
		"no subscription is dropped or duplicated across the two sites")
}

func TestScopeToSite(t *testing.T) {
	type row struct {
		id   string
		site string
	}
	homeSite := func(r row) string { return r.site }
	rows := []row{
		{id: "a", site: "site-local"},
		{id: "b", site: "site-remote"},
		{id: "c", site: "site-local"},
	}

	tests := []struct {
		name    string
		site    string
		wantIDs []string
	}{
		{name: "empty site returns everything unfiltered", site: "", wantIDs: []string{"a", "b", "c"}},
		{name: "a real site filters to that site's rows", site: "site-local", wantIDs: []string{"a", "c"}},
		{name: "an unknown site returns nothing", site: "site-nope", wantIDs: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scopeToSite(rows, tt.site, homeSite)

			gotIDs := make([]string, 0, len(got))
			for _, r := range got {
				gotIDs = append(gotIDs, r.id)
			}
			assert.Equal(t, tt.wantIDs, gotIDs)
		})
	}
}

func TestValidateSite(t *testing.T) {
	tests := []struct {
		name    string
		site    string
		wantErr bool
	}{
		{name: "empty is unfiltered and valid", site: "", wantErr: false},
		{name: "site-local is valid", site: "site-local", wantErr: false},
		{name: "site-remote is valid", site: "site-remote", wantErr: false},
		{name: "unrecognised value is rejected", site: "site-typo", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSite(tt.site)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "site-typo")
				assert.Contains(t, err.Error(), siteLocal)
				assert.Contains(t, err.Error(), siteRemote)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestResolveTarget(t *testing.T) {
	tests := []struct {
		name     string
		envDB    string
		flagDB   string
		flagSite string
		wantDB   string
		wantSite string
	}{
		{
			name:   "defaults match the single-site stack",
			envDB:  "chat",
			wantDB: "chat", wantSite: "",
		},
		{
			name:  "flag overrides the env database",
			envDB: "chat", flagDB: "chat_remote", flagSite: "site-remote",
			wantDB: "chat_remote", wantSite: "site-remote",
		},
		{
			name:  "empty flag falls back to env",
			envDB: "chat_custom", flagDB: "", flagSite: "site-local",
			wantDB: "chat_custom", wantSite: "site-local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, site := resolveTarget(tt.envDB, tt.flagDB, tt.flagSite)

			assert.Equal(t, tt.wantDB, db)
			assert.Equal(t, tt.wantSite, site)
		})
	}
}
