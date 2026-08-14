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
	got := filterBySite([]model.Room{}, "site-local", func(r model.Room) string { return r.SiteID })

	assert.Empty(t, got)
}

// The invariant that makes the two-site dataset usable: a subscription row
// lives in the DB of the SUBSCRIBER's home site, but keeps the ROOM's siteId.
// Routing these by sub.SiteID instead would put ivan's rows in site-local's
// database and render an empty chat list for him.
func TestFilterBySite_SubscriptionsRouteBySubscriberNotRoom(t *testing.T) {
	userSite := userSiteByAccount()
	subscriberSite := func(s model.Subscription) string { return userSite[s.User.Account] }

	all := BuildSubscriptions()
	remote := filterBySite(all, "site-remote", subscriberSite)

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
}

func TestFilterBySite_RoomsRouteByRoomHomeSite(t *testing.T) {
	roomSite := roomSiteByID()

	local := filterBySite(BuildRooms(), "site-local", func(r model.Room) string { return r.SiteID })
	remote := filterBySite(BuildRooms(), "site-remote", func(r model.Room) string { return r.SiteID })

	assert.Len(t, remote, 1, "only r-remote-announce is homed remotely")
	assert.Equal(t, "r-remote-announce", remote[0].ID)
	assert.Len(t, local, len(BuildRooms())-1)
	assert.Equal(t, "site-remote", roomSite["r-remote-announce"])
}

func TestFilterBySite_EverySubscriptionLandsAtExactlyOneSite(t *testing.T) {
	userSite := userSiteByAccount()
	subscriberSite := func(s model.Subscription) string { return userSite[s.User.Account] }

	all := BuildSubscriptions()
	local := filterBySite(all, "site-local", subscriberSite)
	remote := filterBySite(all, "site-remote", subscriberSite)

	assert.Len(t, append(local, remote...), len(all),
		"no subscription is dropped or duplicated across the two sites")
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
			wantDB: "chat", wantSite: "site-local",
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
