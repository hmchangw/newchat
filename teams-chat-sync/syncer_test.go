package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

func TestStartOfDayUTC(t *testing.T) {
	tests := []struct {
		name string
		in   time.Time
		want time.Time
	}{
		{"mid-day utc", time.Date(2026, 7, 14, 13, 45, 6, 7, time.UTC), time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)},
		{"already midnight", time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)},
		{"non-utc zone normalizes to utc day", time.Date(2026, 7, 14, 1, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)), time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, startOfDayUTC(tc.in).Equal(tc.want))
		})
	}
}

func member(id string) msgraph.ChatMember { return msgraph.ChatMember{UserID: id} }

func TestVoteSiteID(t *testing.T) {
	cache := map[string]cachedUser{
		"a1": {siteID: "site-a", account: "alice"},
		"a2": {siteID: "site-a", account: "amy"},
		"b1": {siteID: "site-b", account: "bob"},
		"c1": {siteID: "site-c", account: "carl"},
	}
	tests := []struct {
		name          string
		members       []msgraph.ChatMember
		defaultSiteID string
		want          string
	}{
		{"clear majority", []msgraph.ChatMember{member("a1"), member("a2"), member("b1")}, "", "site-a"},
		{"tie breaks lexicographically", []msgraph.ChatMember{member("a1"), member("b1")}, "", "site-a"},
		{"tie c vs b picks b", []msgraph.ChatMember{member("c1"), member("b1")}, "", "site-b"},
		{"unknown members do not vote", []msgraph.ChatMember{member("ghost"), member("b1")}, "", "site-b"},
		{"all unknown yields empty", []msgraph.ChatMember{member("ghost")}, "", ""},
		{"no members yields empty", nil, "", ""},
		{"all unknown falls back to default", []msgraph.ChatMember{member("ghost")}, "site-default", "site-default"},
		{"no members falls back to default", nil, "site-default", "site-default"},
		{"default never overrides a real vote", []msgraph.ChatMember{member("b1")}, "site-default", "site-b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, voteSiteID(tc.members, cache, tc.defaultSiteID))
		})
	}
}

func TestBuildChat(t *testing.T) {
	cache := map[string]cachedUser{
		"a1": {siteID: "site-a", account: "alice"},
		"b1": {siteID: "site-b", account: "bob"},
	}
	gc := msgraph.Chat{
		ID: "19:g1", ChatType: "group", Topic: "Project X",
		CreatedDateTime:     time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
		LastUpdatedDateTime: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		Members: []msgraph.ChatMember{
			{UserID: "a1", VisibleHistoryStartDateTime: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
			{UserID: "ghost", VisibleHistoryStartDateTime: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
		},
	}
	buildNow := time.Date(2026, 7, 14, 9, 30, 0, 0, time.UTC)
	c := buildChat(gc, cache, buildNow, "")
	assert.Equal(t, "19:g1", c.ID)
	assert.Equal(t, "Project X", c.Name)
	assert.Equal(t, "group", c.ChatType)
	assert.Equal(t, "site-a", c.SiteID)
	assert.True(t, c.UpdatedAt.Equal(buildNow), "UpdatedAt stamped with now at build time")
	assert.True(t, c.NeedMemberSync)
	assert.Equal(t, []model.TeamsChatMember{
		{ID: "a1", Account: "alice", VisibleHistoryStartDateTime: gc.Members[0].VisibleHistoryStartDateTime},
		{ID: "ghost", Account: "", VisibleHistoryStartDateTime: gc.Members[1].VisibleHistoryStartDateTime},
	}, c.Members, "unknown members kept with empty account")
}

func TestBuildChat_OneOnOne(t *testing.T) {
	c := buildChat(msgraph.Chat{ID: "19:one1", ChatType: model.TeamsChatTypeOneOnOne, Topic: ""}, nil, time.Now(), "")
	assert.Equal(t, "", c.Name)
	assert.False(t, c.NeedMemberSync, "oneOnOne never needs member sync")
}

func TestBuildChat_UnknownMembersUseDefaultSiteID(t *testing.T) {
	gc := msgraph.Chat{ID: "19:g9", ChatType: "group", Members: []msgraph.ChatMember{{UserID: "ghost"}}}
	c := buildChat(gc, nil, time.Now(), "site-default")
	assert.Equal(t, "site-default", c.SiteID)
}

func TestSyncerClaim_FirstWinsConcurrently(t *testing.T) {
	s := newSyncer(nil, nil, nil, syncConfig{MaxWorkers: 1, Now: time.Now})
	const goroutines = 32
	var wins int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.claim("19:shared") {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, 1, wins, "exactly one goroutine claims a chat id")
	assert.False(t, s.claim("19:shared"))
	assert.True(t, s.claim("19:other"))
}
