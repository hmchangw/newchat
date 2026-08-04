package searchindex_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestNewSpotlightDoc(t *testing.T) {
	joinedAt := time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC)

	doc := searchindex.NewSpotlightDoc(searchindex.SpotlightFields{
		UserAccount: "alice",
		RoomID:      "room1",
		RoomName:    "general",
		RoomType:    "channel",
		SiteID:      "site-a",
		JoinedAt:    joinedAt,
	})

	assert.Equal(t, "alice", doc.UserAccount)
	assert.Equal(t, "room1", doc.RoomID)
	assert.Equal(t, "general", doc.RoomName)
	assert.Equal(t, "channel", doc.RoomType)
	assert.Equal(t, "site-a", doc.SiteID)
	assert.True(t, doc.JoinedAt.Equal(joinedAt))
}

func TestNewSpotlightDoc_ZeroJoinedAt(t *testing.T) {
	doc := searchindex.NewSpotlightDoc(searchindex.SpotlightFields{UserAccount: "alice", RoomID: "room1"})
	assert.True(t, doc.JoinedAt.IsZero())
}
