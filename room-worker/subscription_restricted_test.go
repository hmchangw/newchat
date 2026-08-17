package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

// TestNewSub_InheritsRoomRestriction asserts a newly-created subscription carries
// the room's {restricted, externalAccess} denorm, so a member added to an
// already-restricted room is not omitted from subscription.list (#298).
func TestNewSub_InheritsRoomRestriction(t *testing.T) {
	ts := time.Now().UTC()
	user := &model.User{ID: "u1", Account: "alice"}

	restricted := &model.Room{ID: "r1", SiteID: "site-a", Type: model.RoomTypeChannel, Restricted: true, ExternalAccess: true}
	sub := newSub("s1", user, restricted, nil, "n", false, ts)
	assert.True(t, sub.Restricted, "sub must inherit room.Restricted")
	assert.True(t, sub.ExternalAccess, "sub must inherit room.ExternalAccess")

	open := &model.Room{ID: "r2", SiteID: "site-a", Type: model.RoomTypeChannel}
	openSub := newSub("s2", user, open, nil, "n", false, ts)
	assert.False(t, openSub.Restricted, "unrestricted room yields Restricted=false")
	assert.False(t, openSub.ExternalAccess, "unrestricted room yields ExternalAccess=false")
}
