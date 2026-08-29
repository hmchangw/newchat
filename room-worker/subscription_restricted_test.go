package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

// TestNewSub_InheritsRoomRestriction asserts a newly-created subscription carries
// the room's {restricted, externalAccess} denorm, so a member added to an
// already-restricted room is not omitted from subscription.list (#298). The
// asymmetric cases guard against a field-swap or copy-one-into-both regression.
func TestNewSub_InheritsRoomRestriction(t *testing.T) {
	ts := time.Now().UTC()
	user := &model.User{ID: "u1", Account: "alice"}

	for _, tc := range []struct {
		name                       string
		restricted, externalAccess bool
	}{
		{"both false", false, false},
		{"both true", true, true},
		{"restricted only", true, false},
		{"externalAccess only", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			room := &model.Room{ID: "r1", SiteID: "site-a", Type: model.RoomTypeChannel, Restricted: tc.restricted, ExternalAccess: tc.externalAccess}
			sub := newSub("s1", user, room, nil, room.Type, "n", false, ts)
			assert.Equal(t, tc.restricted, sub.Restricted, "sub.Restricted must equal room.Restricted")
			assert.Equal(t, tc.externalAccess, sub.ExternalAccess, "sub.ExternalAccess must equal room.ExternalAccess")
		})
	}
}
