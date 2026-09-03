package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func updJSON(action, roomID, roomType string, crossSite *bool) []byte {
	room := ""
	if crossSite != nil {
		room = fmt.Sprintf(`,"room":{"crossSite":%v}`, *crossSite)
	}
	return []byte(fmt.Sprintf(
		`{"action":%q,"subscription":{"roomId":%q,"roomType":%q%s},"timestamp":1}`,
		action, roomID, roomType, room))
}

func TestApplySubscriptionUpdate(t *testing.T) {
	tr, fa := true, false
	cases := []struct {
		name     string
		plan     map[string]bool
		payload  []byte
		wantPlan map[string]bool
		want     []subChange
		// wantAsserted is the room the event vouches for — set even when the
		// event changes nothing, because it is what stops a stale walk from
		// reverting it.
		wantAsserted string
	}{
		{"added channel opens global", map[string]bool{},
			updJSON("added", "r1", "channel", &tr),
			map[string]bool{"r1": true},
			[]subChange{{Op: subOpen, RoomID: "r1", Global: true}}, "r1"},
		{"added channel missing crossSite fails safe to global", map[string]bool{},
			updJSON("added", "r1", "channel", nil),
			map[string]bool{"r1": true},
			[]subChange{{Op: subOpen, RoomID: "r1", Global: true}}, "r1"},
		{"added dm is user-lane only, no change", map[string]bool{},
			updJSON("added", "d1", "dm", nil),
			map[string]bool{}, nil, ""},
		{"removed closes and forgets", map[string]bool{"r1": true},
			updJSON("removed", "r1", "channel", nil),
			map[string]bool{},
			[]subChange{{Op: subClose, RoomID: "r1"}}, "r1"},
		// Deliberately NOT a no-op: a room whose subscribe failed is absent
		// from the plan view but present in missingRooms, and only a close
		// clears it. See TestSimClient_RemovalClearsAFailedRoomAndRestoresReady.
		{"removed room absent from the plan still closes", map[string]bool{},
			updJSON("removed", "rX", "channel", nil),
			map[string]bool{},
			[]subChange{{Op: subClose, RoomID: "rX"}}, "rX"},
		{"crossSite flip closes old namespace, opens new", map[string]bool{"r1": true},
			updJSON("added", "r1", "channel", &fa),
			map[string]bool{"r1": false},
			[]subChange{{Op: subClose, RoomID: "r1"}, {Op: subOpen, RoomID: "r1", Global: false}}, "r1"},
		// No change, but still an assertion about r1: the walk snapshot that
		// disagrees with it is the older fact.
		{"same namespace re-add is a no-op (never double-subscribed)", map[string]bool{"r1": true},
			updJSON("added", "r1", "channel", &tr),
			map[string]bool{"r1": true}, nil, "r1"},
		{"role_updated carrying a subscription does not touch subs", map[string]bool{"r1": true},
			updJSON("role_updated", "r1", "channel", &tr),
			map[string]bool{"r1": true}, nil, ""},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			changes, asserted, err := applySubscriptionUpdate(tt.plan, tt.payload)
			require.NoError(t, err)
			assert.Equal(t, tt.want, changes)
			assert.Equal(t, tt.wantPlan, tt.plan)
			assert.Equal(t, tt.wantAsserted, asserted)
		})
	}
}

func TestApplySubscriptionUpdate_Malformed(t *testing.T) {
	_, _, err := applySubscriptionUpdate(map[string]bool{}, []byte("{oops"))
	assert.Error(t, err)
}

func TestDiffPlans_Resync(t *testing.T) {
	oldPlan := map[string]bool{"r1": true, "r2": false, "r3": true}
	newPlan := map[string]bool{"r1": true, "r2": true, "r4": false}
	changes := diffPlans(oldPlan, newPlan)
	assert.ElementsMatch(t, []subChange{
		{Op: subClose, RoomID: "r2"}, {Op: subOpen, RoomID: "r2", Global: true}, // namespace flip
		{Op: subClose, RoomID: "r3"},               // vanished during the disconnect window
		{Op: subOpen, RoomID: "r4", Global: false}, // appeared during the disconnect window
	}, changes)
}

func TestDiffPlans_Identical(t *testing.T) {
	p := map[string]bool{"r1": true}
	assert.Empty(t, diffPlans(p, map[string]bool{"r1": true}))
}

func TestApplySubscriptionUpdate_RemovalClearsARoomThatNeverOpened(t *testing.T) {
	// A room whose subscribe failed is in missingRooms but NOT in the plan
	// view (which is derived from roomSubs). Treating its removal as
	// "unknown room, nothing to do" emitted no subClose, so nothing ever
	// deleted the missingRooms entry and the client stayed not-ready for the
	// rest of the run — for a room that no longer exists.
	plan := map[string]bool{}
	changes, _, err := applySubscriptionUpdate(plan, updJSON("removed", "gone", "channel", nil))
	require.NoError(t, err)
	require.Len(t, changes, 1, "a removal must still close, so missingRooms is cleared")
	assert.Equal(t, subClose, changes[0].Op)
	assert.Equal(t, "gone", changes[0].RoomID)
}

func TestApplySubscriptionUpdate_RemovalWithoutARoomIDIsMalformed(t *testing.T) {
	// Previously swallowed as a no-op. A removal that names no room may have
	// carried a real membership change, so it is evidence the caller records
	// (and a reason to re-derive the plan), not nothing happening.
	plan := map[string]bool{}
	changes, _, err := applySubscriptionUpdate(plan, updJSON("removed", "", "channel", nil))
	require.Error(t, err)
	assert.Empty(t, changes)
}

// A membership event naming no room is malformed, not a no-op: it may have
// carried a real change the client now cannot apply. Swallowing it left the
// run with no evidence and the plan silently possibly-stale. A DM `added`,
// by contrast, is a legitimate event this tool correctly ignores.
func TestApplySubscriptionUpdate_EmptyRoomIDIsMalformed(t *testing.T) {
	for _, action := range []string{"added", "removed"} {
		t.Run(action+" without a roomId", func(t *testing.T) {
			_, _, err := applySubscriptionUpdate(map[string]bool{}, updJSON(action, "", "channel", nil))
			require.Error(t, err)
			assert.ErrorContains(t, err, "roomId")
		})
	}
	t.Run("a DM add is a legitimate skip, not malformed", func(t *testing.T) {
		changes, asserted, err := applySubscriptionUpdate(map[string]bool{}, updJSON("added", "d1", "dm", nil))
		require.NoError(t, err)
		assert.Empty(t, changes)
		assert.Empty(t, asserted)
	})
}

// The same wildcard hazard on the live path: an update naming room "*" would
// open chat.room.*.event. It is malformed, so it invalidates the plan and
// forces a resync rather than being applied.
func TestApplySubscriptionUpdate_RejectsRoomIDsThatAreNotSubjectTokens(t *testing.T) {
	for _, bad := range []string{"*", ">", "a.b", "has space"} {
		t.Run("roomId "+bad, func(t *testing.T) {
			_, _, err := applySubscriptionUpdate(map[string]bool{}, updJSON("added", bad, "channel", nil))
			require.Error(t, err)
			assert.ErrorContains(t, err, "roomId")
		})
	}
}

// A missing roomType is not the same as "this is a DM". A DM or bot add is a
// legitimate skip — that traffic rides the user lane — but an event that names
// no type at all may be a channel add being dropped on the floor, which is a
// room the client then never subscribes to while reporting ready.
func TestApplySubscriptionUpdate_MissingRoomTypeIsMalformed(t *testing.T) {
	_, _, err := applySubscriptionUpdate(map[string]bool{},
		[]byte(`{"action":"added","subscription":{"roomId":"r1"},"timestamp":1}`))
	require.Error(t, err)
	assert.ErrorContains(t, err, "roomType")
}

// A named non-channel type stays a silent skip, including one this tool does
// not know — a new room type must not fail the run.
func TestApplySubscriptionUpdate_NamedNonChannelTypesAreSkipped(t *testing.T) {
	for _, rt := range []string{"dm", "botDM", "somethingNew"} {
		t.Run(rt, func(t *testing.T) {
			changes, asserted, err := applySubscriptionUpdate(map[string]bool{}, updJSON("added", "d1", rt, nil))
			require.NoError(t, err)
			assert.Empty(t, changes)
			assert.Empty(t, asserted)
		})
	}
}
