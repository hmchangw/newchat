package model

import "testing"

func TestIsSystemMessageType(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"room_created", MessageTypeRoomCreated, true},
		{"members_added", MessageTypeMembersAdded, true},
		{"member_removed", MessageTypeMemberRemoved, true},
		{"member_left", MessageTypeMemberLeft, true},
		{"room_renamed", MessageTypeRoomRenamed, true},
		{"room_restricted", MessageTypeRoomRestricted, true},
		{"teams_meet_started", MessageTypeTeamsMeetStarted, true},
		{"empty is normal user message", "", false},
		{"literal message", "message", false},
		{"cassandra tombstone excluded", "message_removed", false},
		{"teams migration marker excluded", "teams_system", false},
		{"unknown", "call_ended", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsSystemMessageType(tc.in); got != tc.want {
				t.Fatalf("IsSystemMessageType(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
