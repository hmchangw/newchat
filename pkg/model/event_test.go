package model

import (
	"encoding/json"
	"strings"
	"testing"
)

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

func TestPreviewMessageOmitempty(t *testing.T) {
	// previewMessage is *PreviewMessage with omitempty on every carrier: serialized when set,
	// omitted when nil (no eligible message / read error / thread path).
	pvw := &PreviewMessage{MessageID: "m1"}
	cases := []struct {
		name  string
		set   any
		unset any
	}{
		{"EditRoomEvent", &EditRoomEvent{PreviewMessage: pvw}, &EditRoomEvent{}},
		{"DeleteRoomEvent", &DeleteRoomEvent{PreviewMessage: pvw}, &DeleteRoomEvent{}},
		{"MessageEvent", &MessageEvent{PreviewMessage: pvw}, &MessageEvent{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.set)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if !strings.Contains(string(b), `"previewMessage":{"messageId":"m1"`) {
				t.Fatalf("preview not serialized: %s", b)
			}
			b, _ = json.Marshal(tc.unset)
			if strings.Contains(string(b), "previewMessage") {
				t.Fatalf("nil preview should be omitted: %s", b)
			}
		})
	}
}
