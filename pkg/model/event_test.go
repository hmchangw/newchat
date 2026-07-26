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

func TestEditRoomEvent_PreviewMessageRawJSON(t *testing.T) {
	// object case
	e := EditRoomEvent{Type: RoomEventMessageEdited, PreviewMessage: json.RawMessage(`{"messageId":"m1"}`)}
	b, err := json.Marshal(&e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"previewMessage":{"messageId":"m1"}`) {
		t.Fatalf("object preview not serialized: %s", b)
	}
	// null case (cleared)
	e.PreviewMessage = json.RawMessage("null")
	b, _ = json.Marshal(&e)
	if !strings.Contains(string(b), `"previewMessage":null`) {
		t.Fatalf("null preview not serialized: %s", b)
	}
	// absent case (thread path leaves it nil → omitempty drops it)
	e.PreviewMessage = nil
	b, _ = json.Marshal(&e)
	if strings.Contains(string(b), "previewMessage") {
		t.Fatalf("nil preview should be omitted: %s", b)
	}
}

func TestDeleteRoomEvent_PreviewMessageRawJSON(t *testing.T) {
	d := DeleteRoomEvent{Type: RoomEventMessageDeleted, PreviewMessage: json.RawMessage("null")}
	b, err := json.Marshal(&d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"previewMessage":null`) {
		t.Fatalf("null preview not serialized: %s", b)
	}
	d.PreviewMessage = nil
	b, _ = json.Marshal(&d)
	if strings.Contains(string(b), "previewMessage") {
		t.Fatalf("nil preview should be omitted: %s", b)
	}
}

func TestMessageEvent_PreviewMessageOmitempty(t *testing.T) {
	// nil preview omitted on the internal event
	e := MessageEvent{Event: EventUpdated}
	b, err := json.Marshal(&e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(b), "previewMessage") {
		t.Fatalf("nil preview should be omitted: %s", b)
	}
	// non-nil preview serialized
	e.PreviewMessage = &PreviewMessage{MessageID: "m1"}
	b, _ = json.Marshal(&e)
	if !strings.Contains(string(b), `"messageId":"m1"`) {
		t.Fatalf("preview not serialized: %s", b)
	}
}
