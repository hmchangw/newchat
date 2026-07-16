package model_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestSystemMessageTypes_CoversAllSystemConstants(t *testing.T) {
	want := []string{
		model.MessageTypeRoomCreated,
		model.MessageTypeMembersAdded,
		model.MessageTypeMemberRemoved,
		model.MessageTypeMemberLeft,
		model.MessageTypeRoomRenamed,
		model.MessageTypeRoomRestricted,
		model.MessageTypeTeamsMeetStarted,
	}
	assert.Len(t, model.SystemMessageTypes, len(want),
		"SystemMessageTypes must enumerate exactly the system-message constants")
	for _, typ := range want {
		_, ok := model.SystemMessageTypes[typ]
		assert.True(t, ok, "system type %q missing from SystemMessageTypes", typ)
	}
}

func TestIsSystemMessageType(t *testing.T) {
	tests := []struct {
		name string
		typ  string
		want bool
	}{
		{"room_created is system", model.MessageTypeRoomCreated, true},
		{"members_added is system", model.MessageTypeMembersAdded, true},
		{"member_removed is system", model.MessageTypeMemberRemoved, true},
		{"member_left is system", model.MessageTypeMemberLeft, true},
		{"room_renamed is system", model.MessageTypeRoomRenamed, true},
		{"room_restricted is system", model.MessageTypeRoomRestricted, true},
		{"teams_meet_started is system", model.MessageTypeTeamsMeetStarted, true},
		{"empty type is a user message", "", false},
		{"message_removed placeholder is not a system type", "message_removed", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, model.IsSystemMessageType(tt.typ))
		})
	}
}

func TestTrimPreview(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty passthrough", "", ""},
		{"short passthrough", "hello", "hello"},
		{
			"exactly cap passthrough",
			strings.Repeat("a", model.LastMessagePreviewMaxRunes),
			strings.Repeat("a", model.LastMessagePreviewMaxRunes),
		},
		{
			"one over cap trimmed",
			strings.Repeat("a", model.LastMessagePreviewMaxRunes+1),
			strings.Repeat("a", model.LastMessagePreviewMaxRunes),
		},
		{
			"multibyte counted by runes not bytes",
			strings.Repeat("測", model.LastMessagePreviewMaxRunes+44),
			strings.Repeat("測", model.LastMessagePreviewMaxRunes),
		},
		{
			// 200 runes = 600 bytes: over the cap in bytes, under it in runes.
			"multibyte under cap by runes passthrough",
			strings.Repeat("測", 200),
			strings.Repeat("測", 200),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, model.TrimPreview(tt.in))
		})
	}
}

func TestLastMessagePointerJSON(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	p := model.LastMessagePointer{MessageID: "m-sys", CreatedAt: ts}
	data, err := json.Marshal(p)
	require.NoError(t, err)
	var dst model.LastMessagePointer
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, p, dst)
}

func TestLastRoomMessageResponse_PointerOmittedWhenNil(t *testing.T) {
	data, err := json.Marshal(model.LastRoomMessageResponse{})
	require.NoError(t, err)
	assert.NotContains(t, string(data), "pointer")
	assert.NotContains(t, string(data), "lastMessage")
}

func TestLastRoomMessageRequest_BeforeOmittedWhenZero(t *testing.T) {
	data, err := json.Marshal(model.LastRoomMessageRequest{RoomID: "r1"})
	require.NoError(t, err)
	assert.NotContains(t, string(data), "before")

	data, err = json.Marshal(model.LastRoomMessageRequest{RoomID: "r1", Before: 1751364000000})
	require.NoError(t, err)
	assert.Contains(t, string(data), `"before":1751364000000`)
}
