package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

func ptrTime(t time.Time) *time.Time { return &t }

func TestDeriveIntents(t *testing.T) {
	created := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	edited := time.Date(2026, 8, 13, 11, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		evt  eventProjection
		want writeIntents
	}{
		{
			name: "created plain message updates room pointer and sender lastSeen",
			evt: eventProjection{
				Event: model.EventCreated,
				Message: messageProjection{
					ID: "m1", RoomID: "r1", UserAccount: "alice",
					Content: "hello", CreatedAt: created,
				},
			},
			want: writeIntents{
				RoomID: "r1", LastMsgID: "m1", LastMsgAt: created,
				SenderAccount: "alice", SenderSeenAt: created,
			},
		},
		{
			name: "created with mentions badges the mentioned accounts",
			evt: eventProjection{
				Event: model.EventCreated,
				Message: messageProjection{
					ID: "m2", RoomID: "r1", UserAccount: "alice",
					Content: "hi @bob and @carol", CreatedAt: created,
				},
			},
			want: writeIntents{
				RoomID: "r1", LastMsgID: "m2", LastMsgAt: created,
				SenderAccount: "alice", SenderSeenAt: created,
				MentionAccounts: []string{"bob", "carol"}, MentionAt: created,
			},
		},
		{
			name: "created with @all sets lastMentionAllAt and no mention accounts",
			evt: eventProjection{
				Event: model.EventCreated,
				Message: messageProjection{
					ID: "m3", RoomID: "r1", UserAccount: "alice",
					Content: "@all standup", CreatedAt: created,
				},
			},
			want: writeIntents{
				RoomID: "r1", LastMsgID: "m3", LastMsgAt: created,
				LastMentionAllAt: created,
				SenderAccount:    "alice", SenderSeenAt: created,
			},
		},
		{
			name: "hidden thread reply produces no writes",
			evt: eventProjection{
				Event: model.EventCreated,
				Message: messageProjection{
					ID: "m4", RoomID: "r1", UserAccount: "alice",
					Content: "@bob reply", CreatedAt: created,
					ThreadParentMessageID: "p1", TShow: false,
				},
			},
			want: writeIntents{},
		},
		{
			name: "visible thread reply is treated as a room message",
			evt: eventProjection{
				Event: model.EventCreated,
				Message: messageProjection{
					ID: "m5", RoomID: "r1", UserAccount: "alice",
					Content: "reply", CreatedAt: created,
					ThreadParentMessageID: "p1", TShow: true,
				},
			},
			want: writeIntents{
				RoomID: "r1", LastMsgID: "m5", LastMsgAt: created,
				SenderAccount: "alice", SenderSeenAt: created,
			},
		},
		{
			name: "updated badges mentions at editedAt and touches nothing else",
			evt: eventProjection{
				Event: model.EventUpdated,
				Message: messageProjection{
					ID: "m6", RoomID: "r1", UserAccount: "alice",
					Content: "now with @bob", CreatedAt: created, EditedAt: ptrTime(edited),
				},
			},
			want: writeIntents{
				RoomID: "r1", MentionAccounts: []string{"bob"}, MentionAt: edited,
			},
		},
		{
			name: "updated without mentions produces no writes",
			evt: eventProjection{
				Event: model.EventUpdated,
				Message: messageProjection{
					ID: "m7", RoomID: "r1", Content: "no mentions", EditedAt: ptrTime(edited),
				},
			},
			want: writeIntents{},
		},
		{
			name: "updated without editedAt produces no writes",
			evt: eventProjection{
				Event: model.EventUpdated,
				Message: messageProjection{
					ID: "m8", RoomID: "r1", Content: "@bob", EditedAt: nil,
				},
			},
			want: writeIntents{},
		},
		{
			name: "hidden thread reply edit produces no writes",
			evt: eventProjection{
				Event: model.EventUpdated,
				Message: messageProjection{
					ID: "m9", RoomID: "r1", Content: "@bob", EditedAt: ptrTime(edited),
					ThreadParentMessageID: "p1", TShow: false,
				},
			},
			want: writeIntents{},
		},
		{
			name: "deleted produces no writes",
			evt: eventProjection{
				Event:   model.EventDeleted,
				Message: messageProjection{ID: "m10", RoomID: "r1", Content: "@bob"},
			},
			want: writeIntents{},
		},
		{
			name: "reacted produces no writes",
			evt: eventProjection{
				Event:   model.EventReacted,
				Message: messageProjection{ID: "m11", RoomID: "r1"},
			},
			want: writeIntents{},
		},
		{
			name: "pinned produces no writes",
			evt: eventProjection{
				Event:   model.EventPinned,
				Message: messageProjection{ID: "m12", RoomID: "r1"},
			},
			want: writeIntents{},
		},
		{
			name: "missing roomId produces no writes",
			evt: eventProjection{
				Event:   model.EventCreated,
				Message: messageProjection{ID: "m13", RoomID: "", CreatedAt: created},
			},
			want: writeIntents{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveIntents(&tc.evt)
			assert.Equal(t, tc.want, got)
		})
	}
}

// #382 excludes system messages from unread counts and sidebar ordering. That
// classification is derived here, from the message type alone, so the write path
// can advance the room pointer without advancing the user position.
func TestDeriveIntents_FlagsSystemMessages(t *testing.T) {
	at := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		typ  string
		want bool
	}{
		{"a plain user message has no type", "", false},
		{"member_left is a system message", model.MessageTypeMemberLeft, true},
		{"room_renamed is a system message", model.MessageTypeRoomRenamed, true},
		// The one client-settable type: it previews and notifies like any user
		// message, so it must not be swept up by the "Type != \"\"" shorthand.
		{"important is client-set, not system", model.MessageTypeImportant, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := deriveIntents(&eventProjection{
				Event: model.EventCreated,
				Message: messageProjection{
					ID: "m1", RoomID: "r1", UserAccount: "alice", CreatedAt: at, Type: tc.typ,
				},
			})
			assert.Equal(t, tc.want, in.SystemMsg)
		})
	}
}
