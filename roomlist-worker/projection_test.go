package main

import (
	"testing"
	"time"

	"github.com/bytedance/sonic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// canonicalPayload is a MESSAGES-CANONICAL event carrying the fields this
// service does NOT read — attachments, sys-msg data, mentions — alongside the
// nine it does. Marshalled from the real model types so the wire shape is what
// the producers actually emit rather than a hand-written guess.
func canonicalPayload(t *testing.T) []byte {
	t.Helper()
	at := time.Date(2026, 8, 14, 9, 30, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: at.UnixMilli(),
		Message: model.Message{
			ID:                    "msg-1",
			RoomID:                "room-1",
			UserID:                "u-alice",
			UserAccount:           "alice",
			UserDisplayName:       "Alice A",
			Content:               "hey @bob and @carol",
			CreatedAt:             at,
			EditedAt:              &at,
			ThreadParentMessageID: "parent-1",
			TShow:                 true,
			Attachments:           [][]byte{[]byte(`{"name":"a.png"}`), []byte(`{"name":"b.pdf"}`)},
			SysMsgData:            []byte(`{"kind":"join"}`),
			Mentions:              []model.Participant{{UserID: "u-bob", Account: "bob"}},
		},
	}
	raw, err := sonic.Marshal(evt)
	require.NoError(t, err)
	return raw
}

// The narrow projection must decode the nine fields deriveIntents reads exactly
// as the full type does, off the same bytes. If the two ever disagree, this
// service writes room state derived from something other than what the rest of
// the pipeline saw.
func TestEventProjection_MatchesFullDecode(t *testing.T) {
	raw := canonicalPayload(t)

	var full model.MessageEvent
	require.NoError(t, sonic.Unmarshal(raw, &full))
	var got eventProjection
	require.NoError(t, sonic.Unmarshal(raw, &got))

	assert.Equal(t, full.Event, got.Event)
	assert.Equal(t, full.SiteID, got.SiteID)
	assert.Equal(t, full.Message.ID, got.Message.ID)
	assert.Equal(t, full.Message.RoomID, got.Message.RoomID)
	assert.Equal(t, full.Message.UserAccount, got.Message.UserAccount)
	assert.Equal(t, full.Message.Content, got.Message.Content)
	assert.True(t, full.Message.CreatedAt.Equal(got.Message.CreatedAt))
	require.NotNil(t, got.Message.EditedAt)
	assert.True(t, full.Message.EditedAt.Equal(*got.Message.EditedAt))
	assert.Equal(t, full.Message.ThreadParentMessageID, got.Message.ThreadParentMessageID)
	assert.Equal(t, full.Message.TShow, got.Message.TShow)
}

// Intents derived from a projection decoded off a real payload are the ones the
// service would have produced from the full event.
func TestEventProjection_DerivesTheSameIntents(t *testing.T) {
	at := time.Date(2026, 8, 14, 9, 30, 0, 0, time.UTC)
	var evt eventProjection
	require.NoError(t, sonic.Unmarshal(canonicalPayload(t), &evt))

	in := deriveIntents(&evt)

	assert.Equal(t, "room-1", in.RoomID)
	assert.Equal(t, "msg-1", in.LastMsgID)
	assert.True(t, at.Equal(in.LastMsgAt))
	assert.Equal(t, "alice", in.SenderAccount)
	assert.True(t, at.Equal(in.SenderSeenAt))
	assert.ElementsMatch(t, []string{"bob", "carol"}, in.MentionAccounts)
	assert.True(t, in.LastMentionAllAt.IsZero(), "no @all in the content")
}

// Most real events carry none of the optional fields; omitempty means they are
// simply absent from the wire.
func TestEventProjection_MinimalPayload(t *testing.T) {
	raw := []byte(`{"event":"created","siteId":"s","message":{"id":"m","roomId":"r","userAccount":"a","content":"hi","createdAt":"2026-08-14T09:30:00Z"}}`)
	var got eventProjection
	require.NoError(t, sonic.Unmarshal(raw, &got))
	assert.Equal(t, "r", got.Message.RoomID)
	assert.Equal(t, "m", got.Message.ID)
	assert.Nil(t, got.Message.EditedAt)
	assert.False(t, got.Message.TShow)
}
