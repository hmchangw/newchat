package cassandra

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roundTrip marshals src to JSON, unmarshals into dst, asserts equality, and returns dst.
func roundTrip[T any](t *testing.T, src T) T {
	t.Helper()
	data, err := json.Marshal(src)
	require.NoError(t, err)
	var dst T
	require.NoError(t, json.Unmarshal(data, &dst))
	assert.Equal(t, src, dst)
	return dst
}

func TestParticipant_JSON(t *testing.T) {
	p := Participant{
		ID:          "u1",
		EngName:     "Alice Smith",
		CompanyName: "Acme Corp",
		AppID:       "app-1",
		AppName:     "MyApp",
		IsBot:       true,
		Account:     "alice",
	}
	roundTrip(t, p)
}

func TestParticipant_JSON_Minimal(t *testing.T) {
	p := Participant{ID: "u1", Account: "alice"}
	got := roundTrip(t, p)
	assert.Empty(t, got.EngName)
	assert.False(t, got.IsBot)
}

func TestCard_JSON(t *testing.T) {
	c := Card{Template: "approval", Data: []byte(`{"key":"value"}`)}
	roundTrip(t, c)
}

func TestCard_JSON_NilData(t *testing.T) {
	c := Card{Template: "simple"}
	roundTrip(t, c)
}

func TestCardAction_JSON(t *testing.T) {
	ca := CardAction{
		Verb:        "approve",
		Text:        "Approve",
		CardID:      "c1",
		DisplayText: "Click to approve",
		HideExecLog: true,
		CardTmID:    "tm1",
		Data:        []byte(`{"action":"yes"}`),
	}
	roundTrip(t, ca)
}

func TestCardAction_JSON_Minimal(t *testing.T) {
	ca := CardAction{Verb: "click"}
	got := roundTrip(t, ca)
	assert.Empty(t, got.Text)
	assert.Empty(t, got.CardID)
	assert.False(t, got.HideExecLog)
}

func TestQuotedParentMessage_JSON(t *testing.T) {
	threadParent := time.Date(2026, 1, 14, 9, 0, 0, 0, time.UTC)
	q := QuotedParentMessage{
		MessageID:             "m1",
		RoomID:                "r1",
		Sender:                Participant{ID: "u1", Account: "alice"},
		CreatedAt:             time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
		Msg:                   "original message",
		Mentions:              []Participant{{ID: "u2", Account: "bob"}},
		DecodedAttachments:    []Attachment{{ID: "f1", Title: "a.png", Type: "file"}},
		MessageLink:           "https://chat.example.com/r1/m1",
		ThreadParentID:        "thread-parent-uuid",
		ThreadParentCreatedAt: &threadParent,
	}
	got := roundTrip(t, q)
	assert.Equal(t, "thread-parent-uuid", got.ThreadParentID)
	require.NotNil(t, got.ThreadParentCreatedAt)
	assert.Equal(t, threadParent, *got.ThreadParentCreatedAt)
}

func TestQuotedParentMessage_JSON_Minimal(t *testing.T) {
	q := QuotedParentMessage{
		MessageID: "m1",
		RoomID:    "r1",
		Sender:    Participant{ID: "u1"},
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	got := roundTrip(t, q)
	assert.Empty(t, got.Msg)
	assert.Nil(t, got.Mentions)
	assert.Nil(t, got.Attachments)
	assert.Empty(t, got.MessageLink)
	assert.Empty(t, got.ThreadParentID)
	assert.Nil(t, got.ThreadParentCreatedAt)
}

func TestQuotedParentMessage_JSON_WithThreadParent(t *testing.T) {
	parentAt := time.Date(2026, 1, 10, 8, 0, 0, 0, time.UTC)
	q := QuotedParentMessage{
		MessageID:             "reply-1",
		RoomID:                "r1",
		Sender:                Participant{ID: "u1", Account: "alice"},
		CreatedAt:             time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
		Msg:                   "a thread reply",
		ThreadParentID:        "parent-msg-1",
		ThreadParentCreatedAt: &parentAt,
	}
	got := roundTrip(t, q)
	assert.Equal(t, "parent-msg-1", got.ThreadParentID)
	require.NotNil(t, got.ThreadParentCreatedAt)
	assert.Equal(t, parentAt, *got.ThreadParentCreatedAt)
}

func TestQuotedParentMessage_JSON_TShow(t *testing.T) {
	q := QuotedParentMessage{MessageID: "m1", RoomID: "r1", Sender: Participant{ID: "u1"}, CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), TShow: true}
	b, err := json.Marshal(q)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"tshow":true`)
	assert.True(t, roundTrip(t, q).TShow)

	q.TShow = false // omitempty ⇒ absent when false
	b, err = json.Marshal(q)
	require.NoError(t, err)
	assert.NotContains(t, string(b), "tshow")
	assert.False(t, roundTrip(t, q).TShow)
}

func TestMessage_JSON(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	edited := now.Add(5 * time.Minute)
	updated := now.Add(10 * time.Minute)
	threadParent := now.Add(-1 * time.Hour)

	msg := Message{
		RoomID:                "r1",
		CreatedAt:             now,
		MessageID:             "m1",
		Sender:                Participant{ID: "u1", Account: "alice", IsBot: false},
		Msg:                   "hello world",
		Mentions:              []Participant{{ID: "u3", Account: "charlie"}},
		DecodedAttachments:    []Attachment{{ID: "f1", Title: "a.png", Type: "file"}},
		Card:                  &Card{Template: "approval", Data: []byte(`{"k":"v"}`)},
		CardAction:            &CardAction{Verb: "approve", CardID: "c1"},
		TShow:                 true,
		ThreadParentID:        "m-parent",
		ThreadParentCreatedAt: &threadParent,
		QuotedParentMessage: &QuotedParentMessage{
			MessageID: "m-quoted", RoomID: "r1",
			Sender:    Participant{ID: "u5", Account: "eve"},
			CreatedAt: now.Add(-30 * time.Minute), Msg: "original",
		},
		VisibleTo:    "u1",
		Deleted:      false,
		Type:         "user_joined",
		SysMsgData:   []byte(`{"userId":"u3"}`),
		SiteID:       "site-remote",
		EditedAt:     &edited,
		UpdatedAt:    &updated,
		ThreadRoomID: "tr-1",
		PinnedAt:     &edited,
		PinnedBy:     &Participant{ID: "u9", Account: "pinner"},
	}

	got := roundTrip(t, msg)
	assert.Equal(t, "r1", got.RoomID)
	assert.Equal(t, "m1", got.MessageID)
	assert.Equal(t, "alice", got.Sender.Account)
	assert.Len(t, got.Mentions, 1)
	assert.Len(t, got.DecodedAttachments, 1)
	assert.Equal(t, "approval", got.Card.Template)
	assert.Equal(t, "approve", got.CardAction.Verb)
	assert.True(t, got.TShow)
	assert.Equal(t, "m-parent", got.ThreadParentID)
	assert.Equal(t, threadParent, *got.ThreadParentCreatedAt)
	require.NotNil(t, got.QuotedParentMessage)
	assert.Equal(t, "m-quoted", got.QuotedParentMessage.MessageID)
	assert.Equal(t, "u1", got.VisibleTo)
	assert.Equal(t, "user_joined", got.Type)
	assert.Equal(t, "site-remote", got.SiteID)
	assert.Equal(t, edited, *got.EditedAt)
	assert.Equal(t, updated, *got.UpdatedAt)
	assert.Equal(t, "tr-1", got.ThreadRoomID)
	require.NotNil(t, got.PinnedAt)
	assert.Equal(t, edited, *got.PinnedAt)
	require.NotNil(t, got.PinnedBy)
	assert.Equal(t, "u9", got.PinnedBy.ID)
	assert.Equal(t, "pinner", got.PinnedBy.Account)
}

func TestMessage_JSON_Minimal(t *testing.T) {
	msg := Message{
		RoomID:    "r1",
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		MessageID: "m1",
		Sender:    Participant{ID: "u1", Account: "alice"},
		Msg:       "hi",
	}
	got := roundTrip(t, msg)
	assert.Nil(t, got.Card)
	assert.Nil(t, got.CardAction)
	assert.Nil(t, got.Mentions)
	assert.Nil(t, got.Attachments)
	assert.Nil(t, got.Reactions)
	assert.Nil(t, got.EditedAt)
	assert.Nil(t, got.UpdatedAt)
	assert.Nil(t, got.ThreadParentCreatedAt)
	assert.Nil(t, got.QuotedParentMessage)
	assert.Empty(t, got.ThreadParentID)
	assert.False(t, got.TShow)
	assert.False(t, got.Deleted)
	assert.Empty(t, got.ThreadRoomID)
	assert.Nil(t, got.PinnedAt)
	assert.Nil(t, got.PinnedBy)
}

func TestMessage_RoundTripEncrypted(t *testing.T) {
	in := Message{
		RoomID:     "r1",
		MessageID:  "m1",
		EncPayload: []byte{0xDE, 0xAD, 0xBE, 0xEF},
		EncMeta:    &EncMeta{Nonce: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
	}
	roundTrip(t, in)
}

func TestEncMeta_JSON(t *testing.T) {
	in := EncMeta{Nonce: []byte{1, 2, 3}}
	roundTrip(t, in)
}
