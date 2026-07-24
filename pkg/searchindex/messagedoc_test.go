package searchindex_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchindex"
)

func TestNewMessageDoc(t *testing.T) {
	createdAt := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	f := searchindex.MessageFields{
		MessageID:   "msg1",
		RoomID:      "room1",
		SiteID:      "site-a",
		UserID:      "u1",
		UserAccount: "alice",
		Content:     "hello",
		CreatedAt:   createdAt,
		TShow:       true,
	}

	doc := searchindex.NewMessageDoc(f)

	assert.Equal(t, "msg1", doc.MessageID)
	assert.Equal(t, "room1", doc.RoomID)
	assert.Equal(t, "site-a", doc.SiteID)
	assert.Equal(t, "u1", doc.UserID)
	assert.Equal(t, "alice", doc.UserAccount)
	assert.False(t, doc.IsBot)
	assert.Equal(t, "hello", doc.Content)
	assert.True(t, doc.CreatedAt.Equal(createdAt))
	assert.True(t, doc.TShow)
	assert.Nil(t, doc.EditedAt)
	assert.Empty(t, doc.Attachments)
	assert.Nil(t, doc.Card)
}

func TestNewMessageDoc_IsBotFromAccountSuffix(t *testing.T) {
	doc := searchindex.NewMessageDoc(searchindex.MessageFields{UserAccount: "helper.bot"})
	assert.True(t, doc.IsBot)
}

func TestNewMessageDoc_AttachmentsDecodedAndTextJoined(t *testing.T) {
	blob1, _ := jsonMarshal(cassandra.Attachment{Title: "invoice.pdf", Description: "Q3 numbers"})
	blob2, _ := jsonMarshal(cassandra.Attachment{Title: "logo.png"})

	doc := searchindex.NewMessageDoc(searchindex.MessageFields{
		MessageID:   "msg1",
		Attachments: [][]byte{blob1, blob2},
	})

	assert.Len(t, doc.Attachments, 2)
	assert.Equal(t, "invoice.pdf Q3 numbers logo.png", doc.AttachmentText)
}

func TestNewMessageDoc_MalformedAttachmentBlobSkippedNotFatal(t *testing.T) {
	good, _ := jsonMarshal(cassandra.Attachment{Title: "ok.png"})
	doc := searchindex.NewMessageDoc(searchindex.MessageFields{
		Attachments: [][]byte{[]byte("not json"), good},
	})
	assert.Len(t, doc.Attachments, 1)
	assert.Equal(t, "ok.png", doc.AttachmentText)
}

func TestNewMessageDoc_CardPopulatesCardData(t *testing.T) {
	card := &cassandra.Card{Template: "t1", Data: []byte(`{"k":"v"}`)}
	doc := searchindex.NewMessageDoc(searchindex.MessageFields{Card: card})
	assert.Equal(t, card, doc.Card)
	assert.Equal(t, `{"k":"v"}`, doc.CardData)
}

func TestMessageIndexName(t *testing.T) {
	got := searchindex.MessageIndexName("messages-a-v2", time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, "messages-a-v2-2026-03", got)
}

func jsonMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
