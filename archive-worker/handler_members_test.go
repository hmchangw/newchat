package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/model"
)

func inboxMsg(t *testing.T, typ string, inner *model.InboxMemberEvent, seq uint64) (*fakeMsg, []byte) {
	t.Helper()
	payload, err := json.Marshal(inner)
	require.NoError(t, err)
	outer := model.InboxEvent{Type: typ, SiteID: inner.SiteID, DestSiteID: "site-a", Payload: payload, Timestamp: 1759672800000}
	data, err := json.Marshal(outer)
	require.NoError(t, err)
	return &fakeMsg{subject: "chat.inbox.site-a.external." + typ, data: data, seq: seq, stream: "INBOX-site-a"}, data
}

func TestBuildMemberItem(t *testing.T) {
	c, err := auditarchive.NewCipher(testDEK())
	require.NoError(t, err)
	now := time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC)
	inner := model.InboxMemberEvent{RoomID: "r9", RoomName: "apac-sales", RoomType: model.RoomTypeChannel, SiteID: "site-b", Accounts: []string{"d.kwan", "e.wong"}, JoinedAt: 1759672800000, Timestamp: 1759672800000}

	t.Run("member_added fans out per account", func(t *testing.T) {
		msg, data := inboxMsg(t, model.InboxMemberAdded, &inner, 900)
		it, err := buildMemberItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		require.Len(t, it.docs, 2)
		ids := []string{it.docs[0].ID, it.docs[1].ID}
		assert.Equal(t, []string{"site-a-900-0", "site-a-900-1"}, ids)
		d := it.docs[1].Doc.(*auditarchive.MemberDoc)
		assert.Equal(t, "e.wong", d.Account)
		assert.Equal(t, "site-b", d.RoomSiteID, "room is archived at its own site")
		assert.Equal(t, "r9", d.RoomID)
		assert.Equal(t, "member_added", d.EventType)
		assert.Equal(t, "audit-members-site-a-2025.10.05", it.docs[1].Index)
		assert.NotEmpty(t, it.frame)
		assert.Equal(t, it.docs[0].Doc.(*auditarchive.MemberDoc).ContentHash, d.ContentHash, "one frame, one hash")
	})
	t.Run("room_renamed yields one doc without account", func(t *testing.T) {
		in := inner
		in.Accounts = nil
		msg, data := inboxMsg(t, model.InboxRoomRenamed, &in, 901)
		it, err := buildMemberItem(context.Background(), "site-a", msg, data, c, now)
		require.NoError(t, err)
		require.Len(t, it.docs, 1)
		assert.Empty(t, it.docs[0].Doc.(*auditarchive.MemberDoc).Account)
		assert.Equal(t, "apac-sales", it.docs[0].Doc.(*auditarchive.MemberDoc).RoomName)
	})
	t.Run("joinedat refresh is skipped", func(t *testing.T) {
		msg, data := inboxMsg(t, model.InboxMemberJoinedAtRefreshed, &inner, 902)
		_, err := buildMemberItem(context.Background(), "site-a", msg, data, c, now)
		assert.True(t, errors.Is(err, errSkip))
	})
	t.Run("member_added with no accounts is poison", func(t *testing.T) {
		in := inner
		in.Accounts = nil
		msg, data := inboxMsg(t, model.InboxMemberAdded, &in, 903)
		_, err := buildMemberItem(context.Background(), "site-a", msg, data, c, now)
		assert.True(t, errors.Is(err, errPoison))
	})
	t.Run("empty payload is poison", func(t *testing.T) {
		outer, _ := json.Marshal(model.InboxEvent{Type: model.InboxMemberAdded, Timestamp: 1})
		msg := &fakeMsg{subject: "x", data: outer, seq: 904, stream: "INBOX-site-a"}
		_, err := buildMemberItem(context.Background(), "site-a", msg, outer, c, now)
		assert.True(t, errors.Is(err, errPoison))
	})
}
