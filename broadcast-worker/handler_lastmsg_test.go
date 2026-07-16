package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomcrypto"
	"github.com/hmchangw/chat/pkg/subject"
)

// nopLastMsgFetcher is the stateless LastMessageFetcher passed to handlers
// whose test does not assert on the delete-preview fetch (nil preview, no error).
type nopLastMsgFetcher struct{}

func (nopLastMsgFetcher) FetchLastMessage(context.Context, string, time.Time) (*model.LastMessagePreview, *model.LastMessagePointer, error) {
	return nil, nil, nil
}

var defaultLastMsgFetcher = nopLastMsgFetcher{}

// ptrOf derives the pointer a preview implies — what the production fetcher
// does when the server reply carries no explicit pointer.
func ptrOf(p *model.LastMessagePreview) *model.LastMessagePointer {
	if p == nil {
		return nil
	}
	return &model.LastMessagePointer{MessageID: p.MessageID, CreatedAt: p.CreatedAt}
}

// stubLastMsgFetcher is a LastMessageFetcher test double returning a fixed
// preview/pointer (or error) and counting calls, so tests can assert whether
// the delete path fetched the room preview and what ceiling it passed.
// pointer nil derives from preview, mirroring the production fetcher.
type stubLastMsgFetcher struct {
	preview    *model.LastMessagePreview
	pointer    *model.LastMessagePointer
	err        error
	calls      int
	lastBefore time.Time
}

func (s *stubLastMsgFetcher) FetchLastMessage(_ context.Context, _ string, before time.Time) (*model.LastMessagePreview, *model.LastMessagePointer, error) {
	s.calls++
	s.lastBefore = before
	if s.err != nil {
		return nil, nil, s.err
	}
	ptr := s.pointer
	if ptr == nil {
		ptr = ptrOf(s.preview)
	}
	return s.preview, ptr, nil
}

func survivorPreview(at time.Time) *model.LastMessagePreview {
	return &model.LastMessagePreview{
		MessageID:       "m-prev",
		SenderAccount:   "bob",
		SenderName:      "Bob Chen",
		Msg:             "previous message",
		CreatedAt:       at,
		AttachmentCount: 1,
	}
}

func makeDeleteEvent(roomID string, deletedAt time.Time) []byte {
	evt := model.MessageEvent{
		Event:     model.EventDeleted,
		SiteID:    "site-a",
		Timestamp: deletedAt.UnixMilli(),
		Message: model.Message{
			ID:          "msg-1",
			RoomID:      roomID,
			UserID:      "u-alice",
			UserAccount: "alice",
			CreatedAt:   deletedAt.Add(-time.Hour),
			UpdatedAt:   &deletedAt,
		},
	}
	data, _ := json.Marshal(evt)
	return data
}

func TestHandleDeleted_EmbedsLastMessagePreview(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	survivor := survivorPreview(deletedAt.Add(-2 * time.Hour))
	fetcher := &stubLastMsgFetcher{preview: survivor}

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", ptrOf(survivor), survivor, deletedAt).Return(nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt)))

	assert.Equal(t, 1, fetcher.calls, "delete must fetch the surviving preview exactly once")
	require.Len(t, pub.records, 1)
	assert.Equal(t, subject.RoomEvent("r1"), pub.records[0].subject)
	var roomEvt model.DeleteRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	assert.Equal(t, model.RoomEventMessageDeleted, roomEvt.Type)
	require.NotNil(t, roomEvt.LastMessage)
	assert.Equal(t, "m-prev", roomEvt.LastMessage.MessageID)
	assert.Equal(t, "bob", roomEvt.LastMessage.SenderAccount)
	assert.Equal(t, "previous message", roomEvt.LastMessage.Msg)
	assert.Equal(t, 1, roomEvt.LastMessage.AttachmentCount)
	assert.Empty(t, roomEvt.LastMessage.EncMsg, "plaintext room preview must not carry a ciphertext")
}

func TestHandleDeleted_NilPreview_NoPreviewFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	fetcher := &stubLastMsgFetcher{} // nil preview, nil error — valid "no survivor" signal

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", nil, nil, deletedAt).Return(nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt)))

	require.Len(t, pub.records, 1)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(pub.records[0].data, &raw))
	_, hasPlain := raw["lastMessage"]
	assert.False(t, hasPlain, "nil preview must omit lastMessage")
}

func TestHandleDeleted_FetcherError_ReturnsError_NoPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	fetcher := &stubLastMsgFetcher{err: errors.New("history down")}

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	// No RewindRoomLastMessage expectation: the fetch fails first.

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	err := h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "history down")
	assert.Empty(t, pub.records, "a failed preview fetch must Nak before any client saw the delete")
}

func TestHandleDeleted_RewindError_ReturnsError_NoPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	survivor := survivorPreview(deletedAt.Add(-2 * time.Hour))
	fetcher := &stubLastMsgFetcher{preview: survivor}

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", ptrOf(survivor), survivor, deletedAt).
		Return(errors.New("mongo down"))

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	err := h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mongo down")
	assert.Empty(t, pub.records, "a failed rewind must Nak before any client saw the delete")
}

func TestHandleThreadDeleted_HiddenReply_DoesNotFetchPreview(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	deletedAt := msgTime.Add(time.Minute)
	fetcher := &stubLastMsgFetcher{preview: survivorPreview(msgTime)}

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().GetThreadFollowers(gomock.Any(), "parent-1").Return(map[string]struct{}{"bob": {}}, nil)

	evt := model.MessageEvent{
		Event:     model.EventDeleted,
		SiteID:    "site-a",
		Timestamp: deletedAt.UnixMilli(),
		Message: model.Message{
			ID:                    "reply-1",
			RoomID:                "r1",
			UserAccount:           "alice",
			CreatedAt:             msgTime,
			UpdatedAt:             &deletedAt,
			ThreadParentMessageID: "parent-1",
			TShow:                 false,
		},
	}
	data, _ := json.Marshal(evt)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	assert.Equal(t, 0, fetcher.calls, "hidden thread-reply deletes cannot change the room preview")
	require.NotEmpty(t, pub.records)
	for _, r := range pub.records {
		var raw map[string]any
		require.NoError(t, json.Unmarshal(r.data, &raw))
		_, hasPlain := raw["lastMessage"]
		assert.False(t, hasPlain, "thread delete events must not carry lastMessage")
	}
}

func TestHandleDeleted_DMRoom_PreviewInEachMemberEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	survivor := survivorPreview(deletedAt.Add(-time.Hour))
	fetcher := &stubLastMsgFetcher{preview: survivor}

	room := &model.Room{
		ID:       "dm-alice-bob",
		Type:     model.RoomTypeDM,
		SiteID:   "site-a",
		Accounts: []string{"alice", "bob"},
	}
	store.EXPECT().GetRoom(gomock.Any(), "dm-alice-bob").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "dm-alice-bob", "msg-1", ptrOf(survivor), survivor, deletedAt).Return(nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("dm-alice-bob", deletedAt)))

	require.Len(t, pub.records, 2, "per-member DM fan-out")
	subjects := map[string]bool{}
	for _, r := range pub.records {
		subjects[r.subject] = true
		var roomEvt model.DeleteRoomEvent
		require.NoError(t, json.Unmarshal(r.data, &roomEvt))
		require.NotNil(t, roomEvt.LastMessage, "every DM member event must carry the preview")
		assert.Equal(t, "m-prev", roomEvt.LastMessage.MessageID)
	}
	assert.True(t, subjects[subject.UserRoomEvent("alice")])
	assert.True(t, subjects[subject.UserRoomEvent("bob")])
}

func TestHandleDeleted_EncryptedChannel_EncryptsPreview(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	survivor := survivorPreview(deletedAt.Add(-2 * time.Hour))
	fetcher := &stubLastMsgFetcher{preview: survivor}
	key := testRoomKey(t)

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	keyStore.EXPECT().Get(gomock.Any(), "r1").Return(key, nil)

	// Mongo must never see plaintext content for an encrypted room: the stored
	// preview carries EncMsg (content ciphertext) with Msg blanked.
	var stored *model.LastMessagePreview
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", gomock.Any(), gomock.Any(), deletedAt).
		DoAndReturn(func(_ context.Context, _, _ string, _ *model.LastMessagePointer, p *model.LastMessagePreview, _ time.Time) error {
			stored = p
			return nil
		})

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt)))

	require.NotNil(t, stored)
	assert.Empty(t, stored.Msg, "encrypted room: plaintext content must never reach Mongo")
	require.NotEmpty(t, stored.EncMsg)
	assert.Equal(t, "m-prev", stored.MessageID, "metadata stays plaintext")
	assert.Equal(t, "bob", stored.SenderAccount)
	assert.Equal(t, 1, stored.AttachmentCount)

	require.Len(t, pub.records, 1)
	var roomEvt model.DeleteRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	require.NotNil(t, roomEvt.LastMessage, "encrypted rooms still carry lastMessage, with encMsg instead of msg")
	assert.Empty(t, roomEvt.LastMessage.Msg)
	require.NotEmpty(t, roomEvt.LastMessage.EncMsg)
	assert.JSONEq(t, string(stored.EncMsg), string(roomEvt.LastMessage.EncMsg),
		"one ciphertext, two destinations: Mongo and the event share the same envelope")

	var env roomcrypto.EncryptedMessage
	require.NoError(t, json.Unmarshal(roomEvt.LastMessage.EncMsg, &env))
	assert.Equal(t, key.Version, env.Version)
	plaintext, err := decryptForTest(&env, key.KeyPair.PrivateKey)
	require.NoError(t, err)
	assert.Equal(t, "previous message", plaintext, "encMsg is the content-only envelope")
}

func TestHandleDeleted_EncryptedChannel_AttachmentOnlySurvivor_NoEncrypt(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	survivor := survivorPreview(deletedAt.Add(-2 * time.Hour))
	survivor.Msg = "" // attachment-only message: nothing to encrypt
	fetcher := &stubLastMsgFetcher{preview: survivor}

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	// No keyStore expectation: empty content must not trigger a key fetch.
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", ptrOf(survivor), survivor, deletedAt).Return(nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt)))

	require.Len(t, pub.records, 1)
	var roomEvt model.DeleteRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	require.NotNil(t, roomEvt.LastMessage)
	assert.Empty(t, roomEvt.LastMessage.Msg)
	assert.Empty(t, roomEvt.LastMessage.EncMsg, "attachment-only survivor keeps Msg==\"\" and EncMsg==nil")
	assert.Equal(t, 1, roomEvt.LastMessage.AttachmentCount)
}

func TestHandleDeleted_EncryptedChannel_KeyStoreError_NoRewind_NoPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	fetcher := &stubLastMsgFetcher{preview: survivorPreview(deletedAt.Add(-2 * time.Hour))}

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	keyStore.EXPECT().Get(gomock.Any(), "r1").Return(nil, errors.New("valkey down"))
	// No RewindRoomLastMessage expectation: encryption fails first.

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, true)
	err := h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "valkey down")
	assert.Empty(t, pub.records, "a failed preview encryption must Nak before any client saw the delete")
}

func TestHandleDeleted_EncryptedDM_StaysPlaintext(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	survivor := survivorPreview(deletedAt.Add(-time.Hour))
	fetcher := &stubLastMsgFetcher{preview: survivor}

	room := &model.Room{
		ID:       "dm-alice-bob",
		Type:     model.RoomTypeDM,
		SiteID:   "site-a",
		Accounts: []string{"alice", "bob"},
	}
	store.EXPECT().GetRoom(gomock.Any(), "dm-alice-bob").Return(room, nil)
	// No keyStore expectation: DM rooms are never encrypted, even with the flag on.
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "dm-alice-bob", "msg-1", ptrOf(survivor), survivor, deletedAt).Return(nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("dm-alice-bob", deletedAt)))

	require.Len(t, pub.records, 2, "per-member DM fan-out")
	for _, r := range pub.records {
		var roomEvt model.DeleteRoomEvent
		require.NoError(t, json.Unmarshal(r.data, &roomEvt))
		require.NotNil(t, roomEvt.LastMessage)
		assert.Equal(t, "previous message", roomEvt.LastMessage.Msg)
		assert.Empty(t, roomEvt.LastMessage.EncMsg)
	}
}

func TestHandleDeleted_EncryptedChannel_NilPreview_SkipsKeyFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	fetcher := &stubLastMsgFetcher{} // nil preview

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", nil, nil, deletedAt).Return(nil)
	// No keyStore expectation: nothing to encrypt.

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt)))

	require.Len(t, pub.records, 1)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(pub.records[0].data, &raw))
	_, hasPlain := raw["lastMessage"]
	assert.False(t, hasPlain)
}

func TestHandleUpdated_PatchesRoomLastMsgPreview(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	edited := time.Date(2026, 7, 1, 12, 5, 0, 0, time.UTC)
	store.EXPECT().SetRoomLastMessageEdited(gomock.Any(), roomID, "msg-1", "updated content", json.RawMessage(nil), edited).Return(nil)

	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			Content:   "updated content",
			CreatedAt: edited.Add(-time.Hour),
			EditedAt:  &edited, UpdatedAt: &edited,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))
	require.Len(t, pub.records, 1)
}

func TestHandleUpdated_EncryptedChannel_PatchesEncMsg(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	key := testRoomKey(t)
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)
	keyStore.EXPECT().Get(gomock.Any(), roomID).Return(key, nil)

	edited := time.Date(2026, 7, 1, 12, 5, 0, 0, time.UTC)
	// Encrypted room: the store patch carries the content ciphertext and a
	// blanked plaintext — never plaintext at rest.
	var storedEnc json.RawMessage
	store.EXPECT().SetRoomLastMessageEdited(gomock.Any(), roomID, "msg-1", "", gomock.Any(), edited).
		DoAndReturn(func(_ context.Context, _, _, newMsg string, encMsg json.RawMessage, _ time.Time) error {
			storedEnc = encMsg
			return nil
		})

	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			Content:   "secret edit",
			CreatedAt: edited.Add(-time.Hour),
			EditedAt:  &edited, UpdatedAt: &edited,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.NotEmpty(t, storedEnc)
	var env roomcrypto.EncryptedMessage
	require.NoError(t, json.Unmarshal(storedEnc, &env))
	assert.Equal(t, key.Version, env.Version)
	plaintext, err := decryptForTest(&env, key.KeyPair.PrivateKey)
	require.NoError(t, err)
	assert.Equal(t, "secret edit", plaintext)

	require.Len(t, pub.records, 1)
	var roomEvt model.EditRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	assert.Empty(t, roomEvt.NewContent)
	require.NotEmpty(t, roomEvt.EncryptedNewContent)
	assert.Equal(t, string(storedEnc), string(roomEvt.EncryptedNewContent),
		"the ciphertext is computed once and reused for both the store patch and the event")
}

func TestHandleUpdated_EncryptedChannel_KeyStoreError_NoPatch_NoPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)
	keyStore.EXPECT().Get(gomock.Any(), roomID).Return(nil, errors.New("valkey down"))
	// No SetRoomLastMessageEdited expectation: encryption fails first.

	edited := time.Date(2026, 7, 1, 12, 5, 0, 0, time.UTC)
	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			Content:   "secret edit",
			CreatedAt: edited.Add(-time.Hour),
			EditedAt:  &edited, UpdatedAt: &edited,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)
	err = h.HandleMessage(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "valkey down")
	assert.Empty(t, pub.records, "a failed edit encryption must Nak before any client saw the edit")
}

func TestHandleUpdated_PreviewPatchError_ReturnsError_NoPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	edited := time.Date(2026, 7, 1, 12, 5, 0, 0, time.UTC)
	store.EXPECT().SetRoomLastMessageEdited(gomock.Any(), roomID, "msg-1", "updated content", json.RawMessage(nil), edited).
		Return(errors.New("mongo down"))

	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			Content:   "updated content",
			CreatedAt: edited.Add(-time.Hour),
			EditedAt:  &edited, UpdatedAt: &edited,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, false)
	err = h.HandleMessage(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mongo down")
	assert.Empty(t, pub.records, "a failed preview patch must Nak before any client saw the edit")
}

func TestHandleCreated_PassesPreviewToStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	senderUser := model.User{ID: "u-sender", Account: "sender", EngName: "Sender Lin", ChineseName: "寄件者", SiteID: "site-a"}

	var gotPreview *model.LastMessagePreview
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ time.Time, _ bool, p *model.LastMessagePreview) error {
			gotPreview = p
			return nil
		})
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return([]model.User{senderUser}, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), makeMessageEvent("room-1", "hello", msgTime)))

	require.NotNil(t, gotPreview, "created path must persist a lastMsg preview")
	assert.Equal(t, "msg-1", gotPreview.MessageID)
	assert.Equal(t, "sender", gotPreview.SenderAccount)
	assert.Equal(t, "Sender Lin", gotPreview.SenderName)
	assert.Equal(t, "hello", gotPreview.Msg)
	assert.Empty(t, gotPreview.EncMsg, "plaintext room preview must not carry a ciphertext")
	assert.True(t, gotPreview.CreatedAt.Equal(msgTime))
	assert.Zero(t, gotPreview.AttachmentCount)
}

func TestHandleCreated_EncryptedChannel_PreviewCarriesEncMsg(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	key := testRoomKey(t)
	// Two fetches: one for the stored preview, one for the published room event
	// (the production keyStore is the LRU-cached wrapper, so the second is cheap).
	keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(key, nil).Times(2)

	var gotPreview *model.LastMessagePreview
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ time.Time, _ bool, p *model.LastMessagePreview) error {
			gotPreview = p
			return nil
		})
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), makeMessageEvent("room-1", "secret body", msgTime)))

	require.NotNil(t, gotPreview)
	assert.Empty(t, gotPreview.Msg, "encrypted room: lastMsg plaintext content never lands in Mongo")
	require.NotEmpty(t, gotPreview.EncMsg)
	assert.Equal(t, "msg-1", gotPreview.MessageID, "metadata stays plaintext")
	assert.Equal(t, "sender", gotPreview.SenderAccount)

	var env roomcrypto.EncryptedMessage
	require.NoError(t, json.Unmarshal(gotPreview.EncMsg, &env))
	assert.Equal(t, key.Version, env.Version)
	plaintext, err := decryptForTest(&env, key.KeyPair.PrivateKey)
	require.NoError(t, err)
	assert.Equal(t, "secret body", plaintext, "encMsg is the content-only envelope")
}

func TestHandleCreated_EncryptedChannel_EmptyContent_NoEncMsg(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	key := testRoomKey(t)
	// Exactly one fetch — the room-event encryption. The empty preview content
	// must not trigger a preview key fetch.
	keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(key, nil)

	var gotPreview *model.LastMessagePreview
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ time.Time, _ bool, p *model.LastMessagePreview) error {
			gotPreview = p
			return nil
		})
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), makeMessageEvent("room-1", "", msgTime)))

	require.NotNil(t, gotPreview)
	assert.Empty(t, gotPreview.Msg)
	assert.Empty(t, gotPreview.EncMsg, "attachment-only messages keep Msg==\"\" and EncMsg==nil")
}

func TestHandleCreated_EncryptedChannel_KeyStoreError_NoStoreWrite_NoPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(nil, errors.New("valkey down"))

	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)
	// No UpdateRoomLastMessage / AdvanceSubscriptionLastSeen expectations:
	// preview encryption fails before any store write.

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)
	err := h.HandleMessage(context.Background(), makeMessageEvent("room-1", "secret body", msgTime))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "valkey down")
	assert.Empty(t, pub.records, "a failed preview encryption must Nak before any client saw the message")
}

func TestHandleCreated_EncryptedDM_PreviewStaysPlaintext(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	// No keyStore expectation: DM rooms are never encrypted, even with the flag on.

	var gotPreview *model.LastMessagePreview
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "dm-1", "msg-1", msgTime, false, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ time.Time, _ bool, p *model.LastMessagePreview) error {
			gotPreview = p
			return nil
		})
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "dm-1", "sender", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "dm-1").Return(metaOf(testDMRoom), nil)
	store.EXPECT().ListSubscriptions(gomock.Any(), "dm-1").Return(testDMSubs, nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), makeMessageEvent("dm-1", "dm hello", msgTime)))

	require.NotNil(t, gotPreview)
	assert.Equal(t, "dm hello", gotPreview.Msg, "DM previews stay plaintext even with the encryption flag on")
	assert.Empty(t, gotPreview.EncMsg)
}

func TestPreviewSenderName_Fallbacks(t *testing.T) {
	tests := []struct {
		name string
		cm   *model.ClientMessage
		want string
	}{
		{
			name: "eng name wins",
			cm: &model.ClientMessage{
				Message: model.Message{UserAccount: "alice", UserDisplayName: "Alice W."},
				Sender:  &model.Participant{Account: "alice", EngName: "Alice Wang"},
			},
			want: "Alice Wang",
		},
		{
			name: "display name when eng name empty",
			cm: &model.ClientMessage{
				Message: model.Message{UserAccount: "alice", UserDisplayName: "Alice W."},
				Sender:  &model.Participant{Account: "alice"},
			},
			want: "Alice W.",
		},
		{
			name: "account when both empty",
			cm: &model.ClientMessage{
				Message: model.Message{UserAccount: "alice"},
				Sender:  &model.Participant{Account: "alice"},
			},
			want: "alice",
		},
		{
			name: "nil sender falls through to display name",
			cm: &model.ClientMessage{
				Message: model.Message{UserAccount: "alice", UserDisplayName: "Alice W."},
			},
			want: "Alice W.",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, previewSenderName(tc.cm))
		})
	}
}

func TestBuildLastMessagePreview_CountsAttachments(t *testing.T) {
	msgTime := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	cm := &model.ClientMessage{
		Message: model.Message{
			ID: "m1", UserAccount: "alice", Content: "with files",
			CreatedAt: msgTime, Type: "file",
		},
		Sender:      &model.Participant{Account: "alice", EngName: "Alice Wang"},
		Attachments: []model.Attachment{{}, {}},
	}

	p := buildLastMessagePreview(cm)
	assert.Equal(t, "m1", p.MessageID)
	assert.Equal(t, "file", p.Type)
	assert.Equal(t, "with files", p.Msg)
	assert.Empty(t, p.EncMsg, "buildLastMessagePreview always builds the plaintext form")
	assert.Equal(t, 2, p.AttachmentCount)
}

// System messages advance lastMsgAt/lastMsgId (room sorting) but must never
// become the stored rooms.lastMsg preview — the field's contract is "newest
// non-system message" and the docs promise system messages never appear.
func TestHandleCreated_SystemMessage_NoPreviewStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false, nil).Return(nil)
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
			Type:    model.MessageTypeMembersAdded,
			Content: "sender added bob", CreatedAt: msgTime,
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))
	require.Len(t, pub.records, 1, "the system message itself still broadcasts")
}

// Encrypted channel + system message: no preview means no preview key fetch —
// exactly one Get for the room-event encryption.
func TestHandleCreated_SystemMessage_EncryptedChannel_NoPreviewKeyFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	key := testRoomKey(t)
	keyStore.EXPECT().Get(gomock.Any(), "room-1").Return(key, nil)

	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false, nil).Return(nil)
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

	evt := model.MessageEvent{
		Event:     model.EventCreated,
		SiteID:    "site-a",
		Timestamp: msgTime.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: "room-1", UserID: "user-1", UserAccount: "sender",
			Type:    model.MessageTypeRoomRenamed,
			Content: "renamed to General", CreatedAt: msgTime,
		},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), data))
}

func TestHandleCreated_PreviewContentTrimmedToRuneCap(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	msgTime := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	long := strings.Repeat("測", model.LastMessagePreviewMaxRunes+44)

	var gotPreview *model.LastMessagePreview
	store.EXPECT().UpdateRoomLastMessage(gomock.Any(), "room-1", "msg-1", msgTime, false, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ time.Time, _ bool, p *model.LastMessagePreview) error {
			gotPreview = p
			return nil
		})
	store.EXPECT().AdvanceSubscriptionLastSeen(gomock.Any(), "room-1", "sender", msgTime).Return(nil)
	store.EXPECT().GetRoomMeta(gomock.Any(), "room-1").Return(metaOf(testChannelRoom), nil)
	us.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"sender"}).Return(nil, nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), makeMessageEvent("room-1", long, msgTime)))

	require.NotNil(t, gotPreview)
	assert.Equal(t, strings.Repeat("測", model.LastMessagePreviewMaxRunes), gotPreview.Msg,
		"stored preview is a snippet, not the full body")

	require.Len(t, pub.records, 1)
	var roomEvt model.RoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	require.NotNil(t, roomEvt.Message)
	assert.Equal(t, long, roomEvt.Message.Content, "the room event keeps the FULL content — only the preview is trimmed")
}

func TestHandleUpdated_LongEdit_PatchTrimmed_EventKeepsFullContent(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)

	long := strings.Repeat("y", model.LastMessagePreviewMaxRunes+50)
	edited := time.Date(2026, 7, 1, 12, 5, 0, 0, time.UTC)
	store.EXPECT().SetRoomLastMessageEdited(gomock.Any(), roomID, "msg-1",
		strings.Repeat("y", model.LastMessagePreviewMaxRunes), json.RawMessage(nil), edited).Return(nil)

	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			Content:   long,
			CreatedAt: edited.Add(-time.Hour),
			EditedAt:  &edited, UpdatedAt: &edited,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1)
	var roomEvt model.EditRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	assert.Equal(t, long, roomEvt.NewContent, "clients replace the message body — never a snippet")
}

// Long encrypted edit: the event ciphertext seals the FULL content, the store
// patch seals the trimmed snippet — two distinct envelopes.
func TestHandleUpdated_EncryptedChannel_LongEdit_DistinctPreviewCiphertext(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	key := testRoomKey(t)
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)
	keyStore.EXPECT().Get(gomock.Any(), roomID).Return(key, nil).Times(2)

	long := strings.Repeat("z", model.LastMessagePreviewMaxRunes+50)
	edited := time.Date(2026, 7, 1, 12, 5, 0, 0, time.UTC)
	var storedEnc json.RawMessage
	store.EXPECT().SetRoomLastMessageEdited(gomock.Any(), roomID, "msg-1", "", gomock.Any(), edited).
		DoAndReturn(func(_ context.Context, _, _, _ string, encMsg json.RawMessage, _ time.Time) error {
			storedEnc = encMsg
			return nil
		})

	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			Content:   long,
			CreatedAt: edited.Add(-time.Hour),
			EditedAt:  &edited, UpdatedAt: &edited,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.NotEmpty(t, storedEnc)
	var env roomcrypto.EncryptedMessage
	require.NoError(t, json.Unmarshal(storedEnc, &env))
	plaintext, err := decryptForTest(&env, key.KeyPair.PrivateKey)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("z", model.LastMessagePreviewMaxRunes), plaintext,
		"the stored preview envelope seals the trimmed snippet")

	require.Len(t, pub.records, 1)
	var roomEvt model.EditRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	require.NotEmpty(t, roomEvt.EncryptedNewContent)
	var evEnv roomcrypto.EncryptedMessage
	require.NoError(t, json.Unmarshal(roomEvt.EncryptedNewContent, &evEnv))
	evPlain, err := decryptForTest(&evEnv, key.KeyPair.PrivateKey)
	require.NoError(t, err)
	assert.Equal(t, long, evPlain, "the event envelope seals the FULL content")
}

// The delete-path RPC is skipped when the stored preview already identifies
// the survivor: neither pointer targets the deleted message, so the preview
// cannot change and IS the survivor.
func TestHandleDeleted_StoredPreviewIdentifiesSurvivor_SkipsFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	stored := &model.LastMessagePreview{MessageID: "m-last", SenderAccount: "bob", Msg: "current last", CreatedAt: deletedAt.Add(-time.Minute)}
	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a", LastMsgID: "m-last", LastMsg: stored}
	fetcher := &stubLastMsgFetcher{}

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", ptrOf(stored), stored, deletedAt).Return(nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt)))

	assert.Equal(t, 0, fetcher.calls, "a non-latest delete must not pay the history RPC")
	require.Len(t, pub.records, 1)
	var roomEvt model.DeleteRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	require.NotNil(t, roomEvt.LastMessage)
	assert.Equal(t, "m-last", roomEvt.LastMessage.MessageID, "the stored preview is the survivor")
}

// Drift: lastMsgId tracks a newer system message, but the preview still shows
// the deleted message — the survivor is unknown locally, so the RPC must run.
func TestHandleDeleted_DriftPreviewIsDeleted_Fetches(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	survivor := survivorPreview(deletedAt.Add(-2 * time.Hour))
	fetcher := &stubLastMsgFetcher{preview: survivor}
	room := &model.Room{
		ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a",
		LastMsgID: "m-sys",
		LastMsg:   &model.LastMessagePreview{MessageID: "msg-1", SenderAccount: "alice", Msg: "to be deleted", CreatedAt: deletedAt.Add(-time.Minute)},
	}

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", ptrOf(survivor), survivor, deletedAt).Return(nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt)))

	assert.Equal(t, 1, fetcher.calls)
	require.Len(t, pub.records, 1)
	var roomEvt model.DeleteRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	require.NotNil(t, roomEvt.LastMessage)
	assert.Equal(t, "m-prev", roomEvt.LastMessage.MessageID)
}

// Legacy rooms (lastMsgId set pre-feature, no lastMsg subdoc) can't identify
// the survivor locally — fall back to the RPC.
func TestHandleDeleted_LegacyRoomNoStoredPreview_Fetches(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	survivor := survivorPreview(deletedAt.Add(-2 * time.Hour))
	fetcher := &stubLastMsgFetcher{preview: survivor}
	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a", LastMsgID: "m-other"}

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", ptrOf(survivor), survivor, deletedAt).Return(nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt)))

	assert.Equal(t, 1, fetcher.calls, "no stored preview: the survivor must come from history")
}

// Encrypted room, skipped fetch: the stored preview is already in wire form
// (EncMsg sealed at create) — served as-is, no key fetch.
func TestHandleDeleted_EncryptedRoom_StoredPreviewServedAsIs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	enc := json.RawMessage(`{"v":1,"nonce":"bm9uY2U=","ciphertext":"c2VhbGVk"}`)
	stored := &model.LastMessagePreview{MessageID: "m-last", SenderAccount: "bob", EncMsg: enc, CreatedAt: deletedAt.Add(-time.Minute)}
	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a", LastMsgID: "m-last", LastMsg: stored}
	fetcher := &stubLastMsgFetcher{}

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", ptrOf(stored), stored, deletedAt).Return(nil)
	// No keyStore expectations: the stored ciphertext is reused, never re-sealed.

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt)))

	assert.Equal(t, 0, fetcher.calls)
	require.Len(t, pub.records, 1)
	var roomEvt model.DeleteRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	require.NotNil(t, roomEvt.LastMessage)
	assert.Empty(t, roomEvt.LastMessage.Msg)
	assert.JSONEq(t, string(enc), string(roomEvt.LastMessage.EncMsg))
}

// Visible thread replies (TShow=true) delete through the room lane: the event
// must carry the surviving preview AND the thread badge must still update —
// docs/client-api.md documents both for this lane.
func TestHandleDeleted_VisibleThreadReply_CarriesPreviewAndBadge(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	survivor := survivorPreview(deletedAt.Add(-2 * time.Hour))
	fetcher := &stubLastMsgFetcher{preview: survivor}
	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	newTCount := 3
	newTlm := deletedAt.Add(-30 * time.Minute)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", ptrOf(survivor), survivor, deletedAt).Return(nil)

	evt := model.MessageEvent{
		Event:              model.EventDeleted,
		SiteID:             "site-a",
		Timestamp:          deletedAt.UnixMilli(),
		NewTCount:          &newTCount,
		NewThreadLastMsgAt: &newTlm,
		Message: model.Message{
			ID: "msg-1", RoomID: "r1", UserID: "u-alice", UserAccount: "alice",
			ThreadParentMessageID: "m-parent", TShow: true,
			CreatedAt: deletedAt.Add(-time.Hour), UpdatedAt: &deletedAt,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	assert.Equal(t, 1, fetcher.calls, "visible thread-reply deletes ride the room lane incl. the preview fetch")
	require.Len(t, pub.records, 2, "delete event + thread badge")

	assert.Equal(t, subject.RoomEvent("r1"), pub.records[0].subject)
	var delEvt model.DeleteRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &delEvt))
	require.NotNil(t, delEvt.LastMessage, "visible-delete lane carries lastMessage per docs")
	assert.Equal(t, "m-prev", delEvt.LastMessage.MessageID)

	assert.Equal(t, subject.RoomEvent("r1"), pub.records[1].subject)
	var badge model.ThreadMetadataUpdatedEvent
	require.NoError(t, json.Unmarshal(pub.records[1].data, &badge))
	assert.Equal(t, model.RoomEventThreadMetadataUpdated, badge.Type)
	assert.Equal(t, model.ThreadActionReplyDeleted, badge.Action)
	assert.Equal(t, 3, badge.NewTCount)
	assert.Equal(t, "m-parent", badge.ParentMessageID)
	require.NotNil(t, badge.NewThreadLastMsgAt)
	assert.True(t, badge.NewThreadLastMsgAt.Equal(newTlm))
}

// #6: when the newest survivor is a system notice, the rewind receives the
// system POINTER and the user-message PREVIEW separately — room sorting must
// not skip past the system message, while the event still previews the user
// message.
func TestHandleDeleted_SystemPointerAndUserSurvivorRewindSeparately(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	survivor := survivorPreview(deletedAt.Add(-2 * time.Hour))
	sysPointer := &model.LastMessagePointer{MessageID: "m-sys", CreatedAt: deletedAt.Add(-time.Hour)}
	fetcher := &stubLastMsgFetcher{preview: survivor, pointer: sysPointer}

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a", LastMsgID: "msg-1"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", sysPointer, survivor, deletedAt).Return(nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt)))

	require.Len(t, pub.records, 1)
	var roomEvt model.DeleteRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	require.NotNil(t, roomEvt.LastMessage)
	assert.Equal(t, "m-prev", roomEvt.LastMessage.MessageID, "the EVENT previews the user survivor, never the system notice")
}

// #7: the fetch ceiling is the delete-event time — the stored lastMsgAt can
// lag behind coalesced creates, and a survivor created in that window must
// stay findable.
func TestHandleDeleted_PassesDeleteTimeAsFetchCeiling(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	deletedAt := time.Date(2026, 7, 1, 12, 10, 0, 0, time.UTC)
	fetcher := &stubLastMsgFetcher{}

	room := &model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(room, nil)
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", nil, nil, deletedAt).Return(nil)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, fetcher, false)
	require.NoError(t, h.HandleMessage(context.Background(), makeDeleteEvent("r1", deletedAt)))

	assert.True(t, fetcher.lastBefore.Equal(deletedAt), "walk ceiling = delete-event time, not the stored lastMsgAt")
}

// #10: an encrypted-room edit that CLEARS the text (attachment-only edit)
// still publishes an envelope — clients need it to apply the clear — while
// the store patch carries no preview ciphertext at all.
func TestHandleUpdated_EncryptedChannel_EmptyContent_EventCarriesEnvelope_PatchUnsets(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	us := NewMockUserStore(ctrl)
	pub := &mockPublisher{}
	keyStore := NewMockRoomKeyProvider(ctrl)

	roomID := "r1"
	room := &model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a"}
	key := testRoomKey(t)
	store.EXPECT().GetRoom(gomock.Any(), roomID).Return(room, nil)
	keyStore.EXPECT().Get(gomock.Any(), roomID).Return(key, nil)

	edited := time.Date(2026, 7, 1, 12, 5, 0, 0, time.UTC)
	store.EXPECT().SetRoomLastMessageEdited(gomock.Any(), roomID, "msg-1", "", gomock.Nil(), edited).Return(nil)

	evt := model.MessageEvent{
		Event:     model.EventUpdated,
		SiteID:    "site-a",
		Timestamp: edited.UnixMilli(),
		Message: model.Message{
			ID: "msg-1", RoomID: roomID, UserID: "u-alice", UserAccount: "alice",
			Content:   "",
			CreatedAt: edited.Add(-time.Hour),
			EditedAt:  &edited, UpdatedAt: &edited,
		},
	}
	data, err := json.Marshal(&evt)
	require.NoError(t, err)

	h := NewHandler(store, us, pub, keyStore, defaultParentFetcher, defaultLastMsgFetcher, true)
	require.NoError(t, h.HandleMessage(context.Background(), data))

	require.Len(t, pub.records, 1)
	var roomEvt model.EditRoomEvent
	require.NoError(t, json.Unmarshal(pub.records[0].data, &roomEvt))
	assert.Empty(t, roomEvt.NewContent)
	require.NotEmpty(t, roomEvt.EncryptedNewContent, "empty-content edits still seal an envelope")
	var env roomcrypto.EncryptedMessage
	require.NoError(t, json.Unmarshal(roomEvt.EncryptedNewContent, &env))
	plaintext, err := decryptForTest(&env, key.KeyPair.PrivateKey)
	require.NoError(t, err)
	assert.Empty(t, plaintext, "the envelope decrypts to the cleared (empty) text")
}
