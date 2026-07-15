package main

import (
	"context"
	"encoding/json"
	"errors"
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

func (nopLastMsgFetcher) FetchLastMessage(context.Context, string) (*model.LastMessagePreview, error) {
	return nil, nil
}

var defaultLastMsgFetcher = nopLastMsgFetcher{}

// stubLastMsgFetcher is a LastMessageFetcher test double returning a fixed
// preview (or error) and counting calls, so tests can assert whether the
// delete path fetched the room preview.
type stubLastMsgFetcher struct {
	preview *model.LastMessagePreview
	err     error
	calls   int
}

func (s *stubLastMsgFetcher) FetchLastMessage(context.Context, string) (*model.LastMessagePreview, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.preview, nil
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
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", survivor, deletedAt).Return(nil)

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
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", nil, deletedAt).Return(nil)

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
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", survivor, deletedAt).
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
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "dm-alice-bob", "msg-1", survivor, deletedAt).Return(nil)

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
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", gomock.Any(), deletedAt).
		DoAndReturn(func(_ context.Context, _, _ string, p *model.LastMessagePreview, _ time.Time) error {
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
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", survivor, deletedAt).Return(nil)

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
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "dm-alice-bob", "msg-1", survivor, deletedAt).Return(nil)

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
	store.EXPECT().RewindRoomLastMessage(gomock.Any(), "r1", "msg-1", nil, deletedAt).Return(nil)
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
