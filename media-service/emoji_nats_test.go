package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

func newNATSTestHandler(t *testing.T) (*handler, *MockemojiStore, *fakeBlobStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockavatarStore(ctrl)
	emojis := NewMockemojiStore(ctrl)
	blobs := &fakeBlobStore{}
	h := newHandler(store, emojis, blobs, &config{
		SiteID:             "s1",
		EIDCacheCapacity:   1000,
		EIDCacheTTL:        time.Minute,
		EmojiDeleteEnabled: true,
	})
	return h, emojis, blobs
}

func natsCtx() *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": "alice"})
}

func TestHandleEmojiList_Success(t *testing.T) {
	h, emojis, _ := newNATSTestHandler(t)
	emojis.EXPECT().ListEmojis(gomock.Any(), "s1").Return([]model.CustomEmoji{
		{Shortcode: "aaa", ImageURL: "/api/v1/emoji/aaa", ContentType: "image/png", ETag: "e1", UpdatedAt: 1000},
		{Shortcode: "bbb", ImageURL: "/api/v1/emoji/bbb", ContentType: "image/gif", ETag: "e2", UpdatedAt: 2000},
	}, nil)

	resp, err := h.HandleEmojiList(natsCtx())
	require.NoError(t, err)
	require.Len(t, resp.Emojis, 2)
	assert.Equal(t, model.EmojiEntry{
		Shortcode: "aaa", ImageURL: "/api/v1/emoji/aaa", ContentType: "image/png",
		ETag: "e1", UpdatedAt: time.UnixMilli(1000).UTC(),
	}, resp.Emojis[0])
	assert.Equal(t, "bbb", resp.Emojis[1].Shortcode)
}

func TestHandleEmojiList_Empty(t *testing.T) {
	h, emojis, _ := newNATSTestHandler(t)
	emojis.EXPECT().ListEmojis(gomock.Any(), "s1").Return(nil, nil)

	resp, err := h.HandleEmojiList(natsCtx())
	require.NoError(t, err)
	assert.NotNil(t, resp.Emojis, "empty set must marshal as [], not null")
	assert.Empty(t, resp.Emojis)
}

func TestHandleEmojiList_StoreError(t *testing.T) {
	h, emojis, _ := newNATSTestHandler(t)
	emojis.EXPECT().ListEmojis(gomock.Any(), "s1").Return(nil, assert.AnError)

	_, err := h.HandleEmojiList(natsCtx())
	require.Error(t, err)
}

func TestHandleEmojiDelete_Success_RemovesDocThenBlob(t *testing.T) {
	h, emojis, blobs := newNATSTestHandler(t)
	blobs.objects = map[string][]byte{"emoji/s1/party": []byte("x")}
	blobs.info = map[string]blobInfo{"emoji/s1/party": {}}
	emojis.EXPECT().DeleteEmoji(gomock.Any(), "s1", "party").Return("emoji/s1/party", true, nil)

	resp, err := h.HandleEmojiDelete(natsCtx(), model.EmojiDeleteRequest{Shortcode: "party"})
	require.NoError(t, err)
	assert.Equal(t, &model.EmojiDeleteResponse{Shortcode: "party", Deleted: true}, resp)
	_, ok := blobs.objects["emoji/s1/party"]
	assert.False(t, ok, "blob removed after the doc")
}

func TestHandleEmojiDelete_NotFound(t *testing.T) {
	h, emojis, _ := newNATSTestHandler(t)
	emojis.EXPECT().DeleteEmoji(gomock.Any(), "s1", "nope").Return("", false, nil)

	_, err := h.HandleEmojiDelete(natsCtx(), model.EmojiDeleteRequest{Shortcode: "nope"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "emoji not found")
}

func TestHandleEmojiDelete_InvalidShortcode(t *testing.T) {
	h, _, _ := newNATSTestHandler(t)

	_, err := h.HandleEmojiDelete(natsCtx(), model.EmojiDeleteRequest{Shortcode: "Bad Name"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid emoji shortcode")
}

func TestHandleEmojiDelete_StoreError(t *testing.T) {
	h, emojis, _ := newNATSTestHandler(t)
	emojis.EXPECT().DeleteEmoji(gomock.Any(), "s1", "party").Return("", false, assert.AnError)

	_, err := h.HandleEmojiDelete(natsCtx(), model.EmojiDeleteRequest{Shortcode: "party"})
	require.Error(t, err)
}

func TestHandleEmojiDelete_BlobDeleteFailure_StillSucceeds(t *testing.T) {
	h, emojis, blobs := newNATSTestHandler(t)
	blobs.deleteErr = assert.AnError
	emojis.EXPECT().DeleteEmoji(gomock.Any(), "s1", "party").Return("emoji/s1/party", true, nil)

	resp, err := h.HandleEmojiDelete(natsCtx(), model.EmojiDeleteRequest{Shortcode: "party"})
	require.NoError(t, err)
	assert.True(t, resp.Deleted)
}

func TestHandleEmojiDelete_Disabled_Forbidden(t *testing.T) {
	// fresh handler with the kill-switch off (default) — store must never be called
	ctrl := gomock.NewController(t)
	store := NewMockavatarStore(ctrl)
	emojis := NewMockemojiStore(ctrl)
	h := newHandler(store, emojis, &fakeBlobStore{}, &config{
		SiteID:           "s1",
		EIDCacheCapacity: 1000,
		EIDCacheTTL:      time.Minute,
	})
	_, err := h.HandleEmojiDelete(natsCtx(), model.EmojiDeleteRequest{Shortcode: "party"})
	require.Error(t, err)
	assert.ErrorContains(t, err, "emoji delete is disabled")
}
