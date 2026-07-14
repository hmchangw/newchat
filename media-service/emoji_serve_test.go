package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

func TestEmojiGet_LocalHit_Streams(t *testing.T) {
	r, _, emojis, blobs := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "party").
		Return(&model.CustomEmoji{MinioKey: "emoji/s1/party", ETag: "e1"}, true, nil)
	blobs.objects = map[string][]byte{"emoji/s1/party": []byte("GIF!")}
	blobs.info = map[string]blobInfo{"emoji/s1/party": {Size: 4, ContentType: "image/gif", ETag: "e1"}}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/emoji/party", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/gif", w.Header().Get("Content-Type"))
	assert.Equal(t, "GIF!", w.Body.String())
	assert.Contains(t, w.Header().Get("Cache-Control"), "max-age=3600")
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
}

func TestEmojiGet_NotModified_304(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "party").
		Return(&model.CustomEmoji{MinioKey: "emoji/s1/party", ETag: "e1"}, true, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/emoji/party", nil)
	req.Header.Set("If-None-Match", "e1")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotModified, w.Code)
	assert.Empty(t, w.Body.String())
}

func TestEmojiGet_LocalMiss_404(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "nope").Return(nil, false, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/emoji/nope", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestEmojiGet_ExplicitLocalSite_Streams(t *testing.T) {
	r, _, emojis, blobs := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "party").
		Return(&model.CustomEmoji{MinioKey: "emoji/s1/party", ETag: "e1"}, true, nil)
	blobs.objects = map[string][]byte{"emoji/s1/party": []byte("GIF!")}
	blobs.info = map[string]blobInfo{"emoji/s1/party": {Size: 4, ContentType: "image/gif", ETag: "e1"}}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/emoji/party?siteid=s1", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "image/gif", w.Header().Get("Content-Type"))
	assert.Equal(t, "GIF!", w.Body.String())
}

func TestEmojiGet_RemoteSite_307(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/emoji/party?siteid=s2", nil))
	assert.Equal(t, http.StatusTemporaryRedirect, w.Code)
	assert.Equal(t, "https://avatar-s2/api/v1/emoji/party", w.Header().Get("Location"))
}

func TestEmojiGet_UnknownSite_404(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/emoji/party?siteid=s9", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestEmojiGet_InvalidShortcode_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/emoji/Bad%20Name", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestEmojiGet_StoreError_500(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "party").Return(nil, false, assert.AnError)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/emoji/party", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestEmojiGet_BlobMissing_404(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "party").
		Return(&model.CustomEmoji{MinioKey: "emoji/s1/party", ETag: "e1"}, true, nil)
	// fakeBlobStore with no seeded object returns errBlobNotFound.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/emoji/party", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestEmojiGet_BlobError_500(t *testing.T) {
	r, _, emojis, blobs := newEmojiTestRouter(t)
	emojis.EXPECT().EmojiDoc(gomock.Any(), "s1", "party").
		Return(&model.CustomEmoji{MinioKey: "emoji/s1/party", ETag: "e1"}, true, nil)
	blobs.getErr = assert.AnError
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/emoji/party", nil))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
