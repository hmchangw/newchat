package main

import (
	"bytes"
	"image"
	gifpalette "image/color/palette"
	"image/gif"
	"image/jpeg"
	"image/png"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

func pngSized(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, w, h))))
	return buf.Bytes()
}

func gifAnimated(t *testing.T) []byte {
	t.Helper()
	g := &gif.GIF{}
	for i := 0; i < 2; i++ {
		g.Image = append(g.Image, image.NewPaletted(image.Rect(0, 0, 2, 2), gifpalette.Plan9))
		g.Delay = append(g.Delay, 10)
	}
	var buf bytes.Buffer
	require.NoError(t, gif.EncodeAll(&buf, g))
	return buf.Bytes()
}

func TestEmojiUpload_Success_StoresBlobThenUpsertsDoc(t *testing.T) {
	r, _, emojis, blobs := newEmojiTestRouter(t)
	emojis.EXPECT().UpsertEmoji(gomock.Any(), gomock.Any()).DoAndReturn(func(_ any, e *model.CustomEmoji) error {
		assert.Equal(t, "s1:party", e.ID)
		assert.Equal(t, "s1", e.SiteID)
		assert.Equal(t, "party", e.Shortcode)
		assert.Equal(t, "/api/v1/emoji/party", e.ImageURL)
		assert.Equal(t, "emoji/s1/party", e.MinioKey)
		assert.Equal(t, "image/png", e.ContentType)
		assert.Equal(t, "alice", e.CreatedBy)
		assert.Equal(t, "alice", e.UpdatedBy)
		assert.NotEmpty(t, e.ETag)
		assert.Positive(t, e.UpdatedAt)
		return nil
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/api/v1/emoji/party?uploader=alice", pngSized(t, 2, 2), "image/png"))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	body := w.Body.String()
	assert.Contains(t, body, `"shortcode":"party"`)
	assert.Contains(t, body, `"etag":"etag-emoji/s1/party"`)
	assert.Contains(t, body, `"contentType":"image/png"`)
	assert.Regexp(t, `"updatedAt":"\d{4}-\d{2}-\d{2}T`, body, "updatedAt must be RFC3339, not epoch millis")
	_, ok := blobs.objects["emoji/s1/party"]
	assert.True(t, ok, "object stored before the doc")
}

func jpegBytes(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, jpeg.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 2, 2)), nil))
	return buf.Bytes()
}

func TestEmojiUpload_JPEG_Accepted(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().UpsertEmoji(gomock.Any(), gomock.Any()).DoAndReturn(func(_ any, e *model.CustomEmoji) error {
		assert.Equal(t, "image/jpeg", e.ContentType)
		return nil
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/api/v1/emoji/photo?uploader=alice", jpegBytes(t), "image/jpeg"))
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestEmojiUpload_AnimatedGIF_Accepted(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().UpsertEmoji(gomock.Any(), gomock.Any()).DoAndReturn(func(_ any, e *model.CustomEmoji) error {
		assert.Equal(t, "image/gif", e.ContentType)
		return nil
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/api/v1/emoji/dance?uploader=alice", gifAnimated(t), "image/gif"))
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestEmojiUpload_InvalidShortcode_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/api/v1/emoji/BadName", pngSized(t, 2, 2), "image/png"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestEmojiUpload_ReservedStandardShortcode_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/api/v1/emoji/thumbsup", pngSized(t, 2, 2), "image/png"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "emoji_shortcode_reserved")
}

func TestEmojiUpload_NotAnImage_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/api/v1/emoji/party", []byte("<svg></svg>"), "image/svg+xml"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestEmojiUpload_OversizeBytes_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/api/v1/emoji/party", make([]byte, 262144+1), "image/png"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestEmojiUpload_OversizeDimensions_400(t *testing.T) {
	r, _, _, _ := newEmojiTestRouter(t)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/api/v1/emoji/party", pngSized(t, 513, 513), "image/png"))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "512x512")
}

func TestEmojiUpload_BlobPutError_500(t *testing.T) {
	r, _, _, blobs := newEmojiTestRouter(t)
	blobs.putErr = assert.AnError
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/api/v1/emoji/party", pngSized(t, 2, 2), "image/png"))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestEmojiUpload_UpsertError_500(t *testing.T) {
	r, _, emojis, _ := newEmojiTestRouter(t)
	emojis.EXPECT().UpsertEmoji(gomock.Any(), gomock.Any()).Return(assert.AnError)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, putReq("/api/v1/emoji/party", pngSized(t, 2, 2), "image/png"))
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
