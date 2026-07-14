package main

import (
	"bytes"
	"fmt"
	"image"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	_ "image/jpeg" // register decoders
	_ "image/png"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
)

// botUploadResponse is the 200 body on a successful upload — it hands the
// uploader the new ETag (for immediate cache-busting) plus the stored metadata.
type botUploadResponse struct {
	ETag        string    `json:"etag"`
	ContentType string    `json:"contentType"`
	Size        int64     `json:"size"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

func (h *handler) HandleBotUpload(c *gin.Context) {
	ctx := c.Request.Context()
	account := c.Param("botName")

	if !isBot(account) {
		errhttp.Write(ctx, c, errcode.BadRequest("not a bot account"))
		return
	}

	// Existence + cluster locality from one user-record lookup.
	siteID, found, err := h.store.BotSite(ctx, account)
	if err != nil {
		errhttp.Write(ctx, c, err)
		return
	}
	if !found {
		errhttp.Write(ctx, c, errcode.NotFound("bot not found"))
		return
	}
	if siteID != h.cfg.SiteID {
		base := h.cfg.clusterBaseURL(siteID)
		errhttp.Write(ctx, c, errcode.Conflict(
			fmt.Sprintf("bot is owned by another cluster; upload to %s", base),
			errcode.WithReason(errcode.AvatarWrongCluster)))
		return
	}

	// Size cap before reading the body.
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.cfg.MaxUploadBytes)
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("upload too large or unreadable"))
		return
	}

	// Decode to confirm a real PNG/JPEG; capture the detected format.
	_, format, err := image.Decode(bytes.NewReader(raw))
	if err != nil || (format != "png" && format != "jpeg") {
		errhttp.Write(ctx, c, errcode.BadRequest("body is not a valid PNG or JPEG image"))
		return
	}
	contentType := "image/" + format

	// Store the object FIRST, then upsert the doc (doc exists ⟺ object exists).
	key := botObjectKey(account)
	etag, err := h.blobs.Put(ctx, key, bytes.NewReader(raw), int64(len(raw)), contentType)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("store avatar object: %w", err))
		return
	}
	now := time.Now().UTC()
	av := &model.Avatar{
		ID:          "bot:" + account,
		SubjectType: model.AvatarSubjectBot,
		SubjectID:   account,
		MinioKey:    key,
		ContentType: contentType,
		Size:        int64(len(raw)),
		ETag:        etag,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := h.store.SetBotAvatar(ctx, av); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("upsert avatar doc: %w", err))
		return
	}
	c.Header("X-Content-Type-Options", "nosniff")
	c.JSON(http.StatusOK, botUploadResponse{
		ETag:        av.ETag,
		ContentType: av.ContentType,
		Size:        av.Size,
		UpdatedAt:   av.UpdatedAt,
	})
}
