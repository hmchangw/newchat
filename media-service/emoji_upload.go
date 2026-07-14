package main

import (
	"bytes"
	"fmt"
	"image"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	_ "image/gif" // register the GIF decoder (jpeg/png registered in upload.go)

	"github.com/hmchangw/chat/pkg/emoji"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
)

// emojiObjectKey is the MinIO key for an emoji blob; the bucket is shared with
// avatars, so the prefix namespaces it. siteID is included so the key carries
// the emoji's full identity — shortcodes are only unique per site.
func emojiObjectKey(siteID, shortcode string) string {
	return "emoji/" + siteID + "/" + shortcode
}

// emojiDocID is the deterministic custom_emojis document _id.
func emojiDocID(siteID, shortcode string) string { return siteID + ":" + shortcode }

// emojiImagePath is the canonical imageUrl stored on the doc and returned by
// list: "/api/v1/emoji/{shortcode}". It carries no ?siteid= — the doc's siteId
// field (and the site the list was requested from) already identify the owner,
// so a cross-site consumer composes imageUrl + "?siteid=" + siteId itself.
// shortcode is charset-validated, so no escaping is needed.
func emojiImagePath(shortcode string) string {
	return "/api/v1/emoji/" + shortcode
}

func isSupportedEmojiFormat(format string) bool {
	return format == "png" || format == "jpeg" || format == "gif"
}

func errImageExceeds(maxDim int) error {
	return errcode.BadRequest(fmt.Sprintf("image exceeds %dx%d", maxDim, maxDim))
}

// validateEmojiImage confirms raw is a PNG, JPEG, or GIF no larger than
// maxDim on either axis and returns its content type (e.g. "image/png").
//
// The dimension limit is enforced in two phases on purpose — they guard
// different failure modes:
//  1. header pre-check (image.DecodeConfig): rejects oversized *declared*
//     dimensions before image.Decode allocates pixel buffers from them — a
//     small compressed body can declare a huge image (decompression-bomb
//     hardening);
//  2. decoded-bounds check: rejects files whose actual pixels disagree with
//     their header, so nothing larger than maxDim reaches storage. Animated
//     GIFs decode as their first frame, which is what this check applies to.
func validateEmojiImage(raw []byte, maxDim int) (string, error) {
	const invalidImage = "body is not a valid PNG, JPEG, or GIF image"

	cfg, cfgFormat, err := image.DecodeConfig(bytes.NewReader(raw))
	if err != nil || !isSupportedEmojiFormat(cfgFormat) {
		return "", errcode.BadRequest(invalidImage)
	}
	if cfg.Width > maxDim || cfg.Height > maxDim {
		return "", errImageExceeds(maxDim)
	}

	img, format, err := image.Decode(bytes.NewReader(raw))
	if err != nil || !isSupportedEmojiFormat(format) {
		return "", errcode.BadRequest(invalidImage)
	}
	if b := img.Bounds(); b.Dx() > maxDim || b.Dy() > maxDim {
		return "", errImageExceeds(maxDim)
	}
	return "image/" + format, nil
}

// emojiUploadResponse is the 200 body on a successful upload. UpdatedAt
// serializes as RFC3339, matching EmojiEntry on the list wire.
type emojiUploadResponse struct {
	Shortcode   string    `json:"shortcode"`
	ETag        string    `json:"etag"`
	ContentType string    `json:"contentType"`
	Size        int64     `json:"size"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

func (h *handler) HandleEmojiUpload(c *gin.Context) {
	ctx := c.Request.Context()
	c.Set("media_kind", "emoji")
	siteID := h.cfg.SiteID

	shortcode, err := emoji.Canonicalize(c.Param("shortcode"))
	if err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(ctx, c, errcode.BadRequest("invalid emoji shortcode"))
		return
	}
	if emoji.IsStandard(shortcode) {
		c.Set("media_outcome", "error")
		errhttp.Write(ctx, c, errcode.BadRequest(
			"shortcode collides with a built-in standard emoji",
			errcode.WithReason(errcode.EmojiShortcodeReserved)))
		return
	}

	// Size cap before reading the body.
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.cfg.EmojiMaxUploadBytes)
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(ctx, c, errcode.BadRequest("upload too large or unreadable"))
		return
	}

	contentType, err := validateEmojiImage(raw, h.cfg.EmojiMaxDimension)
	if err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(ctx, c, err)
		return
	}

	// Store the object FIRST, then upsert the doc (doc exists ⟺ object exists).
	key := emojiObjectKey(siteID, shortcode)
	// A delete racing this upload can remove the doc AND this just-written
	// blob before the upsert below, leaving a doc without a blob until the
	// next upload; the serve path degrades to 404 (see emoji_serve.go).
	etag, err := h.blobs.Put(ctx, key, bytes.NewReader(raw), int64(len(raw)), contentType)
	if err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(ctx, c, fmt.Errorf("store emoji object: %w", err))
		return
	}
	now := time.Now().UTC().UnixMilli()
	uploader := c.Query("uploader") // v1: unauthenticated, audit-only (§7 client-api)
	if len(uploader) > 64 {
		uploader = uploader[:64]
	}
	e := &model.CustomEmoji{
		ID:          emojiDocID(siteID, shortcode),
		SiteID:      siteID,
		Shortcode:   shortcode,
		ImageURL:    emojiImagePath(shortcode),
		CreatedBy:   uploader,
		CreatedAt:   now,
		UpdatedBy:   uploader,
		UpdatedAt:   now,
		MinioKey:    key,
		ContentType: contentType,
		Size:        int64(len(raw)),
		ETag:        etag,
	}
	if err := h.emojis.UpsertEmoji(ctx, e); err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(ctx, c, fmt.Errorf("upsert emoji doc: %w", err))
		return
	}
	c.Set("media_outcome", "upload")
	c.Header("X-Content-Type-Options", "nosniff")
	c.JSON(http.StatusOK, emojiUploadResponse{
		Shortcode:   shortcode,
		ETag:        e.ETag,
		ContentType: e.ContentType,
		Size:        e.Size,
		UpdatedAt:   time.UnixMilli(e.UpdatedAt).UTC(),
	})
}
