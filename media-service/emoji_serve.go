package main

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/emoji"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// HandleEmojiGet serves a custom emoji image. The path no longer carries
// siteID: the FE only ever fetches the local site's emoji list, so the
// optional lowercase ?siteid= query param (matching avatar's siteIDParam) is
// just a cross-site hint — absent or equal to this site serves locally; a
// known remote site 307-redirects to its owning cluster with the param
// dropped, so the redirect target always defaults to local and resolves
// there — no redirect loop is possible, so no fwd guard is needed. There is
// no generated default: unknown emoji are 404s.
func (h *handler) HandleEmojiGet(c *gin.Context) {
	ctx := c.Request.Context()
	c.Set("media_kind", "emoji")
	siteID := c.Query(siteIDParam)

	shortcode, err := emoji.Canonicalize(c.Param("shortcode"))
	if err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(ctx, c, errcode.BadRequest("invalid emoji shortcode"))
		return
	}

	if siteID != "" && siteID != h.cfg.SiteID {
		base := h.cfg.clusterBaseURL(siteID)
		if base == "" {
			c.Set("media_outcome", "error")
			errhttp.Write(ctx, c, errcode.NotFound("unknown site"))
			return
		}
		c.Set("media_outcome", "redirect")
		c.Redirect(http.StatusTemporaryRedirect, base+"/api/v1/emoji/"+shortcode)
		return
	}

	e, found, err := h.emojis.EmojiDoc(ctx, h.cfg.SiteID, shortcode)
	if err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(ctx, c, fmt.Errorf("find custom emoji: %w", err))
		return
	}
	if !found {
		c.Set("media_outcome", "error")
		errhttp.Write(ctx, c, errcode.NotFound("emoji not found"))
		return
	}

	h.setImageCacheHeaders(c, e.ETag)
	if c.GetHeader("If-None-Match") == e.ETag && e.ETag != "" {
		c.Set("media_outcome", "304")
		c.Status(http.StatusNotModified)
		return
	}
	rc, info, err := h.blobs.Get(ctx, e.MinioKey)
	if errors.Is(err, errBlobNotFound) {
		// doc⟺object invariant briefly broken (concurrent delete): treat as gone.
		c.Set("media_outcome", "error")
		errhttp.Write(ctx, c, errcode.NotFound("emoji not found"))
		return
	}
	if err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(ctx, c, fmt.Errorf("get emoji object: %w", err))
		return
	}
	defer rc.Close()
	c.Set("media_outcome", "stream")
	c.DataFromReader(http.StatusOK, info.Size, info.ContentType, rc, nil)
}
