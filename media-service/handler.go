package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
)

type handler struct {
	store    avatarStore
	emojis   emojiStore
	blobs    blobStore
	cfg      config
	eidCache *eidCache
}

func newHandler(store avatarStore, emojis emojiStore, blobs blobStore, cfg *config) *handler {
	return &handler{
		store:    store,
		emojis:   emojis,
		blobs:    blobs,
		cfg:      *cfg,
		eidCache: newEIDCache(store, cfg.EIDCacheCapacity, cfg.EIDCacheTTL),
	}
}

func (h *handler) HandleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

const forwardedParam = "fwd"
const siteIDParam = "siteid"

func (h *handler) setImageCacheHeaders(c *gin.Context, etag string) {
	c.Header("Cache-Control", fmt.Sprintf("public, max-age=%d", h.cfg.CacheMaxAgeSeconds))
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("Content-Security-Policy", "default-src 'none'")
	if etag != "" {
		c.Header("ETag", etag)
	}
}

func (h *handler) serveDefault(c *gin.Context, kind, seed, name string) {
	c.Set("media_kind", kind)
	c.Set("media_outcome", "default")
	etag := defaultETag(seed, name)
	h.setImageCacheHeaders(c, etag)
	c.Header("Content-Type", "image/svg+xml")
	if c.GetHeader("If-None-Match") == etag {
		c.Set("media_outcome", "304")
		c.Status(http.StatusNotModified)
		return
	}
	c.Data(http.StatusOK, "image/svg+xml", renderDefaultSVG(seed, name))
}

func (h *handler) serveStored(c *gin.Context, kind string, av *model.Avatar, fbSeed, fbName string) {
	c.Set("media_kind", kind)
	h.setImageCacheHeaders(c, av.ETag)
	if m := c.GetHeader("If-None-Match"); m != "" && m == av.ETag {
		c.Set("media_outcome", "304")
		c.Status(http.StatusNotModified)
		return
	}
	rc, info, err := h.blobs.Get(c.Request.Context(), av.MinioKey)
	if errors.Is(err, errBlobNotFound) {
		h.serveDefault(c, kind, fbSeed, fbName)
		return
	}
	if err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(c.Request.Context(), c, err)
		return
	}
	defer rc.Close()
	c.Set("media_outcome", "stream")
	c.DataFromReader(http.StatusOK, info.Size, info.ContentType, rc, nil)
}

// redirectCrossCluster writes a 307 to the owning cluster if it is remote and
// resolvable; returns true if it handled the request.
func (h *handler) redirectCrossCluster(c *gin.Context, kind, owning, path string) bool {
	if owning == "" || owning == h.cfg.SiteID || c.Query(forwardedParam) != "" {
		return false
	}
	base := h.cfg.clusterBaseURL(owning)
	if base == "" {
		return false // unknown site → caller falls through to default
	}
	c.Set("media_kind", kind)
	c.Set("media_outcome", "redirect")
	c.Redirect(http.StatusTemporaryRedirect, base+path+"?fwd=1")
	return true
}

func (h *handler) HandleRoomAvatar(c *gin.Context) {
	roomID := c.Param("roomID")
	ctx := c.Request.Context()

	// Fast path: trust the ?siteid= hint, skip the subscription query.
	if hint := c.Query(siteIDParam); hint != "" {
		if h.redirectCrossCluster(c, "room", hint, "/api/v1/avatar/room/"+url.PathEscape(roomID)) {
			return
		}
		h.serveRoomLocal(c, roomID, roomID) // no Name available → use roomID
		return
	}

	siteID, roomType, name, found, err := h.store.RoomSite(ctx, roomID)
	if err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(c.Request.Context(), c, err)
		return
	}
	if !found {
		h.serveDefault(c, "room", roomID, roomID)
		return
	}
	if roomType == model.RoomTypeDM || roomType == model.RoomTypeBotDM {
		h.serveDefault(c, "room", roomID, name)
		return
	}
	if h.redirectCrossCluster(c, "room", siteID, "/api/v1/avatar/room/"+url.PathEscape(roomID)) {
		return
	}
	h.serveRoomLocal(c, roomID, name)
}

// serveRoomLocal does the avatars-doc lookup + stream/default for a local room.
func (h *handler) serveRoomLocal(c *gin.Context, roomID, name string) {
	av, found, err := h.store.Avatar(c.Request.Context(), model.AvatarSubjectRoom, roomID)
	if err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(c.Request.Context(), c, err)
		return
	}
	if found {
		h.serveStored(c, "room", av, roomID, name)
		return
	}
	h.serveDefault(c, "room", roomID, name)
}

func (h *handler) HandleAccountAvatar(c *gin.Context) {
	account := c.Param("accountName")
	ctx := c.Request.Context()

	if isBot(account) {
		owning := c.Query(siteIDParam)
		if owning == "" {
			s, found, err := h.store.BotSite(ctx, account)
			if err != nil {
				c.Set("media_outcome", "error")
				errhttp.Write(c.Request.Context(), c, err)
				return
			}
			if !found {
				h.serveDefault(c, "bot", account, account)
				return
			}
			owning = s
		}
		if h.redirectCrossCluster(c, "bot", owning, "/api/v1/avatar/"+url.PathEscape(account)) {
			return
		}
		av, found, err := h.store.Avatar(ctx, model.AvatarSubjectBot, account)
		if err != nil {
			c.Set("media_outcome", "error")
			errhttp.Write(c.Request.Context(), c, err)
			return
		}
		if found {
			h.serveStored(c, "bot", av, account, account)
			return
		}
		h.serveDefault(c, "bot", account, account)
		return
	}

	// user (always local)
	eid, found, err := h.eidCache.get(ctx, account)
	if err != nil {
		c.Set("media_outcome", "error")
		errhttp.Write(c.Request.Context(), c, err)
		return
	}
	if !found {
		h.serveDefault(c, "user", account, account)
		return
	}
	c.Set("media_kind", "user")
	c.Set("media_outcome", "redirect")
	c.Redirect(http.StatusTemporaryRedirect, employeePhotoURL(h.cfg.EmployeePhotoBaseURL, eid))
}
