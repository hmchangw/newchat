package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/emoji"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

// HandleEmojiList replies with this site's full custom-emoji set. The subject
// carries the target siteID, so the supercluster routes each request to the
// owning site's media-service — this handler only ever serves its own site.
func (h *handler) HandleEmojiList(c *natsrouter.Context) (*model.EmojiListResponse, error) {
	list, err := h.emojis.ListEmojis(c, h.cfg.SiteID)
	if err != nil {
		return nil, fmt.Errorf("list custom emojis: %w", err)
	}
	entries := make([]model.EmojiEntry, 0, len(list))
	for i := range list {
		e := &list[i]
		entries = append(entries, model.EmojiEntry{
			Shortcode:   e.Shortcode,
			ImageURL:    e.ImageURL,
			ContentType: e.ContentType,
			ETag:        e.ETag,
			UpdatedAt:   time.UnixMilli(e.UpdatedAt).UTC(),
		})
	}
	return &model.EmojiListResponse{Emojis: entries}, nil
}

// HandleEmojiDelete removes one custom emoji. Anyone may delete (v1); the
// authenticated caller comes from the JWT-enforced {account} subject token.
// Gated by the EMOJI_DELETE_ENABLED kill-switch (default off).
func (h *handler) HandleEmojiDelete(c *natsrouter.Context, req model.EmojiDeleteRequest) (*model.EmojiDeleteResponse, error) {
	if !h.cfg.EmojiDeleteEnabled {
		return nil, errcode.Forbidden("emoji delete is disabled",
			errcode.WithReason(errcode.EmojiDeleteDisabled))
	}

	shortcode, err := emoji.Canonicalize(req.Shortcode)
	if err != nil {
		return nil, errcode.BadRequest("invalid emoji shortcode")
	}

	// Doc first: once it is gone the emoji is invisible everywhere; the blob
	// delete below is best-effort because an orphaned object is unreachable.
	minioKey, found, err := h.emojis.DeleteEmoji(c, h.cfg.SiteID, shortcode)
	if err != nil {
		return nil, fmt.Errorf("delete custom emoji: %w", err)
	}
	if !found {
		return nil, errcode.NotFound("emoji not found")
	}
	if err := h.blobs.Delete(c, minioKey); err != nil {
		slog.WarnContext(c, "emoji blob delete failed; doc already removed",
			"shortcode", shortcode, "key", minioKey, "error", err)
	}
	return &model.EmojiDeleteResponse{Shortcode: shortcode, Deleted: true}, nil
}

// registerEmojiNATS wires the emoji request-reply endpoints; panics on
// subscription failure (fatal at startup, matching natsrouter semantics).
func registerEmojiNATS(r *natsrouter.Router, h *handler, siteID string) {
	natsrouter.RegisterNoBody(r, subject.EmojiListPattern(siteID), h.HandleEmojiList)
	natsrouter.Register(r, subject.EmojiDeletePattern(siteID), h.HandleEmojiDelete)
}
