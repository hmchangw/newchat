package model

import "time"

// CustomEmoji is a site-scoped custom reaction emoji; Shortcode is stored bare
// (no wrapping colons). Written by media-service (upload/delete); read by the
// pkg/emoji validator via an existence check. Doc exists ⟺ MinIO object exists.
type CustomEmoji struct {
	// ID is the deterministic "{siteID}:{shortcode}" document key.
	ID        string `json:"id"        bson:"_id"`
	SiteID    string `json:"siteId"    bson:"siteId"`
	Shortcode string `json:"shortcode" bson:"shortcode"`
	// ImageURL is the canonical relative serve path "/api/v1/emoji/{shortcode}" (bare; compose ?siteid= from the siteId field for cross-site use).
	ImageURL  string `json:"imageUrl"  bson:"imageUrl"`
	CreatedBy string `json:"createdBy" bson:"createdBy"`
	CreatedAt int64  `json:"createdAt" bson:"createdAt"`
	// UpdatedBy/UpdatedAt track the last upload; CreatedBy/CreatedAt are
	// preserved on overwrite (audit).
	UpdatedBy string `json:"updatedBy"   bson:"updatedBy"`
	UpdatedAt int64  `json:"updatedAt"   bson:"updatedAt"`
	// MinioKey is "emoji/{siteID}/{shortcode}" — site-scoped: shortcodes are only unique per site.
	MinioKey    string `json:"minioKey"    bson:"minioKey"`
	ContentType string `json:"contentType" bson:"contentType"`
	Size        int64  `json:"size"        bson:"size"`
	ETag        string `json:"etag"        bson:"etag"`
}

// EmojiEntry is the wire shape of one emoji in EmojiListResponse.
type EmojiEntry struct {
	Shortcode   string `json:"shortcode"   bson:"shortcode"`
	ImageURL    string `json:"imageUrl"    bson:"imageUrl"`
	ContentType string `json:"contentType" bson:"contentType"`
	ETag        string `json:"etag"        bson:"etag"`
	// UpdatedAt serializes as RFC3339 (client wire); the stored doc keeps
	// epoch millis — media-service converts at the boundary.
	UpdatedAt time.Time `json:"updatedAt" bson:"updatedAt"`
}

// EmojiListResponse is the reply to chat.user.{account}.request.emoji.{siteID}.list.
type EmojiListResponse struct {
	Emojis []EmojiEntry `json:"emojis" bson:"emojis"`
}

// EmojiDeleteRequest is the body of chat.user.{account}.request.emoji.{siteID}.delete.
type EmojiDeleteRequest struct {
	Shortcode string `json:"shortcode" bson:"shortcode"`
}

// EmojiDeleteResponse is the reply to a successful emoji delete.
type EmojiDeleteResponse struct {
	Shortcode string `json:"shortcode" bson:"shortcode"`
	Deleted   bool   `json:"deleted"   bson:"deleted"`
}
