package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// avatarStore is the data access this service needs. Each method reads/writes
// exactly one collection: users, subscriptions, or avatars.
type avatarStore interface {
	// EmployeeID returns a user's employeeId (users collection). found=false when
	// the account has no user record or no employeeId.
	EmployeeID(ctx context.Context, account string) (eid string, found bool, err error)
	// BotSite returns a bot's owning siteID from its user record (bots are users,
	// synced to every cluster). found=false when no such bot record exists.
	BotSite(ctx context.Context, account string) (siteID string, found bool, err error)
	// RoomSite returns the room's owning site, type, and name from any one of its
	// local subscriptions. found=false when no local subscription exists.
	RoomSite(ctx context.Context, roomID string) (siteID string, roomType model.RoomType, name string, found bool, err error)
	// UserByAccount returns a user's identity fields (id, account, names,
	// deactivated) for the drive.members probe. found=false when no user record
	// exists for account.
	UserByAccount(ctx context.Context, account string) (*model.User, bool, error)
	// RoomMember reports whether account holds a subscription to roomID.
	RoomMember(ctx context.Context, roomID, account string) (bool, error)
	// Avatar looks up a custom-image doc by subject. found=false → serve default.
	Avatar(ctx context.Context, subjectType model.AvatarSubjectType, subjectID string) (*model.Avatar, bool, error)
	// SetBotAvatar upserts a bot's avatars doc (by _id).
	SetBotAvatar(ctx context.Context, av *model.Avatar) error
}

// emojiStore is the custom-emoji data access this service needs. It reads and
// writes the site-local custom_emojis collection — the same collection the
// pkg/emoji reaction validator (history-service) reads existence from.
type emojiStore interface {
	// EmojiDoc returns one emoji doc projecting only what the serve path needs
	// (minioKey, etag). found=false when the emoji is not registered.
	EmojiDoc(ctx context.Context, siteID, shortcode string) (*model.CustomEmoji, bool, error)
	// ListEmojis returns a site's emoji sorted by shortcode, projecting only
	// the wire fields (shortcode, imageUrl, contentType, etag, updatedAt).
	ListEmojis(ctx context.Context, siteID string) ([]model.CustomEmoji, error)
	// UpsertEmoji inserts or overwrites by (siteId, shortcode). createdBy and
	// createdAt are set on insert only; all blob fields update on overwrite.
	UpsertEmoji(ctx context.Context, e *model.CustomEmoji) error
	// DeleteEmoji removes the doc and returns its minioKey so the caller can
	// clean up the blob. found=false when no such emoji exists.
	DeleteEmoji(ctx context.Context, siteID, shortcode string) (minioKey string, found bool, err error)
}
