package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// session is the one-doc-per-token record owned by botplatform-service in the
// chat DB. Sessions are permanent (no TTL); cap eviction is FIFO on issuedAt.
type session struct {
	// ID is base64(sha256(rawToken)) — the primary lookup key for validate.
	ID       string   `bson:"_id"`
	UserID   string   `bson:"userId"`
	Account  string   `bson:"account"`
	SiteID   string   `bson:"siteId"`
	Roles    []string `bson:"roles"`
	IssuedAt int64    `bson:"issuedAt"`
}

// BotplatformStore is the narrow Mongo surface botplatform-service needs. Each
// method touches exactly one collection: users (read-only) or sessions (RW).
type BotplatformStore interface {
	// FindUserByAccount loads a user by users.account. The returned User
	// carries Services.Password.Bcrypt so the handler can verify the
	// password locally. Returns (nil, mongo.ErrNoDocuments) when the
	// account does not exist.
	FindUserByAccount(ctx context.Context, account string) (*model.User, error)

	// InsertSession writes a new session row keyed by s.ID.
	InsertSession(ctx context.Context, s *session) error

	// FindSessionByHash loads a session by its hashed ID (validate path).
	// Returns (nil, mongo.ErrNoDocuments) when the hash is unknown.
	FindSessionByHash(ctx context.Context, hash string) (*session, error)

	// DeleteSessionsBeyondCap drops every session row for userID beyond the
	// `cap` newest, ordered by issuedAt DESC. Implemented in Mongo as
	// Find(sort=issuedAt desc).Skip(cap) -> DeleteMany, which costs one
	// round-trip on the common under-cap case (find returns nothing) and
	// two on the over-cap case. Returns the number actually deleted.
	DeleteSessionsBeyondCap(ctx context.Context, userID string, cap int) (int64, error)

	// Ping is the readiness probe target.
	Ping(ctx context.Context) error
}
