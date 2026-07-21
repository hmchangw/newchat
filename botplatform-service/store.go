package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// BotplatformStore is the narrow Mongo surface botplatform-service needs.
// Session persistence lives behind session.Store, wired at construction.
type BotplatformStore interface {
	// FindUserByAccount loads a user by users.account. The returned User
	// carries Services.Password.Bcrypt so the handler can verify the
	// password locally. Returns (nil, mongo.ErrNoDocuments) when the
	// account does not exist.
	FindUserByAccount(ctx context.Context, account string) (*model.User, error)

	// Session operations are delegated to a pkg/session.Store composed in.

	// InsertSession writes a new session row keyed by s.ID.
	InsertSession(ctx context.Context, s *session.Session) error

	// FindSessionByHash loads a session by its hashed ID (validate path).
	// Returns (nil, session.ErrNotFound) when the hash is unknown.
	FindSessionByHash(ctx context.Context, hash string) (*session.Session, error)

	// DeleteSessionsBeyondCap drops every session row for account beyond the
	// `max` newest, ordered by issuedAt DESC. Returns the number deleted.
	DeleteSessionsBeyondCap(ctx context.Context, account string, max int) (int64, error)

	// Ping is the readiness probe target.
	Ping(ctx context.Context) error
}
