package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/session"
)

// sessionOnlyStore is a BotplatformStore that answers only the session lookup —
// the one method requireBot uses. The embedded nil interface makes any other
// call panic, so a test can't silently depend on more of the store than the
// auth chain actually touches.
type sessionOnlyStore struct {
	BotplatformStore
	FindSessionByHashFn func(ctx context.Context, hash string) (*session.Session, error)
}

func (f *sessionOnlyStore) FindSessionByHash(ctx context.Context, hash string) (*session.Session, error) {
	return f.FindSessionByHashFn(ctx, hash)
}
