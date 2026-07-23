package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/session"
)

// fakeSessionStore is a hand-rolled session.Store double; unset fields panic loudly.
type fakeSessionStore struct {
	InsertFn                 func(ctx context.Context, s *session.Session) error
	FindByHashFn             func(ctx context.Context, hash string) (*session.Session, error)
	DeleteBeyondCapFn        func(ctx context.Context, account string, max int) (int64, error)
	DeleteForAccountExceptFn func(ctx context.Context, siteID, account, exceptID string) (int64, error)
	DeleteForAccountFn       func(ctx context.Context, siteID, account string) (int64, error)
	ListForAccountFn         func(ctx context.Context, siteID, account string) ([]session.Session, error)
	DeleteByIDFn             func(ctx context.Context, siteID, account, id string) (int64, error)
	EnsureIndexesFn          func(ctx context.Context) error
}

func (f *fakeSessionStore) Insert(ctx context.Context, s *session.Session) error {
	return f.InsertFn(ctx, s)
}

func (f *fakeSessionStore) FindByHash(ctx context.Context, hash string) (*session.Session, error) {
	return f.FindByHashFn(ctx, hash)
}

func (f *fakeSessionStore) DeleteBeyondCap(ctx context.Context, account string, max int) (int64, error) {
	return f.DeleteBeyondCapFn(ctx, account, max)
}

func (f *fakeSessionStore) DeleteForAccountExcept(ctx context.Context, siteID, account, exceptID string) (int64, error) {
	return f.DeleteForAccountExceptFn(ctx, siteID, account, exceptID)
}

func (f *fakeSessionStore) DeleteForAccount(ctx context.Context, siteID, account string) (int64, error) {
	return f.DeleteForAccountFn(ctx, siteID, account)
}

func (f *fakeSessionStore) ListForAccount(ctx context.Context, siteID, account string) ([]session.Session, error) {
	return f.ListForAccountFn(ctx, siteID, account)
}

func (f *fakeSessionStore) DeleteByID(ctx context.Context, siteID, account, id string) (int64, error) {
	return f.DeleteByIDFn(ctx, siteID, account, id)
}

func (f *fakeSessionStore) EnsureIndexes(ctx context.Context) error {
	if f.EnsureIndexesFn == nil {
		return nil
	}
	return f.EnsureIndexesFn(ctx)
}
