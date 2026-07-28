package main

import (
	"context"
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// resolvedSender is the nextgen identity a Teams sender or mention maps onto.
type resolvedSender struct {
	Account     string
	UserID      string
	EngName     string
	ChineseName string
	DisplayName string // render-ready, composed once here
}

// identityResolver maps a Teams user (graph id + display name) onto a nextgen
// identity. Split out so the DefaultTransformer can be tested without a store.
type identityResolver interface {
	resolve(ctx context.Context, teamsUserID, displayName string) (resolvedSender, error)
}

// HRIdentityStore is the identity read/write surface the sender resolver needs
// (satisfied by mongoHRIdentityStore). Declared in the consumer per convention.
type HRIdentityStore interface {
	// AccountByTeamsID maps an AAD user object id to its account via teams_user; "" when absent.
	AccountByTeamsID(ctx context.Context, teamsUserID string) (string, error)
	// FindUserByAccount returns the single user with account (globally unique), or (nil,nil).
	FindUserByAccount(ctx context.Context, account string) (*model.User, error)
}

// senderResolver reuses the #70 HR store to map Teams users to nextgen identities.
// The cache is process-wide (injected), shared across every batch the handler runs.
type senderResolver struct {
	store        HRIdentityStore
	siteID       string
	cache        *lru.Cache[string, resolvedSender]
	publishUsers func(ctx context.Context, users []model.IUserWithChange) error
}

func newSenderResolver(store HRIdentityStore, siteID string, cache *lru.Cache[string, resolvedSender], publishUsers func(ctx context.Context, users []model.IUserWithChange) error) *senderResolver {
	return &senderResolver{store: store, siteID: siteID, cache: cache, publishUsers: publishUsers}
}

// resolve order: (1) map the AAD id to its account via teams_user — the globally-unique
// key HR-synced users share; (2) read the user by account, reusing it untouched when found;
// (3) else create a keyless external user (no employeeId) keyed by account. A missing
// teams_user record errors — there's no account to migrate the sender onto.
func (r *senderResolver) resolve(ctx context.Context, teamsUserID, displayName string) (resolvedSender, error) {
	if teamsUserID == "" {
		return resolvedSender{}, fmt.Errorf("empty teams user id")
	}
	if s, ok := r.cache.Get(teamsUserID); ok {
		return s, nil
	}

	account, err := r.store.AccountByTeamsID(ctx, teamsUserID)
	if err != nil {
		return resolvedSender{}, fmt.Errorf("resolve account: %w", err)
	}
	if account == "" {
		return resolvedSender{}, fmt.Errorf("no teams_user account for %q", teamsUserID)
	}

	u, err := r.store.FindUserByAccount(ctx, account)
	if err != nil {
		return resolvedSender{}, fmt.Errorf("find by account: %w", err)
	}
	if u != nil {
		s := senderFromUser(u)
		r.cache.Add(teamsUserID, s)
		return s, nil
	}

	// New external user: the publisher owns a deterministic _id (Graph-id hash) so
	// this person is one identity at every site (hr-sync-worker keys on account).
	// No employeeId — externals aren't employees. displayName lands in chineseName
	// to mirror the HR mapping. Nothing is written locally first, so a redelivery
	// re-derives the same _id instead of splitting the user.
	id := idgen.DeterministicID([]byte(teamsUserID))
	nu := model.User{ID: id, Account: account, SiteID: r.siteID, ChineseName: displayName}
	iuc := model.IUserWithChange{User: nu, ChangeType: model.IChangeTypeNewHire}
	if err := r.publishUsers(ctx, []model.IUserWithChange{iuc}); err != nil {
		return resolvedSender{}, fmt.Errorf("publish user identity fanout: %w", err)
	}
	s := senderFromUser(&nu)
	r.cache.Add(teamsUserID, s)
	return s, nil
}

func senderFromUser(u *model.User) resolvedSender {
	return resolvedSender{
		Account:     u.Account,
		UserID:      u.ID,
		EngName:     u.EngName,
		ChineseName: u.ChineseName,
		DisplayName: displayfmt.CombineWithFallback(u.EngName, u.ChineseName, u.Account),
	}
}
