package main

import (
	"context"
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/teamsmigrate"
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
	// FindUserByEmployeeId returns the single user with employeeId (globally unique), or (nil,nil).
	FindUserByEmployeeId(ctx context.Context, employeeId string) (*model.User, error)
	// FindUserByDisplayName returns the single user whose display name matches;
	// (nil,nil) when zero or many match (ambiguous).
	FindUserByDisplayName(ctx context.Context, name string) (*model.User, error)
	// UpsertUserIdentities $sets IDENTITY FIELDS ONLY (account, siteId, engName,
	// chineseName, employeeId); it never touches roles/services/password.
	UpsertUserIdentities(ctx context.Context, users []model.IUserWithChange) error
}

// senderResolver reuses the #70 HR store to map Teams users to nextgen identities.
// The cache is process-wide (injected), shared across every batch the handler runs.
type senderResolver struct {
	store  HRIdentityStore
	siteID string
	cache  *lru.Cache[string, resolvedSender]
}

func newSenderResolver(store HRIdentityStore, siteID string, cache *lru.Cache[string, resolvedSender]) *senderResolver {
	return &senderResolver{store: store, siteID: siteID, cache: cache}
}

// resolve order: (1) read by employeeId — the authoritative key the HR sync shares,
// so an existing (synced or prior-migrated) user is reused without touching its fields;
// (2) else a unique display-name match (fuzzy fallback); (3) else create via the
// employeeId-keyed upsert. Reaching (3) only for genuinely-new users means the upsert
// never overwrites an existing identity.
func (r *senderResolver) resolve(ctx context.Context, teamsUserID, displayName string) (resolvedSender, error) {
	if teamsUserID == "" {
		return resolvedSender{}, fmt.Errorf("empty teams user id")
	}
	if s, ok := r.cache.Get(teamsUserID); ok {
		return s, nil
	}

	empID := teamsmigrate.EmployeeIDFromGraphID(teamsUserID)
	u, err := r.store.FindUserByEmployeeId(ctx, empID)
	if err != nil {
		return resolvedSender{}, fmt.Errorf("find by employeeId: %w", err)
	}
	if u == nil && displayName != "" {
		if u, err = r.store.FindUserByDisplayName(ctx, displayName); err != nil {
			return resolvedSender{}, fmt.Errorf("find by display name: %w", err)
		}
	}
	if u != nil {
		s := senderFromUser(u)
		r.cache.Add(teamsUserID, s)
		return s, nil
	}

	// Create: no UPN exists at the message layer, so account = employeeId; displayName
	// lands in chineseName to mirror the HR mapping (teams-hr-sync writes it there).
	nu := model.User{Account: empID, SiteID: r.siteID, EmployeeID: empID, ChineseName: displayName}
	if err := r.store.UpsertUserIdentities(ctx, []model.IUserWithChange{{User: nu}}); err != nil {
		return resolvedSender{}, fmt.Errorf("upsert user identity: %w", err)
	}
	// Read back so the sender carries the UserID the upsert set (_id = employeeId),
	// matching the found path; a nil read-back is defensive-only (the row was just written).
	created, err := r.store.FindUserByEmployeeId(ctx, empID)
	if err != nil {
		return resolvedSender{}, fmt.Errorf("read back created identity: %w", err)
	}
	var s resolvedSender
	if created != nil {
		s = senderFromUser(created)
	} else {
		s = resolvedSender{Account: empID, ChineseName: displayName, DisplayName: displayfmt.CombineWithFallback("", displayName, empID)}
	}
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
