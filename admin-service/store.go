package main

import (
	"context"
	"errors"

	"github.com/hmchangw/chat/pkg/model"
)

var (
	ErrUserNotFound  = errors.New("user not found")
	ErrAccountExists = errors.New("account exists")
)

// UserUpdate carries optional account-management edits (nil = leave unchanged).
type UserUpdate struct {
	EngName     *string
	ChineseName *string
	Roles       *[]model.UserRole
	Deactivated *bool
}

// AuditEntry records one mutating admin action. Details holds non-secret context
// only — never passwords, hashes, or tokens.
type AuditEntry struct {
	ID            string            `json:"id"            bson:"_id"`
	ActorUserID   string            `json:"actorUserId"   bson:"actorUserId"`
	ActorAccount  string            `json:"actorAccount"  bson:"actorAccount"`
	Action        string            `json:"action"        bson:"action"`
	TargetUserID  string            `json:"targetUserId,omitempty"  bson:"targetUserId,omitempty"`
	TargetAccount string            `json:"targetAccount,omitempty" bson:"targetAccount,omitempty"`
	Details       map[string]string `json:"details,omitempty"       bson:"details,omitempty"`
	SiteID        string            `json:"siteId"        bson:"siteId"`
	Timestamp     int64             `json:"timestamp"     bson:"timestamp"`
}

// AuditFilter narrows an audit listing; zero-value fields are ignored.
type AuditFilter struct {
	TargetAccount string
	Actor         string
	Action        string
}

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

type AdminStore interface {
	SearchUsers(ctx context.Context, siteID, q string, page, limit int) ([]model.User, int64, error)
	GetUserByAccount(ctx context.Context, siteID, account string) (*model.User, error)
	// GetUserForAuth loads a user for password-verification paths (login and
	// self-service change-password). Returns credential fields (services.password.bcrypt,
	// roles, deactivated, requirePasswordChange, id, siteId, account) — the ONLY
	// reads of the bcrypt hash in this service. Never call from admin management
	// endpoints; those must use GetUserByAccount which scrubs the hash.
	GetUserForAuth(ctx context.Context, siteID, account string) (*model.User, error)
	CreateUser(ctx context.Context, u *model.User) error
	UpdateUser(ctx context.Context, siteID, account string, fields UserUpdate) error

	// UpdateUserPasswordAndRevoke atomically updates the user's bcrypt hash +
	// requirePasswordChange flag AND deletes matching sessions for that account.
	// If exceptSessionID is non-empty, sessions with that _id survive (used by
	// self-service change-password to keep the caller logged in). If empty, ALL
	// sessions for the account are deleted (used by admin setPassword). Both
	// writes run in a single Mongo transaction — requires a replica set.
	UpdateUserPasswordAndRevoke(ctx context.Context, siteID, account, bcryptHash string, requireChange bool, exceptSessionID string) error

	// DeactivateAndRevoke atomically sets deactivated=true on the user AND
	// deletes every session for the account. Runs in one Mongo transaction.
	// Called only for the deactivate branch of updateUser; other UpdateUser
	// patches (name/roles) stay non-transactional.
	DeactivateAndRevoke(ctx context.Context, siteID, account string) error

	AppendAudit(ctx context.Context, e *AuditEntry) error
	ListAudit(ctx context.Context, siteID string, f AuditFilter, page, limit int) ([]AuditEntry, int64, error)

	EnsureIndexes(ctx context.Context) error
	Ping(ctx context.Context) error
}
