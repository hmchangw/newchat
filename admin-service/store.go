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

// Session mirrors the botplatform-issued session row (read/write here).
type Session struct {
	ID       string   `bson:"_id"`
	UserID   string   `bson:"userId"`
	Account  string   `bson:"account"`
	SiteID   string   `bson:"siteId"`
	Roles    []string `bson:"roles"`
	IssuedAt int64    `bson:"issuedAt"`
}

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
	CreateUser(ctx context.Context, u *model.User) error
	UpdateUser(ctx context.Context, siteID, account string, fields UserUpdate) error
	UpdateUserPassword(ctx context.Context, siteID, account, bcryptHash string, requireChange bool) error

	FindSessionByHash(ctx context.Context, hash string) (*Session, error)
	ListSessionsByAccount(ctx context.Context, siteID, account string) ([]Session, error)
	DeleteSessionsByAccount(ctx context.Context, siteID, account string) (int64, error)
	DeleteSession(ctx context.Context, siteID, account, sessionID string) (int64, error)

	AppendAudit(ctx context.Context, e *AuditEntry) error
	ListAudit(ctx context.Context, siteID string, f AuditFilter, page, limit int) ([]AuditEntry, int64, error)

	EnsureIndexes(ctx context.Context) error
	Ping(ctx context.Context) error
}
