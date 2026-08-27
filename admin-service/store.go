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
	Active      *bool
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
	SearchUsers(ctx context.Context, q string, page, limit int) ([]model.User, int64, error)
	GetUserByAccount(ctx context.Context, siteID, account string) (*model.User, error)
	// GetUserForAuth loads a user for password-verification paths (login and
	// self-service change-password). Returns credential fields (services.password.bcrypt,
	// roles, active, requirePasswordChange, id, siteId, account) — the ONLY
	// reads of the bcrypt hash in this service. Never call from admin management
	// endpoints; those must use GetUserByAccount which scrubs the hash.
	GetUserForAuth(ctx context.Context, siteID, account string) (*model.User, error)
	CreateUser(ctx context.Context, u *model.User) error

	// UpdateUser applies the non-nil patch fields. Returns the post-write doc
	// projected to the fanout fields; returns (nil, nil) when the patch is empty.
	UpdateUser(ctx context.Context, siteID, account string, fields UserUpdate) (*model.User, error)

	// UpdateUserPasswordAndRevoke atomically updates the user's bcrypt hash +
	// requirePasswordChange flag AND deletes matching sessions for that account.
	// If exceptSessionID is non-empty, sessions with that _id survive (used by
	// self-service change-password to keep the caller logged in). If empty, ALL
	// sessions for the account are deleted (used by admin setPassword). Both
	// writes run in a single Mongo transaction — requires a replica set.
	UpdateUserPasswordAndRevoke(ctx context.Context, siteID, account, bcryptHash string, requireChange bool, exceptSessionID string) error

	// DeactivateAndRevoke atomically sets active=false on the user AND
	// deletes every session for the account. Runs in one Mongo transaction.
	// Called only for the deactivate branch of updateUser; other UpdateUser
	// patches (name/roles) stay non-transactional.
	// Returns the post-write doc projected to the fanout fields.
	DeactivateAndRevoke(ctx context.Context, siteID, account string) (*model.User, error)

	// ListRooms returns the rooms homed at siteID, ordered by _id, projected to
	// the admin-console columns only. Also returns the unpaged match count.
	ListRooms(ctx context.Context, siteID string, page, limit int) ([]model.Room, int64, error)

	// ListRoomMembers returns every subscription for roomID, projected to the
	// account fields. Reads the same collection room-service's membership check
	// consults, so an account it returns is one the duty toggle will accept as
	// owner. Unpaged — a room's roster is bounded and callers want it whole.
	ListRoomMembers(ctx context.Context, roomID string) ([]model.Subscription, error)

	AppendAudit(ctx context.Context, e *AuditEntry) error
	ListAudit(ctx context.Context, siteID string, f AuditFilter, page, limit int) ([]AuditEntry, int64, error)

	// RecordPermissionChange appends the batch to the ledger and applies the derived
	// state to the subjects' user documents under the per-key watermark guard — one
	// transaction, all-or-nothing. Subject accounts are derived from the grants.
	RecordPermissionChange(ctx context.Context, grants []*model.PermissionGrant, state model.PermissionState) error

	// GetUserPermissions returns the account's materialized permission snapshot;
	// (nil, nil) when the user or snapshot does not exist. Site-unfiltered: subjects
	// may be homed at any site.
	GetUserPermissions(ctx context.Context, account string) (*model.UserPermissions, error)

	// GetUserPermissionsForAccounts returns the materialized snapshots for the given
	// accounts in one read; accounts with no user doc are absent from the map.
	// Site-unfiltered, like GetUserPermissions. Used by the resync endpoint, which only
	// re-delivers this snapshot — never writes.
	GetUserPermissionsForAccounts(ctx context.Context, accounts []string) (map[string]*model.UserPermissions, error)

	// ListPermissionGrants returns the ledger newest-first (recordedAt desc, _id
	// desc), company-wide (site-unfiltered — subjects may be homed at any site).
	// subjectAccount == "" means all subjects; permission == "" means all
	// permissions. The two filters are independent and any combination —
	// including both empty — is valid.
	ListPermissionGrants(ctx context.Context, subjectAccount string, permission model.PermissionKey, page, limit int) ([]model.PermissionGrant, int64, error)

	// FindAccountStates reports account -> IsActive company-wide (the local users
	// collection covers every site's users; subjects may be homed anywhere).
	FindAccountStates(ctx context.Context, accounts []string) (map[string]bool, error)

	// AppendAuditMany inserts all entries in one InsertMany. Best-effort contract same
	// as AppendAudit (caller logs, never fails the request).
	AppendAuditMany(ctx context.Context, entries []*AuditEntry) error

	EnsureIndexes(ctx context.Context) error
	Ping(ctx context.Context) error
}
