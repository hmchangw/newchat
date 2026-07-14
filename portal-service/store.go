package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// employee is one row of the load-time directory built from the users
// collection (primary — every provisioned account, incl. bot/admin) left-
// joined against the HR-owned hr_employee collection (enrichment — human
// accounts only): an account's home site, canonical userId, and the user's
// roles. hr_employee is rewritten by a daily HR sync cron; the portal reads
// users, left-joins hr_employee onto it, and enforces a unique account index
// on hr_employee at startup. Bot/admin accounts have no hr_employee row, so
// EmployeeID comes back empty for them. NATS coordinates are a site-level
// property (see siteURL in handler.go), not per-account, so they don't live
// here.
type employee struct {
	Account    string `json:"account"    bson:"account"`
	EmployeeID string `json:"employeeId" bson:"employeeId"`
	SiteID     string `json:"siteId"     bson:"siteId"`
	// UserID is users._id, projected directly since users is now the primary
	// collection. Held in memory so the portal needs no per-request users
	// query; not returned to the client.
	UserID string `json:"userId" bson:"userId"`
	// Roles is the user's role slice, projected directly from users.roles.
	// Drives the /api/v1/login role gate (only bot/admin may password-login)
	// and the /api/userInfo role-aware response shape.
	Roles []model.UserRole `json:"roles,omitempty" bson:"roles,omitempty"`
}

// DirectoryStore reads the users-primary directory (left-joined with
// hr_employee for human enrichment fields) that backs the in-memory cache.
type DirectoryStore interface {
	ListEmployees(ctx context.Context) ([]employee, error)
	// GetByAccount reads a single account's directory entry live from the
	// users collection. It is the /api/v1/login fallback for a cache miss so
	// an account provisioned since the last periodic refresh can still log in
	// without waiting for the next refresh (or a restart). Returns
	// (_, false, nil) when the account does not exist.
	GetByAccount(ctx context.Context, account string) (employee, bool, error)
}
