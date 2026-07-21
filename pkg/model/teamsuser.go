package model

import "time"

// TeamsUser is the persisted teams_user collection document: a Teams (Azure
// AD) user joined with the HR system's site assignment (derived from the HR
// locationURL), English name, and mail. Written by teams-user-sync; readable
// by any service that needs the mapping. teams-chat-sync additionally writes
// From — its per-user chat-sync watermark, advanced to startOfDay(now, UTC)
// after a fully successful sync of that user.
type TeamsUser struct {
	// ID is the Teams (Azure AD) user object id.
	ID string `json:"id" bson:"_id"`
	// UPN is the user's userPrincipalName as returned by Graph.
	UPN string `json:"upn" bson:"upn"`
	// Account is the lowercased UPN local part (text before '@') — the value
	// matched against hr.accountName.
	Account string `json:"account" bson:"account"`
	// DisplayName is the user's display name as returned by Graph.
	DisplayName string `json:"displayName" bson:"displayName"`
	// SiteID is the HR system's site id for the account.
	SiteID string `json:"siteId" bson:"siteId"`
	// EngName is the HR system's English name for the account; empty when the
	// account had no hr row at sync time.
	EngName string `json:"engName" bson:"engName"`
	// Mail is the HR system's mail address for the account; empty when the
	// account had no hr row at sync time.
	Mail string `json:"mail" bson:"mail"`
	// From is teams-chat-sync's watermark; absent until the user's first
	// successful chat sync.
	From *time.Time `json:"from,omitempty" bson:"from,omitempty"`
}
