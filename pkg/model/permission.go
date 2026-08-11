package model

import "time"

// PermissionKey identifies a whitelist permission.
type PermissionKey string

// PermissionExternalImageView gates viewing images from outside the corporate network.
const PermissionExternalImageView PermissionKey = "external.image.view"

const (
	MaxReasonRunes = 1000
	MaxSubjects    = 200
)

// knownPermissions is the closed set of recognized permission keys.
var knownPermissions = map[PermissionKey]bool{
	PermissionExternalImageView: true,
}

// KnownPermission reports whether k is a recognized permission key.
func KnownPermission(k PermissionKey) bool {
	return knownPermissions[k]
}

// PermissionGrant is one row in the permission_grants append-only ledger:
// insert-only, never updated or deleted. The current decision is not
// stored — EvaluateGrant computes it from the newest row for
// (SiteID, Permission, SubjectAccount).
type PermissionGrant struct {
	ID               string        `json:"id"                      bson:"_id"`
	SiteID           string        `json:"siteId"                  bson:"siteId"`
	Permission       PermissionKey `json:"permission"              bson:"permission"`
	SubjectAccount   string        `json:"subjectAccount"          bson:"subjectAccount"`
	Granted          bool          `json:"granted"                 bson:"granted"`
	EffectiveFrom    *time.Time    `json:"effectiveFrom,omitempty" bson:"effectiveFrom,omitempty"`
	ExpiresAt        *time.Time    `json:"expiresAt,omitempty"     bson:"expiresAt,omitempty"`
	ApplicantAccount string        `json:"applicantAccount"        bson:"applicantAccount"`
	ApproverAccount  string        `json:"approverAccount"         bson:"approverAccount"`
	Reason           string        `json:"reason"                  bson:"reason"`
	RecordedBy       string        `json:"recordedBy"              bson:"recordedBy"`
	RecordedAt       time.Time     `json:"recordedAt"              bson:"recordedAt"`
}

// EvaluateGrant reports whether latest currently grants the permission at
// instant now. This is the single "latest-wins" decision function every
// evaluation site shares, so two callers can never disagree.
func EvaluateGrant(latest *PermissionGrant, now time.Time) bool {
	if latest == nil || !latest.Granted {
		return false
	}
	// A grant row should always carry both bounds. If the data is malformed
	// (manual DB edit, an older bug), deny — never panic, never allow.
	if latest.EffectiveFrom == nil || latest.ExpiresAt == nil {
		return false
	}
	return !now.Before(*latest.EffectiveFrom) && now.Before(*latest.ExpiresAt)
}
