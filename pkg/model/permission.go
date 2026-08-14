package model

import "time"

// PermissionKey identifies a whitelist permission.
type PermissionKey string

// PermissionExternalImageView gates viewing images from outside the corporate network.
const PermissionExternalImageView PermissionKey = "external.image.view"

const MaxReasonRunes = 1000

// KnownPermission reports whether k is a recognized permission key. Derived from
// PermissionFieldName so the closed set is declared in exactly one place.
func KnownPermission(k PermissionKey) bool {
	_, ok := PermissionFieldName(k)
	return ok
}

// PermissionGrant is one row in the permission_grants append-only ledger:
// insert-only, never updated or deleted. It is provenance only — the current
// decision is the PermissionState materialized on the user document.
type PermissionGrant struct {
	ID               string        `json:"id"                      bson:"_id"`
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

// PermissionState is the latest admin decision for one permission key, materialized on
// the user document and replicated to every site. UpdatedAt is the write's RecordedAt
// and doubles as the per-key last-write-wins watermark.
type PermissionState struct {
	Granted       bool       `json:"granted"                 bson:"granted"`
	EffectiveFrom *time.Time `json:"effectiveFrom,omitempty" bson:"effectiveFrom,omitempty"`
	ExpiresAt     *time.Time `json:"expiresAt,omitempty"     bson:"expiresAt,omitempty"`
	UpdatedAt     time.Time  `json:"updatedAt"               bson:"updatedAt"`
}

// Evaluate reports whether the permission is effective at now. A nil state, a revoked
// state, or a grant missing either bound (malformed data) is false — deny, never panic.
// now == EffectiveFrom is inside the window; now == ExpiresAt is outside (half-open).
func (s *PermissionState) Evaluate(now time.Time) bool {
	if s == nil || !s.Granted {
		return false
	}
	if s.EffectiveFrom == nil || s.ExpiresAt == nil {
		return false
	}
	return !now.Before(*s.EffectiveFrom) && now.Before(*s.ExpiresAt)
}

// UserPermissions is the admin-managed permission snapshot on the user document — one
// named field per known PermissionKey. Deliberately a struct, not a map: the key
// "external.image.view" contains dots, and MongoDB dot-path updates cannot address map
// keys containing dots.
type UserPermissions struct {
	ExternalImageView *PermissionState `json:"externalImageView,omitempty" bson:"externalImageView,omitempty"`
}

// Evaluated returns the evaluated boolean for every known permission key. Nil-receiver
// safe: no snapshot means every key is false. settings.get and the admin GET's
// currentlyGranted both call this, so the entry points cannot disagree.
func (p *UserPermissions) Evaluated(now time.Time) map[PermissionKey]bool {
	return map[PermissionKey]bool{
		PermissionExternalImageView: p.State(PermissionExternalImageView).Evaluate(now),
	}
}

// State returns the stored state for the given key, nil when absent or unknown.
func (p *UserPermissions) State(k PermissionKey) *PermissionState {
	if p == nil {
		return nil
	}
	if k == PermissionExternalImageView {
		return p.ExternalImageView
	}
	return nil
}

// PermissionFieldName maps a PermissionKey to its bson field name inside the
// `permissions` sub-document ("external.image.view" → "externalImageView"); false for
// unknown keys. The two guarded-update sites (admin-service store, inbox-worker store)
// build their `permissions.<field>` dot-paths from this.
func PermissionFieldName(k PermissionKey) (string, bool) {
	if k == PermissionExternalImageView {
		return "externalImageView", true
	}
	return "", false
}
