//go:build integration

package mongorepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestPermissionRepo_GetLatestGrant_EmptyCollection(t *testing.T) {
	r, _ := newTestPermissionRepo(t)
	got, err := r.GetLatestGrant(context.Background(), "site-a", model.PermissionExternalImageView, "alice")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestPermissionRepo_GetLatestGrant_SingleGrantRow_ProjectionTrimsOtherFields(t *testing.T) {
	r, db := newTestPermissionRepo(t)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	recordedAt := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	seed(t, db, "permission_grants", model.PermissionGrant{
		ID:               "id-1",
		SiteID:           "site-a",
		Permission:       model.PermissionExternalImageView,
		SubjectAccount:   "alice",
		Granted:          true,
		EffectiveFrom:    &from,
		ExpiresAt:        &until,
		ApplicantAccount: "carol",
		ApproverAccount:  "dave",
		Reason:           "On-call staff must review production line photos from outside the fab.",
		RecordedBy:       "p_admin_wang",
		RecordedAt:       recordedAt,
	})

	got, err := r.GetLatestGrant(context.Background(), "site-a", model.PermissionExternalImageView, "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Granted)
	require.NotNil(t, got.EffectiveFrom)
	assert.True(t, from.Equal(*got.EffectiveFrom))
	require.NotNil(t, got.ExpiresAt)
	assert.True(t, until.Equal(*got.ExpiresAt))
	// Only granted/effectiveFrom/expiresAt are projected — everything else must
	// come back zero-valued, proving the projection actually trims the document.
	// (Mongo still returns _id by default since the projection never excludes it
	// with "_id":0; that's fine, _id never reaches the wire — GetPermission's
	// response never echoes it.)
	assert.Empty(t, got.Reason, "projection must not return reason")
	assert.Empty(t, got.ApplicantAccount, "projection must not return applicantAccount")
	assert.Empty(t, got.ApproverAccount, "projection must not return approverAccount")
	assert.Empty(t, got.RecordedBy, "projection must not return recordedBy")
	assert.True(t, got.RecordedAt.IsZero(), "projection must not return recordedAt")
}

func TestPermissionRepo_GetLatestGrant_RevokeAfterGrant_ReturnsRevoke(t *testing.T) {
	r, db := newTestPermissionRepo(t)
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	seed(t, db, "permission_grants",
		model.PermissionGrant{
			ID: "id-grant", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted: true, EffectiveFrom: &from, ExpiresAt: &until,
			ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "initial grant", RecordedBy: "p_admin_wang",
			RecordedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		},
		model.PermissionGrant{
			ID: "id-revoke", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted:          false,
			ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "project ended", RecordedBy: "p_admin_wang",
			RecordedAt: time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC), // later than the grant
		},
	)

	got, err := r.GetLatestGrant(context.Background(), "site-a", model.PermissionExternalImageView, "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.False(t, got.Granted, "the later revoke row must win over the earlier grant")
	assert.Nil(t, got.EffectiveFrom)
	assert.Nil(t, got.ExpiresAt)
}

func TestPermissionRepo_GetLatestGrant_SameRecordedAt_HigherIDWins(t *testing.T) {
	r, db := newTestPermissionRepo(t)
	recordedAt := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC) // identical on both rows
	seed(t, db, "permission_grants",
		model.PermissionGrant{
			ID: "id-aaa", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted: false, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row a", RecordedBy: "p_admin_wang",
			RecordedAt: recordedAt,
		},
		model.PermissionGrant{
			ID: "id-bbb", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted: true, ApplicantAccount: "carol", ApproverAccount: "dave", Reason: "batch row b", RecordedBy: "p_admin_wang",
			RecordedAt: recordedAt,
		},
	)

	got, err := r.GetLatestGrant(context.Background(), "site-a", model.PermissionExternalImageView, "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "id-bbb", got.ID, "the higher _id must win the recordedAt tie-break")
	assert.True(t, got.Granted)
}

func TestPermissionRepo_GetLatestGrant_FilterIsolation(t *testing.T) {
	r, db := newTestPermissionRepo(t)
	target := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC) // the row that SHOULD be returned
	trap := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)  // later than target — would win if isolation failed
	seed(t, db, "permission_grants",
		// The row that must be returned.
		model.PermissionGrant{
			ID: "id-target", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted: true, RecordedAt: target,
		},
		// Same site+permission, different subject — must not leak into alice's answer.
		model.PermissionGrant{
			ID: "id-trap-subject", SiteID: "site-a", Permission: model.PermissionExternalImageView, SubjectAccount: "bob",
			Granted: false, RecordedAt: trap,
		},
		// Same permission+subject, different site.
		model.PermissionGrant{
			ID: "id-trap-site", SiteID: "site-b", Permission: model.PermissionExternalImageView, SubjectAccount: "alice",
			Granted: false, RecordedAt: trap,
		},
		// Same site+subject, different permission key.
		model.PermissionGrant{
			ID: "id-trap-permission", SiteID: "site-a", Permission: model.PermissionKey("other.permission"), SubjectAccount: "alice",
			Granted: false, RecordedAt: trap,
		},
	)

	got, err := r.GetLatestGrant(context.Background(), "site-a", model.PermissionExternalImageView, "alice")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "id-target", got.ID, "a row failing any one of siteId/permission/subjectAccount must never win, even with a newer recordedAt")
	assert.True(t, got.Granted)
}
