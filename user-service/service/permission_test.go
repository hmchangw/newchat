package service

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/models"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// newPermissionSvc builds a UserService exposing the permissions mock; other
// deps are mocked but unused by GetPermission.
func newPermissionSvc(t *testing.T) (*UserService, *mocks.MockPermissionRepository) {
	t.Helper()
	ctrl := gomock.NewController(t)
	permissions := mocks.NewMockPermissionRepository(ctrl)
	cfg := &config.Config{SiteID: "site-a"}
	svc := New(
		mocks.NewMockSubscriptionRepository(ctrl), mocks.NewMockUserRepository(ctrl), mocks.NewMockAppRepository(ctrl),
		mocks.NewMockThreadSubscriptionRepository(ctrl), mocks.NewMockRoomClient(ctrl),
		mocks.NewMockHistoryClient(ctrl), mocks.NewMockPresenceClient(ctrl),
		mocks.NewMockEventPublisher(ctrl), mocks.NewMockEventPublisher(ctrl),
		mocks.NewMockSSOTokenRepository(ctrl), mocks.NewMockTokenValidator(ctrl), mocks.NewMockTokenRefresher(ctrl),
		permissions, cfg,
	)
	return svc, permissions
}

func TestGetPermission_GrantedActive(t *testing.T) {
	svc, permissions := newPermissionSvc(t)
	from := time.Now().Add(-1 * time.Hour)
	until := time.Now().Add(1 * time.Hour)
	permissions.EXPECT().GetLatestGrant(gomock.Any(), "site-a", model.PermissionExternalImageView, "alice").
		Return(&model.PermissionGrant{Granted: true, EffectiveFrom: &from, ExpiresAt: &until}, nil)

	resp, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "external.image.view"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Granted)
	assert.Equal(t, "external.image.view", resp.Permission, "response must echo the requested key")
}

func TestGetPermission_NoRecord(t *testing.T) {
	svc, permissions := newPermissionSvc(t)
	permissions.EXPECT().GetLatestGrant(gomock.Any(), "site-a", model.PermissionExternalImageView, "alice").
		Return(nil, nil)

	resp, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "external.image.view"})
	require.NoError(t, err)
	assert.False(t, resp.Granted)
	assert.Equal(t, "external.image.view", resp.Permission)
}

// TestGetPermission_NotCurrentlyGranted covers expired, not-yet-effective,
// revoked, and malformed (bounds absent — must fail closed, never panic) rows.
// The handler never has a fixed "now" to test against (it calls time.Now()
// directly), so fixtures use generous ±1h/±2h offsets from the real clock.
func TestGetPermission_NotCurrentlyGranted(t *testing.T) {
	past := time.Now().Add(-2 * time.Hour)
	justPast := time.Now().Add(-1 * time.Hour)
	future := time.Now().Add(1 * time.Hour)
	farFuture := time.Now().Add(2 * time.Hour)

	tests := map[string]*model.PermissionGrant{
		"expired":           {Granted: true, EffectiveFrom: &past, ExpiresAt: &justPast},
		"not yet effective": {Granted: true, EffectiveFrom: &future, ExpiresAt: &farFuture},
		"revoked":           {Granted: false},
		"effectiveFrom absent (malformed, fail closed)": {Granted: true, ExpiresAt: &future},
		"expiresAt absent (malformed, fail closed)":     {Granted: true, EffectiveFrom: &past},
	}
	for name, grant := range tests {
		t.Run(name, func(t *testing.T) {
			svc, permissions := newPermissionSvc(t)
			permissions.EXPECT().GetLatestGrant(gomock.Any(), "site-a", model.PermissionExternalImageView, "alice").
				Return(grant, nil)
			resp, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "external.image.view"})
			require.NoError(t, err)
			assert.False(t, resp.Granted)
		})
	}
}

func TestGetPermission_UnknownPermissionKey(t *testing.T) {
	svc, _ := newPermissionSvc(t)
	// No GetLatestGrant EXPECT set: an unexpected call would fail the gomock
	// controller, proving validation runs before the repo is ever touched.
	_, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "not.a.real.permission"})
	requireCode(t, err, errcode.CodeBadRequest)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.PermissionUnknownKey, ee.Reason)
}

func TestGetPermission_RepoError(t *testing.T) {
	svc, permissions := newPermissionSvc(t)
	permissions.EXPECT().GetLatestGrant(gomock.Any(), "site-a", model.PermissionExternalImageView, "alice").
		Return(nil, errors.New("mongo down"))

	_, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "external.image.view"})
	// Raw wrapped error — classified to the generic boundary code by the router, not here.
	require.Error(t, err)
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee), "store errors must stay raw, not pre-classified")
}

func TestGetPermission_IgnoresAccountInRequestBody(t *testing.T) {
	svc, permissions := newPermissionSvc(t)
	// PermissionGetRequest (models/permission.go) carries no account field at all.
	// gomock's EXACT-argument match on subjectAccount=="alice" — the ctx()-derived
	// account, not anything from the body — IS the assertion that the handler
	// reads identity only from c.Param("account").
	permissions.EXPECT().GetLatestGrant(gomock.Any(), "site-a", model.PermissionExternalImageView, "alice").
		Return(&model.PermissionGrant{}, nil)

	resp, err := svc.GetPermission(ctx("alice", "site-a"), models.PermissionGetRequest{Permission: "external.image.view"})
	require.NoError(t, err)
	assert.False(t, resp.Granted)
}
