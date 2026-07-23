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
	"github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/models"
	"github.com/hmchangw/chat/user-service/service/mocks"
)

// newSSOSvc builds a UserService exposing the SSO-relevant mocks; other deps are mocked but unused by the sso handlers.
func newSSOSvc(t *testing.T) (*UserService, *mocks.MockUserRepository, *mocks.MockSSOTokenRepository, *mocks.MockTokenValidator, *mocks.MockTokenRefresher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	users := mocks.NewMockUserRepository(ctrl)
	ssoTokens := mocks.NewMockSSOTokenRepository(ctrl)
	validator := mocks.NewMockTokenValidator(ctrl)
	refresher := mocks.NewMockTokenRefresher(ctrl)
	cfg := &config.Config{SiteID: "site-a", SSORefreshWindow: time.Hour}
	svc := New(
		mocks.NewMockSubscriptionRepository(ctrl), users, mocks.NewMockAppRepository(ctrl),
		mocks.NewMockThreadSubscriptionRepository(ctrl), mocks.NewMockRoomClient(ctrl),
		mocks.NewMockHistoryClient(ctrl), mocks.NewMockPresenceClient(ctrl),
		mocks.NewMockEventPublisher(ctrl), mocks.NewMockEventPublisher(ctrl),
		ssoTokens, validator, refresher, cfg,
	)
	return svc, users, ssoTokens, validator, refresher
}

func TestSSOSet_HappyPath(t *testing.T) {
	svc, _, ssoTokens, validator, _ := newSSOSvc(t)
	exp := time.Now().Add(30 * time.Minute)
	validator.EXPECT().Validate(gomock.Any(), "access-tok").
		Return(oidc.Claims{PreferredUsername: "alice", Expiry: exp}, nil)
	ssoTokens.EXPECT().Upsert(gomock.Any(), "alice", "access-tok", exp.UnixMilli(), "refresh-tok").Return(nil)

	resp, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "access-tok", RefreshToken: "refresh-tok"})
	require.NoError(t, err)
	assert.True(t, resp.Success)
}

func TestSSOSet_MissingFields(t *testing.T) {
	svc, _, _, _, _ := newSSOSvc(t)
	for name, req := range map[string]models.SSOSetRequest{
		"no ssoToken":     {RefreshToken: "r"},
		"no refreshToken": {SSOToken: "a"},
		"neither":         {},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := svc.SSOSet(ctx("alice", "site-a"), req)
			requireCode(t, err, errcode.CodeBadRequest)
		})
	}
}

func TestSSOSet_ExpiredToken(t *testing.T) {
	svc, _, _, validator, _ := newSSOSvc(t)
	validator.EXPECT().Validate(gomock.Any(), "old").Return(oidc.Claims{}, oidc.ErrTokenExpired)
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "old", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeUnauthenticated)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.AuthTokenExpired, ee.Reason)
}

func TestSSOSet_InvalidToken(t *testing.T) {
	svc, _, _, validator, _ := newSSOSvc(t)
	validator.EXPECT().Validate(gomock.Any(), "junk").Return(oidc.Claims{}, errors.New("bad signature"))
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "junk", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeUnauthenticated)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.AuthInvalidToken, ee.Reason)
}

func TestSSOSet_TokenOwnerMismatch(t *testing.T) {
	svc, _, _, validator, _ := newSSOSvc(t)
	// The submitted token belongs to a different identity than the caller storing it.
	validator.EXPECT().Validate(gomock.Any(), "tok").
		Return(oidc.Claims{PreferredUsername: "mallory", Expiry: time.Now().Add(time.Hour)}, nil)
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "tok", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeBadRequest)
}

func TestSSOSet_StoreErrorIsInternal(t *testing.T) {
	svc, _, ssoTokens, validator, _ := newSSOSvc(t)
	validator.EXPECT().Validate(gomock.Any(), "tok").
		Return(oidc.Claims{PreferredUsername: "alice", Expiry: time.Now().Add(time.Hour)}, nil)
	ssoTokens.EXPECT().Upsert(gomock.Any(), "alice", "tok", gomock.Any(), "r").Return(errors.New("mongo down"))
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "tok", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeInternal)
}

func TestSSOSet_FeatureOffUnavailable(t *testing.T) {
	svc, _, _, _, _ := newSSOSvc(t)
	svc.tokenValidator = nil // simulate unset OIDC_ISSUER_URL
	_, err := svc.SSOSet(ctx("alice", "site-a"), models.SSOSetRequest{SSOToken: "a", RefreshToken: "r"})
	requireCode(t, err, errcode.CodeUnavailable)
}

func TestSSORefresh_FreshTokenReturnedUnchanged(t *testing.T) {
	svc, _, ssoTokens, _, _ := newSSOSvc(t)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(&model.SSOToken{
		Username: "alice", IDToken: "stored-access",
		IDTokenExp:   time.Now().Add(2 * time.Hour).UnixMilli(), // beyond 1h window
		RefreshToken: "stored-refresh",
	}, nil)

	resp, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	require.NoError(t, err)
	assert.Equal(t, "stored-access", resp.SSOToken)
}

func TestSSORefresh_WithinWindowRefreshes(t *testing.T) {
	svc, _, ssoTokens, _, refresher := newSSOSvc(t)
	newExp := time.Now().Add(30 * time.Minute)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(&model.SSOToken{
		Username: "alice", IDToken: "stale-access",
		IDTokenExp:   time.Now().Add(10 * time.Minute).UnixMilli(), // inside 1h window
		RefreshToken: "stored-refresh",
	}, nil)
	refresher.EXPECT().Refresh(gomock.Any(), "stored-refresh").
		Return(oidc.TokenSet{SSOToken: "new-access", RefreshToken: "rotated", Account: "alice", Expiry: newExp}, nil)
	ssoTokens.EXPECT().Upsert(gomock.Any(), "alice", "new-access", newExp.UnixMilli(), "rotated").Return(nil)

	resp, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	require.NoError(t, err)
	assert.Equal(t, "new-access", resp.SSOToken)
}

func TestSSORefresh_AlreadyExpiredRefreshes(t *testing.T) {
	svc, _, ssoTokens, _, refresher := newSSOSvc(t)
	newExp := time.Now().Add(30 * time.Minute)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(&model.SSOToken{
		Username: "alice", IDToken: "dead-access",
		IDTokenExp:   time.Now().Add(-time.Hour).UnixMilli(), // already expired
		RefreshToken: "stored-refresh",
	}, nil)
	refresher.EXPECT().Refresh(gomock.Any(), "stored-refresh").
		Return(oidc.TokenSet{SSOToken: "new-access", RefreshToken: "rotated", Account: "alice", Expiry: newExp}, nil)
	ssoTokens.EXPECT().Upsert(gomock.Any(), "alice", "new-access", newExp.UnixMilli(), "rotated").Return(nil)

	resp, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	require.NoError(t, err)
	assert.Equal(t, "new-access", resp.SSOToken)
}

func TestSSORefresh_RefreshFailureIsTokenExpired(t *testing.T) {
	svc, _, ssoTokens, _, refresher := newSSOSvc(t)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(&model.SSOToken{
		Username: "alice", IDToken: "x",
		IDTokenExp: 1, RefreshToken: "dead-refresh",
	}, nil)
	refresher.EXPECT().Refresh(gomock.Any(), "dead-refresh").
		Return(oidc.TokenSet{}, oidc.ErrRefreshRejected)

	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	requireCode(t, err, errcode.CodeUnauthenticated)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.AuthTokenExpired, ee.Reason)
}

func TestSSORefresh_NoStoredToken(t *testing.T) {
	svc, _, ssoTokens, _, _ := newSSOSvc(t)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(nil, nil)

	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	requireCode(t, err, errcode.CodeNotFound)
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.UserSSOTokenNotFound, ee.Reason)
}

func TestSSORefresh_StoreErrorIsInternal(t *testing.T) {
	svc, _, ssoTokens, _, _ := newSSOSvc(t)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(nil, errors.New("mongo down"))
	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	requireCode(t, err, errcode.CodeInternal)
}

func TestSSORefresh_FeatureOffUnavailable(t *testing.T) {
	svc, _, _, _, _ := newSSOSvc(t)
	svc.tokenRefresher = nil
	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	requireCode(t, err, errcode.CodeUnavailable)
}

func TestSSORefresh_PreservesRefreshTokenWhenResponseOmitsIt(t *testing.T) {
	svc, _, ssoTokens, _, refresher := newSSOSvc(t)
	newExp := time.Now().Add(30 * time.Minute)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(&model.SSOToken{
		Username: "alice", IDToken: "stale", IDTokenExp: 1, RefreshToken: "kept-refresh",
	}, nil)
	// IdP returns no refresh_token — the stored one must be preserved.
	refresher.EXPECT().Refresh(gomock.Any(), "kept-refresh").
		Return(oidc.TokenSet{SSOToken: "new-access", RefreshToken: "", Account: "alice", Expiry: newExp}, nil)
	ssoTokens.EXPECT().Upsert(gomock.Any(), "alice", "new-access", newExp.UnixMilli(), "kept-refresh").Return(nil)

	resp, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	require.NoError(t, err)
	assert.Equal(t, "new-access", resp.SSOToken)
}

func TestSSORefresh_RefreshedTokenOwnerMismatch(t *testing.T) {
	svc, _, ssoTokens, _, refresher := newSSOSvc(t)
	ssoTokens.EXPECT().GetByUsername(gomock.Any(), "alice").Return(&model.SSOToken{
		Username: "alice", IDToken: "stale", IDTokenExp: 1, RefreshToken: "y-refresh",
	}, nil)
	// The stored refresh token mints tokens for a DIFFERENT identity — must be rejected, never stored.
	refresher.EXPECT().Refresh(gomock.Any(), "y-refresh").
		Return(oidc.TokenSet{SSOToken: "y-access", Account: "mallory", Expiry: time.Now().Add(time.Hour)}, nil)

	_, err := svc.SSORefresh(ctx("alice", "site-a"), models.SSORefreshRequest{})
	// Server-side integrity anomaly on refresh ⇒ re-login (unauthenticated), not bad_request.
	requireCode(t, err, errcode.CodeUnauthenticated)
}
