package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// Fixture ops token for the control-surface tests, not a live credential.
// nosemgrep: hardcoded-credential-literal
const testOpsToken = "s3cr3t"

// newFailoverTestServer wires the handler + routes with a mocked store and a
// fixed clock, behind the real requireOps middleware.
func newFailoverTestServer(t *testing.T) (*gin.Engine, *MockFailoverStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockFailoverStore(ctrl)
	h := NewFailoverHandler(store, map[string]siteURL{
		"site-a":  {BaseURL: "http://a", NATSURL: "ws://a"},
		"_backup": {BaseURL: "http://b", NATSURL: "ws://b"},
	}, "_backup")
	h.now = func() time.Time { return time.UnixMilli(1700).UTC() }
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerFailoverRoutes(r, h, testOpsToken)
	return r, store
}

func do(t *testing.T, r *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testOpsToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestFailoverHandler_List(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().List(gomock.Any()).Return([]FailoverState{
		{SiteID: "site-a", Status: StatusFailedOver, Version: 1},
	}, nil)

	w := do(t, r, http.MethodGet, "/internal/v1/failover", "")
	require.Equal(t, http.StatusOK, w.Code)

	var got []failoverStateResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got, 1)
	assert.Equal(t, "site-a", got[0].SiteID)
	assert.Equal(t, ServingBackup, got[0].ServingTarget)
}

func TestFailoverHandler_Get(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(
		FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 0}, nil)

	w := do(t, r, http.MethodGet, "/internal/v1/failover/site-a", "")
	require.Equal(t, http.StatusOK, w.Code)

	var got failoverStateResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, StatusHealthy, got.Status)
	assert.Equal(t, ServingHome, got.ServingTarget)
}

func TestFailoverHandler_PostFailoverHappyPath(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(
		FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 0}, nil)
	store.EXPECT().Transition(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ any, next *FailoverState) error {
			assert.Equal(t, StatusFailedOver, next.Status)
			assert.Equal(t, int64(1), next.Version)
			assert.Equal(t, "jane", next.Operator)
			assert.Equal(t, "nats down", next.Reason)
			assert.Equal(t, int64(1700), next.Timestamp)
			return nil
		})

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
		`{"action":"failover","operator":"jane","reason":"nats down"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var got failoverStateResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, StatusFailedOver, got.Status)
	assert.Equal(t, ServingBackup, got.ServingTarget)
}

func TestFailoverHandler_PostIllegalTransition(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(
		FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 1}, nil)
	// No Transition call expected.

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
		`{"action":"failover","operator":"jane","reason":"again"}`)
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "failover_illegal_transition")
}

func TestFailoverHandler_PostVersionConflict(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(
		FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 0}, nil)
	store.EXPECT().Transition(gomock.Any(), gomock.Any()).Return(errFailoverVersionConflict)

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
		`{"action":"failover","operator":"jane","reason":"race"}`)
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "failover_version_conflict")
}

func TestFailoverHandler_PostValidation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"missing operator", `{"action":"failover","reason":"x"}`},
		{"missing reason", `{"action":"failover","operator":"jane"}`},
		{"blank operator", `{"action":"failover","operator":"   ","reason":"x"}`},
		{"blank reason", `{"action":"failover","operator":"jane","reason":"\t\n"}`},
		{"unknown action", `{"action":"nope","operator":"jane","reason":"x"}`},
		{"malformed json", `{`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newFailoverTestServer(t) // no store calls expected
			w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a", tc.body)
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

// Padded audit fields are stored trimmed, so the audit trail never carries
// whitespace-only or accidentally-padded attribution.
func TestFailoverHandler_PostTrimsAuditFields(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(
		FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 0}, nil)
	store.EXPECT().Transition(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ any, next *FailoverState) error {
			assert.Equal(t, "jane", next.Operator)
			assert.Equal(t, "nats down", next.Reason)
			return nil
		})

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
		`{"action":"failover","operator":"  jane  ","reason":"\tnats down\n"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var got failoverStateResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "jane", got.Operator)
	assert.Equal(t, "nats down", got.Reason)
}

// A site absent from PORTAL_SITE_URLS is rejected before any store call — a
// typo must not mint failover state for a site routing can never serve.
func TestFailoverHandler_PostUnknownSite(t *testing.T) {
	r, _ := newFailoverTestServer(t) // no store calls expected

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-typo",
		`{"action":"failover","operator":"jane","reason":"x"}`)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "failover_unknown_site")
}

func TestFailoverHandler_ListStoreError(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().List(gomock.Any()).Return(nil, errors.New("mongo down"))

	w := do(t, r, http.MethodGet, "/internal/v1/failover", "")
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestFailoverHandler_GetStoreError(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(FailoverState{}, errors.New("mongo down"))

	w := do(t, r, http.MethodGet, "/internal/v1/failover/site-a", "")
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestFailoverHandler_PostGetStoreError(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(FailoverState{}, errors.New("mongo down"))

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
		`{"action":"failover","operator":"jane","reason":"x"}`)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestFailoverHandler_PostTransitionStoreError(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(
		FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 0}, nil)
	store.EXPECT().Transition(gomock.Any(), gomock.Any()).Return(errors.New("mongo down"))

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
		`{"action":"failover","operator":"jane","reason":"x"}`)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestFailoverHandler_Unauthorized(t *testing.T) {
	r, _ := newFailoverTestServer(t)
	// No auth header -> requireOps rejects before any handler runs.
	req := httptest.NewRequest(http.MethodGet, "/internal/v1/failover", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// newFailoverTestServerWithBackup is newFailoverTestServer with an explicit
// backup id and registry, for the backup-route validation tests.
func newFailoverTestServerWithBackup(t *testing.T, backupSiteID string, sites map[string]siteURL) (*gin.Engine, *MockFailoverStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockFailoverStore(ctrl)
	h := NewFailoverHandler(store, sites, backupSiteID)
	h.now = func() time.Time { return time.UnixMilli(1700).UTC() }
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerFailoverRoutes(r, h, testOpsToken)
	return r, store
}

// Failing a site over to a backup that routing cannot resolve would flip the
// state and then 500 every user's /api/userInfo — the site goes dark instead of
// failing over. Refuse at the operator's request instead.
func TestFailoverHandler_PostRejectsFailoverWithoutServableBackup(t *testing.T) {
	registry := map[string]siteURL{"site-a": {BaseURL: "http://a", NATSURL: "ws://a"}}
	tests := []struct {
		name         string
		backupSiteID string
	}{
		{"backup id unset", ""},
		{"backup id absent from registry", "_backup"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, store := newFailoverTestServerWithBackup(t, tt.backupSiteID, registry)
			store.EXPECT().Get(gomock.Any(), "site-a").Return(
				FailoverState{SiteID: "site-a", Status: StatusHealthy, Version: 0}, nil)
			// No Transition: the state must not move.

			w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
				`{"action":"failover","operator":"jane","reason":"nats down"}`)
			assert.Equal(t, http.StatusServiceUnavailable, w.Code)
			assert.Contains(t, w.Body.String(), "failover_backup_unavailable")
		})
	}
}

// The same guard must not block the recovery path: actions that return a site
// home need no backup route.
func TestFailoverHandler_PostAllowsRecoveryWithoutServableBackup(t *testing.T) {
	registry := map[string]siteURL{"site-a": {BaseURL: "http://a", NATSURL: "ws://a"}}
	r, store := newFailoverTestServerWithBackup(t, "", registry)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(
		FailoverState{SiteID: "site-a", Status: StatusFailedOver, Version: 3}, nil)
	store.EXPECT().Transition(gomock.Any(), gomock.Any()).Return(nil)

	w := do(t, r, http.MethodPost, "/internal/v1/failover/site-a",
		`{"action":"resume","operator":"jane","reason":"false alarm"}`)
	require.Equal(t, http.StatusOK, w.Code)

	var got failoverStateResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, StatusHealthy, got.Status)
	assert.Equal(t, ServingHome, got.ServingTarget)
}

func TestValidateFailoverConfig(t *testing.T) {
	sites := map[string]siteURL{
		"site-a":  {BaseURL: "http://a", NATSURL: "ws://a"},
		"_backup": {BaseURL: "http://b", NATSURL: "ws://b"},
	}
	tests := []struct {
		name         string
		opsToken     string
		backupSiteID string
		wantErr      bool
	}{
		{"control surface disabled, no backup", "", "", false},
		{"control surface disabled, bogus backup", "", "nope", false},
		{"enabled with a resolvable backup", "t", "_backup", false},
		{"enabled without a backup id", "t", "", true},
		{"enabled with a backup absent from the registry", "t", "nope", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFailoverConfig(tt.opsToken, tt.backupSiteID, sites)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}
