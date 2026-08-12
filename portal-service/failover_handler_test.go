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

const testOpsToken = "s3cr3t"

// newFailoverTestServer wires the handler + routes with a mocked store and a
// fixed clock, behind the real requireOps middleware.
func newFailoverTestServer(t *testing.T) (*gin.Engine, *MockFailoverStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockFailoverStore(ctrl)
	h := NewFailoverHandler(store)
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
		func(_ any, next FailoverState) error {
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

func TestFailoverHandler_PostGetStoreError(t *testing.T) {
	r, store := newFailoverTestServer(t)
	store.EXPECT().Get(gomock.Any(), "site-a").Return(FailoverState{}, errors.New("mongo down"))

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
