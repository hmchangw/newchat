package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/user-service/models"
)

// routedEngine builds the real route tree, so these tests cover the middleware
// order rather than any one middleware.
func routedEngine(t *testing.T, lister subscriptionLister, d httpDeps) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	auth := authDeps{sso: fakeSSO{claims: map[string]pkgoidc.Claims{"tok-good": {PreferredUsername: "alice"}}}}
	registerRoutes(r, newHandler(lister, testDefaultLimit, testMaxLimit), auth, d)
	return r
}

func authedGet(r *gin.Engine, query, acceptEncoding string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?"+query, nil)
	req.Header.Set(ssoTokenHeader, "tok-good")
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func defaultDeps() httpDeps {
	return httpDeps{maxConcurrency: 256, gzipMinBytes: 1024, handlerTimeout: 30 * time.Second}
}

func TestRoutes_RequiresAuthentication(t *testing.T) {
	lister := NewMocksubscriptionLister(gomock.NewController(t))
	r := routedEngine(t, lister, defaultDeps())

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?type=current", nil))

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestRoutes_ServesAuthenticatedRequest(t *testing.T) {
	lister := NewMocksubscriptionLister(gomock.NewController(t))
	lister.EXPECT().ListSubscriptionsFor(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&models.PagedSubscriptionListResponse{}, nil)

	assert.Equal(t, http.StatusOK, authedGet(routedEngine(t, lister, defaultDeps()), "type=current", "").Code)
}

// The limiter must sit ahead of auth so a shed request costs no token validation.
func TestRoutes_ShedsBeforeAuthenticating(t *testing.T) {
	var shed atomic.Int64
	admitted := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	lister := NewMocksubscriptionLister(gomock.NewController(t))
	lister.EXPECT().ListSubscriptionsFor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string, models.SubscriptionListRequest, int, int) (*models.PagedSubscriptionListResponse, error) {
			admitted <- struct{}{}
			<-release
			return &models.PagedSubscriptionListResponse{}, nil
		})

	d := defaultDeps()
	d.maxConcurrency = 1
	d.onShed = func() { shed.Add(1) }
	r := routedEngine(t, lister, d)

	go func() { authedGet(r, "type=current", "") }()
	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("first request was never admitted")
	}

	// No credential at all: it must still be shed, not rejected as unauthenticated.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?type=current", nil))

	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "1", w.Header().Get("Retry-After"))
	assert.Equal(t, int64(1), shed.Load())
}

func TestRoutes_CompressesLargeResponses(t *testing.T) {
	lister := NewMocksubscriptionLister(gomock.NewController(t))
	lister.EXPECT().ListSubscriptionsFor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&models.PagedSubscriptionListResponse{Subscriptions: manyItems(300)}, nil)

	w := authedGet(routedEngine(t, lister, defaultDeps()), "type=current&limit=300", "gzip")

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
	assert.Contains(t, gunzipBody(t, w), `"hasMore":false`)
}

func TestRoutes_AppliesHandlerTimeout(t *testing.T) {
	lister := NewMocksubscriptionLister(gomock.NewController(t))
	lister.EXPECT().ListSubscriptionsFor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ string, _ models.SubscriptionListRequest, _, _ int) (*models.PagedSubscriptionListResponse, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok, "the handler must run under a deadline")
			assert.WithinDuration(t, time.Now().Add(5*time.Second), deadline, time.Second)
			return &models.PagedSubscriptionListResponse{}, nil
		})

	d := defaultDeps()
	d.handlerTimeout = 5 * time.Second
	assert.Equal(t, http.StatusOK, authedGet(routedEngine(t, lister, d), "type=current", "").Code)
}
