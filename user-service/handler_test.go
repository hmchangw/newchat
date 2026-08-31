package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-service/models"
)

const (
	testDefaultLimit = 40
	testMaxLimit     = 400
)

// handlerEngine mounts the handler behind a stub auth that pins the account, so
// the handler tests exercise binding and delegation, not credentials.
func handlerEngine(t *testing.T, lister subscriptionLister, account string) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) { c.Set(ctxAccountKey, account) })
	h := newHandler(lister, testDefaultLimit, testMaxLimit)
	r.GET("/api/v1/subscriptions", h.ListSubscriptions)
	r.GET("/api/v1/subscriptions/count", h.CountSubscriptions)
	return r
}

// getSubs issues a subscription request with the given query.
func getSubs(t *testing.T, r *gin.Engine, query string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?"+query, nil))
	return w
}

// boolPtr is for the optional-bool query fields.
func boolPtr(b bool) *bool { return &b }

// intPtr is for the optional-int query fields.
func intPtr(i int) *int { return &i }

// TestHandler_BindsQueryToRequest is the contract between the query string and
// the service request: omitted must stay distinguishable from a zero value.
func TestHandler_BindsQueryToRequest(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  models.SubscriptionListRequest
	}{
		{"type only", "type=current", models.SubscriptionListRequest{Type: "current"}},
		{"explicit limit", "type=current&limit=200", models.SubscriptionListRequest{Type: "current", Limit: 200}},
		{"offset and limit", "type=rooms&offset=400&limit=200", models.SubscriptionListRequest{Type: "rooms", Offset: 400, Limit: 200}},
		{"favorite true", "type=current&favorite=true", models.SubscriptionListRequest{Type: "current", Favorite: boolPtr(true)}},
		{"favorite false", "type=current&favorite=false", models.SubscriptionListRequest{Type: "current", Favorite: boolPtr(false)}},
		{"includeLastMessage false", "type=current&includeLastMessage=false", models.SubscriptionListRequest{Type: "current", IncludeLastMessage: boolPtr(false)}},
		{"includeLastMessage omitted stays nil", "type=current", models.SubscriptionListRequest{Type: "current"}},
		{"updatedWithinDays", "type=rooms&updatedWithinDays=7", models.SubscriptionListRequest{Type: "rooms", UpdatedWithinDays: intPtr(7)}},
		{"updatedWithinDays zero is not omitted", "type=rooms&updatedWithinDays=0", models.SubscriptionListRequest{Type: "rooms", UpdatedWithinDays: intPtr(0)}},
		{"apps type", "type=apps", models.SubscriptionListRequest{Type: "apps"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lister := NewMocksubscriptionLister(gomock.NewController(t))
			lister.EXPECT().
				ListSubscriptionsFor(gomock.Any(), "alice", tc.want, testDefaultLimit, testMaxLimit).
				Return(&models.PagedSubscriptionListResponse{}, nil)

			assert.Equal(t, http.StatusOK, getSubs(t, handlerEngine(t, lister, "alice"), tc.query).Code)
		})
	}
}

// A caller must not be able to read another account by naming it.
func TestHandler_UsesAuthenticatedAccountNotQuery(t *testing.T) {
	lister := NewMocksubscriptionLister(gomock.NewController(t))
	lister.EXPECT().
		ListSubscriptionsFor(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&models.PagedSubscriptionListResponse{}, nil)

	// A spoofed account parameter must be ignored entirely.
	w := getSubs(t, handlerEngine(t, lister, "alice"), "type=current&account=mallory")
	assert.Equal(t, http.StatusOK, w.Code)
}

// The HTTP body stays byte-identical to the NATS reply.
func TestHandler_SerializesResponse(t *testing.T) {
	lister := NewMocksubscriptionLister(gomock.NewController(t))
	lister.EXPECT().ListSubscriptionsFor(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&models.PagedSubscriptionListResponse{
			Subscriptions: []model.SubscriptionItem{
				&model.ChannelSubscription{Subscription: &model.Subscription{ID: "s1", RoomID: "r1", RoomType: model.RoomTypeChannel}},
			},
			HasMore: true,
		}, nil)

	w := getSubs(t, handlerEngine(t, lister, "alice"), "type=current")

	require.Equal(t, http.StatusOK, w.Code)
	var got struct {
		Subscriptions []json.RawMessage `json:"subscriptions"`
		HasMore       bool              `json:"hasMore"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Len(t, got.Subscriptions, 1)
	assert.True(t, got.HasMore)
}

// An empty page serializes `subscriptions` as null, not []. That is pre-existing
// on both transports (one shared response struct); pinned here so a change to it
// is a deliberate contract decision rather than an accident.
func TestHandler_EmptyPageSerializesSubscriptionsAsNull(t *testing.T) {
	lister := NewMocksubscriptionLister(gomock.NewController(t))
	lister.EXPECT().ListSubscriptionsFor(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&models.PagedSubscriptionListResponse{}, nil)

	w := getSubs(t, handlerEngine(t, lister, "alice"), "type=current")

	assert.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{"subscriptions":null,"hasMore":false}`, w.Body.String())
}

// Per-account data must never be shared-cacheable: the credential is a
// non-standard header, so Vary cannot keep one user's sidebar from another.
func TestHandler_MarksResponsePrivate(t *testing.T) {
	lister := NewMocksubscriptionLister(gomock.NewController(t))
	lister.EXPECT().ListSubscriptionsFor(gomock.Any(), "alice", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&models.PagedSubscriptionListResponse{}, nil)

	w := getSubs(t, handlerEngine(t, lister, "alice"), "type=current")

	assert.Equal(t, "private, no-store", w.Header().Get("Cache-Control"))
}

// Binding failures are client errors, not 500s.
func TestHandler_RejectsMalformedQuery(t *testing.T) {
	for _, query := range []string{
		"type=current&limit=abc",
		"type=current&offset=xyz",
		"type=current&favorite=maybe",
		"type=current&updatedWithinDays=soon",
	} {
		t.Run(query, func(t *testing.T) {
			lister := NewMocksubscriptionLister(gomock.NewController(t))
			// Nothing must reach the service: binding fails first.
			w := getSubs(t, handlerEngine(t, lister, "alice"), query)

			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), string(errcode.CodeBadRequest))
		})
	}
}

// The service's typed error decides the status, not the transport.
func TestHandler_PropagatesServiceErrorCodes(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   errcode.Code
	}{
		{"bad request", errcode.BadRequest("unknown subscription type"), http.StatusBadRequest, errcode.CodeBadRequest},
		{"not found", errcode.NotFound("nope"), http.StatusNotFound, errcode.CodeNotFound},
		{"raw error collapses to internal", fmt.Errorf("list subscriptions: %w", errors.New("mongo exploded")), http.StatusInternalServerError, errcode.CodeInternal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lister := NewMocksubscriptionLister(gomock.NewController(t))
			lister.EXPECT().ListSubscriptionsFor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, tc.err)

			w := getSubs(t, handlerEngine(t, lister, "alice"), "type=current")

			require.Equal(t, tc.wantStatus, w.Code)
			assert.Contains(t, w.Body.String(), string(tc.wantCode))
			assert.NotContains(t, w.Body.String(), "mongo exploded", "internal detail must not reach the client")
		})
	}
}

// The handler's deadline and cancellation must reach the service.
func TestHandler_PassesRequestContext(t *testing.T) {
	type ctxKey string
	const key ctxKey = "marker"

	lister := NewMocksubscriptionLister(gomock.NewController(t))
	lister.EXPECT().ListSubscriptionsFor(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ string, _ models.SubscriptionListRequest, _, _ int) (*models.PagedSubscriptionListResponse, error) {
			assert.Equal(t, "present", ctx.Value(key), "the handler must forward the request context")
			return &models.PagedSubscriptionListResponse{}, nil
		})

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxAccountKey, "alice")
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), key, "present"))
	})
	r.GET("/api/v1/subscriptions", newHandler(lister, testDefaultLimit, testMaxLimit).ListSubscriptions)

	assert.Equal(t, http.StatusOK, getSubs(t, r, "type=current").Code)
}

// manyItems builds n channel rows with enough content to clear the gzip threshold.
func manyItems(n int) []model.SubscriptionItem {
	items := make([]model.SubscriptionItem, n)
	for i := range items {
		items[i] = &model.ChannelSubscription{Subscription: &model.Subscription{
			ID:       fmt.Sprintf("sub-%d", i),
			RoomID:   fmt.Sprintf("room-%d", i),
			RoomType: model.RoomTypeChannel,
			Name:     fmt.Sprintf("engineering-channel-number-%d", i),
		}}
	}
	return items
}

// gunzipBody decompresses a gzip-encoded recorder body.
func gunzipBody(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(w.Body.Bytes()))
	require.NoError(t, err)
	out, err := io.ReadAll(zr)
	require.NoError(t, err)
	return string(out)
}

// TestHandler_CountBindsUnread pins the unread query → CountRequest mapping
// (omitted stays nil, distinct from false) and delegates to the shared count.
func TestHandler_CountBindsUnread(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  models.CountRequest
	}{
		{"unread true", "unread=true", models.CountRequest{Unread: boolPtr(true)}},
		{"unread false", "unread=false", models.CountRequest{Unread: boolPtr(false)}},
		{"unread omitted stays nil", "", models.CountRequest{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lister := NewMocksubscriptionLister(gomock.NewController(t))
			lister.EXPECT().
				CountSubscriptionsFor(gomock.Any(), "alice", tc.want).
				Return(&models.CountResponse{Count: 3}, nil)

			w := httptest.NewRecorder()
			handlerEngine(t, lister, "alice").ServeHTTP(w,
				httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions/count?"+tc.query, nil))
			assert.Equal(t, http.StatusOK, w.Code)
			assert.JSONEq(t, `{"count":3}`, w.Body.String())
		})
	}
}
