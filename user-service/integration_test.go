//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/user-service/config"
	"github.com/hmchangw/chat/user-service/mongorepo"
	"github.com/hmchangw/chat/user-service/service"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

const (
	seededRooms  = 250
	testSiteID   = "site-a"
	testAccount  = "alice"
	testSSOToken = "tok-integration"
)

// stubRooms records every GetRoomsInfo batch so the test can assert the caps
// history-service and room-service actually enforce.
type stubRooms struct{ batches chan int }

func (s *stubRooms) GetRoomsInfo(_ context.Context, _ string, roomIDs []string) ([]model.RoomInfo, error) {
	s.batches <- len(roomIDs)
	out := make([]model.RoomInfo, 0, len(roomIDs))
	for _, id := range roomIDs {
		out = append(out, model.RoomInfo{RoomID: id, Found: true, Name: "room-" + id})
	}
	return out, nil
}

func (s *stubRooms) GetRoomsMeta(ctx context.Context, site string, roomIDs []string) ([]model.RoomInfo, error) {
	return s.GetRoomsInfo(ctx, site, roomIDs)
}

// The remaining RoomClient methods are outside the list path; the list tests
// never reach them.
func (s *stubRooms) CreateDMRoom(context.Context, string, string, model.RoomType) (model.Subscription, error) {
	return model.Subscription{}, fmt.Errorf("not used by the subscription list")
}

func (s *stubRooms) GetThreadRoomInfoBatch(context.Context, string, []string) ([]model.ThreadRoomInfo, error) {
	return nil, fmt.Errorf("not used by the subscription list")
}

func (s *stubRooms) ClearAllThreadUnread(context.Context, string, string) error {
	return fmt.Errorf("not used by the subscription list")
}

// stubHistory mirrors history-service's hard caps: it fails the batch when a
// caller exceeds them, exactly as the real service does.
type stubHistory struct{ batches chan int }

// fixedPreviewTime keeps two requests byte-comparable; time.Now() per call would
// make the gzip round-trip test fail on the timestamp, not the encoding.
var fixedPreviewTime = time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)

const historyBatchCap = 100

func (s *stubHistory) RoomsGet(_ context.Context, _ string, roomIDs []string, hints map[string]model.RoomTimeHint) (map[string]model.PreviewMessage, error) {
	s.batches <- len(roomIDs)
	if len(roomIDs) > historyBatchCap {
		return nil, fmt.Errorf("batch size %d exceeds limit %d", len(roomIDs), historyBatchCap)
	}
	if len(hints) > historyBatchCap {
		return nil, fmt.Errorf("hint count %d exceeds limit %d", len(hints), historyBatchCap)
	}
	out := make(map[string]model.PreviewMessage, len(roomIDs))
	for _, id := range roomIDs {
		out[id] = model.PreviewMessage{
			MessageID: id + "-msg",
			Content:   "the quick brown fox jumps over the lazy dog, repeatedly, for payload weight",
			CreatedAt: fixedPreviewTime,
		}
	}
	return out, nil
}

func (s *stubHistory) GetThreadList(context.Context, string, model.ThreadSubscriptionListRequest) (model.ThreadSubscriptionListResponse, error) {
	return model.ThreadSubscriptionListResponse{}, nil
}

// seedSubscriptions inserts n local channel subscriptions and their rooms.
func seedSubscriptions(t *testing.T, db *mongo.Database, n int) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()

	rooms := make([]any, 0, n)
	subs := make([]any, 0, n)
	for i := range n {
		roomID := fmt.Sprintf("room-%04d", i)
		rooms = append(rooms, bson.M{
			"_id": roomID, "name": fmt.Sprintf("channel-%04d", i), "siteId": testSiteID,
			"userCount": 5, "lastMsgId": roomID + "-msg", "lastMsgAt": now.Add(-time.Duration(i) * time.Minute),
		})
		subs = append(subs, bson.M{
			"_id":    fmt.Sprintf("sub-%04d", i),
			"u":      bson.M{"_id": "u-alice", "account": testAccount},
			"roomId": roomID, "name": fmt.Sprintf("channel-%04d", i), "roomType": "channel",
			"siteId": testSiteID, "_updatedAt": now, "createdAt": now,
		})
	}
	_, err := db.Collection("rooms").InsertMany(ctx, rooms)
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertMany(ctx, subs)
	require.NoError(t, err)
}

// newTestAPI wires the real route tree over a real Mongo and the stubs above.
func newTestAPI(t *testing.T) (*gin.Engine, *stubRooms, *stubHistory) {
	t.Helper()
	db := testutil.MongoDB(t, "usersvc-http")
	seedSubscriptions(t, db, seededRooms)

	subRepo := mongorepo.NewSubscriptionRepo(db, testSiteID)
	require.NoError(t, subRepo.EnsureIndexes(context.Background()))

	// Buffered well past the expected batch count so a stub never blocks the fan-out.
	rooms := &stubRooms{batches: make(chan int, 64)}
	history := &stubHistory{batches: make(chan int, 64)}

	cfg := &config.Config{
		SiteID: testSiteID, AllSiteIDs: []string{testSiteID},
		MaxSubscriptionLimit: 1000, DefaultSubscriptionLimit: 40,
		MaxAppsLimit: 100, DefaultAppsLimit: 20, MaxAccountNames: 100,
		BadgeCountCap: 10, RoomBatchChunk: 100, MaxSiteFanout: 8,
	}
	svc := service.New(subRepo, mongorepo.NewUserRepo(db), mongorepo.NewAppRepo(db),
		mongorepo.NewThreadSubscriptionRepo(db), rooms, history, nil, nil, nil,
		noopBadgeCache{}, nil, nil, nil, cfg)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, newHandler(svc, 40, 400), authDeps{sso: fakeSSO{claims: map[string]pkgoidc.Claims{
		testSSOToken: {PreferredUsername: testAccount},
	}}}, httpDeps{
		maxConcurrency: 256, gzipMinBytes: 1024, handlerTimeout: 30 * time.Second,
	})
	return r, rooms, history
}

func apiGet(t *testing.T, r *gin.Engine, query, acceptEncoding string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?"+query, nil)
	req.Header.Set(ssoTokenHeader, testSSOToken)
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

type listResponse struct {
	Subscriptions []struct {
		RoomID string `json:"roomId"`
		Room   *struct {
			PreviewMessage *model.PreviewMessage `json:"previewMessage"`
		} `json:"room"`
	} `json:"subscriptions"`
	HasMore bool `json:"hasMore"`
}

func drain(ch chan int) []int {
	var out []int
	for {
		select {
		case v := <-ch:
			out = append(out, v)
		default:
			return out
		}
	}
}

// The regression guard for chunking: before it, a 200-row page came back 200 OK
// with every previewMessage missing, because history-service rejects an
// over-100 batch and the failure is swallowed as "degraded".
func TestHTTPList_LargePageKeepsEveryPreview(t *testing.T) {
	r, _, history := newTestAPI(t)

	w := apiGet(t, r, "type=current&limit=200", "")
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	var got listResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Subscriptions, 200)
	assert.True(t, got.HasMore, "250 seeded rows means a 200-row page has more")

	for i, sub := range got.Subscriptions {
		require.NotNil(t, sub.Room, "row %d (%s) lost its room object", i, sub.RoomID)
		require.NotNil(t, sub.Room.PreviewMessage, "row %d (%s) lost its preview", i, sub.RoomID)
	}

	for _, size := range drain(history.batches) {
		assert.LessOrEqual(t, size, historyBatchCap, "a batch exceeded the history-service cap")
	}
}

func TestHTTPList_PagesToTheEnd(t *testing.T) {
	r, _, _ := newTestAPI(t)

	seen := map[string]bool{}
	for offset := 0; offset < seededRooms; offset += 200 {
		w := apiGet(t, r, fmt.Sprintf("type=current&limit=200&offset=%d", offset), "")
		require.Equal(t, http.StatusOK, w.Code)

		var got listResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
		for _, s := range got.Subscriptions {
			assert.False(t, seen[s.RoomID], "room %s returned on two pages", s.RoomID)
			seen[s.RoomID] = true
		}
		assert.Equal(t, offset+200 < seededRooms, got.HasMore)
	}
	assert.Len(t, seen, seededRooms, "paging must cover every subscription exactly once")
}

func TestHTTPList_LimitClampsToMax(t *testing.T) {
	r, _, _ := newTestAPI(t)

	w := apiGet(t, r, "type=current&limit=99999", "")
	require.Equal(t, http.StatusOK, w.Code)

	var got listResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Len(t, got.Subscriptions, seededRooms, "clamped to 400, so all 250 rows fit")
}

func TestHTTPList_DefaultLimitMatchesNATS(t *testing.T) {
	r, _, _ := newTestAPI(t)

	w := apiGet(t, r, "type=current", "")
	require.Equal(t, http.StatusOK, w.Code)

	var got listResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Len(t, got.Subscriptions, 40, "an omitted limit must behave as it does over NATS")
}

func TestHTTPList_GzipMatchesPlainBody(t *testing.T) {
	r, _, _ := newTestAPI(t)

	plain := apiGet(t, r, "type=current&limit=200", "")
	require.Equal(t, http.StatusOK, plain.Code)
	assert.Empty(t, plain.Header().Get("Content-Encoding"))

	zipped := apiGet(t, r, "type=current&limit=200", "gzip")
	require.Equal(t, http.StatusOK, zipped.Code)
	require.Equal(t, "gzip", zipped.Header().Get("Content-Encoding"))

	zr, err := gzip.NewReader(bytes.NewReader(zipped.Body.Bytes()))
	require.NoError(t, err)
	decoded, err := io.ReadAll(zr)
	require.NoError(t, err)

	assert.JSONEq(t, plain.Body.String(), string(decoded), "compression must not change the payload")
	assert.Less(t, zipped.Body.Len(), plain.Body.Len()/2, "a 200-row page should compress well past 2x")
	t.Logf("200-row page: %d bytes plain, %d bytes gzipped (%.1fx)",
		plain.Body.Len(), zipped.Body.Len(), float64(plain.Body.Len())/float64(zipped.Body.Len()))
}

func TestHTTPList_RejectsUnauthenticated(t *testing.T) {
	r, _, _ := newTestAPI(t)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?type=current", nil))

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHTTPList_SkippingPreviewsAvoidsHistoryEntirely(t *testing.T) {
	r, _, history := newTestAPI(t)

	w := apiGet(t, r, "type=current&limit=200&includeLastMessage=false", "")
	require.Equal(t, http.StatusOK, w.Code)

	assert.Empty(t, drain(history.batches), "includeLastMessage=false must skip the resolve entirely")
}
