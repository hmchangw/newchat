package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

const testSpotlightIndex = "spotlight-*"

type fakeStore struct {
	searchCalls   []searchCall
	searchBody    json.RawMessage
	searchErr     error
	userRoom      UserRoomDoc
	userRoomFound bool
	userRoomErr   error
	userRoomCalls int
}

type searchCall struct {
	indices []string
	body    json.RawMessage
}

func (f *fakeStore) Search(_ context.Context, indices []string, body json.RawMessage) (json.RawMessage, error) {
	f.searchCalls = append(f.searchCalls, searchCall{indices: indices, body: body})
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	if f.searchBody == nil {
		return json.RawMessage(`{"hits":{"total":{"value":0},"hits":[]}}`), nil
	}
	return f.searchBody, nil
}

func (f *fakeStore) GetUserRoomDoc(_ context.Context, _ string) (UserRoomDoc, bool, error) {
	f.userRoomCalls++
	if f.userRoomErr != nil {
		return UserRoomDoc{}, false, f.userRoomErr
	}
	return f.userRoom, f.userRoomFound, nil
}

type fakeCache struct {
	store    map[string]map[string]int64
	getErr   error
	setErr   error
	setCalls int
	getCalls int
}

func newFakeCache() *fakeCache {
	return &fakeCache{store: map[string]map[string]int64{}}
}

func (f *fakeCache) GetRestricted(_ context.Context, account string) (map[string]int64, bool, error) {
	f.getCalls++
	if f.getErr != nil {
		return nil, false, f.getErr
	}
	v, ok := f.store[account]
	return v, ok, nil
}

func (f *fakeCache) SetRestricted(_ context.Context, account string, rooms map[string]int64, _ time.Duration) error {
	f.setCalls++
	if f.setErr != nil {
		return f.setErr
	}
	f.store[account] = rooms
	return nil
}

func newTestHandler(store SearchStore, mongo MongoStore, users SearchUsersClient, cache RestrictedRoomCache) *handler {
	return newHandler(store, mongo, users, cache, handlerConfig{
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            365 * 24 * time.Hour,
		SpotlightReadPattern:    testSpotlightIndex,
	})
}

func ctxWithAccount(account string) *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": account})
}

func TestHandler_SearchMessages_CacheHitUnrestricted(t *testing.T) {
	store := &fakeStore{}
	cache := newFakeCache()
	cache.store["alice"] = map[string]int64{} // empty restricted → cache hit

	h := newTestHandler(store, nil, nil, cache)

	resp, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{Query: "hi"})
	require.NoError(t, err)
	assert.EqualValues(t, 0, resp.Total)

	assert.Equal(t, 0, store.userRoomCalls, "cache hit → no ES user-room call")
	require.Len(t, store.searchCalls, 1)
	assert.Equal(t, MessageIndexPattern, store.searchCalls[0].indices)
}

func TestHandler_SearchMessages_CacheMissPopulatesFromES(t *testing.T) {
	store := &fakeStore{
		userRoom:      UserRoomDoc{UserAccount: "alice", RestrictedRooms: map[string]int64{"rx": 1_700_000_000_000}},
		userRoomFound: true,
	}
	cache := newFakeCache()

	h := newTestHandler(store, nil, nil, cache)
	resp, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{Query: "hi"})
	require.NoError(t, err)
	assert.EqualValues(t, 0, resp.Total)

	assert.Equal(t, 1, store.userRoomCalls)
	assert.Equal(t, 1, cache.setCalls)
	assert.Equal(t, map[string]int64{"rx": 1_700_000_000_000}, cache.store["alice"])
}

func TestHandler_SearchMessages_CacheErrorFallsThroughToES(t *testing.T) {
	store := &fakeStore{userRoomFound: false}
	cache := newFakeCache()
	cache.getErr = errors.New("valkey down")

	h := newTestHandler(store, nil, nil, cache)
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{Query: "hi"})
	require.NoError(t, err)
	assert.Equal(t, 1, store.userRoomCalls, "cache error triggers ES prefetch")
	// Verify the handler skips SetRestricted when the prior GetRestricted
	// errored — the transport is almost certainly still down, and a
	// second failure-warning log adds noise without new signal.
	assert.Equal(t, 0, cache.setCalls, "set must not run after cache-get error")
}

func TestHandler_SearchMessages_CacheAndESFailReturnInternal(t *testing.T) {
	store := &fakeStore{userRoomErr: errors.New("es down")}
	cache := newFakeCache()
	cache.getErr = errors.New("valkey down")

	h := newTestHandler(store, nil, nil, cache)
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{Query: "hi"})
	require.Error(t, err)

	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeInternal, rerr.Code)
}

func TestHandler_SearchMessages_ESSearchError(t *testing.T) {
	store := &fakeStore{searchErr: errors.New("es failed")}
	cache := newFakeCache()
	cache.store["alice"] = map[string]int64{}

	h := newTestHandler(store, nil, nil, cache)
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{Query: "hi"})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeInternal, rerr.Code)
}

func TestHandler_SearchMessages_EmptyQuery(t *testing.T) {
	h := newTestHandler(&fakeStore{}, nil, nil, newFakeCache())
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchMessages_NegativeSizeRejected(t *testing.T) {
	h := newTestHandler(&fakeStore{}, nil, nil, newFakeCache())
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{Query: "x", Size: -1})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchMessages_SizeClamped(t *testing.T) {
	store := &fakeStore{}
	cache := newFakeCache()
	cache.store["alice"] = map[string]int64{}

	h := newHandler(store, nil, nil, cache, handlerConfig{
		DocCounts:               25,
		MaxDocCounts:            50,
		RestrictedRoomsCacheTTL: time.Minute,
		RecentWindow:            time.Hour,
	})
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{Query: "x", Size: 1000})
	require.NoError(t, err)

	// Inspect the emitted query body — size should be clamped to 50.
	require.Len(t, store.searchCalls, 1)
	var body map[string]any
	require.NoError(t, json.Unmarshal(store.searchCalls[0].body, &body))
	assert.Equal(t, float64(50), body["size"])
}

func TestHandler_SearchMessages_UserWithNoSubsReturnsEmpty(t *testing.T) {
	store := &fakeStore{userRoomFound: false}
	cache := newFakeCache()
	h := newTestHandler(store, nil, nil, cache)

	resp, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{Query: "x"})
	require.NoError(t, err)
	assert.EqualValues(t, 0, resp.Total)
	assert.Empty(t, resp.Messages)

	// empty restricted map should be cached to prevent miss-storm
	v, hit := cache.store["alice"]
	assert.True(t, hit)
	assert.Empty(t, v)
}

type fakeMongo struct {
	searchAppsCalls   []searchAppsCall
	searchAppsResults []model.App
	searchAppsErr     error
}

type searchAppsCall struct {
	query            string
	account          string
	assistantEnabled *bool
	offset           int
	limit            int
}

func (f *fakeMongo) SearchAppsByName(
	_ context.Context,
	query, account string,
	assistantEnabled *bool,
	offset, limit int,
) ([]model.App, error) {
	f.searchAppsCalls = append(f.searchAppsCalls, searchAppsCall{
		query: query, account: account, assistantEnabled: assistantEnabled, offset: offset, limit: limit,
	})
	if f.searchAppsErr != nil {
		return nil, f.searchAppsErr
	}
	return f.searchAppsResults, nil
}

func TestHandler_SearchRooms_HappyPath(t *testing.T) {
	store := &fakeStore{
		searchBody: json.RawMessage(`{"hits":{"total":{"value":2},"hits":[` +
			`{"_source":{"roomId":"r1","roomName":"general","roomType":"channel","siteId":"site-a"}},` +
			`{"_source":{"roomId":"r2","roomName":"alice-bob","roomType":"dm","siteId":"site-b"}}]}}`),
	}
	h := newTestHandler(store, &fakeMongo{}, nil, newFakeCache())

	resp, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{Query: "general"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Rooms, 2)
	assert.Equal(t, model.SearchRoom{RoomID: "r1", Name: "general", RoomType: "channel", SiteID: "site-a"}, resp.Rooms[0])
	assert.Equal(t, "r2", resp.Rooms[1].RoomID)
	assert.Equal(t, "site-b", resp.Rooms[1].SiteID)

	require.Len(t, store.searchCalls, 1)
	assert.Equal(t, []string{testSpotlightIndex}, store.searchCalls[0].indices)
}

func TestHandler_SearchRooms_EmptyQueryRejected(t *testing.T) {
	h := newTestHandler(&fakeStore{}, &fakeMongo{}, nil, newFakeCache())
	_, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchRooms_WhitespaceQueryRejected(t *testing.T) {
	h := newTestHandler(&fakeStore{}, &fakeMongo{}, nil, newFakeCache())
	_, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{Query: "   "})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchRooms_RoomTypeAppRejected(t *testing.T) {
	h := newTestHandler(&fakeStore{}, &fakeMongo{}, nil, newFakeCache())
	_, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{Query: "x", RoomType: "app"})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
	assert.Contains(t, rerr.Message, "invalid roomType")
}

func TestHandler_SearchRooms_UnknownRoomTypeRejected(t *testing.T) {
	h := newTestHandler(&fakeStore{}, &fakeMongo{}, nil, newFakeCache())
	_, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{Query: "x", RoomType: "zzz"})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchRooms_ESErrorSanitized(t *testing.T) {
	store := &fakeStore{searchErr: errors.New("es failed")}
	h := newTestHandler(store, &fakeMongo{}, nil, newFakeCache())
	_, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{Query: "general"})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeInternal, rerr.Code)
	assert.NotContains(t, rerr.Message, "es failed")
}

func TestHandler_SearchRooms_EmptyESResult(t *testing.T) {
	store := &fakeStore{
		searchBody: json.RawMessage(`{"hits":{"total":{"value":0},"hits":[]}}`),
	}
	h := newTestHandler(store, &fakeMongo{}, nil, newFakeCache())

	resp, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{Query: "nope"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Rooms, "must be empty slice, not nil")
	assert.Empty(t, resp.Rooms)
}

func TestHandler_SearchRooms_SizeClamped(t *testing.T) {
	store := &fakeStore{}
	h := newTestHandler(store, &fakeMongo{}, nil, newFakeCache())

	_, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{
		Query: "general",
		Size:  500,
	})
	require.NoError(t, err)

	require.Len(t, store.searchCalls, 1)
	var body map[string]any
	require.NoError(t, json.Unmarshal(store.searchCalls[0].body, &body))
	assert.Equal(t, float64(100), body["size"], "Size > MaxDocCounts must be clamped")
}

func TestHandler_SearchRooms_NegativeSizeRejected(t *testing.T) {
	h := newTestHandler(&fakeStore{}, &fakeMongo{}, nil, newFakeCache())
	_, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{Query: "x", Size: -1})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchRooms_UsesSpotlightIndex(t *testing.T) {
	store := &fakeStore{}
	h := newTestHandler(store, &fakeMongo{}, nil, newFakeCache())
	_, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{Query: "x"})
	require.NoError(t, err)

	require.Len(t, store.searchCalls, 1)
	assert.Equal(t, []string{testSpotlightIndex}, store.searchCalls[0].indices,
		"subscription search must hit only the spotlight index")
}

func TestHandler_SearchApps_Happy(t *testing.T) {
	mongo := &fakeMongo{searchAppsResults: []model.App{
		{ID: "a1", Name: "Weather"},
		{ID: "a2", Name: "Weatherly"},
	}}
	h := newTestHandler(nil, mongo, nil, newFakeCache())

	resp, err := h.searchApps(ctxWithAccount("alice"), model.SearchAppsRequest{Query: "weather"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.Apps, 2)
	assert.Equal(t, "Weather", resp.Apps[0].Name)

	require.Len(t, mongo.searchAppsCalls, 1)
	call := mongo.searchAppsCalls[0]
	assert.Equal(t, "weather", call.query)
	assert.Equal(t, "alice", call.account)
	assert.Nil(t, call.assistantEnabled)
	assert.Equal(t, 0, call.offset)
	assert.Equal(t, 25, call.limit, "Size unset → defaults to DocCounts=25")
}

func TestHandler_SearchApps_PassesOffset(t *testing.T) {
	mongo := &fakeMongo{}
	h := newTestHandler(nil, mongo, nil, newFakeCache())

	_, err := h.searchApps(ctxWithAccount("alice"), model.SearchAppsRequest{
		Query:  "weather",
		Offset: 50,
	})
	require.NoError(t, err)

	require.Len(t, mongo.searchAppsCalls, 1)
	assert.Equal(t, 50, mongo.searchAppsCalls[0].offset, "Offset must be propagated to the backend call")
}

func TestHandler_SearchApps_PassesAssistantEnabled(t *testing.T) {
	mongo := &fakeMongo{}
	h := newTestHandler(nil, mongo, nil, newFakeCache())

	enabled := true
	_, err := h.searchApps(ctxWithAccount("alice"), model.SearchAppsRequest{
		Query:            "weather",
		AssistantEnabled: &enabled,
		Size:             10,
	})
	require.NoError(t, err)

	require.Len(t, mongo.searchAppsCalls, 1)
	require.NotNil(t, mongo.searchAppsCalls[0].assistantEnabled)
	assert.Equal(t, true, *mongo.searchAppsCalls[0].assistantEnabled)
	assert.Equal(t, 10, mongo.searchAppsCalls[0].limit)
}

func TestHandler_SearchApps_EmptyQueryRejected(t *testing.T) {
	mongo := &fakeMongo{}
	h := newTestHandler(nil, mongo, nil, newFakeCache())

	_, err := h.searchApps(ctxWithAccount("alice"), model.SearchAppsRequest{Query: ""})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)

	assert.Len(t, mongo.searchAppsCalls, 0, "validation must short-circuit before backend call")
}

func TestHandler_SearchApps_WhitespaceQueryRejected(t *testing.T) {
	mongo := &fakeMongo{}
	h := newTestHandler(nil, mongo, nil, newFakeCache())

	_, err := h.searchApps(ctxWithAccount("alice"), model.SearchAppsRequest{Query: "   \t  "})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchApps_BackendErrorSanitized(t *testing.T) {
	mongo := &fakeMongo{searchAppsErr: errors.New("mongo down")}
	h := newTestHandler(nil, mongo, nil, newFakeCache())

	_, err := h.searchApps(ctxWithAccount("alice"), model.SearchAppsRequest{Query: "weather"})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeInternal, rerr.Code, "raw store error must not leak; sanitize to ErrInternal")
	assert.NotContains(t, rerr.Message, "mongo down", "internal error text must not surface to client")
}

func TestHandler_SearchApps_EmptyResultsReturnsEmptySlice(t *testing.T) {
	mongo := &fakeMongo{searchAppsResults: nil}
	h := newTestHandler(nil, mongo, nil, newFakeCache())

	resp, err := h.searchApps(ctxWithAccount("alice"), model.SearchAppsRequest{Query: "nope"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Apps, "must be empty slice, never nil — to marshal as [] not null")
	assert.Len(t, resp.Apps, 0)
}

func TestHandler_SearchApps_SizeClamped(t *testing.T) {
	mongo := &fakeMongo{}
	h := newTestHandler(nil, mongo, nil, newFakeCache())

	_, err := h.searchApps(ctxWithAccount("alice"), model.SearchAppsRequest{
		Query: "weather",
		Size:  500, // exceeds MaxDocCounts (100)
	})
	require.NoError(t, err)

	require.Len(t, mongo.searchAppsCalls, 1)
	assert.Equal(t, 100, mongo.searchAppsCalls[0].limit, "Size > MaxDocCounts must be clamped")
}

func TestHandler_SearchApps_NegativeSizeRejected(t *testing.T) {
	mongo := &fakeMongo{}
	h := newTestHandler(nil, mongo, nil, newFakeCache())

	_, err := h.searchApps(ctxWithAccount("alice"), model.SearchAppsRequest{
		Query: "weather",
		Size:  -1,
	})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchMessages_ScopedPartitioning(t *testing.T) {
	store := &fakeStore{}
	cache := newFakeCache()
	cache.store["alice"] = map[string]int64{"rr": 1_700_000_000_000}

	h := newTestHandler(store, nil, nil, cache)
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{
		Query:   "x",
		RoomIDs: []string{"r1", "rr", "r2"},
	})
	require.NoError(t, err)

	// Should emit: inline terms for [r1, r2] + restricted A+B for rr = 3 clauses.
	var body map[string]any
	require.NoError(t, json.Unmarshal(store.searchCalls[0].body, &body))
	filter := body["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
	shoulds := filter[1].(map[string]any)["bool"].(map[string]any)["should"].([]any)
	assert.Len(t, shoulds, 3)
}

// fakeUsers is a test double for SearchUsersClient.
type fakeUsers struct {
	calls   []string // queries received
	results []model.SearchUser
	err     error
}

func (f *fakeUsers) SearchUsers(_ context.Context, query string) ([]model.SearchUser, error) {
	f.calls = append(f.calls, query)
	if f.err != nil {
		return nil, f.err
	}
	return f.results, nil
}

func TestHandler_SearchUsers_Happy(t *testing.T) {
	fu := &fakeUsers{results: []model.SearchUser{
		{Account: "alice", EngName: "Alice Wang"},
		{Account: "bob", EngName: "Bob Chen"},
	}}
	h := newTestHandler(nil, nil, fu, newFakeCache())

	got, err := h.searchUsers(ctxWithAccount("alice"), model.SearchUsersRequest{Query: "alice"})
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, *got, 2)
	assert.Equal(t, "alice", (*got)[0].Account)

	require.Len(t, fu.calls, 1)
	assert.Equal(t, "alice", fu.calls[0])
}

func TestHandler_SearchUsers_EmptyQueryRejected(t *testing.T) {
	fu := &fakeUsers{}
	h := newTestHandler(nil, nil, fu, newFakeCache())

	_, err := h.searchUsers(ctxWithAccount("alice"), model.SearchUsersRequest{Query: ""})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)

	assert.Len(t, fu.calls, 0, "validation must short-circuit before backend call")
}

func TestHandler_SearchUsers_WhitespaceQueryRejected(t *testing.T) {
	fu := &fakeUsers{}
	h := newTestHandler(nil, nil, fu, newFakeCache())

	_, err := h.searchUsers(ctxWithAccount("alice"), model.SearchUsersRequest{Query: "   \t  "})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)

	assert.Len(t, fu.calls, 0)
}

func TestHandler_SearchUsers_BackendErrorSanitized(t *testing.T) {
	fu := &fakeUsers{err: errors.New("third-party down")}
	h := newTestHandler(nil, nil, fu, newFakeCache())

	_, err := h.searchUsers(ctxWithAccount("alice"), model.SearchUsersRequest{Query: "alice"})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeInternal, rerr.Code, "raw backend error must not leak")
	assert.NotContains(t, rerr.Message, "third-party down", "internal text must not surface to client")
}

func TestHandler_SearchUsers_EmptyResultsReturnsEmptySlice(t *testing.T) {
	fu := &fakeUsers{results: nil}
	h := newTestHandler(nil, nil, fu, newFakeCache())

	got, err := h.searchUsers(ctxWithAccount("alice"), model.SearchUsersRequest{Query: "nobody"})
	require.NoError(t, err)
	require.NotNil(t, got, "nil result must be normalised to empty slice")
	assert.Len(t, *got, 0)
}

func TestHandler_SearchUsers_QueryTrimmedBeforeBackendCall(t *testing.T) {
	fu := &fakeUsers{results: []model.SearchUser{{Account: "alice"}}}
	h := newTestHandler(nil, nil, fu, newFakeCache())

	_, err := h.searchUsers(ctxWithAccount("alice"), model.SearchUsersRequest{Query: "  alice  "})
	require.NoError(t, err)

	require.Len(t, fu.calls, 1)
	assert.Equal(t, "alice", fu.calls[0], "trimmed query must be forwarded to backend, not the raw padded string")
}

func TestHandler_SearchUsers_AccountExtractedForLogging(t *testing.T) {
	// Verify the method compiles and runs without panicking when the account
	// param is present — the account is used only for logging/metrics.
	fu := &fakeUsers{results: []model.SearchUser{{Account: "charlie"}}}
	h := newTestHandler(nil, nil, fu, newFakeCache())

	got, err := h.searchUsers(ctxWithAccount("charlie"), model.SearchUsersRequest{Query: "charlie"})
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Len(t, *got, 1)
}

func TestHandler_SearchMessages_HitProjection(t *testing.T) {
	// ES returns a hit; verify it projects into SearchMessage with the
	// ES-sourced fields populated. No Mongo enrichment — display fields
	// (user/room name) are resolved by the client via user-service lookups.
	store := &fakeStore{
		searchBody: json.RawMessage(`{"hits":{"total":{"value":1},"hits":[{"_source":{` +
			`"messageId":"m1","roomId":"r1","siteId":"site-a","userId":"u1",` +
			`"userAccount":"alice","content":"hello","createdAt":"2026-04-01T12:00:00Z"}}]}}`),
	}
	cache := newFakeCache()
	cache.store["alice"] = map[string]int64{}

	h := newTestHandler(store, nil, nil, cache)

	resp, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{Query: "hello"})
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.Total)
	require.Len(t, resp.Messages, 1)

	msg := resp.Messages[0]
	assert.Equal(t, "m1", msg.MessageID)
	assert.Equal(t, "r1", msg.RoomID)
	assert.Equal(t, "site-a", msg.SiteID)
	assert.Equal(t, "alice", msg.UserAccount)
	assert.Equal(t, "hello", msg.Content)
}

func TestHandler_SearchMessages_NoHitsReturnsEmpty(t *testing.T) {
	store := &fakeStore{}
	cache := newFakeCache()
	cache.store["alice"] = map[string]int64{}
	h := newTestHandler(store, nil, nil, cache)

	resp, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{Query: "nope"})
	require.NoError(t, err)
	assert.NotNil(t, resp.Messages, "empty slice, not nil — to marshal as [] not null")
	assert.Empty(t, resp.Messages)
	assert.EqualValues(t, 0, resp.Total)
}

func TestHandler_SearchMessages_WithRoomIDsBuildsScopedQuery(t *testing.T) {
	// Supply RoomIDs; verify the ES query uses scoped access clauses.
	// scopedAccessClauses (in query_messages.go) is the single classifier:
	// it iterates req.RoomIDs and splits each ID against the restricted
	// map. No handler-level pre-classification.
	store := &fakeStore{}
	cache := newFakeCache()
	cache.store["alice"] = map[string]int64{"rx": 1_700_000_000_000}
	mongo := &fakeMongo{}
	h := newTestHandler(store, mongo, nil, cache)

	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{
		Query:   "hello",
		RoomIDs: []string{"r1", "rx"},
	})
	require.NoError(t, err)

	// Verify ES was called — we can't directly inspect which clauses were
	// built without parsing the full query body, but we verify it was called.
	require.Len(t, store.searchCalls, 1)
	// The body must NOT be the global-search shape (which has only one should clause).
	var body map[string]any
	require.NoError(t, json.Unmarshal(store.searchCalls[0].body, &body))
	query := body["query"].(map[string]any)
	b := query["bool"].(map[string]any)
	filters := b["filter"].([]any)
	roomAccess := filters[1].(map[string]any)["bool"].(map[string]any)
	shoulds := roomAccess["should"].([]any)
	// rx is restricted → 2 clauses (A+B); r1 is unrestricted + terms-lookup AND → 1 clause
	assert.Greater(t, len(shoulds), 1, "scoped query must have more than 1 should clause")
}
