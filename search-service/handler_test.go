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

func newTestHandler(store SearchStore, cache RestrictedRoomCache) *handler {
	return newHandler(store, cache, handlerConfig{
		DocCounts:               25,
		MaxDocCounts:            100,
		RestrictedRoomsCacheTTL: 5 * time.Minute,
		RecentWindow:            365 * 24 * time.Hour,
	})
}

func ctxWithAccount(account string) *natsrouter.Context {
	return natsrouter.NewContext(map[string]string{"account": account})
}

func TestHandler_SearchMessages_CacheHitUnrestricted(t *testing.T) {
	store := &fakeStore{}
	cache := newFakeCache()
	cache.store["alice"] = map[string]int64{} // empty restricted → cache hit

	h := newTestHandler(store, cache)

	resp, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{SearchText: "hi"})
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

	h := newTestHandler(store, cache)
	resp, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{SearchText: "hi"})
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

	h := newTestHandler(store, cache)
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{SearchText: "hi"})
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

	h := newTestHandler(store, cache)
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{SearchText: "hi"})
	require.Error(t, err)

	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeInternal, rerr.Code)
}

func TestHandler_SearchMessages_ESSearchError(t *testing.T) {
	store := &fakeStore{searchErr: errors.New("es failed")}
	cache := newFakeCache()
	cache.store["alice"] = map[string]int64{}

	h := newTestHandler(store, cache)
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{SearchText: "hi"})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeInternal, rerr.Code)
}

func TestHandler_SearchMessages_EmptySearchText(t *testing.T) {
	h := newTestHandler(&fakeStore{}, newFakeCache())
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchMessages_NegativeSizeRejected(t *testing.T) {
	h := newTestHandler(&fakeStore{}, newFakeCache())
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{SearchText: "x", Size: -1})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchMessages_SizeClamped(t *testing.T) {
	store := &fakeStore{}
	cache := newFakeCache()
	cache.store["alice"] = map[string]int64{}

	h := newHandler(store, cache, handlerConfig{
		DocCounts:               25,
		MaxDocCounts:            50,
		RestrictedRoomsCacheTTL: time.Minute,
		RecentWindow:            time.Hour,
	})
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{SearchText: "x", Size: 1000})
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
	h := newTestHandler(store, cache)

	resp, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{SearchText: "x"})
	require.NoError(t, err)
	assert.EqualValues(t, 0, resp.Total)
	assert.Empty(t, resp.Results)

	// empty restricted map should be cached to prevent miss-storm
	v, hit := cache.store["alice"]
	assert.True(t, hit)
	assert.Empty(t, v)
}

func TestHandler_SearchRooms_ScopeAllHappyPath(t *testing.T) {
	store := &fakeStore{
		searchBody: json.RawMessage(`{"hits":{"total":{"value":1},"hits":[{"_source":{"roomId":"r1","roomName":"general","roomType":"p","userAccount":"alice","siteId":"site-a","joinedAt":"2026-04-01T00:00:00Z"}}]}}`),
	}
	h := newTestHandler(store, newFakeCache())

	resp, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{SearchText: "general"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, resp.Total)
	require.Len(t, resp.Results, 1)
	assert.Equal(t, "r1", resp.Results[0].RoomID)

	require.Len(t, store.searchCalls, 1)
	assert.Equal(t, []string{SpotlightIndex}, store.searchCalls[0].indices)
}

func TestHandler_SearchRooms_ScopeAppRejected(t *testing.T) {
	h := newTestHandler(&fakeStore{}, newFakeCache())
	_, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{SearchText: "x", Scope: scopeApp})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
	assert.Contains(t, rerr.Message, "scope=app")
}

func TestHandler_SearchRooms_UnknownScopeRejected(t *testing.T) {
	h := newTestHandler(&fakeStore{}, newFakeCache())
	_, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{SearchText: "x", Scope: "zzz"})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchRooms_EmptySearchText(t *testing.T) {
	h := newTestHandler(&fakeStore{}, newFakeCache())
	_, err := h.searchRooms(ctxWithAccount("alice"), model.SearchRoomsRequest{})
	require.Error(t, err)
	var rerr *natsrouter.RouteError
	require.True(t, errors.As(err, &rerr))
	assert.Equal(t, natsrouter.CodeBadRequest, rerr.Code)
}

func TestHandler_SearchMessages_ScopedPartitioning(t *testing.T) {
	store := &fakeStore{}
	cache := newFakeCache()
	cache.store["alice"] = map[string]int64{"rr": 1_700_000_000_000}

	h := newTestHandler(store, cache)
	_, err := h.searchMessages(ctxWithAccount("alice"), model.SearchMessagesRequest{
		SearchText: "x",
		RoomIDs:    []string{"r1", "rr", "r2"},
	})
	require.NoError(t, err)

	// Should emit: inline terms for [r1, r2] + restricted A+B for rr = 3 clauses.
	var body map[string]any
	require.NoError(t, json.Unmarshal(store.searchCalls[0].body, &body))
	filter := body["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
	shoulds := filter[1].(map[string]any)["bool"].(map[string]any)["should"].([]any)
	assert.Len(t, shoulds, 3)
}
