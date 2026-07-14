package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

type capturedPublish struct {
	subjects []string
	payloads [][]byte
}

func (c *capturedPublish) fn() presencestore.PublishFunc {
	return func(_ context.Context, subj string, data []byte) error {
		c.subjects = append(c.subjects, subj)
		c.payloads = append(c.payloads, data)
		return nil
	}
}

func fixedNow() func() time.Time {
	t := time.Unix(1700000000, 0).UTC()
	return func() time.Time { return t }
}

type testDeps struct {
	store   *MockPresenceStore
	userDir *MockUserDirectory
	peer    *MockPeerPresenceClient
	publish *capturedPublish
}

func newTestHandler(t *testing.T, batchMax int) (*Handler, *MockPresenceStore, *capturedPublish) {
	t.Helper()
	h, d := newTestHandlerDeps(t, batchMax)
	return h, d.store, d.publish
}

func newTestHandlerDeps(t *testing.T, batchMax int) (*Handler, testDeps) {
	t.Helper()
	ctrl := gomock.NewController(t)
	d := testDeps{
		store:   NewMockPresenceStore(ctrl),
		userDir: NewMockUserDirectory(ctrl),
		peer:    NewMockPeerPresenceClient(ctrl),
		publish: &capturedPublish{},
	}
	h := NewHandler(d.store, d.userDir, d.peer, d.publish.fn(), "site-a", batchMax)
	h.now = fixedNow()
	return h, d
}

func TestHandler_Hello_PublishesOnChange(t *testing.T) {
	h, store, cap := newTestHandler(t, 100)
	store.EXPECT().SetActivity(gomock.Any(), "alice", "c1", false).
		Return(true, model.StatusOnline, nil)

	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	require.NoError(t, h.Hello(c, model.Hello{ConnID: "c1"}))
	require.Len(t, cap.subjects, 1)
	assert.Equal(t, "chat.user.presence.state.alice", cap.subjects[0])
}

func TestHandler_Hello_MissingConnID(t *testing.T) {
	h, _, _ := newTestHandler(t, 100)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	err := h.Hello(c, model.Hello{ConnID: ""})
	require.Error(t, err)
	var re *errcode.Error
	require.ErrorAs(t, err, &re, "malformed input must surface as a bad-request route error")
}

func TestHandler_Ping_PublishesOnFirstSight(t *testing.T) {
	h, store, cap := newTestHandler(t, 100)
	store.EXPECT().Ping(gomock.Any(), "alice", "c1").
		Return(true, model.StatusOnline, nil)

	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	require.NoError(t, h.Ping(c, model.Ping{ConnID: "c1"}))
	require.Len(t, cap.subjects, 1)
	assert.Equal(t, "chat.user.presence.state.alice", cap.subjects[0])
}

func TestHandler_Ping_SuppressesWhenUnchanged(t *testing.T) {
	h, store, cap := newTestHandler(t, 100)
	store.EXPECT().Ping(gomock.Any(), "alice", "c1").
		Return(false, model.StatusOnline, nil)

	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	require.NoError(t, h.Ping(c, model.Ping{ConnID: "c1"}))
	assert.Empty(t, cap.subjects)
}

func TestHandler_Ping_MissingConnID(t *testing.T) {
	h, _, _ := newTestHandler(t, 100)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	err := h.Ping(c, model.Ping{ConnID: ""})
	require.Error(t, err)
	var re *errcode.Error
	require.ErrorAs(t, err, &re)
}

func TestHandler_Activity_PublishesOnChange(t *testing.T) {
	h, store, cap := newTestHandler(t, 100)
	store.EXPECT().SetActivity(gomock.Any(), "alice", "c1", true).
		Return(true, model.StatusAway, nil)

	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	require.NoError(t, h.Activity(c, model.Activity{ConnID: "c1", Away: true}))
	require.Len(t, cap.subjects, 1)
	assert.Equal(t, "chat.user.presence.state.alice", cap.subjects[0])
}

func TestHandler_Activity_SuppressesWhenUnchanged(t *testing.T) {
	h, store, cap := newTestHandler(t, 100)
	store.EXPECT().SetActivity(gomock.Any(), "alice", "c1", true).
		Return(false, model.StatusAway, nil)

	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	require.NoError(t, h.Activity(c, model.Activity{ConnID: "c1", Away: true}))
	assert.Empty(t, cap.subjects)
}

func TestHandler_Activity_MissingConnID(t *testing.T) {
	h, _, _ := newTestHandler(t, 100)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	err := h.Activity(c, model.Activity{ConnID: ""})
	require.Error(t, err)
	var re *errcode.Error
	require.ErrorAs(t, err, &re)
}

func TestHandler_Bye_PublishesOnChange(t *testing.T) {
	h, store, cap := newTestHandler(t, 100)
	store.EXPECT().RemoveConnection(gomock.Any(), "alice", "c1").
		Return(true, model.StatusOffline, nil)

	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	require.NoError(t, h.Bye(c, model.ByeRequest{ConnID: "c1"}))
	require.Len(t, cap.subjects, 1)
	assert.Equal(t, "chat.user.presence.state.alice", cap.subjects[0])
}

func TestHandler_SetManual_Valid(t *testing.T) {
	h, store, cap := newTestHandler(t, 100)
	store.EXPECT().SetManual(gomock.Any(), "alice", model.StatusBusy).
		Return(true, model.StatusBusy, nil)

	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	resp, err := h.SetManual(c, model.ManualStatusRequest{Status: model.StatusBusy})
	require.NoError(t, err)
	assert.Equal(t, "alice", resp.Account)
	assert.Equal(t, model.StatusBusy, resp.Status)
	assert.Equal(t, model.StatusBusy, resp.Effective)
	require.Len(t, cap.subjects, 1)
}

func TestHandler_SetManual_InvalidStatus(t *testing.T) {
	h, _, _ := newTestHandler(t, 100)
	c := natsrouter.NewContext(map[string]string{"account": "alice"})
	_, err := h.SetManual(c, model.ManualStatusRequest{Status: model.PresenceStatus("bogus")})
	require.Error(t, err)
	var re *errcode.Error
	require.ErrorAs(t, err, &re)
}

func TestHandler_QueryBatch_OverLimit(t *testing.T) {
	h, _, _ := newTestHandler(t, 2)
	c := natsrouter.NewContext(nil)
	_, err := h.QueryBatch(c, model.PresenceQuery{Accounts: []string{"a", "b", "c"}})
	require.Error(t, err)
	var re *errcode.Error
	require.ErrorAs(t, err, &re)
}

func TestHandler_QueryBatch_Empty(t *testing.T) {
	h, _, _ := newTestHandler(t, 100)
	c := natsrouter.NewContext(nil)
	resp, err := h.QueryBatch(c, model.PresenceQuery{Accounts: nil})
	require.NoError(t, err)
	assert.Empty(t, resp.States)
}

// QueryBatchPeer is the server-to-server leaf: pure local lookup, no fan-out.
func TestHandler_QueryBatchPeer_MapsStatuses(t *testing.T) {
	h, store, _ := newTestHandler(t, 100)
	store.EXPECT().BatchGet(gomock.Any(), []string{"alice", "bob"}).
		Return(map[string]model.PresenceStatus{"alice": model.StatusOnline, "bob": model.StatusOffline}, nil)

	c := natsrouter.NewContext(nil)
	resp, err := h.QueryBatchPeer(c, model.PresenceQuery{Accounts: []string{"alice", "bob"}})
	require.NoError(t, err)
	require.Len(t, resp.States, 2)
	assert.Equal(t, "alice", resp.States[0].Account)
	assert.Equal(t, model.StatusOnline, resp.States[0].Status)
	assert.Equal(t, "site-a", resp.States[0].SiteID)
	assert.Equal(t, model.StatusOffline, resp.States[1].Status)
}

func TestHandler_QueryBatchPeer_OverLimit(t *testing.T) {
	h, _, _ := newTestHandler(t, 2)
	c := natsrouter.NewContext(nil)
	_, err := h.QueryBatchPeer(c, model.PresenceQuery{Accounts: []string{"a", "b", "c"}})
	require.Error(t, err)
	var re *errcode.Error
	require.ErrorAs(t, err, &re)
}

func TestHandler_QueryBatchPeer_StoreError(t *testing.T) {
	h, store, _ := newTestHandler(t, 100)
	store.EXPECT().BatchGet(gomock.Any(), []string{"alice"}).
		Return(nil, errors.New("valkey down"))
	c := natsrouter.NewContext(nil)
	_, err := h.QueryBatchPeer(c, model.PresenceQuery{Accounts: []string{"alice"}})
	require.Error(t, err)
}

// QueryBatch (client entry): resolves home sites, serves local from the store,
// fans out to peers, aggregates.
func TestHandler_QueryBatch_AllLocal(t *testing.T) {
	h, d := newTestHandlerDeps(t, 100)
	d.userDir.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice", "bob"}).
		Return([]model.User{{Account: "alice", SiteID: "site-a"}, {Account: "bob", SiteID: "site-a"}}, nil)
	d.store.EXPECT().BatchGet(gomock.Any(), []string{"alice", "bob"}).
		Return(map[string]model.PresenceStatus{"alice": model.StatusOnline, "bob": model.StatusAway}, nil)

	c := natsrouter.NewContext(nil)
	resp, err := h.QueryBatch(c, model.PresenceQuery{Accounts: []string{"alice", "bob"}})
	require.NoError(t, err)
	require.Len(t, resp.States, 2)
	assert.Equal(t, model.StatusOnline, resp.States[0].Status)
	assert.Equal(t, "site-a", resp.States[0].SiteID)
	assert.Equal(t, model.StatusAway, resp.States[1].Status)
}

func TestHandler_QueryBatch_MixedSitesFanOut(t *testing.T) {
	h, d := newTestHandlerDeps(t, 100)
	d.userDir.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice", "carol"}).
		Return([]model.User{{Account: "alice", SiteID: "site-a"}, {Account: "carol", SiteID: "site-b"}}, nil)
	d.store.EXPECT().BatchGet(gomock.Any(), []string{"alice"}).
		Return(map[string]model.PresenceStatus{"alice": model.StatusOnline}, nil)
	d.peer.EXPECT().QueryPeer(gomock.Any(), "site-b", []string{"carol"}).
		Return([]model.PresenceState{{Account: "carol", SiteID: "site-b", Status: model.StatusBusy}}, nil)

	c := natsrouter.NewContext(nil)
	resp, err := h.QueryBatch(c, model.PresenceQuery{Accounts: []string{"alice", "carol"}})
	require.NoError(t, err)
	require.Len(t, resp.States, 2)
	// Response preserves request order.
	assert.Equal(t, "alice", resp.States[0].Account)
	assert.Equal(t, model.StatusOnline, resp.States[0].Status)
	assert.Equal(t, "site-a", resp.States[0].SiteID)
	assert.Equal(t, "carol", resp.States[1].Account)
	assert.Equal(t, model.StatusBusy, resp.States[1].Status)
	assert.Equal(t, "site-b", resp.States[1].SiteID)
}

func TestHandler_QueryBatch_UnknownAccountDefaultsOffline(t *testing.T) {
	h, d := newTestHandlerDeps(t, 100)
	// Directory has no record for "ghost"; no store or peer call should happen.
	d.userDir.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"ghost"}).
		Return([]model.User{}, nil)

	c := natsrouter.NewContext(nil)
	resp, err := h.QueryBatch(c, model.PresenceQuery{Accounts: []string{"ghost"}})
	require.NoError(t, err)
	require.Len(t, resp.States, 1)
	assert.Equal(t, "ghost", resp.States[0].Account)
	assert.Equal(t, model.StatusOffline, resp.States[0].Status)
	assert.Equal(t, "site-a", resp.States[0].SiteID)
}

func TestHandler_QueryBatch_PeerErrorDegradesToOffline(t *testing.T) {
	h, d := newTestHandlerDeps(t, 100)
	d.userDir.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice", "carol"}).
		Return([]model.User{{Account: "alice", SiteID: "site-a"}, {Account: "carol", SiteID: "site-b"}}, nil)
	d.store.EXPECT().BatchGet(gomock.Any(), []string{"alice"}).
		Return(map[string]model.PresenceStatus{"alice": model.StatusOnline}, nil)
	d.peer.EXPECT().QueryPeer(gomock.Any(), "site-b", []string{"carol"}).
		Return(nil, errors.New("peer timeout"))

	c := natsrouter.NewContext(nil)
	resp, err := h.QueryBatch(c, model.PresenceQuery{Accounts: []string{"alice", "carol"}})
	require.NoError(t, err) // peer failure degrades, does not fail the query
	require.Len(t, resp.States, 2)
	assert.Equal(t, model.StatusOnline, resp.States[0].Status)
	assert.Equal(t, "carol", resp.States[1].Account)
	assert.Equal(t, model.StatusOffline, resp.States[1].Status)
	assert.Equal(t, "site-b", resp.States[1].SiteID) // home site still reported
}

func TestHandler_QueryBatch_LocalStoreErrorFails(t *testing.T) {
	h, d := newTestHandlerDeps(t, 100)
	d.userDir.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).
		Return([]model.User{{Account: "alice", SiteID: "site-a"}}, nil)
	d.store.EXPECT().BatchGet(gomock.Any(), []string{"alice"}).
		Return(nil, errors.New("valkey down"))

	c := natsrouter.NewContext(nil)
	_, err := h.QueryBatch(c, model.PresenceQuery{Accounts: []string{"alice"}})
	require.Error(t, err)
}

func TestHandler_QueryBatch_DirectoryErrorFails(t *testing.T) {
	h, d := newTestHandlerDeps(t, 100)
	d.userDir.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).
		Return(nil, errors.New("mongo down"))

	c := natsrouter.NewContext(nil)
	_, err := h.QueryBatch(c, model.PresenceQuery{Accounts: []string{"alice"}})
	require.Error(t, err)
}
