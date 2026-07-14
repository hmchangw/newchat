package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

func TestIsInCall(t *testing.T) {
	cases := []struct {
		name string
		p    msgraph.Presence
		want bool
	}{
		{"in a call", msgraph.Presence{Availability: "Busy", Activity: "InACall"}, true},
		{"conference", msgraph.Presence{Availability: "Busy", Activity: "InAConferenceCall"}, true},
		{"presenting", msgraph.Presence{Availability: "DoNotDisturb", Activity: "Presenting"}, true},
		{"available", msgraph.Presence{Availability: "Available", Activity: "Available"}, false},
		{"meeting not call", msgraph.Presence{Availability: "Busy", Activity: "InAMeeting"}, false},
		{"away", msgraph.Presence{Availability: "Away", Activity: "Away"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isInCall(tc.p))
		})
	}
}

func newTestReconciler(t *testing.T) (*reconciler, *MockactiveLister, *MockuserResolver, *MockpresenceReader, *MockexternalApplier, *MockinCallIndex, *MockidMapStore, *MockstatePublisher) {
	ctrl := gomock.NewController(t)
	active := NewMockactiveLister(ctrl)
	users := NewMockuserResolver(ctrl)
	pres := NewMockpresenceReader(ctrl)
	app := NewMockexternalApplier(ctrl)
	idx := NewMockinCallIndex(ctrl)
	idm := NewMockidMapStore(ctrl)
	pub := NewMockstatePublisher(ctrl)
	cfg := reconcileConfig{SiteID: "site-a", ExternalTTL: time.Minute}
	return newReconciler(active, users, pres, app, idx, idm, pub, cfg), active, users, pres, app, idx, idm, pub
}

func TestReconcile_CacheHit_SetsAndClears(t *testing.T) {
	r, active, users, pres, app, idx, idm, pub := newTestReconciler(t)
	ctx := context.Background()

	active.EXPECT().ActiveAccounts(ctx).Return([]string{"alice", "bob"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice", "bob"}).Return(map[string]string{"alice": "ida", "bob": "idb"}, nil)
	pres.EXPECT().GetPresencesByUserId(ctx, gomock.Len(2)).Return([]msgraph.Presence{
		{ID: "ida", Activity: "InACall"},
		{ID: "idb", Activity: "Available"},
	}, nil)
	idx.EXPECT().Members(ctx).Return([]string{"bob"}, nil) // bob was in-call last run

	app.EXPECT().SetExternal(ctx, "alice", model.StatusInCall, time.Minute).Return(true, model.StatusInCall, nil)
	idx.EXPECT().Add(ctx, "alice").Return(nil)
	pub.EXPECT().Publish(ctx, "alice", model.StatusInCall)

	app.EXPECT().SetExternal(ctx, "bob", model.StatusNone, time.Minute).Return(true, model.StatusOnline, nil)
	idx.EXPECT().Remove(ctx, "bob").Return(nil)
	pub.EXPECT().Publish(ctx, "bob", model.StatusOnline)

	// All accounts cached -> no Graph user lookup.
	users.EXPECT().ResolveAccountIDs(gomock.Any(), gomock.Any()).Times(0)

	require.NoError(t, r.run(ctx))
}

func TestReconcile_CacheMiss_FillsIdMap(t *testing.T) {
	r, active, users, pres, app, idx, idm, pub := newTestReconciler(t)
	ctx := context.Background()

	active.EXPECT().ActiveAccounts(ctx).Return([]string{"alice"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice"}).Return(map[string]string{}, nil) // miss
	// Batched, domain-agnostic prefix lookup; keyed by account (UPN correlation
	// happens inside the resolver).
	users.EXPECT().ResolveAccountIDs(ctx, []string{"alice"}).
		Return(map[string]string{"alice": "ida"}, nil)
	idm.EXPECT().Store(ctx, map[string]string{"alice": "ida"}).Return(nil) // permanent, no TTL
	pres.EXPECT().GetPresencesByUserId(ctx, []string{"ida"}).Return([]msgraph.Presence{
		{ID: "ida", Activity: "InACall"},
	}, nil)
	idx.EXPECT().Members(ctx).Return(nil, nil)
	app.EXPECT().SetExternal(ctx, "alice", model.StatusInCall, time.Minute).Return(true, model.StatusInCall, nil)
	idx.EXPECT().Add(ctx, "alice").Return(nil)
	pub.EXPECT().Publish(ctx, "alice", model.StatusInCall)

	require.NoError(t, r.run(ctx))
}

func TestReconcile_CacheMiss_UserNotFound_Skipped(t *testing.T) {
	r, active, users, pres, _, idx, idm, _ := newTestReconciler(t)
	ctx := context.Background()

	active.EXPECT().ActiveAccounts(ctx).Return([]string{"ghost"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"ghost"}).Return(map[string]string{}, nil)
	users.EXPECT().ResolveAccountIDs(ctx, []string{"ghost"}).Return(map[string]string{}, nil) // no AAD match
	// Nothing filled -> no Store, empty id set queried.
	pres.EXPECT().GetPresencesByUserId(ctx, gomock.Len(0)).Return(nil, nil)
	idx.EXPECT().Members(ctx).Return(nil, nil)

	require.NoError(t, r.run(ctx))
}

func TestReconcile_NoActiveAccounts_NoOp(t *testing.T) {
	r, active, _, pres, _, idx, idm, _ := newTestReconciler(t)
	ctx := context.Background()
	active.EXPECT().ActiveAccounts(ctx).Return(nil, nil)
	idm.EXPECT().Resolve(ctx, []string(nil)).Return(map[string]string{}, nil)
	pres.EXPECT().GetPresencesByUserId(ctx, gomock.Len(0)).Return(nil, nil)
	idx.EXPECT().Members(ctx).Return(nil, nil)
	require.NoError(t, r.run(ctx))
}

func TestReconcile_NoChange_NoPublish(t *testing.T) {
	r, active, _, pres, app, idx, idm, _ := newTestReconciler(t)
	ctx := context.Background()

	active.EXPECT().ActiveAccounts(ctx).Return([]string{"alice"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice"}).Return(map[string]string{"alice": "ida"}, nil)
	pres.EXPECT().GetPresencesByUserId(ctx, []string{"ida"}).Return([]msgraph.Presence{
		{ID: "ida", Activity: "InACall"},
	}, nil)
	idx.EXPECT().Members(ctx).Return([]string{"alice"}, nil) // already in-call
	app.EXPECT().SetExternal(ctx, "alice", model.StatusInCall, time.Minute).Return(false, model.StatusInCall, nil)
	idx.EXPECT().Add(ctx, "alice").Return(nil)
	// changed=false -> no Publish

	require.NoError(t, r.run(ctx))
}

var errBoom = errors.New("boom")

func TestReconcile_ActiveAccountsError(t *testing.T) {
	r, active, _, _, _, _, _, _ := newTestReconciler(t)
	ctx := context.Background()
	active.EXPECT().ActiveAccounts(ctx).Return(nil, errBoom)
	require.Error(t, r.run(ctx))
}

func TestReconcile_ResolveError(t *testing.T) {
	r, active, _, _, _, _, idm, _ := newTestReconciler(t)
	ctx := context.Background()
	active.EXPECT().ActiveAccounts(ctx).Return([]string{"alice"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice"}).Return(nil, errBoom)
	require.Error(t, r.run(ctx))
}

// A Graph lookup failure is logged and yields nothing — never fatal.
func TestReconcile_FillLookupError_Continues(t *testing.T) {
	r, active, users, pres, _, idx, idm, _ := newTestReconciler(t)
	ctx := context.Background()
	active.EXPECT().ActiveAccounts(ctx).Return([]string{"alice"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice"}).Return(map[string]string{}, nil)
	users.EXPECT().ResolveAccountIDs(ctx, []string{"alice"}).Return(nil, errBoom) // logged, skipped
	// alice unresolved -> empty id set, run still completes.
	pres.EXPECT().GetPresencesByUserId(ctx, gomock.Len(0)).Return(nil, nil)
	idx.EXPECT().Members(ctx).Return(nil, nil)
	require.NoError(t, r.run(ctx))
}

// Persisting the filled id-map is best-effort: a Store failure is logged, the
// in-memory map still serves the run.
func TestReconcile_FillStoreError_NonFatal(t *testing.T) {
	r, active, users, pres, app, idx, idm, pub := newTestReconciler(t)
	ctx := context.Background()
	active.EXPECT().ActiveAccounts(ctx).Return([]string{"alice"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice"}).Return(map[string]string{}, nil)
	users.EXPECT().ResolveAccountIDs(ctx, []string{"alice"}).
		Return(map[string]string{"alice": "ida"}, nil)
	idm.EXPECT().Store(ctx, map[string]string{"alice": "ida"}).Return(errBoom) // logged, non-fatal
	pres.EXPECT().GetPresencesByUserId(ctx, []string{"ida"}).Return([]msgraph.Presence{{ID: "ida", Activity: "InACall"}}, nil)
	idx.EXPECT().Members(ctx).Return(nil, nil)
	app.EXPECT().SetExternal(ctx, "alice", model.StatusInCall, time.Minute).Return(true, model.StatusInCall, nil)
	idx.EXPECT().Add(ctx, "alice").Return(nil)
	pub.EXPECT().Publish(ctx, "alice", model.StatusInCall)
	require.NoError(t, r.run(ctx))
}

func TestReconcile_GetPresencesError(t *testing.T) {
	r, active, _, pres, _, _, idm, _ := newTestReconciler(t)
	ctx := context.Background()
	active.EXPECT().ActiveAccounts(ctx).Return([]string{"alice"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice"}).Return(map[string]string{"alice": "ida"}, nil)
	pres.EXPECT().GetPresencesByUserId(ctx, []string{"ida"}).Return(nil, errBoom)
	require.Error(t, r.run(ctx))
}

func TestReconcile_MembersError(t *testing.T) {
	r, active, _, pres, _, idx, idm, _ := newTestReconciler(t)
	ctx := context.Background()
	active.EXPECT().ActiveAccounts(ctx).Return([]string{"alice"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice"}).Return(map[string]string{"alice": "ida"}, nil)
	pres.EXPECT().GetPresencesByUserId(ctx, []string{"ida"}).Return([]msgraph.Presence{{ID: "ida", Activity: "InACall"}}, nil)
	idx.EXPECT().Members(ctx).Return(nil, errBoom)
	require.Error(t, r.run(ctx))
}

// Per-account failures are logged and skipped, never failing the whole job.
func TestReconcile_PerAccountSetExternalError_Continues(t *testing.T) {
	r, active, _, pres, app, idx, idm, pub := newTestReconciler(t)
	ctx := context.Background()
	active.EXPECT().ActiveAccounts(ctx).Return([]string{"alice", "bob"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice", "bob"}).Return(map[string]string{"alice": "ida", "bob": "idb"}, nil)
	pres.EXPECT().GetPresencesByUserId(ctx, gomock.Len(2)).Return([]msgraph.Presence{
		{ID: "ida", Activity: "InACall"},
		{ID: "idb", Activity: "InACall"},
	}, nil)
	idx.EXPECT().Members(ctx).Return(nil, nil)
	// alice fails at SetExternal (logged, skipped — no Add/Publish for alice).
	app.EXPECT().SetExternal(ctx, "alice", model.StatusInCall, time.Minute).Return(false, model.StatusOffline, errBoom)
	// bob still processed.
	app.EXPECT().SetExternal(ctx, "bob", model.StatusInCall, time.Minute).Return(true, model.StatusInCall, nil)
	idx.EXPECT().Add(ctx, "bob").Return(nil)
	pub.EXPECT().Publish(ctx, "bob", model.StatusInCall)

	require.NoError(t, r.run(ctx))
}

func TestReconcile_IndexAddError_Continues(t *testing.T) {
	r, active, _, pres, app, idx, idm, _ := newTestReconciler(t)
	ctx := context.Background()
	active.EXPECT().ActiveAccounts(ctx).Return([]string{"alice"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice"}).Return(map[string]string{"alice": "ida"}, nil)
	pres.EXPECT().GetPresencesByUserId(ctx, []string{"ida"}).Return([]msgraph.Presence{{ID: "ida", Activity: "InACall"}}, nil)
	idx.EXPECT().Members(ctx).Return(nil, nil)
	app.EXPECT().SetExternal(ctx, "alice", model.StatusInCall, time.Minute).Return(true, model.StatusInCall, nil)
	idx.EXPECT().Add(ctx, "alice").Return(errBoom) // logged, not fatal
	require.NoError(t, r.run(ctx))
}

func TestReconcile_IndexRemoveError_Continues(t *testing.T) {
	r, active, _, pres, app, idx, idm, _ := newTestReconciler(t)
	ctx := context.Background()
	active.EXPECT().ActiveAccounts(ctx).Return([]string{"alice"}, nil)
	idm.EXPECT().Resolve(ctx, []string{"alice"}).Return(map[string]string{"alice": "ida"}, nil)
	// Teams reports nobody in a call, but the index still has bob -> clear path.
	pres.EXPECT().GetPresencesByUserId(ctx, []string{"ida"}).Return([]msgraph.Presence{{ID: "ida", Activity: "Available"}}, nil)
	idx.EXPECT().Members(ctx).Return([]string{"bob"}, nil)
	app.EXPECT().SetExternal(ctx, "bob", model.StatusNone, time.Minute).Return(true, model.StatusOffline, nil)
	idx.EXPECT().Remove(ctx, "bob").Return(errBoom) // logged, not fatal
	require.NoError(t, r.run(ctx))
}
