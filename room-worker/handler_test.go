package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errnats"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeysender"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
)

type publishedMsg struct {
	subj string
	data []byte
}

func TestHandler_ProcessRoleUpdate_Promote(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	store.EXPECT().AddRole(gomock.Any(), "bob", "r1", model.RoleOwner).Return(nil)
	store.EXPECT().GetSubscription(gomock.Any(), "bob", "r1").
		Return(&model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", SiteID: "site-a", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "bob").
		Return(&model.User{ID: "u2", Account: "bob", SiteID: "site-a"}, nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner, Timestamp: 1}
	data, _ := json.Marshal(req)
	if err := h.processRoleUpdate(context.Background(), data); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(published) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(published))
	}
	if published[0].subj != "chat.user.bob.event.subscription.update" {
		t.Errorf("subject = %q, want subscription update for bob", published[0].subj)
	}

	var evt model.SubscriptionUpdateEvent
	if err := json.Unmarshal(published[0].data, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if evt.Action != "role_updated" {
		t.Errorf("action = %q, want role_updated", evt.Action)
	}
	if evt.UserID != "u2" {
		t.Errorf("userID = %q, want u2", evt.UserID)
	}
	if !slices.Contains(evt.Subscription.Roles, model.RoleOwner) {
		t.Errorf("subscription roles = %v, want to contain owner", evt.Subscription.Roles)
	}
	if !slices.Contains(evt.Subscription.Roles, model.RoleMember) {
		t.Errorf("subscription roles = %v, want to contain member", evt.Subscription.Roles)
	}
	if evt.Timestamp <= 0 {
		t.Error("expected Timestamp > 0")
	}
}

func TestHandler_ProcessRoleUpdate_Demote(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	store.EXPECT().AddRole(gomock.Any(), "bob", "r1", model.RoleMember).Return(nil)
	store.EXPECT().RemoveRole(gomock.Any(), "bob", "r1", model.RoleOwner).Return(nil)
	store.EXPECT().GetSubscription(gomock.Any(), "bob", "r1").
		Return(&model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", SiteID: "site-a", Roles: []model.Role{model.RoleMember}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "bob").
		Return(&model.User{ID: "u2", Account: "bob", SiteID: "site-a"}, nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleMember, Timestamp: 1}
	data, _ := json.Marshal(req)
	if err := h.processRoleUpdate(context.Background(), data); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(published) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(published))
	}

	var evt model.SubscriptionUpdateEvent
	if err := json.Unmarshal(published[0].data, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if evt.Action != "role_updated" {
		t.Errorf("action = %q, want role_updated", evt.Action)
	}
	if slices.Contains(evt.Subscription.Roles, model.RoleOwner) {
		t.Errorf("subscription roles = %v, should not contain owner after demote", evt.Subscription.Roles)
	}
	if !slices.Contains(evt.Subscription.Roles, model.RoleMember) {
		t.Errorf("subscription roles = %v, want to contain member", evt.Subscription.Roles)
	}
}

func TestHandler_ProcessRoleUpdate_CrossSite(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	store.EXPECT().AddRole(gomock.Any(), "bob", "r1", model.RoleOwner).Return(nil)
	store.EXPECT().GetSubscription(gomock.Any(), "bob", "r1").
		Return(&model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", SiteID: "site-a", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)
	// User is on site-b (different from handler's site-a) → cross-site
	store.EXPECT().GetUser(gomock.Any(), "bob").
		Return(&model.User{ID: "u2", Account: "bob", SiteID: "site-b"}, nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner, Timestamp: 1}
	data, _ := json.Marshal(req)
	if err := h.processRoleUpdate(context.Background(), data); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(published) != 2 {
		t.Fatalf("expected 2 publishes, got %d", len(published))
	}
	if published[0].subj != "chat.user.bob.event.subscription.update" {
		t.Errorf("first subject = %q, want subscription update", published[0].subj)
	}

	wantOutboxSubj := "outbox.site-a.to.site-b.role_updated"
	if published[1].subj != wantOutboxSubj {
		t.Errorf("second subject = %q, want %q", published[1].subj, wantOutboxSubj)
	}

	var outbox model.OutboxEvent
	if err := json.Unmarshal(published[1].data, &outbox); err != nil {
		t.Fatalf("unmarshal outbox event: %v", err)
	}
	if outbox.Type != "role_updated" {
		t.Errorf("outbox type = %q, want role_updated", outbox.Type)
	}

	var innerEvt model.SubscriptionUpdateEvent
	if err := json.Unmarshal(outbox.Payload, &innerEvt); err != nil {
		t.Fatalf("unmarshal inner event: %v", err)
	}
	if !slices.Contains(innerEvt.Subscription.Roles, model.RoleOwner) {
		t.Errorf("inner subscription roles = %v, want to contain owner", innerEvt.Subscription.Roles)
	}
	if !slices.Contains(innerEvt.Subscription.Roles, model.RoleMember) {
		t.Errorf("inner subscription roles = %v, want to contain member", innerEvt.Subscription.Roles)
	}
}

// --- Error-path tests for processRoleUpdate ---

func TestHandler_ProcessRoleUpdate_FallsBackToNowOnInvalidTimestamp(t *testing.T) {
	// A missing timestamp should not short-circuit the handler. We confirm
	// processing reached the store layer by stubbing the first store call to
	// return a downstream error and asserting the error is NOT the timestamp
	// rejection.
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().AddRole(gomock.Any(), "bob", "r1", model.RoleOwner).Return(fmt.Errorf("db error"))
	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
		return nil
	}, testKeyStore, testKeySender)
	req := model.UpdateRoleRequest{
		RoomID:    "r1",
		Account:   "bob",
		NewRole:   model.RoleOwner,
		Timestamp: 0,
	}
	data, _ := json.Marshal(req)
	err := h.processRoleUpdate(context.Background(), data)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "timestamp must be > 0")
	assert.Contains(t, err.Error(), "add owner role")
}

func TestHandler_ProcessRoleUpdate_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error {
		t.Fatal("publish should not be called")
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	err := h.processRoleUpdate(context.Background(), []byte("not json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestHandler_ProcessRoleUpdate_AddRoleError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().AddRole(gomock.Any(), "bob", "r1", model.RoleOwner).Return(fmt.Errorf("db error"))

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error {
		t.Fatal("publish should not be called")
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner, Timestamp: 1}
	data, _ := json.Marshal(req)
	err := h.processRoleUpdate(context.Background(), data)
	if err == nil {
		t.Fatal("expected error for AddRole failure")
	}
}

func TestHandler_ProcessRoleUpdate_RemoveRoleError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().AddRole(gomock.Any(), "bob", "r1", model.RoleMember).Return(nil)
	store.EXPECT().RemoveRole(gomock.Any(), "bob", "r1", model.RoleOwner).Return(fmt.Errorf("db error"))

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error {
		t.Fatal("publish should not be called")
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleMember, Timestamp: 1}
	data, _ := json.Marshal(req)
	err := h.processRoleUpdate(context.Background(), data)
	if err == nil {
		t.Fatal("expected error for RemoveRole failure")
	}
}

func TestHandler_ProcessRoleUpdate_GetSubscriptionError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().AddRole(gomock.Any(), "bob", "r1", model.RoleOwner).Return(nil)
	store.EXPECT().GetSubscription(gomock.Any(), "bob", "r1").Return(nil, fmt.Errorf("db error"))

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error {
		t.Fatal("publish should not be called")
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner, Timestamp: 1}
	data, _ := json.Marshal(req)
	err := h.processRoleUpdate(context.Background(), data)
	if err == nil {
		t.Fatal("expected error for GetSubscription failure")
	}
}

func TestHandler_ProcessRoleUpdate_PublishError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().AddRole(gomock.Any(), "bob", "r1", model.RoleOwner).Return(nil)
	store.EXPECT().GetSubscription(gomock.Any(), "bob", "r1").
		Return(&model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", SiteID: "site-a", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error {
		return fmt.Errorf("nats down")
	}}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner, Timestamp: 1}
	data, _ := json.Marshal(req)
	err := h.processRoleUpdate(context.Background(), data)
	if err == nil {
		t.Fatal("expected error for publish failure")
	}
}

func TestHandler_ProcessRoleUpdate_UnsupportedRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error {
		t.Fatal("publish should not be called")
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: "admin", Timestamp: 1}
	data, _ := json.Marshal(req)
	err := h.processRoleUpdate(context.Background(), data)
	if err == nil {
		t.Fatal("expected error for unsupported role")
	}
}

func TestHandler_ProcessRoleUpdate_PropagatesRequestID(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	store.EXPECT().AddRole(gomock.Any(), "bob", "r1", model.RoleOwner).Return(nil)
	store.EXPECT().GetSubscription(gomock.Any(), "bob", "r1").
		Return(&model.Subscription{ID: "s1", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", SiteID: "site-a", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "bob").
		Return(&model.User{ID: "u2", Account: "bob", SiteID: "site-a"}, nil)

	var capturedCtx context.Context
	publish := func(ctx context.Context, subj string, data []byte, msgID string) error {
		capturedCtx = ctx
		return nil
	}
	h := NewHandler(store, "site1", publish, testKeyStore, testKeySender)

	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner, Timestamp: 1}
	reqData, _ := json.Marshal(req)
	err := h.processRoleUpdate(ctx, reqData)
	require.NoError(t, err)
	require.NotNil(t, capturedCtx, "publish wrapper must receive a non-nil ctx")
	assert.Equal(t, testRequestID, natsutil.RequestIDFromContext(capturedCtx),
		"publish wrapper must receive ctx that still carries the request ID")
}

// --- processRemoveMember tests ---

func TestHandler_ProcessRemoveMember_FallsBackToNowOnInvalidTimestamp(t *testing.T) {
	// A missing timestamp should not short-circuit the handler. We confirm
	// processing reached the store layer by stubbing the first store call to
	// return a downstream error and asserting the error is NOT the timestamp
	// rejection.
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().GetUserWithMembership(gomock.Any(), "r1", "alice").Return(nil, fmt.Errorf("db error"))
	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error {
		return nil
	}, testKeyStore, testKeySender)
	req := model.RemoveMemberRequest{
		RoomID:    "r1",
		Account:   "alice",
		Requester: "alice",
		Timestamp: 0,
		RoomType:  model.RoomTypeChannel,
	}
	data, _ := json.Marshal(req)
	err := h.processRemoveMember(context.Background(), data)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "timestamp must be > 0")
}

func TestHandler_ProcessRemoveMember_SelfLeave_IndividualOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	const (
		roomID  = "room-1"
		account = "alice"
		siteID  = "site-a"
	)

	userResult := &UserWithMembership{
		User: model.User{
			ID:          "u1",
			Account:     account,
			SiteID:      siteID,
			EngName:     "Alice",
			ChineseName: "愛麗絲",
		},
		HasOrgMembership: false,
	}

	store.EXPECT().
		GetUserWithMembership(gomock.Any(), roomID, account).
		Return(userResult, nil)
	store.EXPECT().
		DeleteSubscription(gomock.Any(), roomID, account).
		Return(int64(1), nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u1").
		Return(nil)
	store.EXPECT().
		ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	store.EXPECT().
		GetSubscriptionAccounts(gomock.Any(), roomID).Return(nil, nil)

	var published []publishedMsg
	h := NewHandler(store, siteID, func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, testKeyStore, testKeySender)

	// Self-leave: Requester == Account
	req := model.RemoveMemberRequest{RoomID: roomID, Requester: account, Account: account, Timestamp: 1, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.NoError(t, err)

	// Expect: subscription update + member change event + local INBOX + system message = 4 publishes
	assert.Len(t, published, 4, "expected 4 publishes: sub update, member event, local INBOX, sys msg")

	subjSet := make(map[string]bool)
	for _, p := range published {
		subjSet[p.subj] = true
	}

	assert.True(t, subjSet[subject.SubscriptionUpdate(account)], "expected subscription update published")
	assert.True(t, subjSet[subject.MemberEvent(roomID)], "expected member event published")
	assert.True(t, subjSet[subject.InboxMemberRemoved(siteID)], "expected local INBOX member_removed published")

	for _, p := range published {
		if p.subj != subject.SubscriptionUpdate(account) {
			continue
		}
		var evt model.SubscriptionUpdateEvent
		require.NoError(t, json.Unmarshal(p.data, &evt))
		assert.Equal(t, model.RoomTypeChannel, evt.Subscription.RoomType, "subscription update should carry RoomType")
	}

	// Verify timestamps on all events
	for _, p := range published {
		var raw map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(p.data, &raw))
		tsRaw, ok := raw["timestamp"]
		if !ok {
			continue // sys msg may not have timestamp at top level
		}
		var ts int64
		require.NoError(t, json.Unmarshal(tsRaw, &ts))
		assert.NotZero(t, ts, "timestamp should be non-zero for subject %s", p.subj)
	}
}

func TestHandler_ProcessRemoveMember_SelfLeave_DualMembership(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	const (
		roomID  = "room-1"
		account = "alice"
		siteID  = "site-a"
	)

	userResult := &UserWithMembership{
		User: model.User{
			ID:          "u1",
			Account:     account,
			SiteID:      siteID,
			EngName:     "Alice",
			ChineseName: "愛",
		},
		HasOrgMembership: true,
		Roles:            []model.Role{model.RoleMember},
	}

	// Only DeleteRoomMember(individual) called — no subscription delete, no events,
	// no role change (target is not an owner).
	store.EXPECT().
		GetUserWithMembership(gomock.Any(), roomID, account).
		Return(userResult, nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u1").
		Return(nil)

	var published []publishedMsg
	h := NewHandler(store, siteID, func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, testKeyStore, testKeySender)

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: account, Account: account, Timestamp: 1, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.NoError(t, err)

	assert.Empty(t, published, "expected no publishes for dual-membership self-leave")
}

func TestHandler_ProcessRemoveMember_DualMembership_OwnerDemoted(t *testing.T) {
	// Dual-member who also holds the owner role must be demoted when their
	// individual source is removed — org members cannot be owners.
	cases := []struct {
		name      string
		requester string
	}{
		{"self-leave", "alice"},
		{"owner-removes-other", "bob"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockSubscriptionStore(ctrl)

			const (
				roomID  = "room-1"
				account = "alice"
				siteID  = "site-a"
			)

			userResult := &UserWithMembership{
				User:             model.User{ID: "u1", Account: account, SiteID: siteID, EngName: "Alice", ChineseName: "愛"},
				HasOrgMembership: true,
				Roles:            []model.Role{model.RoleOwner, model.RoleMember},
			}

			gomock.InOrder(
				store.EXPECT().
					GetUserWithMembership(gomock.Any(), roomID, account).
					Return(userResult, nil),
				store.EXPECT().
					DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u1").
					Return(nil),
				store.EXPECT().
					RemoveRole(gomock.Any(), account, roomID, model.RoleOwner).
					Return(nil),
			)

			var published []publishedMsg
			h := NewHandler(store, siteID, func(_ context.Context, subj string, data []byte, _ string) error {
				published = append(published, publishedMsg{subj: subj, data: data})
				return nil
			}, testKeyStore, testKeySender)

			req := model.RemoveMemberRequest{RoomID: roomID, Requester: tc.requester, Account: account, Timestamp: 1, RoomType: model.RoomTypeChannel}
			data, _ := json.Marshal(req)

			err := h.processRemoveMember(context.Background(), data)
			require.NoError(t, err)
			assert.Empty(t, published, "dual-membership removal emits no events")
		})
	}
}

func TestHandler_ProcessRemoveMember_OwnerRemovesIndividual(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	const (
		roomID    = "room-1"
		account   = "bob"
		requester = "alice"
		siteID    = "site-a"
	)

	userResult := &UserWithMembership{
		User: model.User{
			ID:          "u2",
			Account:     account,
			SiteID:      siteID,
			EngName:     "Bob",
			ChineseName: "鮑伯",
		},
		HasOrgMembership: false,
	}

	store.EXPECT().
		GetUserWithMembership(gomock.Any(), roomID, account).
		Return(userResult, nil)
	store.EXPECT().
		DeleteSubscription(gomock.Any(), roomID, account).
		Return(int64(1), nil)
	// Owner-removes uses the same single-entry delete as self-leave since the
	// dual-membership branch is the only case that needs separate handling.
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u2").
		Return(nil)
	store.EXPECT().
		ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	store.EXPECT().
		GetSubscriptionAccounts(gomock.Any(), roomID).Return(nil, nil)
	store.EXPECT().
		GetUser(gomock.Any(), requester).
		Return(&model.User{ID: "u_alice", Account: requester, SiteID: siteID, EngName: "Alice", ChineseName: "愛"}, nil)

	var published []publishedMsg
	h := NewHandler(store, siteID, func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, testKeyStore, testKeySender)

	// requester != account means this is owner-removes-other
	req := model.RemoveMemberRequest{RoomID: roomID, Requester: requester, Account: account, Timestamp: 1, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.NoError(t, err)

	assert.Len(t, published, 4, "expected 4 publishes: sub update, member event, local INBOX, sys msg")

	// Verify the sys msg has type "member_removed"
	for _, p := range published {
		if p.subj == subject.MemberEvent(roomID) {
			var evt model.MemberRemoveEvent
			require.NoError(t, json.Unmarshal(p.data, &evt))
			assert.Equal(t, "member_removed", evt.Type)
			assert.Contains(t, evt.Accounts, account)
		}
	}
}

// --- processAddMembers tests ---

func TestHandler_ProcessAddMembers_FallsBackToNowOnInvalidTimestamp(t *testing.T) {
	// A missing timestamp should not short-circuit the handler. We confirm
	// processing reached the store layer by stubbing the first store call to
	// return a downstream error and asserting the error is NOT the timestamp
	// rejection.
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, fmt.Errorf("db error"))
	h := NewHandler(store, "site1", func(_ context.Context, _ string, _ []byte, _ string) error {
		return nil
	}, testKeyStore, testKeySender)
	req := model.AddMembersRequest{
		RoomID:           "r1",
		RequesterAccount: "alice",
		Users:            []string{"bob"},
		Timestamp:        0,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	err := h.processAddMembers(ctx, data)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "timestamp must be > 0")
}

func TestHandler_ProcessAddMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	var published []publishedMsg
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), nil, []string{"bob", "charlie"}, "r1").
		Return([]AddMemberCandidate{
			{Account: "bob", HasSubscription: false, HasIndividualRoomMember: false},
			{Account: "charlie", HasSubscription: false, HasIndividualRoomMember: false},
		}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "charlie"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a", EngName: "Bob", ChineseName: "鮑"},
		{ID: "u3", Account: "charlie", SiteID: "site-b", EngName: "Charlie", ChineseName: "查"},
	}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u1", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛",
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, subs []*model.Subscription) error {
			assert.Len(t, subs, 2)
			for _, s := range subs {
				assert.Equal(t, "site-a", s.SiteID)
				assert.Equal(t, model.RoomTypeChannel, s.RoomType)
				assert.Equal(t, []model.Role{model.RoleMember}, s.Roles)
				require.NotNil(t, s.HistorySharedSince)
				assert.Equal(t, s.JoinedAt, *s.HistorySharedSince)
			}
			return nil
		})
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	req := model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob", "charlie"},
		RequesterAccount: "alice",
		History:          model.HistoryConfig{Mode: model.HistoryModeNone},
		Timestamp:        1,
	}
	reqData, _ := json.Marshal(req)

	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	err := h.processAddMembers(ctx, reqData)
	require.NoError(t, err)

	// 2 SubscriptionUpdate + 1 MemberAddEvent + 1 system msg + 1 batched outbox (site-b)
	assert.GreaterOrEqual(t, len(published), 4)

	// Exactly one outbox event to site-b, carrying only site-b-homed accounts
	// (charlie). bob is on site-a (home), so does not appear in the cross-site
	// payload.
	var outboxCount int
	for _, p := range published {
		if strings.Contains(p.subj, "outbox") {
			outboxCount++
			assert.Contains(t, p.subj, "site-b")
			var outboxEvt model.OutboxEvent
			require.NoError(t, json.Unmarshal(p.data, &outboxEvt))
			var change model.MemberAddEvent
			require.NoError(t, json.Unmarshal(outboxEvt.Payload, &change))
			assert.ElementsMatch(t, []string{"charlie"}, change.Accounts)
		}
	}
	assert.Equal(t, 1, outboxCount, "should publish exactly 1 outbox event per remote site")
}

// TestHandler_ProcessAddMembers_PublishesSubscriptionUpdateBeforeRoomKey locks in
// the ordering invariant: clients must receive subscription.update BEFORE room.key
// for the same account, otherwise the client has no place to store the key.
func TestHandler_ProcessAddMembers_PublishesSubscriptionUpdateBeforeRoomKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	// Wire both the regular publish callback and the keySender to a single
	// mockPublisher so we get one chronological timeline across both event kinds.
	pub := &mockPublisher{}
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		return pub.Publish(subj, data)
	}
	h := NewHandler(store, "site-a", publish, testKeyStore, roomkeysender.NewSender(pub))

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), nil, []string{"bob", "charlie"}, "r1").
		Return([]AddMemberCandidate{
			{Account: "bob", HasSubscription: false, HasIndividualRoomMember: false},
			{Account: "charlie", HasSubscription: false, HasIndividualRoomMember: false},
		}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "charlie"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a", EngName: "Bob", ChineseName: "鮑"},
		{ID: "u3", Account: "charlie", SiteID: "site-a", EngName: "Charlie", ChineseName: "查"},
	}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u1", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛",
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	req := model.AddMembersRequest{
		RoomID: "r1", RequesterAccount: "alice", Users: []string{"bob", "charlie"},
		History:   model.HistoryConfig{Mode: model.HistoryModeNone},
		Timestamp: 1,
	}
	reqData, _ := json.Marshal(req)

	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	require.NoError(t, h.processAddMembers(ctx, reqData))

	for _, account := range []string{"bob", "charlie"} {
		subSubj := subject.SubscriptionUpdate(account)
		keySubj := subject.RoomKeyUpdate(account)
		subIdx, keyIdx := -1, -1
		for i, s := range pub.subjects {
			if s == subSubj && subIdx == -1 {
				subIdx = i
			}
			if s == keySubj && keyIdx == -1 {
				keyIdx = i
			}
		}
		require.NotEqual(t, -1, subIdx, "subscription.update not published for %s", account)
		require.NotEqual(t, -1, keyIdx, "room.key not published for %s", account)
		assert.Less(t, subIdx, keyIdx,
			"account %s: subscription.update (idx %d) must precede room.key (idx %d)",
			account, subIdx, keyIdx)
	}
}

func TestHandler_ProcessAddMembers_HistoryAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), nil, []string{"bob"}, "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a", EngName: "Bob", ChineseName: "鮑"},
	}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u1", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛",
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, subs []*model.Subscription) error {
			assert.Len(t, subs, 1)
			assert.Nil(t, subs[0].HistorySharedSince, "HistorySharedSince should be nil for mode all")
			return nil
		})
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	req := model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob"},
		RequesterAccount: "alice",
		History:          model.HistoryConfig{Mode: model.HistoryModeAll},
		Timestamp:        1,
	}
	reqData, _ := json.Marshal(req)

	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	err := h.processAddMembers(ctx, reqData)
	require.NoError(t, err)
}

// findMemberAddEvent returns the decoded MemberAddEvent published locally on
// RoomMemberEvent(roomID). Fails the test if no such publish occurred.
func findMemberAddEvent(t *testing.T, published []publishedMsg, roomID string) (model.MemberAddEvent, []byte) {
	t.Helper()
	want := subject.RoomMemberEvent(roomID)
	for _, p := range published {
		if p.subj != want {
			continue
		}
		var evt model.MemberAddEvent
		require.NoError(t, json.Unmarshal(p.data, &evt))
		return evt, p.data
	}
	t.Fatalf("no MemberAddEvent published to %s (got %d messages)", want, len(published))
	return model.MemberAddEvent{}, nil
}

// TestHandler_ProcessAddMembers_RestrictedPropagatesPointer verifies that a
// restricted room (HistoryModeNone) emits a MemberAddEvent whose
// HistorySharedSince is a non-nil pointer equal to the request timestamp,
// both for the same-site RoomMemberEvent publish and for the batched
// cross-site outbox event.
func TestHandler_ProcessAddMembers_RestrictedPropagatesPointer(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	var published []publishedMsg
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), nil, []string{"bob", "charlie"}, "r1").
		Return([]AddMemberCandidate{
			{Account: "bob"}, {Account: "charlie"},
		}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "charlie"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a", EngName: "Bob", ChineseName: "鮑"},
		{ID: "u3", Account: "charlie", SiteID: "site-b", EngName: "Charlie", ChineseName: "查"},
	}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u1", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛",
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	const reqTS int64 = 1744300000000
	req := model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob", "charlie"},
		RequesterAccount: "alice",
		History:          model.HistoryConfig{Mode: model.HistoryModeNone},
		Timestamp:        reqTS,
	}
	reqData, _ := json.Marshal(req)
	ctxR := natsutil.WithRequestID(context.Background(), testRequestID)
	require.NoError(t, h.processAddMembers(ctxR, reqData))

	// Local RoomMemberEvent: HSS must be a non-nil pointer equal to request ts.
	memberAddEvt, _ := findMemberAddEvent(t, published, "r1")
	require.NotNil(t, memberAddEvt.HistorySharedSince,
		"restricted room must publish non-nil HistorySharedSince")
	assert.Equal(t, reqTS, *memberAddEvt.HistorySharedSince)

	// Batched outbox to site-b: same HSS pointer on the payload.
	var foundOutbox bool
	for _, p := range published {
		if !strings.Contains(p.subj, "outbox") {
			continue
		}
		foundOutbox = true
		var outboxEvt model.OutboxEvent
		require.NoError(t, json.Unmarshal(p.data, &outboxEvt))
		var change model.MemberAddEvent
		require.NoError(t, json.Unmarshal(outboxEvt.Payload, &change))
		require.NotNil(t, change.HistorySharedSince,
			"outbox restricted payload must carry HistorySharedSince")
		assert.Equal(t, reqTS, *change.HistorySharedSince)
	}
	assert.True(t, foundOutbox, "expected a batched outbox publish for site-b")
}

// TestHandler_ProcessAddMembers_UnrestrictedOmitsFieldFromWire verifies that
// an unrestricted room (HistoryModeAll) produces a MemberAddEvent whose JSON
// wire form does NOT contain the "historySharedSince" key. This is the
// documented publisher contract.
func TestHandler_ProcessAddMembers_UnrestrictedOmitsFieldFromWire(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	var published []publishedMsg
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), nil, []string{"bob"}, "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a", EngName: "Bob", ChineseName: "鮑"},
	}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u1", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛",
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	req := model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob"},
		RequesterAccount: "alice",
		History:          model.HistoryConfig{Mode: model.HistoryModeAll},
		Timestamp:        1,
	}
	reqData, _ := json.Marshal(req)
	ctxU := natsutil.WithRequestID(context.Background(), testRequestID)
	require.NoError(t, h.processAddMembers(ctxU, reqData))

	evt, raw := findMemberAddEvent(t, published, "r1")
	assert.Nil(t, evt.HistorySharedSince, "unrestricted event must decode HSS as nil")

	var rawMap map[string]any
	require.NoError(t, json.Unmarshal(raw, &rawMap))
	_, present := rawMap["historySharedSince"]
	assert.False(t, present, "unrestricted event must omit historySharedSince on the wire")
}

func TestHandler_ProcessAddMembers_WithOrgs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string{"eng"}, []string{"bob"}, "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a", EngName: "Bob", ChineseName: "鮑"},
	}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u1", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛",
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
	// HasOrgRoomMembers is now called unconditionally (Task 2). Return false to
	// preserve this test's first-org-transition semantics so backfill fires.
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	// With orgs: BulkCreateRoomMembers called once with individual "bob" + org "eng" + backfill "alice"
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, members []*model.RoomMember) error {
			assert.Len(t, members, 3)
			return nil
		})
	// Backfill: GetSubscriptionAccounts returns existing "alice" + new "bob"
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), "r1").Return([]string{"alice", "bob"}, nil)
	// Backfill: FindUsersByAccounts for existing accounts that aren't new
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice"}).Return([]model.User{
		{ID: "u1", Account: "alice", SiteID: "site-a"},
	}, nil)

	req := model.AddMembersRequest{
		RoomID:           "r1",
		Users:            []string{"bob"},
		Orgs:             []string{"eng"},
		RequesterAccount: "alice",
		Timestamp:        1000,
		History:          model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	reqData, _ := json.Marshal(req)

	ctxOrgs := natsutil.WithRequestID(context.Background(), testRequestID)
	err := h.processAddMembers(ctxOrgs, reqData)
	require.NoError(t, err)
}

// Backfill partial-subset guard: when an existing subscription points at an
// account whose user document is gone (e.g. directory delete that didn't
// cascade), FindUsersByAccounts returns fewer rows than requested. Without a
// guard, the worker would silently commit room_members for the rows it got,
// flip hadOrgsBefore=true via BulkCreateRoomMembers, and leave the stale
// account with a sub but no individual room_members row that no future retry
// can ever repair (subsequent deliveries see hadOrgsBefore=true and skip
// backfill entirely). Fail hard with errPermanent so the operator sees it
// before partial state is committed.
func TestHandler_ProcessAddMembers_BackfillUserMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)

	// Org-only add on a room with no prior orgs → backfill fires.
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string{"eng"}, nil, "r1").
		Return([]AddMemberCandidate{}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u1", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛",
	}, nil)
	// Existing subs: alice + ghost (ghost has no users document).
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), "r1").Return([]string{"alice", "ghost"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice", "ghost"}).Return([]model.User{
		{ID: "u1", Account: "alice", SiteID: "site-a"},
	}, nil)
	// BulkCreateRoomMembers / ReconcileMemberCounts MUST NOT be called.

	req := model.AddMembersRequest{
		RoomID:           "r1",
		Orgs:             []string{"eng"},
		RequesterAccount: "alice",
		Timestamp:        1000,
		History:          model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	reqData, _ := json.Marshal(req)

	err := h.processAddMembers(natsutil.WithRequestID(context.Background(), testRequestID), reqData)
	require.Error(t, err)
	assert.ErrorIs(t, err, errPermanent)
	assert.Contains(t, err.Error(), "ghost")
	assert.Contains(t, err.Error(), "r1")
}

// New permanent-error contract: when ListNewMembers resolves a candidate
// account that's no longer present in the users collection, processAddMembers
// must NOT silently materialize a smaller membership. It returns errPermanent
// so JetStream Acks (no infinite redelivery) and the requester sees an
// async-job error event naming the missing account.
func TestHandler_ProcessAddMembers_UserNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), nil, []string{"bob", "ghost"}, "r1").
		Return([]AddMemberCandidate{{Account: "bob"}, {Account: "ghost"}}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "ghost"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a"},
	}, nil)
	// BulkCreateSubscriptions / ReconcileMemberCounts MUST NOT be called once
	// a missing account is detected.

	req := model.AddMembersRequest{
		RoomID:           "r1",
		Users:            []string{"bob", "ghost"},
		RequesterAccount: "alice",
		Timestamp:        1000,
		History:          model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	reqData, _ := json.Marshal(req)

	ctxUNF := natsutil.WithRequestID(context.Background(), testRequestID)
	err := h.processAddMembers(ctxUNF, reqData)
	require.Error(t, err)
	assert.ErrorIs(t, err, errPermanent)
	assert.Contains(t, err.Error(), "ghost")
	assert.Contains(t, err.Error(), "r1")
}

func TestHandler_ProcessAddMembers_MultipleSiteOutbox(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	var published []publishedMsg
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), nil, []string{"alice", "bob", "charlie"}, "r1").
		Return([]AddMemberCandidate{
			{Account: "alice"}, {Account: "bob"}, {Account: "charlie"},
		}, nil)
	allUsers := []model.User{
		{ID: "u1", Account: "alice", SiteID: "site-b", EngName: "Alice", ChineseName: "愛"},
		{ID: "u2", Account: "bob", SiteID: "site-b", EngName: "Bob", ChineseName: "鮑"},
		{ID: "u3", Account: "charlie", SiteID: "site-c", EngName: "Charlie", ChineseName: "查"},
	}
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice", "bob", "charlie"}).Return(allUsers, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u1", Account: "alice", SiteID: "site-b", EngName: "Alice", ChineseName: "愛",
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	req := model.AddMembersRequest{
		RoomID:           "r1",
		Users:            []string{"alice", "bob", "charlie"},
		RequesterAccount: "alice",
		Timestamp:        1000,
		History:          model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	reqData, _ := json.Marshal(req)

	ctxMS := natsutil.WithRequestID(context.Background(), testRequestID)
	err := h.processAddMembers(ctxMS, reqData)
	require.NoError(t, err)

	var outboxEvents []publishedMsg
	for _, p := range published {
		if strings.HasPrefix(p.subj, "outbox.") {
			outboxEvents = append(outboxEvents, p)
		}
	}
	assert.Len(t, outboxEvents, 2, "one outbox event per remote site: site-b and site-c")

	// Each remote site receives only its own homed accounts: site-b gets
	// alice+bob, site-c gets charlie.
	want := map[string][]string{
		"site-b": {"alice", "bob"},
		"site-c": {"charlie"},
	}
	for _, p := range outboxEvents {
		var outboxEvt model.OutboxEvent
		require.NoError(t, json.Unmarshal(p.data, &outboxEvt))
		var change model.MemberAddEvent
		require.NoError(t, json.Unmarshal(outboxEvt.Payload, &change))
		expected, ok := want[outboxEvt.DestSiteID]
		require.True(t, ok, "unexpected destSiteID %s", outboxEvt.DestSiteID)
		assert.ElementsMatch(t, expected, change.Accounts,
			"outbox to %s should carry only its homed accounts", outboxEvt.DestSiteID)
	}
}

func TestHandler_ProcessRemoveMember_OwnerRemovesOrg(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	const (
		roomID    = "room-1"
		orgID     = "org-1"
		requester = "alice"
		siteID    = "site-a"
	)

	// 3 org members: carol and dave have no individual membership, eve does
	orgMembers := []OrgMemberStatus{
		{Account: "carol", SiteID: siteID, Name: "Engineering", HasIndividualMembership: false},
		{Account: "dave", SiteID: siteID, Name: "Engineering", HasIndividualMembership: false},
		{Account: "eve", SiteID: siteID, Name: "Engineering", HasIndividualMembership: true},
	}

	store.EXPECT().
		GetOrgMembersWithIndividualStatus(gomock.Any(), roomID, orgID).
		Return(orgMembers, nil)
	store.EXPECT().
		DeleteSubscriptionsByAccounts(gomock.Any(), roomID, gomock.InAnyOrder([]string{"carol", "dave"})).
		Return(int64(2), nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberOrg, orgID).
		Return(nil)
	store.EXPECT().
		ReconcileMemberCounts(gomock.Any(), roomID).Return(nil) // recount after removal
	store.EXPECT().
		GetSubscriptionAccounts(gomock.Any(), roomID).Return(nil, nil)
	store.EXPECT().
		GetUser(gomock.Any(), requester).
		Return(&model.User{ID: "u_alice", Account: requester, SiteID: siteID, EngName: "Alice", ChineseName: "愛"}, nil)

	var published []publishedMsg
	h := NewHandler(store, siteID, func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, testKeyStore, testKeySender)

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: requester, OrgID: orgID, Timestamp: 1000, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.NoError(t, err)

	// Expect: 2 sub updates + 1 member event + 1 local INBOX + 1 sys msg = 5 publishes
	assert.Len(t, published, 5, "expected 5 publishes: 2 sub updates, member event, local INBOX, sys msg")

	subjSet := make(map[string]bool)
	for _, p := range published {
		subjSet[p.subj] = true
	}

	assert.True(t, subjSet[subject.SubscriptionUpdate("carol")], "expected subscription update for carol")
	assert.True(t, subjSet[subject.SubscriptionUpdate("dave")], "expected subscription update for dave")
	assert.False(t, subjSet[subject.SubscriptionUpdate("eve")], "eve has individual membership, should not be removed")
	assert.True(t, subjSet[subject.MemberEvent(roomID)], "expected member event published")

	// Sys-message must carry sender (UserAccount = requester) and Content
	// rendered from the org's SectName (spec §2.4). The previous version of
	// this test only verified counts, leaving Content/UserAccount regressions
	// undetected.
	sysMsg := findSysMsg(t, published, siteID, model.MessageTypeMemberRemoved)
	assert.Equal(t, requester, sysMsg.UserAccount, "sender envelope must be set to requester")
	assert.Equal(t, "u_alice", sysMsg.UserID, "UserID set to requester so message-worker can populate Cassandra sender column")
	assert.Equal(t, `"Engineering" has been removed from the channel`, sysMsg.Content)
}

func TestHandler_ProcessRemoveMember_CrossSiteOutbox(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	const (
		roomID    = "room-1"
		account   = "alice"
		localSite = "site-a"
		userSite  = "site-b" // user is on a different site
	)

	userResult := &UserWithMembership{
		User: model.User{
			ID:          "u1",
			Account:     account,
			SiteID:      userSite, // different from local site
			EngName:     "Alice",
			ChineseName: "愛",
		},
		HasOrgMembership: false,
	}

	store.EXPECT().
		GetUserWithMembership(gomock.Any(), roomID, account).
		Return(userResult, nil)
	store.EXPECT().
		DeleteSubscription(gomock.Any(), roomID, account).
		Return(int64(1), nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u1").
		Return(nil)
	store.EXPECT().
		ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	store.EXPECT().
		GetSubscriptionAccounts(gomock.Any(), roomID).Return(nil, nil)

	var published []publishedMsg
	h := NewHandler(store, localSite, func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, testKeyStore, testKeySender)

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: account, Account: account, Timestamp: 1000, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.NoError(t, err)

	// Expect: sub update + member event + local INBOX + sys msg + outbox = 5 publishes
	assert.Len(t, published, 5, "expected 5 publishes including local INBOX and outbox for federated user")

	outboxSubj := subject.Outbox(localSite, userSite, "member_removed")
	subjSet := make(map[string]bool)
	for _, p := range published {
		subjSet[p.subj] = true
	}
	assert.True(t, subjSet[outboxSubj], "expected outbox event published for remote user")
}

func TestHandler_ProcessRemoveMember_UnmarshalError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, testKeyStore, testKeySender)

	err := h.processRemoveMember(context.Background(), []byte("{not json"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestHandler_ProcessRemoveIndividual_GetUserError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().
		GetUserWithMembership(gomock.Any(), "r1", "alice").
		Return(nil, fmt.Errorf("db down"))

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, testKeyStore, testKeySender)
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "alice", Timestamp: 1000, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get user with membership")
}

func TestHandler_ProcessRemoveIndividual_DeleteRoomMemberError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().
		GetUserWithMembership(gomock.Any(), "r1", "alice").
		Return(&UserWithMembership{
			User:  model.User{ID: "u1", Account: "alice", EngName: "Alice", ChineseName: "愛"},
			Roles: []model.Role{model.RoleMember},
		}, nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), "r1", model.RoomMemberIndividual, "u1").
		Return(fmt.Errorf("write failed"))

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, testKeyStore, testKeySender)
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "alice", Timestamp: 1000, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete room member")
}

func TestHandler_ProcessRemoveIndividual_DualDemoteError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().
		GetUserWithMembership(gomock.Any(), "r1", "alice").
		Return(&UserWithMembership{
			User:             model.User{ID: "u1", Account: "alice", EngName: "Alice", ChineseName: "愛"},
			HasOrgMembership: true,
			Roles:            []model.Role{model.RoleOwner, model.RoleMember},
		}, nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), "r1", model.RoomMemberIndividual, "u1").
		Return(nil)
	store.EXPECT().
		RemoveRole(gomock.Any(), "alice", "r1", model.RoleOwner).
		Return(fmt.Errorf("write failed"))

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, testKeyStore, testKeySender)
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "alice", Timestamp: 1000, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "demote dual-member owner")
}

func TestHandler_ProcessRemoveIndividual_DeleteSubscriptionError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().
		GetUserWithMembership(gomock.Any(), "r1", "alice").
		Return(&UserWithMembership{
			User:  model.User{ID: "u1", Account: "alice", EngName: "Alice", ChineseName: "愛"},
			Roles: []model.Role{model.RoleMember},
		}, nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), "r1", model.RoomMemberIndividual, "u1").
		Return(nil)
	store.EXPECT().
		DeleteSubscription(gomock.Any(), "r1", "alice").
		Return(int64(0), fmt.Errorf("write failed"))

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, testKeyStore, testKeySender)
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "alice", Timestamp: 1000, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete subscription")
}

func TestHandler_ProcessRemoveIndividual_ReconcileMemberCountsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().
		GetUserWithMembership(gomock.Any(), "r1", "alice").
		Return(&UserWithMembership{
			User:  model.User{ID: "u1", Account: "alice", EngName: "Alice", ChineseName: "愛"},
			Roles: []model.Role{model.RoleMember},
		}, nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), "r1", model.RoomMemberIndividual, "u1").
		Return(nil)
	store.EXPECT().
		DeleteSubscription(gomock.Any(), "r1", "alice").
		Return(int64(1), nil)
	store.EXPECT().
		ReconcileMemberCounts(gomock.Any(), "r1").
		Return(fmt.Errorf("write failed"))

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, testKeyStore, testKeySender)
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "alice", Timestamp: 1000, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconcile member counts")
}

func TestHandler_ProcessAddMembers_ExistingOrgsWritesIndividuals(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, "site-a", publish, testKeyStore, testKeySender)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), nil, []string{"bob"}, "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a", EngName: "Bob", ChineseName: "鮑"},
	}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u1", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛",
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(true, nil)
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, members []*model.RoomMember) error {
			require.Len(t, members, 1)
			assert.Equal(t, model.RoomMemberIndividual, members[0].Member.Type)
			assert.Equal(t, "bob", members[0].Member.Account)
			return nil
		})

	req := model.AddMembersRequest{
		RoomID:           "r1",
		Users:            []string{"bob"},
		RequesterAccount: "alice",
		Timestamp:        1000,
		History:          model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	reqData, _ := json.Marshal(req)

	ctxEO := natsutil.WithRequestID(context.Background(), testRequestID)
	err := h.processAddMembers(ctxEO, reqData)
	require.NoError(t, err)
}

// TestHandler_ProcessAddMembers_OrgToIndividualUpgrade verifies the bug fix:
// when an account already has a subscription (e.g. added earlier via org) but
// no individual room_members row, an explicit add via req.Users must write the
// missing individual row WITHOUT creating a duplicate subscription. It also
// verifies that no MsgCanonicalCreated sys-msg and no MemberEvent are
// published — the upgrade is a silent backfill, no membership state changed
// for the room itself.
func TestHandler_ProcessAddMembers_OrgToIndividualUpgrade(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "room-1"
	requestID := idgen.GenerateRequestID()
	ctx := natsutil.WithRequestID(context.Background(), requestID)

	store.EXPECT().GetRoom(ctx, roomID).Return(&model.Room{
		ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a", Name: "Room 1",
	}, nil)
	// Alice has a subscription (added earlier via org) but no individual row.
	// req.Orgs is omitted from the request → unmarshals to []string(nil), not []string{}.
	store.EXPECT().ListAddMemberCandidates(ctx, []string(nil), []string{"alice"}, roomID).Return([]AddMemberCandidate{
		{Account: "alice", HasSubscription: true, HasIndividualRoomMember: false},
	}, nil)
	store.EXPECT().HasOrgRoomMembers(ctx, roomID).Return(true, nil)
	store.EXPECT().FindUsersByAccounts(ctx, []string{"alice"}).Return([]model.User{
		{ID: "u_alice", Account: "alice", EngName: "Alice", ChineseName: "爱丽丝", SiteID: "site-a"},
	}, nil)
	store.EXPECT().GetUser(ctx, "owner").Return(&model.User{
		ID: "u_owner", Account: "owner", EngName: "Owner", ChineseName: "拥有者", SiteID: "site-a",
	}, nil)
	// CRITICAL: BulkCreateSubscriptions must NOT be called — alice already has a sub.
	// BulkCreateRoomMembers must be called with exactly one individual row for alice.
	store.EXPECT().BulkCreateRoomMembers(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, members []*model.RoomMember) error {
		require.Len(t, members, 1)
		assert.Equal(t, model.RoomMemberIndividual, members[0].Member.Type)
		assert.Equal(t, "alice", members[0].Member.Account)
		assert.Equal(t, "u_alice", members[0].Member.ID)
		return nil
	})
	store.EXPECT().ReconcileMemberCounts(ctx, roomID).Return(nil)

	var published []publishedMsg
	h := &Handler{
		store: store, siteID: "site-a",
		publish: func(_ context.Context, subj string, data []byte, _ string) error {
			published = append(published, publishedMsg{subj: subj, data: data})
			return nil
		},
		keyStore: testKeyStore, keySender: testKeySender,
	}

	req := model.AddMembersRequest{
		RoomID: roomID, Users: []string{"alice"}, RequesterAccount: "owner", RequesterID: "u_owner",
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	data, _ := json.Marshal(req)
	err := h.processAddMembers(ctx, data)
	require.NoError(t, err)

	// No membership state changed for the room (only a silent individual-row
	// backfill); the worker MUST NOT emit a sys-msg or member-add event that
	// would render "added members to the channel" with an empty member list.
	memberEventSubj := subject.RoomMemberEvent(roomID)
	sysMsgSubj := subject.MsgCanonicalCreated("site-a")
	for _, p := range published {
		assert.NotEqual(t, memberEventSubj, p.subj,
			"upgrade-only path must NOT publish MemberAddEvent")
		assert.NotEqual(t, sysMsgSubj, p.subj,
			"upgrade-only path must NOT publish a members_added sys-msg")
	}
}

// Bug 4: outbox publish failure must propagate (NAK), not be swallowed.
func TestHandler_ProcessRemoveIndividual_OutboxFailurePropagates(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	const (
		roomID    = "room-1"
		account   = "alice"
		localSite = "site-a"
		userSite  = "site-b"
	)

	store.EXPECT().
		GetUserWithMembership(gomock.Any(), roomID, account).
		Return(&UserWithMembership{
			User:             model.User{ID: "u1", Account: account, SiteID: userSite, EngName: "Alice", ChineseName: "愛"},
			HasOrgMembership: false,
		}, nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u1").
		Return(nil)
	store.EXPECT().
		DeleteSubscription(gomock.Any(), roomID, account).
		Return(int64(1), nil)
	store.EXPECT().
		ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	store.EXPECT().
		GetSubscriptionAccounts(gomock.Any(), roomID).Return(nil, nil)

	outboxSubj := subject.Outbox(localSite, userSite, "member_removed")
	publish := func(_ context.Context, subj string, _ []byte, _ string) error {
		if subj == outboxSubj {
			return fmt.Errorf("outbox publish broken")
		}
		return nil
	}
	h := NewHandler(store, localSite, publish, testKeyStore, testKeySender)

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: account, Account: account, Timestamp: 1000, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.Error(t, err, "outbox failure must return error so JetStream NAKs and retries")
	assert.Contains(t, err.Error(), "outbox")
}

func TestHandler_ProcessRemoveOrg_OutboxFailurePropagates(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	const (
		roomID     = "room-1"
		orgID      = "org-1"
		requester  = "alice"
		localSite  = "site-a"
		remoteSite = "site-b"
	)

	orgMembers := []OrgMemberStatus{
		{Account: "carol", SiteID: remoteSite, Name: "Eng", HasIndividualMembership: false},
	}

	store.EXPECT().GetOrgMembersWithIndividualStatus(gomock.Any(), roomID, orgID).Return(orgMembers, nil)
	store.EXPECT().DeleteSubscriptionsByAccounts(gomock.Any(), roomID, []string{"carol"}).Return(int64(1), nil)
	store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberOrg, orgID).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return(nil, nil)
	store.EXPECT().GetUser(gomock.Any(), requester).
		Return(&model.User{ID: "u_alice", Account: requester, SiteID: localSite, EngName: "Alice", ChineseName: "愛"}, nil)

	outboxSubj := subject.Outbox(localSite, remoteSite, "member_removed")
	publish := func(_ context.Context, subj string, _ []byte, _ string) error {
		if subj == outboxSubj {
			return fmt.Errorf("outbox publish broken")
		}
		return nil
	}
	h := NewHandler(store, localSite, publish, testKeyStore, testKeySender)

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: requester, OrgID: orgID, Timestamp: 1000, RoomType: model.RoomTypeChannel}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.Error(t, err, "outbox failure must return error so JetStream NAKs and retries")
	assert.Contains(t, err.Error(), "outbox")
}

func TestHandler_processAddMembers_PublishesSuccessEventToRequesterSubject(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	var capturedSubject string
	var capturedData []byte
	publish := func(ctx context.Context, subj string, data []byte, msgID string) error {
		if strings.HasPrefix(subj, "chat.user.") {
			capturedSubject = subj
			capturedData = data
		}
		return nil
	}
	h := NewHandler(store, "site1", publish, testKeyStore, testKeySender)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site1"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), []string{"bob"}, "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site1", EngName: "Bob", ChineseName: "鮑"},
	}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u1", Account: "alice", SiteID: "site1", EngName: "Alice", ChineseName: "愛",
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	req := model.AddMembersRequest{
		RoomID:           "r1",
		Users:            []string{"bob"},
		RequesterAccount: "alice",
		Timestamp:        1000,
	}
	reqData, _ := json.Marshal(req)
	err := h.processAddMembers(ctx, reqData)
	require.NoError(t, err)

	assert.Equal(t, subject.UserResponse("alice", testRequestID), capturedSubject)
	var result model.AsyncJobResult
	require.NoError(t, json.Unmarshal(capturedData, &result))
	assert.Equal(t, testRequestID, result.RequestID)
	assert.Equal(t, model.AsyncJobOpRoomMemberAdd, result.Operation)
	assert.Equal(t, "ok", result.Status)
	assert.Equal(t, "", result.Error)
	assert.Greater(t, result.Timestamp, int64(0))
}

func TestHandler_processAddMembers_PublishesFailureEventOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	var capturedSubject string
	var capturedData []byte
	publish := func(ctx context.Context, subj string, data []byte, msgID string) error {
		if strings.HasPrefix(subj, "chat.user.") {
			capturedSubject = subj
			capturedData = data
		}
		return nil
	}
	h := NewHandler(store, "site1", publish, testKeyStore, testKeySender)

	// Mock store to fail on FindUsersByAccounts (first store operation after candidate resolution)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site1"}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), []string{"bob"}, "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return(nil, fmt.Errorf("database connection failed"))

	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	req := model.AddMembersRequest{
		RoomID:           "r1",
		Users:            []string{"bob"},
		RequesterAccount: "alice",
		Timestamp:        1000,
	}
	reqData, _ := json.Marshal(req)
	err := h.processAddMembers(ctx, reqData)
	require.Error(t, err, "processAddMembers must return error on FindUsersByAccounts failure")
	assert.Contains(t, err.Error(), "find users by accounts")

	// Verify failure event was published to requester
	assert.Equal(t, subject.UserResponse("alice", testRequestID), capturedSubject)
	var result model.AsyncJobResult
	require.NoError(t, json.Unmarshal(capturedData, &result))
	assert.Equal(t, testRequestID, result.RequestID)
	assert.Equal(t, model.AsyncJobOpRoomMemberAdd, result.Operation)
	assert.Equal(t, "error", result.Status, "failure event must have Status=error")
	// Raw infra error collapses to internal — the cause never leaks to the client.
	assert.Equal(t, "internal error", result.Error, "failure event must carry sanitized error message")
	assert.Equal(t, string(errcode.CodeInternal), result.Code)
	assert.Greater(t, result.Timestamp, int64(0))
}

func TestHandler_publishAsyncJobResult_PopulatesErrorOnFailure(t *testing.T) {
	var capturedSubject string
	var capturedData []byte
	publish := func(ctx context.Context, subj string, data []byte, msgID string) error {
		if strings.HasPrefix(subj, "chat.user.") {
			capturedSubject = subj
			capturedData = data
		}
		return nil
	}
	h := NewHandler(nil, "site1", publish, testKeyStore, testKeySender)

	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	jobErr := errors.New("oops")
	h.publishAsyncJobResult(ctx, "alice", model.AsyncJobOpRoomMemberAdd, "r1", jobErr)

	assert.Equal(t, subject.UserResponse("alice", testRequestID), capturedSubject)
	var result model.AsyncJobResult
	require.NoError(t, json.Unmarshal(capturedData, &result))
	assert.Equal(t, testRequestID, result.RequestID)
	assert.Equal(t, model.AsyncJobOpRoomMemberAdd, result.Operation)
	assert.Equal(t, "error", result.Status)
	assert.Equal(t, "internal error", result.Error)
	assert.Equal(t, string(errcode.CodeInternal), result.Code)
	assert.Equal(t, "r1", result.RoomID)
}

func TestHandler_publishAsyncJobResult_NoOpOnEmptyRequestID(t *testing.T) {
	called := false
	publish := func(ctx context.Context, subj string, data []byte, msgID string) error {
		called = true
		return nil
	}
	h := NewHandler(nil, "site1", publish, testKeyStore, testKeySender)

	// No WithRequestID on ctx → empty request ID → publish is skipped.
	h.publishAsyncJobResult(context.Background(), "alice", model.AsyncJobOpRoomMemberAdd, "r1", nil)
	assert.False(t, called, "publish must be skipped when request ID is empty")
}

func TestHandler_publishAsyncJobResult_NoOpOnEmptyRequester(t *testing.T) {
	called := false
	publish := func(ctx context.Context, subj string, data []byte, msgID string) error {
		called = true
		return nil
	}
	h := NewHandler(nil, "site1", publish, testKeyStore, testKeySender)

	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	h.publishAsyncJobResult(ctx, "", model.AsyncJobOpRoomMemberAdd, "r1", nil)
	assert.False(t, called, "publish must be skipped when requester account is empty")
}

// ---------------------------------------------------------------------------
// processAddMembers tests (Tasks 12, 14, 14b, 15, 16)
// ---------------------------------------------------------------------------

// newAddMembersTestHandler builds a Handler with a mock store and a capture-publish
// closure, returning (handler, mockStore, getPublished).
func newAddMembersTestHandler(t *testing.T) (*Handler, *MockSubscriptionStore, func() []publishedMsg) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	var published []publishedMsg
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}
	h := &Handler{
		store:     mockStore,
		publish:   publish,
		siteID:    "site-A",
		keyStore:  testKeyStore,
		keySender: testKeySender,
	}
	return h, mockStore, func() []publishedMsg { return published }
}

// setupAddMembersHappyPath sets up the standard happy-path mock expectations.
// All users are on site-A (no cross-site outbox). HasOrgRoomMembers returns false.
func setupAddMembersHappyPath(t *testing.T, mockStore *MockSubscriptionStore, accounts []string) {
	t.Helper()
	mockStore.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Name: "deal team", Type: model.RoomTypeChannel, SiteID: "site-A",
	}, nil)
	candidates := make([]AddMemberCandidate, len(accounts))
	for i, a := range accounts {
		candidates[i] = AddMemberCandidate{Account: a}
	}
	mockStore.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").
		Return(candidates, nil)
	users := make([]model.User, len(accounts))
	for i, a := range accounts {
		users[i] = model.User{ID: "u_" + a, Account: a, SiteID: "site-A", EngName: "X", ChineseName: "X"}
	}
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), accounts).Return(users, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A", EngName: "Alice", ChineseName: "愛麗絲",
	}, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
}

// The legacy TestProcessAddMembers_RequiresRequestID test pinned the old
// reject-on-missing behavior. Under the repo-wide "mint everywhere" policy
// (docs/error-handling.md), missing/malformed X-Request-ID is no longer a
// rejectable condition — the boundary (main.go JetStream consume loop /
// natsrouter.RequestID middleware) mints a fresh UUIDv7 via
// natsutil.StampRequestID before the handler sees the ctx.

func TestProcessAddMembers_MalformedJSON_IsPermanent(t *testing.T) {
	// Regression for the Nak-forever bug: a malformed payload must be Acked
	// (Permanent), not Naked. Pre-fix the bare fmt.Errorf made JetStream
	// redeliver the same corrupt JSON until MaxDeliver.
	h, _, _ := newAddMembersTestHandler(t)
	err := h.processAddMembers(context.Background(), []byte(`{not json`))
	require.Error(t, err)
	assert.ErrorIs(t, err, errPermanent, "unmarshal failure must be Permanent so JetStream Acks")
	assert.NotContains(t, err.Error(), "not json", "must not echo raw payload bytes into the user-facing message")
}

func TestProcessRemoveMember_MalformedJSON_IsPermanent(t *testing.T) {
	// Unmarshal failure short-circuits before any store call — a bare Handler
	// is enough.
	h := &Handler{publish: func(context.Context, string, []byte, string) error { return nil }}
	err := h.processRemoveMember(context.Background(), []byte(`{not json`))
	require.Error(t, err)
	assert.ErrorIs(t, err, errPermanent, "unmarshal failure must be Permanent so JetStream Acks")
	assert.NotContains(t, err.Error(), "not json")
}

// Task 14: subscription must carry Name == room.Name and RoomType == channel.
func TestProcessAddMembers_PopulatesSubName(t *testing.T) {
	h, mockStore, _ := newAddMembersTestHandler(t)
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx := natsutil.WithRequestID(context.Background(), reqID)

	// Use a custom BulkCreateSubscriptions expectation to capture subs.
	mockStore.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Name: "deal team", Type: model.RoomTypeChannel, SiteID: "site-A",
	}, nil)
	mockStore.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u_bob", Account: "bob", SiteID: "site-A", EngName: "X", ChineseName: "X"},
	}, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A", EngName: "Alice", ChineseName: "愛麗絲",
	}, nil)
	var capturedSubs []*model.Subscription
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			capturedSubs = subs
			return nil
		})
	mockStore.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)

	body, err := json.Marshal(model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob"},
		RequesterID: "u_alice", RequesterAccount: "alice", Timestamp: 1740000000000,
	})
	require.NoError(t, err)
	require.NoError(t, h.processAddMembers(ctx, body))

	require.Len(t, capturedSubs, 1)
	assert.Equal(t, "deal team", capturedSubs[0].Name)
	assert.Equal(t, model.RoomTypeChannel, capturedSubs[0].RoomType)
}

// Task 14b: HistoryModeNone — sub.HistorySharedSince falls back to acceptedAt (req.Timestamp).
func TestProcessAddMembers_HistoryNone_NoTimestamp(t *testing.T) {
	h, mockStore, _ := newAddMembersTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), "0193abcd-0193-7abc-89ab-0193abcd0002")

	const reqTimestampMs = int64(1740000000000)
	acceptedAt := time.UnixMilli(reqTimestampMs).UTC()

	mockStore.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Name: "deal team", Type: model.RoomTypeChannel, SiteID: "site-A",
	}, nil)
	mockStore.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u_bob", Account: "bob", SiteID: "site-A", EngName: "X", ChineseName: "X"},
	}, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A", EngName: "Alice", ChineseName: "愛麗絲",
	}, nil)
	var capturedSubs []*model.Subscription
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			capturedSubs = subs
			return nil
		})
	mockStore.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)

	// No SharedSince in HistoryConfig — falls back to req.Timestamp (acceptedAt).
	body, err := json.Marshal(model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob"},
		RequesterID: "u_alice", RequesterAccount: "alice",
		Timestamp: reqTimestampMs,
		History:   model.HistoryConfig{Mode: model.HistoryModeNone},
	})
	require.NoError(t, err)
	require.NoError(t, h.processAddMembers(ctx, body))

	require.Len(t, capturedSubs, 1)
	require.NotNil(t, capturedSubs[0].HistorySharedSince)
	assert.Equal(t, acceptedAt, *capturedSubs[0].HistorySharedSince)
}

// Task 14b: no History.Mode set — sub.HistorySharedSince must be nil.
func TestProcessAddMembers_NoHistoryConfig_LeavesNil(t *testing.T) {
	h, mockStore, _ := newAddMembersTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), "0193abcd-0193-7abc-89ab-0193abcd0003")

	mockStore.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Name: "deal team", Type: model.RoomTypeChannel, SiteID: "site-A",
	}, nil)
	mockStore.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u_bob", Account: "bob", SiteID: "site-A", EngName: "X", ChineseName: "X"},
	}, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A", EngName: "Alice", ChineseName: "愛麗絲",
	}, nil)
	var capturedSubs []*model.Subscription
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			capturedSubs = subs
			return nil
		})
	mockStore.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)

	// No History.Mode — HistorySharedSince must remain nil.
	body, err := json.Marshal(model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob"},
		RequesterID: "u_alice", RequesterAccount: "alice",
		Timestamp: 1740000000000,
	})
	require.NoError(t, err)
	require.NoError(t, h.processAddMembers(ctx, body))

	require.Len(t, capturedSubs, 1)
	assert.Nil(t, capturedSubs[0].HistorySharedSince)
}

// Task 15: outbox MemberAddEvent for cross-site members must carry RoomName.
func TestProcessAddMembers_OutboxCarriesRoomName(t *testing.T) {
	h, mockStore, getPublished := newAddMembersTestHandler(t)
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx := natsutil.WithRequestID(context.Background(), reqID)

	// Cross-site member: bob lives on site-B.
	mockStore.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Name: "deal team", Type: model.RoomTypeChannel, SiteID: "site-A",
	}, nil)
	mockStore.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").
		Return([]AddMemberCandidate{{Account: "bob"}}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u_bob", Account: "bob", SiteID: "site-B", EngName: "Bob", ChineseName: "鲍勃"},
	}, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-A", EngName: "Alice", ChineseName: "愛麗絲",
	}, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)

	body, err := json.Marshal(model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob"},
		RequesterID: "u_alice", RequesterAccount: "alice", Timestamp: 1,
	})
	require.NoError(t, err)
	require.NoError(t, h.processAddMembers(ctx, body))

	// Find outbox publish to site-B with member_added.
	pub := getPublished()
	var found bool
	for _, m := range pub {
		if !strings.HasPrefix(m.subj, "outbox.site-A.to.site-B.member_added") {
			continue
		}
		found = true
		var envelope model.OutboxEvent
		require.NoError(t, json.Unmarshal(m.data, &envelope))
		var evt model.MemberAddEvent
		require.NoError(t, json.Unmarshal(envelope.Payload, &evt))
		assert.Equal(t, "deal team", evt.RoomName)
	}
	require.True(t, found, "expected outbox publish to site-B with member_added subject")
}

// Task 16: successful processAddMembers must publish AsyncJobResult with status "ok".
func TestProcessAddMembers_PublishesAsyncJobOnSuccess(t *testing.T) {
	h, mockStore, getPublished := newAddMembersTestHandler(t)
	const reqID = "0193abcd-0193-7abc-89ab-0193abcd0193"
	ctx := natsutil.WithRequestID(context.Background(), reqID)

	setupAddMembersHappyPath(t, mockStore, []string{"bob"})
	body, err := json.Marshal(model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob"},
		RequesterID: "u_alice", RequesterAccount: "alice", Timestamp: 1,
	})
	require.NoError(t, err)
	require.NoError(t, h.processAddMembers(ctx, body))

	// Find async-job publish on subject.UserResponse("alice", reqID).
	expectedSubj := subject.UserResponse("alice", reqID)
	pub := getPublished()
	var found *publishedMsg
	for i := range pub {
		if pub[i].subj == expectedSubj {
			found = &pub[i]
			break
		}
	}
	require.NotNil(t, found, "expected async-job publish on %s", expectedSubj)

	var got model.AsyncJobResult
	require.NoError(t, json.Unmarshal(found.data, &got))
	assert.Equal(t, reqID, got.RequestID)
	assert.Equal(t, model.AsyncJobOpRoomMemberAdd, got.Operation)
	assert.Equal(t, "ok", got.Status)
}

func TestResolveRoomName(t *testing.T) {
	tests := map[string]struct {
		req      model.CreateRoomRequest
		roomType model.RoomType
		want     string
	}{
		"dm empty":           {model.CreateRoomRequest{RoomID: "u_a|u_b"}, model.RoomTypeDM, ""},
		"botDM empty":        {model.CreateRoomRequest{RoomID: "u_a|u_w"}, model.RoomTypeBotDM, ""},
		"channel given name": {model.CreateRoomRequest{Name: "deal team", RoomID: "r1"}, model.RoomTypeChannel, "deal team"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, resolveRoomName(&tc.req, tc.roomType))
		})
	}
}

func TestNewSubSetsAllFields(t *testing.T) {
	user := &model.User{ID: "u1", Account: "alice"}
	room := &model.Room{ID: "r1", SiteID: "site-A", Type: model.RoomTypeChannel}
	now := time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC)

	sub := newSub("s1", user, room, []model.Role{model.RoleOwner},
		"deal team", false, now)

	assert.Equal(t, "s1", sub.ID)
	assert.Equal(t, "u1", sub.User.ID)
	assert.Equal(t, "alice", sub.User.Account)
	assert.Equal(t, "r1", sub.RoomID)
	assert.Equal(t, "site-A", sub.SiteID)
	assert.Equal(t, []model.Role{model.RoleOwner}, sub.Roles)
	assert.Equal(t, "deal team", sub.Name)
	assert.Equal(t, model.RoomTypeChannel, sub.RoomType)
	assert.False(t, sub.IsSubscribed)
	assert.Equal(t, now, sub.JoinedAt)
}

// ---- processCreateRoom test helpers ----

// newCreateRoomTestHandler builds a Handler with a mock store and capture-publish,
// returning (handler, mockStore, getPublished). siteID is always "site-A".
func newCreateRoomTestHandler(t *testing.T) (*Handler, *MockSubscriptionStore, func() []publishedMsg) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	var published []publishedMsg
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}
	h := &Handler{store: mockStore, publish: publish, siteID: "site-A", keyStore: testKeyStore, keySender: testKeySender}
	return h, mockStore, func() []publishedMsg { return published }
}

// subscriptionUpdates filters published messages to subscription.update events.
func subscriptionUpdates(published []publishedMsg) []publishedMsg {
	var out []publishedMsg
	for _, p := range published {
		if strings.HasPrefix(p.subj, "chat.user.") && strings.HasSuffix(p.subj, ".event.subscription.update") {
			out = append(out, p)
		}
	}
	return out
}

// messagesCanonical filters published messages to the canonical message stream for a siteID.
func messagesCanonical(published []publishedMsg, siteID string) []publishedMsg {
	var out []publishedMsg
	prefix := fmt.Sprintf("chat.msg.canonical.%s", siteID)
	for _, p := range published {
		if strings.HasPrefix(p.subj, prefix) {
			out = append(out, p)
		}
	}
	return out
}

// outboxFor filters published messages to the outbox stream for a specific destSiteID and eventType.
func outboxFor(published []publishedMsg, destSiteID, eventType string) []publishedMsg {
	var out []publishedMsg
	subj := fmt.Sprintf("outbox.site-A.to.%s.%s", destSiteID, eventType)
	for _, p := range published {
		if p.subj == subj {
			out = append(out, p)
		}
	}
	return out
}

// userResponseFor filters published messages to the async-job result for an account.
func userResponseFor(published []publishedMsg, account string) []publishedMsg {
	var out []publishedMsg
	prefix := fmt.Sprintf("chat.user.%s.response.", account)
	for _, p := range published {
		if strings.HasPrefix(p.subj, prefix) {
			out = append(out, p)
		}
	}
	return out
}

const testRequestID = "0193abcd-0193-7abc-89ab-0193abcd0193"

// makeCreateRoomBody marshals a CreateRoomRequest to JSON.
func makeCreateRoomBody(t *testing.T, req *model.CreateRoomRequest) []byte {
	t.Helper()
	data, err := json.Marshal(req)
	require.NoError(t, err)
	return data
}

// ---- Task 32: skeleton tests ----

// TestProcessCreateRoom_RequiresRequestID retired: see comment above the
// add-members equivalent. Missing X-Request-ID is now minted at the boundary
// rather than rejected at the handler.

// ---- Task 33: DM branch tests ----

func TestProcessCreateRoom_DM_BuildsTwoSubs(t *testing.T) {
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice A", ChineseName: "艾麗斯", SiteID: "site-A"}
	other := &model.User{ID: "u_bob", Account: "bob", EngName: "Bob B", ChineseName: "鮑伯", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "bob").Return(other, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)

	var capturedSubs []*model.Subscription
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			capturedSubs = subs
			return nil
		})
	mockStore.EXPECT().FindDMSubscriptionPair(gomock.Any(), "room-dm-1", "alice").
		DoAndReturn(func(_ context.Context, _, _ string) (*model.Subscription, *model.Subscription, error) {
			return capturedSubs[0], capturedSubs[1], nil
		})
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-dm-1").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID:           "room-dm-1",
		RequesterAccount: "alice",
		Users:            []string{"bob"}, // no orgs/channels/name → DM
		Timestamp:        time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	require.Len(t, capturedSubs, 2)

	// alice's sub: Name = other's account
	aliceSub := capturedSubs[0]
	assert.Equal(t, "u_alice", aliceSub.User.ID)
	assert.Equal(t, other.Account, aliceSub.Name)
	assert.Nil(t, aliceSub.Roles)
	assert.False(t, aliceSub.IsSubscribed)
	assert.Equal(t, model.RoomTypeDM, aliceSub.RoomType)

	// bob's sub: Name = requester's account
	bobSub := capturedSubs[1]
	assert.Equal(t, "u_bob", bobSub.User.ID)
	assert.Equal(t, requester.Account, bobSub.Name)
	assert.Nil(t, bobSub.Roles)
	assert.False(t, bobSub.IsSubscribed)

	// No sys messages for DM
	assert.Empty(t, messagesCanonical(getPublished(), "site-A"))
}

func TestProcessCreateRoom_DM_EmitsNoSysMessages(t *testing.T) {
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice A", ChineseName: "艾麗斯", SiteID: "site-A"}
	other := &model.User{ID: "u_bob", Account: "bob", EngName: "Bob B", ChineseName: "鮑伯", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "bob").Return(other, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().FindDMSubscriptionPair(gomock.Any(), "room-dm-1", "alice").Return(
		&model.Subscription{User: model.SubscriptionUser{ID: "u_alice", Account: "alice"}},
		&model.Subscription{User: model.SubscriptionUser{ID: "u_bob", Account: "bob"}},
		nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-dm-1").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: "room-dm-1", RequesterAccount: "alice",
		Users: []string{"bob"}, Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	assert.Empty(t, messagesCanonical(getPublished(), "site-A"), "DM must emit no sys-messages")
}

// ---- Task 33: botDM branch tests ----

func TestProcessCreateRoom_BotDM_HasIsSubscribed(t *testing.T) {
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice A", ChineseName: "艾麗斯", SiteID: "site-A"}
	bot := &model.User{ID: "u_bot", Account: "helper.bot", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "helper.bot").Return(bot, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)

	var capturedSubs []*model.Subscription
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			capturedSubs = subs
			return nil
		})

	// After bulk-create, handler re-reads canonical sub pair via FindDMSubscriptionPair.
	// Return the same in-memory subs (no collision in this happy path).
	mockStore.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").
		DoAndReturn(func(_ context.Context, _, _ string) (*model.Subscription, *model.Subscription, error) {
			return capturedSubs[0], capturedSubs[1], nil
		})

	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-bot-1").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: "room-bot-1", RequesterAccount: "alice",
		Users:     []string{"helper.bot"},
		Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	require.Len(t, capturedSubs, 2)

	// human side (alice): Name = bot's account, IsSubscribed = true
	humanSub := capturedSubs[0]
	assert.Equal(t, "u_alice", humanSub.User.ID)
	assert.Equal(t, bot.Account, humanSub.Name)
	assert.True(t, humanSub.IsSubscribed)

	// bot side: Name = requester's account, IsSubscribed = false
	botSub := capturedSubs[1]
	assert.Equal(t, "u_bot", botSub.User.ID)
	assert.Equal(t, requester.Account, botSub.Name)
	assert.False(t, botSub.IsSubscribed)

	assert.Empty(t, messagesCanonical(getPublished(), "site-A"), "botDM must emit no sys-messages")
}

// ---- Task 34: Channel branch tests ----

func TestProcessCreateRoom_Channel_BuildsSubsAndMembers(t *testing.T) {
	h, mockStore, _ := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice A", ChineseName: "艾麗斯", SiteID: "site-A"}
	invited := []model.User{
		{ID: "u_bob", Account: "bob", EngName: "Bob B", ChineseName: "鮑伯", SiteID: "site-A"},
		{ID: "u_carol", Account: "carol", EngName: "Carol C", ChineseName: "卡羅", SiteID: "site-A"},
	}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	// orgs present → ListNewMembersForNewRoom returns bob+carol (alice already stripped by service)
	mockStore.EXPECT().ListNewMembersForNewRoom(gomock.Any(), []string{"org1"}, []string{"bob", "carol"}, "alice").
		Return([]string{"bob", "carol"}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "carol"}).Return(invited, nil)

	var capturedSubs []*model.Subscription
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			capturedSubs = subs
			return nil
		})

	var capturedMembers []*model.RoomMember
	mockStore.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, members []*model.RoomMember) error {
			capturedMembers = members
			return nil
		})
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-ch-1").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: "room-ch-1", Name: "Deal Team", RequesterAccount: "alice",
		Users: []string{"bob", "carol"}, Orgs: []string{"org1"},
		ResolvedUsers: []string{"bob", "carol"}, ResolvedOrgs: []string{"org1"},
		Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	// 3 subs: alice (owner — first), bob (member), carol (member)
	require.Len(t, capturedSubs, 3)
	ownerSub := capturedSubs[0]
	assert.Equal(t, "u_alice", ownerSub.User.ID)
	assert.Equal(t, []model.Role{model.RoleOwner}, ownerSub.Roles)
	assert.Equal(t, "Deal Team", ownerSub.Name)

	memberSub := capturedSubs[1]
	assert.Equal(t, []model.Role{model.RoleMember}, memberSub.Roles)

	// 4 room_members: 2 individuals (bob+carol) + 1 org + 1 owner (alice)
	require.Len(t, capturedMembers, 4)
	types := make([]model.RoomMemberType, 0, 4)
	for _, m := range capturedMembers {
		types = append(types, m.Member.Type)
	}
	assert.Equal(t, 3, countType(types, model.RoomMemberIndividual))
	assert.Equal(t, 1, countType(types, model.RoomMemberOrg))
}

// countType counts occurrences of t in slice.
func countType(types []model.RoomMemberType, mt model.RoomMemberType) int {
	n := 0
	for _, t := range types {
		if t == mt {
			n++
		}
	}
	return n
}

func TestProcessCreateRoom_Channel_NoOrgsSkipsRoomMembers(t *testing.T) {
	h, mockStore, _ := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice A", ChineseName: "艾麗斯", SiteID: "site-A"}
	invited := []model.User{
		{ID: "u_bob", Account: "bob", EngName: "Bob B", ChineseName: "鮑伯", SiteID: "site-A"},
	}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ListNewMembersForNewRoom(gomock.Any(), gomock.Nil(), []string{"bob"}, "alice").
		Return([]string{"bob"}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return(invited, nil)

	var capturedSubs []*model.Subscription
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			capturedSubs = subs
			return nil
		})
	// Lite-mode: BulkCreateRoomMembers MUST NOT be called when no orgs are
	// resolved. room_members stays empty until an org later joins, at which
	// point the backfill loop in processAddMembers reads from subscriptions.
	// (gomock fails the test on any unexpected call, so omitting the EXPECT
	// is the assertion.)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-ch-2").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: "room-ch-2", Name: "Small Channel", RequesterAccount: "alice",
		Users: []string{"bob"}, Orgs: []string{}, // no orgs
		ResolvedUsers: []string{"bob"}, ResolvedOrgs: []string{},
		Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	// Owner + invited individual sub still land in `subscriptions` — that's
	// the source of truth for who is in the room while in lite-mode.
	require.Len(t, capturedSubs, 2)
}

// ---- Task 35: subscription.update fan-out ----

func TestProcessCreateRoom_Channel_FiresSubscriptionUpdateForEverySub(t *testing.T) {
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice A", ChineseName: "艾麗斯", SiteID: "site-A"}
	invited := []model.User{
		{ID: "u_bob", Account: "bob", EngName: "Bob B", ChineseName: "鮑伯", SiteID: "site-A"},
	}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ListNewMembersForNewRoom(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{"bob"}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(invited, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-ch-3").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: "room-ch-3", Name: "Test Channel", RequesterAccount: "alice",
		Users: []string{"bob"}, Orgs: []string{"org1"},
		ResolvedUsers: []string{"bob"}, ResolvedOrgs: []string{"org1"},
		Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	updates := subscriptionUpdates(getPublished())
	// 2 subs (bob + alice) → 2 subscription.update events
	assert.Len(t, updates, 2)

	// Verify subjects cover both accounts
	subjects := make([]string, 0, len(updates))
	for _, u := range updates {
		subjects = append(subjects, u.subj)
	}
	assert.Contains(t, subjects, subject.SubscriptionUpdate("alice"))
	assert.Contains(t, subjects, subject.SubscriptionUpdate("bob"))
}

// ---- Task 36: sys-messages ----

func TestProcessCreateRoom_Channel_EmitsSysMessages(t *testing.T) {
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice A", ChineseName: "艾麗斯", SiteID: "site-A"}
	invited := []model.User{
		{ID: "u_bob", Account: "bob", EngName: "Bob B", ChineseName: "鮑伯", SiteID: "site-A"},
	}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ListNewMembersForNewRoom(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{"bob"}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(invited, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-ch-4").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: "room-ch-4", Name: "Sys Msg Channel", RequesterAccount: "alice",
		Users: []string{"bob"}, Orgs: []string{"org1"},
		ResolvedUsers: []string{"bob"}, ResolvedOrgs: []string{"org1"},
		Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	canonical := messagesCanonical(getPublished(), "site-A")
	require.Len(t, canonical, 2, "expected room_created + members_added sys messages")

	// Unmarshal and verify message types
	var evt1 model.MessageEvent
	require.NoError(t, json.Unmarshal(canonical[0].data, &evt1))
	assert.Equal(t, model.MessageTypeRoomCreated, evt1.Message.Type)
	assert.Equal(t, "room-ch-4", evt1.Message.RoomID)
	// ID must be deterministic from requestID
	expectedID1 := idgen.MessageIDFromRequestID(testRequestID, "room_created")
	assert.Equal(t, expectedID1, evt1.Message.ID)

	var evt2 model.MessageEvent
	require.NoError(t, json.Unmarshal(canonical[1].data, &evt2))
	assert.Equal(t, model.MessageTypeMembersAdded, evt2.Message.Type)
	expectedID2 := idgen.MessageIDFromRequestID(testRequestID, "members_added")
	assert.Equal(t, expectedID2, evt2.Message.ID)
}

// Sys-message payloads must carry the LITERAL request (Users/Orgs/Channels), not the
// post-expansion resolved set. This guards against drift if someone later changes the
// worker to use ResolvedUsers/ResolvedOrgs in the sys-msg path.
func TestProcessCreateRoom_Channel_SysMsgUsesLiteralRequest(t *testing.T) {
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice A", ChineseName: "艾麗斯", SiteID: "site-A"}
	// Resolved set expands org1 → [bob, carol, dave] but the literal request only named [bob] + [org1].
	invited := []model.User{
		{ID: "u_bob", Account: "bob", EngName: "Bob B", ChineseName: "鮑伯", SiteID: "site-A"},
		{ID: "u_carol", Account: "carol", EngName: "Carol C", ChineseName: "卡羅", SiteID: "site-A"},
		{ID: "u_dave", Account: "dave", EngName: "Dave D", ChineseName: "戴夫", SiteID: "site-A"},
	}
	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ListNewMembersForNewRoom(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]string{"bob", "carol", "dave"}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "carol", "dave"}).Return(invited, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-ch-lit").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: "room-ch-lit", Name: "Literal Test", RequesterAccount: "alice",
		Users: []string{"bob"}, Orgs: []string{"org1"},
		ResolvedUsers: []string{"bob", "carol", "dave"}, ResolvedOrgs: []string{"org1"},
		Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	canonical := messagesCanonical(getPublished(), "site-A")
	require.Len(t, canonical, 2)

	// room_created sys-msg payload
	var evt1 model.MessageEvent
	require.NoError(t, json.Unmarshal(canonical[0].data, &evt1))
	var rc model.RoomCreated
	require.NoError(t, json.Unmarshal(evt1.Message.SysMsgData, &rc))
	assert.Equal(t, []string{"bob"}, rc.Users, "RoomCreated.Users must be the literal request, not the resolved set")
	assert.Equal(t, []string{"org1"}, rc.Orgs, "RoomCreated.Orgs must be the literal request")

	// members_added sys-msg payload
	var evt2 model.MessageEvent
	require.NoError(t, json.Unmarshal(canonical[1].data, &evt2))
	var ma model.MembersAdded
	require.NoError(t, json.Unmarshal(evt2.Message.SysMsgData, &ma))
	assert.Equal(t, []string{"bob"}, ma.Individuals, "MembersAdded.Individuals must be the literal request")
	assert.Equal(t, []string{"org1"}, ma.Orgs, "MembersAdded.Orgs must be the literal request")
}

// ---- Task 37: outbox + async-job ----

func TestProcessCreateRoom_Channel_OutboxPerRemoteSite(t *testing.T) {
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice A", ChineseName: "艾麗斯", SiteID: "site-A"}
	// bob is on site-B → should trigger outbox
	invited := []model.User{
		{ID: "u_bob", Account: "bob", EngName: "Bob B", ChineseName: "鮑伯", SiteID: "site-B"},
	}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ListNewMembersForNewRoom(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{"bob"}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(invited, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-ch-5").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: "room-ch-5", Name: "Cross Site", RequesterAccount: "alice",
		Users: []string{"bob"}, Orgs: []string{"org1"},
		ResolvedUsers: []string{"bob"}, ResolvedOrgs: []string{"org1"},
		Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	outboxMsgs := outboxFor(getPublished(), "site-B", model.OutboxMemberAdded)
	require.Len(t, outboxMsgs, 1)

	var envelope model.OutboxEvent
	require.NoError(t, json.Unmarshal(outboxMsgs[0].data, &envelope))
	assert.Equal(t, model.OutboxMemberAdded, envelope.Type)
	assert.Equal(t, "site-A", envelope.SiteID)
	assert.Equal(t, "site-B", envelope.DestSiteID)

	var payload model.MemberAddEvent
	require.NoError(t, json.Unmarshal(envelope.Payload, &payload))
	assert.Equal(t, "room-ch-5", payload.RoomID)
	assert.Equal(t, model.RoomTypeChannel, payload.RoomType)
	assert.Equal(t, []string{"bob"}, payload.Accounts)
	assert.Equal(t, "alice", payload.RequesterAccount)
}

func TestProcessCreateRoom_Channel_EmitsAsyncJobOk(t *testing.T) {
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice A", ChineseName: "艾麗斯", SiteID: "site-A"}
	invited := []model.User{
		{ID: "u_bob", Account: "bob", EngName: "Bob B", ChineseName: "鮑伯", SiteID: "site-A"},
	}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ListNewMembersForNewRoom(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{"bob"}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(invited, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-ch-6").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: "room-ch-6", Name: "Job Test", RequesterAccount: "alice",
		Users: []string{"bob"}, Orgs: []string{"org1"},
		ResolvedUsers: []string{"bob"}, ResolvedOrgs: []string{"org1"},
		Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	responses := userResponseFor(getPublished(), "alice")
	require.NotEmpty(t, responses)

	var result model.AsyncJobResult
	require.NoError(t, json.Unmarshal(responses[0].data, &result))
	assert.Equal(t, model.AsyncJobStatusOK, result.Status)
	assert.Equal(t, model.AsyncJobOpRoomCreate, result.Operation)
}

// ---- Permanent-error coverage for HandleJetStreamMsg Ack path + errcode.Permanent marker ----

func TestProcessCreateRoom_RoomIDCollisionMismatchType_ReturnsPermanent(t *testing.T) {
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice A", ChineseName: "艾麗斯", SiteID: "site-A"}
	other := &model.User{ID: "u_bob", Account: "bob", EngName: "Bob B", ChineseName: "鮑伯", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	// Counterpart resolved upfront so CreateRoom can set UIDs/Accounts in one write.
	mockStore.EXPECT().GetUser(gomock.Any(), "bob").Return(other, nil)
	// Insert collides on _id.
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(mongo.WriteException{
		WriteErrors: []mongo.WriteError{{Code: 11000, Message: "duplicate key"}},
	})
	// Existing room has DIFFERENT type (channel) than the request (DM).
	mockStore.EXPECT().GetRoom(gomock.Any(), gomock.Any()).Return(&model.Room{
		ID: "room-collide", Type: model.RoomTypeChannel, SiteID: "site-A",
	}, nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID:           "room-collide",
		RequesterAccount: "alice",
		Users:            []string{"bob"}, // DM intent
		Timestamp:        time.Now().UnixMilli(),
	})

	err := h.processCreateRoom(ctx, body)
	require.Error(t, err)
	assert.ErrorIs(t, err, errPermanent)
	var pe *errcode.PermanentError
	assert.True(t, errors.As(err, &pe), "collision must be an explicit permanent error")
	assert.Contains(t, err.Error(), "room ID collision")

	// Async-job error event must be published (defer fires before return).
	// Collision classifies to conflict (mirrors the sync-DM errRoomIDCollision
	// path); permanence is explicit, not category-inferred.
	responses := userResponseFor(getPublished(), "alice")
	require.NotEmpty(t, responses, "permanent error must publish async-job error event")
	var result model.AsyncJobResult
	require.NoError(t, json.Unmarshal(responses[0].data, &result))
	assert.Equal(t, model.AsyncJobStatusError, result.Status)
	assert.Equal(t, string(errcode.CodeConflict), result.Code)
}

func TestFillAsyncError_PermanentForbiddenWithReason(t *testing.T) {
	h := &Handler{}
	var result model.AsyncJobResult
	jobErr := permanent(errcode.Forbidden("only room members can act", errcode.WithReason(errcode.RoomNotMember)))
	h.fillAsyncError(context.Background(), &result, jobErr)
	assert.Equal(t, model.AsyncJobStatusError, result.Status)
	assert.Equal(t, "only room members can act", result.Error)
	assert.Equal(t, string(errcode.CodeForbidden), result.Code)
	assert.Equal(t, string(errcode.RoomNotMember), result.Reason)
}

func TestFillAsyncError_RawInfraCollapsesToInternal(t *testing.T) {
	h := &Handler{}
	var result model.AsyncJobResult
	jobErr := fmt.Errorf("transient store error: %w", errors.New("connection reset"))
	h.fillAsyncError(context.Background(), &result, jobErr)
	assert.Equal(t, model.AsyncJobStatusError, result.Status)
	assert.Equal(t, "internal error", result.Error)
	assert.Equal(t, string(errcode.CodeInternal), result.Code)
	assert.Empty(t, result.Reason)
}

func TestFillAsyncError_PermanentInternalCollision(t *testing.T) {
	h := &Handler{}
	var result model.AsyncJobResult
	jobErr := permanent(errcode.Internal("room ID collision (existing type=channel)"))
	h.fillAsyncError(context.Background(), &result, jobErr)
	assert.Equal(t, string(errcode.CodeInternal), result.Code)
	// Permanence is explicit; an internal-category permanent error still Acks.
	var pe *errcode.PermanentError
	assert.True(t, errors.As(jobErr, &pe))
}

// newRequestCtx returns a context carrying a syntactically-valid X-Request-ID.
func newRequestCtx() context.Context {
	return natsutil.WithRequestID(context.Background(), "01970a4f-8c2d-7c9a-abcd-e0123456789f")
}

// dmCapturedPublish + dmPublishCapture are unit-test-local equivalents of the
// integration_test.go types; same shape under a different name to avoid collision
// when both files compile together under the `integration` build tag.
type dmCapturedPublish struct {
	subject string
	data    []byte
	msgID   string
}

type dmPublishCapture struct {
	mu       sync.Mutex
	captured []dmCapturedPublish
}

func (c *dmPublishCapture) fn(_ context.Context, subj string, data []byte, msgID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.captured = append(c.captured, dmCapturedPublish{subject: subj, data: append([]byte(nil), data...), msgID: msgID})
	return nil
}

// newSyncDMTestHandler builds a Handler wired to a fresh mock store + capture.
// Mirrors newAddMembersTestHandler's shape for consistency.
func newSyncDMTestHandler(t *testing.T) (*Handler, *MockSubscriptionStore, *dmPublishCapture) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	capture := &dmPublishCapture{}
	h := &Handler{siteID: "site-a", store: store, publish: capture.fn}
	return h, store, capture
}

// marshalReq JSON-encodes v or fails the test on error.
func marshalReq(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return data
}

// assertSyncDMInternal asserts err marshals (via errnats) to an internal-code
// envelope with the generic "internal error" message and no leaked cause.
func assertSyncDMInternal(t *testing.T, err error) {
	t.Helper()
	data := errnats.Marshal(context.Background(), err)
	e, ok := errcode.Parse(data)
	require.True(t, ok, "must marshal to an error envelope: %s", data)
	assert.Equal(t, errcode.CodeInternal, e.Code)
	assert.Equal(t, "internal error", e.Message)
}

// TestSyncDMErrorEnvelope asserts the wire envelope errnats produces for each
// sync-DM failure mode (replacing the deleted sanitizeSyncDMError). Validation
// sentinels surface as bad_request with their message; lookup/infra/collision
// detail never leaks — user-lookup and unknown errors collapse to internal,
// the collision becomes a conflict with a generic message.
func TestSyncDMErrorEnvelope(t *testing.T) {
	cases := []struct {
		name     string
		in       error
		wantCode errcode.Code
		wantMsg  string
	}{
		{"invalid sync DM request", errInvalidSyncDMRequest, errcode.CodeBadRequest, "invalid sync DM request"},
		{"cross-site requester", errCrossSiteRequester, errcode.CodeBadRequest, "requester is not on this site"},
		{"user lookup failed collapses to internal", errUserLookupFailed, errcode.CodeInternal, "internal error"},
		{"room ID collision is a conflict", errRoomIDCollision, errcode.CodeConflict, "room id collision (existing room metadata mismatch)"},
		{"unknown error collapses to internal", errors.New("mongo: connection refused"), errcode.CodeInternal, "internal error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := errnats.Marshal(context.Background(), tc.in)
			e, ok := errcode.Parse(data)
			require.True(t, ok, "must marshal to an error envelope: %s", data)
			assert.Equal(t, tc.wantCode, e.Code)
			assert.Equal(t, tc.wantMsg, e.Message)
			assert.NotContains(t, string(data), "mongo", "infra cause must never leak")
		})
	}
}

// TestHandleSyncCreateDM_MissingRequestID retired — see the comment above
// TestProcessAddMembers_RequiresRequestID.

func TestHandleSyncCreateDM_InvalidJSON(t *testing.T) {
	h := &Handler{siteID: "site-a"}
	_, err := h.handleSyncCreateDM(newRequestCtx(), []byte("{not json"))
	assert.ErrorIs(t, err, errInvalidSyncDMRequest)
}

func TestHandleSyncCreateDM_InvalidRoomType(t *testing.T) {
	h := &Handler{siteID: "site-a"}
	req := model.SyncCreateDMRequest{
		RoomType:         model.RoomTypeChannel,
		RequesterAccount: "alice",
		OtherAccount:     "bob",
	}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	assert.ErrorIs(t, err, errInvalidSyncDMRequest)
}

func TestHandleSyncCreateDM_EmptyAccounts(t *testing.T) {
	h := &Handler{siteID: "site-a"}
	cases := []model.SyncCreateDMRequest{
		{RoomType: model.RoomTypeDM, RequesterAccount: "", OtherAccount: "bob"},
		{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: ""},
	}
	for _, req := range cases {
		data := marshalReq(t, req)
		_, err := h.handleSyncCreateDM(newRequestCtx(), data)
		assert.ErrorIs(t, err, errInvalidSyncDMRequest)
	}
}

func TestHandleSyncCreateDM_SelfDM(t *testing.T) {
	h, store, capture := newSyncDMTestHandler(t)

	alice := model.User{ID: "u-alice", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛麗絲"}
	// Self-DM looks up a single account.
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{alice}, nil)

	var insertedRoom *model.Room
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *model.Room) error {
			insertedRoom = r
			return nil
		})

	var captured []*model.Subscription
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			captured = subs
			return nil
		})

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "alice"}
	data := marshalReq(t, req)
	reply, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.NoError(t, err)
	require.NotNil(t, reply)
	assert.True(t, reply.Success)
	assert.True(t, reply.Subscription.Favorite)

	// Single-member room, channel-style id, type dm. No read-back:
	// GetSubscription / FindDMSubscriptionPair are never called.
	require.NotNil(t, insertedRoom)
	assert.Len(t, insertedRoom.ID, 17, "channel-style room id")
	assert.Equal(t, model.RoomTypeDM, insertedRoom.Type)
	assert.Equal(t, "site-a", insertedRoom.SiteID)
	assert.Equal(t, 1, insertedRoom.UserCount)
	assert.Equal(t, 0, insertedRoom.AppCount)
	assert.Equal(t, []string{"u-alice"}, insertedRoom.UIDs)
	assert.Equal(t, []string{"alice"}, insertedRoom.Accounts)

	// Exactly one sub: favorited, subscribed, self-named, type dm.
	require.Len(t, captured, 1)
	assert.Equal(t, "u-alice", captured[0].User.ID)
	assert.Equal(t, insertedRoom.ID, captured[0].RoomID)
	assert.Equal(t, "alice", captured[0].Name)
	assert.Equal(t, model.RoomTypeDM, captured[0].RoomType)
	assert.True(t, captured[0].IsSubscribed)
	assert.True(t, captured[0].Favorite)

	// Reply returns the in-memory sub directly (no read-back round-trip).
	assert.Equal(t, *captured[0], reply.Subscription)

	// subscription.update only — same-site self-DM; no outbox and no canonical event (Option C).
	require.Len(t, capture.captured, 1)
	assert.Equal(t, subject.SubscriptionUpdate("alice"), capture.captured[0].subject)
}

func TestHandleSyncCreateDM_SelfBotDMRejected(t *testing.T) {
	h := &Handler{siteID: "site-a"}
	req := model.SyncCreateDMRequest{
		RoomType:         model.RoomTypeBotDM,
		RequesterAccount: "alice",
		OtherAccount:     "alice",
	}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	assert.ErrorIs(t, err, errInvalidSyncDMRequest)
}

func TestHandleSyncCreateDM_SelfDM_StoreErrors(t *testing.T) {
	cases := []struct {
		name      string
		setup     func(store *MockSubscriptionStore)
		wantErrIn string
	}{
		{
			name: "CreateRoom fails",
			setup: func(store *MockSubscriptionStore) {
				store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(errors.New("mongo down"))
			},
			wantErrIn: "create self-DM room",
		},
		{
			name: "BulkCreateSubscriptions fails",
			setup: func(store *MockSubscriptionStore) {
				store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
				store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(errors.New("write failed"))
			},
			wantErrIn: "create self-DM subscription",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, store, capture := newSyncDMTestHandler(t)
			alice := model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"}
			store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{alice}, nil)
			tc.setup(store)

			data := marshalReq(t, model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "alice"})
			_, err := h.handleSyncCreateDM(newRequestCtx(), data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrIn)
			assert.Empty(t, capture.captured, "no publish on store error")
		})
	}
}

func TestHandleSyncCreateDM_RequesterNotFound(t *testing.T) {
	h, store, _ := newSyncDMTestHandler(t)

	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, nil)

	req := model.SyncCreateDMRequest{
		RoomType:         model.RoomTypeDM,
		RequesterAccount: "alice",
		OtherAccount:     "bob",
	}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	assert.ErrorIs(t, err, errUserLookupFailed)
}

func TestHandleSyncCreateDM_OtherNotFound(t *testing.T) {
	h, store, _ := newSyncDMTestHandler(t)

	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{{ID: "u-alice", Account: "alice", SiteID: "site-a"}}, nil)

	req := model.SyncCreateDMRequest{
		RoomType:         model.RoomTypeDM,
		RequesterAccount: "alice",
		OtherAccount:     "bob",
	}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	assert.ErrorIs(t, err, errUserLookupFailed)
}

func TestHandleSyncCreateDM_CrossSiteRequester(t *testing.T) {
	h, store, _ := newSyncDMTestHandler(t)

	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{{ID: "u-alice", Account: "alice", SiteID: "site-b"}}, nil)

	req := model.SyncCreateDMRequest{
		RoomType:         model.RoomTypeDM,
		RequesterAccount: "alice",
		OtherAccount:     "bob",
	}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	assert.ErrorIs(t, err, errCrossSiteRequester)
}

func TestHandleSyncCreateDM_RoomCollisionMismatch(t *testing.T) {
	roomID := idgen.BuildDMRoomID("u-alice", "u-bob")
	cases := []struct {
		name     string
		existing model.Room
	}{
		// Type and SiteID mismatches still trip the structural-compatibility guard.
		// The structural check alone is sufficient: subscription writes are
		// idempotent, so "requester not a member of existing room" is no longer
		// a collision — it's a legitimate mid-write crash recovery case that
		// the retry must complete by re-running BulkCreateSubscriptions.
		{"type mismatch", model.Room{ID: roomID, Type: model.RoomTypeChannel, SiteID: "site-a", Name: ""}},
		{"siteID mismatch", model.Room{ID: roomID, Type: model.RoomTypeDM, SiteID: "site-other", Name: ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, store, _ := newSyncDMTestHandler(t)
			requester := &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"}
			other := &model.User{ID: "u-bob", Account: "bob", SiteID: "site-a"}
			store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{*requester, *other}, nil)
			dupErr := mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}}
			store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(dupErr)
			existing := tc.existing
			store.EXPECT().GetRoom(gomock.Any(), gomock.Any()).Return(&existing, nil)

			req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
			data := marshalReq(t, req)
			_, err := h.handleSyncCreateDM(newRequestCtx(), data)
			assert.ErrorIs(t, err, errRoomIDCollision)
		})
	}
}

func TestHandleSyncCreateDM_DM_PersistsSubsAndReturnsRequester(t *testing.T) {
	h, store, _ := newSyncDMTestHandler(t)

	requester := &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"}
	other := &model.User{ID: "u-bob", Account: "bob", SiteID: "site-a"}
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{*requester, *other}, nil)
	var insertedRoom *model.Room
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *model.Room) error {
			insertedRoom = r
			return nil
		})

	var captured []*model.Subscription
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			captured = subs
			return nil
		})
	store.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").Return(
		&model.Subscription{
			ID:       "canonical-alice-sub",
			User:     model.SubscriptionUser{ID: "u-alice", Account: "alice"},
			RoomID:   idgen.BuildDMRoomID("u-alice", "u-bob"),
			Name:     "bob",
			RoomType: model.RoomTypeDM,
		},
		&model.Subscription{
			ID:       "canonical-bob-sub",
			User:     model.SubscriptionUser{ID: "u-bob", Account: "bob"},
			RoomID:   idgen.BuildDMRoomID("u-alice", "u-bob"),
			Name:     "alice",
			RoomType: model.RoomTypeDM,
		}, nil)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
	data := marshalReq(t, req)
	reply, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.NoError(t, err)
	require.NotNil(t, reply)
	assert.True(t, reply.Success)
	assert.Equal(t, "canonical-alice-sub", reply.Subscription.ID)

	// DM room is inserted with userCount=2, appCount=0 — no Reconcile needed.
	require.NotNil(t, insertedRoom)
	assert.Equal(t, 2, insertedRoom.UserCount)
	assert.Equal(t, 0, insertedRoom.AppCount)

	// Both subs persisted: requester names other (IsSubscribed=false), other names requester.
	require.Len(t, captured, 2)
	roomID := idgen.BuildDMRoomID("u-alice", "u-bob")
	subByAccount := map[string]*model.Subscription{}
	for _, s := range captured {
		subByAccount[s.User.Account] = s
	}
	require.Contains(t, subByAccount, "alice")
	require.Contains(t, subByAccount, "bob")
	assert.Equal(t, "u-alice", subByAccount["alice"].User.ID)
	assert.Equal(t, roomID, subByAccount["alice"].RoomID)
	assert.Equal(t, "bob", subByAccount["alice"].Name)
	assert.Equal(t, model.RoomTypeDM, subByAccount["alice"].RoomType)
	assert.False(t, subByAccount["alice"].IsSubscribed)
	assert.Equal(t, "u-bob", subByAccount["bob"].User.ID)
	assert.Equal(t, "alice", subByAccount["bob"].Name)
	assert.False(t, subByAccount["bob"].IsSubscribed)
}

func TestHandleSyncCreateDM_BotDM_RequesterSubIsSubscribedTrue(t *testing.T) {
	h, store, _ := newSyncDMTestHandler(t)

	requester := &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"}
	bot := &model.User{ID: "u-bot", Account: "helper.bot", SiteID: "site-a"}
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{*requester, *bot}, nil)
	var insertedRoom *model.Room
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *model.Room) error {
			insertedRoom = r
			return nil
		})
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").Return(
		&model.Subscription{
			User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}, IsSubscribed: true,
		},
		&model.Subscription{
			User: model.SubscriptionUser{ID: "u-bot", Account: "helper.bot"}, IsSubscribed: false,
		}, nil)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeBotDM, RequesterAccount: "alice", OtherAccount: "helper.bot"}
	data := marshalReq(t, req)
	reply, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.NoError(t, err)
	require.NotNil(t, reply)
	assert.True(t, reply.Subscription.IsSubscribed)
	assert.Equal(t, "alice", reply.Subscription.User.Account)

	// botDM room is inserted with userCount=1, appCount=1 — no Reconcile needed.
	require.NotNil(t, insertedRoom)
	assert.Equal(t, 1, insertedRoom.UserCount)
	assert.Equal(t, 1, insertedRoom.AppCount)
}

// On a (roomId, u.account) collision, BulkCreateSubscriptions upserts with
// $setOnInsert (no field refresh) so the in-memory subs carry stale state;
// the handler must return the canonical persisted sub via FindDMSubscriptionPair.
func TestHandleSyncCreateDM_ReturnsCanonicalPersistedSub(t *testing.T) {
	h, store, _ := newSyncDMTestHandler(t)

	requester := &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"}
	other := &model.User{ID: "u-bob", Account: "bob", SiteID: "site-a"}
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{*requester, *other}, nil)
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	existingSub := &model.Subscription{
		ID:       "canonical-sub",
		User:     model.SubscriptionUser{ID: "u-alice", Account: "alice"},
		RoomID:   idgen.BuildDMRoomID("u-alice", "u-bob"),
		Name:     "bob",
		RoomType: model.RoomTypeDM,
	}
	store.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").Return(
		existingSub,
		&model.Subscription{
			ID: "canonical-bob-sub", User: model.SubscriptionUser{ID: "u-bob", Account: "bob"},
			RoomID: idgen.BuildDMRoomID("u-alice", "u-bob"), Name: "alice", RoomType: model.RoomTypeDM,
		}, nil)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
	data := marshalReq(t, req)
	reply, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.NoError(t, err)
	require.NotNil(t, reply)
	assert.Equal(t, "canonical-sub", reply.Subscription.ID)
}

// Transient store errors on GetUser must NOT be tagged as errUserLookupFailed (which
// signals "user does not exist"); they propagate as wrapped errors and surface
// as "internal error" in the errnats envelope.
func TestHandleSyncCreateDM_GetUserTransientError_Internal(t *testing.T) {
	h, store, _ := newSyncDMTestHandler(t)

	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, errors.New("mongo: connection refused"))

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.Error(t, err)
	assert.NotErrorIs(t, err, errUserLookupFailed,
		"transient error must not be tagged as user-not-found")
	assertSyncDMInternal(t, err)
}

func TestHandleSyncCreateDM_PublishesSubscriptionUpdateForBothUsers(t *testing.T) {
	h, store, capture := newSyncDMTestHandler(t)

	requester := &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"}
	other := &model.User{ID: "u-bob", Account: "bob", SiteID: "site-a"}
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{*requester, *other}, nil)
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").Return(
		&model.Subscription{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}},
		&model.Subscription{User: model.SubscriptionUser{ID: "u-bob", Account: "bob"}},
		nil)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.NoError(t, err)

	subjects := map[string]int{}
	for _, p := range capture.captured {
		subjects[p.subject]++
	}
	assert.Equal(t, 1, subjects[subject.SubscriptionUpdate("alice")])
	assert.Equal(t, 1, subjects[subject.SubscriptionUpdate("bob")])
}

func TestHandleSyncCreateDM_CrossSite_EmitsOutbox(t *testing.T) {
	h, store, capture := newSyncDMTestHandler(t)

	requester := &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"}
	other := &model.User{ID: "u-bob", Account: "bob", SiteID: "site-b"}
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{*requester, *other}, nil)
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").Return(
		&model.Subscription{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}},
		&model.Subscription{User: model.SubscriptionUser{ID: "u-bob", Account: "bob"}},
		nil)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.NoError(t, err)

	var outbox *dmCapturedPublish
	for i := range capture.captured {
		if capture.captured[i].subject == subject.Outbox("site-a", "site-b", model.OutboxMemberAdded) {
			outbox = &capture.captured[i]
			break
		}
	}
	require.NotNil(t, outbox, "expected a member_added outbox publish to site-b")

	var env model.OutboxEvent
	require.NoError(t, json.Unmarshal(outbox.data, &env))
	assert.Equal(t, model.OutboxMemberAdded, env.Type)
	assert.Equal(t, "site-a", env.SiteID)
	assert.Equal(t, "site-b", env.DestSiteID)

	var payload model.MemberAddEvent
	require.NoError(t, json.Unmarshal(env.Payload, &payload))
	assert.Equal(t, model.RoomTypeDM, payload.RoomType)
	assert.Equal(t, "", payload.RoomName)
	assert.Equal(t, "site-a", payload.SiteID)
	assert.Equal(t, []string{"bob"}, payload.Accounts)
	assert.Equal(t, "alice", payload.RequesterAccount)
	assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f:site-b", outbox.msgID)
}

func TestHandleSyncCreateDM_SameSite_NoOutbox(t *testing.T) {
	h, store, capture := newSyncDMTestHandler(t)

	requester := &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"}
	other := &model.User{ID: "u-bob", Account: "bob", SiteID: "site-a"}
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{*requester, *other}, nil)
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").Return(
		&model.Subscription{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}},
		&model.Subscription{User: model.SubscriptionUser{ID: "u-bob", Account: "bob"}},
		nil)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.NoError(t, err)

	for _, p := range capture.captured {
		assert.NotContains(t, p.subject, "outbox.", "no outbox publish expected for same-site DM")
	}
}

// Outbox publish failure must fail the request — otherwise the requester sees success
// while the remote site never learns about the room.
func TestHandleSyncCreateDM_OutboxPublishFails_FailsRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	failingPublish := func(_ context.Context, subj string, _ []byte, _ string) error {
		if strings.HasPrefix(subj, "outbox.") {
			return errors.New("jetstream pubAck failed")
		}
		return nil
	}
	h := &Handler{siteID: "site-a", store: store, publish: failingPublish}

	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{
		{ID: "u-alice", Account: "alice", SiteID: "site-a"},
		{ID: "u-bob", Account: "bob", SiteID: "site-b"},
	}, nil)
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").Return(
		&model.Subscription{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}},
		&model.Subscription{User: model.SubscriptionUser{ID: "u-bob", Account: "bob"}},
		nil)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.Error(t, err)
	assertSyncDMInternal(t, err)
}

// BulkCreateSubscriptions returning a transient error must surface as "internal error".
func TestHandleSyncCreateDM_BulkCreateSubsTransientError(t *testing.T) {
	h, store, _ := newSyncDMTestHandler(t)

	requester := &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"}
	other := &model.User{ID: "u-bob", Account: "bob", SiteID: "site-a"}
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{*requester, *other}, nil)
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		Return(errors.New("mongo: connection reset"))

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.Error(t, err)
	assertSyncDMInternal(t, err)
}

// On a CreateRoom dup-key with matching existing room (idempotent re-delivery),
// the handler must reuse existing.CreatedAt as acceptedAt — sub.JoinedAt and event
// timestamps reflect the original creation, not retry wall-clock. The structural
// check (type + site match) is all that's verified — BulkCreateSubscriptions is
// idempotent under the unique (roomId, u.account) index, so re-running it on
// retry is safe whether or not the requester already had a sub.
func TestHandleSyncCreateDM_IdempotentRecreate_UsesExistingCreatedAt(t *testing.T) {
	h, store, _ := newSyncDMTestHandler(t)

	requester := &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"}
	other := &model.User{ID: "u-bob", Account: "bob", SiteID: "site-a"}
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{*requester, *other}, nil)

	// CreateRoom hits dup-key; GetRoom returns a matching existing room with a known CreatedAt.
	originalCreatedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	roomID := idgen.BuildDMRoomID("u-alice", "u-bob")
	dupErr := mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}}
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(dupErr)
	store.EXPECT().GetRoom(gomock.Any(), gomock.Any()).Return(&model.Room{
		ID: roomID, Type: model.RoomTypeDM, SiteID: "site-a",
		Name:      "",
		CreatedAt: originalCreatedAt, UpdatedAt: originalCreatedAt,
	}, nil)

	var captured []*model.Subscription
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			captured = subs
			return nil
		})
	store.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").Return(
		&model.Subscription{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}, JoinedAt: originalCreatedAt},
		&model.Subscription{User: model.SubscriptionUser{ID: "u-bob", Account: "bob"}, JoinedAt: originalCreatedAt},
		nil)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeDM, RequesterAccount: "alice", OtherAccount: "bob"}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.NoError(t, err)

	require.Len(t, captured, 2)
	for _, s := range captured {
		assert.Equal(t, originalCreatedAt, s.JoinedAt,
			"sub.JoinedAt must reflect existing.CreatedAt on idempotent re-delivery, not retry wall-clock")
	}
}

// On a CreateRoom dup-key for a botDM, the in-memory sub.JoinedAt must
// track existing.CreatedAt — same idempotency rule as regular DM. The
// "re-subscribe refresh" semantic is owned by user-service, not by
// room-worker; here the upsert is purely for JetStream redelivery
// idempotency.
func TestHandleSyncCreateDM_BotDM_Recreate_PreservesExistingCreatedAt(t *testing.T) {
	h, store, _ := newSyncDMTestHandler(t)

	requester := &model.User{ID: "u-alice", Account: "alice", SiteID: "site-a"}
	bot := &model.User{ID: "u-bot", Account: "helper.bot", SiteID: "site-a"}
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{*requester, *bot}, nil)

	originalCreatedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	roomID := idgen.BuildDMRoomID("u-alice", "u-bot")
	dupErr := mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}}
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(dupErr)
	store.EXPECT().GetRoom(gomock.Any(), gomock.Any()).Return(&model.Room{
		ID: roomID, Type: model.RoomTypeBotDM, SiteID: "site-a",
		Name:      "",
		CreatedAt: originalCreatedAt, UpdatedAt: originalCreatedAt,
	}, nil)

	var captured []*model.Subscription
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, subs []*model.Subscription) error {
			captured = subs
			return nil
		})
	store.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").Return(
		&model.Subscription{User: model.SubscriptionUser{ID: "u-alice", Account: "alice"}, JoinedAt: originalCreatedAt},
		&model.Subscription{User: model.SubscriptionUser{ID: "u-bot", Account: "helper.bot"}, JoinedAt: originalCreatedAt},
		nil)

	req := model.SyncCreateDMRequest{RoomType: model.RoomTypeBotDM, RequesterAccount: "alice", OtherAccount: "helper.bot"}
	data := marshalReq(t, req)
	_, err := h.handleSyncCreateDM(newRequestCtx(), data)
	require.NoError(t, err)

	require.Len(t, captured, 2)
	for _, s := range captured {
		assert.True(t, s.JoinedAt.Equal(originalCreatedAt),
			"botDM sub.JoinedAt must reuse existing.CreatedAt on dup-key (JetStream redelivery idempotency)")
	}
}

type inboxCapturedPublish struct {
	subj  string
	data  []byte
	msgID string
}

func captureInboxPublishes() (PublishFunc, func() []inboxCapturedPublish) {
	var captured []inboxCapturedPublish
	fn := PublishFunc(func(_ context.Context, subj string, data []byte, msgID string) error {
		captured = append(captured, inboxCapturedPublish{subj: subj, data: append([]byte(nil), data...), msgID: msgID})
		return nil
	})
	return fn, func() []inboxCapturedPublish { return captured }
}

func findInboxMemberAdded(t *testing.T, captured []inboxCapturedPublish, siteID string) inboxCapturedPublish {
	t.Helper()
	want := subject.InboxMemberAdded(siteID)
	var matches []inboxCapturedPublish
	for _, p := range captured {
		if p.subj == want {
			matches = append(matches, p)
		}
	}
	require.Lenf(t, matches, 1, "expected exactly 1 publish to %s, got %d", want, len(matches))
	return matches[0]
}

func TestProcessCreateRoom_DM_PublishesLocalInbox(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	publish, getCaptured := captureInboxPublishes()
	h := &Handler{store: mockStore, publish: publish, siteID: "site-A", keyStore: testKeyStore, keySender: testKeySender}
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice", ChineseName: "艾", SiteID: "site-A"}
	// bob lives on site-B → cross-site DM
	other := &model.User{ID: "u_bob", Account: "bob", EngName: "Bob", ChineseName: "鮑", SiteID: "site-B"}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "bob").Return(other, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().FindDMSubscriptionPair(gomock.Any(), "room-dm-inbox", "alice").Return(
		&model.Subscription{User: model.SubscriptionUser{ID: "u_alice", Account: "alice"}},
		&model.Subscription{User: model.SubscriptionUser{ID: "u_bob", Account: "bob"}},
		nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-dm-inbox").Return(nil)

	ts := time.Now().UnixMilli()
	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID:           "room-dm-inbox",
		RequesterAccount: "alice",
		Users:            []string{"bob"},
		Timestamp:        ts,
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	got := findInboxMemberAdded(t, getCaptured(), "site-A")

	var outbox model.OutboxEvent
	require.NoError(t, json.Unmarshal(got.data, &outbox))
	assert.Equal(t, "member_added", outbox.Type)
	assert.Equal(t, "site-A", outbox.SiteID)
	assert.Equal(t, "site-A", outbox.DestSiteID, "self-loop publish: dest must equal origin")
	assert.Greater(t, outbox.Timestamp, int64(0))

	var inner model.MemberAddEvent
	require.NoError(t, json.Unmarshal(outbox.Payload, &inner))
	assert.Equal(t, "member_added", inner.Type)
	assert.Equal(t, "room-dm-inbox", inner.RoomID)
	assert.Empty(t, inner.RoomName, "DM rooms have no name")
	assert.ElementsMatch(t, []string{"alice", "bob"}, inner.Accounts,
		"DM INBOX publish must carry both creator and recipient")
	assert.Equal(t, "site-A", inner.SiteID)
	assert.Nil(t, inner.HistorySharedSince, "HistorySharedSince must be nil at create-time")

	wantMsgID := natsutil.OutboxDedupID(ctx, "site-A", "room-dm-inbox:alice:"+strconv.FormatInt(ts, 10))
	assert.Equal(t, wantMsgID, got.msgID, "Nats-Msg-Id must be natsutil.OutboxDedupID(ctx, originSite, payloadSeed)")
}

func TestProcessCreateRoom_Channel_PublishesLocalInbox(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	publish, getCaptured := captureInboxPublishes()
	h := &Handler{store: mockStore, publish: publish, siteID: "site-A", keyStore: testKeyStore, keySender: testKeySender}
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice", ChineseName: "艾", SiteID: "site-A"}
	invited := []model.User{
		{ID: "u_bob", Account: "bob", EngName: "Bob", ChineseName: "鮑", SiteID: "site-A"},
		{ID: "u_dave", Account: "dave", EngName: "Dave", ChineseName: "戴", SiteID: "site-B"},
	}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ListNewMembersForNewRoom(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]string{"bob", "dave"}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(invited, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-ch-inbox").Return(nil)

	ts := time.Now().UnixMilli()
	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: "room-ch-inbox", Name: "Mixed", RequesterAccount: "alice",
		Users: []string{"bob", "dave"}, Orgs: []string{"org1"},
		ResolvedUsers: []string{"bob", "dave"}, ResolvedOrgs: []string{"org1"},
		Timestamp: ts,
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	got := findInboxMemberAdded(t, getCaptured(), "site-A")

	var outbox model.OutboxEvent
	require.NoError(t, json.Unmarshal(got.data, &outbox))
	assert.Equal(t, "member_added", outbox.Type)
	assert.Equal(t, "site-A", outbox.SiteID)
	assert.Equal(t, "site-A", outbox.DestSiteID)

	var inner model.MemberAddEvent
	require.NoError(t, json.Unmarshal(outbox.Payload, &inner))
	assert.Equal(t, "room-ch-inbox", inner.RoomID)
	assert.Equal(t, "Mixed", inner.RoomName)
	assert.ElementsMatch(t, []string{"alice", "bob", "dave"}, inner.Accounts,
		"channel INBOX publish must carry creator + every auto-enrolled member (same-site + cross-site)")
	assert.Equal(t, "site-A", inner.SiteID)
	assert.Nil(t, inner.HistorySharedSince, "create-time event must be unrestricted regardless of req.History")

	wantMsgID := natsutil.OutboxDedupID(ctx, "site-A", "room-ch-inbox:alice:"+strconv.FormatInt(ts, 10))
	assert.Equal(t, wantMsgID, got.msgID)
}

func TestProcessCreateRoom_Channel_PublishesCrossSiteMemberAdded(t *testing.T) {
	h, mockStore, getPublished := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_alice", Account: "alice", EngName: "Alice", ChineseName: "艾", SiteID: "site-A"}
	invited := []model.User{
		{ID: "u_bob", Account: "bob", EngName: "Bob", ChineseName: "鮑", SiteID: "site-B"},
	}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ListNewMembersForNewRoom(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]string{"bob"}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(invited, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-ch-xsite").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID: "room-ch-xsite", Name: "Cross", RequesterAccount: "alice",
		Users: []string{"bob"}, Orgs: []string{"org1"},
		ResolvedUsers: []string{"bob"}, ResolvedOrgs: []string{"org1"},
		Timestamp: time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	memberAddedOutbox := outboxFor(getPublished(), "site-B", model.OutboxMemberAdded)
	require.Len(t, memberAddedOutbox, 1,
		"finishCreateRoom must emit outbox.{origin}.to.{remote}.member_added alongside room_created so the remote site's search-sync-worker updates its MV")

	var envelope model.OutboxEvent
	require.NoError(t, json.Unmarshal(memberAddedOutbox[0].data, &envelope))
	assert.Equal(t, model.OutboxMemberAdded, envelope.Type)
	assert.Equal(t, "site-A", envelope.SiteID)
	assert.Equal(t, "site-B", envelope.DestSiteID)

	var inner model.MemberAddEvent
	require.NoError(t, json.Unmarshal(envelope.Payload, &inner))
	assert.Equal(t, "room-ch-xsite", inner.RoomID)
	assert.Equal(t, "Cross", inner.RoomName)
	assert.Equal(t, model.RoomTypeChannel, inner.RoomType, "create-time member_added carries RoomType for inbox-worker dispatch")
	assert.Equal(t, []string{"bob"}, inner.Accounts, "carries only the remote-site accounts, mirroring processAddMembers")
	assert.Equal(t, "site-A", inner.SiteID, "inner SiteID is the origin (room's home)")
	assert.Equal(t, "alice", inner.RequesterAccount, "create-time member_added carries RequesterAccount for DM/botDM counterpart resolution")
	assert.Nil(t, inner.HistorySharedSince, "create-time event must be unrestricted")
}

// ---- Task 10: key-gate and fan-out tests ----

// TestBuildAndFanOutRoomKey_SendsToAllMembersIncludingRemoteSite verifies that buildAndFanOutRoomKey
// publishes a RoomKeyEvent for all members, including remote-site users. NATS supercluster routes
// user-subjects to home sites.
func TestBuildAndFanOutRoomKey_SendsToAllMembersIncludingRemoteSite(t *testing.T) {
	pub := &mockPublisher{}
	sender := roomkeysender.NewSender(pub)

	keyPair := &roomkeystore.VersionedKeyPair{
		Version: 3,
		KeyPair: roomkeystore.RoomKeyPair{
			PrivateKey: []byte("priv"),
		},
	}

	h := &Handler{
		keySender: sender,
		siteID:    "site-A",
	}

	users := []model.User{
		{Account: "alice", SiteID: "site-A"},
		{Account: "bob", SiteID: "site-A"},
		{Account: "carol", SiteID: "site-B"}, // remote — also receives key
	}

	err := h.buildAndFanOutRoomKey(context.Background(), "room-1", keyPair, users)
	require.NoError(t, err)
	assert.Equal(t, 3, pub.publishCount(), "all members including remote-site should receive key events")
}

func TestProcessCreateRoom_PermanentErrorWhenKeyMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	keyStore := NewMockRoomKeyStore(ctrl)

	keyStore.EXPECT().Get(gomock.Any(), "r1").Return(nil, nil) // no key

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore, nil)

	// Name is non-empty → determineRoomTypeFromPayload returns RoomTypeChannel.
	req := model.CreateRoomRequest{
		RoomID: "r1", RequesterAccount: "alice",
		Name: "general", Users: []string{"bob"},
		Timestamp: time.Now().UnixMilli(),
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	err := h.processCreateRoom(ctx, data)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errPermanent), "missing key must be permanent")
	assert.True(t, errors.Is(err, errRoomKeyAbsent), "missing key must satisfy errRoomKeyAbsent sentinel")
	// The SAME error value must yield the *errcode.Error (internal) for the reply
	// envelope AND still satisfy errors.Is(errRoomKeyAbsent) for the alert path.
	var ee *errcode.Error
	require.True(t, errors.As(err, &ee), "absent-key error must carry an *errcode.Error")
	assert.Equal(t, errcode.CodeInternal, ee.Code)
}

// ---- Task 11: fan-out current key to newly-added channel members ----

func TestProcessAddMembers_FansOutKeyToNewAccountsOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	keyStore := NewMockRoomKeyStore(ctrl)
	pub := &mockPublisher{}
	keySender := roomkeysender.NewSender(pub)

	mockStore.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Name: "deal team", Type: model.RoomTypeChannel, SiteID: "site-a",
	}, nil)
	mockStore.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").
		Return([]AddMemberCandidate{{Account: "charlie"}}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"charlie"}).Return([]model.User{
		{ID: "u_charlie", Account: "charlie", SiteID: "site-a", EngName: "Charlie", ChineseName: "查"},
	}, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛",
	}, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)

	pair := &roomkeystore.VersionedKeyPair{
		Version: 1,
		KeyPair: roomkeystore.RoomKeyPair{
			PrivateKey: []byte("privkey"),
		},
	}
	keyStore.EXPECT().Get(gomock.Any(), "r1").Return(pair, nil)

	h := NewHandler(mockStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore, keySender)

	req := model.AddMembersRequest{
		RoomID: "r1", RequesterAccount: "alice", Users: []string{"charlie"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), "0193abcd-0193-7abc-89ab-0193abcd0011")
	require.NoError(t, h.processAddMembers(ctx, data))

	// keySender published exactly one key event — for charlie only.
	assert.Equal(t, 1, pub.publishCount())
	assert.Contains(t, pub.subjects[0], "chat.user.charlie.event.room.key")
}

func TestProcessAddMembers_PermanentErrorWhenKeyMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	keyStore := NewMockRoomKeyStore(ctrl)

	mockStore.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Name: "deal team", Type: model.RoomTypeChannel, SiteID: "site-a",
	}, nil)
	mockStore.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").
		Return([]AddMemberCandidate{{Account: "charlie"}}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"charlie"}).Return([]model.User{
		{ID: "u_charlie", Account: "charlie", SiteID: "site-a", EngName: "Charlie", ChineseName: "查"},
	}, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛",
	}, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
	keyStore.EXPECT().Get(gomock.Any(), "r1").Return(nil, nil) // key missing

	h := NewHandler(mockStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore, roomkeysender.NewSender(&mockPublisher{}))

	req := model.AddMembersRequest{
		RoomID: "r1", RequesterAccount: "alice", Users: []string{"charlie"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), "0193abcd-0193-7abc-89ab-0193abcd0012")
	err := h.processAddMembers(ctx, data)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errPermanent))
	assert.True(t, errors.Is(err, errRoomKeyAbsent), "absent key must satisfy errRoomKeyAbsent sentinel")
}

// TestProcessAddMembers_TransientErrorWhenValkeyFails verifies that a non-nil
// error from keyStore.Get is treated as transient (NAK), not permanent-drop.
func TestProcessAddMembers_TransientErrorWhenValkeyFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	keyStore := NewMockRoomKeyStore(ctrl)

	mockStore.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Name: "deal team", Type: model.RoomTypeChannel, SiteID: "site-a",
	}, nil)
	mockStore.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").
		Return([]AddMemberCandidate{{Account: "charlie"}}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"charlie"}).Return([]model.User{
		{ID: "u_charlie", Account: "charlie", SiteID: "site-a", EngName: "Charlie", ChineseName: "查"},
	}, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{
		ID: "u_alice", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛",
	}, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "r1").Return(nil)
	keyStore.EXPECT().Get(gomock.Any(), "r1").Return(nil, fmt.Errorf("valkey timeout"))

	h := NewHandler(mockStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore, roomkeysender.NewSender(&mockPublisher{}))

	req := model.AddMembersRequest{
		RoomID: "r1", RequesterAccount: "alice", Users: []string{"charlie"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), "0193abcd-0193-7abc-89ab-0193abcd0014")
	err := h.processAddMembers(ctx, data)
	require.Error(t, err)
	assert.False(t, errors.Is(err, errPermanent), "valkey error must be transient (NAK), not permanent-drop")
	assert.Contains(t, err.Error(), "valkey timeout")
}

func TestProcessAddMembers_RejectsNonChannel(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	mockStore.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeDM, SiteID: "site-a",
	}, nil)

	h := NewHandler(mockStore, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, testKeyStore, testKeySender)
	req := model.AddMembersRequest{RoomID: "r1", RequesterAccount: "alice", Users: []string{"x"}, Timestamp: 1}
	data, _ := json.Marshal(req)
	err := h.processAddMembers(natsutil.WithRequestID(context.Background(), "0193abcd-0193-7abc-89ab-0193abcd0013"), data)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errPermanent))
}

// ---- Task 12: channel guard + version gate + fan-out to survivors ----

func TestProcessRemoveMember_RejectsNonChannel(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, testKeyStore, testKeySender)
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "bob", RoomType: model.RoomTypeDM}
	data, _ := json.Marshal(req)
	err := h.processRemoveMember(natsutil.WithRequestID(context.Background(), "req-1"), data)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errPermanent))
}

// TestFanOutRoomKeyToSurvivors_SendsToAllSurvivorsIncludingRemoteSite verifies that all survivors
// receive the updated key, including remote-site subscribers. NATS supercluster routes
// user-subjects to home sites.
func TestFanOutRoomKeyToSurvivors_SendsToAllSurvivorsIncludingRemoteSite(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	pub := &mockPublisher{}
	keySender := roomkeysender.NewSender(pub)

	pair := &roomkeystore.VersionedKeyPair{Version: 5, KeyPair: roomkeystore.RoomKeyPair{
		PrivateKey: bytes.Repeat([]byte{0x03}, 32),
	}}
	// Survivor accounts span the local site (alice, bob) and a remote site
	// (remote-carol); the caller projects these out of the subscriptions.
	survivorAccounts := []string{"alice", "bob", "remote-carol"}

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, nil, keySender)
	h.fanOutRoomKeyToSurvivors(context.Background(), "r1", pair, survivorAccounts)
	// alice, bob (site-a) and remote-carol (site-b) all receive the new key.
	assert.ElementsMatch(t, []string{
		"chat.user.alice.event.room.key",
		"chat.user.bob.event.room.key",
		"chat.user.remote-carol.event.room.key",
	}, pub.subjects)
}

// Task 2: Backfill must fire only on the first-org transition. The
// restructured handler calls HasOrgRoomMembers unconditionally and gates the
// backfill on `len(req.Orgs) > 0 && !hadOrgsBefore`.
func TestHandler_ProcessAddMembers_BackfillRunsOnFirstOrgTransition(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string{"o1"}, []string(nil), roomID).
		Return([]AddMemberCandidate{{Account: "u_new"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u_new"}).
		Return([]model.User{{ID: "u_new", Account: "u_new", SiteID: "site-a", EngName: "New", ChineseName: "新"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil) // first org

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)

	// First-org transition MUST call GetSubscriptionAccounts (backfill kickoff).
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return([]string{"existing_user"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"existing_user"}).
		Return([]model.User{{ID: "u_e", Account: "existing_user", SiteID: "site-a", EngName: "Ex", ChineseName: "存"}}, nil)

	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore: testKeyStore, keySender: testKeySender}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Orgs: []string{"o1"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	require.NoError(t, h.processAddMembers(ctx, data))
}

// Backfill failure must fail-ha. JetStream redelivery is
// safe because subs were written but the org row isn't until
// BulkCreateRoomMembers, so hadOrgsBefore stays false on retry.
func TestHandler_ProcessAddMembers_BackfillSubscriptionAccountsErrorFailsHard(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string{"o1"}, []string(nil), roomID).
		Return([]AddMemberCandidate{{Account: "u_new"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u_new"}).
		Return([]model.User{{ID: "u_new", Account: "u_new", SiteID: "site-a", EngName: "New", ChineseName: "新"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return(nil, fmt.Errorf("transient mongo error"))

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore: testKeyStore, keySender: testKeySender}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Orgs: []string{"o1"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	err := h.processAddMembers(ctx, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get subscription accounts for backfill")
}

func TestHandler_ProcessAddMembers_BackfillFindUsersErrorFailsHard(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string{"o1"}, []string(nil), roomID).
		Return([]AddMemberCandidate{{Account: "u_new"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u_new"}).
		Return([]model.User{{ID: "u_new", Account: "u_new", SiteID: "site-a", EngName: "New", ChineseName: "新"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return([]string{"existing_user"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"existing_user"}).Return(nil, fmt.Errorf("transient mongo error"))

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore: testKeyStore, keySender: testKeySender}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Orgs: []string{"o1"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	err := h.processAddMembers(ctx, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "find users for backfill")
}

func TestHandler_ProcessAddMembers_BackfillSkippedWhenRoomAlreadyHasOrgs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string{"o_new"}, []string(nil), roomID).
		Return([]AddMemberCandidate{{Account: "u_new"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u_new"}).
		Return([]model.User{{ID: "u_new", Account: "u_new", SiteID: "site-a", EngName: "New", ChineseName: "新"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	// Restructured code calls HasOrgRoomMembers unconditionally.
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(true, nil)

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)
	// NO GetSubscriptionAccounts expectation — backfill must be skipped.

	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore: testKeyStore, keySender: testKeySender}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Orgs: []string{"o_new"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	require.NoError(t, h.processAddMembers(ctx, data))
}

// Task 3 (spec §2.1): a user only gets an individual room_members doc iff
// their account is in req.Users. Org-only expansions must NOT emit indiv
// docs for accounts pulled in via org expansion.

// A1: Users=[u1], Orgs=[o1] (o1 has [u1, u2]). Expect indiv only for u1, org for o1.
func TestHandler_ProcessAddMembers_IndividualFilter_DirectAndOrgOverlap(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string{"o1"}, []string{"u1"}, roomID).
		Return([]AddMemberCandidate{{Account: "u1"}, {Account: "u2"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1", "u2"}).
		Return([]model.User{
			{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"},
			{ID: "u2_id", Account: "u2", SiteID: "site-a", EngName: "U2", ChineseName: "二"},
		}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return([]string{}, nil) // no pre-existing subs

	var captured []*model.RoomMember
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, m []*model.RoomMember) error {
			captured = m
			return nil
		})
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore: testKeyStore, keySender: testKeySender}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Users: []string{"u1"}, Orgs: []string{"o1"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	require.NoError(t, h.processAddMembers(ctx, data))

	var indivAccts []string
	var orgIDs []string
	for _, m := range captured {
		switch m.Member.Type {
		case model.RoomMemberIndividual:
			indivAccts = append(indivAccts, m.Member.Account)
		case model.RoomMemberOrg:
			orgIDs = append(orgIDs, m.Member.ID)
		}
	}
	assert.ElementsMatch(t, []string{"u1"}, indivAccts, "indiv docs limited to req.Users")
	assert.ElementsMatch(t, []string{"o1"}, orgIDs)
}

// A2: Users=[], Orgs=[o1]. Expect org only, no indivs.
func TestHandler_ProcessAddMembers_IndividualFilter_OrgOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string{"o1"}, []string(nil), roomID).
		Return([]AddMemberCandidate{{Account: "u1"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).
		Return([]model.User{{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return([]string{}, nil)

	var captured []*model.RoomMember
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, m []*model.RoomMember) error {
			captured = m
			return nil
		})
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore: testKeyStore, keySender: testKeySender}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Orgs: []string{"o1"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	require.NoError(t, h.processAddMembers(ctx, data))

	for _, m := range captured {
		assert.NotEqual(t, model.RoomMemberIndividual, m.Member.Type, "no indiv docs should be written")
	}
}

// A4: Create channel ResolvedUsers=[u1], ResolvedOrgs=[o1] (o1 has [u1, u2]),
// requester r. Expect indiv docs for r and u1, org doc for o1, no indiv for u2.
func TestHandler_ProcessCreateRoom_Channel_IndividualFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	requester := &model.User{ID: "r_id", Account: "r", SiteID: "site-a", EngName: "Req", ChineseName: "請"}

	store.EXPECT().ListNewMembersForNewRoom(gomock.Any(), []string{"o1"}, []string{"u1"}, "r").
		Return([]string{"u1", "u2"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1", "u2"}).
		Return([]model.User{
			{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"},
			{ID: "u2_id", Account: "u2", SiteID: "site-a", EngName: "U2", ChineseName: "二"},
		}, nil)

	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)

	var captured []*model.RoomMember
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, m []*model.RoomMember) error {
			captured = m
			return nil
		})
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore: testKeyStore, keySender: testKeySender}

	room := &model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}
	req := &model.CreateRoomRequest{
		RoomID:        roomID,
		ResolvedUsers: []string{"u1"},
		ResolvedOrgs:  []string{"o1"},
		Timestamp:     1,
	}
	pair, err := testKeyStore.Get(context.Background(), roomID)
	require.NoError(t, err)
	require.NoError(t, h.processCreateRoomChannel(context.Background(), req, room, requester, pair, "req-1", time.UnixMilli(1).UTC(), time.UnixMilli(2).UTC()))

	var indivAccts []string
	var orgIDs []string
	for _, m := range captured {
		switch m.Member.Type {
		case model.RoomMemberIndividual:
			indivAccts = append(indivAccts, m.Member.Account)
		case model.RoomMemberOrg:
			orgIDs = append(orgIDs, m.Member.ID)
		}
	}
	assert.ElementsMatch(t, []string{"r", "u1"}, indivAccts, "indiv docs limited to ResolvedUsers ∪ {requester}")
	assert.ElementsMatch(t, []string{"o1"}, orgIDs)
}

// D1: requester not found → permanent error.
func TestHandler_ProcessAddMembers_RequesterNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string(nil), []string{"u1"}, roomID).
		Return([]AddMemberCandidate{{Account: "u1"}}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).
		Return([]model.User{{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "missing-requester").Return(nil, ErrUserNotFound)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}
	req := model.AddMembersRequest{RoomID: roomID, RequesterID: "missing-id", RequesterAccount: "missing-requester", Users: []string{"u1"}, Timestamp: 1}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	err := h.processAddMembers(ctx, data)
	require.Error(t, err)
	assert.ErrorIs(t, err, errPermanent)
	assert.Contains(t, err.Error(), "missing-requester")

	responses := userResponseFor(published, "missing-requester")
	require.NotEmpty(t, responses, "permanent error must publish async-job error event")
	var result model.AsyncJobResult
	require.NoError(t, json.Unmarshal(responses[0].data, &result))
	assert.Equal(t, model.AsyncJobStatusError, result.Status)
	assert.Contains(t, result.Error, "missing-requester")
	assert.NotContains(t, result.Error, ": permanent")
}

// findSysMsg locates the system message published on MsgCanonicalCreated for
// the given site with the requested Type. Fails the test if no such publish
// occurred.
func findSysMsg(t *testing.T, published []publishedMsg, siteID, msgType string) model.Message {
	t.Helper()
	want := subject.MsgCanonicalCreated(siteID)
	for _, p := range published {
		if p.subj != want {
			continue
		}
		var evt model.MessageEvent
		if err := json.Unmarshal(p.data, &evt); err != nil {
			t.Fatalf("unmarshal MessageEvent: %v", err)
		}
		if evt.Message.Type == msgType {
			return evt.Message
		}
	}
	t.Fatalf("no %s sys-message published on %s", msgType, siteID)
	return model.Message{}
}

// B1: len(subs)==1 → single form.
func TestHandler_ProcessAddMembers_Content_Single(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string(nil), []string{"u1"}, roomID).
		Return([]AddMemberCandidate{{Account: "u1"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).
		Return([]model.User{{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	// No BulkCreateRoomMembers expected (no orgs, no pre-existing orgs → lite-mode add).

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Users: []string{"u1"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	require.NoError(t, h.processAddMembers(ctx, data))

	sysMsg := findSysMsg(t, published, "site-a", "members_added")
	assert.Equal(t, `"Alice 愛" added "U1 一" to the channel`, sysMsg.Content)
}

// B2: len(subs)>=2 → multi form.
func TestHandler_ProcessAddMembers_Content_Multi(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string(nil), []string{"u1", "u2"}, roomID).
		Return([]AddMemberCandidate{{Account: "u1"}, {Account: "u2"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1", "u2"}).
		Return([]model.User{
			{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"},
			{ID: "u2_id", Account: "u2", SiteID: "site-a", EngName: "U2", ChineseName: "二"},
		}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Users: []string{"u1", "u2"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	require.NoError(t, h.processAddMembers(ctx, data))

	sysMsg := findSysMsg(t, published, "site-a", "members_added")
	assert.Equal(t, `"Alice 愛" added members to the channel`, sysMsg.Content)
}

// B3: create-room channel publishes members_added with always-multi form.
func TestHandler_PublishChannelSysMessages_MembersAddedContent(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	room := &model.Room{ID: "r1", Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}
	requester := &model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}
	req := &model.CreateRoomRequest{RoomID: "r1", Users: []string{"u1", "u2"}}

	require.NoError(t, h.publishChannelSysMessages(context.Background(), req, room, requester, 2, "req-1", time.UnixMilli(1).UTC()))

	sysMsg := findSysMsg(t, published, "site-a", model.MessageTypeMembersAdded)
	assert.Equal(t, `"Alice 愛" added members to the channel`, sysMsg.Content)
}

// C1: self-leave full removal → member_left with sender + Content.
func TestHandler_ProcessRemoveIndividual_SelfLeave_Content(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetUserWithMembership(gomock.Any(), roomID, "bob").
		Return(&UserWithMembership{
			User:             model.User{ID: "u_b", Account: "bob", SiteID: "site-a", EngName: "Bob", ChineseName: "鮑"},
			HasOrgMembership: false,
			Roles:            []model.Role{model.RoleMember},
		}, nil)
	store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u_b").Return(nil)
	store.EXPECT().DeleteSubscription(gomock.Any(), roomID, "bob").Return(int64(1), nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return([]string{}, nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: "bob", Account: "bob", Timestamp: 1}
	require.NoError(t, h.processRemoveIndividual(context.Background(), &req, nil))

	sysMsg := findSysMsg(t, published, "site-a", "member_left")
	assert.Equal(t, "bob", sysMsg.UserAccount)
	assert.Equal(t, "u_b", sysMsg.UserID, "self-leave reuses leaving-user's ID as sender")
	assert.Equal(t, `"Bob 鮑" left the channel`, sysMsg.Content)
}

// C2: removed-by-other full removal → member_removed with sender + Content.
func TestHandler_ProcessRemoveIndividual_RemovedByOther_Content(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetUserWithMembership(gomock.Any(), roomID, "bob").
		Return(&UserWithMembership{
			User: model.User{ID: "u_b", Account: "bob", SiteID: "site-a", EngName: "Bob", ChineseName: "鮑"},
		}, nil)
	store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u_b").Return(nil)
	store.EXPECT().DeleteSubscription(gomock.Any(), roomID, "bob").Return(int64(1), nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return([]string{}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: "alice", Account: "bob", Timestamp: 1}
	require.NoError(t, h.processRemoveIndividual(context.Background(), &req, nil))

	sysMsg := findSysMsg(t, published, "site-a", "member_removed")
	assert.Equal(t, "alice", sysMsg.UserAccount)
	assert.Equal(t, "u_a", sysMsg.UserID, "forced removal sets sender to requester")
	assert.Equal(t, `"Bob 鮑" has been removed from the channel`, sysMsg.Content)
}

// C3: org remove with every member also having individual subs (toRemove empty)
// — SectName still populated from unfiltered members; sys-message still published.
func TestHandler_ProcessRemoveOrg_AllOverlap_SectNameFromUnfiltered(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetOrgMembersWithIndividualStatus(gomock.Any(), roomID, "o1").
		Return([]OrgMemberStatus{
			{Account: "u1", SiteID: "site-a", Name: "Engineering", HasIndividualMembership: true},
			{Account: "u2", SiteID: "site-a", Name: "Engineering", HasIndividualMembership: true},
		}, nil)
	// toRemove is empty → no DeleteSubscriptionsByAccounts call expected.
	store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberOrg, "o1").Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: "alice", OrgID: "o1", Timestamp: 1}
	require.NoError(t, h.processRemoveOrg(context.Background(), &req, nil))

	sysMsg := findSysMsg(t, published, "site-a", "member_removed")
	assert.Equal(t, "alice", sysMsg.UserAccount)
	assert.Equal(t, "u_a", sysMsg.UserID, "org removal sets sender to requester")
	assert.Equal(t, `"Engineering" has been removed from the channel`, sysMsg.Content)
}

// D5: every member SectName empty → no permanent error. The orgID fallback
// in displayOrg/CombineWithFallback guarantees a non-empty rendered string,
// so the org-doc deletion and sys-message still go through. A slog.Warn is
// emitted for visibility but the operation succeeds.
func TestHandler_ProcessRemoveOrg_AllSectNamesEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetOrgMembersWithIndividualStatus(gomock.Any(), roomID, "o1").
		Return([]OrgMemberStatus{
			{Account: "u1", SiteID: "site-a", Name: "", TCName: "", IsDept: false, HasIndividualMembership: true},
		}, nil)
	// toRemove is empty (the member has individual membership) → no DeleteSubscriptionsByAccounts expected.
	store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberOrg, "o1").Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}
	req := model.RemoveMemberRequest{RoomID: roomID, Requester: "alice", OrgID: "o1", Timestamp: 1}
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	require.NoError(t, h.processRemoveOrg(ctx, &req, nil))

	sysMsg := findSysMsg(t, published, "site-a", "member_removed")
	assert.Equal(t, `"o1" has been removed from the channel`, sysMsg.Content)
	var payload model.MemberRemoved
	require.NoError(t, json.Unmarshal(sysMsg.SysMsgData, &payload))
	assert.Equal(t, "o1", payload.SectName)
}

// Multi-org overlap: when the user being removed is still covered by ANOTHER
// org row in the same room (HasOtherOrgMembership == true), their subscription
// must NOT be deleted. Reachable because this PR's dept-aware matching lets a
// single user be the union of two org rows (one matching their sectId, one
// matching their deptId).
func TestHandler_ProcessRemoveOrg_OtherOrgCovers_PreservesSub(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	// alice has no individual row, but is still covered by another org row.
	store.EXPECT().GetOrgMembersWithIndividualStatus(gomock.Any(), roomID, "X").
		Return([]OrgMemberStatus{
			{
				Account: "alice", SiteID: "site-a",
				Name: "Eng Sect", TCName: "工程組",
				IsDept:                  false,
				HasIndividualMembership: false,
				HasOtherOrgMembership:   true,
			},
		}, nil)
	// MUST NOT be called — alice is still covered by the sibling org.
	store.EXPECT().DeleteSubscriptionsByAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	// MUST NOT rotate — no survivors were displaced.
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), gomock.Any()).Times(0)
	// The X org row still gets deleted; the count gets reconciled.
	store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberOrg, "X").Return(nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
	store.EXPECT().GetUser(gomock.Any(), "alice-req").
		Return(&model.User{ID: "u_r", Account: "alice-req", SiteID: "site-a", EngName: "Req", ChineseName: "求"}, nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}
	req := model.RemoveMemberRequest{RoomID: roomID, Requester: "alice-req", OrgID: "X", Timestamp: 1}
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	require.NoError(t, h.processRemoveOrg(ctx, &req, nil))

	// Sys-msg is still published — the org WAS removed — but RemovedUsersCount must be 0.
	sysMsg := findSysMsg(t, published, "site-a", "member_removed")
	var payload model.MemberRemoved
	require.NoError(t, json.Unmarshal(sysMsg.SysMsgData, &payload))
	assert.Equal(t, 0, payload.RemovedUsersCount, "no users were actually removed; siblings still cover them")
}

// F1: async DM create sets UIDs/Accounts sorted by UID, paired by index, on
// the initial CreateRoom insert (single Mongo write, no follow-up update).
func TestProcessCreateRoom_DM_SetsParticipantFields(t *testing.T) {
	h, mockStore, _ := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_zzz", Account: "alice", EngName: "Alice", ChineseName: "愛", SiteID: "site-A"}
	other := &model.User{ID: "u_aaa", Account: "bob", EngName: "Bob", ChineseName: "鮑", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "bob").Return(other, nil)

	var captured *model.Room
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *model.Room) error {
			captured = r
			return nil
		})
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().FindDMSubscriptionPair(gomock.Any(), "room-dm-fields", "alice").Return(
		&model.Subscription{User: model.SubscriptionUser{ID: "u_zzz", Account: "alice"}},
		&model.Subscription{User: model.SubscriptionUser{ID: "u_aaa", Account: "bob"}},
		nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-dm-fields").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID:           "room-dm-fields",
		RequesterAccount: "alice",
		Users:            []string{"bob"},
		Timestamp:        time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	require.NotNil(t, captured)
	// UIDs sorted: ["u_aaa","u_zzz"]; Accounts mirror that permutation.
	assert.Equal(t, []string{"u_aaa", "u_zzz"}, captured.UIDs)
	assert.Equal(t, []string{"bob", "alice"}, captured.Accounts, "accounts paired with uid sort order")
}

// F2: async botDM create persists room with UIDs/Accounts paired by index on
// the initial CreateRoom insert (single Mongo write, no follow-up update).
func TestProcessCreateRoom_BotDM_SetsParticipantFields(t *testing.T) {
	h, mockStore, _ := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_zzz", Account: "alice", EngName: "Alice", ChineseName: "愛", SiteID: "site-A"}
	bot := &model.User{ID: "u_aaa", Account: "supportbot.bot", EngName: "Support", ChineseName: "支援", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)
	mockStore.EXPECT().GetUser(gomock.Any(), "supportbot.bot").Return(bot, nil)

	var captured *model.Room
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *model.Room) error {
			captured = r
			return nil
		})
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").Return(
		&model.Subscription{User: model.SubscriptionUser{ID: "u_zzz", Account: "alice"}},
		&model.Subscription{User: model.SubscriptionUser{ID: "u_aaa", Account: "supportbot.bot"}},
		nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-botdm-fields").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID:           "room-botdm-fields",
		RequesterAccount: "alice",
		Users:            []string{"supportbot.bot"},
		Timestamp:        time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))

	require.NotNil(t, captured)
	assert.Equal(t, []string{"u_aaa", "u_zzz"}, captured.UIDs)
	assert.Equal(t, []string{"supportbot.bot", "alice"}, captured.Accounts)
}

// F3: sync DM create sets UIDs/Accounts on the initial CreateRoom literal.
func TestHandleSyncCreateDM_SetsParticipantFieldsOnInitialCreate(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	h := &Handler{store: mockStore, siteID: "site-A", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := model.User{ID: "u_zzz", Account: "alice", EngName: "Alice", ChineseName: "愛", SiteID: "site-A"}
	other := model.User{ID: "u_aaa", Account: "bob", EngName: "Bob", ChineseName: "鮑", SiteID: "site-A"}

	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).
		Return([]model.User{requester, other}, nil)

	var captured *model.Room
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *model.Room) error {
			captured = r
			return nil
		})
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().FindDMSubscriptionPair(gomock.Any(), gomock.Any(), "alice").Return(
		&model.Subscription{User: model.SubscriptionUser{ID: requester.ID, Account: requester.Account}},
		&model.Subscription{User: model.SubscriptionUser{ID: other.ID, Account: other.Account}},
		nil)

	reqBody, err := json.Marshal(model.SyncCreateDMRequest{
		RequesterAccount: "alice",
		OtherAccount:     "bob",
		RoomType:         model.RoomTypeDM,
	})
	require.NoError(t, err)

	_, err = h.handleSyncCreateDM(ctx, reqBody)
	require.NoError(t, err)
	require.NotNil(t, captured)
	assert.Equal(t, []string{"u_aaa", "u_zzz"}, captured.UIDs)
	assert.Equal(t, []string{"bob", "alice"}, captured.Accounts, "accounts paired with uid sort order")
}

// F4: channels must omit UIDs/Accounts; guard test pins the contract.
func TestProcessCreateRoom_Channel_DoesNotSetParticipantFields(t *testing.T) {
	h, mockStore, _ := newCreateRoomTestHandler(t)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	requester := &model.User{ID: "u_a", Account: "alice", EngName: "Alice", ChineseName: "愛", SiteID: "site-A"}
	bob := model.User{ID: "u_b", Account: "bob", EngName: "Bob", ChineseName: "鮑", SiteID: "site-A"}

	mockStore.EXPECT().GetUser(gomock.Any(), "alice").Return(requester, nil)

	var captured *model.Room
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *model.Room) error {
			captured = r
			return nil
		})
	mockStore.EXPECT().ListNewMembersForNewRoom(gomock.Any(), []string(nil), []string{"bob"}, "alice").
		Return([]string{"bob"}, nil)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{bob}, nil)
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	mockStore.EXPECT().ReconcileMemberCounts(gomock.Any(), "room-chan-fields").Return(nil)

	body := makeCreateRoomBody(t, &model.CreateRoomRequest{
		RoomID:           "room-chan-fields",
		RequesterAccount: "alice",
		Name:             "team-room",
		ResolvedUsers:    []string{"bob"},
		Timestamp:        time.Now().UnixMilli(),
	})
	require.NoError(t, h.processCreateRoom(ctx, body))
	require.NotNil(t, captured)
	assert.Nil(t, captured.UIDs, "channels must omit UIDs (omitempty drops nil)")
	assert.Nil(t, captured.Accounts, "channels must omit Accounts")
}

// F4: 1-member org expansion must still render multi-form Content.
func TestHandler_ProcessAddMembers_Content_OrgAddWithOneMember_UsesMulti(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	// req.Users is empty; org "eng" expands to one user "u1".
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string{"eng"}, []string(nil), roomID).
		Return([]AddMemberCandidate{{Account: "u1"}}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"u1"}).
		Return([]model.User{{ID: "u1_id", Account: "u1", SiteID: "site-a", EngName: "U1", ChineseName: "一"}}, nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).Return(false, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().GetSubscriptionAccounts(gomock.Any(), roomID).Return([]string{}, nil)
	store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)

	var published []publishedMsg
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}, keyStore: testKeyStore, keySender: testKeySender}

	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Orgs: []string{"eng"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)
	require.NoError(t, h.processAddMembers(ctx, data))

	sysMsg := findSysMsg(t, published, "site-a", "members_added")
	assert.Equal(t, `"Alice 愛" added members to the channel`, sysMsg.Content,
		"org-add must use multi form even when org expands to a single user")
}

// HasOrgRoomMembers error must surface as non-permanent so JetStream retries.
func TestHandler_ProcessAddMembers_HasOrgRoomMembersError_FailsClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	roomID := "r1"
	store.EXPECT().GetRoom(gomock.Any(), roomID).
		Return(&model.Room{ID: roomID, Name: "Chan", SiteID: "site-a", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), []string(nil), []string{"u1"}, roomID).
		Return([]AddMemberCandidate{{Account: "u1"}}, nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), roomID).
		Return(false, fmt.Errorf("transient mongo error"))
	// No FindUsersByAccounts / GetUser / BulkCreateSubscriptions / BulkCreateRoomMembers /
	// ReconcileMemberCounts — must short-circuit on the HasOrgRoomMembers error,
	// which is now checked immediately after ListAddMemberCandidates.

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, keyStore: testKeyStore, keySender: testKeySender}
	req := model.AddMembersRequest{
		RoomID: roomID, RequesterID: "u_a", RequesterAccount: "alice",
		Users: []string{"u1"}, Timestamp: 1,
	}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), testRequestID)

	err := h.processAddMembers(ctx, data)
	require.Error(t, err)
	assert.NotErrorIs(t, err, errPermanent, "Mongo errors must NOT be permanent — JetStream should retry")
	assert.Contains(t, err.Error(), "check existing org room members")
}

// natsServerCreateDM and the JetStream consume loop call this helper to
// validate the inbound X-Request-ID before any downstream dedup-key derivation
// runs. Missing/malformed → BadRequest (no server-side mint). The asymmetric
// policy vs the consume loop (which still mints defensively) lives in
// docs/error-handling.md §3a.
func TestRequireDedupRequestID(t *testing.T) {
	const validUUID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"

	t.Run("valid_passes", func(t *testing.T) {
		h := nats.Header{natsutil.RequestIDHeader: []string{validUUID}}
		ctx, id, err := requireDedupRequestID(context.Background(), h, "chat.test.subject")
		require.NoError(t, err)
		assert.Equal(t, validUUID, id)
		assert.Equal(t, validUUID, natsutil.RequestIDFromContext(ctx))
	})

	cases := []struct {
		name    string
		headers nats.Header
	}{
		{name: "nil_rejects", headers: nil},
		{name: "empty_rejects", headers: nats.Header{}},
		{name: "malformed_rejects", headers: nats.Header{natsutil.RequestIDHeader: []string{"not-a-uuid"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := requireDedupRequestID(context.Background(), tc.headers, "chat.test.subject")
			require.Error(t, err)
			var ec *errcode.Error
			require.True(t, errors.As(err, &ec))
			assert.Equal(t, errcode.CodeBadRequest, ec.Code)
		})
	}
}

// TestHandler_RotateAndFanOut_ErrNoCurrentKey_UsesPredictedVersion pins the
// contract that when Rotate returns ErrNoCurrentKey (Valkey lost the key between
// Get and Rotate), the fallback calls SetWithVersion at predictedVersion
// (currentPair.Version+1) rather than Set (which would stamp v0), preventing the
// version mismatch that would render the next encrypted message undecryptable.
func TestHandler_RotateAndFanOut_ErrNoCurrentKey_UsesPredictedVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockKeys := NewMockRoomKeyStore(ctrl)

	// currentPair simulates the key the handler fetched before calling rotateAndFanOut.
	currentPair := &roomkeystore.VersionedKeyPair{
		Version: 4,
		KeyPair: roomkeystore.RoomKeyPair{
			PrivateKey: bytes.Repeat([]byte{0xAA}, 32),
		},
	}
	// predictedVersion = currentPair.Version + 1 = 5

	// Step 1: Rotate fails with ErrNoCurrentKey — Valkey lost the current key
	// between Get (which returned currentPair) and Rotate.
	gomock.InOrder(
		mockKeys.EXPECT().
			Rotate(gomock.Any(), "test-room", gomock.Any()).
			Return(0, roomkeystore.ErrNoCurrentKey),
		// Step 2: fallback must write at predictedVersion=5, NOT at v0 via Set.
		// If the bug were present (Set called instead), gomock would raise
		// "unexpected call to Set" because Set is not expected here.
		mockKeys.EXPECT().
			SetWithVersion(gomock.Any(), "test-room", gomock.Any(), 5).
			Return(nil),
	)

	h := &Handler{
		store:     NewMockSubscriptionStore(ctrl),
		siteID:    "site-a",
		keyStore:  mockKeys,
		keySender: testKeySender,
		publish: func(_ context.Context, _ string, _ []byte, _ string) error {
			return nil
		},
	}

	// Call rotateAndFanOut directly — it is unexported but lives in package main,
	// so the test (same package) can call it without test infrastructure.
	// Pass an empty survivors slice: no fan-out side effects needed for this test.
	err := h.rotateAndFanOut(context.Background(), "test-room", currentPair, nil)
	require.NoError(t, err)
}

// Dept-first tiebreak: on overlap (org membership reachable via both sect and
// dept matches), the dept row wins the (Name, TCName) pick. When no dept row
// matches, the first sect row wins. When members carry no names at all, the
// orgID fallback in displayOrg keeps the sys-message non-empty (no permanent
// error).
func TestHandler_ProcessRemoveOrg_DeptFirstTiebreak(t *testing.T) {
	cases := []struct {
		name        string
		members     []OrgMemberStatus
		wantSect    string // MemberRemoved.SectName
		wantContent string // Message.Content
	}{
		{
			name: "all sect users",
			members: []OrgMemberStatus{
				{Account: "u1", SiteID: "site-a", Name: "Sect", TCName: "組", IsDept: false, HasIndividualMembership: true},
				{Account: "u2", SiteID: "site-a", Name: "Sect", TCName: "組", IsDept: false, HasIndividualMembership: true},
			},
			wantSect: "Sect 組", wantContent: `"Sect 組" has been removed from the channel`,
		},
		{
			name: "all dept users",
			members: []OrgMemberStatus{
				{Account: "u1", SiteID: "site-a", Name: "Dept", TCName: "部", IsDept: true, HasIndividualMembership: true},
			},
			wantSect: "Dept 部", wantContent: `"Dept 部" has been removed from the channel`,
		},
		{
			name: "mixed — dept wins",
			members: []OrgMemberStatus{
				{Account: "u1", SiteID: "site-a", Name: "Sect", TCName: "組", IsDept: false, HasIndividualMembership: true},
				{Account: "u2", SiteID: "site-a", Name: "Dept", TCName: "部", IsDept: true, HasIndividualMembership: true},
			},
			wantSect: "Dept 部", wantContent: `"Dept 部" has been removed from the channel`,
		},
		{
			name: "all names empty — fall back to orgID",
			members: []OrgMemberStatus{
				{Account: "u1", SiteID: "site-a", Name: "", TCName: "", IsDept: false, HasIndividualMembership: true},
			},
			wantSect: "o1", wantContent: `"o1" has been removed from the channel`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockSubscriptionStore(ctrl)

			roomID := "r1"
			store.EXPECT().GetOrgMembersWithIndividualStatus(gomock.Any(), roomID, "o1").Return(tc.members, nil)
			// toRemove is empty (all members have individual membership) → no DeleteSubscriptionsByAccounts expected.
			store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberOrg, "o1").Return(nil)
			store.EXPECT().ReconcileMemberCounts(gomock.Any(), roomID).Return(nil)
			store.EXPECT().GetUser(gomock.Any(), "alice").
				Return(&model.User{ID: "u_a", Account: "alice", SiteID: "site-a", EngName: "Alice", ChineseName: "愛"}, nil)

			var published []publishedMsg
			h := &Handler{
				store: store, siteID: "site-a",
				publish: func(_ context.Context, subj string, data []byte, _ string) error {
					published = append(published, publishedMsg{subj: subj, data: data})
					return nil
				},
				keyStore: testKeyStore, keySender: testKeySender,
			}

			req := model.RemoveMemberRequest{RoomID: roomID, Requester: "alice", OrgID: "o1", Timestamp: 1}
			require.NoError(t, h.processRemoveOrg(context.Background(), &req, nil))

			sysMsg := findSysMsg(t, published, "site-a", "member_removed")
			assert.Equal(t, tc.wantContent, sysMsg.Content)
			var payload model.MemberRemoved
			require.NoError(t, json.Unmarshal(sysMsg.SysMsgData, &payload))
			assert.Equal(t, tc.wantSect, payload.SectName)
		})
	}
}

// fakeDEKProvisioner records EnsureDEK calls and can be made to fail.
type fakeDEKProvisioner struct {
	calls []string
	err   error
}

func (f *fakeDEKProvisioner) EnsureDEK(_ context.Context, roomID string) error {
	f.calls = append(f.calls, roomID)
	return f.err
}

func TestHandleSyncCreateDM_SelfDM_ProvisionsDEK(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{
		{ID: "u_alice", Account: "alice", SiteID: "site-a"},
	}, nil)
	var createdRoomID string
	mockStore.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, room *model.Room) error { createdRoomID = room.ID; return nil })
	mockStore.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)

	prov := &fakeDEKProvisioner{}
	h := &Handler{
		store:          mockStore,
		siteID:         "site-a",
		dekProvisioner: prov,
		publish:        func(context.Context, string, []byte, string) error { return nil },
	}

	req := model.SyncCreateDMRequest{RequesterAccount: "alice", OtherAccount: "alice", RoomType: model.RoomTypeDM}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), "0193abcd-0193-7abc-89ab-0193abcd0011")

	reply, err := h.handleSyncCreateDM(ctx, data)
	require.NoError(t, err)
	require.True(t, reply.Success)
	require.Len(t, prov.calls, 1)
	assert.Equal(t, createdRoomID, prov.calls[0], "EnsureDEK must be called with the self-DM room ID")
}

func TestHandleSyncCreateDM_DEKFailure_AbortsBeforeCreate(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockSubscriptionStore(ctrl)
	mockStore.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{
		{ID: "u_alice", Account: "alice", SiteID: "site-a"},
		{ID: "u_bob", Account: "bob", SiteID: "site-a"},
	}, nil)
	// CreateRoom is intentionally NOT expected: a DEK provisioning failure must
	// abort before the room is persisted. gomock fails the test if it is called.

	prov := &fakeDEKProvisioner{err: errors.New("vault down")}
	h := &Handler{
		store:          mockStore,
		siteID:         "site-a",
		dekProvisioner: prov,
		publish:        func(context.Context, string, []byte, string) error { return nil },
	}

	req := model.SyncCreateDMRequest{RequesterAccount: "alice", OtherAccount: "bob", RoomType: model.RoomTypeDM}
	data, _ := json.Marshal(req)
	ctx := natsutil.WithRequestID(context.Background(), "0193abcd-0193-7abc-89ab-0193abcd0011")

	_, err := h.handleSyncCreateDM(ctx, data)
	require.Error(t, err)
	assert.Len(t, prov.calls, 1, "EnsureDEK should have been attempted once")
}

// ---- HandleJetStreamMsg Ack/Nak + async-consumer panic recovery ----

// fakeJSMsg is a minimal jetstream.Msg stub recording Ack/Nak calls so tests
// can assert the consumer's permanence-driven decision without a NATS server.
type fakeJSMsg struct {
	subject string
	data    []byte
	headers nats.Header
	acked   bool
	naked   bool
	ackErr  error
	nakErr  error
}

func (m *fakeJSMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *fakeJSMsg) Data() []byte                              { return m.data }
func (m *fakeJSMsg) Headers() nats.Header                      { return m.headers }
func (m *fakeJSMsg) Subject() string                           { return m.subject }
func (m *fakeJSMsg) Reply() string                             { return "" }
func (m *fakeJSMsg) Ack() error                                { m.acked = true; return m.ackErr }
func (m *fakeJSMsg) DoubleAck(context.Context) error           { m.acked = true; return m.ackErr }
func (m *fakeJSMsg) Nak() error                                { m.naked = true; return m.nakErr }
func (m *fakeJSMsg) NakWithDelay(time.Duration) error          { m.naked = true; return m.nakErr }
func (m *fakeJSMsg) InProgress() error                         { return nil }
func (m *fakeJSMsg) Term() error                               { return nil }
func (m *fakeJSMsg) TermWithReason(string) error               { return nil }

// processRoleUpdate's "unsupported role" path was historically Nak'd forever
// (a bare fmt.Errorf). It is now an explicit permanent error so the consumer
// Acks and JetStream stops redelivering.
func TestHandleJetStreamMsg_UnsupportedRole_Acks(t *testing.T) {
	h, _, _ := newCreateRoomTestHandler(t)
	body := marshalReq(t, model.UpdateRoleRequest{
		Account: "alice", RoomID: "room-1", NewRole: model.Role("bogus"),
	})
	msg := &fakeJSMsg{subject: "chat.room.room-1.member.role-update", data: body}
	h.HandleJetStreamMsg(context.Background(), msg)
	assert.True(t, msg.acked, "unsupported role must Ack (permanent), not Nak forever")
	assert.False(t, msg.naked)
}

// A transient store error on a role update must Nak (retryable), confirming the
// Ack/Nak decision is keyed on the explicit permanent marker, not the category.
func TestHandleJetStreamMsg_TransientRoleUpdate_Naks(t *testing.T) {
	h, mockStore, _ := newCreateRoomTestHandler(t)
	mockStore.EXPECT().AddRole(gomock.Any(), "alice", "room-1", model.RoleOwner).
		Return(errors.New("mongo: connection reset"))
	body := marshalReq(t, model.UpdateRoleRequest{
		Account: "alice", RoomID: "room-1", NewRole: model.RoleOwner,
	})
	msg := &fakeJSMsg{subject: "chat.room.room-1.member.role-update", data: body}
	h.HandleJetStreamMsg(context.Background(), msg)
	assert.True(t, msg.naked, "transient infra error must Nak for redelivery")
	assert.False(t, msg.acked)
}

// panicProcessor panics on every message — stands in for a WithCause/WithMetadata
// misuse that would otherwise crash the async consumer goroutine.
type panicProcessor struct{}

func (panicProcessor) HandleJetStreamMsg(context.Context, jetstream.Msg) {
	panic("boom: errcode option misuse")
}

func TestRunJobWithRecovery_PanicAcksAndDoesNotCrash(t *testing.T) {
	msg := &fakeJSMsg{subject: "chat.room.room-1.create"}
	assert.NotPanics(t, func() {
		runJobWithRecovery(context.Background(), panicProcessor{}, msg)
	}, "a panicking handler must be recovered, not crash the worker")
	assert.True(t, msg.acked, "panic must Ack (poison-pill drop), not Nak — a deterministic panic would otherwise loop on redelivery")
	assert.False(t, msg.naked)
}

func TestFindRemoteSitesForAccounts(t *testing.T) {
	t.Run("dedupes remote, drops local, preserves siteIDs", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockSubscriptionStore(ctrl)
		store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice", "bob", "carol", "dave"}).Return([]model.User{
			{Account: "alice", SiteID: "site-a"}, // local
			{Account: "bob", SiteID: "site-b"},   // remote
			{Account: "carol", SiteID: "site-c"}, // remote
			{Account: "dave", SiteID: "site-b"},  // dup of bob's site
		}, nil)
		h := &Handler{store: store, siteID: "site-a"}
		got, err := h.findRemoteSitesForAccounts(context.Background(), []string{"alice", "bob", "carol", "dave"})
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"site-b", "site-c"}, got)
	})

	t.Run("empty input returns empty slice", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockSubscriptionStore(ctrl)
		h := &Handler{store: store, siteID: "site-a"}
		got, err := h.findRemoteSitesForAccounts(context.Background(), nil)
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}

func TestFindRemoteSitesForAccounts_StoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, errors.New("mongo timeout"))

	h := &Handler{store: store, siteID: "site-a"}
	got, err := h.findRemoteSitesForAccounts(context.Background(), []string{"alice"})
	require.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "find users by accounts")
}

// --- processRoomRename tests ---

// Test 1: Missing X-Request-ID → permanent error, no store calls.
func TestProcessRoomRename_MissingRequestID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := NewMockSubscriptionStore(ctrl)
	// No EXPECT calls — store must not be called.

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error {
		return nil
	}}
	body, _ := json.Marshal(model.RenameRoomRequest{RoomID: "r1", NewName: "renamed", Account: "alice", Timestamp: 1700000000000})
	err := h.processRoomRename(context.Background(), body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errPermanent), "expected permanent error, got %v", err)
}

// Test 2: Invalid UUID → permanent error.
func TestProcessRoomRename_InvalidUUID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := NewMockSubscriptionStore(ctrl)

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error {
		return nil
	}}
	ctx := natsutil.WithRequestID(context.Background(), "not-a-valid-uuid")
	body, _ := json.Marshal(model.RenameRoomRequest{RoomID: "r1", NewName: "renamed", Account: "alice", Timestamp: 1700000000000})
	err := h.processRoomRename(ctx, body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errPermanent), "expected permanent error, got %v", err)
}

// Test 3: Unmarshal failure → permanent + AsyncJobResult does NOT publish (empty requesterAccount short-circuits).
func TestProcessRoomRename_UnmarshalFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := NewMockSubscriptionStore(ctrl)

	requestID := "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	var publishedSubjects []string
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, subj string, _ []byte, _ string) error {
		publishedSubjects = append(publishedSubjects, subj)
		return nil
	}}
	ctx := natsutil.WithRequestID(context.Background(), requestID)
	err := h.processRoomRename(ctx, []byte("not-valid-json"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, errPermanent), "expected permanent error, got %v", err)
	// requesterAccount is empty after unmarshal failure, so publishAsyncJobResult must not publish.
	for _, s := range publishedSubjects {
		assert.False(t,
			strings.HasPrefix(s, "chat.user.") && strings.Contains(s, ".response."+requestID),
			"should not publish AsyncJobResult on unmarshal failure: got %s", s)
	}
}

// Test 4: UpdateRoomName returns ErrRoomNotFound → permanent + AsyncJobResult error.
func TestProcessRoomRename_RoomNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := NewMockSubscriptionStore(ctrl)

	requestID := "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	store.EXPECT().UpdateRoomName(gomock.Any(), "r1", "renamed").Return(ErrRoomNotFound)

	var asyncResults []model.AsyncJobResult
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		if subj == subject.UserResponse("alice", requestID) {
			var r model.AsyncJobResult
			require.NoError(t, json.Unmarshal(data, &r))
			asyncResults = append(asyncResults, r)
		}
		return nil
	}

	h := &Handler{store: store, siteID: "site-a", publish: publish}
	ctx := natsutil.WithRequestID(context.Background(), requestID)
	body, _ := json.Marshal(model.RenameRoomRequest{RoomID: "r1", NewName: "renamed", Account: "alice", Timestamp: 1700000000000})
	err := h.processRoomRename(ctx, body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errPermanent), "expected permanent error, got %v", err)
	require.Len(t, asyncResults, 1)
	assert.Equal(t, model.AsyncJobStatusError, asyncResults[0].Status)
}

// Test 5: UpdateRoomName returns ErrNotChannelRoom → permanent + AsyncJobResult error.
func TestProcessRoomRename_NotChannelRoom(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := NewMockSubscriptionStore(ctrl)

	requestID := "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	store.EXPECT().UpdateRoomName(gomock.Any(), "r1", "renamed").Return(ErrNotChannelRoom)

	var asyncResults []model.AsyncJobResult
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		if subj == subject.UserResponse("alice", requestID) {
			var r model.AsyncJobResult
			require.NoError(t, json.Unmarshal(data, &r))
			asyncResults = append(asyncResults, r)
		}
		return nil
	}

	h := &Handler{store: store, siteID: "site-a", publish: publish}
	ctx := natsutil.WithRequestID(context.Background(), requestID)
	body, _ := json.Marshal(model.RenameRoomRequest{RoomID: "r1", NewName: "renamed", Account: "alice", Timestamp: 1700000000000})
	err := h.processRoomRename(ctx, body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errPermanent), "expected permanent error, got %v", err)
	require.Len(t, asyncResults, 1)
	assert.Equal(t, model.AsyncJobStatusError, asyncResults[0].Status)
}

// Test 6: Transient error on UpdateSubscriptionNamesForRoom → non-permanent error returned.
func TestProcessRoomRename_TransientSubscriptionUpdateError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := NewMockSubscriptionStore(ctrl)

	requestID := "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	store.EXPECT().UpdateRoomName(gomock.Any(), "r1", "renamed").Return(nil)
	store.EXPECT().UpdateSubscriptionNamesForRoom(gomock.Any(), "r1", "renamed").Return(errors.New("mongo timeout"))

	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error {
		return nil
	}}
	ctx := natsutil.WithRequestID(context.Background(), requestID)
	body, _ := json.Marshal(model.RenameRoomRequest{RoomID: "r1", NewName: "renamed", Account: "alice", Timestamp: 1700000000000})
	err := h.processRoomRename(ctx, body)
	require.Error(t, err)
	assert.False(t, errors.Is(err, errPermanent), "expected transient (non-permanent) error, got %v", err)
}

// Test 7: Happy path no remote sites.
func TestProcessRoomRename_HappyPathNoRemoteSites(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := NewMockSubscriptionStore(ctrl)

	const roomID, newName = "r1", "renamed"
	requestID := "01970a4f-8c2d-7c9a-abcd-e0123456789f"

	subs := []model.Subscription{
		{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: roomID},
		{ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: roomID},
	}

	store.EXPECT().UpdateRoomName(gomock.Any(), roomID, newName).Return(nil)
	store.EXPECT().UpdateSubscriptionNamesForRoom(gomock.Any(), roomID, newName).Return(nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	store.EXPECT().ListByRoom(gomock.Any(), roomID).Return(subs, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{
		{Account: "alice", SiteID: "site-a"}, {Account: "bob", SiteID: "site-a"},
	}, nil)

	var publishedSubjects []string
	publish := func(_ context.Context, subj string, _ []byte, _ string) error {
		publishedSubjects = append(publishedSubjects, subj)
		return nil
	}

	h := &Handler{store: store, siteID: "site-a", publish: publish}
	ctx := natsutil.WithRequestID(context.Background(), requestID)
	body, _ := json.Marshal(model.RenameRoomRequest{
		RoomID: roomID, NewName: newName, Account: "alice", Timestamp: time.Now().UTC().UnixMilli(),
	})

	require.NoError(t, h.processRoomRename(ctx, body))

	assert.Contains(t, publishedSubjects, subject.MsgCanonicalCreated("site-a"))
	assert.Contains(t, publishedSubjects, subject.UserResponse("alice", requestID))
	for _, subj := range publishedSubjects {
		assert.NotContains(t, subj, "outbox.", "should not publish to outbox when all members are local")
		assert.NotContains(t, subj, ".event.subscription.update", "rename publishes a single room-scoped sys message; no per-subscription fan-out")
	}
}

// Test 8: Happy path with one remote site.
func TestProcessRoomRename_HappyPathWithRemoteSite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := NewMockSubscriptionStore(ctrl)

	const roomID, newName = "r1", "renamed"
	requestID := "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	ts := int64(1700000000000)

	subs := []model.Subscription{
		{ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: roomID},
		{ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: roomID},
	}

	store.EXPECT().UpdateRoomName(gomock.Any(), roomID, newName).Return(nil)
	store.EXPECT().UpdateSubscriptionNamesForRoom(gomock.Any(), roomID, newName).Return(nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	store.EXPECT().ListByRoom(gomock.Any(), roomID).Return(subs, nil)
	// Bob is on a remote site.
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{
		{Account: "alice", SiteID: "site-a"},
		{Account: "bob", SiteID: "site-b"},
	}, nil)

	var publishedSubjects []string
	var outboxPayloads []model.RoomRenamedOutboxPayload
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		publishedSubjects = append(publishedSubjects, subj)
		if strings.Contains(subj, "outbox.") {
			var env model.OutboxEvent
			require.NoError(t, json.Unmarshal(data, &env))
			var payload model.RoomRenamedOutboxPayload
			require.NoError(t, json.Unmarshal(env.Payload, &payload))
			outboxPayloads = append(outboxPayloads, payload)
		}
		return nil
	}

	h := &Handler{store: store, siteID: "site-a", publish: publish}
	ctx := natsutil.WithRequestID(context.Background(), requestID)
	body, _ := json.Marshal(model.RenameRoomRequest{
		RoomID: roomID, NewName: newName, Account: "alice", Timestamp: ts,
	})

	require.NoError(t, h.processRoomRename(ctx, body))

	assert.Contains(t, publishedSubjects, subject.MsgCanonicalCreated("site-a"))
	assert.Contains(t, publishedSubjects, subject.UserResponse("alice", requestID))
	for _, subj := range publishedSubjects {
		assert.NotContains(t, subj, ".event.subscription.update", "rename publishes a single room-scoped sys message; no per-subscription fan-out")
	}

	// Exactly one outbox publish to site-b.
	outboxSubjects := make([]string, 0)
	for _, s := range publishedSubjects {
		if strings.Contains(s, "outbox.") {
			outboxSubjects = append(outboxSubjects, s)
		}
	}
	require.Len(t, outboxSubjects, 1)
	assert.Contains(t, outboxSubjects[0], "site-b")
	require.Len(t, outboxPayloads, 1)
	assert.Equal(t, roomID, outboxPayloads[0].RoomID)
	assert.Equal(t, newName, outboxPayloads[0].NewName)
	assert.Equal(t, ts, outboxPayloads[0].Timestamp)
}

// Test 9: Error-then-ok retry sequence.
func TestProcessRoomRename_ErrorThenOkRetrySequence(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := NewMockSubscriptionStore(ctrl)
	requestID := "01970a4f-8c2d-7c9a-abcd-e0123456789f"

	store.EXPECT().UpdateRoomName(gomock.Any(), "r1", "x").Return(errors.New("mongo timeout"))
	store.EXPECT().UpdateRoomName(gomock.Any(), "r1", "x").Return(nil)
	store.EXPECT().UpdateSubscriptionNamesForRoom(gomock.Any(), "r1", "x").Return(nil)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
	// Empty subs → accounts is empty → findRemoteSitesForAccounts short-circuits (no FindUsersByAccounts call).
	store.EXPECT().ListByRoom(gomock.Any(), "r1").Return([]model.Subscription{}, nil)

	var asyncResults []model.AsyncJobResult
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		if subj == subject.UserResponse("alice", requestID) {
			var r model.AsyncJobResult
			require.NoError(t, json.Unmarshal(data, &r))
			asyncResults = append(asyncResults, r)
		}
		return nil
	}

	h := &Handler{store: store, siteID: "site-a", publish: publish}
	ctx := natsutil.WithRequestID(context.Background(), requestID)
	body, _ := json.Marshal(model.RenameRoomRequest{RoomID: "r1", NewName: "x", Account: "alice", Timestamp: 1700000000000})

	err := h.processRoomRename(ctx, body)
	require.Error(t, err)
	assert.False(t, errors.Is(err, errPermanent))

	require.NoError(t, h.processRoomRename(ctx, body))

	require.Len(t, asyncResults, 2)
	assert.Equal(t, model.AsyncJobStatusError, asyncResults[0].Status)
	assert.Equal(t, model.AsyncJobStatusOK, asyncResults[1].Status)
}
