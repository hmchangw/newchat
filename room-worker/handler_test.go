package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
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
	}}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner}
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
	}}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleMember}
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
	}}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner}
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

func TestHandler_ProcessRoleUpdate_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", publish: func(_ context.Context, _ string, _ []byte, _ string) error {
		t.Fatal("publish should not be called")
		return nil
	}}

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
	}}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner}
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
	}}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleMember}
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
	}}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner}
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

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: model.RoleOwner}
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
	}}

	req := model.UpdateRoleRequest{RoomID: "r1", Account: "bob", NewRole: "admin"}
	data, _ := json.Marshal(req)
	err := h.processRoleUpdate(context.Background(), data)
	if err == nil {
		t.Fatal("expected error for unsupported role")
	}
}

// --- processRemoveMember tests ---

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
		ReconcileUserCount(gomock.Any(), roomID).Return(nil)

	var published []publishedMsg
	h := NewHandler(store, siteID, func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	})

	// Self-leave: Requester == Account
	req := model.RemoveMemberRequest{RoomID: roomID, Requester: account, Account: account}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.NoError(t, err)

	// Expect: subscription update + member change event + system message = 3 publishes
	assert.Len(t, published, 3, "expected 3 publishes: sub update, member event, sys msg")

	subjSet := make(map[string]bool)
	for _, p := range published {
		subjSet[p.subj] = true
	}

	assert.True(t, subjSet[subject.SubscriptionUpdate(account)], "expected subscription update published")
	assert.True(t, subjSet[subject.MemberEvent(roomID)], "expected member event published")

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
			ID:      "u1",
			Account: account,
			SiteID:  siteID,
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
	})

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: account, Account: account}
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
				User:             model.User{ID: "u1", Account: account, SiteID: siteID},
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
			})

			req := model.RemoveMemberRequest{RoomID: roomID, Requester: tc.requester, Account: account}
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
		ReconcileUserCount(gomock.Any(), roomID).Return(nil)

	var published []publishedMsg
	h := NewHandler(store, siteID, func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	})

	// requester != account means this is owner-removes-other
	req := model.RemoveMemberRequest{RoomID: roomID, Requester: requester, Account: account}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.NoError(t, err)

	assert.Len(t, published, 3, "expected 3 publishes: sub update, member event, sys msg")

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

func TestHandler_ProcessAddMembers(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	var published []publishedMsg
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}
	h := NewHandler(store, "site-a", publish)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", SiteID: "site-a"}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), nil, []string{"bob", "charlie"}, "r1").
		Return([]string{"bob", "charlie"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "charlie"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a"},
		{ID: "u3", Account: "charlie", SiteID: "site-b"},
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, subs []*model.Subscription) error {
			assert.Len(t, subs, 2)
			for _, s := range subs {
				assert.Equal(t, "site-a", s.SiteID)
				assert.Equal(t, []model.Role{model.RoleMember}, s.Roles)
				require.NotNil(t, s.HistorySharedSince)
				assert.Equal(t, s.JoinedAt, *s.HistorySharedSince)
			}
			return nil
		})
	store.EXPECT().ReconcileUserCount(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	req := model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob", "charlie"},
		History: model.HistoryConfig{Mode: model.HistoryModeNone},
	}
	reqData, _ := json.Marshal(req)

	err := h.processAddMembers(context.Background(), reqData)
	require.NoError(t, err)

	// 2 SubscriptionUpdate + 1 MemberAddEvent + 1 system msg + 1 batched outbox (site-b)
	assert.GreaterOrEqual(t, len(published), 4)

	// Verify exactly 1 outbox event for site-b (batched, not per-member)
	var outboxCount int
	for _, p := range published {
		if strings.Contains(p.subj, "outbox") {
			outboxCount++
			assert.Contains(t, p.subj, "site-b")
			var outboxEvt model.OutboxEvent
			require.NoError(t, json.Unmarshal(p.data, &outboxEvt))
			var change model.MemberAddEvent
			require.NoError(t, json.Unmarshal(outboxEvt.Payload, &change))
			assert.Equal(t, []string{"charlie"}, change.Accounts)
		}
	}
	assert.Equal(t, 1, outboxCount, "should publish exactly 1 batched outbox event per destination site")
}

func TestHandler_ProcessAddMembers_HistoryAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, "site-a", publish)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", SiteID: "site-a"}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), nil, []string{"bob"}, "r1").
		Return([]string{"bob"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a"},
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, subs []*model.Subscription) error {
			assert.Len(t, subs, 1)
			assert.Nil(t, subs[0].HistorySharedSince, "HistorySharedSince should be nil for mode all")
			return nil
		})
	store.EXPECT().ReconcileUserCount(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	req := model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob"},
		History: model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	reqData, _ := json.Marshal(req)

	err := h.processAddMembers(context.Background(), reqData)
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
	h := NewHandler(store, "site-a", publish)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", SiteID: "site-a"}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), nil, []string{"bob", "charlie"}, "r1").
		Return([]string{"bob", "charlie"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "charlie"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a"},
		{ID: "u3", Account: "charlie", SiteID: "site-b"},
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileUserCount(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	const reqTS int64 = 1744300000000
	req := model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob", "charlie"},
		History:   model.HistoryConfig{Mode: model.HistoryModeNone},
		Timestamp: reqTS,
	}
	reqData, _ := json.Marshal(req)
	require.NoError(t, h.processAddMembers(context.Background(), reqData))

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
	h := NewHandler(store, "site-a", publish)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", SiteID: "site-a"}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), nil, []string{"bob"}, "r1").
		Return([]string{"bob"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a"},
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileUserCount(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	req := model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob"},
		History: model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	reqData, _ := json.Marshal(req)
	require.NoError(t, h.processAddMembers(context.Background(), reqData))

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
	h := NewHandler(store, "site-a", publish)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", SiteID: "site-a"}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), []string{"eng"}, []string{"bob"}, "r1").
		Return([]string{"bob"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a"},
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileUserCount(gomock.Any(), "r1").Return(nil)
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
		RoomID: "r1", Users: []string{"bob"}, Orgs: []string{"eng"},
		History: model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	reqData, _ := json.Marshal(req)

	err := h.processAddMembers(context.Background(), reqData)
	require.NoError(t, err)
}

func TestHandler_ProcessAddMembers_UserNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, "site-a", publish)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", SiteID: "site-a"}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), nil, []string{"bob", "ghost"}, "r1").
		Return([]string{"bob", "ghost"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob", "ghost"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a"},
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, subs []*model.Subscription) error {
			assert.Len(t, subs, 1, "ghost should be skipped")
			assert.Equal(t, "bob", subs[0].User.Account)
			return nil
		})
	store.EXPECT().ReconcileUserCount(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	req := model.AddMembersRequest{
		RoomID: "r1", Users: []string{"bob", "ghost"},
		History: model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	reqData, _ := json.Marshal(req)

	err := h.processAddMembers(context.Background(), reqData)
	require.NoError(t, err)
}

func TestHandler_ProcessAddMembers_MultipleSiteOutbox(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	var published []publishedMsg
	publish := func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	}
	h := NewHandler(store, "site-a", publish)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", SiteID: "site-a"}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), nil, []string{"alice", "bob", "charlie"}, "r1").
		Return([]string{"alice", "bob", "charlie"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"alice", "bob", "charlie"}).Return([]model.User{
		{ID: "u1", Account: "alice", SiteID: "site-b"},
		{ID: "u2", Account: "bob", SiteID: "site-b"},
		{ID: "u3", Account: "charlie", SiteID: "site-c"},
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileUserCount(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)

	req := model.AddMembersRequest{
		RoomID: "r1", Users: []string{"alice", "bob", "charlie"},
		History: model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	reqData, _ := json.Marshal(req)

	err := h.processAddMembers(context.Background(), reqData)
	require.NoError(t, err)

	var outboxEvents []publishedMsg
	for _, p := range published {
		if strings.Contains(p.subj, "outbox") {
			outboxEvents = append(outboxEvents, p)
		}
	}
	assert.Len(t, outboxEvents, 2, "should batch outbox by site: 1 for site-b, 1 for site-c")

	for _, p := range outboxEvents {
		var outboxEvt model.OutboxEvent
		require.NoError(t, json.Unmarshal(p.data, &outboxEvt))
		var change model.MemberAddEvent
		require.NoError(t, json.Unmarshal(outboxEvt.Payload, &change))

		if strings.Contains(p.subj, "site-b") {
			assert.Len(t, change.Accounts, 2, "site-b should have alice and bob")
		} else if strings.Contains(p.subj, "site-c") {
			assert.Equal(t, []string{"charlie"}, change.Accounts)
		}
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
		{Account: "carol", SiteID: siteID, SectName: "Engineering", HasIndividualMembership: false},
		{Account: "dave", SiteID: siteID, SectName: "Engineering", HasIndividualMembership: false},
		{Account: "eve", SiteID: siteID, SectName: "Engineering", HasIndividualMembership: true},
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
		ReconcileUserCount(gomock.Any(), roomID).Return(nil) // recount after removal

	var published []publishedMsg
	h := NewHandler(store, siteID, func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	})

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: requester, OrgID: orgID}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.NoError(t, err)

	// Expect: 2 sub updates (carol, dave) + 1 member event + 1 sys msg = 4 publishes
	assert.Len(t, published, 4, "expected 4 publishes: 2 sub updates, member event, sys msg")

	subjSet := make(map[string]bool)
	for _, p := range published {
		subjSet[p.subj] = true
	}

	assert.True(t, subjSet[subject.SubscriptionUpdate("carol")], "expected subscription update for carol")
	assert.True(t, subjSet[subject.SubscriptionUpdate("dave")], "expected subscription update for dave")
	assert.False(t, subjSet[subject.SubscriptionUpdate("eve")], "eve has individual membership, should not be removed")
	assert.True(t, subjSet[subject.MemberEvent(roomID)], "expected member event published")
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
			ID:      "u1",
			Account: account,
			SiteID:  userSite, // different from local site
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
		ReconcileUserCount(gomock.Any(), roomID).Return(nil)

	var published []publishedMsg
	h := NewHandler(store, localSite, func(_ context.Context, subj string, data []byte, _ string) error {
		published = append(published, publishedMsg{subj: subj, data: data})
		return nil
	})

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: account, Account: account}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.NoError(t, err)

	// Expect: sub update + member event + sys msg + outbox = 4 publishes
	assert.Len(t, published, 4, "expected 4 publishes including outbox for federated user")

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
	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })

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

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "alice"}
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
			User:  model.User{ID: "u1", Account: "alice"},
			Roles: []model.Role{model.RoleMember},
		}, nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), "r1", model.RoomMemberIndividual, "u1").
		Return(fmt.Errorf("write failed"))

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "alice"}
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
			User:             model.User{ID: "u1", Account: "alice"},
			HasOrgMembership: true,
			Roles:            []model.Role{model.RoleOwner, model.RoleMember},
		}, nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), "r1", model.RoomMemberIndividual, "u1").
		Return(nil)
	store.EXPECT().
		RemoveRole(gomock.Any(), "alice", "r1", model.RoleOwner).
		Return(fmt.Errorf("write failed"))

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "alice"}
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
			User:  model.User{ID: "u1", Account: "alice"},
			Roles: []model.Role{model.RoleMember},
		}, nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), "r1", model.RoomMemberIndividual, "u1").
		Return(nil)
	store.EXPECT().
		DeleteSubscription(gomock.Any(), "r1", "alice").
		Return(int64(0), fmt.Errorf("write failed"))

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "alice"}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete subscription")
}

func TestHandler_ProcessRemoveIndividual_ReconcileUserCountError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)
	store.EXPECT().
		GetUserWithMembership(gomock.Any(), "r1", "alice").
		Return(&UserWithMembership{
			User:  model.User{ID: "u1", Account: "alice"},
			Roles: []model.Role{model.RoleMember},
		}, nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), "r1", model.RoomMemberIndividual, "u1").
		Return(nil)
	store.EXPECT().
		DeleteSubscription(gomock.Any(), "r1", "alice").
		Return(int64(1), nil)
	store.EXPECT().
		ReconcileUserCount(gomock.Any(), "r1").
		Return(fmt.Errorf("write failed"))

	h := NewHandler(store, "site-a", func(_ context.Context, _ string, _ []byte, _ string) error { return nil })
	req := model.RemoveMemberRequest{RoomID: "r1", Requester: "alice", Account: "alice"}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconcile user count")
}

func TestHandler_ProcessAddMembers_ExistingOrgsWritesIndividuals(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, "site-a", publish)

	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", SiteID: "site-a"}, nil)
	store.EXPECT().ListNewMembers(gomock.Any(), nil, []string{"bob"}, "r1").
		Return([]string{"bob"}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), []string{"bob"}).Return([]model.User{
		{ID: "u2", Account: "bob", SiteID: "site-a"},
	}, nil)
	store.EXPECT().BulkCreateSubscriptions(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ReconcileUserCount(gomock.Any(), "r1").Return(nil)
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(true, nil)
	store.EXPECT().BulkCreateRoomMembers(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, members []*model.RoomMember) error {
			require.Len(t, members, 1)
			assert.Equal(t, model.RoomMemberIndividual, members[0].Member.Type)
			assert.Equal(t, "bob", members[0].Member.Account)
			return nil
		})

	req := model.AddMembersRequest{
		RoomID:  "r1",
		Users:   []string{"bob"},
		History: model.HistoryConfig{Mode: model.HistoryModeAll},
	}
	reqData, _ := json.Marshal(req)

	err := h.processAddMembers(context.Background(), reqData)
	require.NoError(t, err)
}

// Message.ID and Nats-Msg-Id both come from idgen.DeriveID now — its
// determinism tests live in pkg/idgen, but keep a smoke test here to catch
// any regressions in the seed format used for JetStream dedup headers.
func TestDedupID_StableAcrossCalls(t *testing.T) {
	a := idgen.DeriveID("addmembers-msg:room-1:1")
	b := idgen.DeriveID("addmembers-msg:room-1:1")
	assert.Equal(t, a, b)
	assert.NotEmpty(t, a)
	// Different seeds must produce different IDs — otherwise JetStream
	// dedup would collapse unrelated operations into a single published event.
	c := idgen.DeriveID("addmembers-msg:room-1:2")
	assert.NotEqual(t, a, c)
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
			User:             model.User{ID: "u1", Account: account, SiteID: userSite},
			HasOrgMembership: false,
		}, nil)
	store.EXPECT().
		DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberIndividual, "u1").
		Return(nil)
	store.EXPECT().
		DeleteSubscription(gomock.Any(), roomID, account).
		Return(int64(1), nil)
	store.EXPECT().
		ReconcileUserCount(gomock.Any(), roomID).Return(nil)

	outboxSubj := subject.Outbox(localSite, userSite, "member_removed")
	publish := func(_ context.Context, subj string, _ []byte, _ string) error {
		if subj == outboxSubj {
			return fmt.Errorf("outbox publish broken")
		}
		return nil
	}
	h := NewHandler(store, localSite, publish)

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: account, Account: account}
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
		{Account: "carol", SiteID: remoteSite, SectName: "Eng", HasIndividualMembership: false},
	}

	store.EXPECT().GetOrgMembersWithIndividualStatus(gomock.Any(), roomID, orgID).Return(orgMembers, nil)
	store.EXPECT().DeleteSubscriptionsByAccounts(gomock.Any(), roomID, []string{"carol"}).Return(int64(1), nil)
	store.EXPECT().DeleteRoomMember(gomock.Any(), roomID, model.RoomMemberOrg, orgID).Return(nil)
	store.EXPECT().ReconcileUserCount(gomock.Any(), roomID).Return(nil)

	outboxSubj := subject.Outbox(localSite, remoteSite, "member_removed")
	publish := func(_ context.Context, subj string, _ []byte, _ string) error {
		if subj == outboxSubj {
			return fmt.Errorf("outbox publish broken")
		}
		return nil
	}
	h := NewHandler(store, localSite, publish)

	req := model.RemoveMemberRequest{RoomID: roomID, Requester: requester, OrgID: orgID}
	data, _ := json.Marshal(req)

	err := h.processRemoveMember(context.Background(), data)
	require.Error(t, err, "outbox failure must return error so JetStream NAKs and retries")
	assert.Contains(t, err.Error(), "outbox")
}
