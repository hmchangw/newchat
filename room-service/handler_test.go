package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestHandler_CreateRoom(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	var capturedSub *model.Subscription
	store.EXPECT().CreateRoom(gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().CreateSubscription(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sub *model.Subscription) error {
		capturedSub = sub
		return nil
	})

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	req := model.CreateRoomRequest{Name: "general", Type: model.RoomTypeChannel, CreatedBy: "u1", CreatedByAccount: "alice", SiteID: "site-a"}
	data, _ := json.Marshal(req)

	resp, err := h.handleCreateRoom(context.Background(), data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var room model.Room
	json.Unmarshal(resp, &room)
	if room.Name != "general" || room.CreatedBy != "u1" {
		t.Errorf("got %+v", room)
	}
	if capturedSub == nil || capturedSub.User.Account != "alice" {
		t.Errorf("expected owner subscription with Account=alice, got %+v", capturedSub)
	}
}

func TestHandler_UpdateRole_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}, nil)
	store.EXPECT().
		GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{
			Subscription:            &model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}},
			HasIndividualMembership: true,
		}, nil)

	var publishedData []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte) error { publishedData = data; return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	resp, err := h.handleUpdateRole(context.Background(), subj, data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]string
	json.Unmarshal(resp, &result)
	if result["status"] != "accepted" {
		t.Errorf("expected status=accepted, got %v", result)
	}
	if publishedData == nil {
		t.Error("expected event published to JetStream")
	}
}

func TestHandler_UpdateRole_NonOwnerRejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().
		GetSubscription(gomock.Any(), "bob", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}}, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "charlie", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("bob", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for non-owner role update")
	}
	if err.Error() != "only owners can update roles" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHandler_UpdateRole_DMRejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "dm-room", Type: model.RoomTypeDM}, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for DM room role update")
	}
	if err.Error() != "role update is only allowed in channel rooms" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHandler_UpdateRole_InvalidRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: "admin"}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for invalid role")
	}
	if err.Error() != "invalid role: must be owner or member" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHandler_UpdateRole_AlreadyHasRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}, nil)
	store.EXPECT().
		GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{
			Subscription:            &model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}},
			HasIndividualMembership: true,
		}, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for duplicate role")
	}
	if err.Error() != "user is already an owner" {
		t.Errorf("unexpected error: %v", err)
	}
}

// Bug 5: an org-only subscriber must not be promotable to owner.
// The system invariant is "owners must hold an individual membership source"
// so that remove-member's dual-membership demote path (strip owner on
// individual-leave) always lands on a non-owner state.
func TestHandler_UpdateRole_PromoteOrgOnlyRejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}, nil)
	// Target is present in the room via an org entry only.
	store.EXPECT().
		GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{
			Subscription: &model.Subscription{
				User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1",
				Roles: []model.Role{model.RoleMember},
			},
			HasIndividualMembership: false,
			HasOrgMembership:        true,
		}, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte) error {
			t.Fatal("must not publish when promotion rejected")
			return nil
		},
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	require.Error(t, err)
	assert.ErrorIs(t, err, errPromoteRequiresIndividual)
}

// Add-member only writes room_members docs for rooms that involve orgs.
// In a room with no orgs, every subscriber is "individual" via their
// subscription alone — GetSubscriptionWithMembership returns both flags
// false. Those users must remain promotable.
func TestHandler_UpdateRole_PromoteSubscriptionOnly_NoRoomMembers_Allowed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}, nil)
	// No orgs in this room → no room_members docs for bob → both flags false.
	store.EXPECT().
		GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{
			Subscription: &model.Subscription{
				User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1",
				Roles: []model.Role{model.RoleMember},
			},
			HasIndividualMembership: false,
			HasOrgMembership:        false,
		}, nil)

	var published []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte) error {
			published = data
			return nil
		},
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	require.NoError(t, err)
	assert.NotNil(t, published, "promote must publish to ROOMS stream when target is a bare subscriber in a room with no orgs")
}

func TestHandler_UpdateRole_DemoteNonOwner(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)
	store.EXPECT().
		GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{
			Subscription:            &model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}},
			HasIndividualMembership: true,
		}, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleMember}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for demoting non-owner")
	}
	if err.Error() != "user is not an owner" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHandler_UpdateRole_LastOwnerCannotDemote(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)
	store.EXPECT().
		GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{
			Subscription:            &model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}},
			HasIndividualMembership: true,
		}, nil)
	store.EXPECT().
		CountOwners(gomock.Any(), "r1").
		Return(1, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "alice", NewRole: model.RoleMember}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for last owner demotion")
	}
	if err.Error() != "cannot demote the last owner" {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- Error-path tests ---

func TestHandler_UpdateRole_MalformedInput(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")
	_, err := h.handleUpdateRole(context.Background(), subj, []byte("not json"))
	if err == nil {
		t.Fatal("expected error for malformed input")
	}
}

func TestHandler_UpdateRole_GetRoomError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, fmt.Errorf("db error"))
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")
	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for GetRoom failure")
	}
}

func TestHandler_UpdateRole_RoomIDMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	// Payload RoomID "r-other" does not match subject RoomID "r1"
	req := model.UpdateRoleRequest{RoomID: "r-other", Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")
	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for RoomID mismatch")
	}
	if err.Error() != "invalid request: room ID mismatch" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHandler_UpdateRole_RequesterSubError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(nil, fmt.Errorf("db error"))
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")
	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for requester subscription failure")
	}
}

func TestHandler_UpdateRole_TargetSubError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		Roles: []model.Role{model.RoleMember, model.RoleOwner},
	}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").Return(nil, fmt.Errorf("db error"))
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")
	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for target subscription failure")
	}
}

func TestHandler_UpdateRole_CountOwnersError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	// Self-demotion triggers CountOwners
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		Roles: []model.Role{model.RoleMember, model.RoleOwner},
	}, nil) // requester lookup
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").Return(&SubscriptionWithMembership{
		Subscription:            &model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}},
		HasIndividualMembership: true,
	}, nil) // target lookup (same user, self-demote)
	store.EXPECT().CountOwners(gomock.Any(), "r1").Return(0, fmt.Errorf("db error"))
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	req := model.UpdateRoleRequest{Account: "alice", NewRole: model.RoleMember}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")
	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for CountOwners failure")
	}
}

func TestHandler_UpdateRole_PublishError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		Roles: []model.Role{model.RoleMember, model.RoleOwner},
	}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").Return(&SubscriptionWithMembership{
		Subscription:            &model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}},
		HasIndividualMembership: true,
	}, nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte) error { return fmt.Errorf("nats down") },
	}
	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")
	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for publish failure")
	}
}

func TestHandler_RemoveMember_SelfLeave_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	hss := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", Roles: []model.Role{model.RoleMember},
		HistorySharedSince: &hss, JoinedAt: hss,
	}
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{Subscription: sub, HasIndividualMembership: true}, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 3, OwnerCount: 2}, nil)

	var publishedSubj string
	var publishedData []byte
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, func(ctx context.Context, subj string, data []byte) error {
		publishedSubj = subj
		publishedData = data
		return nil
	})

	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	resp, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, subject.RoomCanonical("site-a", "member.remove"), publishedSubj)
	var status map[string]string
	require.NoError(t, json.Unmarshal(resp, &status))
	assert.Equal(t, "accepted", status["status"])
	require.NotNil(t, publishedData)

	var published model.RemoveMemberRequest
	require.NoError(t, json.Unmarshal(publishedData, &published))
	assert.Equal(t, "alice", published.Requester)
}

func TestHandler_RemoveMember_OrgOnly_Rejected(t *testing.T) {
	// Org-only guard fires immediately after GetSubscriptionWithMembership;
	// later calls (GetSubscription, CountMembersAndOwners) must not run.
	cases := []struct {
		name      string
		requester string
		target    string
	}{
		{"self-leave", "alice", "alice"},
		{"owner-removes", "bob", "alice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			sub := &model.Subscription{
				ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
				RoomID: "r1", Roles: []model.Role{model.RoleMember},
			}
			store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
				Return(&SubscriptionWithMembership{Subscription: sub, HasOrgMembership: true}, nil)
			handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
			reqSubj := subject.MemberRemove(tc.requester, "r1", "site-a")
			reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: tc.target})
			_, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "org members cannot leave individually")
		})
	}
}

func TestHandler_RemoveMember_SelfLeave_NoOrgs_Allowed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	sub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember},
	}
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{Subscription: sub}, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 2, OwnerCount: 1}, nil)

	var publishedData []byte
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, func(ctx context.Context, _ string, data []byte) error {
		publishedData = data
		return nil
	})
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
	require.NoError(t, err)
	require.NotNil(t, publishedData)
}

func TestHandler_RemoveMember_LastOwner_Rejected(t *testing.T) {
	cases := []struct {
		name      string
		requester string
	}{
		{"self-leave", "alice"},
		{"owner-removes-last-owner", "bob"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			target := &model.Subscription{
				ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
				RoomID: "r1", Roles: []model.Role{model.RoleOwner},
			}
			store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
				Return(&SubscriptionWithMembership{Subscription: target, HasIndividualMembership: true}, nil)
			if tc.requester != "alice" {
				ownerSub := &model.Subscription{
					ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"},
					RoomID: "r1", Roles: []model.Role{model.RoleOwner},
				}
				store.EXPECT().GetSubscription(gomock.Any(), tc.requester, "r1").Return(ownerSub, nil)
			}
			store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
				Return(&RoomCounts{MemberCount: 3, OwnerCount: 1}, nil)
			handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
			reqSubj := subject.MemberRemove(tc.requester, "r1", "site-a")
			reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
			_, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "last owner")
		})
	}
}

func TestHandler_RemoveMember_LastMember_Rejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	sub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember},
	}
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{Subscription: sub, HasIndividualMembership: true}, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 1, OwnerCount: 0}, nil)
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "last member")
}

func TestHandler_RemoveMember_OwnerRemovesOther_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	targetSub := &model.Subscription{
		ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember},
	}
	ownerSub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleOwner},
	}
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{Subscription: targetSub, HasIndividualMembership: true}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(ownerSub, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 3, OwnerCount: 1}, nil)
	var publishedData []byte
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, func(ctx context.Context, subj string, data []byte) error {
		publishedData = data
		return nil
	})
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "bob"})
	resp, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, publishedData)
}

func TestHandler_RemoveMember_NonOwnerRemovesOther_Rejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	targetSub := &model.Subscription{
		ID: "s2", User: model.SubscriptionUser{ID: "u2", Account: "bob"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember},
	}
	requesterSub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember},
	}
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{Subscription: targetSub, HasIndividualMembership: true}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(requesterSub, nil)
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "bob"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only owners can remove members")
}

func TestHandler_RemoveMember_OwnerRemovesOrg_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	ownerSub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleOwner},
	}
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(ownerSub, nil)
	var publishedData []byte
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, func(ctx context.Context, subj string, data []byte) error {
		publishedData = data
		return nil
	})
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", OrgID: "eng-org"})
	resp, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
	require.NoError(t, err)
	require.NotNil(t, resp)
	var published model.RemoveMemberRequest
	require.NoError(t, json.Unmarshal(publishedData, &published))
	assert.Equal(t, "eng-org", published.OrgID)
}

func TestHandler_RemoveMember_BothAccountAndOrgID_Rejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "bob", OrgID: "eng-org"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one")
}

func TestHandler_RemoveMember_NeitherAccountNorOrgID_Rejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one")
}

func TestHandler_RemoveMember_InvalidSubject(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
	_, err := handler.handleRemoveMember(context.Background(), "bogus", []byte("{}"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid remove-member subject")
}

func TestHandler_RemoveMember_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, []byte("{not json"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid request")
}

func TestHandler_RemoveMember_RoomIDMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	body, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r2", Account: "alice"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "room ID mismatch")
}

func TestHandler_RemoveMember_GetTargetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(nil, fmt.Errorf("db down"))
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	body, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get target subscription")
}

func TestHandler_RemoveMember_OwnerRemoves_RequesterLookupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	targetSub := &model.Subscription{
		User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember},
	}
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{Subscription: targetSub, HasIndividualMembership: true}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, fmt.Errorf("db down"))
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	body, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "bob"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get requester subscription")
}

func TestHandler_RemoveMember_CountsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	sub := &model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleMember},
	}
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{Subscription: sub, HasIndividualMembership: true}, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(nil, fmt.Errorf("db down"))
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	body, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "count members")
}

func TestHandler_RemoveMember_OrgPath_RequesterLookupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, fmt.Errorf("db down"))
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	body, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", OrgID: "eng-org"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get requester subscription")
}

func TestHandler_RemoveMember_PublishError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	sub := &model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleMember},
	}
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{Subscription: sub, HasIndividualMembership: true}, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 3, OwnerCount: 2}, nil)
	handler := NewHandler(store, nil, nil, "site-a", 1000, 500, func(_ context.Context, _ string, _ []byte) error {
		return fmt.Errorf("nats down")
	})
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	body, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish to stream")
}

// --- Add Members tests ---

func TestHandler_AddMembers_DMRejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}, nil)
	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "dm-room", Type: model.RoomTypeDM}, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 10,
		publishToStream: func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	req := model.AddMembersRequest{RoomID: "r1", Users: []string{"bob"}}
	data, _ := json.Marshal(req)
	subj := subject.MemberAdd("alice", "r1", "site-a")

	_, err := h.handleAddMembers(context.Background(), subj, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-channel room")
}

func TestHandler_AddMembers_RestrictedNonOwnerRejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetSubscription(gomock.Any(), "bob", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}}, nil)
	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "restricted-room", Type: model.RoomTypeChannel, Restricted: true}, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 10,
		publishToStream: func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	req := model.AddMembersRequest{RoomID: "r1", Users: []string{"charlie"}}
	data, _ := json.Marshal(req)
	subj := subject.MemberAdd("bob", "r1", "site-a")

	_, err := h.handleAddMembers(context.Background(), subj, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only owners can add members")
}

func TestHandler_AddMembers_CapacityExceeded(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}, nil)
	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel, UserCount: 8}, nil)
	store.EXPECT().
		CountNewMembers(gomock.Any(), gomock.Any(), []string{"u1", "u2", "u3", "u4", "u5"}, "r1").
		Return(5, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 10,
		publishToStream: func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	req := model.AddMembersRequest{RoomID: "r1", Users: []string{"u1", "u2", "u3", "u4", "u5"}}
	data, _ := json.Marshal(req)
	subj := subject.MemberAdd("alice", "r1", "site-a")

	_, err := h.handleAddMembers(context.Background(), subj, data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maximum capacity")
}

func TestHandler_AddMembers_RestrictedOwnerAllowed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte) error { return nil }
	h := NewHandler(store, nil, nil, "site-a", 100, 500, publish)

	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		Roles: []model.Role{model.RoleOwner},
	}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, Restricted: true, UserCount: 1,
	}, nil)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "r1").
		Return(1, nil)

	req := model.AddMembersRequest{RoomID: "r1", Users: []string{"bob"}}
	reqData, _ := json.Marshal(req)

	resp, err := h.handleAddMembers(context.Background(), subject.MemberAdd("alice", "r1", "site-a"), reqData)
	require.NoError(t, err)

	var status map[string]string
	require.NoError(t, json.Unmarshal(resp, &status))
	assert.Equal(t, "accepted", status["status"])
}

func TestHandler_AddMembers_EmptyAfterResolve(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte) error { return nil }
	h := NewHandler(store, nil, nil, "site-a", 100, 500, publish)

	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		Roles: []model.Role{model.RoleMember},
	}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, UserCount: 5,
	}, nil)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "r1").
		Return(0, nil)

	req := model.AddMembersRequest{RoomID: "r1", Users: []string{"alice"}}
	reqData, _ := json.Marshal(req)

	resp, err := h.handleAddMembers(context.Background(), subject.MemberAdd("alice", "r1", "site-a"), reqData)
	require.NoError(t, err)

	var status map[string]string
	require.NoError(t, json.Unmarshal(resp, &status))
	assert.Equal(t, "accepted", status["status"])
}

func TestHandler_AddMembers_ChannelExpansion(t *testing.T) {
	t.Run("same-site individuals only", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch1", gomock.Any(), nil, false).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "bob"}},
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "carol"}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		orgs, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.NoError(t, err)
		assert.Empty(t, orgs)
		assert.ElementsMatch(t, []string{"bob", "carol"}, accs)
	})

	t.Run("same-site orgs only", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch1", gomock.Any(), nil, false).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{ID: "org1", Type: model.RoomMemberOrg}},
			{Member: model.RoomMemberEntry{ID: "org2", Type: model.RoomMemberOrg}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		orgs, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"org1", "org2"}, orgs)
		assert.Empty(t, accs)
	})

	t.Run("same-site mixed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch1", gomock.Any(), nil, false).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{ID: "org1", Type: model.RoomMemberOrg}},
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "bob"}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		orgs, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.NoError(t, err)
		assert.Equal(t, []string{"org1"}, orgs)
		assert.Equal(t, []string{"bob"}, accs)
	})

	t.Run("cross-site channel", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-eu"}
		mc.EXPECT().ListMembers(gomock.Any(), "alice", ch, gomock.Any()).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{ID: "org1", Type: model.RoomMemberOrg}},
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "bob"}},
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "carol"}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		orgs, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.NoError(t, err)
		assert.Equal(t, []string{"org1"}, orgs)
		assert.ElementsMatch(t, []string{"bob", "carol"}, accs)
	})

	t.Run("mixed same-site and cross-site", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		local := model.ChannelRef{RoomID: "ch-local", SiteID: "site-a"}
		remote := model.ChannelRef{RoomID: "ch-remote", SiteID: "site-eu"}

		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch-local").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch-local", gomock.Any(), nil, false).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "local-user"}},
		}, nil)
		mc.EXPECT().ListMembers(gomock.Any(), "alice", remote, gomock.Any()).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "remote-user"}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		_, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{local, remote})

		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"local-user", "remote-user"}, accs)
	})

	t.Run("requester not subscribed to same-site source", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(nil, model.ErrSubscriptionNotFound)

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		assert.True(t, errors.Is(err, errNotRoomMember))
	})

	t.Run("same-site GetSubscription generic error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(nil, errors.New("mongo timeout"))

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "subscription check")
	})

	t.Run("same-site ListRoomMembers error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch1", gomock.Any(), nil, false).Return(nil, errors.New("mongo timeout"))

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "local list-members")
	})

	t.Run("cross-site client error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-eu"}
		mc.EXPECT().ListMembers(gomock.Any(), "alice", ch, gomock.Any()).Return(nil, errors.New("nats: timeout"))

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "remote list-members")
	})

	t.Run("cross-site not-a-member returns local sentinel unwrapped", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-eu"}
		// Client already mapped the remote sentinel string back onto errNotRoomMember;
		// expandChannelRefs must pass it through unwrapped (parallel to the same-site
		// branch) so errors.Is matches the sentinel for both paths.
		mc.EXPECT().ListMembers(gomock.Any(), "alice", ch, gomock.Any()).Return(nil, errNotRoomMember)

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		assert.True(t, errors.Is(err, errNotRoomMember))
		assert.NotContains(t, err.Error(), "remote list-members")
	})

	t.Run("cross-site response over maxRoomSize is rejected", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-eu"}
		// Remote returns more members than we'd accept for a room — reject before dedup/resolve
		// so a malicious peer can't inflate downstream work.
		oversized := make([]model.RoomMember, 6)
		for i := range oversized {
			oversized[i] = model.RoomMember{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "u"}}
		}
		mc.EXPECT().ListMembers(gomock.Any(), "alice", ch, gomock.Any()).Return(oversized, nil)

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 5, memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds max")
	})

	t.Run("fail-fast ordering", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ref1 := model.ChannelRef{RoomID: "ch1", SiteID: "site-eu"}
		ref2 := model.ChannelRef{RoomID: "ch2", SiteID: "site-eu"}

		mc.EXPECT().ListMembers(gomock.Any(), "alice", ref1, gomock.Any()).Return(nil, errors.New("nats: timeout"))

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ref1, ref2})

		require.Error(t, err)
	})

	t.Run("empty refs", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		orgs, accs, err := h.expandChannelRefs(context.Background(), "alice", nil)

		require.NoError(t, err)
		assert.Nil(t, orgs)
		assert.Nil(t, accs)
	})

	t.Run("unknown member type skipped", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch1", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch1").Return(&model.Subscription{}, nil)
		store.EXPECT().ListRoomMembers(gomock.Any(), "ch1", gomock.Any(), nil, false).Return([]model.RoomMember{
			{Member: model.RoomMemberEntry{ID: "unknown", Type: ""}},
			{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "bob"}},
		}, nil)

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc}
		_, accs, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.NoError(t, err)
		assert.Equal(t, []string{"bob"}, accs)
	})
}

func TestHandler_ListMembers(t *testing.T) {
	const siteID = "site-a"
	const roomID = "r1"
	const requester = "alice"
	subj := subject.MemberList(requester, roomID, siteID)

	existingMember := model.RoomMember{
		ID: "rm1", RoomID: roomID, Ts: time.Unix(1, 0).UTC(),
		Member: model.RoomMemberEntry{ID: "alice", Type: model.RoomMemberIndividual, Account: "alice"},
	}
	orgMember := model.RoomMember{
		ID: "rm2", RoomID: roomID, Ts: time.Unix(2, 0).UTC(),
		Member: model.RoomMemberEntry{ID: "org-1", Type: model.RoomMemberOrg},
	}

	type want struct {
		errContains string
		errIs       error
		members     []model.RoomMember
	}
	tests := []struct {
		name      string
		subject   string
		body      []byte
		setupMock func(*MockRoomStore)
		want      want
	}{
		{
			name:    "happy path returns members",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().ListRoomMembers(gomock.Any(), roomID, (*int)(nil), (*int)(nil), false).
					Return([]model.RoomMember{orgMember, existingMember}, nil)
			},
			want: want{members: []model.RoomMember{orgMember, existingMember}},
		},
		{
			name:    "fallback path returns synthesized individuals",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				synth := model.RoomMember{
					ID: "sub-xyz", RoomID: roomID, Ts: time.Unix(3, 0).UTC(),
					Member: model.RoomMemberEntry{ID: "u-alice", Type: model.RoomMemberIndividual, Account: "alice"},
				}
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().ListRoomMembers(gomock.Any(), roomID, (*int)(nil), (*int)(nil), false).
					Return([]model.RoomMember{synth}, nil)
			},
			want: want{members: []model.RoomMember{{
				ID: "sub-xyz", RoomID: roomID, Ts: time.Unix(3, 0).UTC(),
				Member: model.RoomMemberEntry{ID: "u-alice", Type: model.RoomMemberIndividual, Account: "alice"},
			}}},
		},
		{
			name:    "requester not a member",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(nil, fmt.Errorf("missing: %w", model.ErrSubscriptionNotFound))
			},
			want: want{errIs: errNotRoomMember},
		},
		{
			name:      "invalid subject",
			subject:   "chat.garbage",
			body:      nil,
			setupMock: func(s *MockRoomStore) {},
			want:      want{errContains: "invalid list-members subject"},
		},
		{
			name:    "invalid JSON body",
			subject: subj,
			body:    []byte("{not json"),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
			},
			want: want{errContains: "invalid request"},
		},
		{
			name:    "non-positive limit: negative",
			subject: subj,
			body:    []byte(`{"limit":-1}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
			},
			want: want{errContains: "limit must be > 0"},
		},
		{
			name:    "non-positive limit: zero",
			subject: subj,
			body:    []byte(`{"limit":0}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
			},
			want: want{errContains: "limit must be > 0"},
		},
		{
			name:    "negative offset",
			subject: subj,
			body:    []byte(`{"offset":-1}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
			},
			want: want{errContains: "offset must be >= 0"},
		},
		{
			name:    "pagination passed through",
			subject: subj,
			body:    []byte(`{"limit":10,"offset":5}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().ListRoomMembers(gomock.Any(), roomID, gomock.Any(), gomock.Any(), false).
					DoAndReturn(func(_ context.Context, _ string, limit, offset *int, _ bool) ([]model.RoomMember, error) {
						require.NotNil(t, limit)
						require.NotNil(t, offset)
						assert.Equal(t, 10, *limit)
						assert.Equal(t, 5, *offset)
						return []model.RoomMember{}, nil
					})
			},
			want: want{members: []model.RoomMember{}},
		},
		{
			name:    "auth probe infra error",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "check room membership"},
		},
		{
			name:    "store error on list",
			subject: subj,
			body:    nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().ListRoomMembers(gomock.Any(), roomID, (*int)(nil), (*int)(nil), false).
					Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "get room members"},
		},
		{
			name:    "enrich=true passed through to store",
			subject: subj,
			body:    []byte(`{"enrich":true}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), requester, roomID).
					Return(&model.Subscription{User: model.SubscriptionUser{Account: requester}, RoomID: roomID}, nil)
				s.EXPECT().ListRoomMembers(gomock.Any(), roomID, (*int)(nil), (*int)(nil), true).
					Return([]model.RoomMember{
						{
							ID: "rm1", RoomID: roomID, Ts: time.Unix(1, 0).UTC(),
							Member: model.RoomMemberEntry{
								ID: "alice", Type: model.RoomMemberIndividual, Account: "alice",
								EngName: "Alice Wang", ChineseName: "愛麗絲", IsOwner: true,
							},
						},
					}, nil)
			},
			want: want{members: []model.RoomMember{
				{
					ID: "rm1", RoomID: roomID, Ts: time.Unix(1, 0).UTC(),
					Member: model.RoomMemberEntry{
						ID: "alice", Type: model.RoomMemberIndividual, Account: "alice",
						EngName: "Alice Wang", ChineseName: "愛麗絲", IsOwner: true,
					},
				},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			tc.setupMock(store)

			h := &Handler{store: store, siteID: siteID}
			resp, err := h.handleListMembers(context.Background(), tc.subject, tc.body)

			if tc.want.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.want.errContains)
				return
			}
			if tc.want.errIs != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tc.want.errIs), "error chain should contain %v, got %v", tc.want.errIs, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want.members, resp.Members)
		})
	}
}

func TestHandler_ListOrgMembers(t *testing.T) {
	const orgID = "sect-eng"
	subj := subject.OrgMembers("alice", orgID)

	members := []model.OrgMember{
		{ID: "u-a", Account: "a", EngName: "A", ChineseName: "AA", SiteID: "site-a"},
		{ID: "u-b", Account: "b", EngName: "B", ChineseName: "BB", SiteID: "site-a"},
	}

	type want struct {
		errContains string
		errIs       error
		members     []model.OrgMember
	}
	tests := []struct {
		name      string
		subject   string
		setupMock func(*MockRoomStore)
		want      want
	}{
		{
			name:    "happy path returns members",
			subject: subj,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().ListOrgMembers(gomock.Any(), orgID).Return(members, nil)
			},
			want: want{members: members},
		},
		{
			name:      "invalid subject",
			subject:   "chat.garbage",
			setupMock: func(s *MockRoomStore) {},
			want:      want{errContains: "invalid org-members subject"},
		},
		{
			name:    "empty org returns errInvalidOrg",
			subject: subj,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().ListOrgMembers(gomock.Any(), orgID).Return(nil, errInvalidOrg)
			},
			want: want{errIs: errInvalidOrg},
		},
		{
			name:    "store error is wrapped",
			subject: subj,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().ListOrgMembers(gomock.Any(), orgID).
					Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "get org members"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			tc.setupMock(store)

			h := &Handler{store: store, siteID: "site-a"}
			resp, err := h.handleListOrgMembers(context.Background(), tc.subject)

			if tc.want.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.want.errContains)
				return
			}
			if tc.want.errIs != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tc.want.errIs), "error chain should contain %v, got %v", tc.want.errIs, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.members, resp.Members)
		})
	}
}
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return data
}

func TestHandler_handleRoomsInfoBatch(t *testing.T) {
	// Shared key material for tests that need Valkey keys.
	privBytes := []byte("01234567890123456789012345678901") // 32 bytes
	privB64 := base64.StdEncoding.EncodeToString(privBytes)

	now := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name       string
		payload    []byte
		setupStore func(*MockRoomStore)
		setupKeys  func(*MockRoomKeyStore)
		wantErr    string
		assertResp func(t *testing.T, resp model.RoomsInfoBatchResponse)
	}{
		{
			name:    "happy path — 3 rooms, 2 keyed, order preserved",
			payload: mustJSON(t, model.RoomsInfoBatchRequest{RoomIDs: []string{"r1", "r2", "r3"}}),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r1", "r2", "r3"}).Return([]model.Room{
					{ID: "r1", Name: "general", SiteID: "site-a"},
					{ID: "r2", Name: "random", SiteID: "site-a"},
					{ID: "r3", Name: "help", SiteID: "site-b"},
				}, nil)
			},
			setupKeys: func(k *MockRoomKeyStore) {
				k.EXPECT().GetMany(gomock.Any(), []string{"r1", "r2", "r3"}).Return(map[string]*roomkeystore.VersionedKeyPair{
					"r1": {Version: 1, KeyPair: roomkeystore.RoomKeyPair{PrivateKey: privBytes, PublicKey: []byte("pub1")}},
					"r3": {Version: 5, KeyPair: roomkeystore.RoomKeyPair{PrivateKey: privBytes, PublicKey: []byte("pub3")}},
				}, nil)
			},
			assertResp: func(t *testing.T, resp model.RoomsInfoBatchResponse) {
				require.Len(t, resp.Rooms, 3)

				// r1: found, keyed
				assert.Equal(t, "r1", resp.Rooms[0].RoomID)
				assert.True(t, resp.Rooms[0].Found)
				assert.Equal(t, "general", resp.Rooms[0].Name)
				require.NotNil(t, resp.Rooms[0].PrivateKey)
				assert.Equal(t, privB64, *resp.Rooms[0].PrivateKey)
				require.NotNil(t, resp.Rooms[0].KeyVersion)
				assert.Equal(t, 1, *resp.Rooms[0].KeyVersion)

				// r2: found, no key
				assert.Equal(t, "r2", resp.Rooms[1].RoomID)
				assert.True(t, resp.Rooms[1].Found)
				assert.Nil(t, resp.Rooms[1].PrivateKey)
				assert.Nil(t, resp.Rooms[1].KeyVersion)

				// r3: found, keyed
				assert.Equal(t, "r3", resp.Rooms[2].RoomID)
				assert.True(t, resp.Rooms[2].Found)
				require.NotNil(t, resp.Rooms[2].PrivateKey)
				assert.Equal(t, 5, *resp.Rooms[2].KeyVersion)
			},
		},
		{
			name:    "missing room → Found=false, LastMsgAt=nil",
			payload: mustJSON(t, model.RoomsInfoBatchRequest{RoomIDs: []string{"r-missing"}}),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r-missing"}).Return([]model.Room{}, nil)
			},
			setupKeys: func(k *MockRoomKeyStore) {
				k.EXPECT().GetMany(gomock.Any(), []string{"r-missing"}).Return(map[string]*roomkeystore.VersionedKeyPair{}, nil)
			},
			assertResp: func(t *testing.T, resp model.RoomsInfoBatchResponse) {
				require.Len(t, resp.Rooms, 1)
				assert.Equal(t, "r-missing", resp.Rooms[0].RoomID)
				assert.False(t, resp.Rooms[0].Found)
				assert.Nil(t, resp.Rooms[0].LastMsgAt)
				assert.Nil(t, resp.Rooms[0].LastMentionAllAt)
			},
		},
		{
			name:    "empty RoomIDs → must not be empty",
			payload: mustJSON(t, model.RoomsInfoBatchRequest{RoomIDs: []string{}}),
			wantErr: "must not be empty",
		},
		{
			name: "oversized batch → exceeds limit",
			payload: mustJSON(t, model.RoomsInfoBatchRequest{RoomIDs: func() []string {
				ids := make([]string, 101)
				for i := range ids {
					ids[i] = fmt.Sprintf("r%d", i)
				}
				return ids
			}()}),
			wantErr: "exceeds limit",
		},
		{
			name:    "invalid JSON → invalid request",
			payload: []byte("not json"),
			wantErr: "invalid request",
		},
		{
			name:    "Mongo error → list rooms by ids",
			payload: mustJSON(t, model.RoomsInfoBatchRequest{RoomIDs: []string{"r1"}}),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r1"}).Return(nil, errors.New("mongo timeout"))
			},
			setupKeys: func(k *MockRoomKeyStore) {
				k.EXPECT().GetMany(gomock.Any(), []string{"r1"}).Return(map[string]*roomkeystore.VersionedKeyPair{}, nil).AnyTimes()
			},
			wantErr: "list rooms by ids",
		},
		{
			name:    "Valkey error → get room keys",
			payload: mustJSON(t, model.RoomsInfoBatchRequest{RoomIDs: []string{"r1"}}),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r1"}).Return([]model.Room{{ID: "r1", Name: "x", SiteID: "s"}}, nil).AnyTimes()
			},
			setupKeys: func(k *MockRoomKeyStore) {
				k.EXPECT().GetMany(gomock.Any(), []string{"r1"}).Return(nil, errors.New("valkey down"))
			},
			wantErr: "get room keys",
		},
		{
			name:    "duplicate IDs → 2 entries",
			payload: mustJSON(t, model.RoomsInfoBatchRequest{RoomIDs: []string{"r1", "r1"}}),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r1", "r1"}).Return([]model.Room{
					{ID: "r1", Name: "general", SiteID: "site-a"},
				}, nil)
			},
			setupKeys: func(k *MockRoomKeyStore) {
				k.EXPECT().GetMany(gomock.Any(), []string{"r1", "r1"}).Return(map[string]*roomkeystore.VersionedKeyPair{}, nil)
			},
			assertResp: func(t *testing.T, resp model.RoomsInfoBatchResponse) {
				require.Len(t, resp.Rooms, 2)
				assert.Equal(t, "r1", resp.Rooms[0].RoomID)
				assert.True(t, resp.Rooms[0].Found)
				assert.Equal(t, "r1", resp.Rooms[1].RoomID)
				assert.True(t, resp.Rooms[1].Found)
			},
		},
		{
			name:    "LastMsgAt set → correct millis",
			payload: mustJSON(t, model.RoomsInfoBatchRequest{RoomIDs: []string{"r1"}}),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r1"}).Return([]model.Room{
					{ID: "r1", Name: "general", SiteID: "site-a", LastMsgAt: &now},
				}, nil)
			},
			setupKeys: func(k *MockRoomKeyStore) {
				k.EXPECT().GetMany(gomock.Any(), []string{"r1"}).Return(map[string]*roomkeystore.VersionedKeyPair{}, nil)
			},
			assertResp: func(t *testing.T, resp model.RoomsInfoBatchResponse) {
				require.Len(t, resp.Rooms, 1)
				require.NotNil(t, resp.Rooms[0].LastMsgAt)
				assert.Equal(t, now.UTC().UnixMilli(), *resp.Rooms[0].LastMsgAt)
			},
		},
		{
			name:    "LastMsgAt zero-time in Mongo → nil in response",
			payload: mustJSON(t, model.RoomsInfoBatchRequest{RoomIDs: []string{"r-zero"}}),
			setupStore: func(s *MockRoomStore) {
				zero := time.Time{}
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r-zero"}).Return([]model.Room{
					{
						ID: "r-zero", Name: "quiet", SiteID: "site-a",
						LastMsgAt: &zero, LastMentionAllAt: &zero,
					},
				}, nil)
			},
			setupKeys: func(k *MockRoomKeyStore) {
				k.EXPECT().GetMany(gomock.Any(), []string{"r-zero"}).Return(map[string]*roomkeystore.VersionedKeyPair{}, nil)
			},
			assertResp: func(t *testing.T, resp model.RoomsInfoBatchResponse) {
				require.Len(t, resp.Rooms, 1)
				assert.Equal(t, "r-zero", resp.Rooms[0].RoomID)
				assert.True(t, resp.Rooms[0].Found)
				assert.Nil(t, resp.Rooms[0].LastMsgAt, "non-nil zero-time Room.LastMsgAt must produce nil RoomInfo.LastMsgAt")
				assert.Nil(t, resp.Rooms[0].LastMentionAllAt, "non-nil zero-time Room.LastMentionAllAt must produce nil RoomInfo.LastMentionAllAt")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			keyStore := NewMockRoomKeyStore(ctrl)

			if tc.setupStore != nil {
				tc.setupStore(store)
			}
			if tc.setupKeys != nil {
				tc.setupKeys(keyStore)
			}

			h := &Handler{
				store:        store,
				keyStore:     keyStore,
				siteID:       "site-a",
				maxBatchSize: 100,
			}

			resp, err := h.handleRoomsInfoBatch(context.Background(), tc.payload)

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}

			require.NoError(t, err)
			var batchResp model.RoomsInfoBatchResponse
			require.NoError(t, json.Unmarshal(resp, &batchResp))
			if tc.assertResp != nil {
				tc.assertResp(t, batchResp)
			}
		})
	}
}

func TestHandler_handleRoomsInfoBatch_chunking(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	keyStore := NewMockRoomKeyStore(ctrl)

	ids := make([]string, 600)
	for i := range ids {
		ids[i] = fmt.Sprintf("r%d", i)
	}
	chunk1 := ids[:500]
	chunk2 := ids[500:]

	// Mongo: single call with all IDs (no chunking)
	store.EXPECT().ListRoomsByIDs(gomock.Any(), ids).Return(nil, nil)
	// Valkey: chunked into 500 + 100
	keyStore.EXPECT().GetMany(gomock.Any(), chunk1).Return(map[string]*roomkeystore.VersionedKeyPair{}, nil)
	keyStore.EXPECT().GetMany(gomock.Any(), chunk2).Return(map[string]*roomkeystore.VersionedKeyPair{}, nil)

	h := &Handler{
		store:        store,
		keyStore:     keyStore,
		siteID:       "site-a",
		maxBatchSize: 1000,
	}

	payload := mustJSON(t, model.RoomsInfoBatchRequest{RoomIDs: ids})
	resp, err := h.handleRoomsInfoBatch(context.Background(), payload)
	require.NoError(t, err)

	var batchResp model.RoomsInfoBatchResponse
	require.NoError(t, json.Unmarshal(resp, &batchResp))
	assert.Len(t, batchResp.Rooms, 600)
	for i, ri := range batchResp.Rooms {
		assert.Equal(t, fmt.Sprintf("r%d", i), ri.RoomID)
		assert.False(t, ri.Found)
	}
}
