package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/subject"
)

// expectAllAccountsExist registers a FindExistingAccounts expectation that
// echoes its input back — i.e. "every account being asked about exists".
// Used by every add-member / create-channel happy-path test that doesn't
// specifically test the missing-user branch.
func expectAllAccountsExist(store *MockRoomStore) *gomock.Call {
	return store.EXPECT().FindExistingAccounts(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, accs []string) ([]string, error) { return accs, nil })
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
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error { publishedData = data; return nil },
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
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "charlie", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("bob", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	require.ErrorIs(t, err, errOnlyOwners)
}

func TestHandler_UpdateRole_DMRejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "dm-room", Type: model.RoomTypeDM}, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	require.ErrorIs(t, err, errRoomTypeGuard)
}

func TestHandler_UpdateRole_InvalidRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: "admin"}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	require.ErrorIs(t, err, errInvalidRole)
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
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	require.ErrorIs(t, err, errAlreadyOwner)
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
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
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
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
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
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleMember}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	require.ErrorIs(t, err, errNotOwner)
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
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "alice", NewRole: model.RoleMember}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(context.Background(), subj, data)
	require.ErrorIs(t, err, errCannotDemoteLast)
}

// --- Error-path tests ---

func TestHandler_UpdateRole_MalformedInput(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
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
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
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
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
	}
	// Payload RoomID "r-other" does not match subject RoomID "r1"
	req := model.UpdateRoleRequest{RoomID: "r-other", Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")
	_, err := h.handleUpdateRole(context.Background(), subj, data)
	if err == nil {
		t.Fatal("expected error for RoomID mismatch")
	}
	if !errors.Is(err, errRoomIDMismatch) {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHandler_UpdateRole_RequesterSubError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(nil, fmt.Errorf("db error"))
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
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
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
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
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
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
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return fmt.Errorf("nats down") },
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
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{Subscription: sub, HasIndividualMembership: true}, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 3, OwnerCount: 2}, nil)

	var publishedSubj string
	var publishedData []byte
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(ctx context.Context, subj string, data []byte, _ string) error {
		publishedSubj = subj
		publishedData = data
		return nil
	}, nil)

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
	assert.Equal(t, model.RoomTypeChannel, published.RoomType, "RoomType must be carried to room-worker")
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
			store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
			store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
				Return(&SubscriptionWithMembership{Subscription: sub, HasOrgMembership: true}, nil)
			handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
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
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{Subscription: sub}, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 2, OwnerCount: 1}, nil)

	var publishedData []byte
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(ctx context.Context, _ string, data []byte, _ string) error {
		publishedData = data
		return nil
	}, nil)
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
			store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
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
			handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
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
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{Subscription: sub, HasIndividualMembership: true}, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 1, OwnerCount: 0}, nil)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
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
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{Subscription: targetSub, HasIndividualMembership: true}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(ownerSub, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 3, OwnerCount: 1}, nil)
	var publishedData []byte
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(ctx context.Context, subj string, data []byte, _ string) error {
		publishedData = data
		return nil
	}, nil)
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
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{Subscription: targetSub, HasIndividualMembership: true}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(requesterSub, nil)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
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
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(ownerSub, nil)
	var publishedData []byte
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(ctx context.Context, subj string, data []byte, _ string) error {
		publishedData = data
		return nil
	}, nil)
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
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "bob", OrgID: "eng-org"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one")
}

func TestHandler_RemoveMember_NeitherAccountNorOrgID_Rejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	reqBody, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, reqBody)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one")
}

func TestHandler_RemoveMember_InvalidSubject(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
	_, err := handler.handleRemoveMember(context.Background(), "bogus", []byte("{}"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid remove-member subject")
}

func TestHandler_RemoveMember_InvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, []byte("{not json"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid request")
}

func TestHandler_RemoveMember_RoomIDMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	body, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r2", Account: "alice"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "room ID mismatch")
}

func TestHandler_RemoveMember_GetTargetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(nil, fmt.Errorf("db down"))
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
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
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{Subscription: targetSub, HasIndividualMembership: true}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, fmt.Errorf("db down"))
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
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
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{Subscription: sub, HasIndividualMembership: true}, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(nil, fmt.Errorf("db down"))
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	body, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "count members")
}

func TestHandler_RemoveMember_OrgPath_RequesterLookupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, fmt.Errorf("db down"))
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil)
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
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{Subscription: sub, HasIndividualMembership: true}, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 3, OwnerCount: 2}, nil)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(_ context.Context, _ string, _ []byte, _ string) error {
		return fmt.Errorf("nats down")
	}, nil)
	reqSubj := subject.MemberRemove("alice", "r1", "site-a")
	body, _ := json.Marshal(model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	_, err := handler.handleRemoveMember(context.Background(), reqSubj, body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish to stream")
}

func TestHandler_RemoveMember_RejectsNonChannelRoom(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeDM,
	}, nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			t.Fatal("publishToStream must not be called")
			return nil
		},
	}
	req := model.RemoveMemberRequest{Account: "bob"}
	data, _ := json.Marshal(req)
	_, err := h.handleRemoveMember(context.Background(),
		"chat.user.alice.request.room.r1.site-a.member.remove", data)
	if err == nil || !strings.Contains(err.Error(), "channel") {
		t.Fatalf("expected channel-type error, got %v", err)
	}
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
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
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
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
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
	expectAllAccountsExist(store)
	store.EXPECT().
		CountNewMembers(gomock.Any(), gomock.Any(), []string{"u1", "u2", "u3", "u4", "u5"}, "r1", gomock.Any()).
		Return(5, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 10,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
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

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, nil, nil, nil, "site-a", 100, 500, 5*time.Second, 5, publish, nil)

	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		Roles: []model.Role{model.RoleOwner},
	}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, Restricted: true, UserCount: 1,
	}, nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "r1", gomock.Any()).
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

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, nil, nil, nil, "site-a", 100, 500, 5*time.Second, 5, publish, nil)

	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		Roles: []model.Role{model.RoleMember},
	}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, UserCount: 5,
	}, nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "r1", gomock.Any()).
		Return(0, nil)

	req := model.AddMembersRequest{RoomID: "r1", Users: []string{"alice"}}
	reqData, _ := json.Marshal(req)

	resp, err := h.handleAddMembers(context.Background(), subject.MemberAdd("alice", "r1", "site-a"), reqData)
	require.NoError(t, err)

	var status map[string]string
	require.NoError(t, json.Unmarshal(resp, &status))
	assert.Equal(t, "accepted", status["status"])
}

func TestHandler_AddMembers_RejectsDirectBot(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:  model.SubscriptionUser{ID: "u1", Account: "alice"},
		Roles: []model.Role{model.RoleOwner},
	}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, UserCount: 1,
	}, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
	}
	body, _ := json.Marshal(model.AddMembersRequest{
		Users: []string{"weather.bot"},
	})
	_, err := h.handleAddMembers(context.Background(), subject.MemberAdd("alice", "r1", "site-a"), body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBotInChannel))
}

func TestHandler_AddMembers_SilentlyFiltersBotsFromChannelRefs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	mc := NewMockMemberListClient(ctrl)

	// requester subscription + target room.
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:  model.SubscriptionUser{ID: "u1", Account: "alice"},
		Roles: []model.Role{model.RoleOwner},
	}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, UserCount: 1,
	}, nil)

	// Channel-ref expansion: same-site source channel returns a human + a bot.
	srcRef := model.ChannelRef{RoomID: "r_src", SiteID: "site-a"}
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r_src").Return(&model.Subscription{}, nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r_src", gomock.Any(), nil, false).Return([]model.RoomMember{
		{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "bob"}},
		{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "weather.bot"}},
	}, nil)

	// CountNewMembers must be called with bob only — the bot is filtered before counting.
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(),
		[]string{"bob"}, "r1", gomock.Any()).Return(1, nil)

	var publishedPayload []byte
	h := &Handler{
		store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
			publishedPayload = data
			return nil
		},
	}

	body, _ := json.Marshal(model.AddMembersRequest{
		Users:    []string{},
		Channels: []model.ChannelRef{srcRef},
	})
	_, err := h.handleAddMembers(context.Background(), subject.MemberAdd("alice", "r1", "site-a"), body)
	require.NoError(t, err)

	var published model.AddMembersRequest
	require.NoError(t, json.Unmarshal(publishedPayload, &published))
	assert.NotContains(t, published.Users, "weather.bot",
		"bot from channel-ref must be silently filtered before publishing")
	assert.Contains(t, published.Users, "bob")
}

// expectAliceOwnerOfR1 wires up the AddMembers preflight: alice is owner of
// channel r1 with 1 existing member. Reused by every AddMembers phantom-validation case.
func expectAliceOwnerOfR1(store *MockRoomStore) {
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:  model.SubscriptionUser{ID: "u1", Account: "alice"},
		Roles: []model.Role{model.RoleOwner},
	}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, UserCount: 1,
	}, nil)
}

// errStoreFailure is a sentinel used in store-error branch tests. Distinct
// from the validators' RoomInvalidOrg/RoomUserNotFound reasons so the test can
// verify that the store error wraps cleanly without being masked by the
// reason-keyed identity check.
var errStoreFailure = errors.New("store boom")

// TestHandler_AddMembers_PhantomValidation covers the gate that converts the
// candidates pipeline's silent-drop into a synchronous reject. Cases:
//   - happy paths (no-orgs / no-users / no-channels) skip the matching validator
//   - phantom org or user (incl. partially-invalid batch) rejects with the
//     sentinel and no publish to the canonical stream
//   - store error from either validator propagates wrapped (validates the
//     coverage gap on the FindExistingOrgIDs / FindExistingAccounts error branch)
func TestHandler_AddMembers_PhantomValidation(t *testing.T) {
	tests := []struct {
		name            string
		req             model.AddMembersRequest
		setupMocks      func(store *MockRoomStore)
		wantErr         bool
		wantReason      errcode.Reason
		wantErrSentinel error
		wantPublish     bool
	}{
		{
			name: "phantom org alone rejected",
			req:  model.AddMembersRequest{Orgs: []string{"org-nope"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingOrgIDs(gomock.Any(), []string{"org-nope"}).Return(nil, nil)
			},
			wantErr: true, wantReason: errcode.RoomInvalidOrg, wantPublish: false,
		},
		{
			name: "partially invalid org rejected",
			req:  model.AddMembersRequest{Orgs: []string{"good-org", "bad-org"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingOrgIDs(gomock.Any(), gomock.InAnyOrder([]string{"good-org", "bad-org"})).
					Return([]string{"good-org"}, nil)
			},
			wantErr: true, wantReason: errcode.RoomInvalidOrg, wantPublish: false,
		},
		{
			name: "no orgs skips org validation",
			req:  model.AddMembersRequest{Users: []string{"bob"}},
			setupMocks: func(store *MockRoomStore) {
				// No FindExistingOrgIDs expectation: gomock fails if called.
				store.EXPECT().FindExistingAccounts(gomock.Any(), []string{"bob"}).Return([]string{"bob"}, nil)
				store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), []string{"bob"}, "r1", gomock.Any()).Return(1, nil)
			},
			wantErr: false, wantPublish: true,
		},
		{
			name: "phantom user alone rejected",
			req:  model.AddMembersRequest{Users: []string{"bob", "ghost"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingAccounts(gomock.Any(), gomock.InAnyOrder([]string{"bob", "ghost"})).
					Return([]string{"bob"}, nil)
			},
			wantErr: true, wantReason: errcode.RoomUserNotFound, wantPublish: false,
		},
		{
			name: "no users skips user validation",
			req:  model.AddMembersRequest{Orgs: []string{"good-org"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingOrgIDs(gomock.Any(), []string{"good-org"}).Return([]string{"good-org"}, nil)
				// No FindExistingAccounts expectation: gomock fails if called.
				store.EXPECT().CountNewMembers(gomock.Any(), []string{"good-org"}, gomock.Any(), "r1", gomock.Any()).Return(1, nil)
			},
			wantErr: false, wantPublish: true,
		},
		{
			name: "FindExistingOrgIDs store error propagates",
			req:  model.AddMembersRequest{Orgs: []string{"org-a"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingOrgIDs(gomock.Any(), []string{"org-a"}).Return(nil, errStoreFailure)
			},
			wantErr: true, wantErrSentinel: errStoreFailure, wantPublish: false,
		},
		{
			name: "FindExistingAccounts store error propagates",
			req:  model.AddMembersRequest{Users: []string{"bob"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingAccounts(gomock.Any(), []string{"bob"}).Return(nil, errStoreFailure)
			},
			wantErr: true, wantErrSentinel: errStoreFailure, wantPublish: false,
		},
		{
			// Org and account existence checks run concurrently, so BOTH the
			// users-collection reads fire even when the org is already known
			// phantom; the org error still takes priority over the account error.
			name: "both phantom — both validators run, org error wins",
			req:  model.AddMembersRequest{Orgs: []string{"org-nope"}, Users: []string{"ghost"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingOrgIDs(gomock.Any(), []string{"org-nope"}).Return(nil, nil)
				store.EXPECT().FindExistingAccounts(gomock.Any(), []string{"ghost"}).Return(nil, nil)
			},
			wantErr: true, wantReason: errcode.RoomInvalidOrg, wantPublish: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			expectAliceOwnerOfR1(store)
			tc.setupMocks(store)

			publishCalled := false
			h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
				publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
					publishCalled = true
					return nil
				},
			}
			body, _ := json.Marshal(tc.req)
			_, err := h.handleAddMembers(context.Background(), subject.MemberAdd("alice", "r1", "site-a"), body)
			if tc.wantErr {
				require.Error(t, err)
				if tc.wantReason != "" {
					assert.True(t, errcode.HasReason(err, tc.wantReason), "want reason %v, got %v", tc.wantReason, err)
				}
				if tc.wantErrSentinel != nil {
					assert.True(t, errors.Is(err, tc.wantErrSentinel), "want %v, got %v", tc.wantErrSentinel, err)
				}
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantPublish, publishCalled, "publish behavior mismatch")
		})
	}
}

// TestHandler_CreateRoomChannel_PhantomValidation is the parallel coverage for
// handleCreateRoomChannel — the same validateOrgIDs / validateAccountsExist
// gates are wired in but until now only the AddMembers path was exercised. A
// regression that dropped the gate on create-channel would have shipped silently.
func TestHandler_CreateRoomChannel_PhantomValidation(t *testing.T) {
	tests := []struct {
		name            string
		req             model.CreateRoomRequest
		setupMocks      func(store *MockRoomStore)
		wantErr         bool
		wantReason      errcode.Reason
		wantErrSentinel error
		wantPublish     bool
	}{
		{
			name: "phantom org rejected",
			req:  model.CreateRoomRequest{Name: "general", Orgs: []string{"org-nope"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingOrgIDs(gomock.Any(), []string{"org-nope"}).Return(nil, nil)
			},
			wantErr: true, wantReason: errcode.RoomInvalidOrg, wantPublish: false,
		},
		{
			name: "phantom user rejected",
			req:  model.CreateRoomRequest{Name: "general", Users: []string{"bob", "ghost"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingAccounts(gomock.Any(), gomock.InAnyOrder([]string{"bob", "ghost"})).
					Return([]string{"bob"}, nil)
			},
			wantErr: true, wantReason: errcode.RoomUserNotFound, wantPublish: false,
		},
		{
			name: "FindExistingOrgIDs store error propagates",
			req:  model.CreateRoomRequest{Name: "general", Orgs: []string{"org-a"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingOrgIDs(gomock.Any(), []string{"org-a"}).Return(nil, errStoreFailure)
			},
			wantErr: true, wantErrSentinel: errStoreFailure, wantPublish: false,
		},
		{
			name: "FindExistingAccounts store error propagates",
			req:  model.CreateRoomRequest{Name: "general", Users: []string{"bob"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingAccounts(gomock.Any(), []string{"bob"}).Return(nil, errStoreFailure)
			},
			wantErr: true, wantErrSentinel: errStoreFailure, wantPublish: false,
		},
		{
			name: "both phantom — both validators run, org error wins",
			req:  model.CreateRoomRequest{Name: "general", Orgs: []string{"org-nope"}, Users: []string{"ghost"}},
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().FindExistingOrgIDs(gomock.Any(), []string{"org-nope"}).Return(nil, nil)
				store.EXPECT().FindExistingAccounts(gomock.Any(), []string{"ghost"}).Return(nil, nil)
			},
			wantErr: true, wantReason: errcode.RoomInvalidOrg, wantPublish: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
			tc.setupMocks(store)

			publishCalled := false
			h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
				publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
					publishCalled = true
					return nil
				},
			}
			body, _ := json.Marshal(tc.req)
			_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
			if tc.wantErr {
				require.Error(t, err)
				if tc.wantReason != "" {
					assert.True(t, errcode.HasReason(err, tc.wantReason), "want reason %v, got %v", tc.wantReason, err)
				}
				if tc.wantErrSentinel != nil {
					assert.True(t, errors.Is(err, tc.wantErrSentinel), "want %v, got %v", tc.wantErrSentinel, err)
				}
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantPublish, publishCalled, "publish behavior mismatch")
		})
	}
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

	t.Run("same-site GetSubscription deadline-exceeded yields typed timeout error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch-slow", SiteID: "site-a"}
		store.EXPECT().GetSubscription(gomock.Any(), "alice", "ch-slow").
			Return(nil, context.DeadlineExceeded)

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc, memberListTimeout: 10 * time.Millisecond}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		var ee *errcode.Error
		require.ErrorAs(t, err, &ee)
		// Channel-expand timeouts surface as Unavailable with site+roomId so the
		// requester sees which channel stalled, NOT a collapsed "internal error".
		assert.Equal(t, errcode.CodeUnavailable, ee.Code)
		assert.Equal(t, "timeout listing members of channel ch-slow@site-a", ee.Message)
	})

	t.Run("cross-site member.list deadline-exceeded yields typed timeout error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockRoomStore(ctrl)
		mc := NewMockMemberListClient(ctrl)

		ch := model.ChannelRef{RoomID: "ch-remote", SiteID: "site-b"}
		mc.EXPECT().ListMembers(gomock.Any(), "alice", ch, gomock.Any()).
			Return(nil, fmt.Errorf("member.list request to site-b: %w", context.DeadlineExceeded))

		h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc, memberListTimeout: 10 * time.Millisecond}
		_, _, err := h.expandChannelRefs(context.Background(), "alice", []model.ChannelRef{ch})

		require.Error(t, err)
		var ee *errcode.Error
		require.ErrorAs(t, err, &ee)
		assert.Equal(t, errcode.CodeUnavailable, ee.Code)
		assert.Equal(t, "timeout listing members of channel ch-remote@site-b", ee.Message)
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
		wantReason  errcode.Reason
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
			name:    "empty org returns RoomInvalidOrg-reason errcode",
			subject: subj,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().ListOrgMembers(gomock.Any(), orgID).Return(nil, errcode.BadRequest(fmt.Sprintf("list org members for %q", orgID), errcode.WithReason(errcode.RoomInvalidOrg)))
			},
			want: want{wantReason: errcode.RoomInvalidOrg},
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
			if tc.want.wantReason != "" {
				require.Error(t, err)
				assert.True(t, errcode.HasReason(err, tc.want.wantReason), "want reason %v, got %v", tc.want.wantReason, err)
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
					"r1": {Version: 1, KeyPair: roomkeystore.RoomKeyPair{PrivateKey: privBytes}},
					"r3": {Version: 5, KeyPair: roomkeystore.RoomKeyPair{PrivateKey: privBytes}},
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

func TestHandler_handleUpdateRole_PropagatesRequestID(t *testing.T) {
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

	var capturedHeader nats.Header
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(ctx context.Context, _ string, _ []byte, _ string) error {
			capturedHeader = natsutil.HeaderForContext(ctx)
			return nil
		},
	}

	ctx := natsutil.WithRequestID(context.Background(), "req-room-svc-test")
	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	data, _ := json.Marshal(req)
	subj := subject.MemberRoleUpdate("alice", "r1", "site-a")

	_, err := h.handleUpdateRole(ctx, subj, data)
	require.NoError(t, err)
	require.NotNil(t, capturedHeader, "publish wrapper must build header from ctx")
	assert.Equal(t, "req-room-svc-test", capturedHeader.Get(natsutil.RequestIDHeader))
}

func TestWrappedCtx_PropagatesValidUUIDFromHeader(t *testing.T) {
	const inbound = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	rawMsg := &nats.Msg{
		Subject: "chat.room.test",
		Data:    []byte("ignored"),
		Header:  nats.Header{natsutil.RequestIDHeader: []string{inbound}},
	}
	m := otelnats.Msg{Msg: rawMsg, Ctx: context.Background()}

	got, err := wrappedCtx(m)

	require.NoError(t, err)
	assert.Equal(t, inbound, natsutil.RequestIDFromContext(got),
		"valid inbound UUID must pass through unchanged")
}

// room-service handlers feed dedup-critical paths in room-worker
// (OutboxDedupID, messageDedupSeed, idgen.MessageIDFromRequestID) where a
// server-side mint would break client-retry dedup. wrappedCtx therefore uses
// the strict natsutil.RequireRequestID and surfaces an errcode.BadRequest when
// the inbound header is missing or malformed.
func TestWrappedCtx_MalformedHeaderRejects(t *testing.T) {
	rawMsg := &nats.Msg{
		Subject: "chat.room.test",
		Data:    []byte("ignored"),
		Header:  nats.Header{natsutil.RequestIDHeader: []string{"not-a-uuid"}},
	}
	m := otelnats.Msg{Msg: rawMsg, Ctx: context.Background()}

	_, err := wrappedCtx(m)

	require.Error(t, err)
	var ec *errcode.Error
	require.True(t, errors.As(err, &ec))
	assert.Equal(t, errcode.CodeBadRequest, ec.Code)
}

func TestWrappedCtx_NoHeaderRejects(t *testing.T) {
	rawMsg := &nats.Msg{
		Subject: "chat.room.test",
		Data:    []byte("ignored"),
		Header:  nats.Header{},
	}
	m := otelnats.Msg{Msg: rawMsg, Ctx: context.Background()}

	_, err := wrappedCtx(m)

	require.Error(t, err)
	var ec *errcode.Error
	require.True(t, errors.As(err, &ec))
	assert.Equal(t, errcode.CodeBadRequest, ec.Code)
}

// --- Phase 5c: handleCreateRoom (3-arg) tests ---

// ctxWithReqID returns a context carrying a valid UUIDv7 request ID.
func ctxWithReqID() context.Context {
	return natsutil.WithRequestID(context.Background(), idgen.GenerateRequestID())
}

// createRoomSubj builds the standard 7-token room.create subject for account/site.
func createRoomSubj(account, siteID string) string {
	return subject.RoomCreate(account, siteID)
}

// aliceUser is a helper that returns a fully populated User for "alice".
func aliceUser() *model.User {
	return &model.User{ID: "u-alice", Account: "alice", EngName: "Alice Wang", ChineseName: "愛麗絲"}
}

// bobUser is a helper that returns a fully populated User for "bob".
func bobUser() *model.User {
	return &model.User{ID: "u-bob", Account: "bob", EngName: "Bob Chen", ChineseName: "陳博"}
}

// botUser is a helper that returns a User for a bot account.
func botUser() *model.User {
	return &model.User{ID: "u-bot", Account: "helper.bot", EngName: "Helper Bot", ChineseName: "助理機器人"}
}

func TestHandleCreateRoom_InvalidSubject(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"bob"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), "bad.subject", body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid create-room subject")
}

// Boundary-level reject behavior is tested via wrappedCtx (above); the
// helper itself is unit-tested in pkg/natsutil.RequireRequestID. The
// dedup-critical paths fanned out from room-service make server-side minting
// unsafe — see docs/error-handling.md §3a.

func TestHandleCreateRoom_EmptyPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	// GetUser is NOT called for an empty request — the empty-check fires first.
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errEmptyCreateRequest))
}

func TestHandleCreateRoom_RequesterNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(nil, ErrUserNotFound)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"bob"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.True(t, errcode.HasReason(err, errcode.RoomUserNotFound), "want RoomUserNotFound, got %v", err)
}

func TestHandleCreateRoom_RequesterMissingNameFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{ID: "u-alice", Account: "alice"}, nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"bob"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errInvalidUserData))
}

func TestHandleCreateRoom_SelfDMRejected(t *testing.T) {
	// Self-DM is caught by classifyAndValidate before any DB lookup, so no GetUser
	// expectation is set — the gomock controller would flag the unexpected call
	// if validation regressed.
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"alice"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errSelfDM))
}

func TestHandleCreateRoom_DM_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	store.EXPECT().GetUser(gomock.Any(), "bob").Return(bobUser(), nil)
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "bob").Return(nil, model.ErrSubscriptionNotFound)

	var publishedData []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
			publishedData = data
			return nil
		},
	}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"bob"}})
	resp, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.NoError(t, err)
	require.NotNil(t, publishedData)

	var reply model.CreateRoomReply
	require.NoError(t, json.Unmarshal(resp, &reply))
	assert.Equal(t, model.CreateRoomReplyAccepted, reply.Status)
	assert.Equal(t, string(model.RoomTypeDM), reply.RoomType)
	assert.Equal(t, idgen.BuildDMRoomID("u-alice", "u-bob"), reply.RoomID)
}

func TestHandleCreateRoom_DM_AlreadyExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	store.EXPECT().GetUser(gomock.Any(), "bob").Return(bobUser(), nil)
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "bob").Return(
		&model.Subscription{RoomID: "existing-dm-room"}, nil,
	)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"bob"}})
	resp, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.NoError(t, err)

	var reply model.CreateRoomReply
	require.NoError(t, json.Unmarshal(resp, &reply))
	assert.Equal(t, model.CreateRoomStatusExists, reply.Status)
	assert.Equal(t, "existing-dm-room", reply.RoomID)
}

func TestHandleCreateRoom_BotDM_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	store.EXPECT().GetUser(gomock.Any(), "helper.bot").Return(botUser(), nil)
	store.EXPECT().GetApp(gomock.Any(), "helper.bot").Return(&model.App{
		Name:      "Helper",
		Assistant: &model.AppAssistant{Enabled: true},
	}, nil)
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "helper.bot").Return(nil, model.ErrSubscriptionNotFound)

	var publishedData []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
			publishedData = data
			return nil
		},
	}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"helper.bot"}})
	resp, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.NoError(t, err)
	require.NotNil(t, publishedData)

	var reply model.CreateRoomReply
	require.NoError(t, json.Unmarshal(resp, &reply))
	assert.Equal(t, model.CreateRoomReplyAccepted, reply.Status)
	assert.Equal(t, string(model.RoomTypeBotDM), reply.RoomType)

	var published model.CreateRoomRequest
	require.NoError(t, json.Unmarshal(publishedData, &published))
	assert.Equal(t, []string{"helper.bot"}, published.Users)
}

func TestHandleCreateRoom_BotDM_AppCounterpartNoNameFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	// Bot user record exists but has empty EngName/ChineseName — common
	// for app/bot accounts in the users collection.
	store.EXPECT().GetUser(gomock.Any(), "weather.bot").Return(&model.User{
		ID: "u_wbot", Account: "weather.bot", SiteID: "site-a",
	}, nil)
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "weather.bot").
		Return(nil, model.ErrSubscriptionNotFound)
	store.EXPECT().GetApp(gomock.Any(), "weather.bot").Return(&model.App{
		ID:        "a_wbot",
		Name:      "Weather Bot",
		Assistant: &model.AppAssistant{Enabled: true, Name: "weather.bot"},
	}, nil)

	var published bool
	h := &Handler{
		store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			published = true
			return nil
		},
	}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"weather.bot"}})
	resp, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.NoError(t, err)
	assert.True(t, published, "canonical event must publish for botDM with bot lacking name fields")
	assert.Contains(t, string(resp), `"roomType":"botDM"`)
}

func TestHandleCreateRoom_BotDM_Disabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	store.EXPECT().GetUser(gomock.Any(), "helper.bot").Return(botUser(), nil)
	// Dedup runs FIRST so an existing botDM with a now-disabled bot still
	// resolves to the existing roomId. This case: no existing sub → fall
	// through to the bot-availability check.
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "helper.bot").Return(nil, model.ErrSubscriptionNotFound)
	store.EXPECT().GetApp(gomock.Any(), "helper.bot").Return(&model.App{
		Name:      "Helper",
		Assistant: &model.AppAssistant{Enabled: false},
	}, nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"helper.bot"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBotNotAvailable))
}

// New: existing botDM where the bot was later disabled MUST still return the
// existing roomId via the success "exists" reply, not errBotNotAvailable. This
// is the idempotent open-or-create contract.
func TestHandleCreateRoom_BotDM_DisabledButExisting(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	store.EXPECT().GetUser(gomock.Any(), "helper.bot").Return(botUser(), nil)
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "helper.bot").
		Return(&model.Subscription{RoomID: "existing-bot-dm"}, nil)
	// GetApp must NOT be called when an existing DM is found.
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"helper.bot"}})
	resp, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.NoError(t, err)

	var reply model.CreateRoomReply
	require.NoError(t, json.Unmarshal(resp, &reply))
	assert.Equal(t, model.CreateRoomStatusExists, reply.Status)
	assert.Equal(t, "existing-bot-dm", reply.RoomID)
}

func TestHandleCreateRoom_Channel_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "", gomock.Any()).Return(2, nil)

	var publishedData []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
			publishedData = data
			return nil
		},
	}

	body, _ := json.Marshal(model.CreateRoomRequest{Name: "general", Users: []string{"bob", "charlie"}})
	resp, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.NoError(t, err)
	require.NotNil(t, publishedData)

	var reply model.CreateRoomReply
	require.NoError(t, json.Unmarshal(resp, &reply))
	assert.Equal(t, model.CreateRoomReplyAccepted, reply.Status)
	assert.Equal(t, string(model.RoomTypeChannel), reply.RoomType)
	assert.NotEmpty(t, reply.RoomID)
}

func TestHandleCreateRoom_Channel_NameRequired(t *testing.T) {
	// Channels must carry a client-supplied Name. Multiple users with no Name
	// shape-classifies as channel; classifyAndValidate must reject before any
	// store call. No GetUser expectation: the controller fails if it leaks.
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"bob", "charlie"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errChannelNameRequired), "expected errChannelNameRequired, got %v", err)
}

func TestHandleCreateRoom_Channel_NameWhitespaceOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Name: "   ", Users: []string{"bob", "charlie"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errChannelNameRequired))
}

func TestHandleCreateRoom_Channel_NameTooLong(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Name: strings.Repeat("a", 101), Users: []string{"bob"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errChannelNameTooLong))
}

func TestHandleCreateRoom_Channel_NameAtBoundary(t *testing.T) {
	// Exactly 100 runes is allowed.
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "", gomock.Any()).Return(2, nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
	}

	body, _ := json.Marshal(model.CreateRoomRequest{Name: strings.Repeat("世", 100), Users: []string{"bob"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.NoError(t, err)
}

func TestHandleCreateRoom_Channel_BotRejected(t *testing.T) {
	// Bot detection is part of the input-only stage — no DB call required.
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Name: "general", Users: []string{"helper.bot"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBotInChannel))
}

func TestHandleCreateRoom_Channel_ExceedsCapacity(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "", gomock.Any()).Return(11, nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 10}

	body, _ := json.Marshal(model.CreateRoomRequest{Name: "big-room", Users: []string{"bob"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum capacity")
}

// A referenced channel can expand to just the creator; reject as empty since
// channels with only the owner are not allowed.
func TestHandleCreateRoom_Channel_ChannelRefsExpandToCreatorOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	expectAllAccountsExist(store)
	// expandChannelRefs would resolve a same-site channel-ref where the only
	// member is alice — after stripping the requester, allUsers/allOrgs are
	// empty for the count call, returning 0.
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "", gomock.Any()).Return(0, nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 10}

	body, _ := json.Marshal(model.CreateRoomRequest{Name: "solo", Users: []string{"bob"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errEmptyCreateRequest))
}

// Boundary case: with maxRoomSize=10, CountNewMembers=10 means the materialized
// room would have 11 members (10 invitees + creator). Capacity check must reject.
func TestHandleCreateRoom_Channel_RejectsWhenCreatorWouldOverflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "", gomock.Any()).Return(10, nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 10}

	body, _ := json.Marshal(model.CreateRoomRequest{Name: "edge", Users: []string{"bob"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum capacity")
	assert.Contains(t, err.Error(), "11 members")
}

// At maxRoomSize-1 invitees the creator-inclusive total equals the cap → accepted.
func TestHandleCreateRoom_Channel_AcceptsAtCreatorInclusiveCap(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "", gomock.Any()).Return(9, nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 10,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil }}

	body, _ := json.Marshal(model.CreateRoomRequest{Name: "edge", Users: []string{"bob"}})
	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.NoError(t, err)
}

// Malformed JSON must surface as a sanitized "invalid request" error, not panic.
func TestHandleCreateRoom_MalformedJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	_, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), []byte("{not json"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid request")
}

// determineRoomType / handleCreateRoom must classify "p_" webhook bots as botDM.
func TestHandleCreateRoom_BotDM_PUnderscoreWebhookBot(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	store.EXPECT().GetUser(gomock.Any(), "p_webhook").Return(&model.User{
		ID: "u_p", Account: "p_webhook", EngName: "Webhook", ChineseName: "网钩",
	}, nil)
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "p_webhook").
		Return(nil, model.ErrSubscriptionNotFound)
	store.EXPECT().GetApp(gomock.Any(), "p_webhook").Return(&model.App{
		Name: "Webhook", Assistant: &model.AppAssistant{Enabled: true},
	}, nil)
	var publishedData []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
			publishedData = data
			return nil
		},
	}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"p_webhook"}})
	resp, err := h.handleCreateRoom(ctxWithReqID(), createRoomSubj("alice", "site-a"), body)
	require.NoError(t, err)

	var reply model.CreateRoomReply
	require.NoError(t, json.Unmarshal(resp, &reply))
	assert.Equal(t, string(model.RoomTypeBotDM), reply.RoomType, "p_ webhook account must classify as botDM")
	assert.NotNil(t, publishedData)
}

// --- Phase 5c: natsCreateRoom adapter tests ---

func TestNatsCreateRoom_DMExistsReply(t *testing.T) {
	// DM-exists is now a SUCCESS reply: {status:"exists", roomId:…}, not an error.
	body, err := json.Marshal(model.CreateRoomReply{Status: model.CreateRoomStatusExists, RoomID: "existing-dm"})
	require.NoError(t, err)

	var reply model.CreateRoomReply
	require.NoError(t, json.Unmarshal(body, &reply))
	assert.Equal(t, model.CreateRoomStatusExists, reply.Status)
	assert.Equal(t, "existing-dm", reply.RoomID)
	assert.Contains(t, string(body), `"roomId":"existing-dm"`)
}

func TestNatsCreateRoom_DMExistsSuccess_FlowTriggered(t *testing.T) {
	// Verify handleCreateRoom returns a SUCCESS "exists" reply (not an error)
	// when FindDMSubscription returns an existing subscription.
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	store.EXPECT().GetUser(gomock.Any(), "bob").Return(bobUser(), nil)
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "bob").Return(
		&model.Subscription{RoomID: "existing-dm"}, nil,
	)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	reqBody, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"bob"}})
	resp, err := h.handleCreateRoom(
		natsutil.WithRequestID(context.Background(), idgen.GenerateRequestID()),
		createRoomSubj("alice", "site-a"),
		reqBody,
	)
	require.NoError(t, err)

	var reply model.CreateRoomReply
	require.NoError(t, json.Unmarshal(resp, &reply))
	assert.Equal(t, model.CreateRoomStatusExists, reply.Status)
	assert.Equal(t, "existing-dm", reply.RoomID)
}

func TestNatsCreateRoom_GenericErrorReply(t *testing.T) {
	// A bare DB error collapses to internal at the reply boundary (Classify).
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(nil, fmt.Errorf("mongo connection refused"))
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	body, _ := json.Marshal(model.CreateRoomRequest{Users: []string{"bob"}})
	_, err := h.handleCreateRoom(
		natsutil.WithRequestID(context.Background(), idgen.GenerateRequestID()),
		createRoomSubj("alice", "site-a"),
		body,
	)
	require.Error(t, err)
	// Not a typed *errcode.Error — Classify will collapse it to internal.
	var ee *errcode.Error
	assert.False(t, errors.As(err, &ee), "bare DB error must not be a typed errcode")
}

// --- message.read tests ---

type messageReadFixture struct {
	store          *MockRoomStore
	publishedSubj  string
	publishedData  []byte
	publishCallErr error
	publishCalls   int
	handler        *Handler
}

func newMessageReadFixture(t *testing.T) *messageReadFixture {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	f := &messageReadFixture{store: store}
	f.handler = &Handler{
		store:  store,
		siteID: "site-a",
		publishToStream: func(_ context.Context, subj string, data []byte, _ string) error {
			f.publishCalls++
			f.publishedSubj = subj
			f.publishedData = data
			return f.publishCallErr
		},
	}
	return f
}

func TestHandler_MessageRead_InvalidSubject(t *testing.T) {
	f := newMessageReadFixture(t)
	_, err := f.handler.handleMessageRead(context.Background(), "garbage", nil)
	require.Error(t, err)
}

func TestHandler_MessageRead_NotMember(t *testing.T) {
	f := newMessageReadFixture(t)
	f.store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, model.ErrSubscriptionNotFound)
	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.ErrorIs(t, err, errNotRoomMember)
}

func TestHandler_MessageRead_HappyLocal_AlertClears(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
		Alert: true, ThreadUnread: nil,
	}, nil)
	f.store.EXPECT().
		UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	minT := lastSeen
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&minT, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &minT).Return(nil)

	subj := subject.MessageRead("alice", "r1", "site-a")
	resp, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.NoError(t, err)

	var got map[string]string
	require.NoError(t, json.Unmarshal(resp, &got))
	assert.Equal(t, "accepted", got["status"])
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageRead_AlertStaysTrueWithThreadUnread(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
		Alert: true, ThreadUnread: []string{"t1"},
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), true).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &lastSeen).Return(nil)

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.NoError(t, err)
}

// LastSeenAt nil means the user has never read the room (e.g. they were just
// invited). The handler must NOT fall back to JoinedAt for the "already past
// content" check — being invited isn't reading. So even if JoinedAt happens
// to sit beyond LastMsgAt, the recompute must still run.
func TestHandler_MessageRead_LastSeenNil_RecomputesAnyway(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(time.Hour) // joined "in the future" relative to lastMsg
	lastMsg := time.Now().UTC().Add(-time.Hour)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, // LastSeenAt nil
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	// Recompute MUST run (no JoinedAt fallback for the early-return). The stored
	// floor is already nil, so the recomputed nil matches it and the redundant
	// write is skipped.
	var nilTime *time.Time
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(nilTime, nil)

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.NoError(t, err)
}

func TestHandler_MessageRead_RoomLastMsgNil_EarlyReturn(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-time.Hour)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", JoinedAt: joined,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: nil}, nil)

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.NoError(t, err)
}

func TestHandler_MessageRead_CrossSite_PublishesOutbox(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
		Alert: true, ThreadUnread: []string{"t1"},
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), true).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &lastSeen).Return(nil)

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, f.publishCalls)
	assert.Equal(t, "outbox.site-a.to.site-b.subscription_read", f.publishedSubj)

	var outbox model.OutboxEvent
	require.NoError(t, json.Unmarshal(f.publishedData, &outbox))
	assert.Equal(t, model.OutboxSubscriptionRead, outbox.Type)
	assert.Equal(t, "site-a", outbox.SiteID)
	assert.Equal(t, "site-b", outbox.DestSiteID)

	var inner model.SubscriptionReadEvent
	require.NoError(t, json.Unmarshal(outbox.Payload, &inner))
	assert.Equal(t, "alice", inner.Account)
	assert.Equal(t, "r1", inner.RoomID)
	assert.True(t, inner.Alert)
	assert.Greater(t, inner.LastSeenAt, int64(0))
}

func TestHandler_MessageRead_CrossSite_PublishFailureAborts(t *testing.T) {
	f := newMessageReadFixture(t)
	f.publishCallErr = fmt.Errorf("nats down")
	joined := time.Now().UTC().Add(-time.Hour)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", JoinedAt: joined,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)
	// GetRoom may run concurrently with GetUserSiteID via errgroup; allow it.
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1"}, nil).AnyTimes()
	// MinSubscriptionLastSeenByRoomID / UpdateRoomMinUserLastSeenAt must NOT run
	// after the publish failure.

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.Error(t, err)
}

func TestHandler_MessageRead_GetUserSiteIDEmpty_NoPublish(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &lastSeen).Return(nil)

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageRead_GetUserSiteIDError_Aborts(t *testing.T) {
	f := newMessageReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: time.Now().UTC().Add(-time.Hour),
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("", errors.New("mongo down"))
	// GetRoom may run concurrently via errgroup; allow it.
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1"}, nil).AnyTimes()

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageRead_MinNil_ClearsRoomField(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	// Room currently carries a non-nil floor; recompute returns nil, so the field
	// is genuinely cleared via UpdateRoomMinUserLastSeenAt(nil).
	storedFloor := lastSeen
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg, MinUserLastSeenAt: &storedFloor}, nil)
	var nilTime *time.Time
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(nilTime, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", nilTime).Return(nil)

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.NoError(t, err)
}

func TestHandler_MessageRead_UpdateSubscriptionReadError(t *testing.T) {
	f := newMessageReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: time.Now().UTC().Add(-time.Hour),
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).
		Return(errors.New("mongo down"))

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.Error(t, err)
}

func TestHandler_MessageRead_GetRoomError(t *testing.T) {
	f := newMessageReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: time.Now().UTC().Add(-time.Hour),
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, errors.New("mongo down"))

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.Error(t, err)
}

func TestHandler_MessageRead_MinSubscriptionError(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(nil, errors.New("agg failed"))

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.Error(t, err)
}

func TestHandler_MessageRead_UpdateRoomMinError(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", gomock.Any()).Return(errors.New("mongo down"))

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.Error(t, err)
}

// Solution 2: when the recomputed floor equals the value already stored on the
// room, the write is redundant and must be skipped — this avoids a no-op Mongo
// round trip and the write-intent lock on the hot rooms document on every read.
func TestHandler_MessageRead_FloorUnchanged_SkipsWrite(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	// stored and computed floors carry the same instant via distinct pointers,
	// so a correct implementation must compare by value, not pointer identity.
	storedFloor := lastSeen
	computedFloor := lastSeen
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", LastMsgAt: &lastMsg, MinUserLastSeenAt: &storedFloor,
	}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&computedFloor, nil)
	// UpdateRoomMinUserLastSeenAt must NOT run — the floor is unchanged.

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.NoError(t, err)
}

// When both the stored floor and the recomputed floor are nil (the common case
// for an active room where at least one member has never read), the write is a
// no-op $unset on an already-absent field and must be skipped.
func TestHandler_MessageRead_FloorNilStoredNil_SkipsWrite(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	var nilTime *time.Time
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(nilTime, nil)
	// UpdateRoomMinUserLastSeenAt must NOT run — stored nil matches computed nil.

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.NoError(t, err)
}

// When the floor genuinely changes (stored value differs from the recomputed
// minimum), the write must still run.
func TestHandler_MessageRead_FloorChanged_Writes(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-3 * time.Hour)
	oldFloor := joined.Add(time.Hour)
	newFloor := oldFloor.Add(30 * time.Minute)
	lastMsg := newFloor.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &oldFloor,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", LastMsgAt: &lastMsg, MinUserLastSeenAt: &oldFloor,
	}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&newFloor, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &newFloor).Return(nil)

	subj := subject.MessageRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageRead(context.Background(), subj, nil)
	require.NoError(t, err)
}

func TestHandler_handleMessageReadReceipt(t *testing.T) {
	const (
		siteID    = "site-a"
		account   = "alice"
		roomID    = "r1"
		messageID = "m1"
	)
	createdAt := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	subj := subject.MessageReadReceipt(account, roomID, siteID)
	body := mustJSON(t, model.ReadReceiptRequest{MessageID: messageID})

	type setup struct {
		store  *MockRoomStore
		reader *MockMessageReader
	}
	type tc struct {
		name      string
		subject   string
		body      []byte
		prep      func(s setup)
		wantErr   error
		wantSubst string
		wantReply *model.ReadReceiptResponse
	}

	tests := []tc{
		{
			name:    "happy path",
			subject: subj,
			body:    body,
			prep: func(s setup) {
				s.store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				s.reader.EXPECT().GetMessageRoomAndCreatedAt(gomock.Any(), messageID).
					Return(roomID, createdAt, account, true, nil)
				s.store.EXPECT().ListReadReceipts(gomock.Any(), roomID, createdAt, account, gomock.Any()).
					Return([]ReadReceiptRow{
						{UserID: "uB", Account: "bob", ChineseName: "鮑勃", EngName: "Bob"},
					}, nil)
			},
			wantReply: &model.ReadReceiptResponse{
				Readers: []model.ReadReceiptEntry{
					{UserID: "uB", Account: "bob", ChineseName: "鮑勃", EngName: "Bob"},
				},
			},
		},
		{
			name:    "empty readers",
			subject: subj,
			body:    body,
			prep: func(s setup) {
				s.store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				s.reader.EXPECT().GetMessageRoomAndCreatedAt(gomock.Any(), messageID).
					Return(roomID, createdAt, account, true, nil)
				s.store.EXPECT().ListReadReceipts(gomock.Any(), roomID, createdAt, account, gomock.Any()).
					Return([]ReadReceiptRow{}, nil)
			},
			wantReply: &model.ReadReceiptResponse{Readers: []model.ReadReceiptEntry{}},
		},
		{
			name:      "invalid subject",
			subject:   "garbage",
			body:      body,
			wantSubst: "invalid",
		},
		{
			name:      "empty messageID",
			subject:   subj,
			body:      mustJSON(t, model.ReadReceiptRequest{}),
			wantSubst: "messageId",
		},
		{
			name:    "not a room member",
			subject: subj,
			body:    body,
			prep: func(s setup) {
				s.store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(nil, model.ErrSubscriptionNotFound)
				s.reader.EXPECT().GetMessageRoomAndCreatedAt(gomock.Any(), messageID).
					Return(roomID, createdAt, account, true, nil).AnyTimes()
			},
			wantErr: errNotRoomMember,
		},
		{
			name:    "message not found",
			subject: subj,
			body:    body,
			prep: func(s setup) {
				s.store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				s.reader.EXPECT().GetMessageRoomAndCreatedAt(gomock.Any(), messageID).
					Return("", time.Time{}, "", false, nil)
			},
			wantErr: errMessageNotFound,
		},
		{
			name:    "message in another room",
			subject: subj,
			body:    body,
			prep: func(s setup) {
				s.store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				s.reader.EXPECT().GetMessageRoomAndCreatedAt(gomock.Any(), messageID).
					Return("other-room", createdAt, account, true, nil)
			},
			wantErr: errMessageRoomMismatch,
		},
		{
			name:    "not the sender",
			subject: subj,
			body:    body,
			prep: func(s setup) {
				s.store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				s.reader.EXPECT().GetMessageRoomAndCreatedAt(gomock.Any(), messageID).
					Return(roomID, createdAt, "bob", true, nil)
			},
			wantErr: errNotMessageSender,
		},
		{
			name:    "store error on subscription",
			subject: subj,
			body:    body,
			prep: func(s setup) {
				s.store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(nil, fmt.Errorf("db down"))
				s.reader.EXPECT().GetMessageRoomAndCreatedAt(gomock.Any(), messageID).
					Return(roomID, createdAt, account, true, nil).AnyTimes()
			},
			wantSubst: "db down",
		},
		{
			name:    "store error on message lookup",
			subject: subj,
			body:    body,
			prep: func(s setup) {
				s.store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				s.reader.EXPECT().GetMessageRoomAndCreatedAt(gomock.Any(), messageID).
					Return("", time.Time{}, "", false, fmt.Errorf("cass down"))
			},
			wantSubst: "cass down",
		},
		{
			name:    "store error on aggregation",
			subject: subj,
			body:    body,
			prep: func(s setup) {
				s.store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				s.reader.EXPECT().GetMessageRoomAndCreatedAt(gomock.Any(), messageID).
					Return(roomID, createdAt, account, true, nil)
				s.store.EXPECT().ListReadReceipts(gomock.Any(), roomID, createdAt, account, gomock.Any()).
					Return(nil, fmt.Errorf("agg failed"))
			},
			wantSubst: "agg failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			store := NewMockRoomStore(ctrl)
			reader := NewMockMessageReader(ctrl)
			if tt.prep != nil {
				tt.prep(setup{store: store, reader: reader})
			}

			h := NewHandler(store, nil, nil, reader, siteID, 1000, 1000, time.Second, 5, nil, nil)
			gotBytes, err := h.handleMessageReadReceipt(context.Background(), tt.subject, tt.body)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			if tt.wantSubst != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantSubst)
				return
			}
			require.NoError(t, err)
			var got model.ReadReceiptResponse
			require.NoError(t, json.Unmarshal(gotBytes, &got))
			require.Equal(t, *tt.wantReply, got)
		})
	}
}

func TestHandler_CreateRoom_WritesKeyBeforePublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	keyStore := NewMockRoomKeyStore(ctrl)

	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "", "alice").
		Return(1, nil)

	var keyStored bool
	var publishCalls int
	keyStore.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, roomID string, pair roomkeystore.RoomKeyPair) (int, error) {
			assert.NotEmpty(t, roomID)
			assert.Len(t, pair.PrivateKey, 32)
			keyStored = true
			return 0, nil
		})

	publish := func(_ context.Context, subj string, _ []byte, _ string) error {
		// Write-before-publish invariant: room-worker reads the key on canonical
		// arrival, so Set must complete before the create event is published.
		assert.True(t, keyStored, "keyStore.Set must run before publishToStream")
		publishCalls++
		assert.Equal(t, "chat.room.canonical.site-a.create", subj)
		return nil
	}

	h := &Handler{store: store, keyStore: keyStore, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: publish}

	req := model.CreateRoomRequest{Name: "general", Users: []string{"bob"}}
	data, _ := json.Marshal(req)
	_, err := h.handleCreateRoom(ctxWithReqID(),
		"chat.user.alice.request.room.site-a.create", data)
	require.NoError(t, err)
	assert.Equal(t, 1, publishCalls)
}

func TestHandler_CreateRoom_AbortsOnKeyStoreSetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	keyStore := NewMockRoomKeyStore(ctrl)

	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "", "alice").
		Return(1, nil)
	keyStore.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(0, fmt.Errorf("valkey down"))

	h := &Handler{store: store, keyStore: keyStore, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			t.Fatal("publishToStream must not be called when Set fails")
			return nil
		},
	}

	req := model.CreateRoomRequest{Name: "general", Users: []string{"bob"}}
	data, _ := json.Marshal(req)
	_, err := h.handleCreateRoom(ctxWithReqID(),
		"chat.user.alice.request.room.site-a.create", data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "store room key")
}

func TestHandler_EnsureRoomKey_KeyExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	keyStore := NewMockRoomKeyStore(ctrl)

	existing := &roomkeystore.VersionedKeyPair{
		Version: 7,
		KeyPair: roomkeystore.RoomKeyPair{
			PrivateKey: bytes.Repeat([]byte{0xCD}, 32),
		},
	}
	keyStore.EXPECT().Get(gomock.Any(), "room-abc").Return(existing, nil)

	h := &Handler{keyStore: keyStore, siteID: "site-local"}
	req := model.RoomKeyEnsureRequest{RoomID: "room-abc"}
	data, _ := json.Marshal(req)

	resp, err := h.handleEnsureRoomKey(context.Background(), data)
	require.NoError(t, err)

	var result model.RoomKeyEnsureResponse
	require.NoError(t, json.Unmarshal(resp, &result))
	assert.Equal(t, "room-abc", result.RoomID)
	assert.Equal(t, 7, result.Version)

	assert.NotContains(t, string(resp), "publicKey", "response must not include public key bytes")
	assert.NotContains(t, string(resp), "privateKey", "response must not include private key bytes")
}

func TestHandler_EnsureRoomKey_KeyNotFound_SetsNew(t *testing.T) {
	ctrl := gomock.NewController(t)
	keyStore := NewMockRoomKeyStore(ctrl)

	keyStore.EXPECT().Get(gomock.Any(), "room-new").Return(nil, nil)

	var capturedPair roomkeystore.RoomKeyPair
	keyStore.EXPECT().
		Set(gomock.Any(), "room-new", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, p roomkeystore.RoomKeyPair) (int, error) {
			capturedPair = p
			return 0, nil
		})

	h := &Handler{keyStore: keyStore, siteID: "site-local"}
	req := model.RoomKeyEnsureRequest{RoomID: "room-new"}
	data, _ := json.Marshal(req)

	resp, err := h.handleEnsureRoomKey(context.Background(), data)
	require.NoError(t, err)

	var result model.RoomKeyEnsureResponse
	require.NoError(t, json.Unmarshal(resp, &result))
	assert.Equal(t, "room-new", result.RoomID)
	assert.Equal(t, 0, result.Version)

	assert.Len(t, capturedPair.PrivateKey, 32, "room secret must be 32 bytes — stored in Valkey")
	assert.NotContains(t, string(resp), "publicKey", "response must not include public key bytes")
	assert.NotContains(t, string(resp), "privateKey", "response must not include private key bytes")
}

func TestHandler_EnsureRoomKey_MalformedRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	keyStore := NewMockRoomKeyStore(ctrl)
	h := &Handler{keyStore: keyStore, siteID: "site-local"}

	_, err := h.handleEnsureRoomKey(context.Background(), []byte("{not json"))
	require.Error(t, err)
}

func TestHandler_EnsureRoomKey_MissingRoomID(t *testing.T) {
	ctrl := gomock.NewController(t)
	keyStore := NewMockRoomKeyStore(ctrl)
	h := &Handler{keyStore: keyStore, siteID: "site-local"}

	data, _ := json.Marshal(model.RoomKeyEnsureRequest{RoomID: ""})
	_, err := h.handleEnsureRoomKey(context.Background(), data)
	require.Error(t, err)
}

func TestHandler_EnsureRoomKey_GetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	keyStore := NewMockRoomKeyStore(ctrl)
	keyStore.EXPECT().Get(gomock.Any(), "room-err").Return(nil, errors.New("valkey down"))

	h := &Handler{keyStore: keyStore, siteID: "site-local"}
	data, _ := json.Marshal(model.RoomKeyEnsureRequest{RoomID: "room-err"})

	_, err := h.handleEnsureRoomKey(context.Background(), data)
	require.Error(t, err)
}

func TestHandler_EnsureRoomKey_SetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	keyStore := NewMockRoomKeyStore(ctrl)
	keyStore.EXPECT().Get(gomock.Any(), "room-setfail").Return(nil, nil)
	keyStore.EXPECT().Set(gomock.Any(), "room-setfail", gomock.Any()).Return(0, errors.New("write failed"))

	h := &Handler{keyStore: keyStore, siteID: "site-local"}
	data, _ := json.Marshal(model.RoomKeyEnsureRequest{RoomID: "room-setfail"})

	_, err := h.handleEnsureRoomKey(context.Background(), data)
	require.Error(t, err)
}

func TestHandler_EnsureRoomKey_NilKeyStore(t *testing.T) {
	h := &Handler{keyStore: nil, siteID: "site-local"}
	data, _ := json.Marshal(model.RoomKeyEnsureRequest{RoomID: "room-abc"})

	_, err := h.handleEnsureRoomKey(context.Background(), data)
	require.Error(t, err)
}

// ===== message.thread.read tests =====

type threadReadFixture struct {
	store          *MockRoomStore
	handler        *Handler
	publishCalls   int
	publishedSubj  string
	publishedData  []byte
	publishCallErr error
}

func newThreadReadFixture(t *testing.T) *threadReadFixture {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	f := &threadReadFixture{store: store}
	f.handler = &Handler{
		store:  store,
		siteID: "site-a",
		publishToStream: func(_ context.Context, subj string, data []byte, _ string) error {
			f.publishCalls++
			f.publishedSubj = subj
			f.publishedData = data
			return f.publishCallErr
		},
	}
	return f
}

func threadReadBody(t *testing.T, threadID string) []byte {
	t.Helper()
	b, err := json.Marshal(model.MessageThreadReadRequest{ThreadID: threadID})
	require.NoError(t, err)
	return b
}

func baseThreadSub(account, roomID, parent, threadRoomID string) *model.ThreadSubscription {
	return &model.ThreadSubscription{
		ID:              "tsub-" + parent,
		ParentMessageID: parent,
		RoomID:          roomID,
		ThreadRoomID:    threadRoomID,
		UserAccount:     account,
		SiteID:          "site-a",
		HasMention:      true,
	}
}

func baseSubForThreadRead(account, roomID string, threadUnread []string, alert bool) *model.Subscription {
	return &model.Subscription{
		User:         model.SubscriptionUser{ID: "u-" + account, Account: account},
		RoomID:       roomID,
		SiteID:       "site-a",
		JoinedAt:     time.Now().UTC().Add(-time.Hour),
		ThreadUnread: threadUnread,
		Alert:        alert,
	}
}

func TestHandler_MessageThreadRead_InvalidSubject(t *testing.T) {
	f := newThreadReadFixture(t)
	_, err := f.handler.handleMessageThreadRead(context.Background(), "garbage", threadReadBody(t, "p1"))
	require.Error(t, err)
}

func TestHandler_MessageThreadRead_EmptyThreadID(t *testing.T) {
	f := newThreadReadFixture(t)
	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, ""))
	require.ErrorIs(t, err, errInvalidThreadID)
}

func TestHandler_MessageThreadRead_MalformedBody(t *testing.T) {
	f := newThreadReadFixture(t)
	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, []byte("{"))
	require.Error(t, err)
}

func TestHandler_MessageThreadRead_NotRoomMember(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, model.ErrSubscriptionNotFound)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil).AnyTimes()
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil).AnyTimes()

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.ErrorIs(t, err, errNotRoomMember)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_ThreadSubNotFound(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSubForThreadRead("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(nil, model.ErrThreadSubscriptionNotFound)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil).AnyTimes()

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.ErrorIs(t, err, errThreadSubNotFound)
	assert.Equal(t, 0, f.publishCalls)
}

// Regression for the errgroup.WithContext bug: against real Mongo, when one
// goroutine fails with ErrThreadSubscriptionNotFound, an errgroup.WithContext
// cancels the others, causing them to return context.Canceled. Earlier code
// then matched `case subErr != nil` first and surfaced the wrapped
// context.Canceled as "internal error" instead of errThreadSubNotFound.
// Simulate by returning context.Canceled on the siblings.
func TestHandler_MessageThreadRead_ThreadSubNotFound_SiblingsCancelled(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, context.Canceled).AnyTimes()
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(nil, model.ErrThreadSubscriptionNotFound)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").
		Return("", context.Canceled).AnyTimes()

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.ErrorIs(t, err, errThreadSubNotFound)
}

func TestHandler_MessageThreadRead_BothMiss_RoomNotMemberWins(t *testing.T) {
	for i := 0; i < 20; i++ {
		f := newThreadReadFixture(t)
		f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
			Return(nil, model.ErrSubscriptionNotFound)
		f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
			Return(nil, model.ErrThreadSubscriptionNotFound).AnyTimes()
		f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil).AnyTimes()

		subj := subject.MessageThreadRead("alice", "r1", "site-a")
		_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
		require.ErrorIs(t, err, errNotRoomMember, "iteration %d", i)
	}
}

func TestHandler_MessageThreadRead_HappyAlertClears(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSubForThreadRead("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice",
		gomock.Len(0), false).Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	resp, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)
	var got map[string]string
	require.NoError(t, json.Unmarshal(resp, &got))
	assert.Equal(t, "accepted", got["status"])
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_HappyAlertStays(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSubForThreadRead("alice", "r1", []string{"p1", "p2"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice",
		[]string{"p2"}, true).Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)
}

func TestHandler_MessageThreadRead_IdempotentIDNotInArray(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSubForThreadRead("alice", "r1", []string{"p2"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice",
		[]string{"p2"}, true).Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)
}

func TestHandler_MessageThreadRead_AlertAlreadyFalse(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSubForThreadRead("alice", "r1", []string{"p1"}, false), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice",
		gomock.Len(0), false).Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)
}

func TestHandler_MessageThreadRead_CrossSite_PublishesOutbox(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSubForThreadRead("alice", "r1", []string{"p1", "p2"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice",
		[]string{"p2"}, true).Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)

	require.Equal(t, 1, f.publishCalls)
	assert.Equal(t, "outbox.site-a.to.site-b.thread_read", f.publishedSubj)

	var outer model.OutboxEvent
	require.NoError(t, json.Unmarshal(f.publishedData, &outer))
	assert.Equal(t, model.OutboxThreadRead, outer.Type)
	assert.Equal(t, "site-a", outer.SiteID)
	assert.Equal(t, "site-b", outer.DestSiteID)

	var inner model.ThreadReadEvent
	require.NoError(t, json.Unmarshal(outer.Payload, &inner))
	assert.Equal(t, "alice", inner.Account)
	assert.Equal(t, "r1", inner.RoomID)
	assert.Equal(t, "tr1", inner.ThreadRoomID)
	assert.Equal(t, "p1", inner.ParentMessageID)
	assert.Equal(t, []string{"p2"}, inner.NewThreadUnread)
	assert.True(t, inner.Alert)
	assert.Greater(t, inner.LastSeenAt, int64(0))
	assert.Greater(t, inner.Timestamp, int64(0))
}

func TestHandler_MessageThreadRead_GetUserSiteID_Empty(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSubForThreadRead("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any(), gomock.Any()).
		Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.NoError(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_GetUserSiteID_Error(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSubForThreadRead("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("", fmt.Errorf("boom"))
	// Writes are short-circuited by the read-phase error, but may race ahead.
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_OutboxPublishError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.publishCallErr = fmt.Errorf("nats down")
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSubForThreadRead("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any(), gomock.Any()).
		Return(nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.Error(t, err)
	require.Equal(t, 1, f.publishCalls)
}

func TestHandler_MessageThreadRead_UpdateSubscriptionError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSubForThreadRead("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("mongo down"))
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil).AnyTimes()

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_UpdateThreadSubscriptionError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(baseSubForThreadRead("alice", "r1", []string{"p1"}, true), nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(fmt.Errorf("mongo down"))

	subj := subject.MessageThreadRead("alice", "r1", "site-a")
	_, err := f.handler.handleMessageThreadRead(context.Background(), subj, threadReadBody(t, "p1"))
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MuteToggle_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice").
		Return(&model.Subscription{
			ID:     "s1",
			User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID: "r1",
			SiteID: "site-a",
			Muted:  true,
		}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("site-a", nil) // same site → no outbox publish

	var coreSubjects []string
	var coreBodies [][]byte
	var streamSubjects []string
	var streamBodies [][]byte
	h := &Handler{
		store:  store,
		siteID: "site-a",
		publishToStream: func(_ context.Context, subj string, data []byte, _ string) error {
			streamSubjects = append(streamSubjects, subj)
			streamBodies = append(streamBodies, data)
			return nil
		},
		publishCore: func(_ context.Context, subj string, data []byte) error {
			coreSubjects = append(coreSubjects, subj)
			coreBodies = append(coreBodies, data)
			return nil
		},
	}

	subj := subject.MuteToggle("alice", "r1", "site-a")
	resp, err := h.handleMuteToggle(context.Background(), subj, nil)
	require.NoError(t, err)

	var got model.MuteToggleResponse
	require.NoError(t, json.Unmarshal(resp, &got))
	assert.Equal(t, "ok", got.Status)
	assert.True(t, got.Muted)

	require.Len(t, coreSubjects, 1)
	assert.Equal(t, subject.SubscriptionUpdate("alice"), coreSubjects[0])

	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(coreBodies[0], &evt))
	assert.Equal(t, "mute_toggled", evt.Action)
	assert.True(t, evt.Subscription.Muted)
	assert.Equal(t, "alice", evt.Subscription.User.Account)

	// Canonical room-stream event for notification-worker cache invalidation.
	require.Len(t, streamSubjects, 1)
	assert.Equal(t, subject.RoomCanonicalMemberEvent("site-a", model.CanonicalMemberEventMuted), streamSubjects[0])
	var canon model.CanonicalMemberEvent
	require.NoError(t, json.Unmarshal(streamBodies[0], &canon))
	assert.Equal(t, model.CanonicalMemberEventMuted, canon.Type)
	assert.Equal(t, "r1", canon.RoomID)
	assert.Equal(t, "alice", canon.Account)
	assert.True(t, canon.Muted)
}

func TestHandler_MuteToggle_CrossSitePublishesOutbox(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice").
		Return(&model.Subscription{
			User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID: "r1",
			SiteID: "site-a",
			Muted:  true,
		}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("site-b", nil)

	var streamSubj string
	var streamData []byte
	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, s string, d []byte, _ string) error {
			streamSubj = s
			streamData = d
			return nil
		},
		publishCore: func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	subj := subject.MuteToggle("alice", "r1", "site-a")
	_, err := h.handleMuteToggle(context.Background(), subj, nil)
	require.NoError(t, err)

	assert.Equal(t, subject.Outbox("site-a", "site-b", model.OutboxSubscriptionMuteToggled), streamSubj)

	var outbox model.OutboxEvent
	require.NoError(t, json.Unmarshal(streamData, &outbox))
	assert.Equal(t, model.OutboxSubscriptionMuteToggled, outbox.Type)
	assert.Equal(t, "site-a", outbox.SiteID)
	assert.Equal(t, "site-b", outbox.DestSiteID)

	var payload model.SubscriptionMuteToggledEvent
	require.NoError(t, json.Unmarshal(outbox.Payload, &payload))
	assert.Equal(t, "alice", payload.Account)
	assert.Equal(t, "r1", payload.RoomID)
	assert.True(t, payload.Muted)
	assert.NotZero(t, payload.Timestamp)
}

func TestHandler_MuteToggle_NotRoomMember(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice").
		Return(nil, model.ErrSubscriptionNotFound)

	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	subj := subject.MuteToggle("alice", "r1", "site-a")
	_, err := h.handleMuteToggle(context.Background(), subj, nil)
	assert.ErrorIs(t, err, errNotRoomMember)
}

func TestHandler_MuteToggle_InvalidSubject(t *testing.T) {
	h := &Handler{
		siteID:          "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	_, err := h.handleMuteToggle(context.Background(), "garbage.subject", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid mute-toggle subject")
}

func TestHandler_MuteToggle_StoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice").
		Return(nil, fmt.Errorf("db down"))

	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	subj := subject.MuteToggle("alice", "r1", "site-a")
	_, err := h.handleMuteToggle(context.Background(), subj, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "toggle subscription mute")
}

func TestHandler_MuteToggle_GetUserSiteIDError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice").
		Return(&model.Subscription{
			User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("", fmt.Errorf("mongo down"))

	h := &Handler{
		store: store, siteID: "site-a",
		// Canonical member event publish happens before GetUserSiteID and is
		// independent of the outbox path — it represents the successful DB mutation.
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	subj := subject.MuteToggle("alice", "r1", "site-a")
	_, err := h.handleMuteToggle(context.Background(), subj, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get user siteId")
}

func TestHandler_MuteToggle_CrossSiteOutboxPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice").
		Return(&model.Subscription{
			User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID: "r1",
			SiteID: "site-a",
			Muted:  true,
		}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("site-b", nil)

	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			return fmt.Errorf("nats unavailable")
		},
		publishCore: func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	subj := subject.MuteToggle("alice", "r1", "site-a")
	_, err := h.handleMuteToggle(context.Background(), subj, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish mute-toggled outbox")
}

func TestHandler_natsGetRoomKey(t *testing.T) {
	const (
		siteID  = "site-a"
		account = "alice"
		roomID  = "room-1"
	)
	subj := subject.RoomKeyGet(account, roomID, siteID)

	sampleKey := roomkeystore.RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0x42}, 32)}
	sampleVersioned := &roomkeystore.VersionedKeyPair{Version: 7, KeyPair: sampleKey}

	type want struct {
		replyJSON string // expected JSON of the success reply (empty when err)
		errSubstr string // expected substring in err.Error() (empty when ok)
	}

	cases := []struct {
		name  string
		body  []byte
		setup func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore)
		want  want
	}{
		{
			name: "current version, happy path",
			body: []byte(`{}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				ks.EXPECT().Get(gomock.Any(), roomID).Return(sampleVersioned, nil)
			},
			want: want{replyJSON: `{"roomId":"room-1","version":7,"privateKey":"QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI="}`},
		},
		{
			name: "explicit version, happy path",
			body: []byte(`{"version":3}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				ks.EXPECT().GetByVersion(gomock.Any(), roomID, 3).Return(&sampleKey, nil)
			},
			want: want{replyJSON: `{"roomId":"room-1","version":3,"privateKey":"QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI="}`},
		},
		{
			name: "not a member",
			body: []byte(`{}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(nil, model.ErrSubscriptionNotFound)
			},
			want: want{errSubstr: "only room members"},
		},
		{
			name: "current key absent",
			body: []byte(`{}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				ks.EXPECT().Get(gomock.Any(), roomID).Return(nil, nil)
			},
			want: want{errSubstr: "room key not available"},
		},
		{
			name: "historical version absent",
			body: []byte(`{"version":1}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				ks.EXPECT().GetByVersion(gomock.Any(), roomID, 1).Return(nil, nil)
			},
			want: want{errSubstr: "room key not available"},
		},
		{
			name: "store error",
			body: []byte(`{}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				ks.EXPECT().Get(gomock.Any(), roomID).Return(nil, errors.New("valkey down"))
			},
			want: want{errSubstr: "get room key:"},
		},
		{
			name: "store error on explicit version",
			body: []byte(`{"version":5}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
				ks.EXPECT().GetByVersion(gomock.Any(), roomID, 5).Return(nil, errors.New("valkey down"))
			},
			want: want{errSubstr: "get room key:"},
		},
		{
			name: "malformed body",
			body: []byte(`not-json`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().GetSubscription(gomock.Any(), account, roomID).
					Return(&model.Subscription{}, nil)
			},
			want: want{errSubstr: "invalid request"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			ks := NewMockRoomKeyStore(ctrl)
			tc.setup(t, store, ks)

			h := NewHandler(store, ks, nil, nil, siteID, 1000, 500, 5*time.Second, 5, nil, nil)
			resp, err := h.handleGetRoomKey(t.Context(), subj, tc.body)
			if tc.want.errSubstr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.want.errSubstr)
				return
			}
			require.NoError(t, err)
			require.JSONEq(t, tc.want.replyJSON, string(resp))
		})
	}
}

// --- RoomRename tests ---

func TestHandleRoomRename_Validation(t *testing.T) {
	const validReqID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"

	tests := []struct {
		name       string
		subj       string
		body       []byte
		ctx        context.Context
		setupStore func(*MockRoomStore)
		wantErr    error
	}{
		{
			name:    "invalid subject",
			subj:    "bad.subject",
			body:    mustJSON(t, model.RenameRoomRequest{NewName: "new"}),
			ctx:     natsutil.WithRequestID(context.Background(), validReqID),
			wantErr: errInvalidRenameSubject,
		},
		{
			name:    "blank name after trim",
			subj:    subject.RoomRename("alice", "r1", "site-a"),
			body:    mustJSON(t, model.RenameRoomRequest{NewName: "   "}),
			ctx:     natsutil.WithRequestID(context.Background(), validReqID),
			wantErr: errInvalidName,
		},
		{
			name:    "name too long (>100 chars)",
			subj:    subject.RoomRename("alice", "r1", "site-a"),
			body:    mustJSON(t, model.RenameRoomRequest{NewName: strings.Repeat("x", 101)}),
			ctx:     natsutil.WithRequestID(context.Background(), validReqID),
			wantErr: errInvalidName,
		},
		{
			name: "room not found",
			subj: subject.RoomRename("alice", "r1", "site-a"),
			body: mustJSON(t, model.RenameRoomRequest{NewName: "new-name"}),
			ctx:  natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, mongo.ErrNoDocuments)
			},
			wantErr: errRoomNotFound,
		},
		{
			name: "wrong room type (DM)",
			subj: subject.RoomRename("alice", "r1", "site-a"),
			body: mustJSON(t, model.RenameRoomRequest{NewName: "new-name"}),
			ctx:  natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeDM}, nil)
			},
			wantErr: errRenameChannelOnly,
		},
		{
			name: "non-admin non-owner",
			subj: subject.RoomRename("alice", "r1", "site-a"),
			body: mustJSON(t, model.RenameRoomRequest{NewName: "new-name"}),
			ctx:  natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice", Roles: []model.UserRole{model.UserRoleUser}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
				// GetSubscription returns member-only role
				s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(
					&model.Subscription{Roles: []model.Role{model.RoleMember}}, nil,
				)
			},
			wantErr: errOnlyOwnersOrAdmins,
		},
		{
			name: "owner subscription allowed",
			subj: subject.RoomRename("alice", "r1", "site-a"),
			body: mustJSON(t, model.RenameRoomRequest{NewName: "new-name"}),
			ctx:  natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice", Roles: []model.UserRole{model.UserRoleUser}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
				s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(
					&model.Subscription{Roles: []model.Role{model.RoleOwner}}, nil,
				)
			},
			wantErr: nil,
		},
		{
			name: "room admin rejected (only owner or platform admin allowed)",
			subj: subject.RoomRename("alice", "r1", "site-a"),
			body: mustJSON(t, model.RenameRoomRequest{NewName: "new-name"}),
			ctx:  natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice", Roles: []model.UserRole{model.UserRoleUser}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
				s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(
					&model.Subscription{Roles: []model.Role{model.RoleAdmin}}, nil,
				)
			},
			wantErr: errOnlyOwnersOrAdmins,
		},
		{
			name: "admin allowed without subscription",
			subj: subject.RoomRename("admin1", "r1", "site-a"),
			body: mustJSON(t, model.RenameRoomRequest{NewName: "new-name"}),
			ctx:  natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, SiteID: "site-a"}, nil)
				// No GetSubscription call expected for platform admin
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			if tt.setupStore != nil {
				tt.setupStore(store)
			}
			h := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5,
				func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, nil)

			_, err := h.handleRoomRename(tt.ctx, tt.subj, tt.body)
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

// --- RoomRestricted tests ---

// happyPathRestrictedSuccessSetup wires the post-validation store calls that
// the sync handler needs to complete: Mongo writes, subscription list, user
// lookup. Used by the success-path table rows.
func happyPathRestrictedSuccessSetup(s *MockRoomStore) {
	s.EXPECT().UpdateRoomVisibility(gomock.Any(), "r1", gomock.Any(), gomock.Any()).Return(nil)
	s.EXPECT().ApplySubscriptionVisibility(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.EXPECT().ListSubscriptionsByRoom(gomock.Any(), "r1").Return(nil, nil)
	s.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, nil)
}

func TestHandleRoomRestricted_Validation(t *testing.T) {
	const validReqID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"

	tests := []struct {
		name       string
		body       []byte
		ctx        context.Context
		setupStore func(*MockRoomStore)
		wantErr    error
	}{
		{
			name:    "missing roomID/account in body",
			body:    mustJSON(t, model.RoomRestrictedRequest{Restricted: true}),
			ctx:     natsutil.WithRequestID(context.Background(), validReqID),
			wantErr: errInvalidRestrictedSubject,
		},
		{
			name: "non-admin requester",
			body: mustJSON(t, model.RoomRestrictedRequest{RoomID: "r1", Account: "alice", Restricted: true}),
			ctx:  natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice", Roles: []model.UserRole{model.UserRoleUser}}, nil)
			},
			wantErr: errOnlyAdmins,
		},
		{
			name: "room not found",
			body: mustJSON(t, model.RoomRestrictedRequest{RoomID: "r1", Account: "admin1", Restricted: true}),
			ctx:  natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, mongo.ErrNoDocuments)
			},
			wantErr: errRoomNotFound,
		},
		{
			name: "non-channel room",
			body: mustJSON(t, model.RoomRestrictedRequest{RoomID: "r1", Account: "admin1", Restricted: true}),
			ctx:  natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeDM}, nil)
			},
			wantErr: errRestrictedChannelOnly,
		},
		{
			name: "restricted=true + ownerAccount given + owner not a member",
			body: mustJSON(t, model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1",
				Restricted: true, OwnerAccount: "nonmember",
			}),
			ctx: natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: true, UserCount: 10}, nil)
				s.EXPECT().GetSubscription(gomock.Any(), "nonmember", "r1").Return(nil, mongo.ErrNoDocuments)
			},
			wantErr: errOwnerNotMember,
		},
		{
			name: "transition false→true without ownerAccount",
			body: mustJSON(t, model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1", Restricted: true,
			}),
			ctx: natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: false, UserCount: 10}, nil)
			},
			wantErr: errOwnerAccountRequired,
		},
		{
			name: "transition with UserCount < 5 (need at least 5)",
			body: mustJSON(t, model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1",
				Restricted: true, OwnerAccount: "owner1",
			}),
			ctx: natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: false, UserCount: 3}, nil)
				s.EXPECT().GetSubscription(gomock.Any(), "owner1", "r1").Return(&model.Subscription{}, nil)
			},
			wantErr: errNotEnoughMembers,
		},
		{
			name: "transition success (admin + ownerAccount + UserCount >= 5)",
			body: mustJSON(t, model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1",
				Restricted: true, OwnerAccount: "owner1",
			}),
			ctx: natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: false, UserCount: 10}, nil)
				s.EXPECT().GetSubscription(gomock.Any(), "owner1", "r1").Return(&model.Subscription{}, nil)
				happyPathRestrictedSuccessSetup(s)
			},
			wantErr: nil,
		},
		{
			name: "unrestrict (no owner/threshold checks)",
			body: mustJSON(t, model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1", Restricted: false,
			}),
			ctx: natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: true, UserCount: 10}, nil)
				happyPathRestrictedSuccessSetup(s)
			},
			wantErr: nil,
		},
		{
			name: "already-restricted owner change success",
			body: mustJSON(t, model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1",
				Restricted: true, OwnerAccount: "owner2",
			}),
			ctx: natsutil.WithRequestID(context.Background(), validReqID),
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: true, UserCount: 2}, nil)
				s.EXPECT().GetSubscription(gomock.Any(), "owner2", "r1").Return(&model.Subscription{}, nil)
				happyPathRestrictedSuccessSetup(s)
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			if tt.setupStore != nil {
				tt.setupStore(store)
			}
			h := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5,
				func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, nil)

			_, err := h.handleRoomRestricted(tt.ctx, tt.body)
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

// publishCore failure is intentionally non-fatal: the DB write is the source
// of truth and other client sessions reconcile on their next subscription
// refetch. The handler must still reply ok.
func TestHandler_MuteToggle_CorePublishFailureIsNonFatal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice").
		Return(&model.Subscription{
			User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID: "r1",
			SiteID: "site-a",
			Muted:  true,
		}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("site-a", nil)

	h := &Handler{
		store: store, siteID: "site-a",
		publishCore: func(_ context.Context, _ string, _ []byte) error {
			return fmt.Errorf("core nats down")
		},
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
	}

	subj := subject.MuteToggle("alice", "r1", "site-a")
	resp, err := h.handleMuteToggle(context.Background(), subj, nil)
	require.NoError(t, err, "publishCore failure must be non-fatal — DB write is the source of truth")

	var got model.MuteToggleResponse
	require.NoError(t, json.Unmarshal(resp, &got))
	assert.Equal(t, "ok", got.Status)
	assert.True(t, got.Muted)
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

func TestPublishCreateRoom_ProvisionsDEKBeforePublish(t *testing.T) {
	prov := &fakeDEKProvisioner{}
	published := 0
	h := &Handler{
		siteID:          "site-a",
		dekProvisioner:  prov,
		publishToStream: func(context.Context, string, []byte, string) error { published++; return nil },
	}

	_, err := h.publishCreateRoom(context.Background(),
		&model.CreateRoomRequest{RoomID: "r-1"},
		&model.User{ID: "u-1", Account: "alice"}, model.RoomTypeChannel)
	require.NoError(t, err)
	assert.Equal(t, []string{"r-1"}, prov.calls, "EnsureDEK must be called with the room ID")
	assert.Equal(t, 1, published)
}

func TestPublishCreateRoom_DEKFailure_BlocksAndSkipsPublish(t *testing.T) {
	prov := &fakeDEKProvisioner{err: errors.New("vault unavailable")}
	published := 0
	h := &Handler{
		siteID:          "site-a",
		dekProvisioner:  prov,
		publishToStream: func(context.Context, string, []byte, string) error { published++; return nil },
	}

	_, err := h.publishCreateRoom(context.Background(),
		&model.CreateRoomRequest{RoomID: "r-1"},
		&model.User{ID: "u-1", Account: "alice"}, model.RoomTypeChannel)
	require.Error(t, err)
	assert.Equal(t, 0, published, "canonical create event must NOT be published when DEK provisioning fails")
}

func TestPublishCreateRoom_NoProvisioner_Skips(t *testing.T) {
	published := 0
	h := &Handler{
		siteID:          "site-a",
		dekProvisioner:  nil, // ATREST disabled
		publishToStream: func(context.Context, string, []byte, string) error { published++; return nil },
	}

	_, err := h.publishCreateRoom(context.Background(),
		&model.CreateRoomRequest{RoomID: "r-1"},
		&model.User{ID: "u-1", Account: "alice"}, model.RoomTypeChannel)
	require.NoError(t, err)
	assert.Equal(t, 1, published)
}

func TestHandler_FavoriteToggle_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice").
		Return(&model.Subscription{
			ID:       "s1",
			User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:   "r1",
			SiteID:   "site-a",
			Favorite: true,
		}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("site-a", nil)

	var coreSubjects []string
	var coreBodies [][]byte
	h := &Handler{
		store:  store,
		siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			t.Fatal("publishToStream must not be called for same-site favorite toggle")
			return nil
		},
		publishCore: func(_ context.Context, subj string, data []byte) error {
			coreSubjects = append(coreSubjects, subj)
			coreBodies = append(coreBodies, data)
			return nil
		},
	}

	subj := subject.FavoriteToggle("alice", "r1", "site-a")
	resp, err := h.handleFavoriteToggle(context.Background(), subj, nil)
	require.NoError(t, err)

	var got model.FavoriteToggleResponse
	require.NoError(t, json.Unmarshal(resp, &got))
	assert.Equal(t, "ok", got.Status)
	assert.True(t, got.Favorite)

	require.Len(t, coreSubjects, 1)
	assert.Equal(t, subject.SubscriptionUpdate("alice"), coreSubjects[0])

	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(coreBodies[0], &evt))
	assert.Equal(t, "favorite_toggled", evt.Action)
	assert.True(t, evt.Subscription.Favorite)
	assert.Equal(t, "alice", evt.Subscription.User.Account)
}

func TestHandler_FavoriteToggle_CrossSitePublishesOutbox(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice").
		Return(&model.Subscription{
			User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:   "r1",
			SiteID:   "site-a",
			Favorite: true,
		}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("site-b", nil)

	var streamSubj string
	var streamData []byte
	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, s string, d []byte, _ string) error {
			streamSubj = s
			streamData = d
			return nil
		},
		publishCore: func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	subj := subject.FavoriteToggle("alice", "r1", "site-a")
	_, err := h.handleFavoriteToggle(context.Background(), subj, nil)
	require.NoError(t, err)

	assert.Equal(t, subject.Outbox("site-a", "site-b", model.OutboxSubscriptionFavoriteToggled), streamSubj)

	var outbox model.OutboxEvent
	require.NoError(t, json.Unmarshal(streamData, &outbox))
	assert.Equal(t, model.OutboxSubscriptionFavoriteToggled, outbox.Type)
	assert.Equal(t, "site-a", outbox.SiteID)
	assert.Equal(t, "site-b", outbox.DestSiteID)

	var payload model.SubscriptionFavoriteToggledEvent
	require.NoError(t, json.Unmarshal(outbox.Payload, &payload))
	assert.Equal(t, "alice", payload.Account)
	assert.Equal(t, "r1", payload.RoomID)
	assert.True(t, payload.Favorite)
	assert.NotZero(t, payload.Timestamp)
}

func TestHandler_FavoriteToggle_NotRoomMember(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice").
		Return(nil, model.ErrSubscriptionNotFound)

	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	subj := subject.FavoriteToggle("alice", "r1", "site-a")
	_, err := h.handleFavoriteToggle(context.Background(), subj, nil)
	assert.ErrorIs(t, err, errNotRoomMember)
}

func TestHandler_FavoriteToggle_InvalidSubject(t *testing.T) {
	h := &Handler{
		siteID:          "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	_, err := h.handleFavoriteToggle(context.Background(), "garbage.subject", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid favorite-toggle subject")
}

func TestHandler_FavoriteToggle_StoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice").
		Return(nil, fmt.Errorf("db down"))

	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	subj := subject.FavoriteToggle("alice", "r1", "site-a")
	_, err := h.handleFavoriteToggle(context.Background(), subj, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "toggle subscription favorite")
}

func TestHandler_FavoriteToggle_GetUserSiteIDError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice").
		Return(&model.Subscription{
			User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("", fmt.Errorf("mongo down"))

	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			t.Fatal("publishToStream must not be called when GetUserSiteID fails")
			return nil
		},
		publishCore: func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	subj := subject.FavoriteToggle("alice", "r1", "site-a")
	_, err := h.handleFavoriteToggle(context.Background(), subj, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get user siteId")
}

func TestHandler_FavoriteToggle_CrossSiteOutboxPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice").
		Return(&model.Subscription{
			User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:   "r1",
			SiteID:   "site-a",
			Favorite: true,
		}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("site-b", nil)

	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			return fmt.Errorf("nats unavailable")
		},
		publishCore: func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	subj := subject.FavoriteToggle("alice", "r1", "site-a")
	_, err := h.handleFavoriteToggle(context.Background(), subj, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish favorite-toggled outbox")
}

func TestHandler_FavoriteToggle_CorePublishFailureIsNonFatal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice").
		Return(&model.Subscription{
			User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID:   "r1",
			SiteID:   "site-a",
			Favorite: true,
		}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("site-a", nil)

	h := &Handler{
		store: store, siteID: "site-a",
		publishCore: func(_ context.Context, _ string, _ []byte) error {
			return fmt.Errorf("core nats down")
		},
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			t.Fatal("publishToStream must not be called for same-site favorite toggle")
			return nil
		},
	}

	subj := subject.FavoriteToggle("alice", "r1", "site-a")
	resp, err := h.handleFavoriteToggle(context.Background(), subj, nil)
	require.NoError(t, err, "publishCore failure must be non-fatal — DB write is the source of truth")

	var got model.FavoriteToggleResponse
	require.NoError(t, json.Unmarshal(resp, &got))
	assert.Equal(t, "ok", got.Status)
	assert.True(t, got.Favorite)
}
