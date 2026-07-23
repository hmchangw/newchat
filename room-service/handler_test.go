package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
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
	store.EXPECT().
		SetOwnerRole(gomock.Any(), "r1", "bob", true, gomock.Any()).
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "bob").
		Return("site-a", nil)

	var coreSubj string
	var coreData []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishCore: func(_ context.Context, subj string, data []byte) error { coreSubj = subj; coreData = data; return nil },
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			t.Error("same-site role update must not publish to a stream")
			return nil
		},
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}

	resp, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.NoError(t, err)
	assert.Equal(t, "ok", resp.Status)

	require.NotNil(t, coreData, "expected subscription.update published via core NATS")
	assert.Equal(t, subject.SubscriptionUpdate("bob"), coreSubj)
	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(coreData, &evt))
	assert.Equal(t, "role_updated", evt.Action)
	assert.Equal(t, []model.Role{model.RoleMember, model.RoleOwner}, evt.Subscription.Roles)
	assert.Equal(t, "general", evt.RoomName, "role_updated carries the channel name")
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

	_, err := h.updateRole(ctxParams(map[string]string{"account": "bob", "roomID": "r1"}), req)
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

	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.ErrorIs(t, err, errRoomTypeGuard)
}

func TestHandler_UpdateRole_InvalidRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: "admin"}

	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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

	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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

	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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
	store.EXPECT().
		SetOwnerRole(gomock.Any(), "r1", "bob", true, gomock.Any()).
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)
	store.EXPECT().GetUserSiteID(gomock.Any(), "bob").Return("site-a", nil)

	var coreData []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishCore:     func(_ context.Context, _ string, data []byte) error { coreData = data; return nil },
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}

	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.NoError(t, err)
	assert.NotNil(t, coreData, "promote must publish subscription.update when target is a bare subscriber in a room with no orgs")
}

func TestHandler_UpdateRole_Demote_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{
			Subscription:            &model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}},
			HasIndividualMembership: true,
		}, nil)
	store.EXPECT().SetOwnerRole(gomock.Any(), "r1", "bob", false, gomock.Any()).
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}}, nil)
	store.EXPECT().GetUserSiteID(gomock.Any(), "bob").Return("site-a", nil)

	var coreData []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishCore: func(_ context.Context, _ string, data []byte) error { coreData = data; return nil },
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			t.Error("same-site demote must not publish to a stream")
			return nil
		},
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleMember}

	resp, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.NoError(t, err)
	assert.Equal(t, "ok", resp.Status)
	require.NotNil(t, coreData)
	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(coreData, &evt))
	assert.Equal(t, "role_updated", evt.Action)
	assert.Equal(t, []model.Role{model.RoleMember}, evt.Subscription.Roles)
}

func TestHandler_UpdateRole_CrossSiteInbox(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{
			Subscription:            &model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}},
			HasIndividualMembership: true,
		}, nil)
	updated := &model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}}
	var roleTs time.Time
	store.EXPECT().SetOwnerRole(gomock.Any(), "r1", "bob", true, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ bool, ts time.Time) (*model.Subscription, error) {
			roleTs = ts
			return updated, nil
		})
	store.EXPECT().GetUserSiteID(gomock.Any(), "bob").Return("site-b", nil)

	var inboxSubj string
	var inboxData []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishCore: func(_ context.Context, _ string, _ []byte) error { return nil },
		publishToStream: func(_ context.Context, subj string, data []byte, _ string) error {
			inboxSubj = subj
			inboxData = data
			return nil
		},
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}

	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.NoError(t, err)

	require.NotNil(t, inboxData, "cross-site target must publish a role_updated inbox event")
	// role_updated federates via the OUTBOX relay, not a direct INBOX publish.
	assert.Equal(t, subject.Outbox("site-a", "site-b", "role_updated"), inboxSubj)
	var fed model.OutboxEvent
	require.NoError(t, json.Unmarshal(inboxData, &fed))
	var inboxEnv model.InboxEvent
	require.NoError(t, json.Unmarshal(fed.Envelope, &inboxEnv))
	assert.Equal(t, model.InboxEventType("role_updated"), inboxEnv.Type)
	assert.Equal(t, "site-a", inboxEnv.SiteID)
	assert.Equal(t, "site-b", inboxEnv.DestSiteID)
	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(inboxEnv.Payload, &evt))
	assert.Equal(t, []model.Role{model.RoleMember, model.RoleOwner}, evt.Subscription.Roles)
	// The origin doc's rolesUpdatedAt and the published event timestamp must be the
	// same instant so remote replicas guard against one high-water mark.
	assert.False(t, roleTs.IsZero())
	assert.Equal(t, roleTs.UnixMilli(), inboxEnv.Timestamp)
	assert.Equal(t, roleTs.UnixMilli(), evt.Timestamp)
}

func TestHandler_UpdateRole_SetOwnerRoleNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{
			Subscription:            &model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}},
			HasIndividualMembership: true,
		}, nil)
	store.EXPECT().SetOwnerRole(gomock.Any(), "r1", "bob", true, gomock.Any()).
		Return(nil, fmt.Errorf("set owner role: %w", model.ErrSubscriptionNotFound))

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishCore: func(_ context.Context, _ string, _ []byte) error {
			t.Error("must not publish on store error")
			return nil
		},
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			t.Error("must not publish on store error")
			return nil
		},
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}

	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.Error(t, err)
	assert.ErrorIs(t, err, errTargetNotMember)
}

func TestHandler_UpdateRole_PublishCoreError_NonFatal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "bob").
		Return(&SubscriptionWithMembership{
			Subscription:            &model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}},
			HasIndividualMembership: true,
		}, nil)
	store.EXPECT().SetOwnerRole(gomock.Any(), "r1", "bob", true, gomock.Any()).
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)
	store.EXPECT().GetUserSiteID(gomock.Any(), "bob").Return("site-a", nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return fmt.Errorf("nats down") },
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
	}

	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}

	resp, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.NoError(t, err, "publishCore failure must be non-fatal")
	assert.Equal(t, "ok", resp.Status)
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

	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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

	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.ErrorIs(t, err, errCannotDemoteLast)
}

// --- Error-path tests ---

func TestHandler_UpdateRole_GetRoomError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, fmt.Errorf("db error"))
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
	}
	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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
	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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
	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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
	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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
	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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
	store.EXPECT().SetOwnerRole(gomock.Any(), "r1", "bob", true, gomock.Any()).
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)
	store.EXPECT().GetUserSiteID(gomock.Any(), "bob").Return("site-b", nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return fmt.Errorf("nats down") },
	}
	req := model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner}
	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	if err == nil {
		t.Fatal("expected error for inbox publish failure")
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
		Return(&RoomCounts{MemberCount: 3, HumanCount: 3, OwnerCount: 2}, nil)

	var publishedSubj string
	var publishedData []byte
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(ctx context.Context, subj string, data []byte, _ string) error {
		publishedSubj = subj
		publishedData = data
		return nil
	}, nil, nil, 0)

	resp, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, subject.RoomCanonical("site-a", "member.remove"), publishedSubj)
	assert.Equal(t, "accepted", resp.Status)
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
			handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
			_, err := handler.removeMember(ctxParams(map[string]string{"account": tc.requester, "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: tc.target})
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
		Return(&RoomCounts{MemberCount: 2, HumanCount: 2, OwnerCount: 1}, nil)

	var publishedData []byte
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(ctx context.Context, _ string, data []byte, _ string) error {
		publishedData = data
		return nil
	}, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
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
				Return(&RoomCounts{MemberCount: 3, HumanCount: 3, OwnerCount: 1}, nil)
			handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
			_, err := handler.removeMember(ctxParams(map[string]string{"account": tc.requester, "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
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
		Return(&RoomCounts{MemberCount: 1, HumanCount: 1, OwnerCount: 0}, nil)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "last member")
}

// A QA "p_" account is an ordinary user, so removing it as the last human is
// blocked by the last-member guard (it is NOT treated as a bot that skips it).
func TestHandler_RemoveMember_QAPUnderscore_CountsAsHuman(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	sub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "p_qa1"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember},
	}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "p_qa1").
		Return(&SubscriptionWithMembership{Subscription: sub, HasIndividualMembership: true}, nil)
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 1, HumanCount: 1, OwnerCount: 0}, nil)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "p_qa1", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "p_qa1"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errCannotRemoveLastMember))
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
		Return(&RoomCounts{MemberCount: 3, HumanCount: 3, OwnerCount: 1}, nil)
	var publishedData []byte
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(ctx context.Context, subj string, data []byte, _ string) error {
		publishedData = data
		return nil
	}, nil, nil, 0)
	resp, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "bob"})
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
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "bob"})
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
	}, nil, nil, 0)
	resp, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", OrgID: "eng-org"})
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
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "bob", OrgID: "eng-org"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one")
}

func TestHandler_RemoveMember_NeitherAccountNorOrgID_Rejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one")
}

func TestHandler_RemoveMember_RoomIDMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r2", Account: "alice"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "room ID mismatch")
}

func TestHandler_RemoveMember_GetTargetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(nil, fmt.Errorf("db down"))
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
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
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "bob"})
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
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "count members")
}

func TestHandler_RemoveMember_OrgPath_RequesterLookupError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, fmt.Errorf("db down"))
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", OrgID: "eng-org"})
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
		Return(&RoomCounts{MemberCount: 3, HumanCount: 3, OwnerCount: 2}, nil)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(_ context.Context, _ string, _ []byte, _ string) error {
		return fmt.Errorf("nats down")
	}, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
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
	_, err := h.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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

	_, err := h.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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

	_, err := h.addMembers(ctxParams(map[string]string{"account": "bob", "roomID": "r1"}), req)
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

	_, err := h.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maximum capacity")
}

func TestHandler_AddMembers_CapacityShortCircuit(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:  model.SubscriptionUser{ID: "u1", Account: "alice"},
		Roles: []model.Role{model.RoleOwner},
	}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Name: "general", Type: model.RoomTypeChannel, UserCount: 3,
	}, nil)
	expectAllAccountsExist(store)
	// Intentionally NO CountNewMembers expectation: with no orgs and ample
	// headroom (3 + 2 candidates ≤ maxRoomSize 1000), the upper bound alone
	// satisfies the capacity check, so the costly resolution query must be
	// skipped. gomock fails the test if CountNewMembers is called.

	published := false
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			published = true
			return nil
		},
	}
	req := model.AddMembersRequest{RoomID: "r1", Users: []string{"bob", "carol"}}

	resp, err := h.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
	assert.True(t, published, "add must be published when the short-circuit accepts")
}

func TestHandler_AddMembers_RestrictedOwnerAllowed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, nil, nil, nil, "site-a", 100, 500, 5*time.Second, 5, publish, nil, nil, 0)

	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		Roles: []model.Role{model.RoleOwner},
	}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, Restricted: true, UserCount: 1,
	}, nil)
	// No CountNewMembers expectation: the capacity short-circuit (no orgs,
	// UserCount 1 + 1 candidate ≤ maxRoomSize 100) accepts without it. gomock
	// fails the test if it is called.
	expectAllAccountsExist(store)

	req := model.AddMembersRequest{RoomID: "r1", Users: []string{"bob"}}

	resp, err := h.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
}

func TestHandler_AddMembers_EmptyAfterResolve(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }
	h := NewHandler(store, nil, nil, nil, "site-a", 100, 500, 5*time.Second, 5, publish, nil, nil, 0)

	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		Roles: []model.Role{model.RoleMember},
	}, nil)
	// UserCount at the cap so the short-circuit does NOT fire (1 candidate would
	// exceed it) and we exercise the precise path: CountNewMembers resolves to 0
	// new members (alice already a member), so the add is still accepted.
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, UserCount: 100,
	}, nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "r1", gomock.Any()).
		Return(0, nil)

	req := model.AddMembersRequest{RoomID: "r1", Users: []string{"alice"}}

	resp, err := h.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
}

func TestHandler_AddMembers_ExplicitBot(t *testing.T) {
	enabledApp := &model.App{ID: "app1", Name: "Weather", Assistant: &model.AppAssistant{Enabled: true, Name: "weather.bot"}}

	tests := []struct {
		name            string
		botAccount      string
		setupMocks      func(store *MockRoomStore)
		wantErr         error
		wantErrContains string
		wantPublish     bool
	}{
		{
			name:       "enabled local bot is accepted",
			botAccount: "weather.bot",
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().GetApp(gomock.Any(), "weather.bot").Return(enabledApp, nil)
				store.EXPECT().GetUserSiteID(gomock.Any(), "weather.bot").Return("site-a", nil)
				expectAllAccountsExist(store)
			},
			wantPublish: true,
		},
		{
			// The platform-admin pseudo-account has NO app and NO assistant, so it
			// must NOT go through GetApp/site validation — it is admitted like a
			// candidate whose existence is enforced downstream by validateMembershipRefs.
			name:       "platform-admin pseudo account admitted without app validation",
			botAccount: "p_tchatadmin_siteA",
			setupMocks: func(store *MockRoomStore) {
				expectAllAccountsExist(store)
			},
			wantPublish: true,
		},
		{
			// A QA p_ account is an ordinary user: added like any human, no GetApp.
			name:       "QA p_ account added as a plain user",
			botAccount: "p_qa1",
			setupMocks: func(store *MockRoomStore) {
				expectAllAccountsExist(store)
			},
			wantPublish: true,
		},
		{
			name:       "unknown app rejected",
			botAccount: "weather.bot",
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().GetApp(gomock.Any(), "weather.bot").Return(nil, ErrAppNotFound)
			},
			wantErr: errBotNotAvailable,
		},
		{
			name:       "disabled assistant rejected",
			botAccount: "weather.bot",
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().GetApp(gomock.Any(), "weather.bot").Return(
					&model.App{ID: "app1", Assistant: &model.AppAssistant{Enabled: false, Name: "weather.bot"}}, nil)
			},
			wantErr: errBotNotAvailable,
		},
		{
			name:       "cross-site bot rejected",
			botAccount: "weather.bot",
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().GetApp(gomock.Any(), "weather.bot").Return(enabledApp, nil)
				store.EXPECT().GetUserSiteID(gomock.Any(), "weather.bot").Return("site-b", nil)
			},
			wantErr: errBotCrossSite,
		},
		{
			name:       "GetApp infra error collapses to internal",
			botAccount: "weather.bot",
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().GetApp(gomock.Any(), "weather.bot").Return(nil, errors.New("mongo timeout"))
			},
			wantErrContains: "get app for bot",
		},
		{
			name:       "GetUserSiteID infra error collapses to internal",
			botAccount: "weather.bot",
			setupMocks: func(store *MockRoomStore) {
				store.EXPECT().GetApp(gomock.Any(), "weather.bot").Return(enabledApp, nil)
				store.EXPECT().GetUserSiteID(gomock.Any(), "weather.bot").Return("", errors.New("mongo timeout"))
			},
			wantErrContains: "get bot siteId",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
				User:  model.SubscriptionUser{ID: "u1", Account: "alice"},
				Roles: []model.Role{model.RoleOwner},
			}, nil)
			store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
				ID: "r1", Type: model.RoomTypeChannel, UserCount: 1,
			}, nil)
			tc.setupMocks(store)

			var publishedPayload []byte
			h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
				publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
					publishedPayload = data
					return nil
				},
			}
			_, err := h.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.AddMembersRequest{Users: []string{tc.botAccount}})
			if tc.wantErr != nil || tc.wantErrContains != "" {
				require.Error(t, err)
				if tc.wantErr != nil {
					assert.True(t, errors.Is(err, tc.wantErr), "want %v, got %v", tc.wantErr, err)
				}
				if tc.wantErrContains != "" {
					assert.Contains(t, err.Error(), tc.wantErrContains)
				}
				assert.Nil(t, publishedPayload)
				return
			}
			require.NoError(t, err)
			require.True(t, tc.wantPublish)
			var published model.AddMembersRequest
			require.NoError(t, json.Unmarshal(publishedPayload, &published))
			assert.Contains(t, published.Users, tc.botAccount)
		})
	}
}

// An empty siteId for a bot is a data anomaly (per the domain owner no empty-siteId
// bot docs exist). We fail open: the bot is still admitted as local, and an
// error-level line is logged as an anomaly alarm rather than rejecting the add.
func TestHandler_AddMembers_ExplicitBot_EmptySiteID_AdmittedWithErrorLog(t *testing.T) {
	rec := installRecorder(t)

	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:  model.SubscriptionUser{ID: "u1", Account: "alice"},
		Roles: []model.Role{model.RoleOwner},
	}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, UserCount: 1,
	}, nil)
	store.EXPECT().GetApp(gomock.Any(), "weather.bot").Return(
		&model.App{ID: "app1", Assistant: &model.AppAssistant{Enabled: true, Name: "weather.bot"}}, nil)
	store.EXPECT().GetUserSiteID(gomock.Any(), "weather.bot").Return("", nil)
	expectAllAccountsExist(store)

	var publishedPayload []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
			publishedPayload = data
			return nil
		},
	}
	_, err := h.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}),
		model.AddMembersRequest{Users: []string{"weather.bot"}})
	require.NoError(t, err)

	var published model.AddMembersRequest
	require.NoError(t, json.Unmarshal(publishedPayload, &published))
	assert.Contains(t, published.Users, "weather.bot", "empty-siteId bot is admitted as local")

	assert.True(t, rec.has(slog.LevelError, "bot has empty siteId"),
		"empty-siteId bot must emit an error-level anomaly log")
}

// A bot listed twice costs one GetApp + one GetUserSiteID (dedup before validate).
func TestHandler_AddMembers_DuplicateBot_ValidatedOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:  model.SubscriptionUser{ID: "u1", Account: "alice"},
		Roles: []model.Role{model.RoleOwner},
	}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, UserCount: 1,
	}, nil)
	store.EXPECT().GetApp(gomock.Any(), "weather.bot").Times(1).Return(
		&model.App{ID: "app1", Assistant: &model.AppAssistant{Enabled: true, Name: "weather.bot"}}, nil)
	store.EXPECT().GetUserSiteID(gomock.Any(), "weather.bot").Times(1).Return("site-a", nil)
	expectAllAccountsExist(store)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
	}
	_, err := h.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}),
		model.AddMembersRequest{Users: []string{"weather.bot", "weather.bot"}})
	require.NoError(t, err)
}

// For a bot-owner target the last-member guard is skipped but the last-owner
// guard still fires — a sole bot-owner cannot be stranded.
func TestHandler_RemoveMember_BotOwnerTarget_LastOwnerGuardApplies(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	botOwnerSub := &model.Subscription{
		ID: "s9", User: model.SubscriptionUser{ID: "u9", Account: "weather.bot", IsBot: true},
		RoomID: "r1", Roles: []model.Role{model.RoleOwner},
	}
	requesterSub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleOwner},
	}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "weather.bot").
		Return(&SubscriptionWithMembership{Subscription: botOwnerSub, HasIndividualMembership: true}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(requesterSub, nil)
	// Owner target → counts fetched; last-owner guard fires on OwnerCount<=1.
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 2, HumanCount: 1, OwnerCount: 1}, nil)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}),
		model.RemoveMemberRequest{RoomID: "r1", Account: "weather.bot"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errLastOwnerCannotLeave))
}

func TestHandler_RemoveMember_BotTarget_SkipsMemberGuards(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	botSub := &model.Subscription{
		ID: "s9", User: model.SubscriptionUser{ID: "u9", Account: "weather.bot", IsBot: true},
		RoomID: "r1", Roles: []model.Role{model.RoleMember},
	}
	ownerSub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleOwner},
	}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "weather.bot").
		Return(&SubscriptionWithMembership{Subscription: botSub, HasIndividualMembership: true}, nil)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(ownerSub, nil)
	// No CountMembersAndOwners expectation: a non-owner bot target skips the guard.
	var publishedData []byte
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, func(_ context.Context, _ string, data []byte, _ string) error {
		publishedData = data
		return nil
	}, nil, nil, 0)
	resp, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "weather.bot"})
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
	var published model.RemoveMemberRequest
	require.NoError(t, json.Unmarshal(publishedData, &published))
	assert.Equal(t, "weather.bot", published.Account)
}

func TestHandler_RemoveMember_LastHumanWithBotsPresent_Rejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	sub := &model.Subscription{
		ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", Roles: []model.Role{model.RoleMember},
	}
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().GetSubscriptionWithMembership(gomock.Any(), "r1", "alice").
		Return(&SubscriptionWithMembership{Subscription: sub, HasIndividualMembership: true}, nil)
	// Two subs (alice + a bot) but one human: removing the last human is blocked.
	store.EXPECT().CountMembersAndOwners(gomock.Any(), "r1").
		Return(&RoomCounts{MemberCount: 2, HumanCount: 1, OwnerCount: 1}, nil)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.removeMember(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.RemoveMemberRequest{RoomID: "r1", Account: "alice"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errCannotRemoveLastMember))
}

func TestHandler_UpdateRole_BotPromotion_Rejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	// The bot check fires before any store read, so no store expectations.
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}),
		model.UpdateRoleRequest{RoomID: "r1", Account: "weather.bot", NewRole: model.RoleOwner})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBotCannotBeOwner))
}

// The platform-admin pseudo-account stays bot-like and cannot be promoted.
func TestHandler_UpdateRole_PlatformAdminPromotion_Rejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	handler := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	_, err := handler.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}),
		model.UpdateRoleRequest{RoomID: "r1", Account: "p_tchatadmin_siteA", NewRole: model.RoleOwner})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBotCannotBeOwner))
}

// A QA "p_" account is an ordinary user and CAN be promoted to owner.
func TestHandler_UpdateRole_QAPUnderscorePromotion_Allowed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1", Name: "general", Type: model.RoomTypeChannel}, nil)
	store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", Roles: []model.Role{model.RoleOwner}}, nil)
	store.EXPECT().
		GetSubscriptionWithMembership(gomock.Any(), "r1", "p_qa1").
		Return(&SubscriptionWithMembership{
			Subscription:            &model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "p_qa1"}, RoomID: "r1", Roles: []model.Role{model.RoleMember}},
			HasIndividualMembership: true,
		}, nil)
	store.EXPECT().
		SetOwnerRole(gomock.Any(), "r1", "p_qa1", true, gomock.Any()).
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u2", Account: "p_qa1"}, RoomID: "r1", Roles: []model.Role{model.RoleMember, model.RoleOwner}}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "p_qa1").
		Return("site-a", nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
	}

	resp, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}),
		model.UpdateRoleRequest{Account: "p_qa1", NewRole: model.RoleOwner})
	require.NoError(t, err)
	assert.Equal(t, "ok", resp.Status)
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
	store.EXPECT().CheckMembership(gomock.Any(), "alice", "r_src").Return(nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), "r_src", gomock.Any(), nil, false).Return([]model.RoomMember{
		{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "bob"}},
		{Member: model.RoomMemberEntry{Type: model.RoomMemberIndividual, Account: "weather.bot"}},
	}, nil)

	// The bot is filtered before publishing. The capacity short-circuit (no orgs,
	// UserCount 1 + 1 candidate ≤ 1000) skips CountNewMembers, so the filtering
	// is asserted on the published payload below rather than on the count args.
	expectAllAccountsExist(store)

	var publishedPayload []byte
	h := &Handler{
		store: store, siteID: "site-a", maxRoomSize: 1000, memberListClient: mc,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
			publishedPayload = data
			return nil
		},
	}

	req := model.AddMembersRequest{
		Users:    []string{},
		Channels: []model.ChannelRef{srcRef},
	}
	_, err := h.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), req)
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
				// No CountNewMembers: the capacity short-circuit (no orgs,
				// UserCount 1 + 1 candidate ≤ 1000) accepts without it.
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
			_, err := h.addMembers(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), tc.req)
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
			_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), tc.req)
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
		store.EXPECT().CheckMembership(gomock.Any(), "alice", "ch1").Return(nil)
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
		store.EXPECT().CheckMembership(gomock.Any(), "alice", "ch1").Return(nil)
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
		store.EXPECT().CheckMembership(gomock.Any(), "alice", "ch1").Return(nil)
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

		store.EXPECT().CheckMembership(gomock.Any(), "alice", "ch-local").Return(nil)
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
		store.EXPECT().CheckMembership(gomock.Any(), "alice", "ch1").Return(model.ErrSubscriptionNotFound)

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
		store.EXPECT().CheckMembership(gomock.Any(), "alice", "ch1").Return(errors.New("mongo timeout"))

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
		store.EXPECT().CheckMembership(gomock.Any(), "alice", "ch-slow").
			Return(context.DeadlineExceeded)

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
		store.EXPECT().CheckMembership(gomock.Any(), "alice", "ch1").Return(nil)
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
		store.EXPECT().CheckMembership(gomock.Any(), "alice", "ch1").Return(nil)
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
		body      []byte
		setupMock func(*MockRoomStore)
		want      want
	}{
		{
			name: "happy path returns members",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().ListRoomMembers(gomock.Any(), roomID, (*int)(nil), (*int)(nil), false).
					Return([]model.RoomMember{orgMember, existingMember}, nil)
			},
			want: want{members: []model.RoomMember{orgMember, existingMember}},
		},
		{
			name: "fallback path returns synthesized individuals",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				synth := model.RoomMember{
					ID: "sub-xyz", RoomID: roomID, Ts: time.Unix(3, 0).UTC(),
					Member: model.RoomMemberEntry{ID: "u-alice", Type: model.RoomMemberIndividual, Account: "alice"},
				}
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().ListRoomMembers(gomock.Any(), roomID, (*int)(nil), (*int)(nil), false).
					Return([]model.RoomMember{synth}, nil)
			},
			want: want{members: []model.RoomMember{{
				ID: "sub-xyz", RoomID: roomID, Ts: time.Unix(3, 0).UTC(),
				Member: model.RoomMemberEntry{ID: "u-alice", Type: model.RoomMemberIndividual, Account: "alice"},
			}}},
		},
		{
			name: "requester not a member",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(fmt.Errorf("missing: %w", model.ErrSubscriptionNotFound))
			},
			want: want{errIs: errNotRoomMember},
		},
		{
			name: "invalid JSON body",
			body: []byte("{not json"),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
			},
			want: want{errContains: "invalid request"},
		},
		{
			name: "non-positive limit: negative",
			body: []byte(`{"limit":-1}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
			},
			want: want{errContains: "limit must be > 0"},
		},
		{
			name: "non-positive limit: zero",
			body: []byte(`{"limit":0}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
			},
			want: want{errContains: "limit must be > 0"},
		},
		{
			name: "negative offset",
			body: []byte(`{"offset":-1}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
			},
			want: want{errContains: "offset must be >= 0"},
		},
		{
			name: "pagination passed through",
			body: []byte(`{"limit":10,"offset":5}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
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
			name: "auth probe infra error",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "check room membership"},
		},
		{
			name: "store error on list",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().ListRoomMembers(gomock.Any(), roomID, (*int)(nil), (*int)(nil), false).
					Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "get room members"},
		},
		{
			name: "enrich=true passed through to store",
			body: []byte(`{"enrich":true}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().ListRoomMembers(gomock.Any(), roomID, (*int)(nil), (*int)(nil), true).
					Return([]model.RoomMember{
						{
							ID: "rm1", RoomID: roomID, Ts: time.Unix(1, 0).UTC(),
							Member: model.RoomMemberEntry{
								ID: "alice", Type: model.RoomMemberIndividual, Account: "alice",
								EngName: "Alice Wang", ChineseName: "愛麗絲", IsOwner: true,
								SectName: "Cardiology", EmployeeID: "E10293",
							},
						},
						{
							ID: "rm2", RoomID: roomID, Ts: time.Unix(2, 0).UTC(),
							Member: model.RoomMemberEntry{
								ID: "DEPT-100", Type: model.RoomMemberOrg,
								OrgName: "Cardiology Department", OrgDescription: "Inpatient care", MemberCount: 42,
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
						SectName: "Cardiology", EmployeeID: "E10293",
					},
				},
				{
					ID: "rm2", RoomID: roomID, Ts: time.Unix(2, 0).UTC(),
					Member: model.RoomMemberEntry{
						ID: "DEPT-100", Type: model.RoomMemberOrg,
						OrgName: "Cardiology Department", OrgDescription: "Inpatient care", MemberCount: 42,
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
			c := ctxParams(map[string]string{"account": requester, "roomID": roomID})
			c.Msg = &nats.Msg{Data: tc.body}
			resp, err := h.listMembers(c)

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

// TestHandler_ListMembers_EmptyBody locks the optional-body contract: a nil
// request body must reach the happy path, not be rejected as malformed.
func TestHandler_ListMembers_EmptyBody(t *testing.T) {
	const (
		siteID    = "site-a"
		roomID    = "r1"
		requester = "alice"
	)
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
		Return(nil)
	store.EXPECT().ListRoomMembers(gomock.Any(), roomID, (*int)(nil), (*int)(nil), false).
		Return([]model.RoomMember{{ID: "rm1", RoomID: roomID, Member: model.RoomMemberEntry{ID: "alice", Type: model.RoomMemberIndividual, Account: "alice"}}}, nil)

	h := &Handler{store: store, siteID: siteID}
	c := ctxParams(map[string]string{"account": requester, "roomID": roomID})
	c.Msg = &nats.Msg{Data: nil}
	resp, err := h.listMembers(c)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Len(t, resp.Members, 1)
}

func TestHandler_ListOrgMembers(t *testing.T) {
	const orgID = "sect-eng"

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
		orgID     string
		setupMock func(*MockRoomStore)
		want      want
	}{
		{
			name:  "happy path returns members",
			orgID: orgID,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().ListOrgMembers(gomock.Any(), orgID).Return(members, nil)
			},
			want: want{members: members},
		},
		{
			name:  "empty org returns RoomInvalidOrg-reason errcode",
			orgID: orgID,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().ListOrgMembers(gomock.Any(), orgID).Return(nil, errcode.BadRequest(fmt.Sprintf("list org members for %q", orgID), errcode.WithReason(errcode.RoomInvalidOrg)))
			},
			want: want{wantReason: errcode.RoomInvalidOrg},
		},
		{
			name:  "store error is wrapped",
			orgID: orgID,
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
			resp, err := h.listOrgMembers(ctxParams(map[string]string{"account": "alice", "orgID": tc.orgID}))

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
		req        model.RoomsInfoBatchRequest
		setupStore func(*MockRoomStore)
		setupKeys  func(*MockRoomKeyStore)
		wantErr    string
		assertResp func(t *testing.T, resp model.RoomsInfoBatchResponse)
	}{
		{
			name: "happy path — 3 rooms, 2 keyed, order preserved",
			req:  model.RoomsInfoBatchRequest{RoomIDs: []string{"r1", "r2", "r3"}},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r1", "r2", "r3"}).Return([]model.Room{
					{ID: "r1", Name: "general", SiteID: "site-a", UserCount: 42, LastMsgID: "m-100"},
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

				// r1: found, keyed, room-doc denorm fields forwarded
				assert.Equal(t, "r1", resp.Rooms[0].RoomID)
				assert.True(t, resp.Rooms[0].Found)
				assert.Equal(t, "general", resp.Rooms[0].Name)
				assert.Equal(t, 42, resp.Rooms[0].UserCount)
				assert.Equal(t, "m-100", resp.Rooms[0].LastMsgID)
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
			name: "missing room → Found=false, LastMsgAt=nil",
			req:  model.RoomsInfoBatchRequest{RoomIDs: []string{"r-missing"}},
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
			req:     model.RoomsInfoBatchRequest{RoomIDs: []string{}},
			wantErr: "must not be empty",
		},
		{
			name: "oversized batch → exceeds limit",
			req: model.RoomsInfoBatchRequest{RoomIDs: func() []string {
				ids := make([]string, 101)
				for i := range ids {
					ids[i] = fmt.Sprintf("r%d", i)
				}
				return ids
			}()},
			wantErr: "exceeds limit",
		},
		{
			name: "Mongo error → list rooms by ids",
			req:  model.RoomsInfoBatchRequest{RoomIDs: []string{"r1"}},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r1"}).Return(nil, errors.New("mongo timeout"))
			},
			setupKeys: func(k *MockRoomKeyStore) {
				k.EXPECT().GetMany(gomock.Any(), []string{"r1"}).Return(map[string]*roomkeystore.VersionedKeyPair{}, nil).AnyTimes()
			},
			wantErr: "list rooms by ids",
		},
		{
			name: "Valkey error → get room keys",
			req:  model.RoomsInfoBatchRequest{RoomIDs: []string{"r1"}},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r1"}).Return([]model.Room{{ID: "r1", Name: "x", SiteID: "s"}}, nil).AnyTimes()
			},
			setupKeys: func(k *MockRoomKeyStore) {
				k.EXPECT().GetMany(gomock.Any(), []string{"r1"}).Return(nil, errors.New("valkey down"))
			},
			wantErr: "get room keys",
		},
		{
			name: "duplicate IDs → 2 entries",
			req:  model.RoomsInfoBatchRequest{RoomIDs: []string{"r1", "r1"}},
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
			name: "LastMsgAt set → correct millis",
			req:  model.RoomsInfoBatchRequest{RoomIDs: []string{"r1"}},
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
			name: "MinUserLastSeenAt set → correct millis",
			req:  model.RoomsInfoBatchRequest{RoomIDs: []string{"r1"}},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r1"}).Return([]model.Room{
					{ID: "r1", Name: "general", SiteID: "site-a", MinUserLastSeenAt: &now},
				}, nil)
			},
			setupKeys: func(k *MockRoomKeyStore) {
				k.EXPECT().GetMany(gomock.Any(), []string{"r1"}).Return(map[string]*roomkeystore.VersionedKeyPair{}, nil)
			},
			assertResp: func(t *testing.T, resp model.RoomsInfoBatchResponse) {
				require.Len(t, resp.Rooms, 1)
				require.NotNil(t, resp.Rooms[0].MinUserLastSeenAt)
				assert.Equal(t, now.UTC().UnixMilli(), *resp.Rooms[0].MinUserLastSeenAt)
			},
		},
		{
			name: "LastMsgAt zero-time in Mongo → nil in response",
			req:  model.RoomsInfoBatchRequest{RoomIDs: []string{"r-zero"}},
			setupStore: func(s *MockRoomStore) {
				zero := time.Time{}
				s.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r-zero"}).Return([]model.Room{
					{
						ID: "r-zero", Name: "quiet", SiteID: "site-a",
						LastMsgAt: &zero, LastMentionAllAt: &zero, MinUserLastSeenAt: &zero,
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
				assert.Nil(t, resp.Rooms[0].MinUserLastSeenAt, "non-nil zero-time Room.MinUserLastSeenAt must produce nil RoomInfo.MinUserLastSeenAt")
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

			resp, err := h.roomsInfoBatch(ctxParams(map[string]string{}), tc.req)

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)
			if tc.assertResp != nil {
				tc.assertResp(t, *resp)
			}
		})
	}
}

func TestHandler_handleRoomsInfoBatch_ForwardsCounts(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	keyStore := NewMockRoomKeyStore(ctrl)

	store.EXPECT().ListRoomsByIDs(gomock.Any(), []string{"r1"}).Return([]model.Room{
		{ID: "r1", Name: "general", SiteID: "site-a", UserCount: 5, AppCount: 2},
	}, nil)
	keyStore.EXPECT().GetMany(gomock.Any(), []string{"r1"}).Return(map[string]*roomkeystore.VersionedKeyPair{}, nil)

	h := &Handler{store: store, keyStore: keyStore, siteID: "site-a", maxBatchSize: 100}

	resp, err := h.roomsInfoBatch(ctxParams(map[string]string{}), model.RoomsInfoBatchRequest{RoomIDs: []string{"r1"}})
	require.NoError(t, err)
	require.Len(t, resp.Rooms, 1)
	assert.Equal(t, 5, resp.Rooms[0].UserCount)
	assert.Equal(t, 2, resp.Rooms[0].AppCount)
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

	resp, err := h.roomsInfoBatch(ctxParams(map[string]string{}), model.RoomsInfoBatchRequest{RoomIDs: ids})
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.Len(t, resp.Rooms, 600)
	for i, ri := range resp.Rooms {
		assert.Equal(t, fmt.Sprintf("r%d", i), ri.RoomID)
		assert.False(t, ri.Found)
	}
}

func TestHandler_threadRoomInfoBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxBatchSize: 100}

	lastMsg := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	store.EXPECT().
		GetThreadRoomInfos(gomock.Any(), []string{"tr1", "tr2"}).
		Return([]ThreadRoomInfoRow{
			{ThreadRoomID: "tr1", LastMsgAt: lastMsg},
		}, nil)

	resp, err := h.threadRoomInfoBatch(ctxParams(map[string]string{}), model.ThreadRoomInfoBatchRequest{
		ThreadRoomIDs: []string{"tr1", "tr2"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Threads, 2)
	assert.Equal(t, model.ThreadRoomInfo{
		ThreadRoomID: "tr1", Found: true,
		LastMsgAt: lastMsg.UTC().UnixMilli(),
	}, resp.Threads[0])
	assert.Equal(t, model.ThreadRoomInfo{ThreadRoomID: "tr2", Found: false}, resp.Threads[1])
}

func TestHandler_threadRoomInfoBatch_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	h := &Handler{store: NewMockRoomStore(ctrl), siteID: "site-a", maxBatchSize: 100}

	_, err := h.threadRoomInfoBatch(ctxParams(map[string]string{}), model.ThreadRoomInfoBatchRequest{})
	var e *errcode.Error
	require.True(t, errors.As(err, &e))
	assert.Equal(t, errcode.CodeBadRequest, e.Code)
}

func TestHandler_threadRoomInfoBatch_OverLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxBatchSize: 1}

	_, err := h.threadRoomInfoBatch(ctxParams(map[string]string{}), model.ThreadRoomInfoBatchRequest{
		ThreadRoomIDs: []string{"tr1", "tr2"},
	})
	var e *errcode.Error
	require.True(t, errors.As(err, &e))
	assert.Equal(t, errcode.CodeBadRequest, e.Code)
}

func TestHandler_threadRoomInfoBatch_StoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetThreadRoomInfos(gomock.Any(), gomock.Any()).Return(nil, errors.New("boom"))
	h := &Handler{store: store, siteID: "site-a", maxBatchSize: 100}

	_, err := h.threadRoomInfoBatch(ctxParams(map[string]string{}), model.ThreadRoomInfoBatchRequest{
		ThreadRoomIDs: []string{"tr1"},
	})
	require.Error(t, err)
}

// --- createRoom tests ---

const testRequestID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"

// ctxParams builds a *natsrouter.Context with subject params and a valid
// request ID on the underlying ctx (for handlers that echo/read it).
func ctxParams(params map[string]string) *natsrouter.Context {
	c := natsrouter.NewContext(params)
	c.SetContext(natsutil.WithRequestID(context.Background(), testRequestID))
	return c
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

// Boundary reject behavior is covered by pkg/natsutil.RequireRequestID and
// pkg/natsrouter; dedup-critical paths make server-side minting unsafe (§3a).

func TestHandleCreateRoom_EmptyPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	// GetUser is NOT called for an empty request — the empty-check fires first.
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errEmptyCreateRequest))
}

func TestHandleCreateRoom_RequesterNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(nil, ErrUserNotFound)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"bob"}})
	require.Error(t, err)
	assert.True(t, errcode.HasReason(err, errcode.RoomUserNotFound), "want RoomUserNotFound, got %v", err)
}

func TestHandleCreateRoom_RequesterMissingNameFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{ID: "u-alice", Account: "alice"}, nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"bob"}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errInvalidUserData))
}

func TestHandleCreateRoom_SelfDM_Creates(t *testing.T) {
	// A self-DM intent ([requester] only) publishes a canonical create event with
	// the deterministic self-DM room id, same async path as a 2-party DM.
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "alice").Return(nil, model.ErrSubscriptionNotFound)

	var published model.CreateRoomRequest
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
			return json.Unmarshal(data, &published)
		},
	}

	reply, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"alice"}})
	require.NoError(t, err)
	wantID := idgen.BuildDMRoomID("u-alice", "u-alice")
	assert.Equal(t, wantID, reply.RoomID)
	assert.Equal(t, model.CreateRoomReplyAccepted, reply.Status)
	assert.Equal(t, string(model.RoomTypeDM), reply.RoomType)
	// The published event carries the self id + the requester as the lone user.
	assert.Equal(t, wantID, published.RoomID)
	assert.Equal(t, []string{"alice"}, published.Users)
}

func TestHandleCreateRoom_SelfDM_Idempotent(t *testing.T) {
	// Repeat create with an existing self-DM returns the same room and never
	// publishes (one-per-user).
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "alice").
		Return(&model.Subscription{RoomID: "room-self"}, nil)

	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error {
			t.Fatal("publish must not be called when the self-DM already exists")
			return nil
		},
	}

	reply, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"alice"}})
	require.NoError(t, err)
	assert.Equal(t, "room-self", reply.RoomID)
	assert.Equal(t, model.CreateRoomStatusExists, reply.Status)
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

	reply, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"bob"}})
	require.NoError(t, err)
	require.NotNil(t, publishedData)

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

	reply, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"bob"}})
	require.NoError(t, err)

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

	reply, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"helper.bot"}})
	require.NoError(t, err)
	require.NotNil(t, publishedData)

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

	resp, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"weather.bot"}})
	require.NoError(t, err)
	assert.True(t, published, "canonical event must publish for botDM with bot lacking name fields")
	assert.Equal(t, string(model.RoomTypeBotDM), resp.RoomType)
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

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"helper.bot"}})
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

	reply, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"helper.bot"}})
	require.NoError(t, err)

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

	reply, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Name: "general", Users: []string{"bob", "charlie"}})
	require.NoError(t, err)
	require.NotNil(t, publishedData)

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

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"bob", "charlie"}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errChannelNameRequired), "expected errChannelNameRequired, got %v", err)
}

func TestHandleCreateRoom_Channel_NameWhitespaceOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Name: "   ", Users: []string{"bob", "charlie"}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errChannelNameRequired))
}

func TestHandleCreateRoom_Channel_NameTooLong(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Name: strings.Repeat("a", 101), Users: []string{"bob"}})
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

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Name: strings.Repeat("世", 100), Users: []string{"bob"}})
	require.NoError(t, err)
}

func TestHandleCreateRoom_Channel_BotRejected(t *testing.T) {
	// Bot detection is part of the input-only stage — no DB call required.
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Name: "general", Users: []string{"helper.bot"}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBotInChannel))
}

// The platform-admin pseudo-account is bot-like and stays rejected in channels.
func TestHandleCreateRoom_Channel_PlatformAdminRejected(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Name: "general", Users: []string{"p_tchatadmin_siteA"}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errBotInChannel))
}

// A QA "p_" account is an ordinary user and may be a channel member.
func TestHandleCreateRoom_Channel_QAPUnderscoreAllowed(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "", gomock.Any()).Return(2, nil)
	var published []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error { published = data; return nil },
	}

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Name: "general", Users: []string{"p_qa1", "bob"}})
	require.NoError(t, err)
	assert.NotNil(t, published, "channel with a QA p_ member must be created")
}

func TestHandleCreateRoom_Channel_ExceedsCapacity(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	expectAllAccountsExist(store)
	store.EXPECT().CountNewMembers(gomock.Any(), gomock.Any(), gomock.Any(), "", gomock.Any()).Return(11, nil)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 10}

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Name: "big-room", Users: []string{"bob"}})
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

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Name: "solo", Users: []string{"bob"}})
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

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Name: "edge", Users: []string{"bob"}})
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

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Name: "edge", Users: []string{"bob"}})
	require.NoError(t, err)
}

// A QA "p_" account is an ordinary user, so a DM with it is a REGULAR DM, not a
// botDM — createRoom must not invoke the bot app-availability check for it.
func TestHandleCreateRoom_DM_QAPUnderscoreAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	store.EXPECT().GetUser(gomock.Any(), "p_webhook").Return(&model.User{
		ID: "u_p", Account: "p_webhook", EngName: "Webhook", ChineseName: "网钩",
	}, nil)
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "p_webhook").
		Return(nil, model.ErrSubscriptionNotFound)
	// No GetApp expectation: a regular DM must not consult the app store.
	var publishedData []byte
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000,
		publishToStream: func(_ context.Context, _ string, data []byte, _ string) error {
			publishedData = data
			return nil
		},
	}

	reply, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"p_webhook"}})
	require.NoError(t, err)

	assert.Equal(t, string(model.RoomTypeDM), reply.RoomType, "QA p_ account must classify as a regular DM")
	assert.NotNil(t, publishedData)
}

// --- createRoom reply-shape tests ---

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
	// Verify createRoom returns a SUCCESS "exists" reply (not an error)
	// when FindDMSubscription returns an existing subscription.
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(aliceUser(), nil)
	store.EXPECT().GetUser(gomock.Any(), "bob").Return(bobUser(), nil)
	store.EXPECT().FindDMSubscription(gomock.Any(), "alice", "bob").Return(
		&model.Subscription{RoomID: "existing-dm"}, nil,
	)
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	reply, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"bob"}})
	require.NoError(t, err)

	assert.Equal(t, model.CreateRoomStatusExists, reply.Status)
	assert.Equal(t, "existing-dm", reply.RoomID)
}

func TestNatsCreateRoom_GenericErrorReply(t *testing.T) {
	// A bare DB error collapses to internal at the reply boundary (Classify).
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().GetUser(gomock.Any(), "alice").Return(nil, fmt.Errorf("mongo connection refused"))
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000}

	_, err := h.createRoom(ctxParams(map[string]string{"account": "alice"}), model.CreateRoomRequest{Users: []string{"bob"}})
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
	coreSubjects   []string
	coreData       [][]byte
	coreErr        error
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
		publishCore: func(_ context.Context, subj string, data []byte) error {
			f.coreSubjects = append(f.coreSubjects, subj)
			f.coreData = append(f.coreData, data)
			return f.coreErr
		},
	}
	return f
}

func TestHandler_MessageRead_NotMember(t *testing.T) {
	f := newMessageReadFixture(t)
	f.store.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, model.ErrSubscriptionNotFound)
	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	resp, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	assert.Equal(t, "accepted", resp.Status)
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
}

func TestHandler_MessageRead_CrossSite_PublishesInbox(t *testing.T) {
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	assert.Equal(t, 1, f.publishCalls)
	assert.Equal(t, subject.Outbox("site-a", "site-b", model.InboxSubscriptionRead), f.publishedSubj)

	var fed model.OutboxEvent
	require.NoError(t, json.Unmarshal(f.publishedData, &fed))

	var inboxEnv model.InboxEvent
	require.NoError(t, json.Unmarshal(fed.Envelope, &inboxEnv))
	assert.Equal(t, model.InboxSubscriptionRead, inboxEnv.Type)
	assert.Equal(t, "site-a", inboxEnv.SiteID)
	assert.Equal(t, "site-b", inboxEnv.DestSiteID)

	var inner model.SubscriptionReadEvent
	require.NoError(t, json.Unmarshal(inboxEnv.Payload, &inner))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
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

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
}

// --- subscription.update (action "read") tests for messageRead ---

// After a successful same-site messageRead, the handler must publish exactly one
// subscription.update (action "read") to the reader's own account via core NATS.
func TestHandler_MessageRead_PublishesSubscriptionUpdate_Local(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", gomock.Any()).Return(nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	// Exactly one core NATS publish to the reader's subscription.update subject.
	require.Len(t, f.coreSubjects, 1, "expected exactly one core NATS publish")
	assert.Equal(t, "chat.user.alice.event.subscription.update", f.coreSubjects[0])

	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(f.coreData[0], &evt))
	assert.Equal(t, "read", evt.Action)
	assert.Equal(t, "u1", evt.UserID)
	assert.NotNil(t, evt.Subscription.LastSeenAt, "published subscription must carry updated lastSeenAt")
	assert.False(t, evt.Subscription.Alert, "published subscription carries updated alert")
	assert.False(t, evt.Subscription.HasMention, "read deliberately clears hasMention")
	assert.False(t, evt.Subscription.HasGroupMention, "no LastMentionAllAt → hasGroupMention false")
}

// Reading the room clears hasGroupMention: the read event always carries
// hasGroupMention=false regardless of room.LastMentionAllAt.
func TestHandler_MessageRead_PublishesSubscriptionUpdate_HasGroupMention(t *testing.T) {
	cases := []struct {
		name          string
		lastMentionAt func(now time.Time) *time.Time
		wantGroup     bool
	}{
		{"caught up: mention before read", func(now time.Time) *time.Time {
			t := now.Add(-time.Minute)
			return &t
		}, false},
		{"concurrent @all: mention after read", func(now time.Time) *time.Time {
			t := now.Add(time.Minute)
			return &t
		}, false},
		{"no mention-all timestamp", func(time.Time) *time.Time { return nil }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newMessageReadFixture(t)
			joined := time.Now().UTC().Add(-2 * time.Hour)
			lastSeen := joined.Add(time.Hour)
			lastMsg := lastSeen.Add(30 * time.Minute)

			f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
				User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
				RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
				HasMention: true, // pre-read state; read must clear it on the event
			}, nil)
			f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
			f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
			f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
				ID: "r1", LastMsgAt: &lastMsg, LastMentionAllAt: tc.lastMentionAt(time.Now().UTC()),
			}, nil)
			f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
			f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", gomock.Any()).Return(nil)

			_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
			require.NoError(t, err)

			require.Len(t, f.coreSubjects, 1)
			var evt model.SubscriptionUpdateEvent
			require.NoError(t, json.Unmarshal(f.coreData[0], &evt))
			assert.False(t, evt.Subscription.HasMention, "read always clears hasMention")
			assert.Equal(t, tc.wantGroup, evt.Subscription.HasGroupMention)
		})
	}
}

// A bot account must NOT receive a subscription.update on messageRead.
func TestHandler_MessageRead_NoPublish_BotAccount(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-time.Hour)

	f.store.EXPECT().GetSubscription(gomock.Any(), "bot.bot", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "b1", Account: "bot.bot"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "bot.bot", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "bot.bot").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1"}, nil)
	// Room has no LastMsgAt → early return; no floor recompute calls expected.

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "bot.bot", "roomID": "r1"}))
	require.NoError(t, err)

	assert.Empty(t, f.coreSubjects, "bot account must not receive a subscription.update")
}

// On the early-return path (room has no content), no subscription.update is fired.
func TestHandler_MessageRead_NoPublish_EarlyReturnNoContent(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-time.Hour)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1", JoinedAt: joined,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: nil}, nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	assert.Empty(t, f.coreSubjects, "no subscription.update on early-return (no content)")
}

// On the early-return path (user already past lastMsgAt), no subscription.update is fired.
func TestHandler_MessageRead_NoPublish_EarlyReturnAlreadyRead(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-3 * time.Hour)
	lastMsg := joined.Add(time.Hour)
	lastSeen := lastMsg.Add(time.Hour) // user already read past the last message

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	assert.Empty(t, f.coreSubjects, "no subscription.update on early-return (already past lastMsgAt)")
}

// publishCore failure on subscription.update must be logged but must NOT fail the RPC.
func TestHandler_MessageRead_PublishFailure_NonFatal(t *testing.T) {
	f := newMessageReadFixture(t)
	f.coreErr = fmt.Errorf("nats down")
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", gomock.Any()).Return(nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err, "publishCore failure on subscription.update must not fail the RPC")
}

// Cross-site messageRead: inbox publish fires for the floor fan-out, AND the
// subscription.update still goes to core NATS for the reader's own account.
func TestHandler_MessageRead_CrossSite_AlsoPublishesSubscriptionUpdate(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", LastMsgAt: &lastMsg}, nil)
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&lastSeen, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", gomock.Any()).Return(nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	// Inbox publish for cross-site floor fan-out.
	assert.Equal(t, 1, f.publishCalls)
	// subscription.update via core NATS.
	require.Len(t, f.coreSubjects, 1)
	assert.Equal(t, "chat.user.alice.event.subscription.update", f.coreSubjects[0])
	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(f.coreData[0], &evt))
	assert.Equal(t, "read", evt.Action)
}

func TestHandler_handleMessageReadReceipt(t *testing.T) {
	const (
		siteID    = "site-a"
		account   = "alice"
		roomID    = "r1"
		messageID = "m1"
	)
	createdAt := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	req := model.ReadReceiptRequest{MessageID: messageID}

	type setup struct {
		store  *MockRoomStore
		reader *MockMessageReader
	}
	type tc struct {
		name      string
		req       model.ReadReceiptRequest
		prep      func(s setup)
		wantErr   error
		wantSubst string
		wantReply *model.ReadReceiptResponse
	}

	tests := []tc{
		{
			name: "happy path",
			req:  req,
			prep: func(s setup) {
				s.store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				s.reader.EXPECT().GetMessageReadMeta(gomock.Any(), account, roomID, messageID).
					Return(MessageReadMeta{RoomID: roomID, CreatedAt: createdAt, Sender: account}, true, nil)
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
			name: "empty readers",
			req:  req,
			prep: func(s setup) {
				s.store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				s.reader.EXPECT().GetMessageReadMeta(gomock.Any(), account, roomID, messageID).
					Return(MessageReadMeta{RoomID: roomID, CreatedAt: createdAt, Sender: account}, true, nil)
				s.store.EXPECT().ListReadReceipts(gomock.Any(), roomID, createdAt, account, gomock.Any()).
					Return([]ReadReceiptRow{}, nil)
			},
			wantReply: &model.ReadReceiptResponse{Readers: []model.ReadReceiptEntry{}},
		},
		{
			// #443: a thread-only reply resolves readers from thread read-state, not the room.
			name: "thread-only reply uses thread read receipts",
			req:  req,
			prep: func(s setup) {
				s.store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				s.reader.EXPECT().GetMessageReadMeta(gomock.Any(), account, roomID, messageID).
					Return(MessageReadMeta{RoomID: roomID, CreatedAt: createdAt, Sender: account, ThreadRoomID: "tr-1", ThreadOnly: true}, true, nil)
				s.store.EXPECT().ListThreadReadReceipts(gomock.Any(), "tr-1", createdAt, account, gomock.Any()).
					Return([]ReadReceiptRow{{UserID: "uB", Account: "bob", ChineseName: "鮑勃", EngName: "Bob"}}, nil)
			},
			wantReply: &model.ReadReceiptResponse{
				Readers: []model.ReadReceiptEntry{
					{UserID: "uB", Account: "bob", ChineseName: "鮑勃", EngName: "Bob"},
				},
			},
		},
		{
			name:      "empty messageID",
			req:       model.ReadReceiptRequest{},
			wantSubst: "messageId",
		},
		{
			name: "not a room member",
			req:  req,
			prep: func(s setup) {
				s.store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(model.ErrSubscriptionNotFound)
				s.reader.EXPECT().GetMessageReadMeta(gomock.Any(), account, roomID, messageID).
					Return(MessageReadMeta{RoomID: roomID, CreatedAt: createdAt, Sender: account}, true, nil).AnyTimes()
			},
			wantErr: errNotRoomMember,
		},
		{
			name: "message not found",
			req:  req,
			prep: func(s setup) {
				s.store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				s.reader.EXPECT().GetMessageReadMeta(gomock.Any(), account, roomID, messageID).
					Return(MessageReadMeta{}, false, nil)
			},
			wantErr: errMessageNotFound,
		},
		{
			name: "message in another room",
			req:  req,
			prep: func(s setup) {
				s.store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				s.reader.EXPECT().GetMessageReadMeta(gomock.Any(), account, roomID, messageID).
					Return(MessageReadMeta{RoomID: "other-room", CreatedAt: createdAt, Sender: account}, true, nil)
			},
			wantErr: errMessageRoomMismatch,
		},
		{
			name: "not the sender",
			req:  req,
			prep: func(s setup) {
				s.store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				s.reader.EXPECT().GetMessageReadMeta(gomock.Any(), account, roomID, messageID).
					Return(MessageReadMeta{RoomID: roomID, CreatedAt: createdAt, Sender: "bob"}, true, nil)
			},
			wantErr: errNotMessageSender,
		},
		{
			name: "store error on subscription",
			req:  req,
			prep: func(s setup) {
				s.store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(fmt.Errorf("db down"))
				s.reader.EXPECT().GetMessageReadMeta(gomock.Any(), account, roomID, messageID).
					Return(MessageReadMeta{RoomID: roomID, CreatedAt: createdAt, Sender: account}, true, nil).AnyTimes()
			},
			wantSubst: "db down",
		},
		{
			name: "store error on message lookup",
			req:  req,
			prep: func(s setup) {
				s.store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				s.reader.EXPECT().GetMessageReadMeta(gomock.Any(), account, roomID, messageID).
					Return(MessageReadMeta{}, false, fmt.Errorf("cass down"))
			},
			wantSubst: "cass down",
		},
		{
			name: "store error on aggregation",
			req:  req,
			prep: func(s setup) {
				s.store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				s.reader.EXPECT().GetMessageReadMeta(gomock.Any(), account, roomID, messageID).
					Return(MessageReadMeta{RoomID: roomID, CreatedAt: createdAt, Sender: account}, true, nil)
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

			h := NewHandler(store, nil, nil, reader, siteID, 1000, 1000, time.Second, 5, nil, nil, nil, 0)
			got, err := h.messageReadReceipt(ctxParams(map[string]string{"account": account, "roomID": roomID}), tt.req)

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
			require.Equal(t, tt.wantReply, got)
		})
	}
}

// The reader may return an errcode.Unavailable (e.g. history-service down). The
// handler wraps it with fmt.Errorf("get message: %w", ...); this verifies the
// reason still survives to the boundary so the client gets read_receipts_unavailable.
func TestHandler_MessageReadReceipt_UnavailablePreservesReason(t *testing.T) {
	const (
		account   = "alice"
		roomID    = "r1"
		siteID    = "site-a"
		messageID = "m1"
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	store := NewMockRoomStore(ctrl)
	reader := NewMockMessageReader(ctrl)

	store.EXPECT().CheckMembership(gomock.Any(), account, roomID).Return(nil)
	reader.EXPECT().GetMessageReadMeta(gomock.Any(), account, roomID, messageID).
		Return(MessageReadMeta{}, false, errcode.Unavailable("read receipts are temporarily unavailable",
			errcode.WithReason(errcode.RoomReadReceiptsUnavailable)))

	h := NewHandler(store, nil, nil, reader, siteID, 1000, 1000, time.Second, 5, nil, nil, nil, 0)
	_, err := h.messageReadReceipt(ctxParams(map[string]string{"account": account, "roomID": roomID}),
		model.ReadReceiptRequest{MessageID: messageID})

	require.Error(t, err)
	assert.True(t, errcode.HasReason(err, errcode.RoomReadReceiptsUnavailable))
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

	resp, err := h.ensureRoomKey(ctxParams(map[string]string{}), model.RoomKeyEnsureRequest{RoomID: "room-abc"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "room-abc", resp.RoomID)
	assert.Equal(t, 7, resp.Version)

	respJSON := mustJSON(t, resp)
	assert.NotContains(t, string(respJSON), "publicKey", "response must not include public key bytes")
	assert.NotContains(t, string(respJSON), "privateKey", "response must not include private key bytes")
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

	resp, err := h.ensureRoomKey(ctxParams(map[string]string{}), model.RoomKeyEnsureRequest{RoomID: "room-new"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "room-new", resp.RoomID)
	assert.Equal(t, 0, resp.Version)

	assert.Len(t, capturedPair.PrivateKey, 32, "room secret must be 32 bytes")
	respJSON := mustJSON(t, resp)
	assert.NotContains(t, string(respJSON), "publicKey", "response must not include public key bytes")
	assert.NotContains(t, string(respJSON), "privateKey", "response must not include private key bytes")
}

func TestHandler_EnsureRoomKey_MissingRoomID(t *testing.T) {
	ctrl := gomock.NewController(t)
	keyStore := NewMockRoomKeyStore(ctrl)
	h := &Handler{keyStore: keyStore, siteID: "site-local"}

	_, err := h.ensureRoomKey(ctxParams(map[string]string{}), model.RoomKeyEnsureRequest{RoomID: ""})
	require.Error(t, err)
}

func TestHandler_EnsureRoomKey_GetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	keyStore := NewMockRoomKeyStore(ctrl)
	keyStore.EXPECT().Get(gomock.Any(), "room-err").Return(nil, errors.New("valkey down"))

	h := &Handler{keyStore: keyStore, siteID: "site-local"}

	_, err := h.ensureRoomKey(ctxParams(map[string]string{}), model.RoomKeyEnsureRequest{RoomID: "room-err"})
	require.Error(t, err)
}

func TestHandler_EnsureRoomKey_SetError(t *testing.T) {
	ctrl := gomock.NewController(t)
	keyStore := NewMockRoomKeyStore(ctrl)
	keyStore.EXPECT().Get(gomock.Any(), "room-setfail").Return(nil, nil)
	keyStore.EXPECT().Set(gomock.Any(), "room-setfail", gomock.Any()).Return(0, errors.New("write failed"))

	h := &Handler{keyStore: keyStore, siteID: "site-local"}

	_, err := h.ensureRoomKey(ctxParams(map[string]string{}), model.RoomKeyEnsureRequest{RoomID: "room-setfail"})
	require.Error(t, err)
}

func TestHandler_EnsureRoomKey_NilKeyStore(t *testing.T) {
	h := &Handler{keyStore: nil, siteID: "site-local"}

	_, err := h.ensureRoomKey(ctxParams(map[string]string{}), model.RoomKeyEnsureRequest{RoomID: "room-abc"})
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
	coreSubjects   []string
	coreData       [][]byte
	coreErr        error
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
		publishCore: func(_ context.Context, subj string, data []byte) error {
			f.coreSubjects = append(f.coreSubjects, subj)
			f.coreData = append(f.coreData, data)
			return f.coreErr
		},
	}
	return f
}

// withNopFloor stubs the thread-floor store methods for tests that do not
// exercise floor recompute — returns nil thread room so recomputeThreadFloor
// exits early without further store calls.
func withNopFloor(f *threadReadFixture) {
	f.store.EXPECT().GetThreadRoomByID(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
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

func TestHandler_MessageThreadRead_EmptyThreadID(t *testing.T) {
	f := newThreadReadFixture(t)
	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: ""})
	require.ErrorIs(t, err, errInvalidThreadID)
}

func TestHandler_MessageThreadRead_NotRoomMember(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(model.ErrSubscriptionNotFound)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil).AnyTimes()
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil).AnyTimes()

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.ErrorIs(t, err, errNotRoomMember)
	assert.Equal(t, 0, f.publishCalls)
}

// A caller who is a room member but does not follow the thread has no thread-read
// state to advance; the mark-as-read is an idempotent no-op that returns success
// rather than an error. No thread-read writes or floor recompute run (strict mock
// on the update methods and no withNopFloor stub enforce this).
func TestHandler_MessageThreadRead_NoThreadSub_ReturnsSuccess(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(nil, model.ErrThreadSubscriptionNotFound)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil).AnyTimes()

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
	assert.Equal(t, 0, f.publishCalls)
}

// Regression for the errgroup.WithContext bug: against real Mongo, when one
// goroutine fails with ErrThreadSubscriptionNotFound, an errgroup.WithContext
// cancels the others, causing them to return context.Canceled. The tsub-not-found
// branch must still be evaluated before the generic subErr branch, so a sibling's
// context.Canceled never masks the success no-op with an internal error.
// Simulate by returning context.Canceled on the siblings.
func TestHandler_MessageThreadRead_NoThreadSub_SiblingsCancelled(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(context.Canceled).AnyTimes()
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(nil, model.ErrThreadSubscriptionNotFound)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").
		Return("", context.Canceled).AnyTimes()

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
}

func TestHandler_MessageThreadRead_BothMiss_RoomNotMemberWins(t *testing.T) {
	for i := 0; i < 20; i++ {
		f := newThreadReadFixture(t)
		f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
			Return(model.ErrSubscriptionNotFound)
		f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
			Return(nil, model.ErrThreadSubscriptionNotFound).AnyTimes()
		f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil).AnyTimes()

		_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
		require.ErrorIs(t, err, errNotRoomMember, "iteration %d", i)
	}
}

func TestHandler_MessageThreadRead_HappyAlertClears(t *testing.T) {
	f := newThreadReadFixture(t)
	withNopFloor(f)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", "p1").
		Return(nil, false, nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_HappyAlertStays(t *testing.T) {
	f := newThreadReadFixture(t)
	withNopFloor(f)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", "p1").
		Return([]string{"p2"}, true, nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
}

func TestHandler_MessageThreadRead_IdempotentIDNotInArray(t *testing.T) {
	f := newThreadReadFixture(t)
	withNopFloor(f)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", "p1").
		Return([]string{"p2"}, true, nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
}

func TestHandler_MessageThreadRead_AlertAlreadyFalse(t *testing.T) {
	f := newThreadReadFixture(t)
	withNopFloor(f)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", "p1").
		Return(nil, false, nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
}

func TestHandler_MessageThreadRead_CrossSite_PublishesInbox(t *testing.T) {
	f := newThreadReadFixture(t)
	withNopFloor(f)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", "p1").
		Return([]string{"p2"}, true, nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)

	require.Equal(t, 1, f.publishCalls)
	assert.Equal(t, subject.Outbox("site-a", "site-b", model.InboxThreadRead), f.publishedSubj)
	var fed model.OutboxEvent
	require.NoError(t, json.Unmarshal(f.publishedData, &fed))
	var outer model.InboxEvent
	require.NoError(t, json.Unmarshal(fed.Envelope, &outer))
	assert.Equal(t, model.InboxThreadRead, outer.Type)
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
	withNopFloor(f)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(nil, false, nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("", nil)

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_GetUserSiteID_Error(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("", fmt.Errorf("boom"))
	// Writes are short-circuited by the read-phase error, but may race ahead.
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, false, nil).AnyTimes()
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).AnyTimes()

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_InboxPublishError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.publishCallErr = fmt.Errorf("nats down")
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(nil, false, nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.Error(t, err)
	require.Equal(t, 1, f.publishCalls)
}

func TestHandler_MessageThreadRead_UpdateSubscriptionError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(nil, false, fmt.Errorf("mongo down"))
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(nil).AnyTimes()

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_MessageThreadRead_UpdateThreadSubscriptionError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(baseThreadSub("alice", "r1", "p1", "tr1"), nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(nil, false, nil).AnyTimes()
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), "tr1", "alice", gomock.Any()).
		Return(fmt.Errorf("mongo down"))

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

// --- clear-all-thread-read (bulk) tests ---

func unwrapThreadReadAllPayload(t *testing.T, outboxData []byte) model.ThreadReadAllEvent {
	t.Helper()
	var fed model.OutboxEvent
	require.NoError(t, json.Unmarshal(outboxData, &fed))
	var env model.InboxEvent
	require.NoError(t, json.Unmarshal(fed.Envelope, &env))
	require.Equal(t, model.InboxThreadReadAll, env.Type)
	var ev model.ThreadReadAllEvent
	require.NoError(t, json.Unmarshal(env.Payload, &ev))
	return ev
}

func TestHandler_ClearAllThreadRead_LocalUser_NoFederation(t *testing.T) {
	f := newThreadReadFixture(t) // fixture handler is scoped to site-a
	f.store.EXPECT().ClearThreadSubscriptionsForAccount(gomock.Any(), "alice", gomock.Any()).Return(nil)
	f.store.EXPECT().ClearSubscriptionThreadUnreadForAccount(gomock.Any(), "alice").Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil) // user home is the handler's own site

	_, err := f.handler.clearAllThreadRead(ctxParams(map[string]string{"account": "alice"}), model.RoomThreadReadAllRequest{Account: "alice"})
	require.NoError(t, err)
	assert.Equal(t, 0, f.publishCalls) // no cross-site federation for a home-local user
}

func TestHandler_ClearAllThreadRead_RemoteUser_FederatesOneEvent(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().ClearThreadSubscriptionsForAccount(gomock.Any(), "alice", gomock.Any()).Return(nil)
	f.store.EXPECT().ClearSubscriptionThreadUnreadForAccount(gomock.Any(), "alice").Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil) // remote home

	_, err := f.handler.clearAllThreadRead(ctxParams(map[string]string{"account": "alice"}), model.RoomThreadReadAllRequest{Account: "alice"})
	require.NoError(t, err)
	assert.Equal(t, 1, f.publishCalls) // one bulk-dismiss event, not one per thread

	ev := unwrapThreadReadAllPayload(t, f.publishedData)
	assert.Equal(t, "alice", ev.Account)
	assert.NotZero(t, ev.LastSeenAt)
}

func TestHandler_ClearAllThreadRead_EmptyAccount_BadRequest(t *testing.T) {
	f := newThreadReadFixture(t)
	_, err := f.handler.clearAllThreadRead(ctxParams(map[string]string{}), model.RoomThreadReadAllRequest{Account: "  "})
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_ClearAllThreadRead_ClearThreadSubsError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.store.EXPECT().ClearThreadSubscriptionsForAccount(gomock.Any(), "alice", gomock.Any()).
		Return(fmt.Errorf("mongo down"))
	f.store.EXPECT().ClearSubscriptionThreadUnreadForAccount(gomock.Any(), "alice").Return(nil).AnyTimes()
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil).AnyTimes()

	_, err := f.handler.clearAllThreadRead(ctxParams(map[string]string{"account": "alice"}), model.RoomThreadReadAllRequest{Account: "alice"})
	require.Error(t, err)
	assert.Equal(t, 0, f.publishCalls)
}

func TestHandler_ClearAllThreadRead_FederatePublishError(t *testing.T) {
	f := newThreadReadFixture(t)
	f.publishCallErr = fmt.Errorf("nats down")
	f.store.EXPECT().ClearThreadSubscriptionsForAccount(gomock.Any(), "alice", gomock.Any()).Return(nil)
	f.store.EXPECT().ClearSubscriptionThreadUnreadForAccount(gomock.Any(), "alice").Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-b", nil)

	_, err := f.handler.clearAllThreadRead(ctxParams(map[string]string{"account": "alice"}), model.RoomThreadReadAllRequest{Account: "alice"})
	require.Error(t, err)
	assert.Equal(t, 1, f.publishCalls)
}

// --- thread floor recompute tests ---

// baseThreadRoomForFloor returns a minimal ThreadRoom with a non-zero LastMsgAt
// so the floor-recompute skip guard does not trip.
func baseThreadRoomForFloor(threadRoomID string) *model.ThreadRoom {
	lastMsg := time.Now().UTC().Add(-30 * time.Minute)
	return &model.ThreadRoom{
		ID:        threadRoomID,
		RoomID:    "r1",
		LastMsgAt: lastMsg,
	}
}

func fullThreadReadSetup(f *threadReadFixture, tsub *model.ThreadSubscription) {
	f.store.EXPECT().CheckMembership(gomock.Any(), "alice", "r1").
		Return(nil)
	f.store.EXPECT().GetThreadSubscriptionByParent(gomock.Any(), "alice", "p1", "r1").
		Return(tsub, nil)
	f.store.EXPECT().UpdateSubscriptionThreadRead(gomock.Any(), "r1", "alice", "p1").
		Return(nil, false, nil)
	f.store.EXPECT().UpdateThreadSubscriptionRead(gomock.Any(), tsub.ThreadRoomID, "alice", gomock.Any()).
		Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
}

func TestHandler_MessageThreadRead_FloorRecomputed_FloorChanges(t *testing.T) {
	f := newThreadReadFixture(t)
	tsub := baseThreadSub("alice", "r1", "p1", "tr1")
	fullThreadReadSetup(f, tsub)

	tr := baseThreadRoomForFloor("tr1")
	f.store.EXPECT().GetThreadRoomByID(gomock.Any(), "tr1").Return(tr, nil)
	minT := time.Now().UTC().Add(-10 * time.Minute)
	f.store.EXPECT().MinThreadSubscriptionLastSeenByThreadRoomID(gomock.Any(), "tr1").Return(&minT, nil)
	f.store.EXPECT().UpdateThreadRoomMinUserLastSeenAt(gomock.Any(), "tr1", &minT).Return(nil)
	// Parent room type unset (botDM) → fan-out is a no-op.
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeBotDM}, nil)

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
	assert.Empty(t, f.coreSubjects, "botDM parent gets no fan-out")
}

func TestHandler_MessageThreadRead_FloorUnchanged_NoWrite(t *testing.T) {
	f := newThreadReadFixture(t)
	tsub := baseThreadSub("alice", "r1", "p1", "tr1")
	fullThreadReadSetup(f, tsub)

	existingFloor := time.Now().UTC().Add(-10 * time.Minute)
	tr := baseThreadRoomForFloor("tr1")
	tr.MinUserLastSeenAt = &existingFloor
	// Distinct pointer to the same value: asserts value-equality, not pointer-identity.
	computedFloor := existingFloor
	f.store.EXPECT().GetThreadRoomByID(gomock.Any(), "tr1").Return(tr, nil)
	f.store.EXPECT().MinThreadSubscriptionLastSeenByThreadRoomID(gomock.Any(), "tr1").Return(&computedFloor, nil)
	// UpdateThreadRoomMinUserLastSeenAt must NOT be called when floor is unchanged.

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
}

func TestHandler_MessageThreadRead_FloorNilNoSubscribers_WritesNil(t *testing.T) {
	f := newThreadReadFixture(t)
	tsub := baseThreadSub("alice", "r1", "p1", "tr1")
	fullThreadReadSetup(f, tsub)

	existingFloor := time.Now().UTC().Add(-10 * time.Minute)
	tr := baseThreadRoomForFloor("tr1")
	tr.MinUserLastSeenAt = &existingFloor
	f.store.EXPECT().GetThreadRoomByID(gomock.Any(), "tr1").Return(tr, nil)
	// Min returns nil — someone hasn't read yet.
	f.store.EXPECT().MinThreadSubscriptionLastSeenByThreadRoomID(gomock.Any(), "tr1").Return(nil, nil)
	f.store.EXPECT().UpdateThreadRoomMinUserLastSeenAt(gomock.Any(), "tr1", (*time.Time)(nil)).Return(nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeBotDM}, nil)

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
}

func TestHandler_MessageThreadRead_ThreadRoomMissing_SkipsFloor(t *testing.T) {
	f := newThreadReadFixture(t)
	tsub := baseThreadSub("alice", "r1", "p1", "tr1")
	fullThreadReadSetup(f, tsub)

	f.store.EXPECT().GetThreadRoomByID(gomock.Any(), "tr1").Return(nil, nil)
	// No further floor calls expected.

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
}

func TestHandler_MessageThreadRead_FloorError_BestEffortAccepted(t *testing.T) {
	f := newThreadReadFixture(t)
	tsub := baseThreadSub("alice", "r1", "p1", "tr1")
	fullThreadReadSetup(f, tsub)

	f.store.EXPECT().GetThreadRoomByID(gomock.Any(), "tr1").Return(nil, fmt.Errorf("mongo down"))
	// Floor failure must not fail the RPC.

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)
}

// expectThreadFloorAdvance stubs the floor recompute so it produces a real
// change (write fires), leaving the GetRoom fan-out routing to the caller.
func expectThreadFloorAdvance(f *threadReadFixture, minT *time.Time) {
	tr := baseThreadRoomForFloor("tr1")
	f.store.EXPECT().GetThreadRoomByID(gomock.Any(), "tr1").Return(tr, nil)
	f.store.EXPECT().MinThreadSubscriptionLastSeenByThreadRoomID(gomock.Any(), "tr1").Return(minT, nil)
	f.store.EXPECT().UpdateThreadRoomMinUserLastSeenAt(gomock.Any(), "tr1", minT).Return(nil)
}

func TestHandler_MessageThreadRead_ChannelFloorMoves_PublishesRoomEvent(t *testing.T) {
	f := newThreadReadFixture(t)
	fullThreadReadSetup(f, baseThreadSub("alice", "r1", "p1", "tr1"))
	minT := time.Now().UTC().Add(-10 * time.Minute)
	expectThreadFloorAdvance(f, &minT)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)

	require.Len(t, f.coreSubjects, 1)
	assert.Equal(t, subject.RoomEvent("r1"), f.coreSubjects[0])
	var evt model.ThreadMessageReadEvent
	require.NoError(t, json.Unmarshal(f.coreData[0], &evt))
	assert.Equal(t, model.RoomEventThreadMessageRead, evt.Type)
	assert.Equal(t, "r1", evt.RoomID)
	assert.Equal(t, "tr1", evt.ThreadRoomID)
	require.NotNil(t, evt.MinUserLastSeenAt)
	assert.True(t, evt.MinUserLastSeenAt.Equal(minT))
	assert.NotZero(t, evt.Timestamp)
}

func TestHandler_MessageThreadRead_DMFloorMoves_PublishesPerSubscriber(t *testing.T) {
	f := newThreadReadFixture(t)
	fullThreadReadSetup(f, baseThreadSub("alice", "r1", "p1", "tr1"))
	minT := time.Now().UTC().Add(-10 * time.Minute)
	expectThreadFloorAdvance(f, &minT)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeDM}, nil)
	f.store.EXPECT().ListSubscriptionsByRoom(gomock.Any(), "r1").Return([]model.Subscription{
		{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1"},
		{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1"},
	}, nil)

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)

	require.Len(t, f.coreSubjects, 2)
	assert.Equal(t, subject.UserRoomEvent("alice"), f.coreSubjects[0])
	assert.Equal(t, subject.UserRoomEvent("bob"), f.coreSubjects[1])
	var evt model.ThreadMessageReadEvent
	require.NoError(t, json.Unmarshal(f.coreData[1], &evt))
	assert.Equal(t, model.RoomEventThreadMessageRead, evt.Type)
	assert.Equal(t, "tr1", evt.ThreadRoomID)
}

func TestHandler_MessageThreadRead_ChannelNilFloor_OmitsFloorField(t *testing.T) {
	f := newThreadReadFixture(t)
	fullThreadReadSetup(f, baseThreadSub("alice", "r1", "p1", "tr1"))
	existingFloor := time.Now().UTC().Add(-10 * time.Minute)
	tr := baseThreadRoomForFloor("tr1")
	tr.MinUserLastSeenAt = &existingFloor // a non-nil → nil transition still advances the floor
	f.store.EXPECT().GetThreadRoomByID(gomock.Any(), "tr1").Return(tr, nil)
	f.store.EXPECT().MinThreadSubscriptionLastSeenByThreadRoomID(gomock.Any(), "tr1").Return(nil, nil)
	f.store.EXPECT().UpdateThreadRoomMinUserLastSeenAt(gomock.Any(), "tr1", (*time.Time)(nil)).Return(nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)

	_, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err)

	require.Len(t, f.coreSubjects, 1)
	assert.NotContains(t, string(f.coreData[0]), "minUserLastSeenAt")
}

func TestHandler_MessageThreadRead_ChannelPublishError_StillAccepted(t *testing.T) {
	f := newThreadReadFixture(t)
	f.coreErr = fmt.Errorf("nats down")
	fullThreadReadSetup(f, baseThreadSub("alice", "r1", "p1", "tr1"))
	minT := time.Now().UTC().Add(-10 * time.Minute)
	expectThreadFloorAdvance(f, &minT)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err, "fan-out publish failure must not fail the RPC")
	assert.Equal(t, "accepted", resp.Status)
}

func TestHandler_MessageThreadRead_GetParentRoomError_StillAccepted(t *testing.T) {
	f := newThreadReadFixture(t)
	fullThreadReadSetup(f, baseThreadSub("alice", "r1", "p1", "tr1"))
	minT := time.Now().UTC().Add(-10 * time.Minute)
	expectThreadFloorAdvance(f, &minT)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, fmt.Errorf("mongo down"))

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err, "parent-room lookup failure must not fail the RPC")
	assert.Equal(t, "accepted", resp.Status)
	assert.Empty(t, f.coreSubjects)
}

func TestHandler_MessageThreadRead_ParentRoomNil_StillAccepted(t *testing.T) {
	f := newThreadReadFixture(t)
	fullThreadReadSetup(f, baseThreadSub("alice", "r1", "p1", "tr1"))
	minT := time.Now().UTC().Add(-10 * time.Minute)
	expectThreadFloorAdvance(f, &minT)
	// GetRoom can return (nil, nil) in this store family (cf. GetThreadRoomByID);
	// a missing parent room must be a no-op, never a nil-deref panic.
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, nil)

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err, "missing parent room must not fail the RPC")
	assert.Equal(t, "accepted", resp.Status)
	assert.Empty(t, f.coreSubjects)
}

func TestHandler_MessageThreadRead_DMListSubscriptionsError_StillAccepted(t *testing.T) {
	f := newThreadReadFixture(t)
	fullThreadReadSetup(f, baseThreadSub("alice", "r1", "p1", "tr1"))
	minT := time.Now().UTC().Add(-10 * time.Minute)
	expectThreadFloorAdvance(f, &minT)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeDM}, nil)
	f.store.EXPECT().ListSubscriptionsByRoom(gomock.Any(), "r1").Return(nil, fmt.Errorf("mongo down"))

	resp, err := f.handler.messageThreadRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}), model.MessageThreadReadRequest{ThreadID: "p1"})
	require.NoError(t, err, "DM subscription-list failure must not fail the RPC")
	assert.Equal(t, "accepted", resp.Status)
	assert.Empty(t, f.coreSubjects)
}

func TestHandler_MuteToggle_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	var muteTs time.Time
	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice", gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, ts time.Time) (*model.Subscription, error) {
			muteTs = ts
			return &model.Subscription{
				ID:     "s1",
				User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
				RoomID: "r1",
				SiteID: "site-a",
				Muted:  true,
			}, nil
		})
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("site-a", nil) // same site → no inbox publish

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

	resp, err := h.muteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	assert.Equal(t, "ok", resp.Status)
	assert.True(t, resp.Muted)

	require.Len(t, coreSubjects, 1)
	assert.Equal(t, subject.SubscriptionUpdate("alice"), coreSubjects[0])

	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(coreBodies[0], &evt))
	assert.Equal(t, "mute_toggled", evt.Action)
	assert.True(t, evt.Subscription.Muted)
	assert.Equal(t, "alice", evt.Subscription.User.Account)
	// The origin doc's muteUpdatedAt and the published event timestamp must be the
	// same instant so remote replicas guard against one high-water mark.
	assert.False(t, muteTs.IsZero())
	assert.Equal(t, muteTs.UnixMilli(), evt.Timestamp)

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

func TestHandler_MuteToggle_CrossSitePublishesInbox(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	var muteTs time.Time
	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice", gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, ts time.Time) (*model.Subscription, error) {
			muteTs = ts
			return &model.Subscription{
				User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
				RoomID: "r1",
				SiteID: "site-a",
				Muted:  true,
			}, nil
		})
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

	_, err := h.muteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	// mute_toggled federates via the OUTBOX relay, not a direct INBOX publish.
	assert.Equal(t, subject.Outbox("site-a", "site-b", model.InboxSubscriptionMuteToggled), streamSubj)
	var fed model.OutboxEvent
	require.NoError(t, json.Unmarshal(streamData, &fed))

	var inboxEnv model.InboxEvent
	require.NoError(t, json.Unmarshal(fed.Envelope, &inboxEnv))
	assert.Equal(t, model.InboxSubscriptionMuteToggled, inboxEnv.Type)
	assert.Equal(t, "site-a", inboxEnv.SiteID)
	assert.Equal(t, "site-b", inboxEnv.DestSiteID)

	var payload model.SubscriptionMuteToggledEvent
	require.NoError(t, json.Unmarshal(inboxEnv.Payload, &payload))
	assert.Equal(t, "alice", payload.Account)
	assert.Equal(t, "r1", payload.RoomID)
	assert.True(t, payload.Muted)
	assert.NotZero(t, payload.Timestamp)
	// Origin write, inbox envelope, and payload must all carry the same instant
	// so the remote replica guards against one high-water mark.
	assert.False(t, muteTs.IsZero())
	assert.Equal(t, muteTs.UnixMilli(), inboxEnv.Timestamp)
	assert.Equal(t, muteTs.UnixMilli(), payload.Timestamp)
}

func TestHandler_MuteToggle_NotRoomMember(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(nil, model.ErrSubscriptionNotFound)

	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	_, err := h.muteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	assert.ErrorIs(t, err, errNotRoomMember)
}

func TestHandler_MuteToggle_StoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(nil, fmt.Errorf("db down"))

	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	_, err := h.muteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "toggle subscription mute")
}

func TestHandler_MuteToggle_GetUserSiteIDError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(&model.Subscription{
			User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1",
		}, nil)
	store.EXPECT().
		GetUserSiteID(gomock.Any(), "alice").
		Return("", fmt.Errorf("mongo down"))

	h := &Handler{
		store: store, siteID: "site-a",
		// Canonical member event publish happens before GetUserSiteID and is
		// independent of the inbox path — it represents the successful DB mutation.
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	_, err := h.muteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get user siteId")
}

func TestHandler_MuteToggle_CrossSiteInboxPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice", gomock.Any()).
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

	_, err := h.muteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "federate mute-toggled")
}

func TestHandler_natsGetRoomKey(t *testing.T) {
	const (
		siteID  = "site-a"
		account = "alice"
		roomID  = "room-1"
	)

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
				store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				ks.EXPECT().Get(gomock.Any(), roomID).Return(sampleVersioned, nil)
			},
			want: want{replyJSON: `{"roomId":"room-1","version":7,"privateKey":"QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI="}`},
		},
		{
			name: "explicit version, happy path",
			body: []byte(`{"version":3}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				ks.EXPECT().GetByVersion(gomock.Any(), roomID, 3).Return(&sampleKey, nil)
			},
			want: want{replyJSON: `{"roomId":"room-1","version":3,"privateKey":"QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI="}`},
		},
		{
			name: "not a member",
			body: []byte(`{}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(model.ErrSubscriptionNotFound)
			},
			want: want{errSubstr: "only room members"},
		},
		{
			name: "current key absent",
			body: []byte(`{}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				ks.EXPECT().Get(gomock.Any(), roomID).Return(nil, nil)
			},
			want: want{errSubstr: "room key not available"},
		},
		{
			name: "historical version absent",
			body: []byte(`{"version":1}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				ks.EXPECT().GetByVersion(gomock.Any(), roomID, 1).Return(nil, nil)
			},
			want: want{errSubstr: "room key not available"},
		},
		{
			name: "store error",
			body: []byte(`{}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				ks.EXPECT().Get(gomock.Any(), roomID).Return(nil, errors.New("valkey down"))
			},
			want: want{errSubstr: "get room key:"},
		},
		{
			name: "store error on explicit version",
			body: []byte(`{"version":5}`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
				ks.EXPECT().GetByVersion(gomock.Any(), roomID, 5).Return(nil, errors.New("valkey down"))
			},
			want: want{errSubstr: "get room key:"},
		},
		{
			name: "malformed body",
			body: []byte(`not-json`),
			setup: func(t *testing.T, store *MockRoomStore, ks *MockRoomKeyStore) {
				store.EXPECT().CheckMembership(gomock.Any(), account, roomID).
					Return(nil)
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

			h := NewHandler(store, ks, nil, nil, siteID, 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
			c := ctxParams(map[string]string{"account": account, "roomID": roomID})
			c.Msg = &nats.Msg{Data: tc.body}
			resp, err := h.getRoomKey(c)
			if tc.want.errSubstr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.want.errSubstr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)
			// #nosec G117 -- test roundtrip on a model whose PrivateKey field is part of the wire schema
			respJSON, mErr := json.Marshal(resp)
			require.NoError(t, mErr)
			require.JSONEq(t, tc.want.replyJSON, string(respJSON))
		})
	}
}

// TestHandler_GetRoomKey_EmptyBody locks the optional-body contract: a nil
// request body must default to the current-version path, not be rejected.
func TestHandler_GetRoomKey_EmptyBody(t *testing.T) {
	const (
		siteID  = "site-a"
		account = "alice"
		roomID  = "room-1"
	)
	sampleKey := roomkeystore.RoomKeyPair{PrivateKey: bytes.Repeat([]byte{0x42}, 32)}
	sampleVersioned := &roomkeystore.VersionedKeyPair{Version: 7, KeyPair: sampleKey}

	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	ks := NewMockRoomKeyStore(ctrl)
	store.EXPECT().CheckMembership(gomock.Any(), account, roomID).Return(nil)
	ks.EXPECT().Get(gomock.Any(), roomID).Return(sampleVersioned, nil)

	h := NewHandler(store, ks, nil, nil, siteID, 1000, 500, 5*time.Second, 5, nil, nil, nil, 0)
	c := ctxParams(map[string]string{"account": account, "roomID": roomID})
	c.Msg = &nats.Msg{Data: nil}
	resp, err := h.getRoomKey(c)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, roomID, resp.RoomID)
	assert.Equal(t, 7, resp.Version)
	assert.Equal(t, sampleKey.PrivateKey, resp.PrivateKey)
}

// --- RoomRename tests ---

func TestHandleRoomRename_Validation(t *testing.T) {
	const validReqID = testRequestID

	tests := []struct {
		name       string
		account    string
		roomID     string
		newName    string
		setupStore func(*MockRoomStore)
		wantErr    error
	}{
		{
			name:    "blank name after trim",
			account: "alice",
			roomID:  "r1",
			newName: "   ",
			wantErr: errInvalidName,
		},
		{
			name:    "name too long (>100 chars)",
			account: "alice",
			roomID:  "r1",
			newName: strings.Repeat("x", 101),
			wantErr: errInvalidName,
		},
		{
			name:    "room not found",
			account: "alice",
			roomID:  "r1",
			newName: "new-name",
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, mongo.ErrNoDocuments)
			},
			wantErr: errRoomNotFound,
		},
		{
			name:    "wrong room type (DM)",
			account: "alice",
			roomID:  "r1",
			newName: "new-name",
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice"}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeDM}, nil)
			},
			wantErr: errRenameChannelOnly,
		},
		{
			name:    "non-admin non-owner",
			account: "alice",
			roomID:  "r1",
			newName: "new-name",
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
			name:    "owner subscription allowed",
			account: "alice",
			roomID:  "r1",
			newName: "new-name",
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
			name:    "room admin rejected (only owner or platform admin allowed)",
			account: "alice",
			roomID:  "r1",
			newName: "new-name",
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
			name:    "admin allowed without subscription",
			account: "admin1",
			roomID:  "r1",
			newName: "new-name",
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
				func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, nil, nil, 0)

			resp, err := h.roomRename(
				ctxParams(map[string]string{"account": tt.account, "roomID": tt.roomID}),
				model.RoomRenameRequest{NewName: tt.newName},
			)
			if tt.wantErr == nil {
				require.NoError(t, err)
				require.NotNil(t, resp)
				assert.Equal(t, "accepted", resp.Status)
				assert.Equal(t, validReqID, resp.RequestID)
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
	s.EXPECT().ApplySubscriptionRestriction(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.EXPECT().ListSubscriptionsByRoom(gomock.Any(), "r1").Return(nil, nil)
	s.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return(nil, nil)
}

func TestHandleRoomRestricted_Validation(t *testing.T) {
	const validReqID = testRequestID

	tests := []struct {
		name       string
		req        model.RoomRestrictedRequest
		setupStore func(*MockRoomStore)
		wantErr    error
	}{
		{
			name:    "missing roomID/account in body",
			req:     model.RoomRestrictedRequest{Restricted: true},
			wantErr: errInvalidRestrictedSubject,
		},
		{
			name: "non-admin requester",
			req:  model.RoomRestrictedRequest{RoomID: "r1", Account: "alice", Restricted: true},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "alice").Return(&model.User{Account: "alice", Roles: []model.UserRole{model.UserRoleUser}}, nil)
			},
			wantErr: errOnlyAdmins,
		},
		{
			name: "room not found",
			req:  model.RoomRestrictedRequest{RoomID: "r1", Account: "admin1", Restricted: true},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, mongo.ErrNoDocuments)
			},
			wantErr: errRoomNotFound,
		},
		{
			name: "non-channel room",
			req:  model.RoomRestrictedRequest{RoomID: "r1", Account: "admin1", Restricted: true},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeDM}, nil)
			},
			wantErr: errRestrictedChannelOnly,
		},
		{
			name: "restricted=true + ownerAccount given + owner not a member",
			req: model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1",
				Restricted: true, OwnerAccount: "nonmember",
			},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: true, UserCount: 10}, nil)
				s.EXPECT().CheckMembership(gomock.Any(), "nonmember", "r1").Return(model.ErrSubscriptionNotFound)
			},
			wantErr: errOwnerNotMember,
		},
		{
			name: "transition false→true without ownerAccount",
			req: model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1", Restricted: true,
			},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: false, UserCount: 10}, nil)
			},
			wantErr: errOwnerAccountRequired,
		},
		{
			name: "transition with UserCount < 5 (need at least 5)",
			req: model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1",
				Restricted: true, OwnerAccount: "owner1",
			},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: false, UserCount: 3}, nil)
				s.EXPECT().CheckMembership(gomock.Any(), "owner1", "r1").Return(nil)
			},
			wantErr: errNotEnoughMembers,
		},
		{
			name: "transition success (admin + ownerAccount + UserCount >= 5)",
			req: model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1",
				Restricted: true, OwnerAccount: "owner1",
			},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: false, UserCount: 10}, nil)
				s.EXPECT().CheckMembership(gomock.Any(), "owner1", "r1").Return(nil)
				happyPathRestrictedSuccessSetup(s)
			},
			wantErr: nil,
		},
		{
			name: "unrestrict (no owner/threshold checks)",
			req: model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1", Restricted: false,
			},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: true, UserCount: 10}, nil)
				happyPathRestrictedSuccessSetup(s)
			},
			wantErr: nil,
		},
		{
			name: "already-restricted owner change success",
			req: model.RoomRestrictedRequest{
				RoomID: "r1", Account: "admin1",
				Restricted: true, OwnerAccount: "owner2",
			},
			setupStore: func(s *MockRoomStore) {
				s.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: true, UserCount: 2}, nil)
				s.EXPECT().CheckMembership(gomock.Any(), "owner2", "r1").Return(nil)
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
				func(_ context.Context, _ string, _ []byte, _ string) error { return nil }, nil, nil, 0)

			resp, err := h.roomRestricted(ctxParams(map[string]string{}), tt.req)
			if tt.wantErr == nil {
				require.NoError(t, err)
				require.NotNil(t, resp)
				assert.Equal(t, "ok", resp.Status)
				assert.Equal(t, validReqID, resp.RequestID)
			} else {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

// TestHandleRoomRestricted_MultiSite_FederatesPerDestination verifies the
// cross-site fan-out publishes one OutboxEvent per remote site, each on its own
// destination-scoped OUTBOX subject.
func TestHandleRoomRestricted_MultiSite_FederatesPerDestination(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().GetUser(gomock.Any(), "admin1").Return(&model.User{Account: "admin1", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, Restricted: true, UserCount: 10}, nil)
	store.EXPECT().UpdateRoomVisibility(gomock.Any(), "r1", false, false).Return(nil)
	store.EXPECT().ApplySubscriptionRestriction(gomock.Any(), "r1", false, false, gomock.Any(), gomock.Any()).Return(nil)
	store.EXPECT().ListSubscriptionsByRoom(gomock.Any(), "r1").Return([]model.Subscription{
		{User: model.SubscriptionUser{Account: "alice"}},
		{User: model.SubscriptionUser{Account: "bob"}},
		{User: model.SubscriptionUser{Account: "carol"}},
	}, nil)
	store.EXPECT().FindUsersByAccounts(gomock.Any(), gomock.Any()).Return([]model.User{
		{Account: "alice", SiteID: "site-a"},
		{Account: "bob", SiteID: "site-b"},
		{Account: "carol", SiteID: "site-c"},
	}, nil)

	type pub struct {
		subj string
		data []byte
	}
	var publishes []pub
	h := NewHandler(store, nil, nil, nil, "site-a", 1000, 500, 5*time.Second, 5,
		func(_ context.Context, subj string, data []byte, _ string) error {
			publishes = append(publishes, pub{subj: subj, data: append([]byte(nil), data...)})
			return nil
		}, nil, nil, 0)

	req := model.RoomRestrictedRequest{RoomID: "r1", Account: "admin1", Restricted: false}
	resp, err := h.roomRestricted(ctxParams(map[string]string{}), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// One OUTBOX publish per remote site; destination + event type ride the subject.
	gotSites := make([]string, 0, 2)
	for _, p := range publishes {
		origin, dest, evt, ok := subject.ParseOutbox(p.subj)
		if !ok {
			continue // non-outbox publishes (subscription.update fan-out) are ignored
		}
		assert.Equal(t, "site-a", origin)
		assert.Equal(t, model.InboxRoomRestricted, evt)
		var fed model.OutboxEvent
		require.NoError(t, json.Unmarshal(p.data, &fed))
		var env model.InboxEvent
		require.NoError(t, json.Unmarshal(fed.Envelope, &env))
		assert.Equal(t, model.InboxRoomRestricted, env.Type)
		assert.Equal(t, "site-a", env.SiteID)
		assert.Equal(t, dest, env.DestSiteID)
		var payload model.RoomRestrictedInboxPayload
		require.NoError(t, json.Unmarshal(env.Payload, &payload))
		assert.Equal(t, "r1", payload.RoomID)
		gotSites = append(gotSites, dest)
	}
	assert.ElementsMatch(t, []string{"site-b", "site-c"}, gotSites)
}

func TestHandler_ListMemberStatuses(t *testing.T) {
	const siteID = "site-a"
	const roomID = "r1"
	const requester = "alice"

	stub := []model.MemberStatus{
		{Account: "alice", EngName: "Alice", ChineseName: "愛", StatusIsShow: true, StatusText: "available"},
		{Account: "bob", EngName: "Bob", ChineseName: "博"},
	}

	type want struct {
		errContains string
		errIs       error
		members     []model.MemberStatus
	}
	tests := []struct {
		name      string
		body      []byte
		setupMock func(*MockRoomStore)
		want      want
	}{
		{
			name: "default limit 3, happy path",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 10}, nil)
				s.EXPECT().ListMemberStatuses(gomock.Any(), roomID, 3).Return(stub, nil)
			},
			want: want{members: stub},
		},
		{
			// Nil-Limit must clamp to the room cap, not fail validation. Without
			// the clamp a 2-member room receiving a no-limit request would get
			// errMemberStatusesLimitInvalid from the default-3 vs cap-2 check —
			// an error the client did not cause.
			name: "nil limit clamped to small UserCount",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 2}, nil)
				s.EXPECT().ListMemberStatuses(gomock.Any(), roomID, 2).Return(stub, nil)
			},
			want: want{members: stub},
		},
		{
			// Empty-room short-circuit: no store call, empty response. Without
			// this branch the cap=0 would still trip the explicit-limit guard
			// against the default.
			name: "nil limit + empty room returns empty without store call",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 0}, nil)
			},
			want: want{members: []model.MemberStatus{}},
		},
		{
			name: "explicit limit passes through",
			body: []byte(`{"limit":7}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 10}, nil)
				s.EXPECT().ListMemberStatuses(gomock.Any(), roomID, 7).Return(stub, nil)
			},
			want: want{members: stub},
		},
		{
			// GetRoom is dispatched in parallel with GetSubscription; it may
			// or may not be invoked depending on goroutine timing before
			// errgroup observes the membership error. AnyTimes() accepts
			// both racing outcomes.
			name: "requester not a member",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(fmt.Errorf("missing: %w", model.ErrSubscriptionNotFound))
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil).AnyTimes()
			},
			want: want{errIs: errNotRoomMember},
		},
		{
			// Precedence regression: when BOTH the membership probe and the
			// room read fail concurrently, errNotRoomMember must still win.
			// Plain errgroup.Group (no WithContext) prevents GetRoom's failure
			// from cancelling GetSubscription mid-flight and surfacing as
			// context.Canceled, which would mask the not-member signal.
			name: "not-member takes precedence over GetRoom error",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(fmt.Errorf("missing: %w", model.ErrSubscriptionNotFound))
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errIs: errNotRoomMember},
		},
		{
			name: "GetSubscription infra error",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(fmt.Errorf("mongo exploded"))
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil).AnyTimes()
			},
			want: want{errContains: "check room membership"},
		},
		{
			name: "limit zero",
			body: []byte(`{"limit":0}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(&model.Room{ID: roomID, UserCount: 10}, nil)
			},
			want: want{errIs: errMemberStatusesLimitInvalid},
		},
		{
			name: "limit negative",
			body: []byte(`{"limit":-1}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(&model.Room{ID: roomID, UserCount: 10}, nil)
			},
			want: want{errIs: errMemberStatusesLimitInvalid},
		},
		{
			name: "limit exceeds room.UserCount",
			body: []byte(`{"limit":11}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(&model.Room{ID: roomID, UserCount: 10}, nil)
			},
			want: want{errIs: errMemberStatusesLimitInvalid},
		},
		{
			name: "limit equal to room.UserCount is valid",
			body: []byte(`{"limit":10}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(&model.Room{ID: roomID, UserCount: 10}, nil)
				s.EXPECT().ListMemberStatuses(gomock.Any(), roomID, 10).Return(stub, nil)
			},
			want: want{members: stub},
		},
		{
			name: "GetRoom errors",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "get room"},
		},
		{
			name: "store errors",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(&model.Room{ID: roomID, UserCount: 10}, nil)
				s.EXPECT().ListMemberStatuses(gomock.Any(), roomID, 3).
					Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "list member statuses"},
		},
		{
			// Body parse now precedes the parallel store dispatch; a malformed
			// body short-circuits before any read.
			name:      "malformed JSON body",
			body:      []byte("{not json"),
			setupMock: func(s *MockRoomStore) {},
			want:      want{errContains: "invalid request"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			tc.setupMock(store)

			h := &Handler{store: store, siteID: siteID}
			c := ctxParams(map[string]string{"account": requester, "roomID": roomID})
			c.Msg = &nats.Msg{Data: tc.body}
			resp, err := h.listMemberStatuses(c)

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

// publishCore failure is intentionally non-fatal: the DB write is the source
// of truth and other client sessions reconcile on their next subscription
// refetch. The handler must still reply ok.
func TestHandler_MuteToggle_CorePublishFailureIsNonFatal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionMute(gomock.Any(), "r1", "alice", gomock.Any()).
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

	resp, err := h.muteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err, "publishCore failure must be non-fatal — DB write is the source of truth")

	assert.Equal(t, "ok", resp.Status)
	assert.True(t, resp.Muted)
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
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice", gomock.Any()).
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

	resp, err := h.favoriteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	assert.Equal(t, "ok", resp.Status)
	assert.True(t, resp.Favorite)

	require.Len(t, coreSubjects, 1)
	assert.Equal(t, subject.SubscriptionUpdate("alice"), coreSubjects[0])

	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(coreBodies[0], &evt))
	assert.Equal(t, "favorite_toggled", evt.Action)
	assert.True(t, evt.Subscription.Favorite)
	assert.Equal(t, "alice", evt.Subscription.User.Account)
}

func TestHandler_FavoriteToggle_CrossSitePublishesInbox(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	var favoriteTs time.Time
	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice", gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, ts time.Time) (*model.Subscription, error) {
			favoriteTs = ts
			return &model.Subscription{
				User:     model.SubscriptionUser{ID: "u1", Account: "alice"},
				RoomID:   "r1",
				SiteID:   "site-a",
				Favorite: true,
			}, nil
		})
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

	_, err := h.favoriteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	assert.Equal(t, subject.Outbox("site-a", "site-b", model.InboxSubscriptionFavoriteToggled), streamSubj)
	var fed model.OutboxEvent
	require.NoError(t, json.Unmarshal(streamData, &fed))

	var inboxEnv model.InboxEvent
	require.NoError(t, json.Unmarshal(fed.Envelope, &inboxEnv))
	assert.Equal(t, model.InboxSubscriptionFavoriteToggled, inboxEnv.Type)
	assert.Equal(t, "site-a", inboxEnv.SiteID)
	assert.Equal(t, "site-b", inboxEnv.DestSiteID)

	var payload model.SubscriptionFavoriteToggledEvent
	require.NoError(t, json.Unmarshal(inboxEnv.Payload, &payload))
	assert.Equal(t, "alice", payload.Account)
	assert.Equal(t, "r1", payload.RoomID)
	assert.True(t, payload.Favorite)
	assert.NotZero(t, payload.Timestamp)
	// The origin doc's favoriteUpdatedAt and the published event timestamp must
	// be the same instant so remote replicas guard against one high-water mark.
	assert.False(t, favoriteTs.IsZero())
	assert.Equal(t, favoriteTs.UnixMilli(), inboxEnv.Timestamp)
	assert.Equal(t, favoriteTs.UnixMilli(), payload.Timestamp)
}

func TestHandler_FavoriteToggle_NotRoomMember(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(nil, model.ErrSubscriptionNotFound)

	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}

	_, err := h.favoriteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	assert.ErrorIs(t, err, errNotRoomMember)
}

func TestHandler_FavoriteToggle_StoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice", gomock.Any()).
		Return(nil, fmt.Errorf("db down"))

	h := &Handler{
		store: store, siteID: "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
	}
	_, err := h.favoriteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "toggle subscription favorite")
}

func TestHandler_FavoriteToggle_GetUserSiteIDError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice", gomock.Any()).
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

	_, err := h.favoriteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get user siteId")
}

func TestHandler_FavoriteToggle_CrossSiteInboxPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice", gomock.Any()).
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

	_, err := h.favoriteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "federate favorite-toggled")
}

func TestHandler_FavoriteToggle_CorePublishFailureIsNonFatal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().
		ToggleSubscriptionFavorite(gomock.Any(), "r1", "alice", gomock.Any()).
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

	resp, err := h.favoriteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err, "publishCore failure must be non-fatal — DB write is the source of truth")

	assert.Equal(t, "ok", resp.Status)
	assert.True(t, resp.Favorite)
}

func TestHandler_marshalBounded(t *testing.T) {
	type sample struct {
		Hello string `json:"hello"`
	}
	big := sample{Hello: strings.Repeat("x", 200)}
	small := sample{Hello: "hi"}

	tests := []struct {
		name             string
		maxResponseBytes int64
		value            any
		wantBodyEmpty    bool
		wantErr          error // errors.Is target; nil = expect no error
		wantErrNonNil    bool  // expect a non-nil error with no specific sentinel
	}{
		{"under cap", 1024, small, false, nil, false},
		{"over cap", 64, big, true, errResponseTooLarge, true},
		{"disabled zero", 0, big, false, nil, false},
		{"disabled negative", -1, big, false, nil, false},
		{"marshal failure", 1024, func() {}, true, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handler{maxResponseBytes: tt.maxResponseBytes}
			body, err := h.marshalBounded(tt.value)
			if tt.wantErrNonNil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			}
			if tt.wantBodyEmpty {
				assert.Nil(t, body)
			} else {
				assert.NotEmpty(t, body)
			}
		})
	}
}

func TestHandler_authorizeRoomAppRead(t *testing.T) {
	tests := []struct {
		name            string
		setupMock       func(*MockRoomStore)
		wantErr         error
		wantErrContains string
	}{
		{
			name: "member allowed",
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
					Return(&model.Subscription{
						User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
						RoomID: "r1",
					}, nil)
			},
			wantErr: nil,
		},
		{
			name: "admin allowed (no sub, admin role, room exists)",
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
					Return(nil, model.ErrSubscriptionNotFound)
				s.EXPECT().GetUser(gomock.Any(), "alice").
					Return(&model.User{ID: "u1", Account: "alice", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").
					Return(&model.Room{ID: "r1"}, nil)
			},
			wantErr: nil,
		},
		{
			name: "denied: admin role but room does not exist",
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
					Return(nil, model.ErrSubscriptionNotFound)
				s.EXPECT().GetUser(gomock.Any(), "alice").
					Return(&model.User{ID: "u1", Account: "alice", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").
					Return(nil, mongo.ErrNoDocuments)
			},
			wantErr: errAppAccessDenied,
		},
		{
			name: "denied: no sub, no admin role",
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
					Return(nil, model.ErrSubscriptionNotFound)
				s.EXPECT().GetUser(gomock.Any(), "alice").
					Return(&model.User{ID: "u1", Account: "alice", Roles: []model.UserRole{model.UserRoleUser}}, nil)
			},
			wantErr: errAppAccessDenied,
		},
		{
			name: "denied: no sub, user not found (cross-site admin path)",
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
					Return(nil, model.ErrSubscriptionNotFound)
				s.EXPECT().GetUser(gomock.Any(), "alice").
					Return(nil, ErrUserNotFound)
			},
			wantErr: errAppAccessDenied,
		},
		{
			name: "transient sub-check error propagates",
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
					Return(nil, errors.New("mongo unavailable"))
			},
			wantErrContains: "check room membership",
		},
		{
			name: "transient user-lookup error propagates",
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
					Return(nil, model.ErrSubscriptionNotFound)
				s.EXPECT().GetUser(gomock.Any(), "alice").
					Return(nil, errors.New("mongo unavailable"))
			},
			wantErrContains: "check platform admin",
		},
		{
			name: "transient room-existence error propagates (admin path)",
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
					Return(nil, model.ErrSubscriptionNotFound)
				s.EXPECT().GetUser(gomock.Any(), "alice").
					Return(&model.User{ID: "u1", Account: "alice", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
				s.EXPECT().GetRoom(gomock.Any(), "r1").
					Return(nil, errors.New("mongo unavailable"))
			},
			wantErrContains: "check room existence",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			tt.setupMock(store)
			h := &Handler{store: store, siteID: "site-a"}
			err := h.authorizeRoomAppRead(context.Background(), "alice", "r1")
			switch {
			case tt.wantErrContains != "":
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
			case tt.wantErr == nil:
				assert.NoError(t, err)
			default:
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func newTabsTestHandler(t *testing.T, siteURL string) (*Handler, *MockRoomStore, *gomock.Controller) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	u, err := url.Parse(siteURL)
	require.NoError(t, err)
	return &Handler{store: store, siteID: "site-a", siteURL: u}, store, ctrl
}

func mockTabApp(id, tabName, urlTemplate string) model.App {
	return model.App{
		ID:        id,
		Assistant: &model.AppAssistant{Enabled: true, Name: id + ".bot"},
		ChannelTab: &model.AppChannelTab{
			Enabled: true, Default: true, Name: tabName,
			URL: model.AppChannelTabURL{Default: urlTemplate},
		},
	}
}

func TestHandler_buildTabURL(t *testing.T) {
	validSiteURL, err := url.Parse("https://chat.example.com")
	require.NoError(t, err)

	tests := []struct {
		name    string
		handler *Handler
		tmpl    string
		roomID  string
		wantURL string
		wantOK  bool
	}{
		{
			name:    "happy path",
			handler: &Handler{siteID: "site-a", siteURL: validSiteURL},
			tmpl:    "https://upstream/tab/${roomId}/${siteId}",
			roomID:  "r1",
			wantURL: "https://chat.example.com/tab/r1/site-a",
			wantOK:  true,
		},
		{
			name:    "empty template",
			handler: &Handler{siteID: "site-a", siteURL: validSiteURL},
			tmpl:    "",
			roomID:  "r1",
			wantOK:  false,
		},
		{
			name:    "nil siteURL",
			handler: &Handler{siteID: "site-a"},
			tmpl:    "https://upstream/tab/${roomId}",
			roomID:  "r1",
			wantOK:  false,
		},
		{
			name:    "non-URL-safe roomID",
			handler: &Handler{siteID: "site-a", siteURL: validSiteURL},
			tmpl:    "https://upstream/tab/${roomId}",
			roomID:  "r1/../../etc",
			wantOK:  false,
		},
		{
			name:    "non-URL-safe siteID",
			handler: &Handler{siteID: "site/../a", siteURL: validSiteURL},
			tmpl:    "https://upstream/tab/${roomId}",
			roomID:  "r1",
			wantOK:  false,
		},
		{
			name:    "malformed template",
			handler: &Handler{siteID: "site-a", siteURL: validSiteURL},
			tmpl:    "://malformed",
			roomID:  "r1",
			wantOK:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.handler.buildTabURL(tt.tmpl, tt.roomID)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantURL, got)
		})
	}
}

func TestHandler_handleGetRoomAppTabs_MemberAllowed(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
		mockTabApp("app1", "Calendar", "https://upstream/cal/${roomId}/${siteId}/index"),
	}, nil)

	resp, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1)
	assert.Equal(t, "app1", resp.Apps[0].ID)
	assert.Equal(t, "Calendar", resp.Apps[0].Name)
	assert.Equal(t, "https://chat.example.com/cal/r1/site-a/index", resp.Apps[0].TabURL)
	require.NotNil(t, resp.Apps[0].Assistant)
	assert.Equal(t, "app1.bot", resp.Apps[0].Assistant.Name)
}

func TestHandler_handleGetRoomAppTabs_AdminAllowed(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, model.ErrSubscriptionNotFound)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{Account: "alice", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1"}, nil)
	store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{}, nil)

	resp, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	assert.Empty(t, resp.Apps)
}

func TestHandler_handleGetRoomAppTabs_Denied(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, model.ErrSubscriptionNotFound)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{Account: "alice", Roles: []model.UserRole{model.UserRoleUser}}, nil)

	_, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	assert.ErrorIs(t, err, errAppAccessDenied)
}

func TestHandler_handleGetRoomAppTabs_DeniedNoUser(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, model.ErrSubscriptionNotFound)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(nil, ErrUserNotFound)

	_, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	assert.ErrorIs(t, err, errAppAccessDenied)
}

func TestHandler_handleGetRoomAppTabs_EmptyResultIsEmptyArray(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return(nil, nil)

	resp, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	assert.NotNil(t, resp.Apps, "must initialize empty slice, not nil, so JSON marshals to []")
	assert.Len(t, resp.Apps, 0)
	data, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"apps":[]`)
}

func TestHandler_handleGetRoomAppTabs_URLRewritePathPrefix(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com/chat")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
		mockTabApp("app1", "Calendar", "https://upstream/tab/${roomId}"),
	}, nil)

	resp, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	assert.Equal(t, "https://chat.example.com/chat/tab/r1", resp.Apps[0].TabURL)
}

func TestHandler_handleGetRoomAppTabs_URLRewriteStripsUserinfo(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
		mockTabApp("app1", "X", "https://user:pass@upstream/path/${roomId}"),
	}, nil)

	resp, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	assert.NotContains(t, resp.Apps[0].TabURL, "user")
	assert.NotContains(t, resp.Apps[0].TabURL, "pass")
	assert.Equal(t, "https://chat.example.com/path/r1", resp.Apps[0].TabURL)
}

func TestHandler_handleGetRoomAppTabs_URLRewritePreservesQueryAndFragment(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
		mockTabApp("app1", "X", "https://upstream/path?room=${roomId}#tab=${siteId}"),
	}, nil)

	resp, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	assert.Equal(t, "https://chat.example.com/path?room=r1#tab=site-a", resp.Apps[0].TabURL)
}

func TestHandler_handleGetRoomAppTabs_URLRewriteSkipsEmptyAndMalformed(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
		mockTabApp("ok1", "OK1", "https://upstream/ok1/${roomId}"),
		mockTabApp("empty", "Empty", ""),
		mockTabApp("bad", "Bad", "://malformed"),
		mockTabApp("ok2", "OK2", "https://upstream/ok2/${roomId}"),
	}, nil)

	resp, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	require.Len(t, resp.Apps, 2, "empty and malformed must be skipped")
	assert.Equal(t, "ok1", resp.Apps[0].ID)
	assert.Equal(t, "ok2", resp.Apps[1].ID)
}

func TestHandler_handleGetRoomAppTabs_SkipsAppWithNilChannelTab(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	// One app has nil ChannelTab (invalid data), one is valid — only the valid one should appear.
	appNoTab := model.App{ID: "notab", ChannelTab: nil}
	store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).Return([]model.App{
		appNoTab,
		mockTabApp("ok1", "OK1", "https://upstream/ok1/${roomId}"),
	}, nil)

	resp, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	require.Len(t, resp.Apps, 1, "app with nil ChannelTab must be skipped")
	assert.Equal(t, "ok1", resp.Apps[0].ID)
}

func TestHandler_handleGetRoomAppTabs_StoreListError(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListDefaultChannelTabApps(gomock.Any()).
		Return(nil, errors.New("mongo down"))

	_, err := h.getRoomAppTabs(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mongo down")
}

func TestHandler_handleGetRoomAppTabs_ContextTimeout(t *testing.T) {
	h, store, _ := newTabsTestHandler(t, "https://chat.example.com")
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		DoAndReturn(func(ctx context.Context, _, _ string) (*model.Subscription, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})

	parent, cancel := context.WithCancel(context.Background())
	cancel()
	c := natsrouter.NewContext(map[string]string{"account": "alice", "roomID": "r1"})
	c.SetContext(parent)
	_, err := h.getRoomAppTabs(c)
	require.Error(t, err)
	assert.True(t,
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
		"expected wrapped context error, got %v", err)
}

func newCmdMenuTestHandler(t *testing.T) (*Handler, *MockRoomStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	return &Handler{store: store, siteID: "site-a"}, store
}

func TestHandler_handleGetRoomAppCommandMenu_MemberAllowed_NoBots(t *testing.T) {
	h, store := newCmdMenuTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return([]RoomBotAppEntry{}, nil)
	// ListActiveCmdMenus must NOT be called.

	resp, err := h.getRoomAppCommandMenu(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	assert.NotNil(t, resp.AppAssistants)
	assert.Len(t, resp.AppAssistants, 0)
	data, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"appAssistants":[]`)
}

func TestHandler_handleGetRoomAppCommandMenu_MemberAllowed_WithMenus(t *testing.T) {
	h, store := newCmdMenuTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return([]RoomBotAppEntry{
		{AssistantName: "stocks.bot", AppName: "Stocks"},
		{AssistantName: "weather.bot", AppName: "Weather"},
	}, nil)
	store.EXPECT().ListActiveCmdMenus(gomock.Any(), []string{"stocks.bot", "weather.bot"}).
		Return([]model.BotCmdMenu{
			{Name: "stocks.bot", CmdBlocks: []model.CmdBlock{{Text: "/quote"}}},
			{Name: "weather.bot", CmdBlocks: []model.CmdBlock{{Text: "/forecast"}}},
		}, nil)

	resp, err := h.getRoomAppCommandMenu(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	require.Len(t, resp.AppAssistants, 2)
	assert.Equal(t, "Stocks", resp.AppAssistants[0].AppName)
	assert.Equal(t, "stocks.bot", resp.AppAssistants[0].Name)
	require.Len(t, resp.AppAssistants[0].CmdBlocks, 1)
	assert.Equal(t, "/quote", resp.AppAssistants[0].CmdBlocks[0].Text)
	assert.Equal(t, "Weather", resp.AppAssistants[1].AppName)
	assert.Equal(t, "/forecast", resp.AppAssistants[1].CmdBlocks[0].Text)
}

func TestHandler_handleGetRoomAppCommandMenu_MemberAllowed_BotWithoutMenu(t *testing.T) {
	h, store := newCmdMenuTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return([]RoomBotAppEntry{
		{AssistantName: "silent.bot", AppName: "Silent"},
	}, nil)
	store.EXPECT().ListActiveCmdMenus(gomock.Any(), []string{"silent.bot"}).
		Return([]model.BotCmdMenu{}, nil)

	resp, err := h.getRoomAppCommandMenu(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	require.Len(t, resp.AppAssistants, 1)
	assert.Equal(t, "Silent", resp.AppAssistants[0].AppName)
	assert.Equal(t, "silent.bot", resp.AppAssistants[0].Name)
	assert.Nil(t, resp.AppAssistants[0].CmdBlocks)
}

func TestHandler_handleGetRoomAppCommandMenu_AdminAllowed(t *testing.T) {
	h, store := newCmdMenuTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, model.ErrSubscriptionNotFound)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{Account: "alice", Roles: []model.UserRole{model.UserRoleAdmin}}, nil)
	store.EXPECT().GetRoom(gomock.Any(), "r1").
		Return(&model.Room{ID: "r1"}, nil)
	store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return([]RoomBotAppEntry{}, nil)

	resp, err := h.getRoomAppCommandMenu(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	assert.Len(t, resp.AppAssistants, 0)
}

func TestHandler_handleGetRoomAppCommandMenu_Denied(t *testing.T) {
	h, store := newCmdMenuTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(nil, model.ErrSubscriptionNotFound)
	store.EXPECT().GetUser(gomock.Any(), "alice").
		Return(&model.User{Account: "alice"}, nil)

	_, err := h.getRoomAppCommandMenu(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	assert.ErrorIs(t, err, errAppAccessDenied)
}

func TestHandler_handleGetRoomAppCommandMenu_StoreListRoomBotAppsError(t *testing.T) {
	h, store := newCmdMenuTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return(nil, errors.New("mongo down"))

	_, err := h.getRoomAppCommandMenu(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mongo down")
}

func TestHandler_handleGetRoomAppCommandMenu_StoreListActiveCmdMenusError(t *testing.T) {
	h, store := newCmdMenuTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		Return(&model.Subscription{User: model.SubscriptionUser{Account: "alice"}, RoomID: "r1"}, nil)
	store.EXPECT().ListRoomBotApps(gomock.Any(), "r1").Return([]RoomBotAppEntry{
		{AssistantName: "weather.bot", AppName: "Weather"},
	}, nil)
	store.EXPECT().ListActiveCmdMenus(gomock.Any(), []string{"weather.bot"}).
		Return(nil, errors.New("mongo down"))

	_, err := h.getRoomAppCommandMenu(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mongo down")
}

func TestHandler_handleGetRoomAppCommandMenu_ContextTimeout(t *testing.T) {
	h, store := newCmdMenuTestHandler(t)
	store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").
		DoAndReturn(func(ctx context.Context, _, _ string) (*model.Subscription, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		})

	parent, cancel := context.WithCancel(context.Background())
	cancel()
	c := natsrouter.NewContext(map[string]string{"account": "alice", "roomID": "r1"})
	c.SetContext(parent)
	_, err := h.getRoomAppCommandMenu(c)
	require.Error(t, err)
	assert.True(t,
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
		"expected wrapped context error, got %v", err)
}

func TestHandler_ListMentionableSubscriptions(t *testing.T) {
	const siteID = "site-a"
	const roomID = "r1"
	const requester = "alice"

	stub := []model.MentionableSubscription{
		{OptionType: "user", UserID: "u-bob", Account: "bob", SiteID: "site-a",
			HRInfo: &model.MentionableHRInfo{EngName: "Bob", ChineseName: "博"}},
	}

	type want struct {
		errContains string
		errIs       error
		subs        []model.MentionableSubscription
	}
	tests := []struct {
		name      string
		body      []byte
		setupMock func(*MockRoomStore)
		want      want
	}{
		{
			name: "default limit 3, empty filter, happy path",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, "", 3).
					Return(stub, nil)
			},
			want: want{subs: stub},
		},
		{
			// Nil-Limit clamps to UserCount+AppCount (the cap), not the default 3.
			// A small room with 1 user + 1 app would otherwise spuriously fail
			// validation with the default 3 vs cap 2.
			name: "nil limit clamped to small UserCount+AppCount",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 1, AppCount: 1}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, "", 2).
					Return(stub, nil)
			},
			want: want{subs: stub},
		},
		{
			// Empty-room short-circuit: skip the store call, return empty.
			name: "nil limit + empty room returns empty without store call",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 0, AppCount: 0}, nil)
			},
			want: want{subs: []model.MentionableSubscription{}},
		},
		{
			name: "explicit limit and filter passed through",
			body: []byte(`{"limit":3,"filter":"bo"}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, "bo", 3).
					Return(stub, nil)
			},
			want: want{subs: stub},
		},
		{
			name: "regex metacharacters in filter are escaped",
			body: []byte(`{"limit":3,"filter":"a.b(c"}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, `a\.b\(c`, 3).
					Return([]model.MentionableSubscription{}, nil)
			},
			want: want{subs: []model.MentionableSubscription{}},
		},
		{
			// GetRoom is dispatched in parallel with GetSubscription; it may
			// or may not be invoked depending on goroutine timing before
			// errgroup observes the membership error. AnyTimes() accepts
			// both racing outcomes.
			name: "requester not a member",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(fmt.Errorf("missing: %w", model.ErrSubscriptionNotFound))
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil).AnyTimes()
			},
			want: want{errIs: errNotRoomMember},
		},
		{
			// Precedence regression: when BOTH the membership probe and the
			// room read fail concurrently, errNotRoomMember must still win.
			// Plain errgroup.Group (no WithContext) prevents GetRoom's failure
			// from cancelling GetSubscription mid-flight and surfacing as
			// context.Canceled, which would mask the not-member signal.
			name: "not-member takes precedence over GetRoom error",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(fmt.Errorf("missing: %w", model.ErrSubscriptionNotFound))
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errIs: errNotRoomMember},
		},
		{
			name: "GetSubscription infra error",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(fmt.Errorf("mongo exploded"))
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil).AnyTimes()
			},
			want: want{errContains: "check room membership"},
		},
		{
			name: "limit zero",
			body: []byte(`{"limit":0}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
			},
			want: want{errIs: errMentionableLimitInvalid},
		},
		{
			name: "limit negative",
			body: []byte(`{"limit":-1}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
			},
			want: want{errIs: errMentionableLimitInvalid},
		},
		{
			// Regression (#464): an over-cap explicit limit must clamp to the
			// cap like the nil-limit branch does, not hard-reject.
			name: "limit exceeds UserCount + AppCount clamps instead of rejecting",
			body: []byte(`{"limit":8}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, "", 7).
					Return(stub, nil)
			},
			want: want{subs: stub},
		},
		{
			// Empty room + explicit over-cap limit must still short-circuit
			// to empty (no store call), never send $limit:0 to the store.
			name: "explicit limit with empty room returns empty without store call",
			body: []byte(`{"limit":5}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 0, AppCount: 0}, nil)
			},
			want: want{subs: []model.MentionableSubscription{}},
		},
		{
			name: "limit at cap is accepted",
			body: []byte(`{"limit":7}`),
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, "", 7).
					Return(stub, nil)
			},
			want: want{subs: stub},
		},
		{
			name: "GetRoom errors",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "get room"},
		},
		{
			name: "store errors",
			body: nil,
			setupMock: func(s *MockRoomStore) {
				s.EXPECT().CheckMembership(gomock.Any(), requester, roomID).
					Return(nil)
				s.EXPECT().GetRoom(gomock.Any(), roomID).
					Return(&model.Room{ID: roomID, UserCount: 5, AppCount: 2}, nil)
				s.EXPECT().
					ListMentionableSubscriptions(gomock.Any(), roomID, requester, "", 3).
					Return(nil, fmt.Errorf("mongo exploded"))
			},
			want: want{errContains: "list mentionable subscriptions"},
		},
		{
			// Body parse now precedes the parallel store dispatch; a malformed
			// body short-circuits before any read.
			name:      "malformed JSON body",
			body:      []byte("{not json"),
			setupMock: func(s *MockRoomStore) {},
			want:      want{errContains: "invalid request"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl)
			tc.setupMock(store)

			h := &Handler{store: store, siteID: siteID}
			c := ctxParams(map[string]string{"account": requester, "roomID": roomID})
			c.Msg = &nats.Msg{Data: tc.body}
			resp, err := h.listMentionableSubscriptions(c)

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
			assert.Equal(t, tc.want.subs, resp.Subscriptions)
		})
	}
}

func TestHandler_MessageRead_ChannelFloorMoves_PublishesRoomEvent(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, LastMsgAt: &lastMsg}, nil)
	minT := lastSeen
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&minT, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &minT).Return(nil)

	resp, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)
	assert.Equal(t, "accepted", resp.Status)

	// coreSubjects[0] = subscription.update for the reader; coreSubjects[1] = room floor event.
	require.Len(t, f.coreSubjects, 2)
	assert.Equal(t, subject.SubscriptionUpdate("alice"), f.coreSubjects[0])
	assert.Equal(t, subject.RoomEvent("r1"), f.coreSubjects[1])
	var evt model.MessageReadEvent
	require.NoError(t, json.Unmarshal(f.coreData[1], &evt))
	assert.Equal(t, model.RoomEventMessageRead, evt.Type)
	assert.Equal(t, "r1", evt.RoomID)
	require.NotNil(t, evt.MinUserLastSeenAt)
	assert.True(t, evt.MinUserLastSeenAt.Equal(minT))
	assert.NotZero(t, evt.Timestamp)
}

func TestHandler_MessageRead_DMFloorMoves_PublishesPerSubscriber(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeDM, LastMsgAt: &lastMsg}, nil)
	minT := lastSeen
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&minT, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &minT).Return(nil)
	f.store.EXPECT().ListSubscriptionsByRoom(gomock.Any(), "r1").Return([]model.Subscription{
		{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, RoomID: "r1"},
		{User: model.SubscriptionUser{ID: "u2", Account: "bob"}, RoomID: "r1"},
	}, nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	// coreSubjects[0] = subscription.update for alice; coreSubjects[1,2] = DM room events.
	require.Len(t, f.coreSubjects, 3)
	assert.Equal(t, subject.SubscriptionUpdate("alice"), f.coreSubjects[0])
	assert.Equal(t, subject.UserRoomEvent("alice"), f.coreSubjects[1])
	assert.Equal(t, subject.UserRoomEvent("bob"), f.coreSubjects[2])
	var evt model.MessageReadEvent
	require.NoError(t, json.Unmarshal(f.coreData[2], &evt))
	assert.Equal(t, model.RoomEventMessageRead, evt.Type)
	assert.Equal(t, "r1", evt.RoomID)
}

func TestHandler_MessageRead_ChannelNilFloor_OmitsFloorField(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)
	storedFloor := lastSeen // room currently has a non-nil floor

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{
		ID: "r1", Type: model.RoomTypeChannel, LastMsgAt: &lastMsg, MinUserLastSeenAt: &storedFloor,
	}, nil)
	var nilFloor *time.Time
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(nilFloor, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", nilFloor).Return(nil)

	_, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err)

	// coreSubjects[0] = subscription.update; coreSubjects[1] = floor event (nil floor).
	require.Len(t, f.coreSubjects, 2)
	assert.Equal(t, subject.SubscriptionUpdate("alice"), f.coreSubjects[0])
	assert.NotContains(t, string(f.coreData[1]), "minUserLastSeenAt")
}

func TestHandler_MessageRead_ChannelPublishError_StillAccepted(t *testing.T) {
	f := newMessageReadFixture(t)
	f.coreErr = fmt.Errorf("nats down")
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel, LastMsgAt: &lastMsg}, nil)
	minT := lastSeen
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&minT, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &minT).Return(nil)

	resp, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err, "fan-out publish failure must not fail the RPC")
	assert.Equal(t, "accepted", resp.Status)
}

func TestHandler_MessageRead_DMListSubscriptionsError_StillAccepted(t *testing.T) {
	f := newMessageReadFixture(t)
	joined := time.Now().UTC().Add(-2 * time.Hour)
	lastSeen := joined.Add(time.Hour)
	lastMsg := lastSeen.Add(30 * time.Minute)

	f.store.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(&model.Subscription{
		User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
		RoomID: "r1", SiteID: "site-a", JoinedAt: joined, LastSeenAt: &lastSeen,
	}, nil)
	f.store.EXPECT().UpdateSubscriptionRead(gomock.Any(), "r1", "alice", gomock.Any(), false).Return(nil)
	f.store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)
	f.store.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeDM, LastMsgAt: &lastMsg}, nil)
	minT := lastSeen
	f.store.EXPECT().MinSubscriptionLastSeenByRoomID(gomock.Any(), "r1").Return(&minT, nil)
	f.store.EXPECT().UpdateRoomMinUserLastSeenAt(gomock.Any(), "r1", &minT).Return(nil)
	f.store.EXPECT().ListSubscriptionsByRoom(gomock.Any(), "r1").Return(nil, fmt.Errorf("mongo down"))

	resp, err := f.handler.messageRead(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}))
	require.NoError(t, err, "DM subscription-list failure must not fail the RPC")
	assert.Equal(t, "accepted", resp.Status)
	// subscription.update fires before the DM fan-out; the DM list failure only suppresses floor events.
	require.Len(t, f.coreSubjects, 1)
	assert.Equal(t, subject.SubscriptionUpdate("alice"), f.coreSubjects[0])
}

func TestHandler_MuteToggle_OmitsRoomName(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().ToggleSubscriptionMute(gomock.Any(), "dmroom", "alice", gomock.Any()).
		Return(&model.Subscription{
			ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID: "dmroom", SiteID: "site-a", RoomType: model.RoomTypeDM, Name: "bob", Muted: true,
		}, nil)
	store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	var coreBodies [][]byte
	h := &Handler{
		store:           store,
		siteID:          "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore: func(_ context.Context, _ string, data []byte) error {
			coreBodies = append(coreBodies, data)
			return nil
		},
	}

	_, err := h.muteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "dmroom"}))
	require.NoError(t, err)

	require.Len(t, coreBodies, 1)
	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(coreBodies[0], &evt))
	assert.Empty(t, evt.RoomName, "mute must not look up or set roomName")
}

func TestHandler_FavoriteToggle_OmitsRoomName(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	store.EXPECT().ToggleSubscriptionFavorite(gomock.Any(), "botroom", "alice", gomock.Any()).
		Return(&model.Subscription{
			ID: "s1", User: model.SubscriptionUser{ID: "u1", Account: "alice"},
			RoomID: "botroom", SiteID: "site-a", RoomType: model.RoomTypeBotDM, Name: "helper.bot", Favorite: true,
		}, nil)
	store.EXPECT().GetUserSiteID(gomock.Any(), "alice").Return("site-a", nil)

	var coreBodies [][]byte
	h := &Handler{
		store:           store,
		siteID:          "site-a",
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
		publishCore: func(_ context.Context, _ string, data []byte) error {
			coreBodies = append(coreBodies, data)
			return nil
		},
	}

	_, err := h.favoriteToggle(ctxParams(map[string]string{"account": "alice", "roomID": "botroom"}))
	require.NoError(t, err)

	require.Len(t, coreBodies, 1)
	var evt model.SubscriptionUpdateEvent
	require.NoError(t, json.Unmarshal(coreBodies[0], &evt))
	assert.Empty(t, evt.RoomName, "favorite must not look up or set roomName")
}

func TestFederateOne_PublishesEnvelopeToOutbox(t *testing.T) {
	var gotSubj, gotMsgID string
	var gotData []byte
	h := &Handler{
		siteID: "site-a",
		publishToStream: func(_ context.Context, subj string, data []byte, msgID string) error {
			gotSubj, gotData, gotMsgID = subj, data, msgID
			return nil
		},
	}

	require.NoError(t, h.federateOne(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}),
		"r1", "site-b", model.InboxSubscriptionMuteToggled, []byte(`{"x":1}`), "seed", 7))

	// Destination + event type ride the subject; the OUTBOX Nats-Msg-Id is the forward's DedupID.
	assert.Equal(t, subject.Outbox("site-a", "site-b", model.InboxSubscriptionMuteToggled), gotSubj)
	assert.NotEmpty(t, gotMsgID)

	var evt model.OutboxEvent
	require.NoError(t, json.Unmarshal(gotData, &evt))
	assert.Equal(t, "r1", evt.RoomID)
	assert.Equal(t, gotMsgID, evt.DedupID, "OUTBOX msgID must equal the forwarded DedupID")

	var env model.InboxEvent
	require.NoError(t, json.Unmarshal(evt.Envelope, &env))
	assert.Equal(t, model.InboxSubscriptionMuteToggled, env.Type)
	assert.Equal(t, "site-a", env.SiteID)
	assert.Equal(t, "site-b", env.DestSiteID)
	assert.Equal(t, int64(7), env.Timestamp)
	assert.JSONEq(t, `{"x":1}`, string(env.Payload))
}

func TestFederateOne_NoopWhenLocalOrEmpty(t *testing.T) {
	called := false
	h := &Handler{siteID: "site-a", publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { called = true; return nil }}
	require.NoError(t, h.federateOne(context.Background(), "r1", "", model.InboxSubscriptionRead, []byte(`{}`), "seed", 1))
	require.NoError(t, h.federateOne(context.Background(), "r1", "site-a", model.InboxSubscriptionRead, []byte(`{}`), "seed", 1))
	assert.False(t, called, "empty or local destination must not publish")
}
