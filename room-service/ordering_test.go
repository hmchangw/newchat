package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyfake"
)

// Both handlers below are request/reply: nothing retries them automatically,
// and their validation short-circuits once the write has landed. So anything a
// side effect needs has to be resolved BEFORE the write that makes a retry
// return early — otherwise a failure in between is permanent.

// A retry of this request would hit errAlreadyOwner and return before
// federating, so the remote replica would keep the old role forever.
func TestUpdateRole_ResolvesTheFederationDestinationBeforeTheWrite(t *testing.T) {
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
	store.EXPECT().GetUserSiteID(gomock.Any(), "bob").Return("", errors.New("mongo down"))
	store.EXPECT().SetOwnerRole(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	fake := valkeyfake.New()
	h := &Handler{store: store, siteID: "site-a", maxRoomSize: 1000, valkey: fake,
		publishCore:     func(_ context.Context, _ string, _ []byte) error { return nil },
		publishToStream: func(_ context.Context, _ string, _ []byte, _ string) error { return nil },
	}

	_, err := h.updateRole(ctxParams(map[string]string{"account": "alice", "roomID": "r1"}),
		model.UpdateRoleRequest{Account: "bob", NewRole: model.RoleOwner})

	require.Error(t, err, "the role must not commit when its federation destination is unknown")
	require.Empty(t, fake.DeletedKeys(), "nothing was changed, so nothing needs busting")
}
