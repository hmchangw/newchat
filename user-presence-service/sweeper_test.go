package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/user-presence-service/presencestore"
)

func TestSweeper_TickPublishesChanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockPresenceStore(ctrl)
	cap := &capturedPublish{}
	sw := NewSweeper(store, cap.fn(), "site-a", 0)
	sw.now = fixedNow()
	store.EXPECT().Sweep(gomock.Any(), gomock.Any()).
		Return([]presencestore.StatusChange{
			{Account: "alice", Effective: model.StatusOffline},
			{Account: "bob", Effective: model.StatusAway},
		}, nil)

	require.NoError(t, sw.tick(context.Background()))
	require.Len(t, cap.subjects, 2)
	assert.Equal(t, "chat.user.presence.state.alice", cap.subjects[0])
	assert.Equal(t, "chat.user.presence.state.bob", cap.subjects[1])
}

func TestSweeper_TickNoChangesPublishesNothing(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockPresenceStore(ctrl)
	cap := &capturedPublish{}
	sw := NewSweeper(store, cap.fn(), "site-a", 0)
	store.EXPECT().Sweep(gomock.Any(), gomock.Any()).Return(nil, nil)

	require.NoError(t, sw.tick(context.Background()))
	assert.Empty(t, cap.subjects)
}

func TestSweeper_TickSurfacesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockPresenceStore(ctrl)
	sw := NewSweeper(store, (&capturedPublish{}).fn(), "site-a", 0)
	store.EXPECT().Sweep(gomock.Any(), gomock.Any()).Return(nil, errors.New("boom"))

	require.Error(t, sw.tick(context.Background()))
}
