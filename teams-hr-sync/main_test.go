package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

func TestRunSync_DiffAndPublish(t *testing.T) {
	graph := &fakeGroupReader{
		groups:  map[string]*msgraph.GroupProfile{"g1": {ID: "g1", DisplayName: "Engineering"}},
		members: map[string][]msgraph.GraphUser{"g1": {{ID: "u1", UserPrincipalName: "alice@corp.com"}}},
	}
	stored := teamsEmployee("eve", "site-a") // absent from graph -> quit
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().ListTeamsEmployees(gomock.Any()).Return([]model.Employee{stored}, nil)

	var got []captured
	pub := newCapturingPublisher(t, &got)

	stats, err := runSync(context.Background(), graph, store, pub,
		[]syncGroup{{GroupID: "g1", SiteID: "site-a"}}, "group", 100)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Created)
	assert.Zero(t, stats.Updated)
	assert.Equal(t, 1, stats.Quits)
	assert.Equal(t, 3, stats.Published) // employees.upsert + users.upsert + one quit
	assert.Len(t, got, 3)
}

func TestRunSync_StoreErrorAborts(t *testing.T) {
	graph := &fakeGroupReader{
		groups:  map[string]*msgraph.GroupProfile{"g1": {ID: "g1"}},
		members: map[string][]msgraph.GraphUser{},
	}
	boom := errors.New("mongo down")
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	store.EXPECT().ListTeamsEmployees(gomock.Any()).Return(nil, boom)

	_, err := runSync(context.Background(), graph, store,
		newPublisher(func(context.Context, string, []byte) error { return nil }, "central", transform.DefaultConverter{}),
		[]syncGroup{{GroupID: "g1", SiteID: "site-a"}}, "group", 100)
	require.ErrorIs(t, err, boom)
}
