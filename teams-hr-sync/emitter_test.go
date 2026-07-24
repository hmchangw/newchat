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

func TestDirectEmitter_UpsertsEmployeesAndConvertedUsers(t *testing.T) {
	store := NewMockWriteStore(gomock.NewController(t))
	e := directEmitter{store: store, converter: transform.DefaultConverter{}}

	diff := diffResult{
		Upserts: []model.IEmployeeWithChange{{
			IEmployee:  teamsEmployee("alice", "site-a"),
			ChangeType: model.IChangeTypeNewHire,
		}},
	}

	store.EXPECT().UpsertEmployees(gomock.Any(), diff.Upserts).Return(nil)
	store.EXPECT().UpsertUserIdentities(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, users []model.IUserWithChange) error {
			require.Len(t, users, 1)
			assert.Equal(t, "alice", users[0].Account)
			assert.Equal(t, "site-a", users[0].SiteID)
			assert.Equal(t, model.IChangeTypeNewHire, users[0].ChangeType)
			return nil
		})

	res, err := e.emit(context.Background(), diff)
	require.NoError(t, err)
	assert.Equal(t, 1, res.EmployeesWritten)
	assert.Equal(t, 1, res.UsersWritten)
}

func TestDirectEmitter_QuitsWhenPresent(t *testing.T) {
	store := NewMockWriteStore(gomock.NewController(t))
	e := directEmitter{store: store, converter: transform.DefaultConverter{}}

	store.EXPECT().QuitTeamsEmployees(gomock.Any(), []string{"eve"}).Return(nil)

	res, err := e.emit(context.Background(), diffResult{Quits: map[string][]string{"site-a": {"eve"}}})
	require.NoError(t, err)
	assert.Equal(t, 1, res.QuitsWritten)
}

func TestDirectEmitter_SkipsEmptyDiff(t *testing.T) {
	store := NewMockWriteStore(gomock.NewController(t)) // no EXPECT — any call fails the test
	e := directEmitter{store: store, converter: transform.DefaultConverter{}}

	res, err := e.emit(context.Background(), diffResult{})
	require.NoError(t, err)
	assert.Equal(t, emitResult{}, res)
}

func TestDirectEmitter_UpsertErrorAborts(t *testing.T) {
	boom := errors.New("mongo down")
	store := NewMockWriteStore(gomock.NewController(t))
	e := directEmitter{store: store, converter: transform.DefaultConverter{}}

	store.EXPECT().UpsertEmployees(gomock.Any(), gomock.Any()).Return(boom)

	_, err := e.emit(context.Background(), diffResult{
		Upserts: []model.IEmployeeWithChange{{IEmployee: teamsEmployee("alice", "site-a")}},
	})
	require.ErrorIs(t, err, boom)
}

func TestStreamEmitter_DelegatesToPublisher(t *testing.T) {
	var got []captured
	pub := newCapturingPublisher(t, &got)
	e := streamEmitter{pub: pub}

	diff := diffResult{Upserts: []model.IEmployeeWithChange{{IEmployee: teamsEmployee("alice", "site-a"), ChangeType: model.IChangeTypeNewHire}}}
	res, err := e.emit(context.Background(), diff)
	require.NoError(t, err)
	assert.Equal(t, 1, res.EmployeesWritten)
	assert.Equal(t, 1, res.UsersWritten)
	assert.Len(t, got, 2) // employees.upsert + users.upsert
}

func TestRunDirectSync_ModePicksEmitter(t *testing.T) {
	graph := &fakeGroupReader{
		groups:  map[string]*msgraph.GroupProfile{"g1": {ID: "g1", DisplayName: "Engineering"}},
		members: map[string][]msgraph.GraphUser{"g1": {{ID: "u1", UserPrincipalName: "alice@corp.com"}}},
	}
	store := NewMockWriteStore(gomock.NewController(t))
	store.EXPECT().UpsertEmployees(gomock.Any(), gomock.Len(1)).Return(nil)
	store.EXPECT().UpsertUserIdentities(gomock.Any(), gomock.Any()).Return(nil)
	emit := directEmitter{store: store, converter: transform.DefaultConverter{}}

	stats, err := runDirectSync(context.Background(), graph, transform.DefaultMapper{}, emit,
		[]syncGroup{{GroupID: "g1", SiteID: "site-a"}}, nil, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Created)
	assert.Zero(t, stats.Quits, "direct mode diffs against an empty baseline: no quits")
	assert.Equal(t, 1, stats.EmployeesPublished)
	assert.Equal(t, 1, stats.UsersPublished)
}
