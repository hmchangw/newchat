package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

// TestLoadAddMemberInputs_RunsReadsConcurrently proves the three independent
// up-front reads (GetRoom, ListAddMemberCandidates, HasOrgRoomMembers) are
// issued concurrently: each mock blocks on a shared release channel after
// signalling arrival, so serial execution would only ever reach one before the
// others are unblocked and the test would time out.
func TestLoadAddMemberInputs_RunsReadsConcurrently(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockSubscriptionStore(ctrl)

	const reads = 3
	arrived := make(chan struct{}, reads)
	release := make(chan struct{})
	block := func() { arrived <- struct{}{}; <-release }

	store.EXPECT().GetRoom(gomock.Any(), "r1").DoAndReturn(
		func(_ context.Context, _ string) (*model.Room, error) {
			block()
			return &model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil
		})
	store.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").DoAndReturn(
		func(_ context.Context, _, _ []string, _ string) ([]AddMemberCandidate, error) {
			block()
			return []AddMemberCandidate{{Account: "bob"}}, nil
		})
	store.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").DoAndReturn(
		func(_ context.Context, _ string) (bool, error) {
			block()
			return true, nil
		})

	h := &Handler{store: store}
	req := &model.AddMembersRequest{RoomID: "r1", Users: []string{"bob"}}

	type result struct {
		in  addMemberInputs
		err error
	}
	done := make(chan result, 1)
	go func() {
		in, err := h.loadAddMemberInputs(context.Background(), req)
		done <- result{in, err}
	}()

	for i := 0; i < reads; i++ {
		select {
		case <-arrived:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d/%d reads in flight; loads are not concurrent", i, reads)
		}
	}
	close(release)

	r := <-done
	require.NoError(t, r.err)
	assert.Equal(t, "r1", r.in.room.ID)
	assert.True(t, r.in.hadOrgsBefore)
	require.Len(t, r.in.candidates, 1)
	assert.Equal(t, "bob", r.in.candidates[0].Account)
}

// TestLoadAddMemberInputs_PropagatesErrors verifies each read's error is wrapped
// with the same context the prior serial code used, so error messages and
// errors.Is behaviour are unchanged.
func TestLoadAddMemberInputs_PropagatesErrors(t *testing.T) {
	sentinel := errors.New("boom")
	tests := []struct {
		name    string
		setup   func(*MockSubscriptionStore)
		wantMsg string
	}{
		{
			name: "get room fails",
			setup: func(s *MockSubscriptionStore) {
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(nil, sentinel)
				s.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").Return(nil, nil)
				s.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
			},
			wantMsg: "get room: boom",
		},
		{
			name: "list candidates fails",
			setup: func(s *MockSubscriptionStore) {
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
				s.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").Return(nil, sentinel)
				s.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, nil)
			},
			wantMsg: "list add-member candidates: boom",
		},
		{
			name: "has-org-members fails",
			setup: func(s *MockSubscriptionStore) {
				s.EXPECT().GetRoom(gomock.Any(), "r1").Return(&model.Room{ID: "r1", Type: model.RoomTypeChannel}, nil)
				s.EXPECT().ListAddMemberCandidates(gomock.Any(), gomock.Any(), gomock.Any(), "r1").Return(nil, nil)
				s.EXPECT().HasOrgRoomMembers(gomock.Any(), "r1").Return(false, sentinel)
			},
			wantMsg: "check existing org room members: boom",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockSubscriptionStore(ctrl)
			tt.setup(store)

			h := &Handler{store: store}
			req := &model.AddMembersRequest{RoomID: "r1", Users: []string{"bob"}}
			_, err := h.loadAddMemberInputs(context.Background(), req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
			assert.ErrorIs(t, err, sentinel)
		})
	}
}
