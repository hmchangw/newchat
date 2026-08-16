package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/subject"
)

type soakRoomOpsTransport struct {
	mu       sync.Mutex
	subjects []string
	bodies   [][]byte
	reply    []byte
	err      error
}

func (t *soakRoomOpsTransport) Request(
	_ context.Context,
	requestSubject string,
	data []byte,
	_ time.Duration,
) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.subjects = append(t.subjects, requestSubject)
	t.bodies = append(t.bodies, append([]byte(nil), data...))
	return t.reply, t.err
}

func (t *soakRoomOpsTransport) calls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.subjects)
}

func newSoakRoomOpsMutator(transport soakRPCTransport) *soakRoomMutator {
	return newSoakRoomMutator(
		"site-a",
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 3}, &soakRecordingSleeper{}, nil),
		time.Second,
		nil,
	)
}

func TestSoakRoomMutator_AddMemberAddressesTheOwnerSubject(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"status":"accepted"}`)}

	outcome, err := newSoakRoomOpsMutator(transport).
		AddMember(context.Background(), "user-1", "room-1", "user-9")

	require.NoError(t, err)
	assert.True(t, outcome.Accepted)
	assert.Equal(t, soakRPCMemberAdd, outcome.Action)
	assert.Equal(t, "room-1", outcome.RoomID)
	assert.Positive(t, outcome.Latency)
	assert.Equal(t, subject.MemberAdd("user-1", "room-1", "site-a"), transport.subjects[0])
	assert.JSONEq(t, `{"roomId":"room-1","users":["user-9"]}`, string(transport.bodies[0]))
}

func TestSoakRoomMutator_RemoveMemberAddressesTheOwnerSubject(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"status":"accepted"}`)}

	outcome, err := newSoakRoomOpsMutator(transport).
		RemoveMember(context.Background(), "user-1", "room-1", "user-9")

	require.NoError(t, err)
	assert.True(t, outcome.Accepted)
	assert.Equal(t, subject.MemberRemove("user-1", "room-1", "site-a"), transport.subjects[0])
	assert.JSONEq(t, `{"roomId":"room-1","account":"user-9"}`, string(transport.bodies[0]))
}

func TestSoakRoomMutator_RenameSendsOnlyTheNewName(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"status":"accepted","requestId":"req-1"}`)}

	outcome, err := newSoakRoomOpsMutator(transport).
		Rename(context.Background(), "user-1", "room-1", "soak-room-r1")

	require.NoError(t, err)
	assert.True(t, outcome.Accepted)
	assert.Equal(t, subject.RoomRename("user-1", "room-1", "site-a"), transport.subjects[0])
	assert.JSONEq(t, `{"newName":"soak-room-r1"}`, string(transport.bodies[0]))
}

func TestSoakRoomMutator_ToggleMuteReturnsTheObservedState(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"status":"ok","muted":true}`)}

	outcome, err := newSoakRoomOpsMutator(transport).
		ToggleMute(context.Background(), "user-2", "room-1")

	require.NoError(t, err)
	assert.True(t, outcome.Accepted)
	assert.True(t, outcome.Muted)
	assert.Equal(t, subject.MuteToggle("user-2", "room-1", "site-a"), transport.subjects[0])
}

func TestSoakRoomMutator_CreateRoomReturnsTheAcceptedRoomID(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"status":"accepted","roomId":"room-new","roomType":"channel"}`),
	}

	outcome, err := newSoakRoomOpsMutator(transport).
		CreateRoom(context.Background(), "user-1", "soak-room", []string{"user-2"})

	require.NoError(t, err)
	assert.True(t, outcome.Accepted)
	assert.Equal(t, "room-new", outcome.RoomID)
	assert.Equal(t, subject.RoomCreate("user-1", "site-a"), transport.subjects[0])
	assert.JSONEq(t, `{"name":"soak-room","users":["user-2"]}`, string(transport.bodies[0]))
}

func TestSoakRoomMutator_AmbiguousMutationsAreNeverRetried(t *testing.T) {
	for _, testCase := range []struct {
		name string
		call func(*soakRoomMutator, context.Context) (soakRoomMutationOutcome, error)
	}{
		{
			name: "member add",
			call: func(m *soakRoomMutator, ctx context.Context) (soakRoomMutationOutcome, error) {
				return m.AddMember(ctx, "user-1", "room-1", "user-9")
			},
		},
		{
			name: "member remove",
			call: func(m *soakRoomMutator, ctx context.Context) (soakRoomMutationOutcome, error) {
				return m.RemoveMember(ctx, "user-1", "room-1", "user-9")
			},
		},
		{
			name: "rename",
			call: func(m *soakRoomMutator, ctx context.Context) (soakRoomMutationOutcome, error) {
				return m.Rename(ctx, "user-1", "room-1", "soak-room-r1")
			},
		},
		{
			name: "mute toggle",
			call: func(m *soakRoomMutator, ctx context.Context) (soakRoomMutationOutcome, error) {
				return m.ToggleMute(ctx, "user-2", "room-1")
			},
		},
		{
			name: "room create",
			call: func(m *soakRoomMutator, ctx context.Context) (soakRoomMutationOutcome, error) {
				return m.CreateRoom(ctx, "user-1", "soak-room", []string{"user-2"})
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			transport := &soakRoomOpsTransport{err: nats.ErrTimeout}

			outcome, err := testCase.call(newSoakRoomOpsMutator(transport), context.Background())

			require.Error(t, err)
			assert.False(t, outcome.Accepted)
			assert.Equal(t, soakErrorTimeout, outcome.ErrorClass)
			assert.Equal(t, 1, transport.calls(),
				"a non-idempotent mutation must never be replayed on the wire")
		})
	}
}

func TestSoakRoomMutator_ClassifiesExplicitRejection(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"error":"only owners can rename","code":"forbidden"}`),
	}

	outcome, err := newSoakRoomOpsMutator(transport).
		Rename(context.Background(), "user-2", "room-1", "soak-room-r1")

	require.Error(t, err)
	assert.False(t, outcome.Accepted)
	assert.Equal(t, soakErrorForbidden, outcome.ErrorClass)
}

func TestSoakRoomMutator_TreatsAnUnexpectedStatusAsNotAccepted(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"status":"queued"}`)}

	outcome, err := newSoakRoomOpsMutator(transport).
		AddMember(context.Background(), "user-1", "room-1", "user-9")

	require.NoError(t, err)
	assert.False(t, outcome.Accepted)
}

func TestSoakRoomMutator_TreatsAnExistingDMAsAccepted(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"status":"exists","roomId":"room-old","roomType":"dm"}`),
	}

	outcome, err := newSoakRoomOpsMutator(transport).
		CreateRoom(context.Background(), "user-1", "soak-room", []string{"user-2"})

	require.NoError(t, err)
	assert.True(t, outcome.Accepted, "an already-existing room is a successful create outcome")
	assert.Equal(t, "room-old", outcome.RoomID)
}

func TestSoakRoomMutator_RejectsMissingArguments(t *testing.T) {
	mutator := newSoakRoomOpsMutator(&soakRoomOpsTransport{reply: []byte(`{"status":"accepted"}`)})

	for _, testCase := range []struct {
		name string
		call func(context.Context) (soakRoomMutationOutcome, error)
	}{
		{
			name: "add without requester",
			call: func(ctx context.Context) (soakRoomMutationOutcome, error) {
				return mutator.AddMember(ctx, "", "room-1", "user-9")
			},
		},
		{
			name: "remove without account",
			call: func(ctx context.Context) (soakRoomMutationOutcome, error) {
				return mutator.RemoveMember(ctx, "user-1", "room-1", "")
			},
		},
		{
			name: "rename without name",
			call: func(ctx context.Context) (soakRoomMutationOutcome, error) {
				return mutator.Rename(ctx, "user-1", "room-1", "")
			},
		},
		{
			name: "mute without room",
			call: func(ctx context.Context) (soakRoomMutationOutcome, error) {
				return mutator.ToggleMute(ctx, "user-2", "")
			},
		},
		{
			name: "create without users",
			call: func(ctx context.Context) (soakRoomMutationOutcome, error) {
				return mutator.CreateRoom(ctx, "user-1", "soak-room", nil)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := testCase.call(context.Background())

			require.Error(t, err)
		})
	}
}
