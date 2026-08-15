package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/subject"
)

func startInProcessNATS(t *testing.T) *nats.Conn {
	t.Helper()
	opts := &natsserver.Options{Port: -1}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

func TestNATSMemberListClient_HappyPath(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	members := []model.RoomMember{
		{Member: model.RoomMemberEntry{ID: "u1", Type: model.RoomMemberIndividual, Account: "bob"}},
		{Member: model.RoomMemberEntry{ID: "org1", Type: model.RoomMemberOrg}},
	}

	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		resp := model.ListRoomMembersResponse{Members: members}
		data, _ := json.Marshal(resp)
		_ = m.Respond(data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	got, err := client.ListMembers(context.Background(), requester, ch, 0)
	require.NoError(t, err)
	assert.Equal(t, members, got)
}

func TestNATSMemberListClient_RecordsBoundedRequestResult(t *testing.T) {
	nc := startInProcessNATS(t)
	recorder := NewMockrequestRecorder(gomock.NewController(t))
	recorder.EXPECT().Request(gomock.Any(), natsmetrics.OperationMemberRead, gomock.Any(), gomock.Nil()).
		Do(func(_ context.Context, _ natsmetrics.Operation, duration time.Duration, _ error) {
			assert.GreaterOrEqual(t, duration, time.Duration(0))
		})
	client := NewNATSMemberListClient(nc, 2*time.Second, withMemberListRequestRecorder(recorder))
	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}

	sub, err := nc.Subscribe(subject.MemberList("alice", ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		data, _ := json.Marshal(model.ListRoomMembersResponse{})
		_ = m.Respond(data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), "alice", ch, 0)
	require.NoError(t, err)
}

func TestNATSMemberListClient_RecordsFinalRequestFailure(t *testing.T) {
	tests := []struct {
		name     string
		response []byte
	}{
		{name: "remote errcode", response: []byte(`{"code":"not_found","error":"room not found"}`)},
		{name: "invalid JSON", response: []byte(`{not json`)},
		{name: "no responder"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := startInProcessNATS(t)
			ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
			if tt.response != nil {
				sub, err := nc.Subscribe(subject.MemberList("alice", ch.RoomID, ch.SiteID), func(m *nats.Msg) {
					_ = m.Respond(tt.response)
				})
				require.NoError(t, err)
				t.Cleanup(func() { _ = sub.Unsubscribe() })
			}
			recorder := NewMockrequestRecorder(gomock.NewController(t))
			recorder.EXPECT().Request(gomock.Any(), natsmetrics.OperationMemberRead, gomock.Any(), gomock.Not(gomock.Nil()))
			client := NewNATSMemberListClient(nc, 2*time.Second, withMemberListRequestRecorder(recorder))

			_, err := client.ListMembers(context.Background(), "alice", ch, 0)

			require.Error(t, err)
		})
	}
}

func TestNATSMemberListClient_RemoteError(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	// Generic remote error (not the not-member reason) is reconstructed as a
	// typed *errcode.Error preserving the remote code and message.
	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		data, _ := json.Marshal(errcode.NotFound("room not found"))
		_ = m.Respond(data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), requester, ch, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "room not found")
	var ee *errcode.Error
	require.ErrorAs(t, err, &ee)
	assert.Equal(t, errcode.CodeNotFound, ee.Code)
	assert.False(t, errors.Is(err, errNotRoomMember), "generic remote errors must not masquerade as the sentinel")
}

func TestNATSMemberListClient_RemoteNotMember_MapsToSentinel(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	// Remote site replies with an errcode envelope carrying reason
	// not_room_member — the client must map it back onto the local
	// errNotRoomMember sentinel so cross-site and same-site "not a member"
	// behave uniformly under errors.Is.
	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		data := []byte(`{"code":"forbidden","reason":"not_room_member","error":"only room members can perform this action"}`)
		_ = m.Respond(data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), requester, ch, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errNotRoomMember))
}

func TestNATSMemberListClient_InvalidJSONReply(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		_ = m.Respond([]byte(`{not json`))
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), requester, ch, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal member.list reply")
}

func TestNATSMemberListClient_Timeout(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 100*time.Millisecond)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	// Responder sleeps longer than the client timeout so the context deadline must fire first.
	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		time.Sleep(500 * time.Millisecond)
		_ = m.Respond([]byte(`{}`))
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), requester, ch, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "expected deadline exceeded, got %v", err)
	assert.Contains(t, err.Error(), "member.list request to site-us")
}

func TestNATSMemberListClient_BodyShape(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	// Capture the parsed body via a buffered channel so -race sees a happens-before edge.
	bodyCh := make(chan model.ListRoomMembersRequest, 1)
	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		var parsed model.ListRoomMembersRequest
		_ = json.Unmarshal(m.Data, &parsed)
		bodyCh <- parsed
		resp := model.ListRoomMembersResponse{}
		data, _ := json.Marshal(resp)
		_ = m.Respond(data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), requester, ch, 0)
	require.NoError(t, err)
	received := <-bodyCh
	assert.Nil(t, received.Limit)
	assert.Nil(t, received.Offset)
	assert.False(t, received.Enrich)
}

func TestNATSMemberListClient_LimitPropagated(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	bodyCh := make(chan model.ListRoomMembersRequest, 1)
	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		var parsed model.ListRoomMembersRequest
		_ = json.Unmarshal(m.Data, &parsed)
		bodyCh <- parsed
		data, _ := json.Marshal(model.ListRoomMembersResponse{})
		_ = m.Respond(data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), requester, ch, 250)
	require.NoError(t, err)
	received := <-bodyCh
	require.NotNil(t, received.Limit, "limit must be forwarded to the remote so it can cap the response at the wire layer")
	assert.Equal(t, 250, *received.Limit)
}

func TestNATSMemberListClient_SubjectCorrectness(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 2*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	expectedSubj := subject.MemberList(requester, ch.RoomID, ch.SiteID)
	subjCh := make(chan string, 1)
	sub, err := nc.Subscribe(expectedSubj, func(m *nats.Msg) {
		subjCh <- m.Subject
		data, _ := json.Marshal(model.ListRoomMembersResponse{})
		_ = m.Respond(data)
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	_, err = client.ListMembers(context.Background(), requester, ch, 0)
	require.NoError(t, err)
	assert.Equal(t, expectedSubj, <-subjCh)
}

func TestNATSMemberListClient_ContextCancellation(t *testing.T) {
	nc := startInProcessNATS(t)
	client := NewNATSMemberListClient(nc, 5*time.Second)

	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.ListMembers(ctx, "alice", ch, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got %v", err)
	assert.Contains(t, err.Error(), "member.list request to site-us")
}

// TestNATSMemberListClient_NotRoomMemberIsNotARequestFailure pins the metric
// semantics for a business rejection. "You are not a member" is a complete,
// well-formed answer from a healthy remote — the request/reply exchange
// succeeded. Counting it as a failed request puts chat_nats_requests into the
// transport-shaped other_error bucket at baseline and makes the family
// unusable as a failure signal, which is the same rule GetMessageReadMeta
// already follows for CodeNotFound.
func TestNATSMemberListClient_NotRoomMemberIsNotARequestFailure(t *testing.T) {
	nc := startInProcessNATS(t)
	ch := model.ChannelRef{RoomID: "room-eng", SiteID: "site-us"}
	requester := "alice"

	sub, err := nc.Subscribe(subject.MemberList(requester, ch.RoomID, ch.SiteID), func(m *nats.Msg) {
		_ = m.Respond([]byte(`{"code":"forbidden","reason":"not_room_member","error":"only room members can perform this action"}`))
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	recorder := NewMockrequestRecorder(gomock.NewController(t))
	recorder.EXPECT().Request(gomock.Any(), natsmetrics.OperationMemberRead, gomock.Any(), gomock.Nil())
	client := NewNATSMemberListClient(nc, 2*time.Second, withMemberListRequestRecorder(recorder))

	_, err = client.ListMembers(context.Background(), requester, ch, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errNotRoomMember))
}
