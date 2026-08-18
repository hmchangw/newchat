package main

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/subject"
)

type soakRoomReadRecorder struct {
	samples []soakReadSample
}

func (r *soakRoomReadRecorder) Record(sample soakReadSample) {
	r.samples = append(r.samples, sample)
}

func newSoakRoomReadFixture(
	t *testing.T,
	transport soakRPCTransport,
	seed int64,
) (*soakRoomReader, *soakRoomStatePool, *soakRoomReadRecorder) {
	t.Helper()
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)
	recorder := &soakRoomReadRecorder{}
	reader := newSoakRoomReader(
		soakRoomReadConfig{SiteID: "site-a", BatchSize: 2, RequestTimeout: time.Second},
		pool,
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		recorder,
		rand.New(rand.NewSource(seed)),
		nil,
	)
	return reader, pool, recorder
}

func TestSoakRoomReader_ListMembersUsesTheRoomMemberSubject(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[{"id":"m1","rid":"room-1"}]}`)}
	reader, _, recorder := newSoakRoomReadFixture(t, transport, 1)

	response, err := reader.ListMembers(context.Background(), "room-1", "user-a0")

	require.NoError(t, err)
	require.Len(t, response.Members, 1)
	assert.Equal(t, subject.MemberList("user-a0", "room-1", "site-a"), transport.subjects[0])
	require.Len(t, recorder.samples, 1)
	assert.Equal(t, soakRPCMemberList, recorder.samples[0].Action)
	assert.Equal(t, 1, recorder.samples[0].Messages)
}

func TestSoakRoomReader_RoomsInfoBatchesRoomIDs(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"rooms":[{"roomId":"room-1","found":true}]}`)}
	reader, _, recorder := newSoakRoomReadFixture(t, transport, 2)

	require.NoError(t, reader.RoomsInfo(context.Background()))

	assert.Equal(t, subject.RoomsInfoBatchSubscribe("site-a"), transport.subjects[0])
	assert.JSONEq(t, `{"roomIds":["room-1"]}`, string(transport.bodies[0]))
	require.Len(t, recorder.samples, 1)
	assert.Equal(t, soakRPCRoomsInfo, recorder.samples[0].Action)
}

func TestSoakRoomReader_SubscriptionListUsesTheUserSubject(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"subscriptions":[{"roomId":"room-1","muted":false}],"hasMore":false}`),
	}
	reader, _, recorder := newSoakRoomReadFixture(t, transport, 3)

	require.NoError(t, reader.SubscriptionList(context.Background()))

	assert.Equal(t, subject.UserSubscriptionList("user-a0", "site-a"), transport.subjects[0])
	require.Len(t, recorder.samples, 1)
	assert.Equal(t, soakRPCSubscriptionList, recorder.samples[0].Action)
}

func TestSoakRoomReader_RoomStateReadIsADistinctAction(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[]}`)}
	reader, _, recorder := newSoakRoomReadFixture(t, transport, 4)

	_, err := reader.RoomState(context.Background(), "room-1", "user-a0")

	require.NoError(t, err)
	require.Len(t, recorder.samples, 1)
	assert.Equal(t, soakRPCRoomStateRead, recorder.samples[0].Action,
		"verification read-back must not be counted as production read traffic")
}

func TestSoakRoomReader_ReadMixedRecordsExactlyOneSample(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[],"rooms":[],"subscriptions":[]}`)}
	reader, _, recorder := newSoakRoomReadFixture(t, transport, 5)

	for range 12 {
		require.NoError(t, reader.ReadMixed(context.Background()))
	}

	assert.Len(t, recorder.samples, 12)
	actions := make(map[soakRPCAction]int)
	for _, sample := range recorder.samples {
		actions[sample.Action]++
	}
	assert.GreaterOrEqual(t, len(actions), 2, "the read mix must exercise more than one shape")
}

func TestSoakRoomReader_RecordsTransportFailures(t *testing.T) {
	transport := &soakRoomOpsTransport{err: nats.ErrNoResponders}
	reader, _, recorder := newSoakRoomReadFixture(t, transport, 6)

	_, err := reader.ListMembers(context.Background(), "room-1", "user-a0")

	require.Error(t, err)
	require.Len(t, recorder.samples, 1)
	assert.Equal(t, soakErrorNoResponder, recorder.samples[0].ErrorClass)
}

func TestSoakRoomReader_RegisteredCreatedRoomJoinsTheReadMix(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[],"rooms":[]}`)}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 7)
	reader.RegisterCreatedRoom("room-created", "user-a0")

	account, ok := reader.Account("room-created")
	require.True(t, ok)
	assert.Equal(t, "user-a0", account)

	seen := false
	for range 40 {
		require.NoError(t, reader.RoomsInfo(context.Background()))
	}
	for _, body := range transport.bodies {
		if strings.Contains(string(body), "room-created") {
			seen = true
			break
		}
	}
	assert.True(t, seen, "a created room must become a read target")
}

func TestSoakRoomReader_IgnoresEmptyCreatedRoomRegistration(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"rooms":[]}`)}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 8)

	reader.RegisterCreatedRoom("", "user-a0")
	reader.RegisterCreatedRoom("room-x", "")

	_, ok := reader.Account("room-x")
	assert.False(t, ok)
}

func TestSoakRoomReader_SkipsWhenNoRoomIsAvailable(t *testing.T) {
	recorder := &soakRoomReadRecorder{}
	reader := newSoakRoomReader(
		soakRoomReadConfig{SiteID: "site-a"},
		nil,
		newSoakRPCClient(&soakRoomOpsTransport{}, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		recorder,
		rand.New(rand.NewSource(9)),
		nil,
	)

	require.NoError(t, reader.ReadMixed(context.Background()))
	require.NoError(t, reader.RoomsInfo(context.Background()))
	require.NoError(t, reader.SubscriptionList(context.Background()))

	require.NotEmpty(t, recorder.samples)
	for _, sample := range recorder.samples {
		assert.True(t, sample.Skipped)
	}
}

func TestSoakRoomReader_RejectsMissingArguments(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[]}`)}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 10)

	_, err := reader.ListMembers(context.Background(), "", "user-a0")
	require.Error(t, err)

	_, err = reader.RoomState(context.Background(), "room-1", "")
	require.Error(t, err)
}

func TestSoakRoomReader_RoomInfoForReturnsTheRequestedRoom(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"rooms":[{"roomId":"other","found":true},{"roomId":"room-1","found":true,"name":"soak-channel","userCount":4}]}`),
	}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 11)

	info, err := reader.RoomInfoFor(context.Background(), "room-1")

	require.NoError(t, err)
	assert.True(t, info.Found)
	assert.Equal(t, "soak-channel", info.Name)
	assert.JSONEq(t, `{"roomIds":["room-1"]}`, string(transport.bodies[0]))
}

func TestSoakRoomReader_RoomInfoForReportsAnAbsentRoom(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"rooms":[]}`)}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 12)

	info, err := reader.RoomInfoFor(context.Background(), "room-1")

	require.NoError(t, err)
	assert.False(t, info.Found, "a room the batch omits is reported absent, not as an error")
}

func TestSoakRoomReader_RoomInfoForRejectsAnEmptyRoom(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"rooms":[]}`)}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 13)

	_, err := reader.RoomInfoFor(context.Background(), "")

	require.Error(t, err)
}

func TestSoakRoomReader_RoomInfoForSurfacesTransportFailures(t *testing.T) {
	transport := &soakRoomOpsTransport{err: nats.ErrNoResponders}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 14)

	_, err := reader.RoomInfoFor(context.Background(), "room-1")

	require.Error(t, err)
}

func TestSoakRoomReader_SubscriptionForRejectsAnEmptyAccountOrRoom(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 15)

	_, err := reader.SubscriptionFor(context.Background(), "", "room-1")
	require.Error(t, err)

	_, err = reader.SubscriptionFor(context.Background(), "user-a0", "")
	require.Error(t, err, "an unscoped lookup would answer about the wrong room")
}

func TestSoakRoomReader_RequiresAnRPCClient(t *testing.T) {
	reader := newSoakRoomReader(
		soakRoomReadConfig{SiteID: "site-a"}, nil, nil, nil,
		rand.New(rand.NewSource(16)), nil,
	)

	_, err := reader.ListMembers(context.Background(), "room-1", "user-a0")

	require.Error(t, err)
}

// soakRoomMessageStub stands in for the catalog when the read-receipt read
// needs a persisted message to ask about.
type soakRoomMessageStub struct {
	message soakCatalogMessage
	found   bool
	rooms   []string
	actions []soakCatalogAction
}

func (s *soakRoomMessageStub) PickAnyEligible(
	roomID string,
	action soakCatalogAction,
) (soakCatalogMessage, bool) {
	s.rooms = append(s.rooms, roomID)
	s.actions = append(s.actions, action)
	return s.message, s.found
}

func TestSoakRoomReader_ReadReceiptsUsesTheRoomMessageSubject(t *testing.T) {
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"readers":[{"userId":"u1","account":"user-a0"}]}`),
	}
	reader, _, recorder := newSoakRoomReadFixture(t, transport, 11)
	messages := &soakRoomMessageStub{
		message: soakCatalogMessage{
			soakCatalogCandidate: soakCatalogCandidate{
				ID: "msg-1", RoomID: "room-1", Author: "user-a0",
			},
		}, found: true,
	}
	reader.SetMessageSource(messages)

	err := reader.ReadReceipts(context.Background())

	require.NoError(t, err)
	require.Len(t, transport.subjects, 1)
	assert.True(t, strings.HasSuffix(transport.subjects[0], ".message.read-receipt"),
		"subject=%s", transport.subjects[0])
	assert.Contains(t, transport.subjects[0], "room-1")
	assert.Contains(t, transport.subjects[0], "user-a0",
		"the request is addressed as the message's author")
	require.Len(t, recorder.samples, 1)
	assert.Equal(t, soakRPCReadReceiptList, recorder.samples[0].Action)
	assert.Equal(t, 1, recorder.samples[0].Messages, "the reader count is the payload size")
	assert.Equal(t, []soakCatalogAction{soakCatalogReadReceipt}, messages.actions)
}

// A room with no persisted message yet is a skip, not an error: asking for
// receipts on a message that does not exist would only measure a 400.
func TestSoakRoomReader_ReadReceiptsSkipsWhenNoMessageIsEligible(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"readers":[]}`)}
	reader, _, recorder := newSoakRoomReadFixture(t, transport, 12)
	reader.SetMessageSource(&soakRoomMessageStub{found: false})

	err := reader.ReadReceipts(context.Background())

	require.NoError(t, err)
	assert.Empty(t, transport.subjects)
	require.Len(t, recorder.samples, 1)
	assert.Equal(t, soakRPCReadReceiptList, recorder.samples[0].Action)
	assert.True(t, recorder.samples[0].Skipped)
}

// Without a message source wired the read is inert rather than panicking, so a
// configuration that omits the catalog degrades to the other room reads.
func TestSoakRoomReader_ReadReceiptsSkipsWithoutAMessageSource(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"readers":[]}`)}
	reader, _, recorder := newSoakRoomReadFixture(t, transport, 13)

	err := reader.ReadReceipts(context.Background())

	require.NoError(t, err)
	assert.Empty(t, transport.subjects)
	require.Len(t, recorder.samples, 1)
	assert.True(t, recorder.samples[0].Skipped)
}

// Every bounded action label must correspond to traffic the soak actually
// sends; an allowlisted-but-undispatched label reads as coverage it does not
// have.
func TestSoakRoomReader_ReadMixedEventuallyDispatchesEveryRoomReadAction(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"members":[],"rooms":[],"subscriptions":[],"readers":[]}`)}
	reader, _, recorder := newSoakRoomReadFixture(t, transport, 7)
	reader.SetMessageSource(&soakRoomMessageStub{
		message: soakCatalogMessage{
			soakCatalogCandidate: soakCatalogCandidate{ID: "msg-1", RoomID: "room-1"},
		}, found: true,
	})

	for range 400 {
		require.NoError(t, reader.ReadMixed(context.Background()))
	}

	seen := map[soakRPCAction]bool{}
	for i := range recorder.samples {
		seen[recorder.samples[i].Action] = true
	}
	for _, action := range []soakRPCAction{
		soakRPCMemberList, soakRPCRoomsInfo, soakRPCSubscriptionList,
		soakRPCReadReceiptList,
	} {
		assert.True(t, seen[action], "action=%s", action)
	}
}
