package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// The list types user-service accepts (user-service/service/subscriptions.go).
// A request that carries anything else — including the zero value a missing
// body decodes to — is rejected as "unknown subscription type".
var soakValidSubscriptionListTypes = map[string]bool{
	"current": true, "rooms": true, "apps": true,
}

func decodeSoakRequestBody(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var body map[string]any
	require.NoError(t, json.Unmarshal(data, &body),
		"request body must be a JSON object, got %q", string(data))
	return body
}

func TestSoakRoomReader_SubscriptionListSendsAnAcceptedListType(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 1)

	require.NoError(t, reader.SubscriptionList(context.Background()))

	require.Len(t, transport.bodies, 1)
	body := decodeSoakRequestBody(t, transport.bodies[0])
	listType, _ := body["type"].(string)
	assert.True(t, soakValidSubscriptionListTypes[listType],
		"user-service rejects any other type; got %q", listType)
}

// The room-state observer needs one room's subscription, not a page of the
// account's rooms. subscription.list is paged (SUBSCRIPTION_DEFAULT_LIMIT), and
// the verifiers treat "room not in the response" as a mismatch — the impossible
// state — so a room that merely fell off the page would be recorded as evidence
// of corruption. The by-room lookup answers 0-or-1 with no page to fall off.
func TestSoakRoomReader_SubscriptionForIsARoomScopedPointLookup(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[],"total":0}`)}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 1)

	_, err := reader.SubscriptionFor(context.Background(), "user-a0", "room-1")
	require.NoError(t, err)

	require.Len(t, transport.subjects, 1)
	assert.Equal(t,
		subject.UserSubscriptionGetByRoomID("user-a0", "site-a"),
		transport.subjects[0],
		"a paged list cannot distinguish an absent room from a paged-out one")
	require.Len(t, transport.bodies, 1)
	body := decodeSoakRequestBody(t, transport.bodies[0])
	assert.Equal(t, "room-1", body["roomId"])
}

// Two failure modes, one assertion each. Naming the requester collapses
// user-service's intersection to a single account so every channel matches;
// naming an account that shares no channel makes an empty page the lane's
// normal answer, which is indistinguishable from a broken query. Both make the
// lane look healthy while measuring nothing.
func TestSoakUserReader_SubscriptionChannelsAsksForARealCoMember(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
	// user-c is active but belongs to no channel, so a picker that draws any
	// active account will name it and produce a permanently empty page.
	reader, err := newSoakUserReader(
		soakUserReadConfig{SiteID: "site-a", PageLimit: 5, RequestTimeout: time.Second},
		&soakTopology{
			ActiveUsers: []model.User{
				{ID: "u1", Account: "user-a"},
				{ID: "u2", Account: "user-b"},
				{ID: "u3", Account: "user-c"},
			},
			Rooms: []model.Room{{ID: "room-1", Type: model.RoomTypeChannel}},
			Subscriptions: []model.Subscription{
				{RoomID: "room-1", RoomType: model.RoomTypeChannel, IsSubscribed: true,
					User: model.SubscriptionUser{ID: "u1", Account: "user-a"}},
				{RoomID: "room-1", RoomType: model.RoomTypeChannel, IsSubscribed: true,
					User: model.SubscriptionUser{ID: "u2", Account: "user-b"}},
			},
		},
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		&soakRoomReadRecorder{},
		rand.New(rand.NewSource(1)),
		nil,
	)
	require.NoError(t, err)

	for range 20 {
		require.NoError(t, reader.SubscriptionChannels(context.Background()))
	}

	require.Len(t, transport.subjects, 20)
	valid := map[string]string{
		subject.UserSubscriptionGetChannels("user-a", "site-a"): "user-b",
		subject.UserSubscriptionGetChannels("user-b", "site-a"): "user-a",
	}
	for i, requestSubject := range transport.subjects {
		coMember, known := valid[requestSubject]
		require.True(t, known, "unexpected requester in %q", requestSubject)
		assert.Equal(t, coMember,
			decodeSoakRequestBody(t, transport.bodies[i])["membersContain"],
			"the co-member must share a channel with the requester, and never be the requester")
	}
}

func TestSoakRoomReader_ReadReceiptsAsksAsTheMessageAuthor(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"readers":[]}`)}
	reader, pool, _ := newSoakRoomReadFixture(t, transport, 1)
	roomID := pool.RoomIDs()[0]
	// An author who is not the account the reader addresses the room as:
	// room-service answers read receipts only to the message's own sender.
	reader.SetMessageSource(&soakStubMessageSource{
		message: soakCatalogMessage{
			soakCatalogCandidate: soakCatalogCandidate{
				ID: "msg-1", RoomID: roomID, Author: "author-of-record",
			},
		},
		found: true,
	})

	require.NoError(t, reader.ReadReceipts(context.Background()))

	require.Len(t, transport.subjects, 1)
	assert.Contains(t, transport.subjects[0], "author-of-record",
		"asking as anyone but the sender is rejected with a permission error")
}

type soakStubMessageSource struct {
	message soakCatalogMessage
	found   bool
}

func (s *soakStubMessageSource) PickAnyEligible(
	roomID string,
	_ soakCatalogAction,
) (soakCatalogMessage, bool) {
	if !s.found {
		return soakCatalogMessage{}, false
	}
	message := s.message
	message.RoomID = roomID
	return message, true
}

// newSoakUserReadDMFixture builds a topology with one DM room whose two
// participants are known, so the DM read has a pair that actually resolves.
func newSoakUserReadDMFixture(
	t *testing.T,
	transport soakRPCTransport,
	seed int64,
) *soakUserReader {
	t.Helper()
	reader, err := newSoakUserReader(
		soakUserReadConfig{SiteID: "site-a", PageLimit: 5, RequestTimeout: time.Second},
		&soakTopology{
			ActiveUsers: []model.User{
				{ID: "u1", Account: "user-a"},
				{ID: "u2", Account: "user-b"},
				{ID: "u3", Account: "user-c"},
			},
			Rooms: []model.Room{
				{ID: "room-1", Type: model.RoomTypeChannel},
				{ID: "dm-1", Type: model.RoomTypeDM},
			},
			Subscriptions: []model.Subscription{
				{RoomID: "dm-1", RoomType: model.RoomTypeDM, IsSubscribed: true,
					User: model.SubscriptionUser{ID: "u1", Account: "user-a"}},
				{RoomID: "dm-1", RoomType: model.RoomTypeDM, IsSubscribed: true,
					User: model.SubscriptionUser{ID: "u2", Account: "user-b"}},
				{RoomID: "room-1", RoomType: model.RoomTypeChannel, IsSubscribed: true,
					User: model.SubscriptionUser{ID: "u3", Account: "user-c"}},
			},
		},
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		&soakRoomReadRecorder{},
		rand.New(rand.NewSource(seed)),
		nil,
	)
	require.NoError(t, err)
	return reader
}

func TestSoakUserReader_SubscriptionDMTargetsAKnownDMPeer(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscription":{"id":"dm-1"}}`)}
	reader := newSoakUserReadDMFixture(t, transport, 1)

	// Repeated so a lucky single draw cannot pass a picker that still chooses
	// an arbitrary pair.
	for range 20 {
		require.NoError(t, reader.SubscriptionDM(context.Background()))
	}

	require.Len(t, transport.subjects, 20)
	// Compared as whole (subject, target) pairs rather than by substring: an
	// account name that is a prefix of another would make a Contains check pass
	// on the wrong requester.
	valid := map[string]string{
		subject.UserSubscriptionGetDM("user-a", "site-a"): "user-b",
		subject.UserSubscriptionGetDM("user-b", "site-a"): "user-a",
	}
	for i, requestSubject := range transport.subjects {
		body := decodeSoakRequestBody(t, transport.bodies[i])
		peer, known := valid[requestSubject]
		require.True(t, known, "unexpected requester in %q", requestSubject)
		assert.Equal(t, peer, body["accountName"],
			"a pair with no DM is a guaranteed not-found, so the lane must pick a real peer")
	}
}

// Channel and DM membership is represented by row existence. Production does
// not set isSubscribed for these rows; only botDM uses it as a soft toggle.
func TestSoakUserReader_RoomPairsUseExistingDMMembershipRows(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscription":{"id":"dm-1"}}`)}
	reader, err := newSoakUserReader(
		soakUserReadConfig{SiteID: "site-a", PageLimit: 5, RequestTimeout: time.Second},
		&soakTopology{
			ActiveUsers: []model.User{
				{ID: "u1", Account: "user-a"},
				{ID: "u2", Account: "user-b"},
			},
			Rooms: []model.Room{{ID: "dm-1", Type: model.RoomTypeDM}},
			Subscriptions: []model.Subscription{
				{RoomID: "dm-1", RoomType: model.RoomTypeDM,
					User: model.SubscriptionUser{ID: "u1", Account: "user-a"}},
				{RoomID: "dm-1", RoomType: model.RoomTypeDM,
					User: model.SubscriptionUser{ID: "u2", Account: "user-b"}},
			},
		},
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		&soakRoomReadRecorder{},
		rand.New(rand.NewSource(3)),
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, reader.SubscriptionDM(context.Background()))

	require.Len(t, transport.subjects, 1)
}

// The user lane addresses 2000 active accounts by design. DM rooms are seeded
// active↔active and active↔borrowed, so indexing every participant as a
// requester would silently start issuing reads as borrowed accounts no other
// lane touches — a change in what the lane measures that nothing would flag.
func TestSoakUserReader_SubscriptionDMAsksFromTheActiveSide(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscription":{"id":"dm-1"}}`)}
	reader, err := newSoakUserReader(
		soakUserReadConfig{SiteID: "site-a", PageLimit: 5, RequestTimeout: time.Second},
		&soakTopology{
			ActiveUsers: []model.User{{ID: "u1", Account: "user-a"}},
			Rooms:       []model.Room{{ID: "dm-1", Type: model.RoomTypeDM}},
			Subscriptions: []model.Subscription{
				{RoomID: "dm-1", RoomType: model.RoomTypeDM, IsSubscribed: true,
					User: model.SubscriptionUser{ID: "u1", Account: "user-a"}},
				// Borrowed, never an active account.
				{RoomID: "dm-1", RoomType: model.RoomTypeDM, IsSubscribed: true,
					User: model.SubscriptionUser{ID: "u9", Account: "borrowed-z"}},
			},
		},
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		&soakRoomReadRecorder{},
		rand.New(rand.NewSource(5)),
		nil,
	)
	require.NoError(t, err)

	for range 10 {
		require.NoError(t, reader.SubscriptionDM(context.Background()))
	}

	require.Len(t, transport.subjects, 10)
	for i, requestSubject := range transport.subjects {
		assert.Equal(t, subject.UserSubscriptionGetDM("user-a", "site-a"), requestSubject)
		assert.Equal(t, "borrowed-z",
			decodeSoakRequestBody(t, transport.bodies[i])["accountName"])
	}
}

// The active-account filter must choose which participants may be the
// requester, not be applied to a pre-truncated slice. A channel whose first two
// members are both borrowed still has active members who can ask about it, and
// dropping it shrinks the lane to whichever rooms happen to be ordered well.
func TestSoakUserReader_ChannelPairsFindActiveMembersBeyondTheFirstTwo(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
	subscribed := func(account string) model.Subscription {
		return model.Subscription{
			RoomID: "room-1", RoomType: model.RoomTypeChannel, IsSubscribed: true,
			User: model.SubscriptionUser{ID: account, Account: account},
		}
	}
	reader, err := newSoakUserReader(
		soakUserReadConfig{SiteID: "site-a", PageLimit: 5, RequestTimeout: time.Second},
		&soakTopology{
			ActiveUsers: []model.User{{ID: "active-a", Account: "active-a"}},
			Rooms:       []model.Room{{ID: "room-1", Type: model.RoomTypeChannel}},
			Subscriptions: []model.Subscription{
				subscribed("borrowed-a"), subscribed("borrowed-b"), subscribed("active-a"),
			},
		},
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		&soakRoomReadRecorder{},
		rand.New(rand.NewSource(7)),
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, reader.SubscriptionChannels(context.Background()))

	require.Len(t, transport.subjects, 1,
		"the room has an active member, so the lane must not skip it")
	assert.Equal(t, subject.UserSubscriptionGetChannels("active-a", "site-a"),
		transport.subjects[0])
	assert.Contains(t, []any{"borrowed-a", "borrowed-b"},
		decodeSoakRequestBody(t, transport.bodies[0])["membersContain"])
}

// A room whose rows all name the same account has no counterpart to ask about.
// The previous index guarded this by comparing the first two participants; the
// scan that replaced it has to keep refusing, otherwise the lane would send an
// account its own name as membersContain — the degenerate query that made
// getChannels return nothing in the first place.
func TestSoakUserReader_ChannelPairsSkipRoomsWithASingleDistinctMember(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
	duplicated := model.Subscription{
		RoomID: "room-1", RoomType: model.RoomTypeChannel, IsSubscribed: true,
		User: model.SubscriptionUser{ID: "active-a", Account: "active-a"},
	}
	reader, err := newSoakUserReader(
		soakUserReadConfig{SiteID: "site-a", PageLimit: 5, RequestTimeout: time.Second},
		&soakTopology{
			ActiveUsers:   []model.User{{ID: "active-a", Account: "active-a"}},
			Rooms:         []model.Room{{ID: "room-1", Type: model.RoomTypeChannel}},
			Subscriptions: []model.Subscription{duplicated, duplicated},
		},
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		&soakRoomReadRecorder{},
		rand.New(rand.NewSource(11)),
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, reader.SubscriptionChannels(context.Background()))

	assert.Empty(t, transport.subjects,
		"the room names one account twice, so there is no co-member to query for")
}
