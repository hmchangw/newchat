package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"strings"
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

func TestSoakRoomReader_SubscriptionsForSendsAnAcceptedListType(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
	reader, _, _ := newSoakRoomReadFixture(t, transport, 1)

	_, err := reader.SubscriptionsFor(context.Background(), "user-a0")
	require.NoError(t, err)

	require.Len(t, transport.bodies, 1)
	body := decodeSoakRequestBody(t, transport.bodies[0])
	listType, _ := body["type"].(string)
	assert.True(t, soakValidSubscriptionListTypes[listType],
		"the room-state observer's RPC source is rejected without a type; got %q", listType)
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

func TestSoakUserReader_SubscriptionChannelsSendsExactlyOneMemberSelector(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
	reader, _ := newSoakUserReadFixture(t, transport, 1)

	require.NoError(t, reader.SubscriptionChannels(context.Background()))

	require.Len(t, transport.bodies, 1)
	body := decodeSoakRequestBody(t, transport.bodies[0])
	contains, _ := body["membersContain"].(string)
	names, _ := body["accountNames"].([]any)
	assert.NotEqual(t, contains != "", len(names) > 0,
		"user-service requires exactly one of membersContain / accountNames, got %v", body)
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
	for i, requestSubject := range transport.subjects {
		body := decodeSoakRequestBody(t, transport.bodies[i])
		target, _ := body["accountName"].(string)
		requester := "user-a"
		peer := "user-b"
		if strings.Contains(requestSubject, "user-b") {
			requester, peer = "user-b", "user-a"
		}
		assert.Equal(t, subject.UserSubscriptionGetDM(requester, "site-a"), requestSubject)
		assert.Equal(t, peer, target,
			"a pair with no DM is a guaranteed not-found, so the lane must pick a real peer")
	}
}
