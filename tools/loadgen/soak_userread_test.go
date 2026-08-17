package main

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

func newSoakUserReadFixture(
	t *testing.T,
	transport soakRPCTransport,
	seed int64,
) (*soakUserReader, *soakRoomReadRecorder) {
	t.Helper()
	recorder := &soakRoomReadRecorder{}
	reader, err := newSoakUserReader(
		soakUserReadConfig{SiteID: "site-a", PageLimit: 5, RequestTimeout: time.Second},
		&soakTopology{
			ActiveUsers: []model.User{
				{ID: "u1", Account: "user-a"},
				{ID: "u2", Account: "user-b"},
			},
			Rooms: []model.Room{
				{ID: "room-1", Type: model.RoomTypeChannel},
				// The DM read draws its peer from a real DM room, so the shared
				// fixture carries one between the same two accounts.
				{ID: "dm-1", Type: model.RoomTypeDM},
			},
			Subscriptions: []model.Subscription{
				{RoomID: "dm-1", RoomType: model.RoomTypeDM, IsSubscribed: true,
					User: model.SubscriptionUser{ID: "u1", Account: "user-a"}},
				{RoomID: "dm-1", RoomType: model.RoomTypeDM, IsSubscribed: true,
					User: model.SubscriptionUser{ID: "u2", Account: "user-b"}},
			},
		},
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		recorder,
		rand.New(rand.NewSource(seed)),
		nil,
	)
	require.NoError(t, err)
	return reader, recorder
}

func TestNewSoakUserReader_RequiresATopologyWithAccounts(t *testing.T) {
	_, err := newSoakUserReader(soakUserReadConfig{SiteID: "site-a"}, nil, nil, nil, nil, nil)
	require.Error(t, err)

	_, err = newSoakUserReader(
		soakUserReadConfig{SiteID: "site-a"}, &soakTopology{}, nil, nil, nil, nil,
	)
	require.Error(t, err, "a lane with no account to read as would silently send nothing")
}

func TestSoakUserReader_EachReadTargetsItsOwnSubject(t *testing.T) {
	tests := []struct {
		name    string
		call    func(*soakUserReader, context.Context) error
		action  soakRPCAction
		subject func(account string) string
		reply   string
	}{
		{
			name: "me", call: (*soakUserReader).Me, action: soakRPCUserMe,
			subject: func(a string) string { return subject.UserMe(a, "site-a") },
			reply:   `{"account":"user-a"}`,
		},
		{
			name: "profile", call: (*soakUserReader).ProfileByName,
			action:  soakRPCUserProfileGet,
			subject: func(a string) string { return subject.UserProfileGetByName(a, "site-a") },
			reply:   `{"account":"user-b"}`,
		},
		{
			name: "status", call: (*soakUserReader).StatusByName,
			action:  soakRPCUserStatusGet,
			subject: func(a string) string { return subject.UserStatusGetByName(a, "site-a") },
			reply:   `{"account":"user-b"}`,
		},
		{
			name: "settings", call: (*soakUserReader).Settings,
			action:  soakRPCUserSettingsGet,
			subject: func(a string) string { return subject.UserSettingsGet(a, "site-a") },
			reply:   `{"permissions":{"canPost":true}}`,
		},
		{
			name: "chatlist", call: (*soakUserReader).Chatlist,
			action:  soakRPCUserChatlistGet,
			subject: func(a string) string { return subject.UserChatlistGet(a, "site-a") },
			reply:   `{"sections":[{"id":"s1"}]}`,
		},
		{
			name: "priority contacts", call: (*soakUserReader).PriorityContacts,
			action:  soakRPCUserPriorityContacts,
			subject: func(a string) string { return subject.UserPriorityContactsGet(a, "site-a") },
			reply:   `{"contacts":[{"account":"user-b"}]}`,
		},
		{
			name: "apps list", call: (*soakUserReader).AppsList,
			action:  soakRPCUserAppsList,
			subject: func(a string) string { return subject.UserAppsList(a, "site-a") },
			reply:   `{"apps":[{"id":"app-1"}],"hasMore":false}`,
		},
		{
			name: "apps categories", call: (*soakUserReader).AppsCategories,
			action:  soakRPCUserAppsCategories,
			subject: func(a string) string { return subject.UserAppsCategories(a, "site-a") },
			reply:   `{"categories":[{"id":"chat","name":"Chat","siteId":"site-a"}]}`,
		},
		{
			name: "subscription count", call: (*soakUserReader).SubscriptionCount,
			action:  soakRPCUserSubscriptionCount,
			subject: func(a string) string { return subject.UserSubscriptionCount(a, "site-a") },
			reply:   `{"count":7}`,
		},
		{
			name: "subscription by room", call: (*soakUserReader).SubscriptionByRoom,
			action:  soakRPCUserSubscriptionByRoom,
			subject: func(a string) string { return subject.UserSubscriptionGetByRoomID(a, "site-a") },
			reply:   `{"subscriptions":[{"roomId":"room-1"}]}`,
		},
		{
			name: "subscription channels", call: (*soakUserReader).SubscriptionChannels,
			action:  soakRPCUserSubscriptionChannel,
			subject: func(a string) string { return subject.UserSubscriptionGetChannels(a, "site-a") },
			reply:   `{"subscriptions":[{"roomId":"room-1"}],"hasMore":false}`,
		},
		{
			name: "subscription dm", call: (*soakUserReader).SubscriptionDM,
			action:  soakRPCUserSubscriptionDM,
			subject: func(a string) string { return subject.UserSubscriptionGetDM(a, "site-a") },
			reply:   `{"subscription":{"roomId":"dm-1"}}`,
		},
		{
			name: "thread list", call: (*soakUserReader).ThreadList,
			action:  soakRPCUserThreadList,
			subject: func(a string) string { return subject.UserThreadList(a, "site-a") },
			reply:   `{"items":[{"threadRoomId":"t1"}],"hasNext":false}`,
		},
		{
			name: "thread unread", call: (*soakUserReader).ThreadUnread,
			action:  soakRPCUserThreadUnread,
			subject: func(a string) string { return subject.UserThreadUnreadSummary(a, "site-a") },
			reply:   `{"unread":true,"unreadDirectMessage":false,"unreadMention":false}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &soakRoomOpsTransport{reply: []byte(tt.reply)}
			reader, recorder := newSoakUserReadFixture(t, transport, 3)

			require.NoError(t, tt.call(reader, context.Background()))

			require.Len(t, transport.subjects, 1)
			// The requester is drawn at random, so assert the subject matches
			// whichever account the reader chose rather than pinning one.
			assert.Contains(t,
				[]string{tt.subject("user-a"), tt.subject("user-b")},
				transport.subjects[0],
			)
			require.Len(t, recorder.samples, 1)
			assert.Equal(t, tt.action, recorder.samples[0].Action)
			assert.False(t, recorder.samples[0].Skipped)
		})
	}
}

func TestSoakUserReader_CountsPayloadSizes(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"count":7}`)}
	reader, recorder := newSoakUserReadFixture(t, transport, 4)

	require.NoError(t, reader.SubscriptionCount(context.Background()))

	require.Len(t, recorder.samples, 1)
	assert.Equal(t, 7, recorder.samples[0].Messages)
}

// A topology with no rooms cannot ask for a subscription by room ID. That is a
// skip, not an error: sending a blank room ID would only measure a rejection.
func TestSoakUserReader_SubscriptionByRoomSkipsWithoutARoom(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"subscriptions":[]}`)}
	recorder := &soakRoomReadRecorder{}
	reader, err := newSoakUserReader(
		soakUserReadConfig{SiteID: "site-a"},
		&soakTopology{ActiveUsers: []model.User{{ID: "u1", Account: "user-a"}}},
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		recorder, rand.New(rand.NewSource(5)), nil,
	)
	require.NoError(t, err)

	require.NoError(t, reader.SubscriptionByRoom(context.Background()))

	assert.Empty(t, transport.subjects)
	require.Len(t, recorder.samples, 1)
	assert.True(t, recorder.samples[0].Skipped)
}

// pickAccountPair must terminate with a single account rather than spinning
// forever looking for a distinct target.
func TestSoakUserReader_AccountPairCollapsesWithASingleAccount(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"account":"user-a"}`)}
	recorder := &soakRoomReadRecorder{}
	reader, err := newSoakUserReader(
		soakUserReadConfig{SiteID: "site-a"},
		&soakTopology{ActiveUsers: []model.User{{ID: "u1", Account: "user-a"}}},
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		recorder, rand.New(rand.NewSource(6)), nil,
	)
	require.NoError(t, err)

	requester, target := reader.pickAccountPair()

	assert.Equal(t, "user-a", requester)
	assert.Equal(t, "user-a", target)
}

// An action that is allowlisted but never dispatched reads as coverage the run
// does not have, so the dispatch table and the allowlist must agree exactly.
func TestSoakUserReads_MatchTheAllowlistedActions(t *testing.T) {
	dispatched := make([]soakRPCAction, 0, len(soakUserReads()))
	for _, read := range soakUserReads() {
		dispatched = append(dispatched, read.Action)
		assert.NotNil(t, read.Call, "action=%s", read.Action)
		assert.True(t, validSoakRPCAction(read.Action), "action=%s", read.Action)
	}
	assert.ElementsMatch(t, soakUserReadActions, dispatched)
}

func TestSoakUserReader_ReadMixedEventuallyDispatchesEveryAction(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{}`)}
	reader, recorder := newSoakUserReadFixture(t, transport, 8)

	for range 800 {
		require.NoError(t, reader.ReadMixed(context.Background()))
	}

	seen := map[soakRPCAction]bool{}
	for i := range recorder.samples {
		seen[recorder.samples[i].Action] = true
	}
	for _, action := range soakUserReadActions {
		assert.True(t, seen[action], "action=%s", action)
	}
}

// The action label is a Prometheus dimension, so every user read must be
// bounded and none may collide with an existing lane's label.
func TestSoakUserReadActions_AreBoundedAndDistinct(t *testing.T) {
	seen := map[soakRPCAction]bool{}
	for _, action := range soakUserReadActions {
		assert.False(t, seen[action], "duplicate action=%s", action)
		seen[action] = true
	}
	for _, existing := range []soakRPCAction{
		soakRPCSend, soakRPCLoadHistory, soakRPCSubscriptionList,
		soakRPCRoomStateRead, soakRPCReadReceiptList, soakRPCPresenceQuery,
	} {
		assert.False(t, seen[existing], "action=%s collides with an existing lane", existing)
	}
}
