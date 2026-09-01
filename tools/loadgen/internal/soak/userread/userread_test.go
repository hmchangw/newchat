package userread

import (
	"context"
	"encoding/json"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	soakrpc "github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	soaktopology "github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

type recordingTransport struct {
	reply    []byte
	err      error
	subjects []string
	bodies   [][]byte
}

func (t *recordingTransport) Request(
	_ context.Context,
	subjectName string,
	body []byte,
	_ time.Duration,
) ([]byte, error) {
	t.subjects = append(t.subjects, subjectName)
	t.bodies = append(t.bodies, append([]byte(nil), body...))
	return t.reply, t.err
}

type recordingRecorder struct {
	samples []Sample
}

func (r *recordingRecorder) Record(sample *Sample) {
	r.samples = append(r.samples, *sample)
}

func testTopology() *soaktopology.Topology {
	return &soaktopology.Topology{
		ActiveUsers: []model.User{
			{ID: "u1", Account: "user-a"},
			{ID: "u2", Account: "user-b"},
		},
		Rooms: []model.Room{
			{ID: "room-1", Type: model.RoomTypeChannel},
			{ID: "dm-1", Type: model.RoomTypeDM},
		},
		Subscriptions: []model.Subscription{
			{RoomID: "dm-1", RoomType: model.RoomTypeDM,
				User: model.SubscriptionUser{ID: "u1", Account: "user-a"}},
			{RoomID: "dm-1", RoomType: model.RoomTypeDM,
				User: model.SubscriptionUser{ID: "u2", Account: "user-b"}},
			{RoomID: "room-1", RoomType: model.RoomTypeChannel,
				User: model.SubscriptionUser{ID: "u1", Account: "user-a"}},
			{RoomID: "room-1", RoomType: model.RoomTypeChannel,
				User: model.SubscriptionUser{ID: "u2", Account: "user-b"}},
		},
	}
}

func newFixture(
	t *testing.T,
	transport *recordingTransport,
	seed int64,
) (*Reader, *recordingRecorder) {
	t.Helper()
	recorder := &recordingRecorder{}
	reader, err := New(
		Config{SiteID: "site-a", PageLimit: 5, RequestTimeout: time.Second},
		testTopology(),
		soakrpc.NewClient(transport, soakrpc.RetryConfig{MaxAttempts: 1}, nil, nil),
		recorder,
		rand.New(rand.NewSource(seed)),
		nil,
	)
	require.NoError(t, err)
	return reader, recorder
}

func TestSoakNew_RequiresTopologyWithAnActiveAccount(t *testing.T) {
	_, err := New(Config{SiteID: "site-a"}, nil, nil, nil, nil, nil)
	require.Error(t, err)

	_, err = New(
		Config{SiteID: "site-a"}, &soaktopology.Topology{}, nil, nil, nil, nil,
	)
	require.Error(t, err)
}

func TestSoakReader_EachReadTargetsItsOwnSubject(t *testing.T) {
	tests := []struct {
		name    string
		call    func(*Reader, context.Context) error
		action  soakrpc.Action
		subject func(string) string
		reply   string
		rows    int
		counted bool
	}{
		{name: "me", call: (*Reader).Me, action: soakrpc.ActionUserMe,
			subject: func(a string) string { return subject.UserMe(a, "site-a") },
			reply:   `{"account":"user-a"}`},
		{name: "profile", call: (*Reader).ProfileByName, action: soakrpc.ActionUserProfileGet,
			subject: func(a string) string { return subject.UserProfileGetByName(a, "site-a") },
			reply:   `{"account":"user-b"}`},
		{name: "status", call: (*Reader).StatusByName, action: soakrpc.ActionUserStatusGet,
			subject: func(a string) string { return subject.UserStatusGetByName(a, "site-a") },
			reply:   `{"account":"user-b"}`},
		{name: "settings", call: (*Reader).Settings, action: soakrpc.ActionUserSettingsGet,
			subject: func(a string) string { return subject.UserSettingsGet(a, "site-a") },
			reply:   `{"permissions":{"canPost":true}}`},
		{name: "chatlist", call: (*Reader).Chatlist, action: soakrpc.ActionUserChatlistGet,
			subject: func(a string) string { return subject.UserChatlistGet(a, "site-a") },
			reply:   `{"sections":[{"id":"s1"}]}`, rows: 1, counted: true},
		{name: "priority contacts", call: (*Reader).PriorityContacts,
			action:  soakrpc.ActionUserPriorityContacts,
			subject: func(a string) string { return subject.UserPriorityContactsGet(a, "site-a") },
			reply:   `{"contacts":[{"account":"user-b"}]}`, rows: 1, counted: true},
		{name: "apps list", call: (*Reader).AppsList, action: soakrpc.ActionUserAppsList,
			subject: func(a string) string { return subject.UserAppsList(a, "site-a") },
			reply:   `{"apps":[{"id":"app-1"}]}`, rows: 1, counted: true},
		{name: "apps categories", call: (*Reader).AppsCategories,
			action:  soakrpc.ActionUserAppsCategories,
			subject: func(a string) string { return subject.UserAppsCategories(a, "site-a") },
			reply:   `{"categories":[{"id":"chat"}]}`, rows: 1, counted: true},
		{name: "subscription count", call: (*Reader).SubscriptionCount,
			action:  soakrpc.ActionUserSubscriptionCount,
			subject: func(a string) string { return subject.UserSubscriptionCount(a, "site-a") },
			reply:   `{"count":7}`, rows: 7},
		{name: "subscription by room", call: (*Reader).SubscriptionByRoom,
			action:  soakrpc.ActionUserSubscriptionByRoom,
			subject: func(a string) string { return subject.UserSubscriptionGetByRoomID(a, "site-a") },
			reply:   `{"subscriptions":[{"roomId":"room-1"}]}`, rows: 1},
		{name: "subscription channels", call: (*Reader).SubscriptionChannels,
			action:  soakrpc.ActionUserSubscriptionChannel,
			subject: func(a string) string { return subject.UserSubscriptionGetChannels(a, "site-a") },
			reply:   `{"subscriptions":[{"roomId":"room-1"}]}`, rows: 1, counted: true},
		{name: "subscription dm", call: (*Reader).SubscriptionDM,
			action:  soakrpc.ActionUserSubscriptionDM,
			subject: func(a string) string { return subject.UserSubscriptionGetDM(a, "site-a") },
			reply:   `{"subscription":{"roomId":"dm-1"}}`},
		{name: "thread list", call: (*Reader).ThreadList, action: soakrpc.ActionUserThreadList,
			subject: func(a string) string { return subject.UserThreadList(a, "site-a") },
			reply:   `{"items":[{"threadRoomId":"t1"}]}`, rows: 1, counted: true},
		{name: "thread unread", call: (*Reader).ThreadUnread,
			action:  soakrpc.ActionUserThreadUnread,
			subject: func(a string) string { return subject.UserThreadUnreadSummary(a, "site-a") },
			reply:   `{"unread":true}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &recordingTransport{reply: []byte(tt.reply)}
			reader, recorder := newFixture(t, transport, 3)

			require.NoError(t, tt.call(reader, context.Background()))

			require.Len(t, transport.subjects, 1)
			assert.Contains(t,
				[]string{tt.subject("user-a"), tt.subject("user-b")},
				transport.subjects[0],
			)
			require.Len(t, recorder.samples, 1)
			assert.Equal(t, tt.action, recorder.samples[0].Action)
			assert.Equal(t, tt.rows, recorder.samples[0].Messages)
			assert.Equal(t, tt.counted, recorder.samples[0].RowsCounted)
			assert.False(t, recorder.samples[0].Skipped)
		})
	}
}

func TestSoakReader_SkipsReadsWithoutAnEligibleTarget(t *testing.T) {
	transport := &recordingTransport{reply: []byte(`{}`)}
	recorder := &recordingRecorder{}
	reader, err := New(
		Config{SiteID: "site-a"},
		&soaktopology.Topology{ActiveUsers: []model.User{{Account: "user-a"}}},
		soakrpc.NewClient(transport, soakrpc.RetryConfig{MaxAttempts: 1}, nil, nil),
		recorder, rand.New(rand.NewSource(5)), nil,
	)
	require.NoError(t, err)

	require.NoError(t, reader.SubscriptionByRoom(context.Background()))
	require.NoError(t, reader.SubscriptionChannels(context.Background()))
	require.NoError(t, reader.SubscriptionDM(context.Background()))

	assert.Empty(t, transport.subjects)
	require.Len(t, recorder.samples, 3)
	for i := range recorder.samples {
		assert.True(t, recorder.samples[i].Skipped)
	}
}

func TestSoakReader_AccountPairCollapsesWithASingleAccount(t *testing.T) {
	reader, err := New(
		Config{SiteID: "site-a"},
		&soaktopology.Topology{ActiveUsers: []model.User{{Account: "user-a"}}},
		nil, nil, rand.New(rand.NewSource(6)), nil,
	)
	require.NoError(t, err)

	requester, target := reader.pickAccountPair()

	assert.Equal(t, "user-a", requester)
	assert.Equal(t, "user-a", target)
}

func TestSoakReader_UsesOnlyRealRoomPairsFromTheActiveSide(t *testing.T) {
	topology := &soaktopology.Topology{
		ActiveUsers: []model.User{{ID: "active", Account: "active"}},
		Rooms: []model.Room{
			{ID: "dm", Type: model.RoomTypeDM},
			{ID: "channel", Type: model.RoomTypeChannel},
		},
		Subscriptions: []model.Subscription{
			{RoomID: "dm", RoomType: model.RoomTypeDM,
				User: model.SubscriptionUser{ID: "active", Account: "active"}},
			{RoomID: "dm", RoomType: model.RoomTypeDM,
				User: model.SubscriptionUser{ID: "borrowed", Account: "borrowed"}},
			{RoomID: "channel", RoomType: model.RoomTypeChannel,
				User: model.SubscriptionUser{ID: "borrowed-a", Account: "borrowed-a"}},
			{RoomID: "channel", RoomType: model.RoomTypeChannel,
				User: model.SubscriptionUser{ID: "borrowed-b", Account: "borrowed-b"}},
			{RoomID: "channel", RoomType: model.RoomTypeChannel,
				User: model.SubscriptionUser{ID: "active", Account: "active"}},
		},
	}
	transport := &recordingTransport{reply: []byte(`{}`)}
	reader, err := New(
		Config{SiteID: "site-a"}, topology,
		soakrpc.NewClient(transport, soakrpc.RetryConfig{MaxAttempts: 1}, nil, nil),
		nil, rand.New(rand.NewSource(7)), nil,
	)
	require.NoError(t, err)

	require.NoError(t, reader.SubscriptionDM(context.Background()))
	require.NoError(t, reader.SubscriptionChannels(context.Background()))

	require.Len(t, transport.subjects, 2)
	assert.Equal(t, subject.UserSubscriptionGetDM("active", "site-a"), transport.subjects[0])
	assert.Equal(t, subject.UserSubscriptionGetChannels("active", "site-a"), transport.subjects[1])
	var dmBody, channelBody map[string]any
	require.NoError(t, json.Unmarshal(transport.bodies[0], &dmBody))
	require.NoError(t, json.Unmarshal(transport.bodies[1], &channelBody))
	assert.Equal(t, "borrowed", dmBody["accountName"])
	assert.Contains(t, []any{"borrowed-a", "borrowed-b"}, channelBody["membersContain"])
}

func TestSoakReader_RecordsRPCFailuresAndPreservesTheCause(t *testing.T) {
	transport := &recordingTransport{err: context.DeadlineExceeded}
	reader, recorder := newFixture(t, transport, 9)

	err := reader.Me(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, recorder.samples, 1)
	assert.Equal(t, soakrpc.ErrorTimeout, recorder.samples[0].ErrorClass)
}

func TestSoakReader_RequiresAnRPCClientAtCallTime(t *testing.T) {
	reader, err := New(Config{SiteID: "site-a"}, testTopology(), nil, nil, nil, nil)
	require.NoError(t, err)

	err = reader.Me(context.Background())

	require.Error(t, err)
}

func TestSoakReads_MatchTheRPCAllowlist(t *testing.T) {
	dispatched := make([]soakrpc.Action, 0, len(soakUserReads()))
	for _, read := range soakUserReads() {
		dispatched = append(dispatched, read.Action)
		assert.NotNil(t, read.Call)
		assert.True(t, soakrpc.ValidAction(read.Action), "action=%s", read.Action)
	}
	assert.ElementsMatch(t, soakrpc.UserReadActions(), dispatched)
}

func TestSoakReader_ReadMixedEventuallyDispatchesEveryAction(t *testing.T) {
	transport := &recordingTransport{reply: []byte(`{}`)}
	reader, recorder := newFixture(t, transport, 8)

	for range 800 {
		require.NoError(t, reader.ReadMixed(context.Background()))
	}

	seen := make(map[soakrpc.Action]bool)
	for i := range recorder.samples {
		seen[recorder.samples[i].Action] = true
	}
	for _, action := range soakrpc.UserReadActions() {
		assert.True(t, seen[action], "action=%s", action)
	}
}
