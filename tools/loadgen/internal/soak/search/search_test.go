package search

import (
	"context"
	"math/rand" // #nosec G404 -- deterministic load-generator test input // nosemgrep: math-random-used
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/rpc"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

func newSearchFixture(
	t *testing.T,
	transport rpc.Transport,
	seed int64,
	now func() time.Time,
) (*Reader, *testRecorder) {
	t.Helper()
	recorder := &testRecorder{}
	reader, err := New(
		Config{
			SiteID: "site-a", PageSize: 5, RequestTimeout: time.Second,
			Settle: time.Minute,
		},
		&topology.Topology{ActiveUsers: []model.User{{ID: "u1", Account: "user-a"}}},
		rpc.NewClient(
			transport,
			rpc.RetryConfig{MaxAttempts: 1},
			&testSleeper{},
			nil,
		),
		recorder,
		rand.New(rand.NewSource(seed)),
		now,
	)
	require.NoError(t, err)
	return reader, recorder
}

func TestNewSoakSearchReader_RequiresAnAccount(t *testing.T) {
	_, err := New(Config{SiteID: "site-a"}, nil, nil, nil, nil, nil)
	require.Error(t, err)

	_, err = New(
		Config{SiteID: "site-a"},
		&topology.Topology{},
		nil,
		nil,
		nil,
		nil,
	)
	require.Error(t, err)
}

func TestSoakSearchReader_ReadsTargetTheSearchSubjects(t *testing.T) {
	tests := []struct {
		name   string
		call   func(*Reader, context.Context) error
		action rpc.Action
		target string
		reply  string
		count  int
	}{
		{
			name: "messages", call: (*Reader).SearchMessages,
			action: rpc.ActionSearchMessages, target: subject.SearchMessages("user-a", "site-a"),
			reply: `{"messages":[{"messageId":"m1"},{"messageId":"m2"}],"total":2}`, count: 2,
		},
		{
			name: "rooms", call: (*Reader).SearchRooms,
			action: rpc.ActionSearchRooms, target: subject.SearchRooms("user-a", "site-a"),
			reply: `{"rooms":[{"id":"room-1"}]}`, count: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &testTransport{reply: []byte(tt.reply)}
			reader, recorder := newSearchFixture(t, transport, 1, nil)

			require.NoError(t, tt.call(reader, context.Background()))

			assert.Equal(t, []string{tt.target}, transport.calls())
			samples := recorder.snapshot()
			require.Len(t, samples, 1)
			assert.Equal(t, tt.action, samples[0].Action)
			assert.Equal(t, tt.count, samples[0].Messages)
		})
	}
}

func TestSoakSearchReader_ReadMixedDispatchesBothSearches(t *testing.T) {
	transport := &testTransport{reply: []byte(`{"messages":[],"rooms":[]}`)}
	reader, recorder := newSearchFixture(t, transport, 9, nil)

	for range 200 {
		require.NoError(t, reader.ReadMixed(context.Background()))
	}

	seen := map[rpc.Action]bool{}
	for _, sample := range recorder.snapshot() {
		seen[sample.Action] = true
	}
	assert.True(t, seen[rpc.ActionSearchMessages])
	assert.True(t, seen[rpc.ActionSearchRooms])
}

func TestSoakSearchReader_IndexProbeWaitsOutTheSettleWindow(t *testing.T) {
	publishedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := publishedAt.Add(30 * time.Second)
	transport := &testTransport{reply: []byte(`{"messages":[]}`)}
	reader, recorder := newSearchFixture(t, transport, 2, func() time.Time { return now })

	result, queried, err := reader.IndexedAt(
		context.Background(), "user-a", "room-1", "m1", "hello soak", publishedAt,
	)

	require.NoError(t, err)
	assert.Equal(t, IndexTooEarly, result)
	assert.False(t, queried)
	assert.Empty(t, transport.calls())
	assert.Empty(t, recorder.snapshot())
}

func TestSoakSearchReader_IndexProbeClassifiesTheAnswer(t *testing.T) {
	publishedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := publishedAt.Add(2 * time.Minute)
	tests := []struct {
		name  string
		reply string
		want  IndexResult
	}{
		{name: "hit", reply: `{"messages":[{"messageId":"other"},{"messageId":"m1"}]}`, want: IndexFound},
		{name: "answered without the message", reply: `{"messages":[{"messageId":"other"}]}`, want: IndexMissing},
		{name: "empty answer", reply: `{"messages":[]}`, want: IndexMissing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &testTransport{reply: []byte(tt.reply)}
			reader, _ := newSearchFixture(t, transport, 3, func() time.Time { return now })

			result, queried, err := reader.IndexedAt(
				context.Background(), "user-a", "room-1", "m1", "hello soak", publishedAt,
			)

			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
			assert.True(t, queried)
			assert.Len(t, transport.calls(), 1)
		})
	}
}

func TestSoakSearchReader_IndexProbeIsUnknownWhenSearchIsDown(t *testing.T) {
	publishedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := publishedAt.Add(2 * time.Minute)
	transport := &testTransport{err: assert.AnError}
	reader, recorder := newSearchFixture(t, transport, 4, func() time.Time { return now })

	result, queried, err := reader.IndexedAt(
		context.Background(), "user-a", "room-1", "m1", "hello soak", publishedAt,
	)

	require.Error(t, err)
	assert.Equal(t, IndexUnknown, result)
	assert.True(t, queried)
	samples := recorder.snapshot()
	require.Len(t, samples, 1)
	assert.NotEmpty(t, samples[0].ErrorClass)
}

func TestSoakSearchReader_IndexProbeRejectsIncompleteTargets(t *testing.T) {
	reader, _ := newSearchFixture(t, &testTransport{}, 5, nil)

	_, _, err := reader.IndexedAt(context.Background(), "", "room-1", "m1", "x", time.Time{})
	require.Error(t, err)
	_, _, err = reader.IndexedAt(context.Background(), "user-a", "", "m1", "x", time.Time{})
	require.Error(t, err)
	_, _, err = reader.IndexedAt(context.Background(), "user-a", "room-1", "", "x", time.Time{})
	require.Error(t, err)
}

func TestSoakSearchReader_RequiresRPCClient(t *testing.T) {
	reader, err := New(
		Config{SiteID: "site-a"},
		&topology.Topology{ActiveUsers: []model.User{{Account: "user-a"}}},
		nil,
		nil,
		rand.New(rand.NewSource(1)),
		nil,
	)
	require.NoError(t, err)

	err = reader.SearchMessages(context.Background())
	require.Error(t, err)
}

func TestSoakSearchReader_SettleBoundaryUsesUTC(t *testing.T) {
	reader, _ := newSearchFixture(t, &testTransport{}, 1, nil)
	publishedAt := time.Date(2026, 8, 16, 20, 0, 0, 0, time.FixedZone("local", 8*60*60))

	assert.Equal(
		t,
		publishedAt.UTC().Add(time.Minute),
		reader.SettleBoundary(publishedAt),
	)
}
