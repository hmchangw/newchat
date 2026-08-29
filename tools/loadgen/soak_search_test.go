package main

import (
	"context"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/distribution"
)

func newSoakSearchFixture(
	t *testing.T,
	transport soakRPCTransport,
	seed int64,
	now func() time.Time,
) (*soakSearchReader, *soakRoomReadRecorder) {
	t.Helper()
	recorder := &soakRoomReadRecorder{}
	reader, err := newSoakSearchReader(
		soakSearchConfig{
			SiteID: "site-a", PageSize: 5, RequestTimeout: time.Second,
			Settle: time.Minute,
		},
		&soakTopology{ActiveUsers: []model.User{{ID: "u1", Account: "user-a"}}},
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		recorder, rand.New(rand.NewSource(seed)), now,
	)
	require.NoError(t, err)
	return reader, recorder
}

func TestNewSoakSearchReader_RequiresAnAccount(t *testing.T) {
	_, err := newSoakSearchReader(soakSearchConfig{SiteID: "site-a"}, nil, nil, nil, nil, nil)
	require.Error(t, err)

	_, err = newSoakSearchReader(
		soakSearchConfig{SiteID: "site-a"}, &soakTopology{}, nil, nil, nil, nil,
	)
	require.Error(t, err)
}

func TestSoakSearchReader_ReadsTargetTheSearchSubjects(t *testing.T) {
	tests := []struct {
		name    string
		call    func(*soakSearchReader, context.Context) error
		action  soakRPCAction
		subject string
		reply   string
		count   int
	}{
		{
			name: "messages", call: (*soakSearchReader).SearchMessages,
			action: soakRPCSearchMessages, subject: subject.SearchMessages("user-a", "site-a"),
			reply: `{"messages":[{"messageId":"m1"},{"messageId":"m2"}],"total":2}`, count: 2,
		},
		{
			name: "rooms", call: (*soakSearchReader).SearchRooms,
			action: soakRPCSearchRooms, subject: subject.SearchRooms("user-a", "site-a"),
			reply: `{"rooms":[{"id":"room-1"}]}`, count: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &soakRoomOpsTransport{reply: []byte(tt.reply)}
			reader, recorder := newSoakSearchFixture(t, transport, 1, nil)

			require.NoError(t, tt.call(reader, context.Background()))

			require.Len(t, transport.subjects, 1)
			assert.Equal(t, tt.subject, transport.subjects[0])
			require.Len(t, recorder.samples, 1)
			assert.Equal(t, tt.action, recorder.samples[0].Action)
			assert.Equal(t, tt.count, recorder.samples[0].Messages)
		})
	}
}

func TestSoakSearchReader_ReadMixedDispatchesBothSearches(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"messages":[],"rooms":[]}`)}
	reader, recorder := newSoakSearchFixture(t, transport, 9, nil)

	for range 200 {
		require.NoError(t, reader.ReadMixed(context.Background()))
	}

	seen := map[soakRPCAction]bool{}
	for i := range recorder.samples {
		seen[recorder.samples[i].Action] = true
	}
	assert.True(t, seen[soakRPCSearchMessages])
	assert.True(t, seen[soakRPCSearchRooms])
}

// Inside the settle window an absent hit is legal, so the probe must not spend
// a read slot to learn nothing — and must never call it missing.
func TestSoakSearchReader_IndexProbeWaitsOutTheSettleWindow(t *testing.T) {
	publishedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := publishedAt.Add(30 * time.Second)
	transport := &soakRoomOpsTransport{reply: []byte(`{"messages":[]}`)}
	reader, recorder := newSoakSearchFixture(t, transport, 2, func() time.Time { return now })

	result, _, err := reader.IndexedAt(
		context.Background(), "user-a", "room-1", "m1", "hello soak", publishedAt,
	)

	require.NoError(t, err)
	assert.Equal(t, soakSearchIndexTooEarly, result)
	assert.Empty(t, transport.subjects, "no query is issued before the settle window elapses")
	assert.Empty(t, recorder.samples)
}

func TestSoakSearchReader_IndexProbeClassifiesTheAnswer(t *testing.T) {
	publishedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := publishedAt.Add(2 * time.Minute)

	tests := []struct {
		name  string
		reply string
		want  soakSearchIndexResult
	}{
		{
			name:  "hit",
			reply: `{"messages":[{"messageId":"other"},{"messageId":"m1"}]}`,
			want:  soakSearchIndexFound,
		},
		{
			name:  "answered without the message",
			reply: `{"messages":[{"messageId":"other"}]}`,
			want:  soakSearchIndexMissing,
		},
		{
			name:  "empty answer",
			reply: `{"messages":[]}`,
			want:  soakSearchIndexMissing,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := &soakRoomOpsTransport{reply: []byte(tt.reply)}
			reader, _ := newSoakSearchFixture(
				t, transport, 3, func() time.Time { return now },
			)

			result, _, err := reader.IndexedAt(
				context.Background(), "user-a", "room-1", "m1", "hello soak", publishedAt,
			)

			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
			require.Len(t, transport.subjects, 1)
		})
	}
}

// An unreachable search-service proves nothing about the message. Reporting
// missing here would turn every dependency outage into a data-loss claim.
func TestSoakSearchReader_IndexProbeIsUnknownWhenSearchIsDown(t *testing.T) {
	publishedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := publishedAt.Add(2 * time.Minute)
	transport := &soakRoomOpsTransport{err: assert.AnError}
	reader, _ := newSoakSearchFixture(t, transport, 4, func() time.Time { return now })

	result, _, err := reader.IndexedAt(
		context.Background(), "user-a", "room-1", "m1", "hello soak", publishedAt,
	)

	require.Error(t, err)
	assert.Equal(t, soakSearchIndexUnknown, result)
}

func TestSoakSearchReader_IndexProbeRejectsIncompleteTargets(t *testing.T) {
	reader, _ := newSoakSearchFixture(t, &soakRoomOpsTransport{}, 5, nil)

	_, _, err := reader.IndexedAt(context.Background(), "", "room-1", "m1", "x", time.Time{})
	require.Error(t, err)
	_, _, err = reader.IndexedAt(context.Background(), "user-a", "", "m1", "x", time.Time{})
	require.Error(t, err)
	_, _, err = reader.IndexedAt(context.Background(), "user-a", "room-1", "", "x", time.Time{})
	require.Error(t, err)
}

// This documents what the probe actually gets today, not a payload shape the
// soak never produces. Bodies are a single run of one character with no
// whitespace, so the "term" is the whole body — which is why the observer is
// refused at startup until payloads carry a per-message marker.
func TestSearchProbeTerm_ReflectsTheRealPayloadShape(t *testing.T) {
	body := distribution.ContentOfSize(64)

	assert.Equal(t, body, searchProbeTerm(body),
		"a body with no whitespace yields itself, not a distinguishing term")
	assert.Equal(t, "soak", searchProbeTerm(""))
}

// A recovered operation has no catalog entry, so the probe reports unknown
// rather than inventing a verdict from evidence it does not have.
func TestSoakSearchIndexProbe_UnknownWithoutACatalogEntry(t *testing.T) {
	transport := &soakRoomOpsTransport{reply: []byte(`{"messages":[]}`)}
	reader, _ := newSoakSearchFixture(t, transport, 6, nil)
	probe := newSoakSearchIndexProbe(reader, newSoakCatalog(8, 8, 0, nil))

	result, _, err := probe.Indexed(context.Background(), &failureOperation{
		ID:         "m1",
		Targets:    map[string]string{"roomId": "room-1", "messageId": "m1"},
		Attributes: map[string]string{soakFailureAttributeAccount: "user-a"},
	})

	require.NoError(t, err)
	assert.Equal(t, soakSearchIndexUnknown, result)
	assert.Empty(t, transport.subjects)
}

func TestSoakSearchIndexProbe_UnknownWithoutCompleteTargets(t *testing.T) {
	reader, _ := newSoakSearchFixture(t, &soakRoomOpsTransport{}, 7, nil)
	probe := newSoakSearchIndexProbe(reader, newSoakCatalog(8, 8, 0, nil))

	for _, operation := range []*failureOperation{
		nil,
		{ID: "m1", Targets: map[string]string{"messageId": "m1"}},
		{ID: "m1", Targets: map[string]string{"roomId": "room-1"}},
		{
			ID:      "m1",
			Targets: map[string]string{"roomId": "room-1", "messageId": "m1"},
		},
	} {
		result, _, err := probe.Indexed(context.Background(), operation)
		require.NoError(t, err)
		assert.Equal(t, soakSearchIndexUnknown, result)
	}
}

// The search observer is additive: with it disabled the contract must be
// byte-identical to one built before it existed, so an existing ledger epoch
// stays valid and no re-seed is forced.
func TestFailureObserverContract_SearchObserverIsAdditive(t *testing.T) {
	disabled := newFailureObserverContract(false, false)

	assert.NotContains(t, disabled.Observers, failureObserverSearchIndex)
	assert.NotContains(t, disabled.Lanes[soakFailureLaneMessageSend], failureObserverSearchIndex)

	enabled := newFailureObserverContract(false, true)
	assert.Contains(t, enabled.Observers, failureObserverSearchIndex)
	assert.Contains(t, enabled.Lanes[soakFailureLaneMessageSend], failureObserverSearchIndex)
}

func TestMessageCreateExpectedEffects_AddsTheIndexEffectOnlyWhenEnabled(t *testing.T) {
	without := messageCreateExpectedEffectsForObservers(false, false, 0, "")
	for _, effect := range without {
		assert.NotEqual(t, failureEffectMessageIndexed, effect.Effect)
	}

	with := messageCreateExpectedEffectsForObservers(false, true, 0, "")
	found := false
	for _, effect := range with {
		if effect.Effect == failureEffectMessageIndexed {
			found = true
			assert.Equal(t, failureObserverSearchIndex, effect.Observer)
			assert.True(t, effect.Required)
		}
	}
	assert.True(t, found)
}

// Inside the settle window the probe must say so explicitly rather than
// reporting a generic unknown. The reconciler reschedules a too-early
// operation to the settle boundary; treating it as unknown would re-ask every
// retry interval and burn the whole reconciliation budget on queries that
// cannot succeed yet.
func TestSoakSearchReader_IndexProbeReportsTooEarlyDistinctly(t *testing.T) {
	publishedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := publishedAt.Add(30 * time.Second)
	transport := &soakRoomOpsTransport{reply: []byte(`{"messages":[]}`)}
	reader, _ := newSoakSearchFixture(t, transport, 8, func() time.Time { return now })

	result, _, err := reader.IndexedAt(
		context.Background(), "user-a", "room-1", "m1", "hello soak", publishedAt,
	)

	require.NoError(t, err)
	assert.Equal(t, soakSearchIndexTooEarly, result)
	assert.Empty(t, transport.subjects)
}

// The probe reports whether it actually queried, because the reconciler refunds
// the read allowance for a claim that reached no service. Deriving that from
// configuration would charge the read lane for the several paths that answer
// from local state alone.
func TestSoakSearchIndexProbe_ReportsWhetherItQueried(t *testing.T) {
	t.Run("no catalogue entry issues no query", func(t *testing.T) {
		transport := &soakRoomOpsTransport{reply: []byte(`{"messages":[]}`)}
		reader, _ := newSoakSearchFixture(t, transport, 6, nil)
		probe := newSoakSearchIndexProbe(reader, newSoakCatalog(8, 8, 0, nil))

		result, queried, err := probe.Indexed(context.Background(), &failureOperation{
			ID:         "m1",
			Targets:    map[string]string{"roomId": "room-1", "messageId": "m1"},
			Attributes: map[string]string{soakFailureAttributeAccount: "user-a"},
		})

		require.NoError(t, err)
		assert.Equal(t, soakSearchIndexUnknown, result)
		assert.False(t, queried)
		assert.Empty(t, transport.subjects)
	})

	t.Run("an unconfigured probe issues no query", func(t *testing.T) {
		var probe *soakSearchIndexProbe

		result, queried, err := probe.Indexed(context.Background(), &failureOperation{ID: "m1"})

		require.NoError(t, err)
		assert.Equal(t, soakSearchIndexUnknown, result)
		assert.False(t, queried)
	})
}
