package main

import (
	"math/rand" // #nosec G404 -- deterministic load-generator test input // nosemgrep: math-random-used
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestSoakReactionRateIsIndependentConfiguration(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.SendRate = 1
	cfg.ReactionRate = 87
	cfg.ReactionsPerHotMessage = 2
	require.NoError(t, validateSoakConfig(&cfg, "chat"))
	assert.Equal(t, float64(87), cfg.ReactionRate)

	cfg.SendRate = 1000
	require.NoError(t, validateSoakConfig(&cfg, "chat"))
	assert.Equal(t, float64(87), cfg.ReactionRate)
}

type soakMutationRecorder struct {
	mu      sync.Mutex
	samples []soakMutationSample
}

func (r *soakMutationRecorder) Record(sample soakMutationSample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, sample)
}

func (r *soakMutationRecorder) snapshot() []soakMutationSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]soakMutationSample(nil), r.samples...)
}

func newTestSoakMutator(
	catalog *soakCatalog,
	transport soakRPCTransport,
	recorder soakMutationSampleRecorder,
	topology *soakTopology,
	clock *fakeSoakClock,
) *soakMutator {
	return newSoakMutator(&soakMutationConfig{
		SiteID:                 "site-1",
		MutationRetries:        2,
		RetryMinBackoff:        time.Millisecond,
		RetryMaxBackoff:        time.Millisecond,
		MaxPinnedPerRoom:       10,
		ReactionsPerHotMessage: 30,
		ReactionRemoveShare:    0.20,
		ReactionMessageScope:   "hot_only",
		RequestTimeout:         time.Second,
	}, topology, catalog, newSoakRPCClient(
		transport,
		soakRetryConfig{
			MaxAttempts: 1,
			MinBackoff:  time.Millisecond,
			MaxBackoff:  time.Millisecond,
		},
		&soakRecordingSleeper{},
		nil,
	), recorder, rand.New(rand.NewSource(1)), clock, &soakRecordingSleeper{})
}

func mutationTopology() *soakTopology {
	return &soakTopology{Subscriptions: []model.Subscription{
		{
			RoomID: "room-1", IsSubscribed: true,
			User: model.SubscriptionUser{ID: "u-alice", Account: "alice"},
		},
		{
			RoomID: "room-1", IsSubscribed: true,
			User: model.SubscriptionUser{ID: "u-bob", Account: "bob"},
		},
	}}
}

func acceptedMutationMessage(
	t *testing.T,
	clock *fakeSoakClock,
	messageID string,
	author string,
) *soakCatalog {
	t.Helper()
	catalog := newSoakCatalog(16, 100, 0, clock)
	acceptMutationCatalogMessage(t, catalog, clock, messageID, author)
	return catalog
}

func acceptMutationCatalogMessage(
	t *testing.T,
	catalog *soakCatalog,
	clock *fakeSoakClock,
	messageID string,
	author string,
) {
	t.Helper()
	require.NoError(t, catalog.TrackPublished(&soakCatalogCandidate{
		ID: messageID, RoomID: "room-1", Author: author, Content: "original",
		CreatedAt: clock.Now(), ThreadReplyLimit: 10,
	}))
	require.True(t, catalog.Accept("room-1", messageID))
	clock.Advance(time.Millisecond)
}
