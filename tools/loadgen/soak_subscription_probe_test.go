package main

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSoakSubscriptionProbeFixture builds a room reader around a caller-supplied
// config, which the shared fixture does not allow — these tests exist to assert
// on what the configuration puts on the wire.
func newSoakSubscriptionProbeFixture(
	t *testing.T,
	cfg soakRoomReadConfig,
) (*soakRoomReader, *soakRoomOpsTransport) {
	t.Helper()
	pool, _ := newSoakRoomStateTestPool(t, 3, 8)
	transport := &soakRoomOpsTransport{
		reply: []byte(`{"subscriptions":[{"roomId":"room-1","muted":false}],"hasMore":false}`),
	}
	cfg.SiteID = "site-a"
	cfg.RequestTimeout = time.Second
	reader := newSoakRoomReader(
		cfg,
		pool,
		newSoakRPCClient(transport, soakRetryConfig{MaxAttempts: 1}, &soakRecordingSleeper{}, nil),
		&soakRoomReadRecorder{},
		rand.New(rand.NewSource(11)),
		nil,
	)
	return reader, transport
}

// An unconfigured probe must not change the request the lane has been sending,
// or every percentile recorded before this change becomes incomparable with
// every one recorded after it.
func TestSoakRoomReader_SubscriptionListOmitsThePagingControlsByDefault(t *testing.T) {
	reader, transport := newSoakSubscriptionProbeFixture(t, soakRoomReadConfig{})

	require.NoError(t, reader.SubscriptionList(context.Background()))

	require.Len(t, transport.bodies, 1)
	assert.JSONEq(t, `{"type":"rooms"}`, string(transport.bodies[0]),
		"an unset probe must leave the service's own defaults in force")
}

// The lane sends no limit today, so it measures whatever page size the service
// happens to default to rather than a workload we chose.
func TestSoakRoomReader_SubscriptionListSendsTheConfiguredPageLimit(t *testing.T) {
	reader, transport := newSoakSubscriptionProbeFixture(t,
		soakRoomReadConfig{SubscriptionListLimit: 5})

	require.NoError(t, reader.SubscriptionList(context.Background()))

	require.Len(t, transport.bodies, 1)
	assert.JSONEq(t, `{"type":"rooms","limit":5}`, string(transport.bodies[0]))
}

// Last-message enrichment fans every listed room out to history-service. Being
// able to switch it off is what separates "the page is expensive" from "the
// enrichment is expensive" without re-seeding anything.
func TestSoakRoomReader_SubscriptionListCanDisableLastMessageEnrichment(t *testing.T) {
	disabled := false
	reader, transport := newSoakSubscriptionProbeFixture(t,
		soakRoomReadConfig{SubscriptionListIncludeLastMessage: &disabled})

	require.NoError(t, reader.SubscriptionList(context.Background()))

	require.Len(t, transport.bodies, 1)
	assert.JSONEq(t, `{"type":"rooms","includeLastMessage":false}`, string(transport.bodies[0]))
}

// The service reads a missing includeLastMessage as true, so an explicit true
// has to travel as a present field rather than collapse back into the default.
func TestSoakRoomReader_SubscriptionListSendsAnExplicitEnrichmentRequest(t *testing.T) {
	enabled := true
	reader, transport := newSoakSubscriptionProbeFixture(t,
		soakRoomReadConfig{SubscriptionListLimit: 40, SubscriptionListIncludeLastMessage: &enabled})

	require.NoError(t, reader.SubscriptionList(context.Background()))

	require.Len(t, transport.bodies, 1)
	assert.JSONEq(t, `{"type":"rooms","limit":40,"includeLastMessage":true}`,
		string(transport.bodies[0]))
}

// The knobs are diagnostic: leaving them unset has to reproduce the request the
// lane sent before they existed, so a run started without them stays
// comparable with every run recorded before this change.
func TestSoakConfig_SubscriptionListProbeIsUnsetByDefault(t *testing.T) {
	cfg := mustDefaultSoakConfig(t)

	assert.Zero(t, cfg.SubscriptionListLimit,
		"an unset limit must let user-service apply its own default")
	assert.Nil(t, cfg.SubscriptionListIncludeLastMessage,
		"an unset enrichment flag must not travel on the wire at all")
}

func TestSoakConfig_SubscriptionListProbeReadsItsEnvironment(t *testing.T) {
	cfg, err := env.ParseAsWithOptions[soakConfig](env.Options{
		Prefix: "SOAK_",
		Environment: map[string]string{
			"SOAK_SUBSCRIPTION_LIST_LIMIT":                "5",
			"SOAK_SUBSCRIPTION_LIST_INCLUDE_LAST_MESSAGE": "false",
		},
	})

	require.NoError(t, err)
	assert.Equal(t, 5, cfg.SubscriptionListLimit)
	require.NotNil(t, cfg.SubscriptionListIncludeLastMessage)
	assert.False(t, *cfg.SubscriptionListIncludeLastMessage)
}

// A negative limit reaches user-service's normalizePage, which silently swaps
// it for the default — the probe would then report on a page size nobody asked
// for. Refusing it at startup is the difference between a wrong measurement and
// no measurement.
func TestValidateSoakConfig_RejectsANegativeSubscriptionListLimit(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.SubscriptionListLimit = -1

	err := validateSoakConfig(&cfg, "soak")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "SOAK_SUBSCRIPTION_LIST_LIMIT")
}

// A knob that parses but never reaches the lane is the failure this whole
// series keeps running into: the run looks configured and measures the old
// thing. The mapping is its own function so it can be asserted directly.
func TestSoakRoomReadConfigFrom_CarriesTheSubscriptionListProbe(t *testing.T) {
	disabled := false
	soak := validSoakConfig(t)
	soak.SubscriptionListLimit = 5
	soak.SubscriptionListIncludeLastMessage = &disabled

	got := soakRoomReadConfigFrom("site-a", &soak)

	assert.Equal(t, "site-a", got.SiteID)
	assert.Equal(t, 5, got.SubscriptionListLimit)
	require.NotNil(t, got.SubscriptionListIncludeLastMessage)
	assert.False(t, *got.SubscriptionListIncludeLastMessage)
}

func TestSoakRoomReadConfigFrom_LeavesAnUnsetProbeUnset(t *testing.T) {
	soak := validSoakConfig(t)

	got := soakRoomReadConfigFrom("site-a", &soak)

	assert.Zero(t, got.SubscriptionListLimit)
	assert.Nil(t, got.SubscriptionListIncludeLastMessage)
	assert.Equal(t, soakRoomInfoBatchSize, got.BatchSize)
	assert.Equal(t, soakRequestTimeout, got.RequestTimeout)
}
