package main

import (
	"context"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestSoakConfig_EncryptionPreflightDefaultsOn(t *testing.T) {
	cfg := mustDefaultSoakConfig(t)

	assert.True(t, cfg.EncryptionPreflight,
		"skipping the encrypted-write check must be opted into, never inherited")
	assert.Equal(t, 30*time.Second, cfg.EncryptionPreflightTimeout)
}

func TestValidateSoakConfig_EncryptionPreflightMayOnlyBeSkippedOutsideStaging(t *testing.T) {
	tests := []struct {
		name        string
		environment string
		wantErr     bool
	}{
		{name: "local", environment: "local"},
		{name: "test", environment: "test"},
		{name: "staging", environment: "staging", wantErr: true},
		{name: "production", environment: "production", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validSoakConfig(t)
			cfg.Environment = tt.environment
			cfg.EncryptionPreflight = false

			err := validateSoakConfig(&cfg, "soak_keyspace")

			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err,
				"a promoted environment must not be able to skip the check")
			assert.Contains(t, err.Error(), "SOAK_ENCRYPTION_PREFLIGHT")
		})
	}
}

func TestValidateSoakConfig_EncryptionPreflightTimeoutMustBePositive(t *testing.T) {
	cfg := validSoakConfig(t)
	cfg.EncryptionPreflightTimeout = 0

	err := validateSoakConfig(&cfg, "soak_keyspace")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "SOAK_ENCRYPTION_PREFLIGHT_TIMEOUT")
}

// A disabled preflight must be consulted before anything is sent. Passing nil
// collaborators is the probe: a flag that is stored but never read would reach
// them, which is exactly how six lane rates were once configured and ignored.
func TestRunSoakEncryptionPreflight_DisabledSendsNothing(t *testing.T) {
	err := runSoakEncryptionPreflight(
		context.Background(),
		soakEncryptionPreflightConfig{Enabled: false, Timeout: time.Second},
		nil, nil, nil, nil,
	)

	require.NoError(t, err)
}

// soakPreflightTestTimeout is short enough that the probe's doomed wait does not
// slow the suite, and long enough that the wiring cost between creating the
// deadline and entering Publish stays well inside the assertions below.
const soakPreflightTestTimeout = 200 * time.Millisecond

// soakPreflightDeadlinePublisher records whether the publish ran under the
// preflight's own budget. The publish is the part most likely to hang during
// the front-door outage this check exists to notice, so a timeout that starts
// only after it returns does not bound what it claims to.
type soakPreflightDeadlinePublisher struct {
	publishedAt time.Time
	deadline    time.Time
	hasDeadline bool
}

func (p *soakPreflightDeadlinePublisher) Publish(
	ctx context.Context,
	_ string,
	_ []byte,
) error {
	// Anchored here rather than after the call, because the preflight goes on to
	// wait out the whole budget: measured from the far side, remaining time is
	// near zero whatever the timeout was.
	p.publishedAt = time.Now()
	p.deadline, p.hasDeadline = ctx.Deadline()
	return nil
}

func TestRunSoakEncryptionPreflight_PublishesUnderTheConfiguredTimeout(t *testing.T) {
	publisher := &soakPreflightDeadlinePublisher{}
	clock := newFakeSoakClock(time.Unix(100, 0).UTC())
	sender := newSoakSender(soakSendConfig{
		SiteID: "site-a", ReplyTimeout: 5 * time.Second,
	}, newSoakCatalog(8, 100, 0, clock), publisher, clock,
		rand.New(rand.NewSource(1)), nil)
	cfg := validSoakConfig(t)
	selector, err := newSoakRuntimeSelector(&soakTopology{
		Rooms: []model.Room{{ID: "room-1"}},
		Subscriptions: []model.Subscription{{
			RoomID: "room-1", IsSubscribed: true,
			User: model.SubscriptionUser{ID: "u-1", Account: "alice"},
		}},
	}, &cfg, 42)
	require.NoError(t, err)

	// The reply never arrives, so the preflight fails on its own deadline; the
	// assertion is about the context the publish already ran under.
	err = runSoakEncryptionPreflight(
		context.Background(),
		soakEncryptionPreflightConfig{Enabled: true, Timeout: soakPreflightTestTimeout},
		&soakStubEncryptionStore{},
		sender,
		selector,
		make(chan soakSendObservation),
	)
	require.Error(t, err)

	require.True(t, publisher.hasDeadline,
		"the probe publish ran under an unbounded context, so a stalled publish "+
			"would outlive SOAK_ENCRYPTION_PREFLIGHT_TIMEOUT")
	budget := publisher.deadline.Sub(publisher.publishedAt)
	assert.LessOrEqual(t, budget, soakPreflightTestTimeout,
		"the publish was given a deadline from some other budget, not the "+
			"preflight's own")
	assert.Greater(t, budget, soakPreflightTestTimeout/2,
		"the publish got a remnant of the budget rather than the budget; the "+
			"deadline must be created before the send, not part-way through it")
}

type soakStubEncryptionStore struct{}

func (soakStubEncryptionStore) HasWrappedDEK(context.Context, string) (bool, error) {
	return false, nil
}
