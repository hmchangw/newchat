package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// startCoreNATS starts an embedded core NATS server (no JetStream needed for
// presence) and returns its client URL. Shut down via t.Cleanup.
func startCoreNATS(t *testing.T) string {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
}

// startFakePresence subscribes to the presence write subjects for siteID and
// echoes the service's observable contract back on chat.user.presence.state.*:
// hello->online, bye->offline, activity->away/online per the `Away` flag, ping->no-op.
func startFakePresence(t *testing.T, url, siteID string) {
	t.Helper()
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	publishState := func(account string, status model.PresenceStatus) {
		st := model.PresenceState{Account: account, SiteID: siteID, Status: status, Timestamp: time.Now().UTC().UnixMilli()}
		data, mErr := json.Marshal(st)
		if mErr != nil {
			return
		}
		_ = nc.Publish(subject.PresenceState(account), data)
	}

	wildcard := "chat.user.*.event.presence." + siteID + ".*"
	_, err = nc.Subscribe(wildcard, func(m *nats.Msg) {
		// chat.user.<account>.event.presence.<site>.<verb>
		parts := strings.Split(m.Subject, ".")
		if len(parts) < 7 {
			return
		}
		account := parts[2]
		verb := parts[len(parts)-1]
		switch verb {
		case "hello":
			publishState(account, model.StatusOnline)
		case "bye":
			publishState(account, model.StatusOffline)
		case "activity":
			var a model.Activity
			if json.Unmarshal(m.Data, &a) == nil {
				if a.Away {
					publishState(account, model.StatusAway)
				} else {
					publishState(account, model.StatusOnline)
				}
			}
		case "ping":
			// no-op: a known-connection ping does not change state
		}
	})
	require.NoError(t, err)
	require.NoError(t, nc.Flush())
}

func TestPresenceSustained_EmbeddedEndToEnd(t *testing.T) {
	url := startCoreNATS(t)
	startFakePresence(t, url, "site-local")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := presenceConfig{
		Steps: []int{20}, Warmup: 300 * time.Millisecond, Hold: 2 * time.Second, Cooldown: 0,
		Heartbeat: 100 * time.Millisecond, ActivityFlipRate: 6000, ReconnectRate: 0,
		StopOnTrip: false, PublisherConns: 2, ObserverConns: 1,
		P95Ms: 2000, P99Ms: 5000, ErrorRate: 0.5,
	}
	baseCfg := &config{NatsURL: url, SiteID: "site-local"}
	results, err := runPresenceSustainedForTest(ctx, cfg, &prodPresenceFactory{baseCfg: baseCfg})
	require.NoError(t, err)
	require.Len(t, results, 1)
	r := results[0]
	assert.Equal(t, verdictPass, r.Kind, "reasons: %v", r.Reasons)
	assert.Greater(t, r.Attempted, int64(0), "expected measured transitions")
	assert.Greater(t, r.P95Ms, 0.0, "expected at least one latency sample")
}

func TestPresenceStorm_EmbeddedEndToEnd(t *testing.T) {
	url := startCoreNATS(t)
	startFakePresence(t, url, "site-local")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := stormConfig{
		Users: 20, StormSteps: []float64{1.0}, Mode: "graceful",
		Warmup: 300 * time.Millisecond, Settle: 0, RecoverySLO: 5 * time.Second,
		Heartbeat: 100 * time.Millisecond, SilentWait: 0, ReconnectRate: 0,
		StopOnTrip: false, PublisherConns: 2, ObserverConns: 1, P99Ms: 5000, ErrorRate: 0.9,
	}
	baseCfg := &config{NatsURL: url, SiteID: "site-local"}
	env := (&prodStormFactory{baseCfg: baseCfg}).Build(cfg)
	warmStormPopulation(ctx, env)
	// Wait (no sleep) until the warm-up hellos have all been observed online.
	require.Eventually(t, func() bool {
		return len(env.collector.LatenciesMs()) >= cfg.Users
	}, 5*time.Second, 20*time.Millisecond, "warm-up hellos did not all resolve")

	results := runStormSteps(ctx, cfg, env)
	env.pool.Close()
	require.Len(t, results, 1)
	assert.True(t, results[0].RecoveryComplete, "reasons: %v", results[0].Reasons)
	assert.Equal(t, verdictPass, results[0].Kind, "reasons: %v", results[0].Reasons)
}
