package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// roomIDFromSendSubject extracts the roomID token from a frontdoor send subject
// of the form "chat.user.{account}.room.{roomID}.{siteID}.msg.send".
func roomIDFromSendSubject(subj string) string {
	parts := strings.Split(subj, ".")
	for i, tok := range parts {
		if tok == "room" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func TestGenerator_ThreadMode_SetsParentFromRoom(t *testing.T) {
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	tf := BuildThreadFixtures(&p, 42, 3, "site-a")
	require.NotEmpty(t, tf.Subscriptions)

	validByRoom := map[string]map[string]bool{}
	for rid, list := range tf.ParentsByRoom {
		validByRoom[rid] = map[string]bool{}
		for _, pm := range list {
			validByRoom[rid][pm.MessageID] = true
		}
	}

	pub := &recordingPublisher{}
	metrics := NewMetrics()
	collector := NewCollector(metrics, p.Name)
	gen := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: tf.Fixtures, SiteID: "site-a",
		Rate: 1, Inject: InjectFrontdoor, Publisher: pub,
		Metrics: metrics, Collector: collector,
		ParentsByRoom:  tf.ParentsByRoom,
		WarmupDeadline: time.Now().Add(-time.Hour), // past deadline: all sends counted as measured
	}, 42)

	for i := 0; i < 50; i++ {
		gen.publishOne(context.Background())
	}

	calls := pub.snapshot()
	require.NotEmpty(t, calls)
	for i, call := range calls {
		roomID := roomIDFromSendSubject(call.subject)
		require.NotEmpty(t, roomID, "call %d: could not parse roomID from subject %q", i, call.subject)

		var req model.SendMessageRequest
		require.NoError(t, json.Unmarshal(call.data, &req))
		assert.NotEmpty(t, req.ThreadParentMessageID, "reply %d must set a thread parent", i)
		assert.True(t, validByRoom[roomID][req.ThreadParentMessageID],
			"call %d: parent %s must belong to room %s", i, req.ThreadParentMessageID, roomID)
	}
}

func TestGenerator_PlainMode_NoParent(t *testing.T) {
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	f := BuildFixtures(&p, 42, "site-a")

	pub := &recordingPublisher{}
	metrics := NewMetrics()
	collector := NewCollector(metrics, p.Name)
	gen := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: f, SiteID: "site-a",
		Rate: 1, Inject: InjectFrontdoor, Publisher: pub,
		Metrics: metrics, Collector: collector,
		WarmupDeadline: time.Now().Add(-time.Hour), // past deadline: all sends counted as measured
	}, 42)

	for i := 0; i < 10; i++ {
		gen.publishOne(context.Background())
	}

	calls := pub.snapshot()
	require.NotEmpty(t, calls)
	for _, call := range calls {
		var req model.SendMessageRequest
		require.NoError(t, json.Unmarshal(call.data, &req))
		assert.Empty(t, req.ThreadParentMessageID, "plain send must not set a thread parent")
	}
}

func TestGenerator_ThreadMode_EmptyParentsSkips(t *testing.T) {
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	f := BuildFixtures(&p, 42, "site-a")
	require.NotEmpty(t, f.Subscriptions)

	// Non-nil map (thread mode) but every room has an empty parent slice →
	// publishOne must hit the early return and never publish.
	empty := map[string][]threadParent{}
	for _, s := range f.Subscriptions {
		empty[s.RoomID] = []threadParent{}
	}

	pub := &recordingPublisher{}
	metrics := NewMetrics()
	collector := NewCollector(metrics, p.Name)
	gen := NewGenerator(&GeneratorConfig{
		Preset: &p, Fixtures: f, SiteID: "site-a",
		Rate: 1, Inject: InjectFrontdoor, Publisher: pub,
		Metrics: metrics, Collector: collector,
		ParentsByRoom:  empty,
		WarmupDeadline: time.Now().Add(-time.Hour), // past deadline: all sends counted as measured
	}, 42)

	for i := 0; i < 20; i++ {
		gen.publishOne(context.Background())
	}

	// Nothing should have been published.
	assert.Empty(t, pub.snapshot(), "thread mode with empty parent slices must publish nothing")
}

func TestThreadWorkload_Label(t *testing.T) {
	w := &threadWorkload{}
	assert.Equal(t, "thread", w.Label())
}

func TestRunMaxRPS_ThreadRequiresPreset(t *testing.T) {
	code := runMaxRPS(context.Background(), &config{}, []string{"--workload=thread"})
	assert.Equal(t, 2, code)
}

func TestRunMaxRPS_ThreadUnknownPreset(t *testing.T) {
	code := runMaxRPS(context.Background(), &config{}, []string{"--workload=thread", "--preset=nope"})
	assert.Equal(t, 2, code)
}

func TestRunMaxRPS_ThreadRequiresCassandra(t *testing.T) {
	// valid preset but no CassandraHosts → fast-fail with code 2
	code := runMaxRPS(context.Background(), &config{}, []string{"--workload=thread", "--preset=medium"})
	assert.Equal(t, 2, code)
}

func TestRunMaxRPS_ThreadParentsPerRoomFlagAccepted(t *testing.T) {
	// flag parses and the Cassandra guard still fires (code 2) before any NATS connect
	code := runMaxRPS(context.Background(), &config{}, []string{
		"--workload=thread", "--preset=medium", "--parents-per-room=4",
	})
	assert.Equal(t, 2, code)
}
