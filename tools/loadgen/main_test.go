package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestHardPublishErrorCount_ExcludesSelfLimitAndGatekeeper(t *testing.T) {
	m := NewMetrics()
	// Real publish-side failures.
	m.PublishErrors.WithLabelValues("p", "publish").Add(3)
	m.PublishErrors.WithLabelValues("p", "marshal").Add(2)
	m.PublishErrors.WithLabelValues("p", "bad_reply").Add(1)
	// Not hard errors: gatekeeper is reported separately; saturated and underrun
	// are load-box pacing signals, not publish failures.
	m.PublishErrors.WithLabelValues("p", "gatekeeper").Add(50)
	m.PublishErrors.WithLabelValues("p", "saturated").Add(400)
	m.PublishErrors.WithLabelValues("p", "underrun").Add(900)

	mfs, err := m.Registry.Gather()
	require.NoError(t, err)

	// Only publish+marshal+bad_reply count: 3+2+1 = 6.
	assert.Equal(t, 6, hardPublishErrorCount(mfs))
}

func TestLastToken(t *testing.T) {
	cases := []struct{ in, want string }{
		{"chat.user.alice.response.abc-123", "abc-123"},
		{"abc", "abc"},       // no dot
		{"", ""},             // empty
		{"a.b.c.d.e.f", "f"}, // many dots
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			assert.Equal(t, c.want, lastToken(c.in))
		})
	}
}

func TestCounterValue(t *testing.T) {
	m := NewMetrics()
	m.Published.WithLabelValues("small", "measured").Inc()
	m.Published.WithLabelValues("small", "measured").Inc()
	m.Published.WithLabelValues("medium", "measured").Inc()
	assert.Equal(t, float64(3), counterValue(m, "loadgen_published_total"))
	assert.Equal(t, float64(0), counterValue(m, "nonexistent_metric"))
}

func TestCounterValueLabeled(t *testing.T) {
	m := NewMetrics()
	m.PublishErrors.WithLabelValues("small", "publish").Inc()
	m.PublishErrors.WithLabelValues("small", "publish").Inc()
	m.PublishErrors.WithLabelValues("small", "gatekeeper").Inc()
	m.PublishErrors.WithLabelValues("large", "publish").Inc()
	// By reason=publish: two "small" + one "large" = 3
	assert.Equal(t, float64(3), counterValueLabeled(m, "loadgen_publish_errors_total", "reason", "publish"))
	// By reason=gatekeeper: one
	assert.Equal(t, float64(1), counterValueLabeled(m, "loadgen_publish_errors_total", "reason", "gatekeeper"))
	// Unknown label value
	assert.Equal(t, float64(0), counterValueLabeled(m, "loadgen_publish_errors_total", "reason", "nope"))
}

func TestWriteCSVFile_RoundTrip(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	now := time.Unix(0, 0)
	c.RecordPublish("r-1", "m-1", now)
	c.RecordReply("r-1", now.Add(5*time.Millisecond))
	c.RecordBroadcast("m-1", now.Add(8*time.Millisecond))

	path := filepath.Join(t.TempDir(), "out.csv")
	require.NoError(t, writeCSVFile(path, c))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	out := string(data)
	// Header present
	require.True(t, strings.HasPrefix(out, "timestamp_ns,request_id,metric,latency_ns"))
	// At least one E1 row and one E2 row
	require.Contains(t, out, ",E1,")
	require.Contains(t, out, ",E2,")
}

func TestWriteCSVFile_EmptyCollector(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")

	path := filepath.Join(t.TempDir(), "empty.csv")
	require.NoError(t, writeCSVFile(path, c))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	out := string(data)
	// Header still present, no data rows
	require.True(t, strings.HasPrefix(out, "timestamp_ns,request_id,metric,latency_ns"))
	require.NotContains(t, out, ",E1,")
	require.NotContains(t, out, ",E2,")
}

func TestNewNatsCorePublisher_CanonicalSetsUseJetStream(t *testing.T) {
	p := newNatsCorePublisher(nil, InjectCanonical, nil)
	require.True(t, p.useJetStream)
}

func TestNewNatsCorePublisher_FrontdoorDoesNotSetUseJetStream(t *testing.T) {
	p := newNatsCorePublisher(nil, InjectFrontdoor, nil)
	require.False(t, p.useJetStream)
}

func TestNewNatsCorePublisher_FieldWiring(t *testing.T) {
	p := newNatsCorePublisher(nil, InjectCanonical, nil)
	assert.Nil(t, p.nc)
	assert.Nil(t, p.js)
	assert.True(t, p.useJetStream)

	p2 := newNatsCorePublisher(nil, InjectFrontdoor, nil)
	assert.Nil(t, p2.nc)
	assert.Nil(t, p2.js)
	assert.False(t, p2.useJetStream)
}

func TestNewE2Handler_RecordsWhenMessageNil(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	c.RecordPublish("req-1", "m-1", time.Unix(0, 0))

	handler := newE2Handler(c)
	evt := model.RoomEvent{Type: model.RoomEventNewMessage, RoomID: "r", LastMsgID: "m-1"}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	handler(&nats.Msg{Subject: "chat.room.r.event", Data: data})

	assert.Equal(t, 1, c.E2Count())
}

func TestNewE2Handler_RecordsWhenMessagePopulated(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	c.RecordPublish("req-1", "m-2", time.Unix(0, 0))

	handler := newE2Handler(c)
	evt := model.RoomEvent{
		Type:      model.RoomEventNewMessage,
		RoomID:    "r",
		LastMsgID: "m-2",
		Message:   &model.ClientMessage{Message: model.Message{ID: "m-2"}},
	}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	handler(&nats.Msg{Subject: "chat.room.r.event", Data: data})

	assert.Equal(t, 1, c.E2Count())
}

func TestNewE2Handler_SkipsEventWithoutLastMsgID(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	c.RecordPublish("req-1", "m-3", time.Unix(0, 0))

	handler := newE2Handler(c)
	evt := model.RoomEvent{Type: model.RoomEventNewMessage, RoomID: "r"}
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	handler(&nats.Msg{Subject: "chat.room.r.event", Data: data})

	assert.Equal(t, 0, c.E2Count())
}

func TestNewE2Handler_SkipsMalformedJSON(t *testing.T) {
	m := NewMetrics()
	c := NewCollector(m, "small")
	handler := newE2Handler(c)
	handler(&nats.Msg{Subject: "chat.room.r.event", Data: []byte("not json")})
	assert.Equal(t, 0, c.E2Count())
}

func TestMetricsHandler_ServesOpenMetrics(t *testing.T) {
	m := NewMetrics()
	m.Published.WithLabelValues("small", "measured").Inc()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	require.Equal(t, 200, rec.Code)
	require.Contains(t, rec.Body.String(), "loadgen_published_total")
}

func TestMetricsHandler_ContentType(t *testing.T) {
	m := NewMetrics()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	require.Equal(t, 200, rec.Code)
	ct := rec.Header().Get("Content-Type")
	require.NotEmpty(t, ct)
	// Prometheus text format
	require.Contains(t, ct, "text/plain")
}

func TestNewMetrics_RegistersMemberCollectors(t *testing.T) {
	m := NewMetrics()

	want := []string{
		"loadgen_member_published_total",
		"loadgen_member_publish_errors_total",
		"loadgen_member_e1_latency_seconds",
		"loadgen_member_e2_latency_seconds",
		"loadgen_member_room_size",
	}
	// Some metrics only appear after first Observe/Inc — force them to surface.
	m.MemberPublished.WithLabelValues("p", "warmup", "frontdoor", "users").Inc()
	m.MemberPublishErrors.WithLabelValues("publish").Inc()
	m.MemberE1Latency.WithLabelValues("p", "frontdoor").Observe(0.001)
	m.MemberE2Latency.WithLabelValues("p", "frontdoor").Observe(0.001)
	m.MemberRoomSize.WithLabelValues("room-x").Set(1)

	mfs, err := m.Registry.Gather()
	require.NoError(t, err)
	got := map[string]bool{}
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}
	for _, name := range want {
		assert.True(t, got[name], "metric %s not registered", name)
	}
}

func TestRunSeed_RejectsUnknownWorkload(t *testing.T) {
	cfg := &config{}
	code := runSeed(context.Background(), cfg, []string{"--workload=widgets", "--preset=members-small"})
	assert.Equal(t, 2, code)
}

func TestRunSeed_RejectsUnknownMembersPreset(t *testing.T) {
	cfg := &config{}
	code := runSeed(context.Background(), cfg, []string{"--workload=members", "--preset=nope"})
	assert.Equal(t, 2, code)
}

func TestDispatch_MembersSustained_UnknownPreset(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"loadgen", "members-sustained", "--preset=nope"}
	cfg := &config{NatsURL: "nats://localhost:1", MongoURI: "mongodb://localhost:1"}
	code := dispatch(context.Background(), cfg)
	assert.Equal(t, 2, code)
}

func TestDispatch_MembersSustained_RejectsBadShape(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"loadgen", "members-sustained", "--preset=members-small", "--shape=orgs"}
	cfg := &config{NatsURL: "nats://localhost:1", MongoURI: "mongodb://localhost:1"}
	code := dispatch(context.Background(), cfg)
	assert.Equal(t, 2, code)
}

func TestDispatch_MembersCapacity_RequiresTargetSize(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"loadgen", "members-capacity", "--preset=members-capacity"}
	cfg := &config{NatsURL: "nats://localhost:1", MongoURI: "mongodb://localhost:1"}
	code := dispatch(context.Background(), cfg)
	assert.Equal(t, 2, code)
}

func TestDispatch_DailySubcommand(t *testing.T) {
	// dispatch should accept "daily" and return non-zero for unknown preset
	// (so we don't actually run a daily session — just exercise routing).
	old := os.Args
	defer func() { os.Args = old }()
	os.Args = []string{"loadgen", "daily", "--preset=nope"}
	cfg := &config{NatsURL: "nats://x", MongoURI: "mongodb://x"}
	rc := dispatch(context.Background(), cfg)
	require.Equal(t, 2, rc)
}

func TestParseCapacityConfig_HelpReturnsErrHelp(t *testing.T) {
	_, err := parseCapacityConfig([]string{"-h"})
	require.ErrorIs(t, err, flag.ErrHelp)
}
