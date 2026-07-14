//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestThreadReadWorkload_EndToEnd seeds history fixtures (with thread parents)
// into a real Mongo, then drives the generator briefly against a stub
// GetThreadMessages responder and asserts it records samples with no errors.
func TestThreadReadWorkload_EndToEnd(t *testing.T) {
	ctx := context.Background()

	db := testutil.MongoDB(t, "loadgen_threadread")

	siteID := "site-test"
	p := HistoryPreset{
		Name: "thread-it", Users: 12, Rooms: 3, BaselineSize: 6,
		MessagesPerRoom: 60, MessageSpanDays: 1, ThreadRate: 0.25,
		RepliesPerThread: 3, ContentBytes: 50,
	}
	fixtures := BuildHistoryFixtures(&p, 42, siteID, time.Now().UTC())
	require.NoError(t, Seed(ctx, db, &fixtures.Fixtures))
	require.Len(t, fixtures.ThreadParents, len(fixtures.Fixtures.Rooms),
		"every seeded room must have thread parents so the Zipf picker never hits a parentless room")

	// Stub GetThreadMessages responder.
	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Drain() })

	sub, err := nc.Subscribe(subject.MsgThreadWildcard(siteID), func(m *nats.Msg) {
		_ = m.Respond([]byte(`{"messages":[],"parentMessage":{"messageId":"x"},"hasNext":false}`))
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	collector := newThreadReadCollector()
	gen := newThreadReadGenerator(&threadReadGeneratorConfig{
		Fixtures:       &fixtures,
		SiteID:         siteID,
		Rate:           50,
		PageLimit:      20,
		RequestTimeout: 2 * time.Second,
		Requester:      newNATSHistoryRequester(nc),
		Collector:      collector,
		MaxInFlight:    16,
	}, 42)

	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	require.NoError(t, gen.Run(runCtx))

	assert.NotEmpty(t, collector.Samples(), "generator produced zero samples")
	assert.Equal(t, 0, collector.TimeoutErrors(), "no requests should time out against the stub")
	assert.Equal(t, 0, collector.ReplyErrors(), "stub never returns an error")
	assert.Equal(t, 0, collector.BadReplyCount(), "stub always returns a parentMessage")
	assert.Equal(t, 0, collector.NoParentsCount(), "every seeded room has parents")
}
