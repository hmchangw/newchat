package main

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// fakeThreadReadRequester records every (subject, body, request-ID) it is asked
// to request and returns a configurable reply/error.
type fakeThreadReadRequester struct {
	mu       sync.Mutex
	subjects []string
	bodies   [][]byte
	reqIDs   []string
	reply    []byte
	err      error
}

func (f *fakeThreadReadRequester) Request(ctx context.Context, subj string, data []byte, _ time.Duration) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subjects = append(f.subjects, subj)
	f.bodies = append(f.bodies, data)
	f.reqIDs = append(f.reqIDs, natsutil.RequestIDFromContext(ctx))
	if f.err != nil {
		return nil, f.err
	}
	return f.reply, nil
}

func (f *fakeThreadReadRequester) snapshot() (subs []string, bodies [][]byte, ids []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	subs = append(subs, f.subjects...)
	bodies = append(bodies, f.bodies...)
	ids = append(ids, f.reqIDs...)
	return
}

// threadTestPreset is a tiny deterministic history preset that guarantees thread
// parents in every room without the cost of the medium/large presets.
func threadTestPreset() HistoryPreset {
	return HistoryPreset{
		Name: "thread-test", Users: 12, Rooms: 3, BaselineSize: 6,
		MessagesPerRoom: 40, MessageSpanDays: 1, ThreadRate: 0.25,
		RepliesPerThread: 3, ContentBytes: 50,
	}
}

func newThreadReadTestGen(t *testing.T, req HistoryRequester, c *threadReadCollector) (*threadReadGenerator, HistoryFixtures) {
	t.Helper()
	p := threadTestPreset()
	f := BuildHistoryFixtures(&p, 42, "site-test", time.Now().UTC())
	gen := newThreadReadGenerator(&threadReadGeneratorConfig{
		Fixtures:       &f,
		SiteID:         "site-test",
		Rate:           200,
		PageLimit:      20,
		RequestTimeout: time.Second,
		Requester:      req,
		Collector:      c,
		MaxInFlight:    8,
	}, 42)
	return gen, f
}

func TestThreadReadGenerator_EmitsRealThreadRequests(t *testing.T) {
	req := &fakeThreadReadRequester{reply: []byte(`{"messages":[],"parentMessage":{"messageId":"x"},"hasNext":false}`)}
	c := newThreadReadCollector()
	gen, f := newThreadReadTestGen(t, req, c)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	subs, bodies, _ := req.snapshot()
	require.NotEmpty(t, subs, "generator issued no requests")
	for i, s := range subs {
		assert.True(t, strings.HasPrefix(s, "chat.user."), "unexpected subject %q", s)
		assert.True(t, strings.HasSuffix(s, ".msg.thread"), "unexpected subject %q", s)

		// Subject tokens: chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread
		toks := strings.Split(s, ".")
		require.GreaterOrEqual(t, len(toks), 9, "subject %q has too few tokens", s)
		account, roomID := toks[2], toks[5]

		var body getThreadMessagesRequest
		require.NoError(t, json.Unmarshal(bodies[i], &body))
		assert.Equal(t, 20, body.Limit)

		// threadMessageId must be a real seeded parent of that room.
		parentIDs := map[string]bool{}
		for _, p := range f.ThreadParents[roomID] {
			parentIDs[p.MessageID] = true
		}
		assert.True(t, parentIDs[body.ThreadMessageID],
			"threadMessageId %q is not a seeded parent of room %q", body.ThreadMessageID, roomID)

		// account must be a subscriber of that room.
		isMember := false
		for j := range f.Fixtures.Subscriptions {
			sub := f.Fixtures.Subscriptions[j]
			if sub.RoomID == roomID && sub.User.Account == account {
				isMember = true
				break
			}
		}
		assert.True(t, isMember, "account %q is not a subscriber of room %q", account, roomID)
	}
	assert.NotEmpty(t, c.Samples(), "healthy replies should be recorded as samples")
}

func TestThreadReadGenerator_ErrorEnvelopeRecordedAsReplyError(t *testing.T) {
	req := &fakeThreadReadRequester{reply: []byte(`{"error":"room not found"}`)}
	c := newThreadReadCollector()
	gen, _ := newThreadReadTestGen(t, req, c)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	assert.Empty(t, c.Samples(), "error envelopes must not count as samples")
	assert.Greater(t, c.ReplyErrors(), 0, "error envelope must count as a reply error")
}

func TestThreadReadGenerator_MissingParentMessageIsBadReply(t *testing.T) {
	req := &fakeThreadReadRequester{reply: []byte(`{"messages":[]}`)}
	c := newThreadReadCollector()
	gen, _ := newThreadReadTestGen(t, req, c)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	assert.Empty(t, c.Samples())
	assert.Greater(t, c.BadReplyCount(), 0, "reply without parentMessage must count as bad reply")
}

func TestThreadReadGenerator_TimeoutRecorded(t *testing.T) {
	req := &fakeThreadReadRequester{err: context.DeadlineExceeded}
	c := newThreadReadCollector()
	gen, _ := newThreadReadTestGen(t, req, c)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	assert.Greater(t, c.TimeoutErrors(), 0, "DeadlineExceeded must count as a timeout")
}

func TestThreadReadGenerator_NoParentsSkippedAndCounted(t *testing.T) {
	// history-small has ThreadRate 0 -> no parents in any room.
	p, ok := BuiltinHistoryPreset("history-small")
	require.True(t, ok)
	f := BuildHistoryFixtures(&p, 42, "site-test", time.Now().UTC())
	req := &fakeThreadReadRequester{reply: []byte(`{"parentMessage":{"messageId":"x"}}`)}
	c := newThreadReadCollector()
	gen := newThreadReadGenerator(&threadReadGeneratorConfig{
		Fixtures: &f, SiteID: "site-test", Rate: 200, PageLimit: 20,
		RequestTimeout: time.Second, Requester: req, Collector: c, MaxInFlight: 8,
	}, 42)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	subs, _, _ := req.snapshot()
	assert.Empty(t, subs, "no parents means no requests should be issued")
	assert.Empty(t, c.Samples())
	assert.Greater(t, c.NoParentsCount(), 0, "no-parent rooms must be counted")
}

func TestThreadReadGenerator_RequiresPositiveRate(t *testing.T) {
	req := &fakeThreadReadRequester{reply: []byte(`{"parentMessage":{"messageId":"x"}}`)}
	c := newThreadReadCollector()
	gen, _ := newThreadReadTestGen(t, req, c)
	gen.cfg.Rate = 0
	assert.Error(t, gen.Run(context.Background()))
}

func TestThreadReadGenerator_CarriesFreshRequestID(t *testing.T) {
	req := &fakeThreadReadRequester{reply: []byte(`{"parentMessage":{"messageId":"x"}}`)}
	c := newThreadReadCollector()
	gen, _ := newThreadReadTestGen(t, req, c)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	require.NoError(t, gen.Run(ctx))

	_, _, ids := req.snapshot()
	require.NotEmpty(t, ids)
	seen := map[string]bool{}
	for _, id := range ids {
		assert.True(t, idgen.IsValidUUID(id), "request ID %q must be a valid UUID", id)
		assert.False(t, seen[id], "each request must mint a fresh X-Request-ID, got duplicate %q", id)
		seen[id] = true
	}
}
