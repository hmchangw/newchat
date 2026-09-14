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

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	usermodels "github.com/hmchangw/chat/user-service/models"
)

// fakeSubListRequester records every request and replies with a scripted body.
type fakeSubListRequester struct {
	mu       sync.Mutex
	subjects []string
	bodies   [][]byte
	reply    []byte
	err      error
}

func (f *fakeSubListRequester) Request(_ context.Context, subj string, data []byte, _ time.Duration) ([]byte, error) {
	f.mu.Lock()
	f.subjects = append(f.subjects, subj)
	f.bodies = append(f.bodies, append([]byte(nil), data...))
	f.mu.Unlock()
	return f.reply, f.err
}

func (f *fakeSubListRequester) seen() ([]string, [][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.subjects...), append([][]byte(nil), f.bodies...)
}

func okSubListReply(t *testing.T, rows int, hasMore bool) []byte {
	t.Helper()
	subs := make([]map[string]any, rows)
	for i := range subs {
		subs[i] = map[string]any{"roomId": "room-1"}
	}
	b, err := json.Marshal(map[string]any{"subscriptions": subs, "hasMore": hasMore})
	require.NoError(t, err)
	return b
}

func newTestSubListGenerator(t *testing.T, req SubscriptionListRequester, c *SubscriptionListCollector) *subscriptionListGenerator {
	t.Helper()
	return newTestSubListGeneratorWithRunCtx(t, req, c, context.Background())
}

func newTestSubListGeneratorWithRunCtx(t *testing.T, req SubscriptionListRequester, c *SubscriptionListCollector, runCtx context.Context) *subscriptionListGenerator {
	t.Helper()
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	f := BuildSubscriptionListFixtures(&p, 42, "site-a", time.Now().UTC())
	include := true
	return newSubscriptionListGenerator(&subscriptionListGeneratorConfig{
		Fixtures:           &f,
		SiteID:             "site-a",
		Rate:               10,
		RequestTimeout:     time.Second,
		Requester:          req,
		Collector:          c,
		MaxInFlight:        4,
		ListType:           "current",
		Limit:              200,
		IncludeLastMessage: &include,
		RunCtx:             runCtx,
	}, 42)
}

func TestSubscriptionListGenerator_RequestShapeAndSubject(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 40, true)}
	c := NewSubscriptionListCollector()
	g := newTestSubListGenerator(t, req, c)

	g.requestOne(context.Background())

	subjects, bodies := req.seen()
	require.Len(t, subjects, 1)
	assert.Regexp(t, `^chat\.user\.user-\d+\.request\.user\.site-a\.subscription\.list$`, subjects[0])

	var sent soakSubscriptionListRequest
	require.NoError(t, json.Unmarshal(bodies[0], &sent))
	assert.Equal(t, "current", sent.Type)
	assert.Equal(t, 200, sent.Limit)
	require.NotNil(t, sent.IncludeLastMessage)
	assert.True(t, *sent.IncludeLastMessage)
}

func TestSubscriptionListGenerator_RecordsSuccessfulPage(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 40, true)}
	c := NewSubscriptionListCollector()
	g := newTestSubListGenerator(t, req, c)

	g.requestOne(context.Background())

	samples := c.Samples()
	require.Len(t, samples, 1)
	assert.Equal(t, 40, samples[0].Rows)
	assert.True(t, samples[0].HasMore)
	assert.Positive(t, samples[0].Latency)
}

func TestSubscriptionListGenerator_ClassifiesReplies(t *testing.T) {
	errEnvelope, err := json.Marshal(errcode.Error{Code: errcode.CodeUnavailable, Message: "subscription list timed out, please retry"})
	require.NoError(t, err)

	tests := []struct {
		name       string
		reply      []byte
		wantSample int
		check      func(t *testing.T, c *SubscriptionListCollector)
	}{
		{
			name:  "service error envelope is a reply error, never an empty page",
			reply: errEnvelope,
			check: func(t *testing.T, c *SubscriptionListCollector) {
				assert.Equal(t, 1, c.ReplyErrors())
				assert.Equal(t, 0, c.EmptyPageCount())
			},
		},
		{
			name:  "undecodable body is a bad reply",
			reply: []byte("not json"),
			check: func(t *testing.T, c *SubscriptionListCollector) {
				assert.Equal(t, 1, c.BadReplyCount())
			},
		},
		{
			name:  "missing collection is a contract violation, not an empty page",
			reply: []byte(`{}`),
			check: func(t *testing.T, c *SubscriptionListCollector) {
				assert.Equal(t, 1, c.BadReplyCount())
				assert.Equal(t, 0, c.EmptyPageCount())
			},
		},
		{
			name:  "present but empty collection is an empty page",
			reply: []byte(`{"subscriptions":[],"hasMore":false}`),
			check: func(t *testing.T, c *SubscriptionListCollector) {
				assert.Equal(t, 1, c.EmptyPageCount())
				assert.Equal(t, 0, c.BadReplyCount())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &fakeSubListRequester{reply: tc.reply}
			c := NewSubscriptionListCollector()
			g := newTestSubListGenerator(t, req, c)

			g.requestOne(context.Background())

			assert.Len(t, c.Samples(), tc.wantSample)
			tc.check(t, c)
		})
	}
}

func TestSubscriptionListGenerator_TransportErrorIsClassified(t *testing.T) {
	req := &fakeSubListRequester{err: context.DeadlineExceeded}
	c := NewSubscriptionListCollector()
	g := newTestSubListGenerator(t, req, c)

	g.requestOne(context.Background())

	assert.Equal(t, 1, c.TimeoutErrors())
	assert.Empty(t, c.Samples())
}

func TestSubscriptionListGenerator_RunRejectsNonPositiveRate(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 1, false)}
	c := NewSubscriptionListCollector()
	g := newTestSubListGenerator(t, req, c)
	g.cfg.Rate = 0

	assert.Error(t, g.Run(context.Background()))
}

func TestSubscriptionListGenerator_PicksOnlySeededAccountsAndIsDeterministic(t *testing.T) {
	p, ok := BuiltinPreset("small")
	require.True(t, ok)
	f := BuildSubscriptionListFixtures(&p, 42, "site-a", time.Now().UTC())
	seeded := map[string]bool{}
	for i := range f.Subscriptions {
		seeded[f.Subscriptions[i].User.Account] = true
	}

	pick := func() []string {
		req := &fakeSubListRequester{reply: okSubListReply(t, 1, false)}
		g := newTestSubListGenerator(t, req, NewSubscriptionListCollector())
		for i := 0; i < 30; i++ {
			g.requestOne(context.Background())
		}
		accounts, _ := g.cfg.Requester.(*fakeSubListRequester).seen()
		return accounts
	}

	first := pick()
	require.Len(t, first, 30)
	for _, subj := range first {
		parts := strings.Split(subj, ".")
		require.Greater(t, len(parts), 2, "subject %q is not account-scoped", subj)
		assert.True(t, seeded[parts[2]],
			"account %q was addressed but owns no seeded subscriptions, so its page would come back empty", parts[2])
	}
	assert.Equal(t, first, pick(), "same seed must replay the same account sequence")
}

func TestSubscriptionListGenerator_EmptyFixturesRecordNothing(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 1, false)}
	c := NewSubscriptionListCollector()
	empty := Fixtures{}
	include := true
	g := newSubscriptionListGenerator(&subscriptionListGeneratorConfig{
		Fixtures: &empty, SiteID: "site-a", Rate: 10, RequestTimeout: time.Second,
		Requester: req, Collector: c, MaxInFlight: 1,
		ListType: "current", Limit: 200, IncludeLastMessage: &include,
	}, 42)

	g.requestOne(context.Background())

	subjects, _ := req.seen()
	assert.Empty(t, subjects, "no accounts means no request at all")
	assert.Empty(t, c.Samples())
}

// The ramp's whole request shape rides on these three keys landing in
// user-service's own body type. A silent mismatch would not error: an unknown
// type is a 400 on every request, and a dropped limit quietly re-sizes the page
// the ramp thinks it is measuring.
func TestSubscriptionListRequest_MatchesUserServiceWireBody(t *testing.T) {
	include := false
	encoded, err := json.Marshal(soakSubscriptionListRequest{
		Type: "current", Limit: 200, IncludeLastMessage: &include,
	})
	require.NoError(t, err)

	var real usermodels.SubscriptionListRequest
	require.NoError(t, json.Unmarshal(encoded, &real))
	assert.Equal(t, "current", real.Type)
	assert.Equal(t, 200, real.Limit)
	require.NotNil(t, real.IncludeLastMessage)
	assert.False(t, *real.IncludeLastMessage)
}

// An unset IncludeLastMessage must stay off the wire: user-service reads a
// missing key as true, so sending an explicit false would change the workload.
func TestSubscriptionListRequest_OmitsUnsetOptionalFields(t *testing.T) {
	encoded, err := json.Marshal(soakSubscriptionListRequest{Type: "current"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"current"}`, string(encoded))
}

// Mirrors user-service's validListTypes; a drift here rejects a type the
// service accepts, or lets through one it 400s on every request.
func TestValidSubscriptionListTypes_MatchesUserService(t *testing.T) {
	assert.Equal(t, map[string]bool{"current": true, "rooms": true, "apps": true}, validSubscriptionListTypes)
}

// apps is a type user-service serves but these fixtures cannot: BuildFixtures
// creates only channel and dm rooms, while apps matches subscribed botDM rows
// alone. Accepting it would make every request return an empty page — recorded
// as a failure, contributing no latency — so the ramp would report a total
// failure rate against a perfectly healthy service.
func TestWorkloadSupportedListTypes_ExcludesAppsWithoutBotDMFixtures(t *testing.T) {
	assert.Equal(t, map[string]bool{"current": true, "rooms": true}, workloadSupportedListTypes)

	for listType := range workloadSupportedListTypes {
		assert.True(t, validSubscriptionListTypes[listType],
			"%q must also be a type user-service accepts", listType)
	}
	assert.True(t, validSubscriptionListTypes["apps"],
		"apps stays valid for the service; it is only this workload's fixtures that cannot serve it")
	assert.False(t, workloadSupportedListTypes["apps"])
}

// The fixtures the workload seeds must contain no botDM rows, which is the
// reason apps is unsupported. If that ever changes, apps can be supported and
// this test says so.
func TestBuildSubscriptionListFixtures_ContainNoBotDMRows(t *testing.T) {
	for _, name := range []string{"small", "medium", "realistic"} {
		t.Run(name, func(t *testing.T) {
			p, ok := BuiltinPreset(name)
			require.True(t, ok)
			f := BuildSubscriptionListFixtures(&p, 42, "site-a", time.Now().UTC())
			for i := range f.Subscriptions {
				assert.NotEqual(t, model.RoomTypeBotDM, f.Subscriptions[i].RoomType)
			}
		})
	}
}

func TestSubscriptionListGenerator_RunDispatchesOnBothPaths(t *testing.T) {
	for _, tc := range []struct {
		name        string
		maxInFlight int
	}{
		{"paced dispatch", 4},
		{"serial dispatch (bisection path)", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &fakeSubListRequester{reply: okSubListReply(t, 3, false)}
			c := NewSubscriptionListCollector()
			g := newTestSubListGenerator(t, req, c)
			g.cfg.MaxInFlight = tc.maxInFlight
			g.cfg.Rate = 200

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			require.NoError(t, g.Run(ctx))

			subjects, _ := req.seen()
			assert.NotEmpty(t, subjects, "the run must have dispatched at least one request")
		})
	}
}

func TestSubListRowName(t *testing.T) {
	channel := &model.Room{Type: model.RoomTypeChannel, Name: "engineering"}
	dm := &model.Room{Type: model.RoomTypeDM, Name: "room-7"}

	tests := []struct {
		name    string
		room    *model.Room
		members []string
		account string
		want    string
	}{
		{"channel carries the room name", channel, []string{"alice", "bob"}, "alice", "engineering"},
		{"dm carries the counterpart account", dm, []string{"alice", "bob"}, "alice", "bob"},
		{"dm from the other side", dm, []string{"alice", "bob"}, "bob", "alice"},
		{"self-dm falls back to the room name", dm, []string{"alice"}, "alice", "room-7"},
		{"dm with no members falls back", dm, nil, "alice", "room-7"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, subListRowName(tc.room, tc.members, tc.account))
		})
	}
}

// The realistic preset's last 10% of rooms are DMs, so it is the preset that
// exercises the counterpart-naming branch end to end.
func TestBuildSubscriptionListFixtures_NamesDMRowsAfterTheCounterpart(t *testing.T) {
	p, ok := BuiltinPreset("realistic")
	require.True(t, ok)
	f := BuildSubscriptionListFixtures(&p, 42, "site-a", time.Now().UTC())

	roomType := map[string]model.RoomType{}
	for i := range f.Rooms {
		roomType[f.Rooms[i].ID] = f.Rooms[i].Type
	}
	dmRows := 0
	for i := range f.Subscriptions {
		s := &f.Subscriptions[i]
		if roomType[s.RoomID] != model.RoomTypeDM {
			continue
		}
		dmRows++
		assert.NotEqual(t, s.User.Account, s.Name, "a DM row is named after the counterpart, not the subscriber")
		assert.NotEmpty(t, s.Name)
	}
	require.Positive(t, dmRows, "the realistic preset must produce DM rows")
}

// The body is marshalled once at construction, so a request must still carry
// the full shape on every call — not just the first.
func TestSubscriptionListGenerator_ReusesTheSameBodyAcrossRequests(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 2, false)}
	g := newTestSubListGenerator(t, req, NewSubscriptionListCollector())

	for i := 0; i < 3; i++ {
		g.requestOne(context.Background())
	}

	_, bodies := req.seen()
	require.Len(t, bodies, 3)
	for _, b := range bodies {
		var sent soakSubscriptionListRequest
		require.NoError(t, json.Unmarshal(b, &sent))
		assert.Equal(t, "current", sent.Type)
		assert.Equal(t, 200, sent.Limit)
		require.NotNil(t, sent.IncludeLastMessage)
	}
}

// A body that cannot be built is a configuration fault, so it must surface once
// from Run rather than as one bad reply per request.
func TestSubscriptionListGenerator_RunReportsBodyMarshalFailureOnce(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 1, false)}
	c := NewSubscriptionListCollector()
	g := newTestSubListGenerator(t, req, c)
	g.bodyErr = assert.AnError

	err := g.Run(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 0, c.BadReplyCount())
	subjects, _ := req.seen()
	assert.Empty(t, subjects, "no request is dispatched when the body could not be built")
}

// A row a client cannot use is a contract failure, the same as the top-level
// `{}` case — and it is also the fastest possible reply, so accepting it would
// contribute a fast successful sample.
func TestSubscriptionListGenerator_RejectsRowsMissingRoomID(t *testing.T) {
	tests := []struct {
		name        string
		reply       string
		wantSamples int
		wantBad     int
	}{
		{"row without roomId", `{"subscriptions":[{}],"hasMore":false}`, 0, 1},
		{"one good row, one malformed", `{"subscriptions":[{"roomId":"r1"},{}]}`, 0, 1},
		{"all rows well formed", `{"subscriptions":[{"roomId":"r1"},{"roomId":"r2"}]}`, 1, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &fakeSubListRequester{reply: []byte(tc.reply)}
			c := NewSubscriptionListCollector()
			g := newTestSubListGenerator(t, req, c)

			g.requestOne(context.Background())

			assert.Len(t, c.Samples(), tc.wantSamples)
			assert.Equal(t, tc.wantBad, c.BadReplyCount())
		})
	}
}

// The window context stops new dispatches; it must not abort and erase requests
// already admitted into the measured window. Those are disproportionately the
// slow ones, so dropping them biases latency and error rate downward.
func TestSubscriptionListGenerator_AdmittedRequestSurvivesDispatchCancellation(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 3, false)}
	c := NewSubscriptionListCollector()
	g := newTestSubListGenerator(t, req, c)

	dispatchCtx, cancelDispatch := context.WithCancel(context.Background())
	cancelDispatch()

	g.requestOne(dispatchCtx)

	assert.Len(t, c.Samples(), 1,
		"a request admitted before the boundary must still be measured")
}

// Outer cancellation is the run genuinely ending, and must still discard.
func TestSubscriptionListGenerator_OuterCancellationStillDiscards(t *testing.T) {
	req := &fakeSubListRequester{err: context.Canceled}
	c := NewSubscriptionListCollector()
	runCtx, cancelRun := context.WithCancel(context.Background())
	g := newTestSubListGeneratorWithRunCtx(t, req, c, runCtx)
	cancelRun()

	g.requestOne(context.Background())

	assert.Empty(t, c.Samples())
	assert.Equal(t, 0, c.TimeoutErrors())
}

// An unset RunCtx falls back to the dispatch context, so a caller that does not
// thread the run context keeps the previous behaviour rather than losing
// cancellation entirely.
func TestSubscriptionListGenerator_RequestCtxFallsBackToDispatchCtx(t *testing.T) {
	req := &fakeSubListRequester{reply: okSubListReply(t, 1, false)}
	g := newTestSubListGenerator(t, req, NewSubscriptionListCollector())

	type ctxKey string
	dispatchCtx := context.WithValue(context.Background(), ctxKey("k"), "v")

	g.cfg.RunCtx = nil
	assert.Equal(t, "v", g.requestCtx(dispatchCtx).Value(ctxKey("k")))

	runCtx := context.WithValue(context.Background(), ctxKey("k"), "run")
	g.cfg.RunCtx = runCtx
	assert.Equal(t, "run", g.requestCtx(dispatchCtx).Value(ctxKey("k")))
}
