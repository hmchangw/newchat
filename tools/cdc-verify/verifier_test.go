package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

func verifierMapping() *Mapping {
	return &Mapping{Sources: []SourceMapping{{
		Collection: "rocketchat_message",
		// "update" is deliberately absent so it classifies as op-skip; "replace"
		// gives the supersede test a second verified op distinct from "insert".
		Ops: map[string]OpAction{"insert": OpVerify, "replace": OpVerify, "delete": OpVerifyAbsent},
		Targets: map[string]Target{
			"byId": {Kind: "cassandra", Table: "messages_by_id",
				Key: map[string]KeyFrom{"message_id": {From: []string{"_id"}}}},
			"room": {Kind: "mongo", Collection: "rooms",
				Key: map[string]KeyFrom{"_id": {From: []string{"rid"}}}},
		},
		Fields: map[string][]DestRef{
			"msg": {{Dest: "byId.body"}},
			"rid": {{Dest: "room._id"}},
		},
	}}}
}

// testVerifier wires mocks and a deterministic clock. Each sleep advances the
// clock by poll; timeout after N polls is then exact.
func testVerifier(t *testing.T, cfg verifierConfig) (*verifier, *MockSourceStore, *MockTargetStore, *MockCassStore, *resultsStore) {
	t.Helper()
	return customVerifier(t, verifierMapping(), cfg)
}

// customVerifier is testVerifier over an arbitrary mapping.
func customVerifier(t *testing.T, m *Mapping, cfg verifierConfig) (*verifier, *MockSourceStore, *MockTargetStore, *MockCassStore, *resultsStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	src := NewMockSourceStore(ctrl)
	tgt := NewMockTargetStore(ctrl)
	cass := NewMockCassStore(ctrl)
	results := newResultsStore(100, 100, nil)
	v := newVerifier(m, src, tgt, cass,
		newTransformRegistry(msgbucket.New(72*time.Hour)), results, cfg)
	var mu sync.Mutex
	fake := time.Unix(0, 0)
	v.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return fake }
	v.sleep = func(ctx context.Context, d time.Duration) bool {
		mu.Lock()
		fake = fake.Add(d)
		mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	v.sampleFn = func() int { return 0 } // always sampled in
	return v, src, tgt, cass, results
}

func waitState(t *testing.T, results *resultsStore, docID string, want CheckState) CheckResult {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		recent := results.Recent()
		for i := range recent {
			if recent[i].DocID == docID && recent[i].State == want {
				return recent[i]
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no result for %s in state %s; have %+v", docID, want, results.Recent())
	return CheckResult{}
}

func srcDoc() map[string]any {
	return map[string]any{"_id": "m1", "msg": "hi", "rid": "r1"}
}

func TestVerifier_MatchFirstAttempt(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 10 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), "rocketchat_message", "m1").Return(srcDoc(), nil).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), "messages_by_id", map[string]any{"message_id": "m1"}, gomock.Any()).
		Return(map[string]any{"body": "hi"}, nil)
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", map[string]any{"_id": "r1"}, gomock.Any()).
		Return(map[string]any{"_id": "r1"}, nil)

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateMatched)
	assert.Equal(t, 1, r.Attempts)
	for _, tr := range r.Targets {
		assert.True(t, tr.Matched)
	}
}

func TestVerifier_ConvergesAfterDelay(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 10 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", gomock.Any(), gomock.Any()).
		Return(map[string]any{"_id": "r1"}, nil).AnyTimes()
	first := cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errNotFound)
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]any{"body": "hi"}, nil).After(first)

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateMatched)
	assert.Equal(t, 2, r.Attempts)
}

func TestVerifier_FrozenTargetNotRechecked(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 10 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	// room target matches on attempt 1 and MUST NOT be looked up again
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", gomock.Any(), gomock.Any()).
		Return(map[string]any{"_id": "r1"}, nil).Times(1)
	first := cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errNotFound)
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]any{"body": "hi"}, nil).After(first)

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	waitState(t, results, "m1", StateMatched)
}

func TestVerifier_TimeoutFailsWithDiffs(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 3 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", gomock.Any(), gomock.Any()).
		Return(map[string]any{"_id": "r1"}, nil).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]any{"body": "WRONG"}, nil).AnyTimes()

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateFailed)
	require.Len(t, r.Targets, 2)
	for _, tr := range r.Targets {
		if tr.Alias == "byId" {
			assert.Equal(t, "mismatch", tr.LastCause)
			require.Len(t, tr.Diffs, 1)
			assert.Equal(t, "hi", tr.Diffs[0].Want)
			assert.Equal(t, "WRONG", tr.Diffs[0].Got)
		}
	}
	assert.Len(t, results.Failures(), 1)
}

func TestVerifier_AmbiguousKeyFailsImmediately(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 60 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", gomock.Any(), gomock.Any()).Return(nil, errAmbiguous).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]any{"body": "hi"}, nil).AnyTimes()

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateFailed)
	assert.Equal(t, 1, r.Attempts) // no polling loop on ambiguity
	for _, tr := range r.Targets {
		if tr.Alias == "room" {
			assert.Equal(t, "ambiguous-key", tr.LastCause)
		}
	}
}

func TestVerifier_VerifyAbsentDelete(t *testing.T) {
	v, _, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 10 * time.Second, MaxChecks: 4, SamplePercent: 100})
	// delete: no source lookup; dest keys derive from documentKey only (_id)
	cass.EXPECT().SelectOne(gomock.Any(), "messages_by_id", map[string]any{"message_id": "m1"}, gomock.Any()).
		Return(nil, errNotFound)
	// A verify-absent key not derivable from documentKey alone reports
	// key-unresolvable: byId matches by absence, room (needs rid) fails at deadline.
	_ = tgt
	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "delete", DocID: "m1"})
	r := waitState(t, results, "m1", StateFailed)
	for _, tr := range r.Targets {
		switch tr.Alias {
		case "byId":
			assert.True(t, tr.Matched)
		case "room":
			assert.Equal(t, "key-unresolvable", tr.LastCause)
		}
	}
}

func TestVerifier_SkipClassification(t *testing.T) {
	v, _, _, _, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: time.Second, MaxChecks: 4, SamplePercent: 100})
	v.Submit(CDCEvent{Collection: "unmapped_coll", Op: "insert", DocID: "x"})
	r := waitState(t, results, "x", StateSkipped)
	assert.Equal(t, "unmapped", r.SkipReason)

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "update", DocID: "y"}) // op not in Ops map
	r = waitState(t, results, "y", StateSkipped)
	assert.Equal(t, "op-skip", r.SkipReason)
}

func TestVerifier_SampledOut(t *testing.T) {
	v, _, _, _, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: time.Second, MaxChecks: 4, SamplePercent: 50})
	v.sampleFn = func() int { return 99 } // above 50 -> sampled out
	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateSkipped)
	assert.Equal(t, "sampled-out", r.SkipReason)
}

func TestVerifier_Supersede(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 60 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errNotFound).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errNotFound).AnyTimes()
	// Park checks between polls instead of advancing the fake clock — otherwise
	// the first check burns its whole deadline before the newer event arrives.
	v.sleep = func(ctx context.Context, _ time.Duration) bool {
		<-ctx.Done()
		return false
	}

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	waitState(t, results, "m1", StatePending)
	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "replace", DocID: "m1"})
	// old check flips to superseded; new check is pending on the same key
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var superseded, pending bool
		recent := results.Recent()
		for i := range recent {
			r := &recent[i]
			if r.DocID == "m1" && r.State == StateSuperseded {
				superseded = true
			}
			if r.DocID == "m1" && r.State == StatePending && r.Op == "replace" {
				pending = true
			}
		}
		if superseded && pending {
			v.Shutdown(context.Background())
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("supersede did not happen")
}

func TestVerifier_ResolverMissKeepsPolling(t *testing.T) {
	m := verifierMapping()
	m.Sources[0].Resolvers = map[string]Resolver{
		"user": {DB: "source", Collection: "users",
			Key: map[string]KeyFrom{"_id": {From: []string{"u._id"}}}, Fields: []string{"username"}},
	}
	m.Sources[0].Targets["room"] = Target{Kind: "mongo", Collection: "rooms",
		Key: map[string]KeyFrom{"_id": {From: []string{"@user.username"}}}}

	ctrl := gomock.NewController(t)
	src := NewMockSourceStore(ctrl)
	tgt := NewMockTargetStore(ctrl)
	cass := NewMockCassStore(ctrl)
	results := newResultsStore(100, 100, nil)
	v := newVerifier(m, src, tgt, cass, newTransformRegistry(msgbucket.New(72*time.Hour)), results,
		verifierConfig{Poll: time.Second, Timeout: 2 * time.Second, MaxChecks: 4, SamplePercent: 100})
	var mu sync.Mutex
	fake := time.Unix(0, 0)
	v.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return fake }
	v.sleep = func(ctx context.Context, d time.Duration) bool {
		mu.Lock()
		fake = fake.Add(d)
		mu.Unlock()
		return true
	}
	v.sampleFn = func() int { return 0 }

	doc := srcDoc()
	doc["u"] = map[string]any{"_id": "u1"}
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(doc, nil).AnyTimes()
	src.EXPECT().FindOne(gomock.Any(), "users", map[string]any{"_id": "u1"}, []string{"username"}).
		Return(nil, errNotFound).AnyTimes() // resolver never resolves
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]any{"body": "hi"}, nil).AnyTimes()

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "m1"})
	r := waitState(t, results, "m1", StateFailed)
	assert.Greater(t, r.Attempts, 1) // it kept polling, then failed at deadline
	for _, tr := range r.Targets {
		if tr.Alias == "room" {
			assert.Equal(t, "resolver-miss: user", tr.LastCause)
		}
	}
}

// --- Additional coverage for branches the lifecycle table does not reach ----

func insertEvent(docID string) CDCEvent {
	return CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: docID}
}

func TestVerifier_SourceLookupFailureKeepsPolling(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantCause string
	}{
		{"deleted between event and check", errNotFound, "source-missing"},
		{"source unreachable", errors.New("boom"), "source-error: boom"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, src, _, _, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 2 * time.Second, MaxChecks: 4, SamplePercent: 100})
			// No dest lookups are expected: without a source doc there is nothing
			// to compare, so the mocks would fail the test if we looked anyway.
			src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(nil, tc.err).AnyTimes()

			v.Submit(insertEvent("m1"))
			r := waitState(t, results, "m1", StateFailed)
			assert.Greater(t, r.Attempts, 1, "source failures must keep polling")
			require.Len(t, r.Targets, 2)
			for i := range r.Targets {
				assert.False(t, r.Targets[i].Matched)
				assert.Equal(t, tc.wantCause, r.Targets[i].LastCause)
			}
		})
	}
}

func TestVerifier_VerifyAbsentStillPresent(t *testing.T) {
	v, _, _, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 2 * time.Second, MaxChecks: 4, SamplePercent: 100})
	cass.EXPECT().SelectOne(gomock.Any(), "messages_by_id", map[string]any{"message_id": "m1"}, gomock.Any()).
		Return(map[string]any{"body": "hi"}, nil).AnyTimes()

	v.Submit(CDCEvent{Collection: "rocketchat_message", Op: "delete", DocID: "m1"})
	r := waitState(t, results, "m1", StateFailed)
	for i := range r.Targets {
		if r.Targets[i].Alias == "byId" {
			assert.False(t, r.Targets[i].Matched)
			assert.Equal(t, "still-present", r.Targets[i].LastCause)
		}
	}
}

func TestVerifier_DestLookupErrorKeepsPolling(t *testing.T) {
	v, src, tgt, cass, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 2 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", gomock.Any(), gomock.Any()).
		Return(map[string]any{"_id": "r1"}, nil).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("cassandra down")).AnyTimes()

	v.Submit(insertEvent("m1"))
	r := waitState(t, results, "m1", StateFailed)
	assert.Greater(t, r.Attempts, 1, "a transient lookup error must not end the check")
	for i := range r.Targets {
		if r.Targets[i].Alias == "byId" {
			assert.Equal(t, "lookup-error: cassandra down", r.Targets[i].LastCause)
		}
	}
}

func TestVerifier_MissingCassandraStoreReportsLookupError(t *testing.T) {
	v, src, tgt, _, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: time.Second, MaxChecks: 4, SamplePercent: 100})
	v.cass = nil // mapping has a cassandra target but main wired no cassandra store
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", gomock.Any(), gomock.Any()).
		Return(map[string]any{"_id": "r1"}, nil).AnyTimes()

	v.Submit(insertEvent("m1"))
	r := waitState(t, results, "m1", StateFailed)
	for i := range r.Targets {
		if r.Targets[i].Alias == "byId" {
			assert.Contains(t, r.Targets[i].LastCause, "lookup-error:")
		}
	}
}

func verbatimMapping() *Mapping {
	return &Mapping{Sources: []SourceMapping{{
		Collection: "rocketchat_room",
		Ops:        map[string]OpAction{"insert": OpVerify},
		Targets: map[string]Target{
			"rooms": {Kind: "mongo", Collection: "rooms", Mode: "verbatim",
				Ignore: []string{"_updatedAt"},
				Key:    map[string]KeyFrom{"_id": {From: []string{"_id"}}}},
		},
	}}}
}

func TestVerifier_VerbatimTarget(t *testing.T) {
	tests := []struct {
		name      string
		dest      map[string]any
		wantState CheckState
	}{
		{"equal ignoring the ignored key", map[string]any{"_id": "r1", "name": "general", "_updatedAt": int64(9)}, StateMatched},
		{"differing compared key", map[string]any{"_id": "r1", "name": "random", "_updatedAt": int64(5)}, StateFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, src, tgt, _, results := customVerifier(t, verbatimMapping(),
				verifierConfig{Poll: time.Second, Timeout: 2 * time.Second, MaxChecks: 4, SamplePercent: 100})
			src.EXPECT().FindByID(gomock.Any(), "rocketchat_room", "r1").
				Return(map[string]any{"_id": "r1", "name": "general", "_updatedAt": int64(5)}, nil).AnyTimes()
			// A verbatim target projects nothing — it must fetch the whole document.
			tgt.EXPECT().FindOne(gomock.Any(), "rooms", map[string]any{"_id": "r1"}, gomock.Nil()).
				Return(tc.dest, nil).AnyTimes()

			v.Submit(CDCEvent{Collection: "rocketchat_room", Op: "insert", DocID: "r1"})
			r := waitState(t, results, "r1", tc.wantState)
			if tc.wantState == StateFailed {
				require.Len(t, r.Targets, 1)
				assert.Equal(t, "mismatch", r.Targets[0].LastCause)
				require.Len(t, r.Targets[0].Diffs, 1)
				assert.Equal(t, "general", r.Targets[0].Diffs[0].Want)
				assert.Equal(t, "random", r.Targets[0].Diffs[0].Got)
			}
		})
	}
}

// Resolver values are reachable from compared fields, not just keys: the check
// overlays each resolved doc under "@alias" so one getPath walk serves both.
func TestVerifier_ResolverPathInComparedFields(t *testing.T) {
	m := &Mapping{Sources: []SourceMapping{{
		Collection: "rocketchat_message",
		Ops:        map[string]OpAction{"insert": OpVerify},
		Resolvers: map[string]Resolver{
			// db=target also exercises resolving against the destination store.
			"user": {DB: "target", Collection: "users",
				Key: map[string]KeyFrom{"_id": {From: []string{"u._id"}}}, Fields: []string{"username"}},
		},
		Targets: map[string]Target{
			"byId": {Kind: "cassandra", Table: "messages_by_id",
				Key: map[string]KeyFrom{"message_id": {From: []string{"_id"}}}},
		},
		Fields: map[string][]DestRef{"@user.username": {{Dest: "byId.sender"}}},
	}}}

	v, src, tgt, cass, results := customVerifier(t, m,
		verifierConfig{Poll: time.Second, Timeout: 2 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").
		Return(map[string]any{"_id": "m1", "u": map[string]any{"_id": "u1"}}, nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), "users", map[string]any{"_id": "u1"}, []string{"username"}).
		Return(map[string]any{"username": "alice"}, nil).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), "messages_by_id", map[string]any{"message_id": "m1"},
		[]string{"message_id", "sender"}).Return(map[string]any{"sender": "alice"}, nil)

	v.Submit(insertEvent("m1"))
	r := waitState(t, results, "m1", StateMatched)
	assert.Equal(t, 1, r.Attempts)
}

func TestVerifier_DerivedAndKeyTransforms(t *testing.T) {
	ts := time.Unix(1700000000, 0).UTC()
	m := &Mapping{Sources: []SourceMapping{{
		Collection: "rocketchat_message",
		Ops:        map[string]OpAction{"insert": OpVerify},
		Targets: map[string]Target{
			"byRoom": {Kind: "cassandra", Table: "messages_by_room", Key: map[string]KeyFrom{
				"room_id":    {From: []string{"rid"}},
				"created_at": {From: []string{"ts"}, Transform: "unixMilli"},
			}},
		},
		Derived: []Derived{{From: []string{"ts"}, Transform: "unixMilli", Dest: []string{"byRoom.created_at"}}},
	}}}

	v, src, _, cass, results := customVerifier(t, m,
		verifierConfig{Poll: time.Second, Timeout: 2 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").
		Return(map[string]any{"_id": "m1", "rid": "r1", "ts": ts}, nil).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), "messages_by_room",
		map[string]any{"room_id": "r1", "created_at": ts.UnixMilli()},
		[]string{"created_at", "room_id"}).
		Return(map[string]any{"created_at": ts.UnixMilli()}, nil)

	v.Submit(insertEvent("m1"))
	r := waitState(t, results, "m1", StateMatched)
	assert.Equal(t, 1, r.Attempts)
}

func TestVerifier_KeyTransformFailureIsUnresolvable(t *testing.T) {
	m := &Mapping{Sources: []SourceMapping{{
		Collection: "rocketchat_message",
		Ops:        map[string]OpAction{"insert": OpVerify},
		Targets: map[string]Target{
			// ts is a string here, so unixMilli cannot coerce it.
			"byRoom": {Kind: "cassandra", Table: "messages_by_room",
				Key: map[string]KeyFrom{"created_at": {From: []string{"ts"}, Transform: "unixMilli"}}},
		},
	}}}

	v, src, _, _, results := customVerifier(t, m,
		verifierConfig{Poll: time.Second, Timeout: time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").
		Return(map[string]any{"_id": "m1", "ts": "not-a-time"}, nil).AnyTimes()

	v.Submit(insertEvent("m1"))
	r := waitState(t, results, "m1", StateFailed)
	require.Len(t, r.Targets, 1)
	assert.Equal(t, "key-unresolvable", r.Targets[0].LastCause)
}

func TestVerifier_ShutdownBoundedByContext(t *testing.T) {
	v, src, tgt, cass, _ := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: 60 * time.Second, MaxChecks: 4, SamplePercent: 100})
	src.EXPECT().FindByID(gomock.Any(), gomock.Any(), "m1").Return(srcDoc(), nil).AnyTimes()
	tgt.EXPECT().FindOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errNotFound).AnyTimes()
	cass.EXPECT().SelectOne(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errNotFound).AnyTimes()

	release := make(chan struct{})
	parked := make(chan struct{}, 1)
	v.sleep = func(_ context.Context, _ time.Duration) bool {
		select {
		case parked <- struct{}{}:
		default:
		}
		<-release
		return false
	}
	v.Submit(insertEvent("m1"))
	<-parked

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { v.Shutdown(expired); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not honour its context deadline")
	}

	close(release) // let the parked check finish, then drain it
	v.Shutdown(context.Background())
}

func TestSleepCtx(t *testing.T) {
	assert.True(t, sleepCtx(context.Background(), time.Millisecond), "timer fired")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, sleepCtx(ctx, time.Minute), "cancelled context returns immediately")
}

func TestVerifier_SubmitAfterShutdownIsDropped(t *testing.T) {
	// Mocks are wired with no expectations: a dropped check must not touch them.
	v, _, _, _, results := testVerifier(t, verifierConfig{Poll: time.Second, Timeout: time.Second, MaxChecks: 4, SamplePercent: 100})
	v.Shutdown(context.Background())

	v.Submit(insertEvent("m1"))
	assert.Empty(t, results.Recent(), "a check submitted after shutdown records nothing")

	// A second Shutdown is a no-op and must not block on the dropped check.
	v.Shutdown(context.Background())
}

// Verbatim targets must diff the source doc as stored, never the resolver
// overlay — a sibling target's resolver would inject phantom "@alias" diffs.
func overlayVerbatimMapping() *Mapping {
	return &Mapping{Sources: []SourceMapping{{
		Collection: "rocketchat_room",
		Ops:        map[string]OpAction{"insert": OpVerify},
		Resolvers: map[string]Resolver{
			"user": {DB: "source", Collection: "users",
				Key: map[string]KeyFrom{"_id": {From: []string{"u._id"}}}, Fields: []string{"username"}},
		},
		Targets: map[string]Target{
			// Plain key, no resolver reference — but it shares the attempt's view.
			"whole": {Kind: "mongo", Collection: "rooms", Mode: "verbatim",
				Key: map[string]KeyFrom{"_id": {From: []string{"_id"}}}},
			// Sibling whose key resolves the resolver, so the overlay is non-empty.
			"byUser": {Kind: "mongo", Collection: "userdocs",
				Key: map[string]KeyFrom{"_id": {From: []string{"@user.username"}}}},
		},
		Fields: map[string][]DestRef{"name": {{Dest: "byUser.name"}}},
	}}}
}

func TestVerifier_VerbatimIgnoresResolverOverlay(t *testing.T) {
	srcRoom := func() map[string]any {
		return map[string]any{"_id": "r1", "name": "general", "u": map[string]any{"_id": "u1"}}
	}

	// The sibling matches immediately, so the overlay only exists on attempt 1.
	t.Run("verbatim matches on the first attempt", func(t *testing.T) {
		v, src, tgt, _, results := customVerifier(t, overlayVerbatimMapping(),
			verifierConfig{Poll: time.Second, Timeout: 2 * time.Second, MaxChecks: 4, SamplePercent: 100})
		src.EXPECT().FindByID(gomock.Any(), "rocketchat_room", "r1").Return(srcRoom(), nil).AnyTimes()
		src.EXPECT().FindOne(gomock.Any(), "users", map[string]any{"_id": "u1"}, []string{"username"}).
			Return(map[string]any{"username": "alice"}, nil).AnyTimes()
		// The verbatim destination is byte-for-byte the source document.
		tgt.EXPECT().FindOne(gomock.Any(), "rooms", map[string]any{"_id": "r1"}, gomock.Nil()).
			Return(srcRoom(), nil).AnyTimes()
		tgt.EXPECT().FindOne(gomock.Any(), "userdocs", map[string]any{"_id": "alice"}, gomock.Any()).
			Return(map[string]any{"name": "general"}, nil).AnyTimes()

		v.Submit(CDCEvent{Collection: "rocketchat_room", Op: "insert", DocID: "r1"})
		r := waitState(t, results, "r1", StateMatched)
		assert.Equal(t, 1, r.Attempts, "the overlay must not delay the verbatim match")
	})

	// The sibling never freezes, so the resolver stays needed and the overlay is
	// rebuilt on every attempt — the verbatim target must still match, forever.
	t.Run("verbatim matches while the sibling keeps the overlay alive", func(t *testing.T) {
		v, src, tgt, _, results := customVerifier(t, overlayVerbatimMapping(),
			verifierConfig{Poll: time.Second, Timeout: 2 * time.Second, MaxChecks: 4, SamplePercent: 100})
		src.EXPECT().FindByID(gomock.Any(), "rocketchat_room", "r1").Return(srcRoom(), nil).AnyTimes()
		src.EXPECT().FindOne(gomock.Any(), "users", map[string]any{"_id": "u1"}, []string{"username"}).
			Return(map[string]any{"username": "alice"}, nil).AnyTimes()
		tgt.EXPECT().FindOne(gomock.Any(), "rooms", map[string]any{"_id": "r1"}, gomock.Nil()).
			Return(srcRoom(), nil).AnyTimes()
		tgt.EXPECT().FindOne(gomock.Any(), "userdocs", map[string]any{"_id": "alice"}, gomock.Any()).
			Return(nil, errNotFound).AnyTimes()

		v.Submit(CDCEvent{Collection: "rocketchat_room", Op: "insert", DocID: "r1"})
		r := waitState(t, results, "r1", StateFailed) // fails only on the sibling
		require.Len(t, r.Targets, 2)
		for i := range r.Targets {
			switch r.Targets[i].Alias {
			case "whole":
				assert.True(t, r.Targets[i].Matched, "verbatim must not diff the @alias overlay keys")
				assert.Empty(t, r.Targets[i].Diffs)
			case "byUser":
				assert.Equal(t, "dest-missing", r.Targets[i].LastCause)
			}
		}
	})
}

// Inspect fetches live source + dest docs on demand, independent of any check.
func TestVerifier_Inspect(t *testing.T) {
	ctrl := gomock.NewController(t)
	src := NewMockSourceStore(ctrl)
	tgt := NewMockTargetStore(ctrl)
	cass := NewMockCassStore(ctrl)
	results := newResultsStore(10, 10, nil)
	v := newVerifier(verifierMapping(), src, tgt, cass,
		newTransformRegistry(msgbucket.New(72*time.Hour)), results,
		verifierConfig{Poll: time.Second, Timeout: time.Minute, MaxChecks: 4, SamplePercent: 100})

	doc := srcDoc()
	src.EXPECT().FindByID(gomock.Any(), "rocketchat_message", "m1").Return(doc, nil)
	cass.EXPECT().SelectOne(gomock.Any(), "messages_by_id", map[string]any{"message_id": "m1"}, nil).
		Return(map[string]any{"message_id": "m1", "body": "hi"}, nil)
	tgt.EXPECT().FindOne(gomock.Any(), "rooms", map[string]any{"_id": "r1"}, nil).
		Return(nil, errNotFound)

	got, err := v.Inspect(context.Background(), "rocketchat_message", "m1")
	require.NoError(t, err)
	assert.Equal(t, doc, got.Source)
	assert.Empty(t, got.SourceErr)
	assert.Equal(t, []string{"_id", "msg", "rid"}, got.SourceFields)
	require.Len(t, got.Targets, 2) // sorted: byId, room
	assert.Equal(t, "byId", got.Targets[0].Alias)
	assert.Equal(t, "cassandra", got.Targets[0].Kind)
	assert.Equal(t, "messages_by_id", got.Targets[0].Dest)
	assert.Equal(t, "hi", got.Targets[0].Doc["body"])
	assert.Equal(t, []string{"message_id"}, got.Targets[0].KeyFields)
	assert.Equal(t, []string{"body"}, got.Targets[0].CompareFields)
	assert.False(t, got.Targets[0].Verbatim)
	assert.Equal(t, "room", got.Targets[1].Alias)
	assert.Equal(t, "not-found", got.Targets[1].Error)
	assert.Nil(t, got.Targets[1].Doc)
	assert.Equal(t, []string{"_id"}, got.Targets[1].KeyFields)
	assert.Equal(t, []string{"_id"}, got.Targets[1].CompareFields)
}

func TestVerifier_Inspect_SourceGone(t *testing.T) {
	ctrl := gomock.NewController(t)
	src := NewMockSourceStore(ctrl)
	tgt := NewMockTargetStore(ctrl)
	cass := NewMockCassStore(ctrl)
	results := newResultsStore(10, 10, nil)
	v := newVerifier(verifierMapping(), src, tgt, cass,
		newTransformRegistry(msgbucket.New(72*time.Hour)), results,
		verifierConfig{Poll: time.Second, Timeout: time.Minute, MaxChecks: 4, SamplePercent: 100})

	src.EXPECT().FindByID(gomock.Any(), "rocketchat_message", "gone").Return(nil, errNotFound)
	// _id-keyed target still inspectable via the documentKey fallback view
	cass.EXPECT().SelectOne(gomock.Any(), "messages_by_id", map[string]any{"message_id": "gone"}, nil).
		Return(map[string]any{"message_id": "gone"}, nil)
	// room target keys on rid, unresolvable without the source doc

	got, err := v.Inspect(context.Background(), "rocketchat_message", "gone")
	require.NoError(t, err)
	assert.Nil(t, got.Source)
	assert.Equal(t, "not-found", got.SourceErr)
	require.Len(t, got.Targets, 2)
	assert.NotNil(t, got.Targets[0].Doc)
	assert.Contains(t, got.Targets[1].Error, "key-unresolvable")
}

func TestVerifier_Inspect_UnmappedCollection(t *testing.T) {
	ctrl := gomock.NewController(t)
	results := newResultsStore(10, 10, nil)
	v := newVerifier(verifierMapping(), NewMockSourceStore(ctrl), NewMockTargetStore(ctrl), NewMockCassStore(ctrl),
		newTransformRegistry(msgbucket.New(72*time.Hour)), results,
		verifierConfig{Poll: time.Second, Timeout: time.Minute, MaxChecks: 4, SamplePercent: 100})
	_, err := v.Inspect(context.Background(), "nope", "x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmapped")
}
