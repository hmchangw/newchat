//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/minio/minio-go/v7"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

const (
	// segmentWait covers one 1s fill, the 5s ES refresh interval (forced away
	// by refreshing explicitly) and slow CI.
	segmentWait = 30 * time.Second
	pollEvery   = 250 * time.Millisecond
	// recoveryWait covers the redelivery after the bucket heals. The server
	// enforces BackOff[dc-1] as the redelivery deadline even on a NakWithDelay,
	// so the dominant term is that offset (BackOff[1] - AckWait, about 30s)
	// plus the jittered client delay (up to 5s), not the client schedule alone.
	recoveryWait = 90 * time.Second
)

// esHTTPClient is bounded so a stalled container cannot hang the suite.
var esHTTPClient = &http.Client{Timeout: 15 * time.Second}

// archiveEnv is one test's isolated archive deployment: its own site id (so
// the streams, indexes and DEK are its own), its own locked bucket and its own
// Vault transit key.
type archiveEnv struct {
	site   string
	esURL  string
	js     jetstream.JetStream
	engine searchengine.SearchEngine
	mc     *minio.Client
	bucket string
	sink   *bucketSink
	cipher *auditarchive.Cipher
	cfg    config
}

// siteFor derives a per-test site id. Stream names are <STREAM>-<siteID> and
// cannot be made unique any other way; the hash is lowercase hex so the id is
// also a valid index-name component.
func siteFor(t *testing.T) string {
	t.Helper()
	h := fnv.New64a()
	_, _ = h.Write([]byte(t.Name())) // hash.Hash.Write never errors
	return fmt.Sprintf("site-x-%x", h.Sum64())
}

// setupArchive wires everything main.run wires before the lanes start, in the
// same order and through the same constructors, against real containers.
func setupArchive(t *testing.T, eventTimes ...time.Time) *archiveEnv {
	t.Helper()
	ctx := context.Background()
	site := siteFor(t)
	esURL := testutil.Elasticsearch(t)

	nc, err := nats.Connect(testutil.NATS(t))
	require.NoError(t, err)
	// Drain's error is dropped: it only reports a connection that is already closed, and this is the last cleanup.
	t.Cleanup(func() { _ = nc.Drain() }) // runs last: streams and lanes are torn down while the connection is still up
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	// Streams: schema only (Name + Subjects), as bootstrapStreams does in dev.
	// INBOX belongs to inbox-worker in production, so the test creates it itself.
	streamNames := []string{stream.MessagesCanonical(site).Name, stream.Inbox(site).Name}
	for _, sc := range []stream.Config{stream.MessagesCanonical(site), stream.Inbox(site)} {
		_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: sc.Name, Subjects: sc.Subjects})
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, n := range streamNames {
			if err := js.DeleteStream(context.Background(), n); err != nil {
				t.Logf("delete stream %s: %v", n, err)
			}
		}
	})

	engine, err := searchengine.New(ctx, searchengine.Config{Backend: "elasticsearch", URL: esURL})
	require.NoError(t, err)
	// Daily event indexes derive from the fixture timestamps, never time.Now().
	indexes := []string{auditarchive.KeysIndex(site)}
	for _, at := range eventTimes {
		indexes = append(indexes, auditarchive.EventsIndex(site, at), auditarchive.MembersIndex(site, at))
	}
	t.Cleanup(func() {
		for _, idx := range indexes {
			deleteIndex(t, esURL, idx)
		}
	})
	require.NoError(t, bootstrapIndex(ctx, engine, site, "30d", true))

	mc, bucket := lockedBucket(t)
	require.NoError(t, checkObjectLock(ctx, mc, bucket, true))

	v := testutil.Vault(t, ctx)
	wrapper, err := atrest.NewVaultKeyWrapper(ctx, atrest.VaultConfig{Address: v.Address, TransitMount: v.TransitMount, TransitKey: v.TransitKey, Token: v.Token})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wrapper.Close() }) // Close only releases the Vault token renewer; a failure leaves nothing to recover
	dek, err := loadOrCreateDEK(ctx, engine, wrapper, site, time.Now)
	require.NoError(t, err)
	cipher, err := auditarchive.NewCipher(dek)
	require.NoError(t, err)

	// Production defaults for the consumer, with the 1-second fill the scenario needs.
	// The CONSUMER_ prefix is the one production reads, so a runner exporting a
	// bare ACK_WAIT or MAX_DELIVER cannot alter the consumer under test.
	consumer, err := env.ParseAsWithOptions[stream.ConsumerSettings](env.Options{Prefix: "CONSUMER_"})
	require.NoError(t, err)
	cfg := config{
		SiteID: site, FillInterval: time.Second, BatchEvents: 2000, BatchBytes: 8 << 20, FetchBatch: 100,
		// 5s timeouts keep fill + attempts x (put + bulk) under AckWait, as validate requires.
		PutTimeout: 5 * time.Second, BulkTimeout: 5 * time.Second, WriteAttempts: 2, Consumer: consumer,
		IndexRetention: "30d", BlobWorkers: 1, Vault: atrest.VaultConfig{Address: v.Address},
	}
	require.NoError(t, cfg.validate(), "the test config must be one the service accepts")
	return &archiveEnv{site: site, esURL: esURL, js: js, engine: engine, mc: mc, bucket: bucket, sink: newBucketSink(mc, bucket), cipher: cipher, cfg: cfg}
}

type laneSpec struct {
	name       string // "events" or "members": the lane label in segment keys
	durable    string
	streamName string
	filters    []string
	build      builder
	objects    objectStore
	index      indexStore
}

// runningLane is a started lane and its durable's consumer handle.
type runningLane struct {
	cons       jetstream.Consumer
	stop       func()
	unexpected *atomic.Bool
}

// startLane creates the durable exactly as main's mkConsumer does (rawConsumerAdapter
// in place of the o11y adapter) and starts the lane through the same
// newLane / newBatcher / newFlusher constructors. It registers a cleanup that
// stops the lane before the stream it consumes from is deleted.
func (e *archiveEnv) startLane(t *testing.T, s *laneSpec) *runningLane {
	t.Helper()
	ctx := context.Background()
	cc := consumerConfig(s.durable, s.filters, laneConsumerSettings(&e.cfg))
	cons, err := e.js.CreateOrUpdateConsumer(ctx, s.streamName, cc)
	require.NoError(t, err)

	unexpected := &atomic.Bool{}
	guard := loopguard.New(s.name+"-lane", func() { unexpected.Store(true) })
	flushCfg := flushConfig{putTimeout: e.cfg.PutTimeout, bulkTimeout: e.cfg.BulkTimeout, attempts: e.cfg.WriteAttempts}
	l := newLane(newLaneConfig(&e.cfg, s.name, nil), rawConsumerAdapter{c: cons}, s.build, e.cipher,
		newBatcher(e.cfg.BatchEvents, e.cfg.BatchBytes, e.cfg.FillInterval), newFlusher(s.objects, s.index, flushCfg, nil), guard)

	g := newLaneGroup()
	g.start(ctx, l.run)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			guard.BeginShutdown()
			g.stopAll()
			wctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			assert.NoError(t, g.wait(wctx), "lane %s did not drain", s.name)
			assert.False(t, unexpected.Load(), "lane %s reported an unexpected stop", s.name)
		})
	}
	t.Cleanup(stop)
	return &runningLane{cons: cons, stop: stop, unexpected: unexpected}
}

func (e *archiveEnv) eventsSpec(durable string, objects objectStore, index indexStore) *laneSpec {
	return &laneSpec{
		name: "events", durable: durable, streamName: stream.MessagesCanonical(e.site).Name,
		filters: []string{subject.MsgCanonicalMessageWildcard(e.site)}, build: buildEventItem, objects: objects, index: index,
	}
}

func (e *archiveEnv) membersSpec(durable string, objects objectStore, index indexStore) *laneSpec {
	return &laneSpec{
		name: "members", durable: durable, streamName: stream.Inbox(e.site).Name,
		filters: subject.InboxMemberEventSubjects(e.site), build: buildMemberItem, objects: objects, index: index,
	}
}

// bulkStatusRecorder remembers every bulk result so a test can assert on the
// per-document statuses (the flusher does not expose them).
type bulkStatusRecorder struct {
	indexStore
	mu      sync.Mutex
	results []searchengine.BulkResult
}

func (r *bulkStatusRecorder) Bulk(ctx context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
	res, err := r.indexStore.Bulk(ctx, actions)
	if err == nil {
		r.mu.Lock()
		r.results = append(r.results, res...)
		r.mu.Unlock()
	}
	return res, err
}

func (r *bulkStatusRecorder) statuses() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, len(r.results))
	for i, res := range r.results {
		out[i] = res.Status
	}
	return out
}

// failingThen is the bucket-outage decorator: it fails the first N Put calls
// and delegates every call after that to the real sink.
type failingThen struct {
	next      objectStore
	remaining atomic.Int64
	failed    atomic.Int64
}

func newFailingThen(next objectStore, failFirst int64) *failingThen {
	f := &failingThen{next: next}
	f.remaining.Store(failFirst)
	return f
}

// heal makes every later Put reach the real sink.
func (f *failingThen) heal() { f.remaining.Store(0) }

func (f *failingThen) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string) error {
	for {
		n := f.remaining.Load()
		if n <= 0 {
			return f.next.Put(ctx, key, body, size, contentType)
		}
		if f.remaining.CompareAndSwap(n, n-1) {
			f.failed.Add(1)
			return fmt.Errorf("injected bucket outage (%d failures left)", n-1)
		}
	}
}

// publishJSON publishes v on subject and returns the stream sequence the
// server assigned, which is also the archive's document-id sequence.
func publishJSON(t *testing.T, js jetstream.JetStream, subj string, v any) uint64 {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	ack, err := js.Publish(context.Background(), subj, data)
	require.NoError(t, err)
	return ack.Sequence
}

func esURLFor(t *testing.T, base string, segments ...string) string {
	t.Helper()
	u, err := url.Parse(base)
	require.NoError(t, err)
	return u.JoinPath(segments...).String()
}

// esRequest is the error-returning request helper; polling conditions use it
// directly, so a failed call never reaches a require from a non-test goroutine.
func esRequest(method, rawURL string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(context.Background(), method, rawURL, bytes.NewReader(body))
	if err != nil {
		return 0, nil, fmt.Errorf("build %s request: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := esHTTPClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%s %s: %w", method, rawURL, err)
	}
	defer resp.Body.Close() // read side: the body is fully consumed below, a close error is moot
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, fmt.Errorf("read %s response: %w", method, err)
	}
	return resp.StatusCode, out, nil
}

func esDo(t *testing.T, method, rawURL string, body []byte) (int, []byte) {
	t.Helper()
	status, out, err := esRequest(method, rawURL, body)
	require.NoError(t, err)
	return status, out
}

// deleteIndex removes one index by exact name (ES refuses wildcard deletes by
// default). Best effort: a missing index is the normal case for a lane that
// never wrote.
func deleteIndex(t *testing.T, esURL, index string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, esURLFor(t, esURL, index), nil)
	if err != nil {
		t.Logf("delete index %s: %v", index, err)
		return
	}
	resp, err := esHTTPClient.Do(req)
	if err != nil {
		t.Logf("delete index %s: %v", index, err)
		return
	}
	_ = resp.Body.Close() // best-effort cleanup: the response body carries nothing we need
}

// docCount refreshes index first (engine.Search does not, and the template's
// refresh_interval is 5s), then counts every document in it. A missing index
// counts as zero. It returns errors rather than failing so a polling condition
// can report them from the test goroutine.
func docCount(engine searchengine.SearchEngine, esURL, index string) (int, error) {
	u, err := url.Parse(esURL)
	if err != nil {
		return 0, fmt.Errorf("parse es url: %w", err)
	}
	status, body, err := esRequest(http.MethodPost, u.JoinPath(index, "_refresh").String(), nil)
	if err != nil {
		return 0, fmt.Errorf("refresh %s: %w", index, err)
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return 0, fmt.Errorf("refresh %s: status %d: %s", index, status, body)
	}
	raw, err := engine.Search(context.Background(), []string{index}, json.RawMessage(`{"query":{"match_all":{}},"size":0,"track_total_hits":true}`))
	if err != nil {
		return 0, fmt.Errorf("search %s: %w", index, err)
	}
	var resp struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return 0, fmt.Errorf("decode search %s: %w", index, err)
	}
	return resp.Hits.Total.Value, nil
}

func countDocs(t *testing.T, engine searchengine.SearchEngine, esURL, index string) int {
	t.Helper()
	n, err := docCount(engine, esURL, index)
	require.NoError(t, err)
	return n
}

// waitFor polls cond on the test goroutine until it reports true or timeout
// passes. A condition error fails the test at once instead of burning the
// budget, and nothing runs off the test goroutine, so there is no require
// outside it and no log after the test returns.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() (bool, error)) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()
	for {
		ok, err := cond()
		require.NoError(t, err, what)
		if ok {
			return
		}
		select {
		case <-deadline.C:
			require.FailNow(t, "timed out", "%s (after %s)", what, timeout)
		case <-tick.C:
		}
	}
}

// getEventDoc and getMemberDoc read one document by id; GetDoc is realtime,
// so no refresh is needed.
func getEventDoc(t *testing.T, e *archiveEnv, index string, seq uint64) auditarchive.EventDoc {
	t.Helper()
	raw, found, err := e.engine.GetDoc(context.Background(), index, auditarchive.EventDocID(e.site, seq))
	require.NoError(t, err)
	require.True(t, found, "event doc for seq %d in %s", seq, index)
	var hit struct {
		Source auditarchive.EventDoc `json:"_source"`
	}
	require.NoError(t, json.Unmarshal(raw, &hit))
	return hit.Source
}

func getMemberDoc(t *testing.T, e *archiveEnv, index string, seq uint64, i int) auditarchive.MemberDoc {
	t.Helper()
	raw, found, err := e.engine.GetDoc(context.Background(), index, auditarchive.MemberDocID(e.site, seq, i))
	require.NoError(t, err)
	require.True(t, found, "member doc for seq %d/%d in %s", seq, i, index)
	var hit struct {
		Source auditarchive.MemberDoc `json:"_source"`
	}
	require.NoError(t, json.Unmarshal(raw, &hit))
	return hit.Source
}

// listKeys returns the object keys under prefix (current versions only).
func listKeys(t *testing.T, client *minio.Client, bucket, prefix string) []string {
	t.Helper()
	var keys []string
	for obj := range client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		require.NoError(t, obj.Err)
		keys = append(keys, obj.Key)
	}
	return keys
}

// listLaneKeys narrows listKeys to one lane's segments ("events"/"members").
func listLaneKeys(t *testing.T, e *archiveEnv, lane string) []string {
	t.Helper()
	var out []string
	for _, k := range listKeys(t, e.mc, e.bucket, e.site+"/") {
		if strings.HasPrefix(path.Base(k), lane+"-") && strings.HasSuffix(k, ".seg") {
			out = append(out, k)
		}
	}
	return out
}

// countLaneVersions counts stored object versions (delete markers excluded) of
// one lane's segments; a second PUT of an existing key adds a version.
func countLaneVersions(t *testing.T, e *archiveEnv, lane string) int {
	t.Helper()
	n := 0
	for obj := range e.mc.ListObjects(context.Background(), e.bucket, minio.ListObjectsOptions{Prefix: e.site + "/", Recursive: true, WithVersions: true}) {
		require.NoError(t, obj.Err)
		if !obj.IsDeleteMarker && strings.HasPrefix(path.Base(obj.Key), lane+"-") && strings.HasSuffix(obj.Key, ".seg") {
			n++
		}
	}
	return n
}

// openFrame resolves a document's location the way an auditor would: range-read
// the frame at frameOffset from the segment object, open it with the DEK, and
// check the keyed digest against the document's contentHash.
func openFrame(t *testing.T, e *archiveEnv, segmentKey string, frameOffset int64, seq uint64, contentHash string) (auditarchive.Record, []byte) {
	t.Helper()
	obj, err := e.mc.GetObject(context.Background(), e.bucket, segmentKey, minio.GetObjectOptions{})
	require.NoError(t, err)
	defer obj.Close()
	frame, err := auditarchive.ReadFrameAt(obj, frameOffset)
	require.NoError(t, err, "ReadFrameAt %s@%d", segmentKey, frameOffset)
	plain, err := e.cipher.Open(frame, auditarchive.FrameAAD(e.site, seq))
	require.NoError(t, err, "frame for seq %d must open with the DEK", seq)
	assert.Equal(t, contentHash, e.cipher.Digest(plain), "frame for seq %d must hash to its document's contentHash", seq)
	var rec auditarchive.Record
	require.NoError(t, json.Unmarshal(plain, &rec))
	return rec, frame
}

// readSegmentFrames fetches a whole segment and parses it with ReadSegment.
func readSegmentFrames(t *testing.T, e *archiveEnv, key string) (auditarchive.Header, [][]byte) {
	t.Helper()
	obj, err := e.mc.GetObject(context.Background(), e.bucket, key, minio.GetObjectOptions{})
	require.NoError(t, err)
	defer obj.Close()
	h, frames, err := auditarchive.ReadSegment(obj)
	require.NoError(t, err, "ReadSegment %s", key)
	return h, frames
}

func consumerState(c jetstream.Consumer) (*jetstream.ConsumerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := c.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("consumer info: %w", err)
	}
	return info, nil
}

func consumerInfo(t *testing.T, c jetstream.Consumer) *jetstream.ConsumerInfo {
	t.Helper()
	info, err := consumerState(c)
	require.NoError(t, err)
	return info
}

// drained reports that the durable has delivered and had acked everything.
func drained(c jetstream.Consumer) (bool, error) {
	info, err := consumerState(c)
	if err != nil {
		return false, err
	}
	return info.NumAckPending == 0 && info.NumPending == 0, nil
}

// canonicalEvent builds a canonical message event whose timestamp (and so its
// daily index) is fixed by the fixture, not the wall clock.
func canonicalEvent(site string, ev model.EventType, id string, ts time.Time, mutate func(*model.MessageEvent)) model.MessageEvent {
	e := model.MessageEvent{
		Event: ev,
		Message: model.Message{
			ID: id, RoomID: "room-1", UserID: "u-alice", UserAccount: "alice",
			Content: "hello " + id, CreatedAt: ts,
		},
		SiteID: site, Timestamp: ts.UnixMilli(),
	}
	if mutate != nil {
		mutate(&e)
	}
	return e
}

func memberAddedEvent(t *testing.T, site string, ts time.Time) model.InboxEvent {
	t.Helper()
	inner, err := json.Marshal(model.InboxMemberEvent{
		RoomID: "room-1", RoomName: "general", RoomType: model.RoomTypeChannel, SiteID: site,
		Accounts: []string{"bob", "carol"}, Timestamp: ts.UnixMilli(),
	})
	require.NoError(t, err)
	return model.InboxEvent{Type: model.InboxMemberAdded, SiteID: site, DestSiteID: site, Payload: inner, Timestamp: ts.UnixMilli()}
}

func TestArchiveWorker_EndToEnd(t *testing.T) {
	// Fixture time: fixed so the expected daily indexes never depend on time.Now.
	base := time.Date(2026, 3, 14, 9, 26, 53, 0, time.UTC)
	createdAt, updatedAt, deletedAt := base, base.Add(time.Second), base.Add(2*time.Second)
	memberAt := base.Add(3 * time.Second)

	e := setupArchive(t, createdAt, memberAt)
	ctx := context.Background()
	eventsIndex := auditarchive.EventsIndex(e.site, createdAt)
	membersIndex := auditarchive.MembersIndex(e.site, memberAt)
	require.Equal(t, eventsIndex, auditarchive.EventsIndex(e.site, deletedAt), "fixture events must share one daily index")

	// Publish before the lanes start, so the three events always land in one
	// batch and "exactly one segment" cannot be split by a fill-interval tick.
	att, err := json.Marshal(cassandra.Attachment{ID: "att-1", Title: "report.pdf", Type: "file", FileType: "application/pdf", TitleLink: "/files/att-1"})
	require.NoError(t, err)
	evSeqs := []uint64{
		publishJSON(t, e.js, subject.MsgCanonicalCreated(e.site), canonicalEvent(e.site, model.EventCreated, "msg-1", createdAt, func(ev *model.MessageEvent) {
			ev.Message.Attachments = [][]byte{att}
		})),
		publishJSON(t, e.js, subject.MsgCanonicalUpdated(e.site), canonicalEvent(e.site, model.EventUpdated, "msg-1", updatedAt, func(ev *model.MessageEvent) {
			ev.Message.Content = "hello msg-1 (edited)"
			ev.Message.EditedAt = &updatedAt
		})),
		publishJSON(t, e.js, subject.MsgCanonicalDeleted(e.site), canonicalEvent(e.site, model.EventDeleted, "msg-1", deletedAt, nil)),
	}
	memberSeq := publishJSON(t, e.js, subject.InboxInternal(e.site, "member_added"), memberAddedEvent(t, e.site, memberAt))
	require.Equal(t, []uint64{1, 2, 3}, evSeqs, "a fresh stream numbers the events 1..3")
	require.Equal(t, uint64(1), memberSeq)

	eventsIdx := &bulkStatusRecorder{indexStore: e.engine}
	membersIdx := &bulkStatusRecorder{indexStore: e.engine}
	eventsLane := e.startLane(t, e.eventsSpec(eventsDurable, e.sink, eventsIdx))
	membersLane := e.startLane(t, e.membersSpec(membersDurable, e.sink, membersIdx))

	// Both lanes sealed and wrote: segments in the bucket, documents indexed.
	waitFor(t, segmentWait, "events and members documents must be indexed", func() (bool, error) {
		ev, err := docCount(e.engine, e.esURL, eventsIndex)
		if err != nil {
			return false, err
		}
		mem, err := docCount(e.engine, e.esURL, membersIndex)
		return ev == 3 && mem == 2, err
	})

	t.Run("one segment per lane that parses and opens with the DEK", func(t *testing.T) {
		evKeys, memKeys := listLaneKeys(t, e, "events"), listLaneKeys(t, e, "members")
		require.Len(t, evKeys, 1, "exactly one events segment: %v", evKeys)
		require.Len(t, memKeys, 1, "exactly one members segment: %v", memKeys)

		h, frames := readSegmentFrames(t, e, evKeys[0])
		assert.Equal(t, e.site, h.Site)
		assert.Equal(t, "events", h.Lane)
		assert.Equal(t, uint64(1), h.FirstSeq)
		assert.Equal(t, uint64(3), h.LastSeq)
		require.Len(t, frames, 3)
		for i, frame := range frames {
			seq := evSeqs[i]
			doc := getEventDoc(t, e, eventsIndex, seq)
			plain, err := e.cipher.Open(frame, auditarchive.FrameAAD(e.site, seq))
			require.NoError(t, err, "events frame %d must open with the DEK", i)
			assert.Equal(t, doc.ContentHash, e.cipher.Digest(plain), "events frame %d must hash to its document's contentHash", i)
			var rec auditarchive.Record
			require.NoError(t, json.Unmarshal(plain, &rec))
			assert.Equal(t, seq, rec.Seq)
			assert.Equal(t, e.site, rec.Site)
		}

		mh, mframes := readSegmentFrames(t, e, memKeys[0])
		assert.Equal(t, "members", mh.Lane)
		assert.Equal(t, memberSeq, mh.FirstSeq)
		require.Len(t, mframes, 1)
		plain, err := e.cipher.Open(mframes[0], auditarchive.FrameAAD(e.site, memberSeq))
		require.NoError(t, err, "members frame must open with the DEK")
		for i := 0; i < 2; i++ {
			assert.Equal(t, getMemberDoc(t, e, membersIndex, memberSeq, i).ContentHash, e.cipher.Digest(plain), "both account documents carry the one frame's digest")
		}
	})

	t.Run("documents are keyed by sequence and resolve to their frame", func(t *testing.T) {
		evKey, memKey := listLaneKeys(t, e, "events")[0], listLaneKeys(t, e, "members")[0]
		_, evFrames := readSegmentFrames(t, e, evKey)
		wantTypes := []string{"created", "updated", "deleted"}
		for i, seq := range evSeqs {
			doc := getEventDoc(t, e, eventsIndex, seq)
			assert.Equal(t, seq, doc.Seq)
			assert.Equal(t, wantTypes[i], doc.EventType)
			assert.Equal(t, "msg-1", doc.MessageID)
			assert.Equal(t, e.site, doc.SiteID)
			assert.Equal(t, evKey, doc.SegmentKey)
			rec, frame := openFrame(t, e, doc.SegmentKey, doc.FrameOffset, seq, doc.ContentHash)
			assert.Equal(t, seq, rec.Seq, "frameOffset must land on this document's own frame")
			assert.Equal(t, evFrames[i], frame, "ReadFrameAt must return the frame ReadSegment returns at that position")
		}
		created := getEventDoc(t, e, eventsIndex, evSeqs[0])
		assert.Equal(t, 1, created.AttachmentCount)
		assert.Equal(t, []string{"application/pdf"}, created.AttachmentTypes)
		require.NotEmpty(t, created.EncBody)
		body, err := e.cipher.Open(created.EncBody, auditarchive.BodyAAD(e.site, evSeqs[0]))
		require.NoError(t, err)
		var eb eventBody
		require.NoError(t, json.Unmarshal(body, &eb))
		assert.Equal(t, "hello msg-1", eb.Content)
		assert.Empty(t, getEventDoc(t, e, eventsIndex, evSeqs[2]).EncBody, "a delete carries no body")

		_, memFrames := readSegmentFrames(t, e, memKey)
		for i, acct := range []string{"bob", "carol"} {
			doc := getMemberDoc(t, e, membersIndex, memberSeq, i)
			assert.Equal(t, acct, doc.Account)
			assert.Equal(t, "member_added", doc.EventType)
			assert.Equal(t, "room-1", doc.RoomID)
			assert.Equal(t, memKey, doc.SegmentKey)
			rec, frame := openFrame(t, e, doc.SegmentKey, doc.FrameOffset, memberSeq, doc.ContentHash)
			assert.Equal(t, memberSeq, rec.Seq)
			assert.Equal(t, memFrames[0], frame)
		}
		assert.Equal(t, 3, countDocs(t, e.engine, e.esURL, eventsIndex), "three event documents")
		assert.Equal(t, 2, countDocs(t, e.engine, e.esURL, membersIndex), "two member documents")
	})

	t.Run("everything is acked", func(t *testing.T) {
		for name, c := range map[string]jetstream.Consumer{"events": eventsLane.cons, "members": membersLane.cons} {
			waitFor(t, segmentWait, name+" durable must end with NumAckPending=0 and NumPending=0", func() (bool, error) { return drained(c) })
		}
		for _, s := range append(eventsIdx.statuses(), membersIdx.statuses()...) {
			assert.Equal(t, http.StatusCreated, s, "the first write of every document is a create")
		}
	})

	t.Run("redelivery writes a second segment and creates no documents", func(t *testing.T) {
		require.Equal(t, 1, countLaneVersions(t, e, "events"), "precondition: one stored events segment")

		// A fresh durable on the same stream (DeliverAll) is the redelivery: the
		// server hands back the same three messages with the same stream
		// sequences, so every document id already exists.
		replayIdx := &bulkStatusRecorder{indexStore: e.engine}
		replay := e.startLane(t, e.eventsSpec(eventsDurable+"-replay", e.sink, replayIdx))

		waitFor(t, segmentWait, "the replay must attempt all three creates", func() (bool, error) { return len(replayIdx.statuses()) == 3, nil })
		assert.Equal(t, []int{http.StatusConflict, http.StatusConflict, http.StatusConflict}, replayIdx.statuses(), "every replayed create is refused with 409")
		waitFor(t, segmentWait, "409s count as archived, so the replay acks everything", func() (bool, error) { return drained(replay.cons) })

		// Same site, lane, hour and sequence range give the same key, so the
		// second segment is a second stored version of it (Object Lock retains
		// both); across an hour boundary it is a second key. Either way: two.
		assert.Equal(t, 2, countLaneVersions(t, e, "events"), "the redelivery must write a second segment")
		assert.Equal(t, 3, countDocs(t, e.engine, e.esURL, eventsIndex), "no new event documents")
		replay.stop()
	})

	t.Run("a create_doc-only role is refused writes and deletes", func(t *testing.T) {
		// The test cluster runs with xpack.security.enabled=false (the role API
		// is then unavailable), and a secured cluster refuses the unauthenticated
		// probe: either way this cannot be exercised, and the status says which.
		status, _ := esDo(t, http.MethodGet, esURLFor(t, e.esURL, "_security", "role"), nil)
		if status != http.StatusOK {
			t.Skipf("security API unavailable in the test cluster (status %d)", status)
		}
		role := `{"indices":[{"names":["audit-*"],"privileges":["create_doc","auto_configure"]}]}`
		status, body := esDo(t, http.MethodPut, esURLFor(t, e.esURL, "_security", "role", "audit-writer"), []byte(role))
		require.Equal(t, http.StatusOK, status, string(body))
		user := `{"password":"audit-writer-pass-1","roles":["audit-writer"]}`
		status, body = esDo(t, http.MethodPut, esURLFor(t, e.esURL, "_security", "user", "audit-writer-test"), []byte(user))
		require.Equal(t, http.StatusOK, status, string(body))

		doAs := func(method, rawURL string, payload []byte) int {
			req, err := http.NewRequestWithContext(ctx, method, rawURL, bytes.NewReader(payload))
			require.NoError(t, err)
			req.SetBasicAuth("audit-writer-test", "audit-writer-pass-1")
			req.Header.Set("Content-Type", "application/json")
			resp, err := esHTTPClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close() // the status is all this helper returns; a close error is moot
			return resp.StatusCode
		}
		fresh := auditarchive.EventDocID(e.site, 9001)
		assert.Equal(t, http.StatusCreated, doAs(http.MethodPut, esURLFor(t, e.esURL, eventsIndex, "_create", fresh), []byte(`{"seq":9001}`)), "op_type create is allowed")
		assert.Equal(t, http.StatusForbidden, doAs(http.MethodPut, esURLFor(t, e.esURL, eventsIndex, "_doc", fresh), []byte(`{"seq":9001}`)), "overwriting a document is refused")
		assert.Equal(t, http.StatusForbidden, doAs(http.MethodDelete, esURLFor(t, e.esURL, eventsIndex, "_doc", fresh), nil), "deleting a document is refused")
	})
}

func TestArchiveWorker_BucketOutage(t *testing.T) {
	at := time.Date(2026, 3, 14, 11, 0, 0, 0, time.UTC)
	e := setupArchive(t, at)
	eventsIndex := auditarchive.EventsIndex(e.site, at)

	bucket := newFailingThen(e.sink, 100)
	seq := publishJSON(t, e.js, subject.MsgCanonicalCreated(e.site), canonicalEvent(e.site, model.EventCreated, "msg-outage", at, nil))
	lane := e.startLane(t, e.eventsSpec(eventsDurable, bucket, e.engine))

	// The segment PUT fails after its attempts and the batch is NAKed; the
	// server redelivers it. Nothing was written, so nothing may be acked.
	waitFor(t, segmentWait, "the NAKed event must be redelivered", func() (bool, error) {
		info, err := consumerState(lane.cons)
		if err != nil {
			return false, err
		}
		return info.NumRedelivered >= 1, nil
	})
	info := consumerInfo(t, lane.cons)
	assert.GreaterOrEqual(t, bucket.failed.Load(), int64(1), "the decorator must have refused at least one PUT")
	assert.Zero(t, info.AckFloor.Stream, "an event whose segment was not stored must not be acked")
	assert.Equal(t, 0, countDocs(t, e.engine, e.esURL, eventsIndex), "no document without its segment")
	assert.Empty(t, listLaneKeys(t, e, "events"), "no segment reached the bucket")

	// The bucket recovers: the next delivery stores the segment, indexes the
	// document and acks.
	bucket.heal()
	waitFor(t, recoveryWait, "the document must appear once the bucket recovers", func() (bool, error) {
		n, err := docCount(e.engine, e.esURL, eventsIndex)
		return n == 1, err
	})
	waitFor(t, segmentWait, "the recovered event must be acked", func() (bool, error) { return drained(lane.cons) })
	assert.Len(t, listLaneKeys(t, e, "events"), 1, "exactly one segment after recovery")
	doc := getEventDoc(t, e, eventsIndex, seq)
	rec, _ := openFrame(t, e, doc.SegmentKey, doc.FrameOffset, seq, doc.ContentHash)
	assert.Equal(t, seq, rec.Seq)
}
