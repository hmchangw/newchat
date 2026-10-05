package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/drive"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
)

type fakeSource struct {
	data        map[string][]byte
	err         error
	unknownSize bool
	// errFor fails only the named attachment IDs, so a multi-attachment
	// message can have one transient failure among successes.
	errFor map[string]error
}

func (s *fakeSource) Open(_ context.Context, _ string, att cassandra.Attachment) (io.ReadCloser, int64, string, error) { //nolint:gocritic // hugeParam: signature fixed by the blobSource interface
	if s.err != nil {
		return nil, 0, "", s.err
	}
	if err, ok := s.errFor[att.ID]; ok {
		return nil, 0, "", err
	}
	b, ok := s.data[att.ID]
	if !ok {
		return nil, 0, "", errBlobMissing
	}
	size := int64(len(b))
	if s.unknownSize {
		size = -1
	}
	return io.NopCloser(bytes.NewReader(b)), size, att.FileType, nil
}

// recordingIndex is shared by lane workers, so docs is guarded.
type recordingIndex struct {
	fakeIndex
	mu   sync.Mutex
	docs []searchengine.BulkAction
}

func (r *recordingIndex) Bulk(ctx context.Context, a []searchengine.BulkAction) ([]searchengine.BulkResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.docs = append(r.docs, a...)
	return r.fakeIndex.Bulk(ctx, a)
}

func (r *recordingIndex) docAt(t *testing.T, i int) auditarchive.BlobDoc {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Greater(t, len(r.docs), i)
	var d auditarchive.BlobDoc
	require.NoError(t, json.Unmarshal(r.docs[i].Doc, &d))
	return d
}

func plainDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func TestDriveHostOf(t *testing.T) {
	tests := []struct {
		name       string
		link       string
		wantHost   string
		wantLegacy bool
		wantErr    bool
	}{
		{name: "drive link", link: "api/v1/file/rooms/r1/file/f1?drive_host=https://drive.example", wantHost: "https://drive.example"},
		{name: "legacy minio link", link: "api/v1/file-upload/f1/mock.png", wantLegacy: true},
		{name: "legacy minio link with leading slash", link: "/api/v1/file-upload/f1/mock.png", wantLegacy: true},
		{name: "missing drive_host", link: "api/v1/file/rooms/r1/file/f1", wantErr: true},
		{name: "empty link", link: "", wantErr: true},
		{name: "unparseable link", link: "http://[::1", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, legacy, err := driveHostOf(tt.link)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantHost, host)
			assert.Equal(t, tt.wantLegacy, legacy)
		})
	}
}

// driveFixture serves the two Drive endpoints GetGroupImage calls: the signer
// and the presigned download.
func driveFixture(t *testing.T, signer, download http.HandlerFunc) (*driveSource, string) {
	t.Helper()
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/api/v1/groups/{groupId}/files/{fileId}", signer)
	mux.HandleFunc("/download/", download)
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &driveSource{client: drive.NewClient(&drive.Config{URL: srv.URL, Token: "tok"})}, srv.URL
}

func TestDriveSource_Open(t *testing.T) {
	att := cassandra.Attachment{ID: "f1", Title: "mock.png", FileType: "image/png"}
	signerOK := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"url": "http://" + r.Host + "/download/f1"})
	}
	downloadOK := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("png bytes"))
	}

	t.Run("fetches through the signer", func(t *testing.T) {
		src, host := driveFixture(t, signerOK, downloadOK)
		a := att
		a.TitleLink = "api/v1/file/rooms/r1/file/f1?drive_host=" + host
		body, size, ctype, err := src.Open(context.Background(), "r1", a)
		require.NoError(t, err)
		defer body.Close()
		got, err := io.ReadAll(body)
		require.NoError(t, err)
		assert.Equal(t, "png bytes", string(got))
		assert.Equal(t, int64(len(got)), size)
		assert.Equal(t, "image/png", ctype)
	})
	t.Run("legacy link is never fetched", func(t *testing.T) {
		src, _ := driveFixture(t, func(http.ResponseWriter, *http.Request) { t.Error("legacy link must not reach Drive") }, downloadOK)
		a := att
		a.TitleLink = "api/v1/file-upload/f1/mock.png"
		_, _, _, err := src.Open(context.Background(), "r1", a)
		assert.ErrorIs(t, err, errBlobLegacy)
	})
	t.Run("link without a host is missing", func(t *testing.T) {
		src, _ := driveFixture(t, signerOK, downloadOK)
		a := att
		a.TitleLink = "api/v1/file/rooms/r1/file/f1"
		_, _, _, err := src.Open(context.Background(), "r1", a)
		assert.ErrorIs(t, err, errBlobMissing)
	})
	t.Run("host outside the allow-list is missing", func(t *testing.T) {
		src, _ := driveFixture(t, signerOK, downloadOK)
		a := att
		a.TitleLink = "api/v1/file/rooms/r1/file/f1?drive_host=https://elsewhere.example"
		_, _, _, err := src.Open(context.Background(), "r1", a)
		assert.ErrorIs(t, err, errBlobMissing)
	})
	t.Run("deleted at the signer is missing", func(t *testing.T) {
		src, host := driveFixture(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "no such file"})
		}, downloadOK)
		a := att
		a.TitleLink = "api/v1/file/rooms/r1/file/f1?drive_host=" + host
		_, _, _, err := src.Open(context.Background(), "r1", a)
		assert.ErrorIs(t, err, errBlobMissing)
	})
	t.Run("deleted in storage is missing", func(t *testing.T) {
		src, host := driveFixture(t, signerOK, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) })
		a := att
		a.TitleLink = "api/v1/file/rooms/r1/file/f1?drive_host=" + host
		_, _, _, err := src.Open(context.Background(), "r1", a)
		assert.ErrorIs(t, err, errBlobMissing)
	})
	t.Run("server errors are transient", func(t *testing.T) {
		for name, h := range map[string]http.HandlerFunc{
			"signer 503":   func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) },
			"download 500": nil,
		} {
			t.Run(name, func(t *testing.T) {
				signer, download := h, downloadOK
				if h == nil {
					signer, download = signerOK, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }
				}
				src, host := driveFixture(t, signer, download)
				a := att
				a.TitleLink = "api/v1/file/rooms/r1/file/f1?drive_host=" + host
				_, _, _, err := src.Open(context.Background(), "r1", a)
				require.Error(t, err)
				assert.NotErrorIs(t, err, errBlobMissing)
				assert.NotErrorIs(t, err, errBlobLegacy)
			})
		}
	})
}

func newTestBlobLane(src blobSource, obj objectStore, idx indexStore, c *auditarchive.Cipher, maxBytes int64) *blobLane {
	now := func() time.Time { return time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC) }
	return newBlobLane(blobLaneConfig{site: "site-a", maxBytes: maxBytes, chunkBytes: 4096, workers: 1, ackWait: time.Minute, heartbeatMax: time.Minute, now: now},
		nil, src, obj, idx, c, loopguard.New("b", func() {}), &metrics{})
}

func TestBlobLane_ArchiveAttachment(t *testing.T) {
	c, err := auditarchive.NewCipher(testDEK())
	require.NoError(t, err)
	plain := bytes.Repeat([]byte{7}, 10_000)
	att := cassandra.Attachment{ID: "f1", Title: "mock.png", FileType: "image/png", TitleLink: "api/v1/file/rooms/r1/file/f1?drive_host=https://drive.example"}

	t.Run("archived, encrypted, documented", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &recordingIndex{}
		l := newTestBlobLane(&fakeSource{data: map[string][]byte{"f1": plain}}, obj, idx, c, 1<<20)
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
		require.Equal(t, []string{"site-a/blobs/f1"}, obj.puts)
		require.Len(t, idx.docs, 1)
		assert.Equal(t, searchengine.ActionCreate, idx.docs[0].Action)
		assert.Equal(t, "audit-blobs-site-a", idx.docs[0].Index)
		assert.Equal(t, "site-a-f1", idx.docs[0].DocID)
		d := idx.docAt(t, 0)
		assert.Equal(t, "site-a/blobs/f1", d.BlobKey)
		assert.Equal(t, plainDigest(plain), d.PlainSHA256)
		assert.Equal(t, int64(len(plain)), d.SizeBytes)
		assert.Equal(t, 4096, d.ChunkBytes)
		assert.Equal(t, "image/png", d.ContentType)
		assert.Equal(t, "m1", d.MessageID)
		assert.Equal(t, "r1", d.RoomID)
		assert.Equal(t, "site-a", d.SiteID)
		assert.Empty(t, d.Skipped)
	})
	t.Run("stored object decrypts to the original bytes", func(t *testing.T) {
		var stored bytes.Buffer
		obj := &captureObjects{buf: &stored}
		l := newTestBlobLane(&fakeSource{data: map[string][]byte{"f1": plain}}, obj, &recordingIndex{}, c, 1<<20)
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
		var out bytes.Buffer
		_, n, err := auditarchive.DecryptBlob(&out, &stored, c, "f1")
		require.NoError(t, err)
		assert.Equal(t, int64(len(plain)), n)
		assert.Equal(t, plain, out.Bytes())
	})
	t.Run("over the cap is recorded as skipped size, not uploaded", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &recordingIndex{}
		l := newTestBlobLane(&fakeSource{data: map[string][]byte{"f1": plain}}, obj, idx, c, 100)
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
		assert.Empty(t, obj.puts)
		d := idx.docAt(t, 0)
		assert.Equal(t, "size", d.Skipped)
		assert.Empty(t, d.BlobKey)
		assert.Equal(t, int64(len(plain)), d.SizeBytes)
	})
	t.Run("over the cap with unknown size is caught while reading", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &recordingIndex{}
		l := newTestBlobLane(&fakeSource{data: map[string][]byte{"f1": plain}, unknownSize: true}, obj, idx, c, 100)
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
		assert.Empty(t, obj.puts)
		assert.Equal(t, "size", idx.docAt(t, 0).Skipped)
	})
	t.Run("exactly at the cap is archived", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &recordingIndex{}
		l := newTestBlobLane(&fakeSource{data: map[string][]byte{"f1": plain}, unknownSize: true}, obj, idx, c, int64(len(plain)))
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
		assert.Equal(t, []string{"site-a/blobs/f1"}, obj.puts)
		assert.Empty(t, idx.docAt(t, 0).Skipped)
	})
	t.Run("missing at source is recorded as skipped missing", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &recordingIndex{}
		l := newTestBlobLane(&fakeSource{data: map[string][]byte{}}, obj, idx, c, 1<<20)
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
		assert.Empty(t, obj.puts)
		assert.Equal(t, "missing", idx.docAt(t, 0).Skipped)
	})
	t.Run("legacy minio link is recorded as skipped legacy", func(t *testing.T) {
		obj, idx := &fakeObjects{}, &recordingIndex{}
		legacy := att
		legacy.TitleLink = "api/v1/file-upload/f1/mock.png"
		l := newTestBlobLane(&fakeSource{err: errBlobLegacy}, obj, idx, c, 1<<20)
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", legacy))
		assert.Empty(t, obj.puts)
		assert.Equal(t, "legacy", idx.docAt(t, 0).Skipped)
	})
	t.Run("transient source error is returned for a nak", func(t *testing.T) {
		idx := &recordingIndex{}
		l := newTestBlobLane(&fakeSource{err: errors.New("drive 503")}, &fakeObjects{}, idx, c, 1<<20)
		assert.Error(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
		assert.Empty(t, idx.docs, "no document is written for a retryable failure")
	})
	t.Run("object store failure is returned for a nak and writes no document", func(t *testing.T) {
		idx := &recordingIndex{}
		l := newTestBlobLane(&fakeSource{data: map[string][]byte{"f1": plain}}, &fakeObjects{failFirst: 1}, idx, c, 1<<20)
		assert.Error(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
		assert.Empty(t, idx.docs, "the document is written only after the blob is stored")
	})
	t.Run("index failure is returned for a nak", func(t *testing.T) {
		idx := &recordingIndex{fakeIndex: fakeIndex{err: errors.New("es down")}}
		l := newTestBlobLane(&fakeSource{data: map[string][]byte{"f1": plain}}, &fakeObjects{}, idx, c, 1<<20)
		assert.Error(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
	})
	t.Run("doc create conflict is done", func(t *testing.T) {
		idx := &recordingIndex{fakeIndex: fakeIndex{results: []searchengine.BulkResult{{Status: 409}}}}
		l := newTestBlobLane(&fakeSource{data: map[string][]byte{"f1": plain}}, &fakeObjects{}, idx, c, 1<<20)
		require.NoError(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
	})
	t.Run("a rejected doc create is returned for a nak", func(t *testing.T) {
		idx := &recordingIndex{fakeIndex: fakeIndex{results: []searchengine.BulkResult{{Status: 500, ErrorType: "boom"}}}}
		l := newTestBlobLane(&fakeSource{data: map[string][]byte{"f1": plain}}, &fakeObjects{}, idx, c, 1<<20)
		assert.Error(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
	})
	t.Run("an empty bulk response is returned for a nak", func(t *testing.T) {
		idx := &recordingIndex{fakeIndex: fakeIndex{results: []searchengine.BulkResult{}}}
		l := newTestBlobLane(&fakeSource{data: map[string][]byte{"f1": plain}}, &fakeObjects{}, idx, c, 1<<20)
		assert.Error(t, l.archiveAttachment(context.Background(), "m1", "r1", att))
	})
}

// captureObjects keeps the last uploaded object so a test can decrypt it.
type captureObjects struct{ buf *bytes.Buffer }

func (c *captureObjects) Put(_ context.Context, _ string, body io.Reader, _ int64, _ string) error {
	_, err := io.Copy(c.buf, body)
	return err
}

func attMsg(t *testing.T, seq uint64, event model.EventType, atts ...cassandra.Attachment) *fakeMsg {
	t.Helper()
	ev := model.MessageEvent{Event: event, SiteID: "site-a", Timestamp: 1759672800000, Message: model.Message{ID: "m1", RoomID: "r1"}}
	for i := range atts {
		raw, err := json.Marshal(atts[i])
		require.NoError(t, err)
		ev.Message.Attachments = append(ev.Message.Attachments, raw)
	}
	return eventMsg(t, seq, ev)
}

func driveAtt(id string) cassandra.Attachment {
	return cassandra.Attachment{ID: id, Title: id + ".png", FileType: "image/png", TitleLink: "api/v1/file/rooms/r1/file/" + id + "?drive_host=https://drive.example"}
}

// runBlobLane runs the lane over one scripted batch and returns once it has
// settled everything, so the test can read the messages without racing the
// worker goroutines (the lane closes done only after its workers return).
func runBlobLane(t *testing.T, l *blobLane, f *scriptedFetcher) {
	t.Helper()
	l.fetcher = f
	stop, done := make(chan struct{}), make(chan struct{})
	go l.run(context.Background(), stop, done)
	f.waitIdle(t)
	close(stop)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blob lane did not stop")
	}
}

func TestBlobLane_Run(t *testing.T) {
	c, err := auditarchive.NewCipher(testDEK())
	require.NoError(t, err)
	events := loadEvents(t)
	withAtt := eventMsg(t, 1, events["created"])
	noAtt := eventMsg(t, 2, events["deleted"])
	obj, idx := &fakeObjects{}, &recordingIndex{}
	l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, chunkBytes: 4096, workers: 2, ackWait: time.Minute, heartbeatMax: time.Minute, now: time.Now},
		nil, &fakeSource{data: map[string][]byte{"f1": []byte("png bytes")}}, obj, idx, c, loopguard.New("b", func() {}), &metrics{})
	runBlobLane(t, l, scripted([]jetstream.Msg{withAtt, noAtt}))
	assert.True(t, withAtt.acked)
	assert.True(t, noAtt.acked)
	assert.Equal(t, []string{"site-a/blobs/f1"}, obj.puts)
}

func TestBlobLane_Run_Settlement(t *testing.T) {
	c, err := auditarchive.NewCipher(testDEK())
	require.NoError(t, err)
	transient := errors.New("drive 503")
	tests := []struct {
		name      string
		msg       func(t *testing.T) *fakeMsg
		source    *fakeSource
		wantAck   bool
		wantNak   bool
		wantTerm  bool
		wantPuts  []string
		wantDocs  int
		wantFirst string // Skipped of the first document, when any
	}{
		{
			name:     "every attachment archived is acked",
			msg:      func(t *testing.T) *fakeMsg { return attMsg(t, 1, model.EventCreated, driveAtt("a"), driveAtt("b")) },
			source:   &fakeSource{data: map[string][]byte{"a": []byte("aa"), "b": []byte("bb")}},
			wantAck:  true,
			wantPuts: []string{"site-a/blobs/a", "site-a/blobs/b"},
			wantDocs: 2,
		},
		{
			name:     "a transient failure on a later attachment naks the whole message",
			msg:      func(t *testing.T) *fakeMsg { return attMsg(t, 1, model.EventCreated, driveAtt("a"), driveAtt("b")) },
			source:   &fakeSource{data: map[string][]byte{"a": []byte("aa"), "b": []byte("bb")}, errFor: map[string]error{"b": transient}},
			wantNak:  true,
			wantPuts: []string{"site-a/blobs/a"},
			wantDocs: 1,
		},
		{
			name:      "skips count as done",
			msg:       func(t *testing.T) *fakeMsg { return attMsg(t, 1, model.EventCreated, driveAtt("gone")) },
			source:    &fakeSource{data: map[string][]byte{}},
			wantAck:   true,
			wantDocs:  1,
			wantFirst: "missing",
		},
		{
			name:     "an edit is not archived again",
			msg:      func(t *testing.T) *fakeMsg { return attMsg(t, 1, model.EventUpdated, driveAtt("a")) },
			source:   &fakeSource{data: map[string][]byte{"a": []byte("aa")}},
			wantAck:  true,
			wantDocs: 0,
		},
		{
			name: "a message without an id is poison",
			msg: func(t *testing.T) *fakeMsg {
				return eventMsg(t, 1, model.MessageEvent{Event: model.EventCreated, Message: model.Message{RoomID: "r1"}})
			},
			source:   &fakeSource{},
			wantTerm: true,
		},
		{
			name: "an unparseable payload is poison",
			msg: func(*testing.T) *fakeMsg {
				return &fakeMsg{subject: "s", data: []byte("{not json"), seq: 1, stream: "S"}
			},
			source:   &fakeSource{},
			wantTerm: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := tt.msg(t)
			obj, idx := &fakeObjects{}, &recordingIndex{}
			l := newTestBlobLane(tt.source, obj, idx, c, 1<<20)
			runBlobLane(t, l, scripted([]jetstream.Msg{msg}))
			assert.Equal(t, tt.wantAck, msg.acked, "acked")
			assert.Equal(t, tt.wantNak, msg.naked, "naked")
			assert.Equal(t, tt.wantTerm, msg.termed, "termed")
			if tt.wantNak {
				assert.Positive(t, msg.nakDelay, "a nak always carries a delay")
			}
			assert.Equal(t, tt.wantPuts, obj.puts)
			assert.Len(t, idx.docs, tt.wantDocs)
			if tt.wantFirst != "" {
				assert.Equal(t, tt.wantFirst, idx.docAt(t, 0).Skipped)
			}
		})
	}
}

func TestBlobLane_Run_FetchErrors(t *testing.T) {
	c, err := auditarchive.NewCipher(testDEK())
	require.NoError(t, err)
	src := &fakeSource{data: map[string][]byte{"a": []byte("aa")}}

	t.Run("terminal fetch error stops the guard", func(t *testing.T) {
		for _, terr := range []error{jetstream.ErrConsumerNotFound, jetstream.ErrConsumerDeleted, jetstream.ErrStreamNotFound} {
			fired := make(chan struct{}, 1)
			guard := loopguard.New("b", func() { fired <- struct{}{} })
			l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, workers: 1, now: time.Now}, errFetcher{err: terr}, src, &fakeObjects{}, &recordingIndex{}, c, guard, &metrics{})
			done := make(chan struct{})
			go l.run(context.Background(), make(chan struct{}), done)
			<-done
			assert.ErrorIs(t, guard.Check().Probe(context.Background()), terr)
			select {
			case <-fired:
			default:
				t.Error("unexpected-stop hook did not run")
			}
		}
	})
	t.Run("terminal batch error settles delivered messages then stops the guard", func(t *testing.T) {
		for _, terr := range []error{jetstream.ErrConsumerDeleted, jetstream.ErrBadRequest, jetstream.ErrConsumerNotFound} {
			msg := attMsg(t, 1, model.EventCreated, driveAtt("a"))
			fired := make(chan struct{}, 1)
			guard := loopguard.New("b", func() { fired <- struct{}{} })
			sf := scriptedErr(terr, []jetstream.Msg{msg})
			l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, workers: 1, now: time.Now}, sf, src, &fakeObjects{}, &recordingIndex{}, c, guard, &metrics{})
			done := make(chan struct{})
			go l.run(context.Background(), make(chan struct{}), done)
			<-done
			assert.Equal(t, int32(1), sf.calls.Load(), "the loop stops instead of re-fetching a dead consumer")
			assert.True(t, msg.acked, "messages delivered with the failing batch are archived first")
			assert.ErrorIs(t, guard.Check().Probe(context.Background()), terr)
			select {
			case <-fired:
			default:
				t.Error("unexpected-stop hook did not run")
			}
		}
	})
	t.Run("benign batch errors keep the lane running", func(t *testing.T) {
		for _, berr := range []error{nats.ErrTimeout, jetstream.ErrNoMessages, jetstream.ErrNoHeartbeat} {
			msg := attMsg(t, 1, model.EventCreated, driveAtt("a"))
			guard := loopguard.New("b", func() {})
			sf := scriptedErr(berr, []jetstream.Msg{msg})
			l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, workers: 1, now: time.Now}, sf, src, &fakeObjects{}, &recordingIndex{}, c, guard, &metrics{})
			stop, done := make(chan struct{}), make(chan struct{})
			go l.run(context.Background(), stop, done)
			sf.waitIdle(t)
			close(stop)
			<-done
			assert.True(t, msg.acked)
			assert.NoError(t, guard.Check().Probe(context.Background()))
		}
	})
	t.Run("an unknown batch error pauses before the next fetch", func(t *testing.T) {
		sf := scriptedErr(nats.ErrNoResponders, []jetstream.Msg{})
		guard := loopguard.New("b", func() {})
		l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, workers: 1, now: time.Now}, sf, src, &fakeObjects{}, &recordingIndex{}, c, guard, &metrics{})
		l.fetchRetry = time.Hour
		paused := make(chan time.Duration, 1)
		l.newTimer = func(d time.Duration) *time.Timer {
			paused <- d
			return time.NewTimer(time.Hour)
		}
		stop, done := make(chan struct{}), make(chan struct{})
		go l.run(context.Background(), stop, done)
		select {
		case d := <-paused:
			assert.Equal(t, time.Hour, d)
		case <-time.After(2 * time.Second):
			t.Fatal("lane did not pause after a batch error")
		}
		assert.Equal(t, int32(1), sf.calls.Load(), "no re-fetch while paused")
		close(stop)
		<-done
		assert.NoError(t, guard.Check().Probe(context.Background()), "no responders is retried, not a loop death")
	})
	t.Run("context cancel ends the loop without tripping the guard", func(t *testing.T) {
		guard := loopguard.New("b", func() {})
		l := newBlobLane(blobLaneConfig{site: "site-a", maxBytes: 1 << 20, workers: 1, now: time.Now}, scripted(), src, &fakeObjects{}, &recordingIndex{}, c, guard, &metrics{})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go l.run(ctx, make(chan struct{}), done)
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("lane ignored context cancel")
		}
		assert.NoError(t, guard.Check().Probe(context.Background()))
	})
}

func TestNewBlobLane_Defaults(t *testing.T) {
	l := newBlobLane(blobLaneConfig{}, nil, nil, nil, nil, nil, nil, &metrics{})
	assert.Equal(t, auditarchive.DefaultChunkBytes, l.cfg.chunkBytes)
	assert.Equal(t, 1, l.cfg.workers)
	assert.NotNil(t, l.cfg.now)
	assert.Equal(t, defaultFetchRetry, l.fetchRetry)
}
