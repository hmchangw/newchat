package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/auditarchive"
	"github.com/hmchangw/chat/pkg/jobguard"
	"github.com/hmchangw/chat/pkg/jsretry"
	"github.com/hmchangw/chat/pkg/loopguard"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// Skipped values recorded on a BlobDoc when an attachment is not archived.
const (
	skipSize    = "size"
	skipMissing = "missing"
	skipLegacy  = "legacy"
)

type blobLaneConfig struct {
	site         string
	maxBytes     int64
	chunkBytes   int
	workers      int
	ackWait      time.Duration
	heartbeatMax time.Duration
	// maxDeliver is the consumer's MaxDeliver; on its last delivery a failing
	// message is Termed and logged instead of nak'd into the void. Zero or
	// negative means unlimited.
	maxDeliver int
	now        func() time.Time
}

// blobLane archives each created message's attachments as chunk-encrypted
// blobs plus one BlobDoc per attachment. It runs on its own consumer, apart
// from the events lane, so a slow Drive never delays event archiving.
type blobLane struct {
	cfg        blobLaneConfig
	fetcher    msgFetcher
	source     blobSource
	objects    objectStore
	index      indexStore
	cipher     *auditarchive.Cipher
	guard      *loopguard.Guard
	metrics    *metrics
	fetchRetry time.Duration
	newTimer   func(time.Duration) *time.Timer // swapped in tests to observe the pause
}

func newBlobLane(cfg blobLaneConfig, fetcher msgFetcher, src blobSource, objects objectStore, index indexStore, c *auditarchive.Cipher, guard *loopguard.Guard, m *metrics) *blobLane {
	if cfg.chunkBytes <= 0 {
		cfg.chunkBytes = auditarchive.DefaultChunkBytes
	}
	if cfg.workers <= 0 {
		cfg.workers = 1
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	return &blobLane{
		cfg: cfg, fetcher: fetcher, source: src, objects: objects, index: index, cipher: c, guard: guard, metrics: m,
		fetchRetry: defaultFetchRetry, newTimer: time.NewTimer,
	}
}

// run is the consume loop. A worker slot is reserved before each Fetch and
// the Fetch asks for exactly as many messages as there are free slots, so a
// delivered message never waits for a worker with its AckWait running and no
// heartbeat. On stop or context cancel it waits for in-flight work; a terminal
// fetch error (consumer deleted/not found) does the same, reports
// guard.Stopped(err) and returns.
func (l *blobLane) run(ctx context.Context, stopCh <-chan struct{}, doneCh chan<- struct{}) {
	defer close(doneCh)
	sem := make(chan struct{}, l.cfg.workers)
	var wg sync.WaitGroup
	defer wg.Wait()

	stopped := func() bool {
		select {
		case <-stopCh:
			return true
		case <-ctx.Done():
			return true
		default:
			return false
		}
	}
	for {
		if stopped() {
			return
		}
		// Block for one slot, then take whatever else is free.
		select {
		case sem <- struct{}{}:
		case <-stopCh:
			return
		case <-ctx.Done():
			return
		}
		reserved := 1
		for reserved < l.cfg.workers {
			select {
			case sem <- struct{}{}:
				reserved++
				continue
			default:
			}
			break
		}

		batch, err := l.fetcher.Fetch(ctx, reserved, jetstream.FetchMaxWait(maxFetchWait))
		started := 0
		if err == nil {
			for fm := range batch.Messages() {
				if started >= reserved {
					// The server returned more than requested; take a slot for
					// the extra rather than run past the worker bound.
					sem <- struct{}{}
				}
				started++
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-sem }()
					jobguard.Guard("archive blob", func() { l.handle(fm.Ctx, fm.Msg) })
				}()
			}
		}
		// Slots reserved for messages that never arrived go back.
		for i := started; i < reserved; i++ {
			<-sem
		}
		if err == nil {
			// Only the batch's own error can be benign: a Fetch call that
			// itself fails always takes the pause below.
			if err = batch.Error(); benignBatchErr(err) {
				continue
			}
		}
		if stopped() {
			return
		}
		if terminalFetchErr(err) {
			wg.Wait()
			l.guard.Stopped(err)
			return
		}
		slog.WarnContext(ctx, "fetch failed, retrying", "lane", "blobs", "error", err)
		l.pause(ctx, stopCh)
	}
}

// pause waits fetchRetry, or less if the lane is told to stop, so a consumer
// that fails instantly cannot turn the loop into a hot spin.
func (l *blobLane) pause(ctx context.Context, stopCh <-chan struct{}) {
	t := l.newTimer(l.fetchRetry)
	defer t.Stop()
	select {
	case <-t.C:
	case <-stopCh:
	case <-ctx.Done():
	}
}

// handle archives every attachment of one message and settles it exactly
// once: Ack when all are done, Nak with backoff on the first retryable
// failure, Term for a message that can never be processed.
func (l *blobLane) handle(ctx context.Context, msg jetstream.Msg) {
	noteRedelivery(l.metrics, msg)
	data, err := natsutil.DecodePayload(msg)
	if err != nil {
		slog.ErrorContext(ctx, "undecodable payload, terminating", "lane", "blobs", "subject", msg.Subject(), "error", err)
		l.term(ctx, msg)
		return
	}
	meta, err := metaOf(msg)
	if err != nil {
		slog.ErrorContext(ctx, "poison event, terminating", "lane", "blobs", "subject", msg.Subject(), "error", err)
		l.term(ctx, msg)
		return
	}
	var ev model.MessageEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		slog.ErrorContext(ctx, "poison event, terminating", "lane", "blobs", "stream", meta.Stream, "seq", meta.Seq, "error", fmt.Errorf("%w: unmarshal message event: %w", errPoison, err))
		l.term(ctx, msg)
		return
	}
	if ev.Event != "" && ev.Event != model.EventCreated {
		natsutil.Ack(msg, "not a created event: "+string(ev.Event))
		return
	}
	if ev.Message.ID == "" || ev.Message.RoomID == "" {
		slog.ErrorContext(ctx, "poison event, terminating", "lane", "blobs", "stream", meta.Stream, "seq", meta.Seq, "error", fmt.Errorf("%w: message id or room id missing", errPoison))
		l.term(ctx, msg)
		return
	}
	atts, _ := cassandra.DecodeAttachments(ev.Message.Attachments) // skipped count discarded: an undecodable attachment has no id or link to archive
	if len(atts) == 0 {
		natsutil.Ack(msg, "no attachments")
		return
	}
	for i := range atts {
		if atts[i].ID == "" {
			slog.WarnContext(ctx, "attachment has no id, skipping", "lane", "blobs", "messageId", ev.Message.ID, "roomId", ev.Message.RoomID)
			continue
		}
		if err := l.archiveWithHeartbeat(ctx, msg, ev.Message.ID, ev.Message.RoomID, atts[i]); err != nil {
			l.fail(ctx, msg, ev.Message.ID, ev.Message.RoomID, len(atts), atts[i].ID, err)
			return
		}
	}
	natsutil.Ack(msg, "attachments archived")
}

// archiveWithHeartbeat holds the message's ack deadline open while one
// attachment downloads, and releases it before the caller settles the message.
func (l *blobLane) archiveWithHeartbeat(ctx context.Context, msg jetstream.Msg, msgID, roomID string, att cassandra.Attachment) error { //nolint:gocritic // hugeParam: attachments travel by value, as in blobSource
	stop := jsretry.Heartbeat(ctx, msg, jsretry.HeartbeatBudget{Every: jsretry.HeartbeatInterval(l.cfg.ackWait), Max: l.cfg.heartbeatMax})
	defer stop()
	return l.archiveAttachment(ctx, msgID, roomID, att)
}

// fail settles a message whose attachment hit a retryable error: a Nak with
// backoff, or, on the consumer's last delivery, an explicit Term so the give-up
// is logged instead of vanishing.
func (l *blobLane) fail(ctx context.Context, msg jetstream.Msg, msgID, roomID string, attachments int, fileID string, err error) {
	if md, mdErr := msg.Metadata(); mdErr == nil && jsretry.IsLastAttempt(md.NumDelivered, l.cfg.maxDeliver) {
		slog.WarnContext(ctx, "attachment not archived on the last delivery, dropping", "lane", "blobs", "disposition", "drop",
			"messageId", msgID, "roomId", roomID, "attachments", attachments, "fileId", fileID, "error", err)
		l.term(ctx, msg)
		return
	}
	slog.WarnContext(ctx, "attachment not archived, will redeliver", "lane", "blobs", "messageId", msgID, "fileId", fileID, "error", err)
	jsretry.Nak(ctx, msg, jsretry.DefaultBackoff, "archive attachment failed")
}

func (l *blobLane) term(ctx context.Context, msg jetstream.Msg) {
	if err := msg.Term(); err != nil {
		slog.ErrorContext(ctx, "term failed", "lane", "blobs", "error", err)
	}
}

// archiveAttachment copies one attachment once. A skip (size, missing,
// legacy) still writes a document so the console can show why; any other
// error is retryable and writes nothing, so the document appears only after
// the blob is durably stored.
func (l *blobLane) archiveAttachment(ctx context.Context, msgID, roomID string, att cassandra.Attachment) error { //nolint:gocritic // hugeParam: signature fixed by the lane contract
	doc := auditarchive.BlobDoc{FileID: att.ID, MessageID: msgID, RoomID: roomID, SiteID: l.cfg.site, FileName: att.Title, ContentType: att.FileType, ArchivedAt: l.cfg.now().UTC()}
	body, size, ctype, err := l.source.Open(ctx, roomID, att)
	switch {
	case errors.Is(err, errBlobLegacy):
		doc.Skipped = skipLegacy
	case errors.Is(err, errBlobMissing):
		doc.Skipped = skipMissing
	case err != nil:
		return fmt.Errorf("open attachment %s: %w", att.ID, err)
	default:
		defer body.Close()
		if ctype != "" {
			doc.ContentType = ctype
		}
		if err := l.store(ctx, &doc, body, size); err != nil {
			return err
		}
	}
	if doc.Skipped != "" {
		l.metrics.blobs("skipped_"+doc.Skipped, 0)
	}
	return l.createDoc(ctx, &doc)
}

// store encrypts and uploads the attachment unless it exceeds the cap, in
// which case it records a size skip. The size is checked up front when the
// source knows it and again while reading when it does not.
func (l *blobLane) store(ctx context.Context, doc *auditarchive.BlobDoc, body io.Reader, size int64) error {
	doc.SizeBytes = size
	if size > l.cfg.maxBytes {
		doc.Skipped = skipSize
		return nil
	}
	limited := &io.LimitedReader{R: body, N: l.cfg.maxBytes + 1}
	// minio-go needs the object size up front for a single-part PUT, so the
	// encrypted blob is buffered; the cap and worker count bound the memory.
	var enc bytes.Buffer
	sum, n, err := auditarchive.EncryptBlob(&enc, limited, l.cipher, doc.FileID, l.cfg.chunkBytes)
	if err != nil {
		return fmt.Errorf("encrypt attachment %s: %w", doc.FileID, err)
	}
	if n > l.cfg.maxBytes {
		// The read stopped at the cap, so the real size is unknown: -1 means
		// "over the cap, size unknown".
		doc.Skipped, doc.SizeBytes = skipSize, -1
		return nil
	}
	key := auditarchive.BlobKey(l.cfg.site, doc.FileID)
	if err := l.objects.Put(ctx, key, bytes.NewReader(enc.Bytes()), int64(enc.Len()), "application/octet-stream"); err != nil {
		return fmt.Errorf("put attachment %s: %w", doc.FileID, err)
	}
	doc.BlobKey, doc.PlainSHA256, doc.SizeBytes, doc.ChunkBytes = key, sum, n, l.cfg.chunkBytes
	l.metrics.blobs("archived", n)
	return nil
}

// createDoc writes the BlobDoc with create semantics; a conflict means an
// earlier delivery already recorded this file, which is done.
func (l *blobLane) createDoc(ctx context.Context, doc *auditarchive.BlobDoc) error {
	raw, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal blob doc %s: %w", doc.FileID, err)
	}
	results, err := l.index.Bulk(ctx, []searchengine.BulkAction{{
		Action: searchengine.ActionCreate,
		Index:  auditarchive.BlobsIndex(l.cfg.site),
		DocID:  auditarchive.BlobDocID(l.cfg.site, doc.FileID),
		Doc:    raw,
	}})
	if err != nil {
		return fmt.Errorf("create blob doc %s: %w", doc.FileID, err)
	}
	if len(results) != 1 {
		return fmt.Errorf("create blob doc %s: got %d bulk results, want 1", doc.FileID, len(results))
	}
	if !searchengine.IsBulkItemSuccess(searchengine.ActionCreate, results[0]) {
		return fmt.Errorf("create blob doc %s: status %d %s", doc.FileID, results[0].Status, results[0].ErrorType)
	}
	return nil
}
