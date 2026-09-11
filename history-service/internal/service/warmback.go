package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/natsutil"
)

const (
	defaultWarmBackWorkers = 8
	defaultWarmBackQueue   = 1024
)

// warmBackJob is one resolved preview waiting to be stored. Queued by pointer: it is
// allocated once at the submit site and read by exactly one worker, so the alternative is
// copying its embedded preview through the channel. It carries the request id
// rather than the request context: a queued job outlives its request, and holding that
// context would pin the whole *natsrouter.Context — including the inbound message body —
// for as long as the job sits in the queue.
type warmBackJob struct {
	roomID    string
	preview   models.PreviewMessage
	forMsgID  string
	asOf      int64
	requestID string
}

// warmBackMetrics counts what the queue would otherwise hide. A shed job and a failed
// write are both ways a room silently stays on the lazy walk, which is the failure this
// whole path exists to end — and at saturation they arrive too fast to log one by one.
// Instruments are no-ops without a MeterProvider, so recording is always safe.
type warmBackMetrics struct {
	stored  metric.Int64Counter
	dropped metric.Int64Counter
	failed  metric.Int64Counter
}

var warmBackCounters = newWarmBackMetrics(otel.Meter("history_preview_warmback"))

func newWarmBackMetrics(m metric.Meter) warmBackMetrics {
	stored, sErr := m.Int64Counter("preview_warmback_stored_total",
		metric.WithDescription("Walk-resolved room previews written back to the room document."))
	dropped, dErr := m.Int64Counter("preview_warmback_dropped_total",
		metric.WithDescription("Room preview warm-backs shed because the writer queue was full."))
	failed, fErr := m.Int64Counter("preview_warmback_failed_total",
		metric.WithDescription("Room preview warm-backs that reached the store and failed to write."))
	if sErr != nil || dErr != nil || fErr != nil {
		// Recording must never be conditional on the meter having accepted the instruments.
		n := noop.NewMeterProvider().Meter("history_preview_warmback")
		stored, _ = n.Int64Counter("preview_warmback_stored_total")
		dropped, _ = n.Int64Counter("preview_warmback_dropped_total")
		failed, _ = n.Int64Counter("preview_warmback_failed_total")
	}
	return warmBackMetrics{stored: stored, dropped: dropped, failed: failed}
}

// previewWriter is the warm-back writer; PREVIEW_WARMBACK_ENABLED=false installs the no-op.
// An interface so "off" never encodes as closed: Close is nil-channel-safe only via that guard.
type previewWriter interface {
	Submit(ctx context.Context, job *warmBackJob)
	Close(ctx context.Context) error
}

// nopPreviewWarmer is the disabled form: no queue, no workers, nothing for Close to drain.
type nopPreviewWarmer struct{}

func (nopPreviewWarmer) Submit(context.Context, *warmBackJob) {}
func (nopPreviewWarmer) Close(context.Context) error          { return nil }

// PreviewWarmer stores walk-resolved previews off the request path.
//
// The write is optional; the reply is not. Running it inline made the two share a budget,
// so it was skipped whenever the request had less than a couple of seconds left — which is
// exactly the case a cold, slow batch produces. A room that never warms back re-walks on
// the next read, stays slow, and skips again: the repair switched itself off precisely
// where it was needed. Queued, the write keeps its own clock and a spent request budget no
// longer decides whether a room ever heals.
//
// Bounded on both axes, because unbounded background work would trade the latency problem
// for a memory one: workers cap concurrent writes, the queue caps what a burst can pin, and
// a full queue sheds the job rather than blocking the reply behind it.
type PreviewWarmer struct {
	rooms   RoomRepository
	jobs    chan *warmBackJob
	wg      sync.WaitGroup
	timeout time.Duration

	// Guards jobs against a send racing Close's close(). Read-locked on the hot path,
	// so submits contend only with shutdown.
	mu     sync.RWMutex
	closed bool
}

// NewPreviewWarmer starts the writer's workers. Non-positive sizes take the defaults.
// "Off" is not this constructor's concern: PREVIEW_WARMBACK_ENABLED=false makes New
// install nopPreviewWarmer instead of calling it.
//
// Exported so main can build one pool and hand it to both lanes' services with
// WithPreviewWarmer: the pool writes to Mongo, which is up whichever lane is
// serving, so a second set of workers would be overhead for a lane that is idle
// almost always. A service built without the option starts its own.
func NewPreviewWarmer(rooms RoomRepository, workers, queue int) *PreviewWarmer {
	return newPreviewWarmerWithTimeout(rooms, workers, queue, warmBackTimeout)
}

func newPreviewWarmerWithTimeout(rooms RoomRepository, workers, queue int, timeout time.Duration) *PreviewWarmer {
	if workers <= 0 {
		workers = defaultWarmBackWorkers
	}
	if queue <= 0 {
		queue = defaultWarmBackQueue
	}
	w := &PreviewWarmer{rooms: rooms, jobs: make(chan *warmBackJob, queue), timeout: timeout}
	w.wg.Add(workers)
	for range workers {
		go w.run()
	}
	return w
}

// Submit queues a warm-back, dropping it when the writer is saturated. Never blocks: the
// reply it precedes must not wait on an optional write, and a dropped job is self-
// correcting — the next read re-walks the room and submits again.
func (w *PreviewWarmer) Submit(ctx context.Context, job *warmBackJob) {
	job.requestID = natsutil.RequestIDFromContext(ctx)

	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return
	}
	select {
	case w.jobs <- job:
	default:
		warmBackCounters.dropped.Add(ctx, 1)
		slog.DebugContext(ctx, "room preview warm-back dropped; writer queue full",
			"room_id", job.roomID, "request_id", job.requestID)
	}
}

func (w *PreviewWarmer) run() {
	defer w.wg.Done()
	for job := range w.jobs {
		w.store(job)
	}
}

// store performs one write on a fresh budget, detached from the request that produced it.
func (w *PreviewWarmer) store(job *warmBackJob) {
	ctx, cancel := context.WithTimeout(natsutil.WithRequestID(context.Background(), job.requestID), w.timeout)
	defer cancel()
	if err := w.rooms.SetPreviewMessage(ctx, job.roomID, job.preview, job.forMsgID, job.asOf); err != nil {
		warmBackCounters.failed.Add(ctx, 1)
		slog.WarnContext(ctx, "room preview warm-back failed", "room_id", job.roomID,
			"request_id", job.requestID, "error", err)
		return
	}
	warmBackCounters.stored.Add(ctx, 1)
}

// Close stops accepting work and waits for the queue to drain, giving up when ctx expires
// so a wedged Mongo cannot hold the whole shutdown past its deadline — the writes are
// optional, and a later read re-derives what they would have stored. Idempotent.
func (w *PreviewWarmer) Close(ctx context.Context) error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.jobs)
	}
	w.mu.Unlock()

	// The workers' own timeout bounds every in-flight write, so this goroutine always ends.
	drained := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
