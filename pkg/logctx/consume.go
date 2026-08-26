package logctx

import (
	"context"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// ConsumeContext prepares the per-message context a stream consumer runs its
// handler under: it stamps the request id, admits the inbound X-Debug rung and
// payload-capture intent, and captures the payload once the intent is on the
// context. It returns that context and the request id.
//
// It is the consumer-side counterpart to natsrouter's RequestID middleware,
// which does the same three things for request/reply. Consumers had no
// equivalent and hand-rolled it, so the three had drifted apart: thirteen sites
// stamped the request id, but only six admitted the rung and only six captured
// the payload. In the other seven — outbox-worker, notification-worker,
// inbox-worker, hr-sync-worker and the oplog services — a request that entered
// the system with X-Debug set had its verbose-logging intent silently dropped at
// the stream boundary, which is precisely where a cross-service trace needs it.
//
// Takes the three primitives rather than a message so it serves both shapes: a
// jetstream.Msg passes msg.Headers()/msg.Subject()/msg.Data(), a core *nats.Msg
// passes msg.Header/msg.Subject/msg.Data.
//
// Admission is free when nothing asked for it: with no X-Debug header the rung
// is Off and the context is returned unchanged, and the capture is additionally
// gated on the service's own DEBUG_LOG_PAYLOADS.
func ConsumeContext(ctx context.Context, headers nats.Header, subject string, data []byte) (context.Context, string) {
	ctx, id := natsutil.StampRequestID(ctx, headers, subject)
	ctx = Admit(ctx, headers)
	// After Admit, which is what puts the capture intent on the context.
	CapturePayload(ctx, "consumed", subject, data)
	return ctx, id
}
