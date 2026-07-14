package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// historyClient applies a migrated edit/soft-delete via history-service's internal handlers.
type historyClient interface {
	Edit(ctx context.Context, req model.MigrationEditRequest) error
	Delete(ctx context.Context, req model.MigrationDeleteRequest) error
}

type natsHistoryClient struct {
	nc      *nats.Conn
	siteID  string
	timeout time.Duration
	metrics *metrics // nil-safe; records the permanent-rejection Term signal
}

//nolint:gocritic // req passed by value to satisfy the historyClient interface; one per edit event, off the hot path.
func (c *natsHistoryClient) Edit(ctx context.Context, req model.MigrationEditRequest) error {
	return c.request(ctx, subject.MigrationInternalMsgEdit(c.siteID), req)
}

//nolint:gocritic // req passed by value to satisfy the historyClient interface; one per delete event, off the hot path.
func (c *natsHistoryClient) Delete(ctx context.Context, req model.MigrationDeleteRequest) error {
	return c.request(ctx, subject.MigrationInternalMsgDelete(c.siteID), req)
}

func (c *natsHistoryClient) request(ctx context.Context, subj string, payload any) (err error) {
	ctx, span := otel.Tracer("oplog-transformer").Start(ctx, "history.request")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal migration request: %w", err)
	}
	rctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	reply, err := c.nc.RequestMsgWithContext(rctx, natsutil.NewMsg(rctx, subj, data))
	if err != nil {
		// Transport failure (timeout / no responders) — retryable. Nak.
		return fmt.Errorf("history request %q: %w", subj, err)
	}
	termCode, derr := classifyHistoryReply(subj, reply.Data)
	if termCode != "" {
		// A permanent history rejection — the transformer will Term (migration.ErrPoison). Record the
		// rejecting category so a genuine permanent failure is visible, not buried in generic terms.
		c.metrics.onHistoryRejected(ctx, string(termCode))
	}
	return derr
}

// permanentHistoryRejection reports whether a history error code is a permanent rejection (Term).
// Only explicit client-contract categories qualify; NotFound/infra/too-many-requests/unknown stay retryable (Nak).
func permanentHistoryRejection(code errcode.Code) bool {
	switch code {
	case errcode.CodeBadRequest, errcode.CodeUnauthenticated, errcode.CodeForbidden, errcode.CodeConflict:
		return true
	default:
		return false
	}
}

// classifyHistoryReply maps a history reply to a disposition error: nil → Ack; migration.ErrPoison-wrapped
// (permanent rejection) → Term; plain error (retryable/unknown/not-ok ack/undecodable) → Nak.
// termCode is the rejecting category only for a permanent rejection (Term metric), else "".
func classifyHistoryReply(subj string, data []byte) (termCode errcode.Code, err error) {
	if ec, ok := errcode.Parse(data); ok {
		if permanentHistoryRejection(ec.Code) {
			return ec.Code, fmt.Errorf("%w: history permanently rejected %q (%s): %s", migration.ErrPoison, subj, ec.Code, ec.Message)
		}
		return "", fmt.Errorf("history rejected %q (retryable %s): %s", subj, ec.Code, ec.Message)
	}
	// Not an error envelope — a success ack (or a malformed reply).
	var ack model.MigrationAck
	if jerr := json.Unmarshal(data, &ack); jerr != nil {
		return "", fmt.Errorf("decode migration ack on %q: %w", subj, jerr)
	}
	if !ack.OK {
		return "", fmt.Errorf("history rejected migration op on %q (ack not ok)", subj)
	}
	return "", nil
}
