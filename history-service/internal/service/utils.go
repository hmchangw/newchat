package service

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/pagefit"
)

// checkAccessAndRoomTimes runs the subscription access check and the room-times
// resolve concurrently — both are independent Mongo reads on the read hot path.
// Access errors take precedence over room-times errors so a "not subscribed"
// 403 is never masked by a transient room-metadata failure.
func (s *HistoryService) checkAccessAndRoomTimes(
	ctx context.Context,
	account, roomID string,
	meta *models.RoomMeta,
	now time.Time,
) (accessSince *time.Time, lastMsgAt, createdAt time.Time, err error) {
	var accessErr, rtErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		accessSince, accessErr = s.getAccessSince(ctx, account, roomID)
	}()
	go func() {
		defer wg.Done()
		lastMsgAt, createdAt, rtErr = s.resolveRoomTimesOrError(ctx, roomID, meta, now)
	}()
	wg.Wait()
	if accessErr != nil {
		return nil, time.Time{}, time.Time{}, accessErr
	}
	if rtErr != nil {
		return nil, time.Time{}, time.Time{}, rtErr
	}
	return accessSince, lastMsgAt, createdAt, nil
}

// getAccessSince checks subscription and returns the historySharedSince lower bound (nil = full access).
func (s *HistoryService) getAccessSince(ctx context.Context, account, roomID string) (*time.Time, error) {
	accessSince, subscribed, err := s.subscriptions.GetHistorySharedSince(ctx, account, roomID)
	if err != nil {
		return nil, fmt.Errorf("verifying room access for %s/%s: %w", account, roomID, err)
	}
	if !subscribed {
		// Parity with message-gatekeeper's identical condition: same reason
		// lets the frontend branch consistently without service-by-service text matching.
		return nil, errcode.Forbidden("not subscribed to room", errcode.WithReason(errcode.MessageNotSubscribed))
	}
	return accessSince, nil
}

func millisToTime(millis *int64) time.Time {
	if millis == nil {
		return time.Time{}
	}
	return time.UnixMilli(*millis).UTC()
}

func parsePageRequest(cursor string, limit int) (cassrepo.PageRequest, error) {
	q, err := cassrepo.ParsePageRequest(cursor, limit)
	if err != nil {
		// Cause is the parse error (cursor format/decode) — server-only;
		// the user-safe message stays generic.
		return cassrepo.PageRequest{}, errcode.BadRequest("invalid pagination cursor", errcode.WithCause(err))
	}
	return q, nil
}

func (s *HistoryService) findMessage(ctx context.Context, roomID, messageID string) (*models.Message, error) {
	if messageID == "" {
		return nil, errcode.BadRequest("messageId is required")
	}
	msg, err := s.msgReader.GetMessageByID(ctx, messageID)
	if err != nil {
		return nil, fmt.Errorf("retrieving message %s: %w", messageID, err)
	}
	if msg == nil {
		return nil, errcode.NotFound("message not found")
	}
	if msg.RoomID != roomID {
		return nil, errcode.NotFound("message not found")
	}
	return msg, nil
}

// UnavailableQuoteMsg is written into QuotedParentMessage.Msg when the quoted message is outside the access window.
const UnavailableQuoteMsg = "This message is unavailable"

// quoteInaccessible reports whether a quoted message falls outside the access window.
// For TShow replies, uses ThreadParentCreatedAt embedded at write time; nil timestamp
// means the parent time was never captured, so redact conservatively to prevent leaks.
func quoteInaccessible(m *models.Message, q *models.QuotedParentMessage, accessSince time.Time) bool {
	tshowParentInaccessible := m.TShow && q.ThreadParentID != "" && q.ThreadParentCreatedAt != nil && q.ThreadParentCreatedAt.Before(accessSince)
	legacyTShowMissingParentTime := m.TShow && q.ThreadParentID != "" && q.ThreadParentCreatedAt == nil
	return q.CreatedAt.Before(accessSince) || tshowParentInaccessible || legacyTShowMissingParentTime
}

func redactUnavailableQuote(m *models.Message, accessSince *time.Time) {
	if m == nil || accessSince == nil {
		return
	}
	q := m.QuotedParentMessage
	if q == nil {
		return
	}
	if quoteInaccessible(m, q, *accessSince) {
		m.QuotedParentMessage = &models.QuotedParentMessage{Msg: UnavailableQuoteMsg}
	}
}

func redactUnavailableQuotes(msgs []models.Message, accessSince *time.Time) {
	if accessSince == nil {
		return
	}
	for i := range msgs {
		q := msgs[i].QuotedParentMessage
		if q == nil {
			continue
		}
		if quoteInaccessible(&msgs[i], q, *accessSince) {
			msgs[i].QuotedParentMessage = &models.QuotedParentMessage{Msg: UnavailableQuoteMsg}
		}
	}
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// timeMax returns the later of two timestamps; zero values are ignored.
func timeMax(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() {
		return a
	}
	if a.After(b) {
		return a
	}
	return b
}

// canModify reports whether the given account is authorized to edit or
// soft-delete the target message. Under sender-only authorization, the caller
// must be the message's original sender; room-owner roles are not honored.
// The helper treats an empty account on either side as unauthorized to avoid
// matching messages with missing sender data.
func canModify(msg *models.Message, account string) bool {
	if msg == nil {
		return false
	}
	if account == "" {
		return false
	}
	if msg.Sender.Account == "" {
		return false
	}
	return msg.Sender.Account == account
}

// stripRichContent clears the heavy fields shared by both placeholder paths,
// so "which fields are heavy" lives in one place as the model grows.
func stripRichContent(m *models.Message) {
	m.Mentions = nil
	m.Attachments = nil
	m.DecodedAttachments = nil
	m.Card = nil
	m.CardAction = nil
	m.QuotedParentMessage = nil
	m.Reactions = nil
	m.SysMsgData = nil
}

// blankOversize strips a row that alone exceeds the budget so the page can be
// sent and the client can page past it; identifiers, sender, timestamps and
// Type stay for placeholder rendering. Type is kept unlike
// redactUnavailablePins — the caller may see this row, it is merely too large.
func blankOversize(m *models.Message) {
	stripRichContent(m)
	m.Msg = ""
	// The encrypted body is the payload in an E2E room — leaving it would
	// defeat the strip for exactly the rooms most likely to overflow.
	m.EncPayload = nil
	m.EncMeta = nil
	m.Truncated = true
}

// fitPage trims msgs to the budget and reports whether rows were dropped, for
// the caller's "more" flag. The cut lands on a timestamp boundary: the client
// resumes with a strict created_at < before, so ending mid-millisecond would
// skip every tied row for good.
func (s *HistoryService) fitPage(ctx context.Context, msgs []models.Message, envelope int) ([]models.Message, bool, error) {
	kept, oversize, err := pagefit.Fit(msgs, s.pageBudget, envelope)
	if err != nil {
		return nil, false, fmt.Errorf("fit history page: %w", err)
	}
	if oversize {
		blankOversize(&kept[0])
	}
	if len(kept) == len(msgs) {
		return kept, false, nil
	}
	start, end := timestampRun(msgs, len(kept))
	n := start
	if n == 0 {
		// The kept prefix sits inside one millisecond that continues past it.
		// Nothing can be cut without losing rows, so keep the run whole and
		// shrink its rows instead.
		n = end
		blankRange(msgs[:n])
		s.warnIfStillOversize(ctx, msgs[:n], envelope)
	}
	return msgs[:n], n < len(msgs), nil
}

// fitWindow trims a centred window to the budget, blanking the pivot when it
// alone will not fit. Both edges come off a shared timestamp for the same
// reason fitPage cuts on boundaries — the client pages outward by time.
func (s *HistoryService) fitWindow(ctx context.Context, msgs []models.Message, pivot, envelope int) (int, int, error) {
	lo, hi, oversize, err := pagefit.FitWindow(msgs, pivot, s.pageBudget, envelope)
	if err != nil {
		return 0, 0, fmt.Errorf("fit surrounding window: %w", err)
	}
	if oversize {
		blankOversize(&msgs[lo])
	}
	lo, hi, expanded := timestampBounds(msgs, lo, hi, pivot)
	// Only an expanded window can exceed what FitWindow approved; measuring
	// otherwise would cost a full sizing pass on every read.
	if expanded {
		s.warnIfStillOversize(ctx, msgs[lo:hi], envelope)
	}
	return lo, hi, nil
}

// warnIfStillOversize reports a page that outgrew the budget after being kept
// whole for a timestamp run. Trimming it would split the millisecond and lose
// rows for good, so the rows are returned and the transport's
// response_too_large surfaces the problem instead of hiding it.
func (s *HistoryService) warnIfStillOversize(ctx context.Context, msgs []models.Message, envelope int) {
	kept, _, err := pagefit.Fit(msgs, s.pageBudget, envelope)
	if err != nil || len(kept) == len(msgs) {
		return
	}
	slog.WarnContext(ctx, "timestamp run exceeds the reply budget even blanked",
		"rows", len(msgs), "budget_bytes", s.pageBudget.Bytes(),
		"request_id", natsutil.RequestIDFromContext(ctx))
}

// timestampRun returns [start,end) of the maximal run of rows sharing msgs[i]'s
// timestamp. One primitive so the tie predicate is stated once.
func timestampRun(msgs []models.Message, i int) (int, int) {
	at := msgs[i].CreatedAt
	start, end := i, i+1
	for start > 0 && msgs[start-1].CreatedAt.Equal(at) {
		start--
	}
	for end < len(msgs) && msgs[end].CreatedAt.Equal(at) {
		end++
	}
	return start, end
}

// timestampBounds pulls [lo,hi) off any shared timestamp at its edges. An edge
// that cannot retreat without losing the pivot expands instead, blanking the
// rows it takes on so the run is delivered whole rather than split. Reports
// whether it expanded, since only then can the span exceed the approved size.
func timestampBounds(msgs []models.Message, lo, hi, pivot int) (int, int, bool) {
	pivot = min(max(pivot, 0), len(msgs)-1)
	expanded := false
	if lo > 0 && msgs[lo].CreatedAt.Equal(msgs[lo-1].CreatedAt) {
		start, end := timestampRun(msgs, lo)
		if pivot < end {
			blankRange(msgs[start:pivot])
			lo, expanded = start, true
		} else {
			lo = end
		}
	}
	if hi < len(msgs) && msgs[hi-1].CreatedAt.Equal(msgs[hi].CreatedAt) {
		start, end := timestampRun(msgs, hi-1)
		if pivot >= start {
			blankRange(msgs[pivot+1 : end])
			hi, expanded = end, true
		} else {
			hi = start
		}
	}
	return lo, hi, expanded
}

// blankRange shrinks every row in the slice.
func blankRange(msgs []models.Message) {
	for i := range msgs {
		blankOversize(&msgs[i])
	}
}
