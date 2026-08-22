package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
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

// blankOversize strips the heavy fields from a row that alone exceeds the reply
// budget, so the page can be sent and the client can page past it. Identifiers,
// sender and timestamps stay for placeholder rendering.
//
// Type is deliberately kept, unlike redactUnavailablePins: that path clears it
// so a pre-access system message cannot leak event details, but here the caller
// is authorised to see this row and needs Type to pick a placeholder.
func blankOversize(m *models.Message) {
	m.Msg = ""
	m.Mentions = nil
	m.Attachments = nil
	m.DecodedAttachments = nil
	m.Card = nil
	m.CardAction = nil
	m.QuotedParentMessage = nil
	m.Reactions = nil
	m.SysMsgData = nil
	m.Truncated = true
}

// fitPage trims msgs to the reply budget, blanking a lone row that still will
// not fit. Reports whether anything was dropped so the caller can set its
// "more" flag.
func (s *HistoryService) fitPage(msgs []models.Message, envelope int) ([]models.Message, bool) {
	if len(msgs) == 0 || pagefit.Fits(msgs, s.pageBudget, envelope) {
		return msgs, false
	}
	kept := msgs[:pagefit.Prefix(msgs, s.pageBudget, envelope)]
	// A lone row that still overflows is degraded rather than dropped, so the
	// client can page past it instead of dead-ending on this position.
	if len(kept) == 1 && !pagefit.Fits(kept, s.pageBudget, envelope) {
		blankOversize(&kept[0])
	}
	return kept, len(kept) < len(msgs)
}

// fitWindow trims a centred window to the reply budget, blanking the pivot row
// when it alone will not fit so the caller still gets the row they asked for.
func (s *HistoryService) fitWindow(msgs []models.Message, pivot, envelope int) (int, int) {
	if len(msgs) == 0 || pagefit.Fits(msgs, s.pageBudget, envelope) {
		return 0, len(msgs)
	}
	lo, hi := pagefit.Window(msgs, pivot, s.pageBudget, envelope)
	if hi-lo == 1 && !pagefit.Fits(msgs[lo:hi], s.pageBudget, envelope) {
		blankOversize(&msgs[lo])
	}
	return lo, hi
}
