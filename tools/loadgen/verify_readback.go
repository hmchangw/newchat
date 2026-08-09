package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/hmchangw/chat/pkg/subject"
)

// ReadbackTarget is one probe to confirm in history.
type ReadbackTarget struct {
	MsgID          string
	RoomID         string
	SenderID       string
	ThreadParentID string
}

// getMessagesByIDsRequest mirrors history-service's msg.get.ids batch RPC body
// (models.GetMessagesByIDsRequest). Kept as a local copy rather than an
// import: that type lives under history-service/internal/models, which Go's
// internal-package rule already forbids importing from tools/loadgen.
type getMessagesByIDsRequest struct {
	MessageIDs []string `json:"messageIds"`
}

// historyMessage is the narrow projection verify decodes from history-service's
// msg.get.ids reply (models.GetMessagesByIDsResponse's Message entries,
// cassandra.Message on the wire). Content (Msg) is deliberately absent --
// verify asserts metadata only, which keeps it clear of encryption and the
// no-logging-bodies rule, and never needs a message body in memory at all.
type historyMessage struct {
	MessageID      string `json:"messageId"`
	ThreadParentID string `json:"threadParentId"`
	Sender         struct {
		ID string `json:"id"`
	} `json:"sender"`
}

type historyResponse struct {
	Messages []historyMessage `json:"messages"`
}

// maxBatchIDs mirrors history-service's maxGetByIDsBatchSize
// (history-service/internal/service/messages.go): requests over this size are
// rejected with errcode.BadRequest("too many messageIds"), so verify must
// chunk a room's probes rather than sending them all in one request.
const maxBatchIDs = 100

// Readback confirms probes persisted, via the history-service msg.get.ids RPC
// rather than direct CQL. This exercises the real client read path and avoids
// coupling the test to MESSAGE_BUCKET_HOURS -- a hand-rolled bucket
// computation that ever drifted from what the services run with would target
// the wrong Cassandra partition and report phantom data loss.
type Readback struct {
	req      requestFn
	siteID   string
	attempts int
	backoff  time.Duration
}

// NewReadback returns a Readback with a bounded retry budget.
func NewReadback(req requestFn, siteID string, attempts int, backoff time.Duration) *Readback {
	if attempts < 1 {
		attempts = 1
	}
	return &Readback{req: req, siteID: siteID, attempts: attempts, backoff: backoff}
}

// Verify checks every target, batched per room. Returns violations for probes
// that are genuinely absent or stored wrong. Returns an error -- never a
// violation -- when the query itself fails, since an unreachable service says
// nothing about whether the write happened.
func (r *Readback) Verify(ctx context.Context, account string, targets []ReadbackTarget) ([]Violation, error) {
	byRoom := map[string][]ReadbackTarget{}
	for _, t := range targets {
		byRoom[t.RoomID] = append(byRoom[t.RoomID], t)
	}

	roomIDs := make([]string, 0, len(byRoom))
	for id := range byRoom {
		roomIDs = append(roomIDs, id)
	}
	sort.Strings(roomIDs)

	var out []Violation
	for _, roomID := range roomIDs {
		vs, err := r.verifyRoom(ctx, account, roomID, byRoom[roomID])
		if err != nil {
			return nil, fmt.Errorf("readback room %s: %w", roomID, err)
		}
		out = append(out, vs...)
	}
	return out, nil
}

// verifyRoom queries one room's still-pending probes -- chunked into at most
// maxBatchIDs message IDs per request -- and retries only while something is
// still missing; a room fully resolved on the first attempt is not queried
// again.
func (r *Readback) verifyRoom(ctx context.Context, account, roomID string, targets []ReadbackTarget) ([]Violation, error) {
	pending := make(map[string]ReadbackTarget, len(targets))
	for _, t := range targets {
		pending[t.MsgID] = t
	}

	var mismatches []Violation

	for attempt := 0; attempt < r.attempts && len(pending) > 0; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("readback cancelled: %w", ctx.Err())
			case <-time.After(r.backoff):
			}
		}

		ids := make([]string, 0, len(pending))
		for id := range pending {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		for _, chunk := range chunkIDs(ids, maxBatchIDs) {
			resp, err := r.queryChunk(ctx, account, roomID, chunk)
			if err != nil {
				return nil, err
			}
			for _, hm := range resp.Messages {
				want, ok := pending[hm.MessageID]
				if !ok {
					continue
				}
				delete(pending, hm.MessageID)
				if v, bad := compareStored(want, hm); bad {
					mismatches = append(mismatches, v)
				}
			}
		}
	}

	missing := make([]string, 0, len(pending))
	for id := range pending {
		missing = append(missing, id)
	}
	sort.Strings(missing)
	for _, id := range missing {
		mismatches = append(mismatches, Violation{
			Kind: KindPersistenceMiss, MsgID: id, RoomID: roomID,
			Detail: fmt.Sprintf("absent from history after %d attempts", r.attempts),
		})
	}
	return mismatches, nil
}

// queryChunk issues one msg.get.ids request for at most maxBatchIDs message
// IDs. ids is always non-empty: verifyRoom only calls this while pending is
// non-empty, and history-service rejects an empty messageIds list.
func (r *Readback) queryChunk(ctx context.Context, account, roomID string, ids []string) (historyResponse, error) {
	body, err := json.Marshal(getMessagesByIDsRequest{MessageIDs: ids})
	if err != nil {
		return historyResponse{}, fmt.Errorf("marshal history query: %w", err)
	}
	raw, err := r.req(ctx, subject.MsgGetIDs(account, roomID, r.siteID), body, defaultRequestTimeout)
	if err != nil {
		return historyResponse{}, fmt.Errorf("history query: %w", err)
	}
	var resp historyResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return historyResponse{}, fmt.Errorf("decode history response: %w", err)
	}
	return resp, nil
}

// chunkIDs splits ids into groups of at most n, preserving order.
func chunkIDs(ids []string, n int) [][]string {
	if len(ids) == 0 {
		return nil
	}
	chunks := make([][]string, 0, (len(ids)+n-1)/n)
	for len(ids) > 0 {
		end := n
		if end > len(ids) {
			end = len(ids)
		}
		chunks = append(chunks, ids[:end])
		ids = ids[end:]
	}
	return chunks
}

// compareStored checks the stored metadata against what was published.
//
// RoomID is deliberately NOT compared here: history-service's msg.get.ids
// scopes its Cassandra fetch to the roomID baked into the request subject and
// silently drops any returned row whose actual RoomID doesn't match
// (history-service/internal/service/messages.go, GetMessagesByIDs). A message
// persisted under the wrong room can therefore only ever come back absent --
// never present with a mismatched roomId -- so a roomID check here would be
// permanently dead code. Do not re-add it; that class of bug surfaces as
// KindPersistenceMiss instead.
func compareStored(want ReadbackTarget, got historyMessage) (Violation, bool) {
	var reason string
	switch {
	case got.Sender.ID != want.SenderID:
		reason = fmt.Sprintf("senderId=%s want %s", got.Sender.ID, want.SenderID)
	case got.ThreadParentID != want.ThreadParentID:
		reason = fmt.Sprintf("threadParentId=%s want %s", got.ThreadParentID, want.ThreadParentID)
	default:
		return Violation{}, false
	}
	return Violation{
		Kind: KindPersistenceMismatch, MsgID: want.MsgID, RoomID: want.RoomID,
		Detail: reason,
	}, true
}
