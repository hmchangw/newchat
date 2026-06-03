package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// PresenceSnapshotter batches presence lookups for push-eligible accounts (Stage 4).
// Errors are swallowed; an absent account defaults to push.
type PresenceSnapshotter interface {
	Snapshot(ctx context.Context, accounts []string) (map[string]model.Presence, error)
}

// noopPresenceSnapshotter returns an empty map so all push-eligible recipients receive a push.
type noopPresenceSnapshotter struct{}

func (noopPresenceSnapshotter) Snapshot(context.Context, []string) (map[string]model.Presence, error) {
	return map[string]model.Presence{}, nil
}

// presenceRequester is the narrow NATS surface bulkPresenceSource uses, injectable by tests.
type presenceRequester interface {
	Request(ctx context.Context, subj string, data []byte, timeout time.Duration) (*nats.Msg, error)
}

type bulkPresenceSource struct {
	req       presenceRequester
	siteID    string
	batchSize int
	timeout   time.Duration
}

func newBulkPresenceSource(req presenceRequester, siteID string, batchSize int, timeout time.Duration) *bulkPresenceSource {
	if batchSize <= 0 {
		batchSize = 512
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &bulkPresenceSource{req: req, siteID: siteID, batchSize: batchSize, timeout: timeout}
}

func (b *bulkPresenceSource) Snapshot(ctx context.Context, accounts []string) (map[string]model.Presence, error) {
	if len(accounts) == 0 {
		return map[string]model.Presence{}, nil
	}
	subj := subject.PresenceSnapshot(b.siteID)
	chunks := chunkStrings(accounts, b.batchSize)

	var (
		mu  sync.Mutex
		out = make(map[string]model.Presence, len(accounts))
		wg  sync.WaitGroup
	)
	for _, ch := range chunks {
		ch := ch
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := json.Marshal(model.PresenceSnapshotRequest{Accounts: ch})
			if err != nil {
				slog.Warn("presence marshal failed", "error", err)
				return
			}
			msg, err := b.req.Request(ctx, subj, data, b.timeout)
			if err != nil {
				slog.Warn("presence rpc failed", "error", err, "chunk", len(ch))
				return
			}
			if errResp, ok := errcode.Parse(msg.Data); ok {
				slog.Warn("presence rpc returned error response",
					"error", errResp.Message,
					"code", errResp.Code,
					"chunk", len(ch))
				return
			}
			var reply model.PresenceSnapshotReply
			if err := json.Unmarshal(msg.Data, &reply); err != nil {
				slog.Warn("presence unmarshal failed", "error", err)
				return
			}
			mu.Lock()
			for k, v := range reply.Presences {
				out[k] = v
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out, nil
}

func chunkStrings(in []string, size int) [][]string {
	if size <= 0 || len(in) <= size {
		return [][]string{in}
	}
	out := make([][]string, 0, (len(in)+size-1)/size)
	for i := 0; i < len(in); i += size {
		end := i + size
		if end > len(in) {
			end = len(in)
		}
		out = append(out, in[i:end])
	}
	return out
}

// shouldPush returns true unless the account is explicitly DND; fail-open on missing/unknown status.
func shouldPush(p model.Presence) bool {
	switch p.AggregatedStatus {
	case "busy", "in-call":
		return false
	default:
		return true
	}
}

type natsPresenceRequester struct {
	nc *nats.Conn
}

var _ presenceRequester = (*natsPresenceRequester)(nil)

func (n *natsPresenceRequester) Request(ctx context.Context, subj string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	msg, err := n.nc.RequestWithContext(rctx, subj, data)
	if err != nil {
		return nil, fmt.Errorf("presence request: %w", err)
	}
	return msg, nil
}
