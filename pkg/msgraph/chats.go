package msgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// ChatsReader lists a user's Teams chats. Consumed by teams-chat-sync; kept
// separate from Client/DirectoryReader so meeting/directory consumers don't
// depend on the chats surface. App-only (Chat.Read.All).
type ChatsReader interface {
	// ListUserChats returns the user's chats whose lastUpdatedDateTime falls in
	// the half-open window [from, to), with members expanded. It follows
	// @odata.nextLink pagination. Throttled (429/503) responses are retried
	// per Retry-After AND arm a tenant-wide gate shared by all goroutines on
	// this client, since Graph throttles per app+tenant.
	ListUserChats(ctx context.Context, userID string, from, to time.Time) ([]Chat, error)
}

// NewChatsClient returns an app-only chats reader (shares the graph client
// used for meetings; New always returns a *graphClient).
//
//nolint:gocritic // hugeParam: startup-only constructor; Config passed by value is intentional.
func NewChatsClient(cfg Config, opts ...Option) (ChatsReader, error) {
	g := New(cfg, opts...).(*graphClient)
	if err := applyProxyURL(g.httpClient, cfg.ProxyURL); err != nil {
		return nil, err
	}
	return g, nil
}

// Chat is the subset of a Graph chat resource the sync consumes.
type Chat struct {
	ID                  string       `json:"id"`
	ChatType            string       `json:"chatType"` // oneOnOne | group | meeting
	Topic               string       `json:"topic"`    // Graph null (oneOnOne/unnamed) decodes to ""
	CreatedDateTime     time.Time    `json:"createdDateTime"`
	LastUpdatedDateTime time.Time    `json:"lastUpdatedDateTime"`
	Members             []ChatMember `json:"members"`
}

// ChatMember is the subset of an aadUserConversationMember the sync consumes.
// UserID is the member's AAD object id (the teams_user _id).
type ChatMember struct {
	UserID                      string    `json:"userId"`
	VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime"`
}

// Throttle bounds for chat listing. Graph rate-limits per app+tenant, so a
// throttle response arms a client-wide gate shared by every worker goroutine
// (see noteThrottle/waitThrottle) in addition to the per-request retry loop.
// The gate is capped so a hostile Retry-After can't stall the run.
const (
	chatsMaxAttempts      = 4
	chatsDefaultRetryWait = 2 * time.Second
	chatsMaxThrottleWait  = 5 * time.Minute
)

// defaultChatsPageSize is the $top sent on the first list-chats request when
// no override is configured. 50 is Graph's documented maximum for this
// endpoint; later pages follow @odata.nextLink, which carries Graph's own
// paging parameters.
const defaultChatsPageSize = 50

// WithChatsPageSize overrides the $top page size ListUserChats requests.
// Values <= 0 fall back to defaultChatsPageSize.
func WithChatsPageSize(n int) Option {
	return func(g *graphClient) { g.chatsPageSize = n }
}

func (g *graphClient) ListUserChats(ctx context.Context, userID string, from, to time.Time) ([]Chat, error) {
	if userID == "" {
		return nil, fmt.Errorf("list user chats: userID is required")
	}
	token, err := g.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire graph token: %w", err)
	}

	q := url.Values{}
	q.Set("$filter", fmt.Sprintf(
		"lastUpdatedDateTime ge %s and lastUpdatedDateTime lt %s",
		from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339),
	))
	q.Set("$expand", "members")
	q.Set("$select", "id,chatType,topic,createdDateTime,lastUpdatedDateTime")
	pageSize := g.chatsPageSize
	if pageSize <= 0 {
		pageSize = defaultChatsPageSize
	}
	q.Set("$top", strconv.Itoa(pageSize))
	next := fmt.Sprintf("%s/users/%s/chats?%s", g.baseURL, url.PathEscape(userID), q.Encode())

	var chats []Chat
	for next != "" {
		body, err := g.getThrottled(ctx, token, next)
		if err != nil {
			return nil, err
		}
		var page struct {
			Value    []Chat `json:"value"`
			NextLink string `json:"@odata.nextLink"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("decode chats response: %w", err)
		}
		chats = append(chats, page.Value...)
		next = page.NextLink
	}
	return chats, nil
}

// getThrottled GETs a Graph URL. Every attempt first waits out the
// tenant-wide throttle gate; a 429/503 response (Graph throttles per
// app+tenant) arms/extends the gate for ALL workers sharing this client and
// retries up to chatsMaxAttempts. The gate is armed even on the final failed
// attempt so the rest of the pool still backs off after this user fails.
func (g *graphClient) getThrottled(ctx context.Context, token, endpoint string) ([]byte, error) {
	for attempt := 1; ; attempt++ {
		if err := g.waitThrottle(ctx); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("build chats request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := g.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("get chats: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
		if closeErr := resp.Body.Close(); closeErr != nil {
			return nil, fmt.Errorf("close chats response: %w", closeErr)
		}
		if readErr != nil {
			return nil, fmt.Errorf("read chats response: %w", readErr)
		}
		throttled := resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusServiceUnavailable
		if throttled {
			g.noteThrottle(resp.Header.Get("Retry-After"))
		}
		switch {
		case resp.StatusCode == http.StatusOK:
			return body, nil
		case throttled && attempt < chatsMaxAttempts:
			// Loop: the next iteration's waitThrottle waits out the gate we
			// just armed.
		default:
			// Surface only status + Graph error code; never the raw body (it can
			// carry upstream payload).
			var graphErr struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			_ = json.Unmarshal(body, &graphErr)
			if graphErr.Error.Code != "" {
				return nil, fmt.Errorf("get chats: graph returned status %d (%s)", resp.StatusCode, graphErr.Error.Code)
			}
			return nil, fmt.Errorf("get chats: graph returned status %d", resp.StatusCode)
		}
	}
}

// noteThrottle arms/extends the tenant-wide throttle gate from a Retry-After
// header (default when absent/invalid, capped against hostile values). The
// gate only ever extends — a later, shorter Retry-After never shrinks it.
func (g *graphClient) noteThrottle(retryAfter string) {
	wait := chatsDefaultRetryWait
	if secs, err := strconv.Atoi(retryAfter); err == nil && secs >= 0 {
		wait = time.Duration(secs) * time.Second
	}
	if wait > chatsMaxThrottleWait {
		wait = chatsMaxThrottleWait
	}
	until := time.Now().Add(wait)
	g.throttleMu.Lock()
	defer g.throttleMu.Unlock()
	if until.After(g.throttleUntil) {
		g.throttleUntil = until
	}
}

// throttleDeadline returns the current gate deadline (zero when unarmed).
func (g *graphClient) throttleDeadline() time.Time {
	g.throttleMu.Lock()
	defer g.throttleMu.Unlock()
	return g.throttleUntil
}

// waitThrottle blocks until the tenant-wide gate has expired, aborting early
// when ctx is done. Timer-based, not time.Sleep, so a cancelled run stops
// waiting immediately (this is backoff, not goroutine synchronization). The
// deadline is re-read after waking because another worker's 429 may have
// extended the gate while we slept.
func (g *graphClient) waitThrottle(ctx context.Context) error {
	for {
		wait := time.Until(g.throttleDeadline())
		if wait <= 0 {
			return nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("wait for graph throttle gate: %w", ctx.Err())
		case <-timer.C:
		}
	}
}
