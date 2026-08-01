package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/restyutil"
)

// verifyPath is the inspector's verification endpoint.
const verifyPath = "/internal/teams/rooms/verify"

// verifyFunc asks one site's inspector about a batch of chat ids. Injected into
// the runner so unit tests exercise the comparison logic without HTTP.
type verifyFunc func(ctx context.Context, baseURL string, chatIDs []string) (*model.TeamsRoomVerifyResponse, error)

// newHTTPVerifier returns a verifyFunc backed by one shared Resty client. The
// base URL varies per site, so each call posts to an absolute URL rather than
// the client's base.
func newHTTPVerifier(timeout time.Duration) verifyFunc {
	client := restyutil.New("", restyutil.WithTimeout(timeout))
	return func(ctx context.Context, baseURL string, chatIDs []string) (*model.TeamsRoomVerifyResponse, error) {
		var out model.TeamsRoomVerifyResponse
		resp, err := client.R().
			SetContext(ctx).
			SetBody(model.TeamsRoomVerifyRequest{ChatIDs: chatIDs}).
			SetResult(&out).
			Post(baseURL + verifyPath)
		if err != nil {
			return nil, fmt.Errorf("call inspector at %q: %w", baseURL, err)
		}
		if resp.IsError() {
			return nil, fmt.Errorf("inspector at %q returned status %d", baseURL, resp.StatusCode())
		}
		// A syntactically valid 200 body that carries no results (e.g. a buggy or
		// version-mismatched inspector replying `{}` or an empty chats array) is
		// treated as a failed call rather than "every chat is missing its room" —
		// the latter reading would leave every chat flagged forever.
		if len(out.Chats) == 0 {
			return nil, fmt.Errorf("inspector at %q returned no results for %d chat ids", baseURL, len(chatIDs))
		}
		return &out, nil
	}
}
