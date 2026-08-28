package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

// messageLink builds the canonical deep link to a message from trusted inputs.
// Single source of truth for the link format, shared by the authoritative
// history fetch (fetcher_history.go) and the degraded-mode placeholder snapshot
// (handler.go) so the two paths can't drift. baseURL is operator-supplied
// (CHAT_BASE_URL); its trailing slash is trimmed so the link never doubles up.
func messageLink(baseURL, roomID, messageID string) string {
	return fmt.Sprintf("%s/%s/%s", strings.TrimRight(baseURL, "/"), roomID, messageID)
}

// isBot reports whether account is bot-like — a real ".bot" bot or the
// "p_admin" platform-admin pseudo-account — via the model taxonomy. Plain
// "p_" QA test accounts are ordinary users and return false.
func isBot(account string) bool {
	return model.IsBot(account) || model.IsPlatformAdminAccount(account)
}

// threadParentMissing reports whether a parent fetch failed with the one verdict
// a re-check can overturn: history says the message is not there, which is also
// what an unlanded write looks like. Forbidden and bad_request read the same on
// every attempt, and a transient failure already degrades rather than rejects.
func threadParentMissing(err error) bool {
	var ee *errcode.Error
	return quoteFetchErrIsTerminal(err) && errors.As(err, &ee) && ee.Code == errcode.CodeNotFound
}

// waitFor blocks for d, returning the context's error if the caller goes away first.
func waitFor(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
