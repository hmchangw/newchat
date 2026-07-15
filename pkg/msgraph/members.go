package msgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

// ChatMembersReader lists a chat's members. Consumed by teams-chat-member-sync;
// kept separate from ChatsReader so consumers depend only on the surface they
// use. App-only (Chat.Read.All / ChatMember.Read.All).
type ChatMembersReader interface {
	// ListChatMembers returns the chat's members, following @odata.nextLink
	// pagination. Throttled (429/503) responses are retried per Retry-After and
	// arm the shared tenant-wide gate, exactly like ListUserChats.
	ListChatMembers(ctx context.Context, chatID string) ([]ChatMemberDetail, error)
}

// NewChatMembersClient returns an app-only chat-members reader (shares the
// graph client; New always returns a *graphClient).
//
//nolint:gocritic // hugeParam: startup-only constructor; Config passed by value is intentional.
func NewChatMembersClient(cfg Config, opts ...Option) ChatMembersReader {
	return New(cfg, opts...).(*graphClient)
}

// ChatMemberDetail is the subset of an aadUserConversationMember returned by
// GET /chats/{id}/members. Only the UPN is consulted for the account; the
// Graph email field is intentionally not requested.
type ChatMemberDetail struct {
	UserID                      string    `json:"userId"`
	UserPrincipalName           string    `json:"userPrincipalName"`
	VisibleHistoryStartDateTime time.Time `json:"visibleHistoryStartDateTime"`
}

func (g *graphClient) ListChatMembers(ctx context.Context, chatID string) ([]ChatMemberDetail, error) {
	if chatID == "" {
		return nil, fmt.Errorf("list chat members: chatID is required")
	}
	token, err := g.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire graph token: %w", err)
	}

	// GET /chats/{id}/members uses server-driven paging (@odata.nextLink) and
	// does not accept $top, so we only $select the fields we need.
	q := url.Values{}
	q.Set("$select", "userId,userPrincipalName,visibleHistoryStartDateTime")
	next := fmt.Sprintf("%s/chats/%s/members?%s", g.baseURL, url.PathEscape(chatID), q.Encode())

	var members []ChatMemberDetail
	for next != "" {
		body, err := g.getThrottled(ctx, token, next)
		if err != nil {
			return nil, fmt.Errorf("list chat members: %w", err)
		}
		var page struct {
			Value    []ChatMemberDetail `json:"value"`
			NextLink string             `json:"@odata.nextLink"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("decode chat members response: %w", err)
		}
		members = append(members, page.Value...)
		next = page.NextLink
	}
	return members, nil
}
