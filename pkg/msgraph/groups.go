package msgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// GroupReader reads a Graph group's profile and walks its user members.
// Kept separate from Client/DirectoryReader/UserLister so consumers depend
// only on the surface they use. App-only (Group.Read.All).
type GroupReader interface {
	// GetGroup fetches the group's profile (GET /groups/{id}).
	GetGroup(ctx context.Context, groupID string) (*GroupProfile, error)
	// ListGroupMembers calls fn once per page of user-typed members
	// (GET /groups/{id}/members, following @odata.nextLink), skipping non-user
	// member objects (nested groups, devices, ...) and returning their count.
	ListGroupMembers(ctx context.Context, groupID string, pageSize int, fn func([]GraphUser) error) (skipped int, err error)
}

// NewGroupReaderClient returns an app-only group reader (shares the graph
// client used for meetings; New always returns a *graphClient).
//
//nolint:gocritic // hugeParam: startup-only constructor; Config passed by value is intentional.
func NewGroupReaderClient(cfg Config, opts ...Option) GroupReader {
	return New(cfg, opts...).(*graphClient)
}

// GroupProfile is the subset of a Graph group resource we decode.
type GroupProfile struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	Description string `json:"description"`
}

// graphUserType is the @odata.type marking a member object as a user.
const graphUserType = "#microsoft.graph.user"

// memberSelect is the $select for the members walk — the identity fields the
// HR sync maps into an Employee.
const memberSelect = "id,userPrincipalName,displayName,givenName,surname,employeeId,mail,mailNickname,userType,accountEnabled"

func (g *graphClient) GetGroup(ctx context.Context, groupID string) (*GroupProfile, error) {
	token, err := g.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire graph token: %w", err)
	}
	q := url.Values{}
	q.Set("$select", "id,displayName,description")
	endpoint := g.baseURL + "/groups/" + url.PathEscape(groupID) + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build get-group request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get group: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read get-group response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// Never wrap the response body — surface the status only.
		return nil, fmt.Errorf("get group: graph returned status %d", resp.StatusCode)
	}
	var profile GroupProfile
	if err := json.Unmarshal(body, &profile); err != nil {
		return nil, fmt.Errorf("decode get-group response: %w", err)
	}
	return &profile, nil
}

// memberElement is one /members element; ODataType distinguishes users from
// other directory objects (nested groups, devices).
type memberElement struct {
	GraphUser
	ODataType string `json:"@odata.type"`
}

// membersPage is one page of the /groups/{id}/members walk.
type membersPage struct {
	Value    []memberElement `json:"value"`
	NextLink string          `json:"@odata.nextLink"`
}

// ListGroupMembers walks GET /groups/{id}/members page by page, invoking fn
// per page with only the user-typed members. The first request carries
// $select/$top; subsequent pages follow Graph's opaque @odata.nextLink,
// pinned to the configured origin (same token-exfiltration guard as
// ListUsers).
func (g *graphClient) ListGroupMembers(ctx context.Context, groupID string, pageSize int, fn func([]GraphUser) error) (int, error) {
	token, err := g.accessToken(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire graph token: %w", err)
	}
	q := url.Values{}
	q.Set("$select", memberSelect)
	q.Set("$top", strconv.Itoa(pageSize))
	origin, err := url.Parse(g.baseURL)
	if err != nil {
		return 0, fmt.Errorf("parse graph base URL: %w", err)
	}
	skipped := 0
	next := g.baseURL + "/groups/" + url.PathEscape(groupID) + "/members?" + q.Encode()
	for next != "" {
		nextURL, err := url.Parse(next)
		if err != nil {
			return skipped, fmt.Errorf("parse nextLink: %w", err)
		}
		if nextURL.Scheme != origin.Scheme || nextURL.Host != origin.Host {
			return skipped, fmt.Errorf("nextLink %q deviates from configured graph origin %q", next, g.baseURL)
		}
		page, err := g.fetchMembersPage(ctx, token, next)
		if err != nil {
			return skipped, err
		}
		users := make([]GraphUser, 0, len(page.Value))
		for _, m := range page.Value {
			if m.ODataType != graphUserType {
				skipped++
				continue
			}
			users = append(users, m.GraphUser)
		}
		if err := fn(users); err != nil {
			return skipped, fmt.Errorf("process members page: %w", err)
		}
		next = page.NextLink
	}
	return skipped, nil
}

func (g *graphClient) fetchMembersPage(ctx context.Context, token, endpoint string) (*membersPage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build list-members request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list group members: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
	if err != nil {
		return nil, fmt.Errorf("read list-members response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// Never wrap the response body — surface the status only.
		return nil, fmt.Errorf("list group members: graph returned status %d", resp.StatusCode)
	}
	var page membersPage
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, fmt.Errorf("decode list-members response: %w", err)
	}
	return &page, nil
}
