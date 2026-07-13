// Package msgraph is a minimal Microsoft Graph client for the chat Teams
// integration. It supports the client-credentials (app-only) OAuth2 flow and
// creating an onlineMeeting. Only the surface room-service needs is exposed,
// and it sits behind the Client interface so the meetings RPC can be unit
// tested against a mock without reaching Azure.
package msgraph

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client is the Graph surface room-service depends on. Only the meetings RPC
// touches Graph, so this is intentionally tiny. Mocked in tests.
type Client interface {
	// CreateOnlineMeeting creates (or returns the existing) onlineMeeting on
	// behalf of the configured organizer and returns its ID and join URL. It
	// uses Graph's idempotent createOrGet endpoint keyed on req.ExternalID, so
	// concurrent or repeated calls with the same ExternalID return the same
	// meeting — Graph itself is the idempotency source of truth.
	CreateOnlineMeeting(ctx context.Context, req CreateOnlineMeetingRequest) (*OnlineMeeting, error)
}

// DirectoryReader resolves accounts to Azure object IDs. Kept separate from
// Client so consumers that only need meetings (room-service) don't depend on
// the user-lookup surface. App-only (User.Read.All).
type DirectoryReader interface {
	// ResolveAccountIDs resolves account local-parts to Azure object IDs by
	// matching startsWith(userPrincipalName,'account@') — domain-agnostic, so
	// accounts under different domains still resolve. Accounts must be lowercase;
	// the result is keyed by account. Batched into chunked $filter queries.
	ResolveAccountIDs(ctx context.Context, accounts []string) (map[string]string, error)
}

// NewDirectoryClient returns an app-only directory reader (shares the graph
// client used for meetings; New always returns a *graphClient).
func NewDirectoryClient(cfg *Config, opts ...Option) DirectoryReader {
	return New(cfg, opts...).(*graphClient)
}

// UserLister walks the tenant's user directory page by page. Kept separate
// from Client/DirectoryReader so consumers depend only on the surface they
// use. App-only (User.Read.All).
type UserLister interface {
	// ListUsers calls fn once per page of up to pageSize users
	// (GET /users?$select=id,userPrincipalName&$top={pageSize}), following
	// @odata.nextLink until the directory is exhausted. A non-nil error from
	// fn aborts the walk.
	ListUsers(ctx context.Context, pageSize int, fn func([]GraphUser) error) error
}

// NewUserListerClient returns an app-only user lister (shares the graph
// client used for meetings; New always returns a *graphClient).
func NewUserListerClient(cfg *Config, opts ...Option) UserLister {
	return New(cfg, opts...).(*graphClient)
}

// GraphUser is the subset of a Graph user resource we decode when resolving
// accounts to object IDs.
type GraphUser struct {
	ID                string `json:"id"`
	UserPrincipalName string `json:"userPrincipalName"`
}

// CreateOnlineMeetingRequest carries the attributes used to create a meeting.
type CreateOnlineMeetingRequest struct {
	// ExternalID is the stable per-room idempotency key passed to Graph's
	// createOrGet endpoint. Graph guarantees exactly one meeting per
	// (organizer, externalId), so repeated/concurrent calls with the same
	// ExternalID return the same meeting instead of creating duplicates.
	// Required: createOrGet rejects an empty externalId.
	ExternalID string
	// Subject is the meeting title shown in Teams.
	Subject string
	// OrganizerEmail is the user the meeting is created for (the organizer).
	// When empty the application-context default mailbox is used.
	OrganizerEmail string
	// AttendeeEmails are the invited attendees (excluding the organizer).
	AttendeeEmails []string
}

// OnlineMeeting is the subset of the Graph onlineMeeting resource we return.
type OnlineMeeting struct {
	ID      string `json:"id"`
	JoinURL string `json:"joinWebUrl"`
}

// Config holds the Azure app-registration credentials and tenant.
type Config struct {
	TenantID     string
	ClientID     string
	ClientSecret string
	// TLSInsecureSkipVerify disables Graph TLS verification. Opt-in, dev/on-prem
	// only (e.g. a self-signed cert fronting Graph). Never enable in production.
	TLSInsecureSkipVerify bool
	// ProxyURL, when non-empty, routes the presence client's HTTP requests
	// through this proxy (overriding HTTPS_PROXY/HTTP_PROXY). Honored only by the
	// presence client (NewPresenceClient); the app-only and directory clients
	// ignore it. Must include a scheme and host (e.g. "http://proxy.corp:8080").
	ProxyURL string
	// UserAgent overrides the User-Agent header sent on presence requests. When
	// empty the presence client falls back to defaultUserAgent. Honored only by
	// the presence client (NewPresenceClient). Set this to whatever a fronting
	// proxy/WAF expects (e.g. a browser string) when the default is rejected.
	UserAgent string
}

const (
	defaultGraphBaseURL = "https://graph.microsoft.com/v1.0"
	graphScope          = "https://graph.microsoft.com/.default"
	// tokenExpirySkew is subtracted from the token's reported lifetime so the
	// cached token is refreshed before the server-side expiry.
	tokenExpirySkew = 60 * time.Second
)

// graphClient is the live (*Client) implementation.
type graphClient struct {
	cfg        Config
	httpClient *http.Client
	baseURL    string
	tokenURL   string

	mu      sync.Mutex
	token   string
	tokenAt time.Time // when the cached token expires
}

// Option customizes the client (used in tests to point at an httptest server).
type Option func(*graphClient)

// WithHTTPClient overrides the HTTP client.
func WithHTTPClient(c *http.Client) Option {
	return func(g *graphClient) { g.httpClient = c }
}

// WithBaseURL overrides the Graph API base URL (no trailing slash).
func WithBaseURL(u string) Option {
	return func(g *graphClient) { g.baseURL = strings.TrimRight(u, "/") }
}

// WithTokenURL overrides the OAuth2 token endpoint.
func WithTokenURL(u string) Option {
	return func(g *graphClient) { g.tokenURL = u }
}

// New constructs a live Graph client for the given config.
func New(cfg *Config, opts ...Option) Client {
	hc := &http.Client{Timeout: 30 * time.Second}
	if cfg.TLSInsecureSkipVerify {
		// Clone the default transport so proxy (ProxyFromEnvironment) and dial
		// settings survive — an on-prem Graph behind a self-signed cert is the
		// scenario most likely to also sit behind a corporate proxy.
		tr := http.DefaultTransport.(*http.Transport).Clone()
		// #nosec G402 -- InsecureSkipVerify is opt-in via TLSInsecureSkipVerify config for dev/on-prem environments
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12} //nolint:gosec
		hc.Transport = tr
	}
	g := &graphClient{
		cfg:        *cfg,
		httpClient: hc,
		baseURL:    defaultGraphBaseURL,
		tokenURL: fmt.Sprintf(
			"https://login.microsoftonline.com/%s/oauth2/v2.0/token",
			url.PathEscape(cfg.TenantID),
		),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	Error       string `json:"error"`
	ErrorDesc   string `json:"error_description"`
}

// accessToken returns a cached bearer token, fetching a fresh one via the
// client-credentials grant when the cache is empty or near expiry.
func (g *graphClient) accessToken(ctx context.Context) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.token != "" && time.Now().Before(g.tokenAt) {
		return g.token, nil
	}

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", g.cfg.ClientID)
	form.Set("client_secret", g.cfg.ClientSecret)
	form.Set("scope", graphScope)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("request token: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read token response: %w", err)
	}

	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", fmt.Errorf("decode token response (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK || tr.AccessToken == "" {
		// Never log the credentials; surface the OAuth error code/description only.
		return "", fmt.Errorf("token endpoint returned status %d: %s", resp.StatusCode, tr.Error)
	}

	g.token = tr.AccessToken
	lifetime := time.Duration(tr.ExpiresIn) * time.Second
	if lifetime <= tokenExpirySkew {
		lifetime = tokenExpirySkew
	}
	g.tokenAt = time.Now().Add(lifetime - tokenExpirySkew)
	return g.token, nil
}

// maxUserFilterClauses caps startsWith clauses per $filter query. Microsoft
// Graph rejects overly complex filters, so accounts are looked up in chunks.
const maxUserFilterClauses = 15

// maxAccountsPerQuery bounds accounts per query. Each account emits both a
// lower- and upper-cased startsWith clause (see casedVariants), so up to two
// clauses each — keep the total within maxUserFilterClauses.
const maxAccountsPerQuery = maxUserFilterClauses / 2

// ResolveAccountIDs resolves account local-parts to their Azure object IDs,
// matching startsWith(userPrincipalName,'account@') so any domain resolves. Both
// the lower- and upper-cased account are matched (rather than relying on Graph's
// case-insensitivity). Accounts must be lowercase; the result is keyed by
// account (the returned UPN's local-part, lowercased). Batched into chunked
// $filter queries; first match wins on a duplicate local-part.
func (g *graphClient) ResolveAccountIDs(ctx context.Context, accounts []string) (map[string]string, error) {
	out := make(map[string]string, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	token, err := g.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire graph token: %w", err)
	}
	want := make(map[string]struct{}, len(accounts))
	for _, a := range accounts {
		want[a] = struct{}{}
	}
	for start := 0; start < len(accounts); start += maxAccountsPerQuery {
		end := min(start+maxAccountsPerQuery, len(accounts))
		if err := g.resolveChunk(ctx, token, accounts[start:end], want, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// casedVariants returns the lower- and upper-cased forms of s (deduped when the
// value has no cased letters), so the startsWith filter matches UPNs stored in
// either case.
func casedVariants(s string) []string {
	lower, upper := strings.ToLower(s), strings.ToUpper(s)
	if lower == upper {
		return []string{lower}
	}
	return []string{lower, upper}
}

// localPart returns the part before the first '@' in an email/UPN.
func localPart(email string) (string, bool) {
	at := strings.Index(email, "@")
	if at <= 0 {
		return "", false
	}
	return email[:at], true
}

// resolveChunk queries one chunk of accounts and records the account->id matches
// (keyed by the lowercased UPN local-part) into out.
func (g *graphClient) resolveChunk(ctx context.Context, token string, chunk []string, want map[string]struct{}, out map[string]string) error {
	clauses := make([]string, 0, len(chunk)*2)
	for _, a := range chunk {
		for _, variant := range casedVariants(a) {
			// Escape single quotes for the OData string literal.
			esc := strings.ReplaceAll(variant, "'", "''")
			clauses = append(clauses, fmt.Sprintf("startsWith(userPrincipalName,'%s@')", esc))
		}
	}
	q := url.Values{}
	q.Set("$filter", strings.Join(clauses, " or "))
	q.Set("$select", "id,userPrincipalName")
	// $count pairs with ConsistencyLevel: eventual to satisfy Graph's advanced-
	// query contract for startsWith on userPrincipalName.
	q.Set("$count", "true")
	endpoint := g.baseURL + "/users?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build get-users request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	// startsWith on userPrincipalName is served as an advanced query — request
	// eventual consistency so Graph accepts it regardless of tenant defaults.
	req.Header.Set("ConsistencyLevel", "eventual")
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("get users: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
	if err != nil {
		return fmt.Errorf("read get-users response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("get users: graph returned status %d", resp.StatusCode)
	}
	var page struct {
		Value []GraphUser `json:"value"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		return fmt.Errorf("decode get-users response: %w", err)
	}
	for _, u := range page.Value {
		if u.ID == "" {
			continue
		}
		local, ok := localPart(u.UserPrincipalName)
		if !ok {
			continue
		}
		account := strings.ToLower(local)
		if _, mine := want[account]; !mine {
			continue
		}
		if _, dup := out[account]; dup {
			continue // first match wins
		}
		out[account] = u.ID
	}
	return nil
}

// onlineMeetingPayload is the Graph createOrGet-onlineMeeting request body.
// externalId is required by createOrGet and is the per-room idempotency key.
type onlineMeetingPayload struct {
	ExternalID   string               `json:"externalId"`
	Subject      string               `json:"subject,omitempty"`
	Participants *meetingParticipants `json:"participants,omitempty"`
}

type meetingParticipants struct {
	Attendees []meetingAttendee `json:"attendees,omitempty"`
}

type meetingAttendee struct {
	Upn string `json:"upn"`
}

func (g *graphClient) CreateOnlineMeeting(ctx context.Context, req CreateOnlineMeetingRequest) (*OnlineMeeting, error) {
	if req.ExternalID == "" {
		return nil, fmt.Errorf("create onlineMeeting: externalId is required for createOrGet idempotency")
	}
	token, err := g.accessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire graph token: %w", err)
	}

	// createOrGet pushes idempotency to Graph: it returns the existing meeting
	// for an (organizer, externalId) pair if one exists, otherwise creates one.
	// App-only context requires targeting a specific organizer mailbox via the
	// /users/{id}/onlineMeetings/createOrGet path; delegated context uses /me.
	// We use the organizer-scoped path when an organizer email is supplied.
	var endpoint string
	if req.OrganizerEmail != "" {
		endpoint = fmt.Sprintf("%s/users/%s/onlineMeetings/createOrGet", g.baseURL, url.PathEscape(req.OrganizerEmail))
	} else {
		endpoint = g.baseURL + "/me/onlineMeetings/createOrGet"
	}

	payload := onlineMeetingPayload{ExternalID: req.ExternalID, Subject: req.Subject}
	if len(req.AttendeeEmails) > 0 {
		attendees := make([]meetingAttendee, 0, len(req.AttendeeEmails))
		for _, email := range req.AttendeeEmails {
			attendees = append(attendees, meetingAttendee{Upn: email})
		}
		payload.Participants = &meetingParticipants{Attendees: attendees}
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal onlineMeeting payload: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("build onlineMeeting request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := g.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("create onlineMeeting: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read onlineMeeting response: %w", err)
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		// Never wrap the raw response body into the error/cause — it can carry
		// upstream payload. Parse the Graph error envelope and surface only the
		// status + sanitized error code.
		var graphErr struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(respBody, &graphErr)
		if graphErr.Error.Code != "" {
			return nil, fmt.Errorf("create onlineMeeting: graph returned status %d (%s)", resp.StatusCode, graphErr.Error.Code)
		}
		return nil, fmt.Errorf("create onlineMeeting: graph returned status %d", resp.StatusCode)
	}

	var meeting OnlineMeeting
	if err := json.Unmarshal(respBody, &meeting); err != nil {
		return nil, fmt.Errorf("decode onlineMeeting response: %w", err)
	}
	if meeting.JoinURL == "" {
		return nil, fmt.Errorf("create onlineMeeting: graph response missing joinWebUrl")
	}
	return &meeting, nil
}

// usersPage is one page of the /users walk.
type usersPage struct {
	Value    []GraphUser `json:"value"`
	NextLink string      `json:"@odata.nextLink"`
}

// ListUsers walks GET /users page by page, invoking fn per page. The first
// request carries $select/$top; subsequent pages follow Graph's opaque
// @odata.nextLink verbatim (it embeds the paging state).
func (g *graphClient) ListUsers(ctx context.Context, pageSize int, fn func([]GraphUser) error) error {
	token, err := g.accessToken(ctx)
	if err != nil {
		return fmt.Errorf("acquire graph token: %w", err)
	}
	q := url.Values{}
	q.Set("$select", "id,userPrincipalName")
	q.Set("$top", strconv.Itoa(pageSize))
	next := g.baseURL + "/users?" + q.Encode()
	for next != "" {
		page, err := g.fetchUsersPage(ctx, token, next)
		if err != nil {
			return err
		}
		if err := fn(page.Value); err != nil {
			return fmt.Errorf("process users page: %w", err)
		}
		next = page.NextLink
	}
	return nil
}

func (g *graphClient) fetchUsersPage(ctx context.Context, token, endpoint string) (*usersPage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build list-users request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<22))
	if err != nil {
		return nil, fmt.Errorf("read list-users response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// Never wrap the response body — surface the status only.
		return nil, fmt.Errorf("list users: graph returned status %d", resp.StatusCode)
	}
	var page usersPage
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, fmt.Errorf("decode list-users response: %w", err)
	}
	return &page, nil
}
