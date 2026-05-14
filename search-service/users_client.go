package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/model"
)

// httpUsersClient implements SearchUsersClient by proxying to a third-party
// HR endpoint via Resty. The third-party owns the search index and the
// company-scoping logic; this client is a thin adapter.
//
// Wire shape:
//   - Request:  POST to the base URL configured via USERS_API_URL.
//   - Response: JSON array of user objects decoded into []model.SearchUser.
//
// TODO(searchUsers-thirdparty): fill in the exact request body and URL path
// once the third-party endpoint spec is available. Current placeholder sends
// the query as a JSON body field and expects a top-level JSON array response.
type httpUsersClient struct {
	rc    *resty.Client
	token string // optional Bearer token; empty string means no Authorization header
}

// newHTTPUsersClient builds an httpUsersClient. rc must already have its
// base URL and timeout configured (done in main.go via restyutil.New).
func newHTTPUsersClient(rc *resty.Client, token string) *httpUsersClient {
	return &httpUsersClient{rc: rc, token: token}
}

// SearchUsers calls the third-party HR endpoint and returns a slice of
// SearchUser. Any non-2xx status is treated as a backend error — 4xx
// responses from the third party are not forwarded to the NATS caller;
// they are logged by the handler and converted to ErrInternal.
func (c *httpUsersClient) SearchUsers(ctx context.Context, query string) ([]model.SearchUser, error) {
	// TODO(searchUsers-thirdparty): replace the request body struct and URL
	// path below with the real third-party contract. The response type
	// ([]model.SearchUser) is stable; adjust model.SearchUser's field list
	// in pkg/model/search.go to match the actual response shape.
	type requestBody struct {
		Query string `json:"query"`
	}

	req := c.rc.R().
		SetContext(ctx).
		SetBody(requestBody{Query: query}).
		SetResult(&[]model.SearchUser{})

	if c.token != "" {
		req = req.SetAuthToken(c.token)
	}

	// TODO(searchUsers-thirdparty): replace "/search" with the real path.
	resp, err := req.Post("/search")
	if err != nil {
		return nil, fmt.Errorf("users api request: %w", err)
	}
	if resp.StatusCode() < http.StatusOK || resp.StatusCode() >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("users api returned status %d", resp.StatusCode())
	}

	result, ok := resp.Result().(*[]model.SearchUser)
	if !ok || result == nil {
		return nil, fmt.Errorf("users api: unexpected response type")
	}
	return *result, nil
}
