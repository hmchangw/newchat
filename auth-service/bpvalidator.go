package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-resty/resty/v2"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/principal"
)

// httpBotplatformValidator is the production BotplatformValidator: it POSTs
// the supplied authToken to {baseURL}/api/v1/auth/validate and unmarshals
// the {valid, principal} response. Resty handles timeout, TLS, and redirects;
// the Resty client is supplied externally (so main wires the shared
// restyutil instance).
type httpBotplatformValidator struct {
	client  *resty.Client
	baseURL string
}

// newHTTPBotplatformValidator returns a validator that talks to the
// botplatform-service at baseURL. baseURL should be the public botplatform
// URL of the LOCAL site (validation is local-DB only — cross-site routing is
// the gateway's job).
func newHTTPBotplatformValidator(client *resty.Client, baseURL string) *httpBotplatformValidator {
	return &httpBotplatformValidator{client: client, baseURL: baseURL}
}

// Validate POSTs to botplatform /api/v1/auth/validate. Returns:
//   - a typed errcode.Unauthenticated on a 401 from upstream (caller will
//     surface it to the client as 401 invalid_token).
//   - a raw wrapped error on network failures or non-2xx/401 statuses
//     (caller will surface it as 503 upstream_unavailable).
func (v *httpBotplatformValidator) Validate(ctx context.Context, authToken string) (principal.Principal, error) {
	var body struct {
		Valid     bool                `json:"valid"`
		Principal principal.Principal `json:"principal"`
	}
	req := v.client.R().
		SetContext(ctx).
		SetBody(map[string]string{"authToken": authToken}).
		SetResult(&body)
	if id := natsutil.RequestIDFromContext(ctx); id != "" {
		req = req.SetHeader(natsutil.RequestIDHeader, id)
	}
	resp, err := req.Post(v.baseURL + "/api/v1/auth/validate")
	if err != nil {
		return principal.Principal{}, fmt.Errorf("validate authToken: %w", err)
	}
	switch resp.StatusCode() {
	case http.StatusOK:
		if !body.Valid {
			return principal.Principal{}, errcode.Unauthenticated("session token invalid",
				errcode.WithReason(errcode.BotplatformInvalidToken))
		}
		return body.Principal, nil
	case http.StatusUnauthorized:
		return principal.Principal{}, errcode.Unauthenticated("session token invalid",
			errcode.WithReason(errcode.BotplatformInvalidToken))
	default:
		return principal.Principal{}, fmt.Errorf("botplatform validate: HTTP %d (body %d bytes)",
			resp.StatusCode(), len(resp.Body()))
	}
}
