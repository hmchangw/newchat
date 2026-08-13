// Package restyutil constructs a resty client with codebase defaults: OTel transport, slog request/response logging, 30s timeout.
package restyutil

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// 30s suits arbitrary downstream HTTP, distinct from the 5s probe in mongoutil/valkeyutil.
const defaultTimeout = 30 * time.Second

type Option func(*resty.Client)

func WithTimeout(d time.Duration) Option {
	return func(c *resty.Client) { c.SetTimeout(d) }
}

func WithHeader(key, value string) Option {
	return func(c *resty.Client) { c.SetHeader(key, value) }
}

func WithBearerToken(token string) Option {
	return func(c *resty.Client) { c.SetAuthToken(token) }
}

// WithTransport replaces the default transport; OTel is wrapped on for you by New.
func WithTransport(rt http.RoundTripper) Option {
	return func(c *resty.Client) { c.SetTransport(rt) }
}

// WithMaxIdleConns sizes the idle keep-alive pool for a single-host client: the
// stdlib keeps only 2, so a third concurrent request pays a fresh handshake. It
// tunes the transport installed when it runs, so place it after WithTransport.
func WithMaxIdleConns(n int) Option {
	return func(c *resty.Client) {
		// A RoundTripper that is not an *http.Transport has no pool to size.
		tr, ok := c.GetClient().Transport.(*http.Transport)
		if n <= 0 || !ok {
			return
		}
		tr.MaxIdleConnsPerHost = n
		// MaxIdleConns == 0 means unlimited, so only raise a finite value.
		if tr.MaxIdleConns > 0 && tr.MaxIdleConns < n {
			tr.MaxIdleConns = n
		}
	}
}

func New(baseURL string, opts ...Option) *resty.Client {
	// A mutable clone, so transport-tuning options can adjust it in place; the
	// OTel wrap happens after the options so it can never be clobbered by one.
	c := resty.New().
		SetBaseURL(baseURL).
		SetTimeout(defaultTimeout).
		SetTransport(http.DefaultTransport.(*http.Transport).Clone())

	c.OnAfterResponse(logResponse)
	c.OnError(logError)

	for _, opt := range opts {
		opt(c)
	}

	c.SetTransport(otelhttp.NewTransport(c.GetClient().Transport))
	return c
}

func logResponse(_ *resty.Client, resp *resty.Response) error {
	slog.Debug("http response", logFields(resp.Request, resp.StatusCode(), resp.Time(), nil)...)
	return nil
}

func logError(req *resty.Request, err error) {
	slog.Error("http error", logFields(req, 0, time.Since(req.Time), err)...)
}

func logFields(req *resty.Request, status int, dur time.Duration, err error) []any {
	// Strip RawQuery from URL — query strings can carry tokens (CLAUDE.md: never log tokens).
	host, path := "", req.URL
	if rr := req.RawRequest; rr != nil && rr.URL != nil {
		host = rr.URL.Host
		path = rr.URL.Path
	}
	fields := []any{
		"method", req.Method,
		"host", host,
		"path", path,
		"duration_ms", dur.Milliseconds(),
	}
	if rr := req.RawRequest; rr != nil {
		if id := natsutil.RequestIDFromContext(rr.Context()); id != "" {
			fields = append(fields, "request_id", id)
		}
	}
	if status > 0 {
		fields = append(fields, "status", status)
	}
	if err != nil {
		fields = append(fields, "error", err.Error())
	}
	return fields
}
