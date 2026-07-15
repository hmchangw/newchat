package searchengine

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	o11yes "github.com/flywindy/o11y/elasticsearch"
)

// Config bundles every connection-time knob for the search backend.
type Config struct {
	Backend  string
	URL      string
	Username string
	Password string
	// TLSSkipVerify disables server cert verification — opt-in via env for
	// self-signed/internal clusters; MUST stay false in production.
	TLSSkipVerify bool
}

// New creates a SearchEngine for the configured backend
// ("elasticsearch" or "opensearch") and verifies connectivity via Ping
// before returning.
func New(ctx context.Context, cfg Config, opts ...Option) (SearchEngine, error) {
	cc := newConnectConfig(opts...)
	var transport Transporter
	switch cfg.Backend {
	case "elasticsearch":
		esCfg := elasticsearch.Config{
			Addresses: []string{cfg.URL},
			Username:  cfg.Username,
			Password:  cfg.Password,
		}
		if cfg.TLSSkipVerify {
			dt, ok := http.DefaultTransport.(*http.Transport)
			if !ok {
				return nil, fmt.Errorf("create elasticsearch client: http.DefaultTransport is not *http.Transport")
			}
			httpTransport := dt.Clone()
			httpTransport.TLSClientConfig = &tls.Config{
				// #nosec G402 -- InsecureSkipVerify is opt-in via TLSSkipVerify config for self-signed ES certs
				InsecureSkipVerify: true, //nolint:gosec
				MinVersion:         tls.VersionTLS12,
			}
			esCfg.Transport = httpTransport
		}
		// o11yes.NewClient wires the first-party OTel instrumentation into the
		// transport and returns the standard *elasticsearch.Client; the adapter
		// then drives that instrumentation around its raw requests. A plain
		// client (instrumentation absent) takes the adapter's no-op path.
		client, err := newESClient(&esCfg, cc.obs)
		if err != nil {
			return nil, fmt.Errorf("create elasticsearch client: %w", err)
		}
		transport = client
	default:
		return nil, fmt.Errorf("unsupported search backend: %s", cfg.Backend)
	}

	adapter := newAdapter(transport)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := adapter.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("search engine ping failed: %w", err)
	}

	return adapter, nil
}

// newESClient builds the Elasticsearch client, instrumented via
// o11y/elasticsearch when observability is configured, otherwise plain.
func newESClient(esCfg *elasticsearch.Config, obs Observability) (*elasticsearch.Client, error) {
	if obs != nil {
		return o11yes.NewClient(*esCfg, obs.TracerProvider())
	}
	return elasticsearch.NewClient(*esCfg)
}
