package main

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

// httpWriteTimeout bounds a response write measured from the request, so in-handler
// waits count against it: RoomRPCTimeout and FanoutTimeout must stay under it so
// the handler always wins the race against net/http closing the connection.
const httpWriteTimeout = 40 * time.Second

// maxMultipartMemory caps how much of an upload Gin keeps in memory before
// spilling the part to a temp file. Matches upload-service's cap for the same
// reason: the body is streamed onward, so it never needs to be resident.
const maxMultipartMemory = 1 << 20

// requestBudget is the absolute per-request deadline the permission handlers pin at
// entry (withRequestBudget): httpWriteTimeout minus a margin to write the response
// after the last in-handler wait, so syncFailures always reaches the caller.
const requestBudget = httpWriteTimeout - 2*time.Second

type Config struct {
	Port                  string `env:"PORT" envDefault:"8082"`
	SiteID                string `env:"SITE_ID,required"`
	MongoURI              string `env:"MONGO_URI,required"`
	MongoDB               string `env:"MONGO_DB" envDefault:"chat"`
	MongoUsername         string `env:"MONGO_USERNAME"`
	MongoPassword         string `env:"MONGO_PASSWORD"`
	BcryptCost            int    `env:"BCRYPT_COST" envDefault:"10"`
	SessionsMaxPerAccount int    `env:"SESSIONS_MAX_PER_ACCOUNT" envDefault:"100"`
	// NatsURL backs the room-service RPC behind the duty toggle. Required: the
	// toggle has no fallback transport, so a missing value must fail at startup.
	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE"`
	// RoomRPCTimeout must stay below the HTTP server's WriteTimeout, or net/http
	// closes the connection before the handler can answer.
	RoomRPCTimeout time.Duration `env:"ROOM_RPC_TIMEOUT" envDefault:"5s"`
	// FanoutTimeout is the server-side budget for ONE request's whole cross-site
	// permission fanout — every destination lane, every batch, every chunk. Sized for a
	// whole-site batch over cross-site gateways. Like RoomRPCTimeout it must stay below
	// the HTTP write timeout, or net/http drops the connection before the admin can read
	// syncFailures. The default exceeds main.go's 25s graceful-shutdown budget: a SIGTERM
	// mid-fanout can kill the request before the admin sees syncFailures. Accepted — the
	// ledger write already committed, and resync re-delivers anything cut short.
	FanoutTimeout time.Duration `env:"FANOUT_TIMEOUT" envDefault:"30s"`
	// AllSiteIDs lists every site in the federation (including this one); empty means
	// no cross-site fanout — correct for single-site dev.
	AllSiteIDs []string `env:"ALL_SITE_IDS" envSeparator:"," envDefault:""`

	// ClientUpdateURL is the base URL of the LOCAL site's client-update-service,
	// whose upload endpoint only this service's account may call.
	ClientUpdateURL string `env:"CLIENT_UPDATE_URL,required"`
	// ClientUpdateToken is admin-service's entry in that service's UPLOAD_TOKENS.
	// Never logged, never returned to a caller.
	ClientUpdateToken string `env:"CLIENT_UPDATE_TOKEN,required"`
	// ClientUpdateTimeout bounds one artifact upload end to end. It is
	// deliberately far ABOVE httpWriteTimeout: the upload handler extends its own
	// read/write deadlines (client_update.go) rather than raising the server's,
	// so this value must NOT be passed through checkHandlerTimeout.
	//
	// This bounds the WHOLE handler — the inbound read and the outbound call —
	// because uploadClientVersion pins it on the request context; the two phases
	// are sequential, not overlapping (see uploadResponseMargin for the whole
	// ordering). Deployment constraint: client-update-service's own
	// HTTP_WRITE_TIMEOUT should be at least this value, so a slow upstream is not
	// cut off mid-write on a request this service is still waiting on.
	ClientUpdateTimeout time.Duration `env:"CLIENT_UPDATE_UPLOAD_TIMEOUT" envDefault:"10m"`
	// ClientUpdateMaxUploadBytes caps one upload's request body. A guard rail,
	// not a policy: the default is far above any real artifact, but without it
	// c.MultipartForm spools an unbounded body to the pod's ephemeral storage
	// before maxUploadParts is ever consulted, so one caller could fill the disk.
	ClientUpdateMaxUploadBytes int64 `env:"CLIENT_UPDATE_MAX_UPLOAD_BYTES" envDefault:"2147483648"`

	// Pool caps the Mongo connection pool. NOTE: admin-service deliberately takes
	// NO shared HTTP request-timeout (ginutil.TimeoutConfig): its permission
	// handlers pin their own budget via withRequestBudget (requestBudget, just
	// under httpWriteTimeout) and the cross-site fanout self-limits to
	// min(FanoutTimeout, request deadline). A blanket router timeout shorter than
	// FanoutTimeout would silently shrink the fanout — see applyBaseMiddleware.
	Pool mongoutil.PoolConfig
}

func loadConfig() (Config, error) {
	var c Config
	if err := env.Parse(&c); err != nil {
		return Config{}, err
	}
	if err := checkHandlerTimeout("ROOM_RPC_TIMEOUT", c.RoomRPCTimeout); err != nil {
		return Config{}, err
	}
	if err := checkHandlerTimeout("FANOUT_TIMEOUT", c.FanoutTimeout); err != nil {
		return Config{}, err
	}
	if err := c.Pool.Validate(); err != nil {
		return Config{}, err
	}
	if err := validateClientUpdate(c.ClientUpdateURL, c.ClientUpdateToken, c.ClientUpdateTimeout, c.ClientUpdateMaxUploadBytes); err != nil {
		return Config{}, err
	}
	return c, nil
}

// checkHandlerTimeout rejects a per-request budget the handler cannot honour: a
// non-positive value yields an already-expired context, and one at or above
// httpWriteTimeout lets net/http close the connection before the handler answers.
func checkHandlerTimeout(name string, d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("invalid %s %s: must be > 0", name, d)
	}
	if d >= httpWriteTimeout {
		return fmt.Errorf("invalid %s %s: must be below the %s HTTP write timeout", name, d, httpWriteTimeout)
	}
	return nil
}

// clientUpdateSendsTokenInClear reports whether the upload credential will
// cross the network unencrypted. http:// stays legal — service-to-service
// traffic here is plaintext inside the cluster, and rejecting it would break
// both the local compose stack and production — so main.go warns instead, and
// the operator decides whether that link needs TLS or a mesh.
func clientUpdateSendsTokenInClear(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		// Already rejected by validateClientUpdate; nothing useful to warn about.
		return false
	}
	return strings.EqualFold(u.Scheme, "http")
}

// validateClientUpdate checks the relay's configuration at startup. Error text
// names the field only — never the token, which would reach the logs.
func validateClientUpdate(rawURL, token string, timeout time.Duration, maxBytes int64) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid CLIENT_UPDATE_URL: %w", err)
	}
	// Only http/https: the uploader resolves the version path against this base and
	// sends it through *http.Transport, which rejects any other scheme at request
	// time. Failing here turns a per-upload 503 into a startup error.
	// Hostname(), not Host: "http://:8080" has a non-empty Host (":8080") but no
	// host to dial, and would fail per-request rather than at startup.
	if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return fmt.Errorf("invalid CLIENT_UPDATE_URL %q: need an absolute http or https URL with a host", rawURL)
	}
	if token == "" {
		return fmt.Errorf("CLIENT_UPDATE_TOKEN must not be empty")
	}
	if timeout <= 0 {
		return fmt.Errorf("invalid CLIENT_UPDATE_UPLOAD_TIMEOUT %s: must be > 0", timeout)
	}
	if maxBytes <= 0 {
		return fmt.Errorf("invalid CLIENT_UPDATE_MAX_UPLOAD_BYTES %d: must be > 0", maxBytes)
	}
	return nil
}
