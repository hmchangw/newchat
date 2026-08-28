package main

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

// httpWriteTimeout bounds a response write measured from the request, so in-handler
// waits count against it: RoomRPCTimeout and FanoutTimeout must stay under it so
// the handler always wins the race against net/http closing the connection.
const httpWriteTimeout = 40 * time.Second

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
	ReadPreference        string `env:"MONGO_READ_PREFERENCE" envDefault:"primaryPreferred"`
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
