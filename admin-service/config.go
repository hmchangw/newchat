package main

import (
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// httpWriteTimeout bounds a response write; RoomRPCTimeout must stay under it so
// the handler always wins the race against net/http closing the connection.
const httpWriteTimeout = 30 * time.Second

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
}

func loadConfig() (Config, error) {
	var c Config
	if err := env.Parse(&c); err != nil {
		return Config{}, err
	}
	if c.RoomRPCTimeout <= 0 {
		return Config{}, fmt.Errorf("invalid ROOM_RPC_TIMEOUT %s: must be > 0", c.RoomRPCTimeout)
	}
	if c.RoomRPCTimeout >= httpWriteTimeout {
		return Config{}, fmt.Errorf("invalid ROOM_RPC_TIMEOUT %s: must be below the %s HTTP write timeout", c.RoomRPCTimeout, httpWriteTimeout)
	}
	return c, nil
}
