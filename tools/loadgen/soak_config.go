package main

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"
)

const maxBorrowedSoakUsers = 20000

// soakConfig is the Run A configuration contract. I8, I10, and I12 remain
// explicit inputs because their production interpretation is not yet confirmed.
type soakConfig struct {
	RunID                       string        `env:"RUN_ID"                          envDefault:""`
	RunDuration                 time.Duration `env:"RUN_DURATION"                    envDefault:"72h"`
	Warmup                      time.Duration `env:"WARMUP"                          envDefault:"30s"`
	SendRate                    float64       `env:"SEND_RATE"                       envDefault:"100"`
	ReadRate                    float64       `env:"READ_RATE"                       envDefault:"700"`
	ThreadShare                 float64       `env:"THREAD_SHARE"                    envDefault:"0.10"`
	MutationRate                float64       `env:"MUTATION_RATE"                   envDefault:"5"`
	SoftDeleteRatio             float64       `env:"SOFT_DELETE_RATIO"               envDefault:"0.001"`
	ReactionRate                float64       `env:"REACTION_RATE"                   envDefault:"100"`
	ReactionsPerHotMessage      int           `env:"REACTIONS_PER_HOT_MESSAGE"       envDefault:"30"`
	ReactionMessageScope        string        `env:"REACTION_MESSAGE_SCOPE"          envDefault:"hot_only"`
	ReactionRemoveShare         float64       `env:"REACTION_REMOVE_SHARE"           envDefault:"0.20"`
	PinnedListRate              float64       `env:"PINNED_LIST_RATE"                 envDefault:"1"`
	VerifyRate                  float64       `env:"VERIFY_RATE"                      envDefault:"1"`
	MaxUsers                    int           `env:"MAX_USERS"                        envDefault:"20000"`
	ActiveUsers                 int           `env:"ACTIVE_USERS"                     envDefault:"2000"`
	RoomCount                   int           `env:"ROOM_COUNT"                       envDefault:"10000"`
	ChannelRatio                float64       `env:"CHANNEL_RATIO"                    envDefault:"0.30"`
	ChannelMembers              int           `env:"CHANNEL_MEMBERS"                  envDefault:"100"`
	RateScope                   string        `env:"RATE_SCOPE"                       envDefault:"site"`
	MessagesPerActiveUserPerDay float64       `env:"MESSAGES_PER_ACTIVE_USER_PER_DAY" envDefault:"0"`
	PayloadMedianBytes          int           `env:"PAYLOAD_MEDIAN_BYTES"             envDefault:"1024"`
	PayloadP95Bytes             int           `env:"PAYLOAD_P95_BYTES"                envDefault:"2048"`
	PayloadMaxBytes             int           `env:"PAYLOAD_MAX_BYTES"                envDefault:"10240"`
	PersistGrace                time.Duration `env:"PERSIST_GRACE"                    envDefault:"10s"`
	MutationRetries             int           `env:"MUTATION_RETRIES"                 envDefault:"3"`
	RetryMinBackoff             time.Duration `env:"RETRY_MIN_BACKOFF"                envDefault:"100ms"`
	RetryMaxBackoff             time.Duration `env:"RETRY_MAX_BACKOFF"                envDefault:"5s"`
	RecentPerRoom               int           `env:"RECENT_PER_ROOM"                  envDefault:"128"`
	RecentTotal                 int           `env:"RECENT_TOTAL"                     envDefault:"200000"`
	CassandraCleanup            string        `env:"CASSANDRA_CLEANUP"                envDefault:"none"`
	ConfirmKeyspace             string        `env:"CONFIRM_KEYSPACE"                 envDefault:""`
}

func validateSoakConfig(cfg *soakConfig, cassandraKeyspace string) error {
	if strings.TrimSpace(cfg.RunID) == "" {
		return fmt.Errorf("SOAK_RUN_ID is required")
	}
	if cfg.RunDuration <= 0 {
		return fmt.Errorf("SOAK_RUN_DURATION must be greater than zero")
	}
	if cfg.Warmup < 0 || cfg.Warmup >= cfg.RunDuration {
		return fmt.Errorf("SOAK_WARMUP must be non-negative and less than SOAK_RUN_DURATION")
	}

	if err := validatePositiveRate("SOAK_SEND_RATE", cfg.SendRate); err != nil {
		return err
	}
	if err := validatePositiveRate("SOAK_READ_RATE", cfg.ReadRate); err != nil {
		return err
	}
	for name, value := range map[string]float64{
		"SOAK_MUTATION_RATE":    cfg.MutationRate,
		"SOAK_REACTION_RATE":    cfg.ReactionRate,
		"SOAK_PINNED_LIST_RATE": cfg.PinnedListRate,
		"SOAK_VERIFY_RATE":      cfg.VerifyRate,
	} {
		if err := validateNonNegativeRate(name, value); err != nil {
			return err
		}
	}
	for name, value := range map[string]float64{
		"SOAK_THREAD_SHARE":          cfg.ThreadShare,
		"SOAK_SOFT_DELETE_RATIO":     cfg.SoftDeleteRatio,
		"SOAK_REACTION_REMOVE_SHARE": cfg.ReactionRemoveShare,
		"SOAK_CHANNEL_RATIO":         cfg.ChannelRatio,
	} {
		if !isFinite(value) || value < 0 || value > 1 {
			return fmt.Errorf("%s must be between zero and one", name)
		}
	}

	if cfg.PersistGrace < 0 {
		return fmt.Errorf("SOAK_PERSIST_GRACE must be non-negative")
	}
	if cfg.MutationRetries < 0 {
		return fmt.Errorf("SOAK_MUTATION_RETRIES must be non-negative")
	}
	if cfg.RetryMinBackoff <= 0 {
		return fmt.Errorf("SOAK_RETRY_MIN_BACKOFF must be greater than zero")
	}
	if cfg.RetryMaxBackoff < cfg.RetryMinBackoff {
		return fmt.Errorf("SOAK_RETRY_MAX_BACKOFF must be at least SOAK_RETRY_MIN_BACKOFF")
	}
	if cfg.RecentPerRoom <= 0 {
		return fmt.Errorf("SOAK_RECENT_PER_ROOM must be greater than zero")
	}
	if cfg.RecentTotal < cfg.RecentPerRoom {
		return fmt.Errorf("SOAK_RECENT_TOTAL must be at least SOAK_RECENT_PER_ROOM")
	}

	if cfg.MaxUsers <= 0 || cfg.MaxUsers > maxBorrowedSoakUsers {
		return fmt.Errorf("SOAK_MAX_USERS must be between 1 and %d", maxBorrowedSoakUsers)
	}
	if cfg.ActiveUsers <= 0 || cfg.ActiveUsers > cfg.MaxUsers {
		return fmt.Errorf("SOAK_ACTIVE_USERS must be between 1 and SOAK_MAX_USERS")
	}
	if cfg.RoomCount <= 0 {
		return fmt.Errorf("SOAK_ROOM_COUNT must be greater than zero")
	}
	if cfg.ChannelMembers < 2 || cfg.ChannelMembers > cfg.MaxUsers {
		return fmt.Errorf("SOAK_CHANNEL_MEMBERS must be between 2 and SOAK_MAX_USERS")
	}
	if cfg.ReactionsPerHotMessage <= 0 || cfg.ReactionsPerHotMessage > cfg.ActiveUsers {
		return fmt.Errorf("SOAK_REACTIONS_PER_HOT_MESSAGE must be between 1 and SOAK_ACTIVE_USERS")
	}
	if !isFinite(cfg.MessagesPerActiveUserPerDay) || cfg.MessagesPerActiveUserPerDay < 0 {
		return fmt.Errorf("SOAK_MESSAGES_PER_ACTIVE_USER_PER_DAY must be non-negative")
	}

	if cfg.PayloadMedianBytes <= 0 {
		return fmt.Errorf("SOAK_PAYLOAD_MEDIAN_BYTES must be greater than zero")
	}
	if cfg.PayloadP95Bytes < cfg.PayloadMedianBytes {
		return fmt.Errorf("SOAK_PAYLOAD_P95_BYTES must be at least SOAK_PAYLOAD_MEDIAN_BYTES")
	}
	if cfg.PayloadMaxBytes < cfg.PayloadP95Bytes {
		return fmt.Errorf("SOAK_PAYLOAD_MAX_BYTES must be at least SOAK_PAYLOAD_P95_BYTES")
	}

	switch cfg.ReactionMessageScope {
	case "hot_only", "all_messages":
	default:
		return fmt.Errorf("SOAK_REACTION_MESSAGE_SCOPE must be hot_only or all_messages")
	}
	switch cfg.RateScope {
	case "site", "global":
	default:
		return fmt.Errorf("SOAK_RATE_SCOPE must be site or global")
	}
	switch cfg.CassandraCleanup {
	case "none":
	case "truncate":
		if cfg.ConfirmKeyspace == "" || cfg.ConfirmKeyspace != cassandraKeyspace {
			return fmt.Errorf("SOAK_CONFIRM_KEYSPACE must exactly match CASSANDRA_KEYSPACE before truncate")
		}
	default:
		return fmt.Errorf("SOAK_CASSANDRA_CLEANUP must be none or truncate")
	}

	return nil
}

func validatePositiveRate(name string, value float64) error {
	if !isFinite(value) || value <= 0 {
		return fmt.Errorf("%s must be greater than zero", name)
	}
	return nil
}

func validateNonNegativeRate(name string, value float64) error {
	if !isFinite(value) || value < 0 {
		return fmt.Errorf("%s must be non-negative", name)
	}
	return nil
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func logSoakAssumptions(cfg *soakConfig) {
	messagesPerActiveUserPerDay := cfg.MessagesPerActiveUserPerDay
	i12Derived := messagesPerActiveUserPerDay == 0
	if i12Derived && cfg.ActiveUsers > 0 {
		messagesPerActiveUserPerDay = cfg.SendRate * (24 * time.Hour).Seconds() / float64(cfg.ActiveUsers)
	}

	slog.Info("Cassandra Run A provisional forecast assumptions",
		"provisional", true,
		"i8ReactionMessageScope", cfg.ReactionMessageScope,
		"i8ReactionsPerHotMessage", cfg.ReactionsPerHotMessage,
		"i10RateScope", cfg.RateScope,
		"i12MessagesPerActiveUserPerDay", messagesPerActiveUserPerDay,
		"i12Derived", i12Derived,
	)
}
