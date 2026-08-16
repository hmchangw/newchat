package main

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"
)

const (
	maxBorrowedSoakUsers             = 20000
	maxFailureRecipientObserverQueue = 65536
)

const (
	soakRunModeDuration   = "duration"
	soakRunModeContinuous = "continuous"
)

var soakEnvironmentRegistry = map[string]struct{}{
	"local": {}, "test": {}, "staging": {}, "production": {},
}

// soakConfig is the Run A configuration contract. I8, I10, and I12 remain
// explicit inputs because their production interpretation is not yet confirmed.
type soakConfig struct {
	RunID                        string        `env:"RUN_ID"                          envDefault:""`
	Environment                  string        `env:"ENVIRONMENT"                     envDefault:"local"`
	RunMode                      string        `env:"RUN_MODE"                        envDefault:"duration"`
	RunDuration                  time.Duration `env:"RUN_DURATION"                    envDefault:"72h"`
	Warmup                       time.Duration `env:"WARMUP"                          envDefault:"30s"`
	HeartbeatInterval            time.Duration `env:"HEARTBEAT_INTERVAL"               envDefault:"30s"`
	HeartbeatStaleAfter          time.Duration `env:"HEARTBEAT_STALE_AFTER"            envDefault:"2m"`
	SendRate                     float64       `env:"SEND_RATE"                       envDefault:"100"`
	ReadRate                     float64       `env:"READ_RATE"                       envDefault:"700"`
	ThreadShare                  float64       `env:"THREAD_SHARE"                    envDefault:"0.10"`
	MutationRate                 float64       `env:"MUTATION_RATE"                   envDefault:"5"`
	SoftDeleteRatio              float64       `env:"SOFT_DELETE_RATIO"               envDefault:"0.001"`
	ReactionRate                 float64       `env:"REACTION_RATE"                   envDefault:"100"`
	ReactionsPerHotMessage       int           `env:"REACTIONS_PER_HOT_MESSAGE"       envDefault:"30"`
	ReactionMessageScope         string        `env:"REACTION_MESSAGE_SCOPE"          envDefault:"hot_only"`
	ReactionRemoveShare          float64       `env:"REACTION_REMOVE_SHARE"           envDefault:"0.20"`
	PinnedListRate               float64       `env:"PINNED_LIST_RATE"                 envDefault:"1"`
	VerifyRate                   float64       `env:"VERIFY_RATE"                      envDefault:"1"`
	MemberMutationRate           float64       `env:"MEMBER_MUTATION_RATE"             envDefault:"2"`
	RoomMutationRate             float64       `env:"ROOM_MUTATION_RATE"               envDefault:"1"`
	RoomReadRate                 float64       `env:"ROOM_READ_RATE"                   envDefault:"20"`
	RoomCreateRate               float64       `env:"ROOM_CREATE_RATE"                 envDefault:"0.05"`
	RoomCreateBudget             int           `env:"ROOM_CREATE_BUDGET"               envDefault:"2000"`
	RoomCreateSize               int           `env:"ROOM_CREATE_SIZE"                 envDefault:"5"`
	RoomReconcileReadShare       float64       `env:"ROOM_RECONCILE_READ_SHARE"        envDefault:"0.5"`
	MemberQuarantineMax          int           `env:"MEMBER_QUARANTINE_MAX"            envDefault:"10000"`
	MaxUsers                     int           `env:"MAX_USERS"                        envDefault:"20000"`
	ActiveUsers                  int           `env:"ACTIVE_USERS"                     envDefault:"2000"`
	RoomCount                    int           `env:"ROOM_COUNT"                       envDefault:"10000"`
	ChannelRatio                 float64       `env:"CHANNEL_RATIO"                    envDefault:"0.30"`
	ChannelMembers               int           `env:"CHANNEL_MEMBERS"                  envDefault:"100"`
	LargeRoomThreshold           int           `env:"LARGE_ROOM_THRESHOLD"             envDefault:"500"`
	RateScope                    string        `env:"RATE_SCOPE"                       envDefault:"site"`
	MessagesPerActiveUserPerDay  float64       `env:"MESSAGES_PER_ACTIVE_USER_PER_DAY" envDefault:"0"`
	PayloadMedianBytes           int           `env:"PAYLOAD_MEDIAN_BYTES"             envDefault:"1024"`
	PayloadP95Bytes              int           `env:"PAYLOAD_P95_BYTES"                envDefault:"2048"`
	PayloadMaxBytes              int           `env:"PAYLOAD_MAX_BYTES"                envDefault:"10240"`
	PersistGrace                 time.Duration `env:"PERSIST_GRACE"                    envDefault:"10s"`
	MutationRetries              int           `env:"MUTATION_RETRIES"                 envDefault:"3"`
	RetryMinBackoff              time.Duration `env:"RETRY_MIN_BACKOFF"                envDefault:"100ms"`
	RetryMaxBackoff              time.Duration `env:"RETRY_MAX_BACKOFF"                envDefault:"5s"`
	RecentPerRoom                int           `env:"RECENT_PER_ROOM"                  envDefault:"128"`
	RecentTotal                  int           `env:"RECENT_TOTAL"                     envDefault:"200000"`
	LedgerDir                    string        `env:"LEDGER_DIR"                       envDefault:""`
	LedgerEpoch                  string        `env:"LEDGER_EPOCH"                     envDefault:"v1"`
	LedgerCapacity               int           `env:"LEDGER_CAPACITY"                  envDefault:"200000"`
	ReconcileDeadline            time.Duration `env:"RECONCILE_DEADLINE"               envDefault:"10m"`
	ReconcileRetryInterval       time.Duration `env:"RECONCILE_RETRY_INTERVAL"         envDefault:"1s"`
	ReconcileReadShare           float64       `env:"RECONCILE_READ_SHARE"             envDefault:"0.5"`
	RecipientObserverEnabled     bool          `env:"RECIPIENT_OBSERVER_ENABLED"        envDefault:"false"`
	RecipientObserverQueue       int           `env:"RECIPIENT_OBSERVER_QUEUE"          envDefault:"8192"`
	RecipientObserverConnections int           `env:"RECIPIENT_OBSERVER_CONNECTIONS"    envDefault:"32"`
	CassandraCleanup             string        `env:"CASSANDRA_CLEANUP"                envDefault:"none"`
	ConfirmKeyspace              string        `env:"CONFIRM_KEYSPACE"                 envDefault:""`
	TeardownBatchRooms           int           `env:"TEARDOWN_BATCH_ROOMS"              envDefault:"250"`
	TeardownBatchDelay           time.Duration `env:"TEARDOWN_BATCH_DELAY"              envDefault:"100ms"`
	TeardownBatchTimeout         time.Duration `env:"TEARDOWN_BATCH_TIMEOUT"            envDefault:"30s"`
}

// soakPayloadBudgetRatio is the share of max_payload a page of message bodies
// may occupy. The rest is headroom for JSON structure, per-message metadata and
// the response envelope, none of which is counted by the message-body estimate.
const soakPayloadBudgetRatio = 0.75

// validateSoakPageBudget rejects a page size that cannot fit in the broker's
// max_payload at the configured message size.
//
// Neither value is safe alone: --page-limit and SOAK_PAYLOAD_MAX_BYTES multiply,
// so raising the payload size silently reintroduces oversize replies that the
// run would score as read failures rather than as a misconfiguration.
func validateSoakPageBudget(pageLimit, payloadMaxBytes int, brokerMaxPayload int64) error {
	if pageLimit <= 0 || payloadMaxBytes <= 0 || brokerMaxPayload <= 0 {
		return nil // caller validates these separately
	}
	budget := int64(float64(brokerMaxPayload) * soakPayloadBudgetRatio)
	// Divide rather than multiply: pageLimit*payloadMaxBytes can overflow for
	// large inputs and read as comfortably under budget. The error path also
	// avoids recomputing that unsafe product.
	if int64(pageLimit) > budget/int64(payloadMaxBytes) {
		return fmt.Errorf(
			"page-limit %d with SOAK_PAYLOAD_MAX_BYTES %d exceeds the %d-byte page budget "+
				"(%d-byte broker max_payload); lower --page-limit or SOAK_PAYLOAD_MAX_BYTES",
			pageLimit, payloadMaxBytes, budget, brokerMaxPayload)
	}
	return nil
}

func validateSoakConfig(cfg *soakConfig, cassandraKeyspace string) error {
	if strings.TrimSpace(cfg.RunID) == "" {
		return fmt.Errorf("SOAK_RUN_ID is required")
	}
	if !failureRunIDPattern.MatchString(cfg.RunID) || cfg.RunID == "." || cfg.RunID == ".." {
		return fmt.Errorf("SOAK_RUN_ID must be a filename-safe run identifier")
	}
	if _, known := soakEnvironmentRegistry[cfg.Environment]; !known {
		return fmt.Errorf("SOAK_ENVIRONMENT must be local, test, staging, or production")
	}
	switch cfg.RunMode {
	case soakRunModeDuration:
		if cfg.RunDuration <= 0 {
			return fmt.Errorf("SOAK_RUN_DURATION must be greater than zero")
		}
		if cfg.Warmup < 0 || cfg.Warmup >= cfg.RunDuration {
			return fmt.Errorf("SOAK_WARMUP must be non-negative and less than SOAK_RUN_DURATION")
		}
	case soakRunModeContinuous:
		if cfg.Warmup < 0 {
			return fmt.Errorf("SOAK_WARMUP must be non-negative")
		}
	default:
		return fmt.Errorf("SOAK_RUN_MODE must be duration or continuous")
	}
	if cfg.HeartbeatInterval <= 0 {
		return fmt.Errorf("SOAK_HEARTBEAT_INTERVAL must be greater than zero")
	}
	if cfg.HeartbeatStaleAfter <= cfg.HeartbeatInterval {
		return fmt.Errorf(
			"SOAK_HEARTBEAT_STALE_AFTER must be greater than SOAK_HEARTBEAT_INTERVAL",
		)
	}

	if err := validatePositiveRate("SOAK_SEND_RATE", cfg.SendRate); err != nil {
		return err
	}
	if err := validatePositiveRate("SOAK_READ_RATE", cfg.ReadRate); err != nil {
		return err
	}
	for _, rate := range []struct {
		name  string
		value float64
	}{
		{"SOAK_MUTATION_RATE", cfg.MutationRate},
		{"SOAK_REACTION_RATE", cfg.ReactionRate},
		{"SOAK_PINNED_LIST_RATE", cfg.PinnedListRate},
		{"SOAK_VERIFY_RATE", cfg.VerifyRate},
		{"SOAK_MEMBER_MUTATION_RATE", cfg.MemberMutationRate},
		{"SOAK_ROOM_MUTATION_RATE", cfg.RoomMutationRate},
		{"SOAK_ROOM_READ_RATE", cfg.RoomReadRate},
		{"SOAK_ROOM_CREATE_RATE", cfg.RoomCreateRate},
	} {
		if err := validateNonNegativeRate(rate.name, rate.value); err != nil {
			return err
		}
	}
	for _, ratio := range []struct {
		name  string
		value float64
	}{
		{"SOAK_THREAD_SHARE", cfg.ThreadShare},
		{"SOAK_SOFT_DELETE_RATIO", cfg.SoftDeleteRatio},
		{"SOAK_REACTION_REMOVE_SHARE", cfg.ReactionRemoveShare},
		{"SOAK_CHANNEL_RATIO", cfg.ChannelRatio},
	} {
		if !isFinite(ratio.value) || ratio.value < 0 || ratio.value > 1 {
			return fmt.Errorf("%s must be between zero and one", ratio.name)
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
	if cfg.LedgerCapacity <= 0 {
		return fmt.Errorf("SOAK_LEDGER_CAPACITY must be greater than zero")
	}
	if !failureRunIDPattern.MatchString(cfg.LedgerEpoch) ||
		cfg.LedgerEpoch == "." || cfg.LedgerEpoch == ".." {
		return fmt.Errorf("SOAK_LEDGER_EPOCH must be a filename-safe identifier")
	}
	if cfg.ReconcileDeadline <= cfg.PersistGrace {
		return fmt.Errorf("SOAK_RECONCILE_DEADLINE must be greater than SOAK_PERSIST_GRACE")
	}
	if cfg.ReconcileRetryInterval <= 0 {
		return fmt.Errorf("SOAK_RECONCILE_RETRY_INTERVAL must be greater than zero")
	}
	if !isFinite(cfg.ReconcileReadShare) ||
		cfg.ReconcileReadShare <= 0 || cfg.ReconcileReadShare > 1 {
		return fmt.Errorf("SOAK_RECONCILE_READ_SHARE must be greater than zero and at most 1")
	}
	if err := validateSoakRoomLaneConfig(cfg); err != nil {
		return err
	}
	if cfg.RecipientObserverEnabled && strings.TrimSpace(cfg.LedgerDir) == "" {
		return fmt.Errorf("SOAK_LEDGER_DIR is required when SOAK_RECIPIENT_OBSERVER_ENABLED=true")
	}
	if cfg.RecipientObserverQueue <= 0 || cfg.RecipientObserverQueue > maxFailureRecipientObserverQueue {
		return fmt.Errorf("SOAK_RECIPIENT_OBSERVER_QUEUE must be between 1 and %d", maxFailureRecipientObserverQueue)
	}
	if cfg.RecipientObserverConnections <= 0 || cfg.RecipientObserverConnections > 256 {
		return fmt.Errorf("SOAK_RECIPIENT_OBSERVER_CONNECTIONS must be between 1 and 256")
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
	if cfg.LargeRoomThreshold <= 0 {
		return fmt.Errorf("SOAK_LARGE_ROOM_THRESHOLD must be greater than zero")
	}
	if cfg.ChannelMembers > cfg.LargeRoomThreshold {
		return fmt.Errorf(
			"SOAK_CHANNEL_MEMBERS must not exceed SOAK_LARGE_ROOM_THRESHOLD",
		)
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
	if err := validateSoakTeardownConfig(cfg, cassandraKeyspace); err != nil {
		return err
	}

	return nil
}

func validateSoakRoomLaneConfig(cfg *soakConfig) error {
	if !isFinite(cfg.RoomReconcileReadShare) ||
		cfg.RoomReconcileReadShare <= 0 || cfg.RoomReconcileReadShare > 1 {
		return fmt.Errorf(
			"SOAK_ROOM_RECONCILE_READ_SHARE must be greater than zero and at most 1",
		)
	}
	if cfg.RoomCreateBudget < 0 {
		return fmt.Errorf("SOAK_ROOM_CREATE_BUDGET must be non-negative")
	}
	if cfg.RoomCreateSize < 2 || cfg.RoomCreateSize > 50 {
		return fmt.Errorf("SOAK_ROOM_CREATE_SIZE must be between 2 and 50")
	}
	if cfg.MemberQuarantineMax <= 0 || cfg.MemberQuarantineMax > 1000000 {
		return fmt.Errorf("SOAK_MEMBER_QUARANTINE_MAX must be between 1 and 1000000")
	}

	// Room and member reconciliation borrows room-read slots, so the read lane
	// must retire mutations at least as fast as they are produced. Below that
	// the unresolved backlog grows without bound and every mutation eventually
	// expires unverified — a run that can conclude nothing.
	mutationRate := cfg.MemberMutationRate + cfg.RoomMutationRate + cfg.RoomCreateRate
	if mutationRate <= 0 {
		return nil
	}
	reconcileCapacity := cfg.RoomReadRate * cfg.RoomReconcileReadShare
	if reconcileCapacity < mutationRate {
		return fmt.Errorf(
			"SOAK_ROOM_READ_RATE %.3f at SOAK_ROOM_RECONCILE_READ_SHARE %.3f reconciles %.3f "+
				"operations/s, below the %.3f operations/s the room and member mutation lanes "+
				"produce; raise SOAK_ROOM_READ_RATE or lower the mutation rates",
			cfg.RoomReadRate, cfg.RoomReconcileReadShare, reconcileCapacity, mutationRate,
		)
	}
	return nil
}

func validateSoakTeardownConfig(
	cfg *soakConfig,
	cassandraKeyspace string,
) error {
	if cfg == nil {
		return fmt.Errorf("soak configuration is required")
	}
	if strings.TrimSpace(cfg.RunID) == "" {
		return fmt.Errorf("SOAK_RUN_ID is required")
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
	if cfg.TeardownBatchRooms <= 0 ||
		cfg.TeardownBatchRooms > soakOwnershipChunkSize {
		return fmt.Errorf(
			"SOAK_TEARDOWN_BATCH_ROOMS must be between 1 and %d",
			soakOwnershipChunkSize,
		)
	}
	if cfg.TeardownBatchDelay < 0 {
		return fmt.Errorf("SOAK_TEARDOWN_BATCH_DELAY must be non-negative")
	}
	if cfg.TeardownBatchTimeout <= 0 {
		return fmt.Errorf("SOAK_TEARDOWN_BATCH_TIMEOUT must be greater than zero")
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
