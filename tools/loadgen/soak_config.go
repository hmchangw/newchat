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
	RunID                       string        `env:"RUN_ID"                          envDefault:""`
	Environment                 string        `env:"ENVIRONMENT"                     envDefault:"local"`
	RunMode                     string        `env:"RUN_MODE"                        envDefault:"duration"`
	RunDuration                 time.Duration `env:"RUN_DURATION"                    envDefault:"72h"`
	Warmup                      time.Duration `env:"WARMUP"                          envDefault:"30s"`
	HeartbeatInterval           time.Duration `env:"HEARTBEAT_INTERVAL"               envDefault:"30s"`
	HeartbeatStaleAfter         time.Duration `env:"HEARTBEAT_STALE_AFTER"            envDefault:"2m"`
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
	MemberMutationRate          float64       `env:"MEMBER_MUTATION_RATE"             envDefault:"2"`
	RoomMutationRate            float64       `env:"ROOM_MUTATION_RATE"               envDefault:"1"`
	RoomReadRate                float64       `env:"ROOM_READ_RATE"                   envDefault:"20"`
	RoomCreateRate              float64       `env:"ROOM_CREATE_RATE"                 envDefault:"0.05"`
	RoomCreateBudget            int           `env:"ROOM_CREATE_BUDGET"               envDefault:"2000"`
	RoomCreateSize              int           `env:"ROOM_CREATE_SIZE"                 envDefault:"5"`
	RoomReconcileReadShare      float64       `env:"ROOM_RECONCILE_READ_SHARE"        envDefault:"0.5"`
	MemberQuarantineMax         int           `env:"MEMBER_QUARANTINE_MAX"            envDefault:"10000"`
	ReadReceiptRate             float64       `env:"READ_RECEIPT_RATE"                envDefault:"5"`
	UserReadRate                float64       `env:"USER_READ_RATE"                   envDefault:"10"`
	SearchReadRate              float64       `env:"SEARCH_READ_RATE"                 envDefault:"5"`
	SearchObserverEnabled       bool          `env:"SEARCH_OBSERVER_ENABLED"          envDefault:"false"`
	SearchSettle                time.Duration `env:"SEARCH_SETTLE"                    envDefault:"30s"`
	PresenceRate                float64       `env:"PRESENCE_RATE"                    envDefault:"30"`
	PresenceConnections         int           `env:"PRESENCE_CONNECTIONS"             envDefault:"2000"`
	PresenceQueryShare          float64       `env:"PRESENCE_QUERY_SHARE"             envDefault:"0.1"`
	PresenceQueryBatch          int           `env:"PRESENCE_QUERY_BATCH"             envDefault:"50"`
	PresenceSettle              time.Duration `env:"PRESENCE_SETTLE"                  envDefault:"5s"`
	PresenceTTL                 time.Duration `env:"PRESENCE_TTL"                     envDefault:"5m"`
	MaxUsers                    int           `env:"MAX_USERS"                        envDefault:"20000"`
	ActiveUsers                 int           `env:"ACTIVE_USERS"                     envDefault:"2000"`
	RoomCount                   int           `env:"ROOM_COUNT"                       envDefault:"10000"`
	ChannelRatio                float64       `env:"CHANNEL_RATIO"                    envDefault:"0.30"`
	ChannelMembers              int           `env:"CHANNEL_MEMBERS"                  envDefault:"100"`
	LargeRoomThreshold          int           `env:"LARGE_ROOM_THRESHOLD"             envDefault:"500"`
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
	LedgerDir                   string        `env:"LEDGER_DIR"                       envDefault:""`
	LedgerEpoch                 string        `env:"LEDGER_EPOCH"                     envDefault:"v2"`
	LedgerCapacity              int           `env:"LEDGER_CAPACITY"                  envDefault:"200000"`
	// LedgerCompactEvery and LedgerMaxBytes both bound the journal. The finalize
	// count alone is not enough: a run whose operations all outlive their
	// deadline never reaches it, and a process that restarts first loses the
	// count entirely, so the file would only ever grow.
	LedgerCompactEvery int   `env:"LEDGER_COMPACT_EVERY"             envDefault:"10000"`
	LedgerMaxBytes     int64 `env:"LEDGER_MAX_BYTES"                 envDefault:"536870912"`
	// LedgerExpireBatch bounds one expiry sweep, which otherwise holds the
	// ledger lock while it journals every operation it retires. It defaults to
	// unbounded because a bound below the rate operations arrive at is worse
	// than the stall it prevents: the backlog then grows until Start hits
	// LedgerCapacity and invalidates the ledger during the incident it exists
	// to observe. Size it above (operations/sec x sweep interval), where the
	// sweep runs every min(30s, max(1s, RECONCILE_DEADLINE/10)).
	LedgerExpireBatch      int           `env:"LEDGER_EXPIRE_BATCH"              envDefault:"0"`
	ReconcileDeadline      time.Duration `env:"RECONCILE_DEADLINE"               envDefault:"10m"`
	ReconcileRetryInterval time.Duration `env:"RECONCILE_RETRY_INTERVAL"         envDefault:"1s"`
	ReconcileReadShare     float64       `env:"RECONCILE_READ_SHARE"             envDefault:"0.5"`
	// EncryptionPreflight gates the one-message check that the encrypted write
	// path reaches Cassandra and wraps the room DEK. It is on by default and
	// may only be turned off outside staging and production: a run that skipped
	// it proves nothing about at-rest encryption, so the choice has to be
	// explicit and is refused where the answer matters.
	EncryptionPreflight bool `env:"ENCRYPTION_PREFLIGHT" envDefault:"true"`
	// EncryptionPreflightTimeout budgets that check on its own, rather than
	// borrowing SOAK_PERSIST_GRACE, which also decides when every ledger
	// verification starts. A slow environment needs a longer preflight without
	// moving the evidence timing.
	EncryptionPreflightTimeout   time.Duration `env:"ENCRYPTION_PREFLIGHT_TIMEOUT"      envDefault:"30s"`
	RecipientObserverEnabled     bool          `env:"RECIPIENT_OBSERVER_ENABLED"        envDefault:"false"`
	RecipientObserverQueue       int           `env:"RECIPIENT_OBSERVER_QUEUE"          envDefault:"8192"`
	RecipientObserverConnections int           `env:"RECIPIENT_OBSERVER_CONNECTIONS"    envDefault:"32"`
	CassandraCleanup             string        `env:"CASSANDRA_CLEANUP"                envDefault:"none"`
	ConfirmKeyspace              string        `env:"CONFIRM_KEYSPACE"                 envDefault:""`
	TeardownBatchRooms           int           `env:"TEARDOWN_BATCH_ROOMS"              envDefault:"250"`
	TeardownBatchDelay           time.Duration `env:"TEARDOWN_BATCH_DELAY"              envDefault:"100ms"`
	TeardownBatchTimeout         time.Duration `env:"TEARDOWN_BATCH_TIMEOUT"            envDefault:"30s"`
	// HeapProfileDir turns on periodic heap profiling. A pod being OOM-killed
	// cannot be port-forwarded in time, so the profile has to already be on a
	// volume when the process dies; point this at the ledger mount to keep it.
	HeapProfileDir      string        `env:"HEAP_PROFILE_DIR"      envDefault:""`
	HeapProfileInterval time.Duration `env:"HEAP_PROFILE_INTERVAL" envDefault:"5m"`
	HeapProfileKeep     int           `env:"HEAP_PROFILE_KEEP"     envDefault:"3"`
	// SubscriptionListLimit and SubscriptionListIncludeLastMessage shape the
	// subscription-list request the room-read lane sends. Both are unset by
	// default so the lane keeps sending exactly what it sent before they
	// existed: user-service reads a missing limit as its own default and a
	// missing includeLastMessage as true, and a baseline recorded without them
	// has to stay comparable. Setting includeLastMessage to false drops the
	// per-room fan-out to history-service, which is what separates that cost
	// from the rest of the request without re-seeding anything.
	SubscriptionListLimit              int   `env:"SUBSCRIPTION_LIST_LIMIT"          envDefault:"0"`
	SubscriptionListIncludeLastMessage *bool `env:"SUBSCRIPTION_LIST_INCLUDE_LAST_MESSAGE"`

	// RoomZipfS and RoomZipfV shape which rooms the send lane picks. Defaults
	// reproduce the constants they replaced; raise V to model a site whose
	// busiest room is a few percent of traffic rather than a fifth of it.
	RoomZipfS float64 `env:"ROOM_ZIPF_S" envDefault:"1.2"`
	RoomZipfV float64 `env:"ROOM_ZIPF_V" envDefault:"1.0"`
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
	minimumHeartbeatStaleAfter, validLeaseDurations := minimumSoakHeartbeatStaleAfter(
		cfg.HeartbeatInterval,
		soakHeartbeatAttemptTimeout,
	)
	if !validLeaseDurations {
		return fmt.Errorf("SOAK_HEARTBEAT_INTERVAL is too large to calculate a safe heartbeat lease")
	}
	if cfg.HeartbeatStaleAfter < minimumHeartbeatStaleAfter {
		return fmt.Errorf(
			"SOAK_HEARTBEAT_STALE_AFTER must be at least %s for SOAK_HEARTBEAT_INTERVAL=%s "+
				"(two heartbeat intervals plus the %s attempt timeout)",
			minimumHeartbeatStaleAfter,
			cfg.HeartbeatInterval,
			soakHeartbeatAttemptTimeout,
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
		{"SOAK_READ_RECEIPT_RATE", cfg.ReadReceiptRate},
		{"SOAK_USER_READ_RATE", cfg.UserReadRate},
		{"SOAK_SEARCH_READ_RATE", cfg.SearchReadRate},
		{"SOAK_PRESENCE_RATE", cfg.PresenceRate},
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
	if cfg.LedgerCompactEvery <= 0 {
		return fmt.Errorf("SOAK_LEDGER_COMPACT_EVERY must be greater than zero")
	}
	if cfg.LedgerMaxBytes < 0 {
		return fmt.Errorf("SOAK_LEDGER_MAX_BYTES must not be negative")
	}
	if cfg.LedgerExpireBatch < 0 {
		return fmt.Errorf("SOAK_LEDGER_EXPIRE_BATCH must not be negative")
	}
	// A negative limit reaches user-service's normalizePage, which swaps it for
	// the default — the run would then report on a page size nobody chose. Only
	// the lower bound is checked: normalizePage also caps at the service's
	// MAX_SUBSCRIPTION_LIMIT, but that is the service's own configuration and
	// loadgen cannot read it, whereas a negative page size is wrong under every
	// configuration.
	// NaN and +Inf compare false against every bound, so they reach the room
	// picker, whose generator then never terminates. See newSoakRoomPicker.
	if math.IsNaN(cfg.RoomZipfS) || math.IsInf(cfg.RoomZipfS, 0) || cfg.RoomZipfS <= 1 {
		return fmt.Errorf("SOAK_ROOM_ZIPF_S must be greater than 1, got %v", cfg.RoomZipfS)
	}
	if math.IsNaN(cfg.RoomZipfV) || math.IsInf(cfg.RoomZipfV, 0) || cfg.RoomZipfV < 1 {
		return fmt.Errorf("SOAK_ROOM_ZIPF_V must be at least 1, got %v", cfg.RoomZipfV)
	}
	if cfg.SubscriptionListLimit < 0 {
		return fmt.Errorf("SOAK_SUBSCRIPTION_LIST_LIMIT must not be negative")
	}
	if cfg.HeapProfileDir != "" {
		if cfg.HeapProfileInterval <= 0 {
			return fmt.Errorf("SOAK_HEAP_PROFILE_INTERVAL must be greater than zero")
		}
		if cfg.HeapProfileKeep <= 0 {
			return fmt.Errorf("SOAK_HEAP_PROFILE_KEEP must be greater than zero")
		}
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
	if err := validateSoakEncryptionPreflight(cfg); err != nil {
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

// soakRoomCreateBudgetMax bounds the rooms one run may create, and with them
// the read-target set the create lane grows in memory.
const soakRoomCreateBudgetMax = 100000

// soakPreflightRequiredEnvironments are the environments whose results are
// reported onwards, so a run there must have proven the encrypted write path
// rather than assumed it.
var soakPreflightRequiredEnvironments = map[string]struct{}{
	"staging": {}, "production": {},
}

func validateSoakEncryptionPreflight(cfg *soakConfig) error {
	if cfg.EncryptionPreflightTimeout <= 0 {
		return fmt.Errorf("SOAK_ENCRYPTION_PREFLIGHT_TIMEOUT must be greater than zero")
	}
	if cfg.EncryptionPreflight {
		return nil
	}
	if _, required := soakPreflightRequiredEnvironments[cfg.Environment]; required {
		return fmt.Errorf(
			"SOAK_ENCRYPTION_PREFLIGHT cannot be disabled when SOAK_ENVIRONMENT is %q",
			cfg.Environment,
		)
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
	// Every created room is retained for the lifetime of the run: the read lane
	// keeps its ID and owning account so the room can receive traffic, and
	// nothing evicts them. The cap keeps that growth bounded by configuration
	// rather than by how long the run happens to last.
	if cfg.RoomCreateBudget < 0 || cfg.RoomCreateBudget > soakRoomCreateBudgetMax {
		return fmt.Errorf(
			"SOAK_ROOM_CREATE_BUDGET must be between 0 and %d", soakRoomCreateBudgetMax,
		)
	}
	if cfg.RoomCreateSize < 2 || cfg.RoomCreateSize > 50 {
		return fmt.Errorf("SOAK_ROOM_CREATE_SIZE must be between 2 and 50")
	}
	if cfg.MemberQuarantineMax <= 0 || cfg.MemberQuarantineMax > 1000000 {
		return fmt.Errorf("SOAK_MEMBER_QUARANTINE_MAX must be between 1 and 1000000")
	}
	if err := validateSoakPresenceConfig(cfg); err != nil {
		return err
	}
	if err := validateSoakSearchConfig(cfg); err != nil {
		return err
	}

	// Room and member reconciliation borrows room-read slots, so the read lane
	// must retire mutations at least as fast as they are produced. Below that
	// the unresolved backlog grows without bound and every mutation eventually
	// expires unverified — a run that can conclude nothing.
	mutationRate := cfg.MemberMutationRate + cfg.RoomMutationRate +
		cfg.RoomCreateRate + cfg.ReadReceiptRate
	if mutationRate <= 0 {
		return nil
	}
	reconcileCapacity := cfg.RoomReadRate * cfg.RoomReconcileReadShare
	if reconcileCapacity < mutationRate {
		return fmt.Errorf(
			"SOAK_ROOM_READ_RATE %.3f at SOAK_ROOM_RECONCILE_READ_SHARE %.3f reconciles %.3f "+
				"operations/s, below the %.3f operations/s the room, member and read-receipt "+
				"lanes produce; raise SOAK_ROOM_READ_RATE or lower the mutation rates",
			cfg.RoomReadRate, cfg.RoomReconcileReadShare, reconcileCapacity, mutationRate,
		)
	}
	return nil
}

// validateSoakPresenceConfig guards the two values that decide whether a
// presence comparison is meaningful. The TTL must match the presence service's
// CONNS_TTL: set it too high and an expectation the server was entitled to drop
// gets reported as a mismatch.
func validateSoakPresenceConfig(cfg *soakConfig) error {
	if cfg.PresenceRate <= 0 {
		return nil
	}
	if cfg.PresenceConnections <= 0 || cfg.PresenceConnections > maxBorrowedSoakUsers {
		return fmt.Errorf(
			"SOAK_PRESENCE_CONNECTIONS must be between 1 and %d", maxBorrowedSoakUsers,
		)
	}
	if !isFinite(cfg.PresenceQueryShare) ||
		cfg.PresenceQueryShare < 0 || cfg.PresenceQueryShare > 1 {
		return fmt.Errorf("SOAK_PRESENCE_QUERY_SHARE must be between zero and one")
	}
	if cfg.PresenceQueryBatch <= 0 || cfg.PresenceQueryBatch > 500 {
		return fmt.Errorf("SOAK_PRESENCE_QUERY_BATCH must be between 1 and 500")
	}
	if cfg.PresenceSettle <= 0 {
		return fmt.Errorf("SOAK_PRESENCE_SETTLE must be greater than zero")
	}
	if cfg.PresenceTTL <= cfg.PresenceSettle {
		return fmt.Errorf("SOAK_PRESENCE_TTL must be greater than SOAK_PRESENCE_SETTLE")
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

// validateSoakSearchConfig guards the two ways search observation can be
// configured into uselessness.
//
// The observer adds a second reconcile step to every admitted message, drawn
// from the same read-lane share the history step already uses. Below capacity
// the unresolved backlog grows without bound and every message eventually
// expires unverified — the run would report a search problem it created itself.
func validateSoakSearchConfig(cfg *soakConfig) error {
	if !cfg.SearchObserverEnabled {
		return nil
	}
	if err := validateSoakSearchSettle(cfg); err != nil {
		return err
	}
	// The probe locates a message by full-text search, so the payload has to
	// contain something that identifies one message. Today every soak body is a
	// run of the same character differing only in length, which analyzes to a
	// single huge token: the query matches nothing useful and every message
	// would be reported missing. Refuse rather than let the run manufacture a
	// data-loss report.
	return fmt.Errorf(
		"SOAK_SEARCH_OBSERVER_ENABLED is not usable yet: soak message bodies carry no " +
			"per-message searchable marker, so the index probe cannot distinguish one " +
			"message from another and would report every message as lost",
	)
}

// validateSoakSearchSettle keeps the window inside the reconcile deadline, or
// the probe never gets an answer before the operation expires.
func validateSoakSearchSettle(cfg *soakConfig) error {
	if cfg.SearchSettle <= 0 {
		return fmt.Errorf("SOAK_SEARCH_SETTLE must be greater than zero")
	}
	if cfg.SearchSettle >= cfg.ReconcileDeadline {
		return fmt.Errorf(
			"SOAK_SEARCH_SETTLE %s must be shorter than SOAK_RECONCILE_DEADLINE %s, "+
				"or the index probe never gets an answer before the deadline",
			cfg.SearchSettle, cfg.ReconcileDeadline,
		)
	}
	return nil
}

// soakReconcileStepsPerMessage counts the claims a message costs the read lane.
//
// soakFailureReconciler.Try advances one observer per claim, but only a
// query-mode observer reaches a service: the recipient observer is event mode,
// so finalizing it reads evidence already in memory and its allowance is
// refunded rather than spent. Counting it here would demand read capacity for
// work that never leaves the process.
//
// This holds only because reconcileReadAction drains the free claims inside one
// admission. A read action is the only way to take a claim, so without that
// drain an event-mode claim would still cost a callback and the real demand
// would be every observer, not every querying observer.
func soakReconcileStepsPerMessage(cfg *soakConfig) int {
	steps := 1
	if cfg.SearchObserverEnabled {
		steps++
	}
	return steps
}

// soakReconcileCapacity is the read-lane arithmetic behind the reconciliation
// floor: what the observers in play demand against what the share supplies.
//
// It is a floor, not a budget. It covers one claim per observer and nothing for
// the history poll's retries, which depend on how many messages are slow to
// persist and cannot be derived from configuration.
type soakReconcileCapacity struct {
	Steps    int
	Required float64
	Supplied float64
}

// Sufficient reports whether the lane can serve one claim per observer for
// every message sent.
func (c soakReconcileCapacity) Sufficient() bool { return c.Supplied >= c.Required }

func soakReconcileCapacityFor(cfg *soakConfig) soakReconcileCapacity {
	steps := soakReconcileStepsPerMessage(cfg)
	return soakReconcileCapacity{
		Steps:    steps,
		Required: cfg.SendRate * float64(steps),
		Supplied: cfg.ReadRate * cfg.ReconcileReadShare,
	}
}

// warnSoakReconcileConfig reports every reconcile-configuration problem the
// startup path can see, and returns whether the run began below the
// reconciliation floor. It exists so a new check has one place to be reached
// from: both checks warn rather than refuse, so an unreached one is silent
// instead of loud, and nothing else would catch it.
//
// The breaches are returned rather than recorded because they are known before
// the ledger that must carry them is open. The caller invalidates the ledger
// once it has one, in the order given: the capacity floor comes first because
// it is the breach that makes every verdict unverified, where the lag range
// only makes the window unreadable.
func warnSoakReconcileConfig(cfg *soakConfig) []string {
	var reasons []string
	if warnSoakReconcileCapacity(cfg) {
		reasons = append(reasons, invalidReasonReconcileCapacity)
	}
	if warnSoakReconcileLagRange(cfg) {
		reasons = append(reasons, invalidReasonReconcileLagRange)
	}
	return reasons
}

// warnSoakReconcileCapacity reports an under-provisioned reconcile lane without
// refusing the run. Below the floor the unresolved backlog grows and every
// message eventually expires unverified — a run that reports a storage problem
// it created itself, and reports it identically to a real one.
//
// It warns rather than exits because refusing at startup spends a whole deploy
// on a configuration mistake the message already states, and only the operator
// knows whether a degraded lane is worth the window.
// loadgen_failure_reconcile_claims_total shows at runtime whether it bit, and
// the returned breach marks the run itself as one whose evidence was already in
// question when it started.
//
// It reports the breach rather than recording it: a log line is not
// machine-readable, but the record belongs in the ledger, whose invalidation
// both sets Snapshot().InvalidReason and raises
// loadgen_failure_invalidations_total. Raising the counter here instead would
// leave the run's own record claiming evidence it cannot stand behind.
func warnSoakReconcileCapacity(cfg *soakConfig) bool {
	capacity := soakReconcileCapacityFor(cfg)
	if cfg.SendRate <= 0 || capacity.Sufficient() {
		return false
	}
	slog.Warn("soak reconcile lane is below the capacity its observers demand",
		"observerSteps", capacity.Steps,
		"requiredOperationsPerSecond", capacity.Required,
		"suppliedOperationsPerSecond", capacity.Supplied,
		"soakSendRate", cfg.SendRate,
		"soakReadRate", cfg.ReadRate,
		"soakReconcileReadShare", cfg.ReconcileReadShare,
		"remedy", "raise SOAK_READ_RATE or lower SOAK_SEND_RATE",
	)
	return true
}

// recordSoakStartupInvalidations puts a configuration breach into the run's own
// evidence rather than only into a log line: the ledger journals it, replay
// restores it, and Snapshot reports it, so the run cannot later be read as
// though it had never been disowned.
//
// A breach only warns and the run proceeds — but that bargain depends on the
// verdict outliving the process. If the journal refuses the record, a crash
// before Close would replay this run without its disqualifier and present the
// evidence as trustworthy, so the caller is told to stop instead.
func recordSoakStartupInvalidations(ledger *failureLedger, reasons []string) error {
	for _, reason := range reasons {
		ledger.Invalidate(reason)
	}
	unpersisted := ledger.UnpersistedInvalidations()
	if len(unpersisted) == 0 {
		return nil
	}
	return fmt.Errorf(
		"failure ledger could not persist startup invalidation %s",
		strings.Join(unpersisted, ", "),
	)
}
