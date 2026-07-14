package model

// PresenceSnapshotRequest is the bulk presence RPC request payload.
type PresenceSnapshotRequest struct {
	Accounts []string `json:"accounts" bson:"accounts"`
}

// PresenceSnapshotReply is the bulk presence RPC reply; absent accounts are treated fail-open.
type PresenceSnapshotReply struct {
	Presences map[string]Presence `json:"presences" bson:"presences"`
}

// Presence is an account's aggregated status. Only "busy" and "in-call" suppress push.
type Presence struct {
	AggregatedStatus string `json:"aggregatedStatus" bson:"aggregatedStatus"`
}

// PresenceStatus is a user's effective or manual presence value.
type PresenceStatus string

const (
	StatusOnline        PresenceStatus = "online"
	StatusAway          PresenceStatus = "away"
	StatusBusy          PresenceStatus = "busy"
	StatusOffline       PresenceStatus = "offline"
	StatusAppearOffline PresenceStatus = "appear_offline" // manual-only
	StatusInCall        PresenceStatus = "in-call"        // external-only (Teams); DND, never manual
	StatusNone          PresenceStatus = ""               // no manual override / clear
)

// Hello initializes a connection. The client sends it once when a connection
// (browser tab / SharedWorker / socket) comes up. Fire-and-forget; the connId
// is client-generated and reused for that connection's ping/activity/bye.
type Hello struct {
	ConnID    string `json:"connId"    bson:"connId"`
	Timestamp int64  `json:"timestamp" bson:"timestamp"`
}

// Ping refreshes a connection's liveness (resets its TTL). Fire-and-forget,
// sent roughly every 15s. It does not change activity; the server only
// recomputes status when the ping creates a not-yet-seen connection.
type Ping struct {
	ConnID    string `json:"connId"    bson:"connId"`
	Timestamp int64  `json:"timestamp" bson:"timestamp"`
}

// Activity marks a single connection active or inactive based on client-side
// interaction (mouse/keyboard). Fire-and-forget, sent only when the idle edge
// flips. away=true marks the connection inactive; the server aggregates across
// connections (all inactive -> away).
type Activity struct {
	ConnID    string `json:"connId"    bson:"connId"`
	Away      bool   `json:"away"      bson:"away"`
	Timestamp int64  `json:"timestamp" bson:"timestamp"`
}

// ByeRequest is a best-effort client signal on disconnect (tab close).
type ByeRequest struct {
	ConnID    string `json:"connId"    bson:"connId"`
	Timestamp int64  `json:"timestamp" bson:"timestamp"`
}

// ManualStatusRequest sets or clears a user's manual override. Status ""
// (StatusNone) clears it.
type ManualStatusRequest struct {
	Status    PresenceStatus `json:"status"    bson:"status"`
	Timestamp int64          `json:"timestamp" bson:"timestamp"`
}

// ManualStatusResponse is the reply to a manual-set request.
type ManualStatusResponse struct {
	Account   string         `json:"account"   bson:"account"`
	Status    PresenceStatus `json:"status"    bson:"status"`
	SetAt     int64          `json:"setAt"     bson:"setAt"`
	Effective PresenceStatus `json:"effective" bson:"effective"`
}

// PresenceQuery is a batch initial-state request body.
type PresenceQuery struct {
	Accounts []string `json:"accounts" bson:"accounts"`
}

// PresenceState is one user's published effective status.
type PresenceState struct {
	Account   string         `json:"account"   bson:"account"`
	SiteID    string         `json:"siteId"    bson:"siteId"`
	Status    PresenceStatus `json:"status"    bson:"status"`
	Timestamp int64          `json:"timestamp" bson:"timestamp"`
}

// PresenceQueryResponse is the batch-query reply.
type PresenceQueryResponse struct {
	States    []PresenceState `json:"states"    bson:"states"`
	Timestamp int64           `json:"timestamp" bson:"timestamp"`
}
