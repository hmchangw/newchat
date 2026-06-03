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
