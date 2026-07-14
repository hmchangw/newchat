package model

import "encoding/json"

// OplogEvent is the envelope the oplog-connector publishes to MIGRATION_OPLOG_{siteID} per change. Documents stay opaque (json.RawMessage); the transformer decodes them per collection.
type OplogEvent struct {
	// EventID is the change-stream event id (_id._data), also set as Nats-Msg-Id so
	// JetStream dedup collapses replays and the migration-handoff overlap.
	EventID string `json:"eventId" bson:"eventId"`
	// Op is the change-stream operation type: insert | update | replace | delete.
	Op string `json:"op" bson:"op"`
	DB string `json:"db" bson:"db"`
	// Collection is the raw source collection name (e.g. rocketchat_message).
	Collection string `json:"coll" bson:"coll"`
	// DocumentKey is the changed document's key, opaque ({ _id: ... }).
	DocumentKey json.RawMessage `json:"documentKey" bson:"documentKey"`
	// ClusterTime is the source op time in unix milliseconds.
	ClusterTime int64 `json:"clusterTime" bson:"clusterTime"`
	// FullDocument is the document, present natively for insert and replace
	// (it's in the oplog entry — no lookup). Absent for update and delete.
	FullDocument json.RawMessage `json:"fullDocument,omitempty" bson:"fullDocument,omitempty"`
	// UpdateDescription is the raw change delta (updatedFields/removedFields/truncatedArrays)
	// for update ops, forwarded verbatim; no updateLookup post-image (that's a downstream lookup).
	UpdateDescription json.RawMessage `json:"updateDescription,omitempty" bson:"updateDescription,omitempty"`
	// Degraded is true when one or more opaque fields could not be encoded; the affected field
	// is omitted. The event is still published (never dropped) so the stream stays lossless.
	Degraded bool `json:"degraded,omitempty" bson:"degraded,omitempty"`
	// DegradedReason describes which field failed and why (first failure wins).
	DegradedReason string `json:"degradedReason,omitempty" bson:"degradedReason,omitempty"`
	SiteID         string `json:"siteId" bson:"siteId"`
	// Timestamp is the event-level publish time in unix milliseconds.
	Timestamp int64 `json:"timestamp" bson:"timestamp"`
}
