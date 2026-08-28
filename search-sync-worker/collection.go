package main

import (
	"encoding/json"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/searchengine"
)

// Collection defines a search-indexable data source. Each collection
// encapsulates its own stream config, ES template, and document mapping.
// To add a new collection (e.g., room search), implement this interface.
type Collection interface {
	// StreamConfig returns the JetStream stream to create/update and consume
	// from. Each collection supplies a native jetstream.StreamConfig so it
	// can configure Subjects, Sources, SubjectTransforms, etc. as needed.
	StreamConfig(siteID string) jetstream.StreamConfig
	// ConsumerName returns the durable consumer name for this collection.
	ConsumerName() string
	// FilterSubjects returns the list of subjects this collection's consumer
	// should subscribe to. An empty slice means "all subjects in the stream".
	FilterSubjects(siteID string) []string
	// TemplateName returns the ES index template name. Empty string means the
	// collection has no index template to upsert (e.g., single-index targets).
	TemplateName() string
	// TemplateBody returns the ES index template JSON. nil means no template.
	TemplateBody() json.RawMessage
	// MappingUpdate returns a pattern + additive `{"properties":...}` PUT onto
	// existing indices at startup (rolling indices only). Empty/nil = no update.
	MappingUpdate() (indexPattern string, body json.RawMessage)
	// StoredScripts returns the ES stored scripts this collection depends on,
	// keyed by script id. Each value is the full `PUT /_scripts/{id}` body.
	// nil/empty means the collection inlines no scripts (or uses none). The
	// worker registers these at startup before consuming so BuildAction can
	// emit lightweight `{"script":{"id":...}}` references instead of repeating
	// the full source in every fan-out bulk action.
	StoredScripts() map[string]json.RawMessage
	// BuildAction converts the already-decompressed JetStream message body
	// into one or more BulkActions (empty slice = ack with no ES write,
	// e.g. filtered).
	BuildAction(data []byte) ([]searchengine.BulkAction, error)
}

// ByQueryCollection is an optional Collection capability for events that mutate
// many documents keyed by a field rather than by DocID — a shape the DocID-keyed
// Bulk API can't express. A collection implementing it gets first crack at a
// message: ok=true means "I handled it as one _update_by_query against index";
// ok=false falls through to the normal BuildAction bulk path. Spotlight uses it
// for room_renamed (re-index roomName across every member's doc).
type ByQueryCollection interface {
	BuildByQuery(data []byte) (index string, body json.RawMessage, ok bool, err error)
}

// sequencedCollection is a Collection whose ordering guard needs the source
// message's JetStream stream sequence — a strict total order assigned
// server-side at publish. Handler routes through BuildActionSeq when a
// collection implements this, and through BuildAction otherwise. Same
// optional-capability shape as ByQueryCollection above.
//
// Only spotlight-org needs it. The INBOX and canonical collections carry an
// event timestamp in their payload and guard on that (external versioning for
// messages/spotlight, a painless timestamp compare for user-room); the HR
// employees.upsert payload is a bare array with no timestamp of its own, so the
// stream sequence is the only ordering token available to it.
type sequencedCollection interface {
	Collection
	BuildActionSeq(data []byte, streamSeq uint64) ([]searchengine.BulkAction, error)
}
