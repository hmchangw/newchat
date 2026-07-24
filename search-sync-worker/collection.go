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
