# oplog-direct-transfer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new `data-migration/oplog-direct-transfer` service that verbatim-copies a configured set of source Mongo collections into the same-named new-stack per-site Mongo collections, keyed by source `_id`, mirroring insert/replace/update/delete.

**Architecture:** A JetStream consumer on the connector-owned `MIGRATION_OPLOG_{site}` stream, filtered to the direct-transfer collections. Each event is dispatched by op: insert/replace upsert the inline doc, update re-reads the current source doc then upserts, delete removes by `_id`. Docs are opaque `bson.D` (no typed model). Reuses `pkg/migration` (disposition + `SourceLookup`) and the shared `oplog-connector`.

**Tech Stack:** Go 1.25, `nats.go/jetstream`, `mongo-driver/v2`, `caarlos0/env`, OpenTelemetry + Prometheus, `testify`, `testcontainers-go`.

**Reference template:** `data-migration/oplog-collections-transformer/` is a near-identical sibling — mirror its structure. This plan gives full code for the parts that differ (config, event, handler, store) and copy-and-adapt instructions for the boilerplate (metrics, main, bootstrap, deploy).

**Spec:** `docs/superpowers/specs/2026-07-01-oplog-direct-transfer-design.md`

**Key invariant — `_id` may be any BSON type.** These are arbitrary system collections, so `_id` can be a string OR an ObjectId OR an int. Never decode `_id` into a Go `string`. Always carry it as `any` (decoded from extended JSON, it becomes the correct Go type) and use it in a `bson.D{{Key: "_id", Value: id}}` filter.

**All commands run from repo root** (`/home/user/chat`). Use `make`, never raw `go`.

---

### Task 1: Scaffold + config

**Files:**
- Create: `data-migration/oplog-direct-transfer/config.go`
- Test: `data-migration/oplog-direct-transfer/config_test.go`

- [ ] **Step 1: Write the failing test**

Create `data-migration/oplog-direct-transfer/config_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setRequiredEnv(t *testing.T) {
	t.Setenv("SITE_ID", "site1")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SOURCE_MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("TARGET_MONGO_URI", "mongodb://localhost:27018")
}

func TestParseConfig_Defaults(t *testing.T) {
	setRequiredEnv(t)
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.Equal(t, "site1", cfg.SiteID)
	assert.Equal(t, "rocketchat", cfg.SourceDB)
	assert.Equal(t, "chat", cfg.TargetDB)
	assert.Equal(t, "oplog-direct-transfer", cfg.ConsumerDurable)
	assert.Contains(t, cfg.DirectCollections, "rocketchat_avatar")
	assert.Contains(t, cfg.DirectCollections, "user_devices")
	assert.Len(t, cfg.DirectCollections, 8)
}

func TestParseConfig_TrimsAndValidatesRequired(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SITE_ID", "   ")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SITE_ID")
}

func TestParseConfig_RejectsEmptyCollectionEntry(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("DIRECT_COLLECTIONS", "rocketchat_avatar,,user_devices")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DIRECT_COLLECTIONS")
}

func TestParseConfig_RejectsDuplicateCollection(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("DIRECT_COLLECTIONS", "rocketchat_avatar,rocketchat_avatar")
	_, err := parseConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=data-migration/oplog-direct-transfer`
Expected: FAIL — `config.go` / `parseConfig` don't exist (build error).

- [ ] **Step 3: Write `config.go`**

```go
package main

import (
	"fmt"
	"strings"

	"github.com/caarlos0/env/v11"
)

// config holds every tunable, parsed from the environment via caarlos0/env.
// Required fields have no default and fail-fast at startup when absent.
type config struct {
	SiteID string `env:"SITE_ID,required"`

	// DirectCollections is the set of source collections copied verbatim to the same-named
	// destination collection. Config-driven so adding one is an env + WATCH_COLLECTIONS change.
	DirectCollections []string `env:"DIRECT_COLLECTIONS" envSeparator:"," envDefault:"rocketchat_avatar,company_apps_v,company_bot_cmd_men,company_tsso_tokens,rocketchat_uploads,company_bot_authorization,ufsTokens,user_devices"`

	NatsURL       string `env:"NATS_URL,required"`
	NatsCredsFile string `env:"NATS_CREDS_FILE" envDefault:""`

	// Source legacy Mongo: re-read the full current doc by _id on update events.
	SourceMongoURI string `env:"SOURCE_MONGO_URI,required"`
	SourceUsername string `env:"SOURCE_MONGO_USERNAME" envDefault:""`
	SourcePassword string `env:"SOURCE_MONGO_PASSWORD" envDefault:""`
	SourceDB       string `env:"SOURCE_DB" envDefault:"rocketchat"`

	// Target new-stack per-site Mongo: verbatim upsert/delete by _id.
	TargetMongoURI string `env:"TARGET_MONGO_URI,required"`
	TargetUsername string `env:"TARGET_MONGO_USERNAME" envDefault:""`
	TargetPassword string `env:"TARGET_MONGO_PASSWORD" envDefault:""`
	TargetDB       string `env:"TARGET_DB" envDefault:"chat"`

	SourceReadPreference string `env:"SOURCE_READ_PREFERENCE" envDefault:"primaryPreferred"`

	ConsumerDurable string `env:"CONSUMER_DURABLE" envDefault:"oplog-direct-transfer"`
	MaxDeliver      int    `env:"MAX_DELIVER" envDefault:"1000"`

	Bootstrap bootstrapConfig `envPrefix:"BOOTSTRAP_"`

	MetricsAddr string `env:"METRICS_ADDR" envDefault:":9090"`
	LogLevel    string `env:"LOG_LEVEL" envDefault:"info"`
}

type bootstrapConfig struct {
	Enabled bool `env:"STREAMS" envDefault:"false"`
}

// parseConfig parses and validates the environment configuration.
func parseConfig() (config, error) {
	cfg, err := env.ParseAs[config]()
	if err != nil {
		return config{}, fmt.Errorf("parse config: %w", err)
	}
	// `required` only rejects an unset var, not whitespace. Trim + re-validate required scalars.
	cfg.SiteID = strings.TrimSpace(cfg.SiteID)
	cfg.NatsURL = strings.TrimSpace(cfg.NatsURL)
	cfg.SourceMongoURI = strings.TrimSpace(cfg.SourceMongoURI)
	cfg.TargetMongoURI = strings.TrimSpace(cfg.TargetMongoURI)
	for name, v := range map[string]string{
		"SITE_ID":          cfg.SiteID,
		"NATS_URL":         cfg.NatsURL,
		"SOURCE_MONGO_URI": cfg.SourceMongoURI,
		"TARGET_MONGO_URI": cfg.TargetMongoURI,
	} {
		if v == "" {
			return config{}, fmt.Errorf("%s must be non-empty", name)
		}
	}
	// Trim, non-empty, dedup the collection list — each maps to one consumer subject + one lookup.
	seen := make(map[string]struct{}, len(cfg.DirectCollections))
	trimmed := make([]string, 0, len(cfg.DirectCollections))
	for _, c := range cfg.DirectCollections {
		c = strings.TrimSpace(c)
		if c == "" {
			return config{}, fmt.Errorf("DIRECT_COLLECTIONS has an empty entry (check for stray commas)")
		}
		if _, dup := seen[c]; dup {
			return config{}, fmt.Errorf("DIRECT_COLLECTIONS has duplicate entry %q", c)
		}
		seen[c] = struct{}{}
		trimmed = append(trimmed, c)
	}
	if len(trimmed) == 0 {
		return config{}, fmt.Errorf("DIRECT_COLLECTIONS must list at least one collection")
	}
	cfg.DirectCollections = trimmed
	return cfg, nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `make test SERVICE=data-migration/oplog-direct-transfer`
Expected: PASS (4 config tests).

- [ ] **Step 5: Commit**

```bash
git add data-migration/oplog-direct-transfer/config.go data-migration/oplog-direct-transfer/config_test.go
git commit -m "feat(oplog-direct-transfer): config parsing + validation"
```

---

### Task 2: Event decode + `_id` extraction

**Files:**
- Create: `data-migration/oplog-direct-transfer/event.go`
- Test: `data-migration/oplog-direct-transfer/event_test.go`

- [ ] **Step 1: Write the failing test**

Create `data-migration/oplog-direct-transfer/event_test.go`:

```go
package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/migration"
)

func TestDocumentID_StringID(t *testing.T) {
	id, err := documentID(json.RawMessage(`{"_id":"abc123"}`))
	require.NoError(t, err)
	assert.Equal(t, "abc123", id)
}

func TestDocumentID_ObjectID(t *testing.T) {
	// Extended-JSON ObjectId decodes to a non-string BSON type — must not error.
	id, err := documentID(json.RawMessage(`{"_id":{"$oid":"5f9b1c2d3e4a5b6c7d8e9f01"}}`))
	require.NoError(t, err)
	assert.NotNil(t, id)
}

func TestDocumentID_MissingID_Poison(t *testing.T) {
	_, err := documentID(json.RawMessage(`{}`))
	assert.ErrorIs(t, err, migration.ErrPoison)
}

func TestDocumentID_Malformed_Poison(t *testing.T) {
	_, err := documentID(json.RawMessage(`{bad`))
	assert.ErrorIs(t, err, migration.ErrPoison)
}

func TestDecodeExtJSONDoc_PreservesFields(t *testing.T) {
	doc, err := decodeExtJSONDoc(json.RawMessage(`{"_id":"u1","name":"avatar","n":3}`))
	require.NoError(t, err)
	m := map[string]any{}
	for _, e := range doc {
		m[e.Key] = e.Value
	}
	assert.Equal(t, "u1", m["_id"])
	assert.Equal(t, "avatar", m["name"])
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=data-migration/oplog-direct-transfer`
Expected: FAIL — `event.go` doesn't exist.

- [ ] **Step 3: Write `event.go`**

```go
package main

import (
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/migration"
)

// oplogEvent is the subset of the connector's event wire shape this service needs.
// fullDocument/documentKey are relaxed extended JSON (the connector's encoding).
type oplogEvent struct {
	EventID      string          `json:"eventId"`
	Op           string          `json:"op"`
	Collection   string          `json:"coll"`
	DocumentKey  json.RawMessage `json:"documentKey"`
	FullDocument json.RawMessage `json:"fullDocument"`
}

// documentID extracts the _id value from a documentKey/doc as its native BSON type (string,
// ObjectID, int, …) — NOT forced to string, since these collections may key by any type.
// Returns migration.ErrPoison when the payload is malformed or has no _id.
func documentID(raw json.RawMessage) (any, error) {
	var d bson.D
	if err := bson.UnmarshalExtJSON(raw, false, &d); err != nil {
		return nil, fmt.Errorf("%w: bad documentKey: %v", migration.ErrPoison, err) //nolint:errorlint // single-%w sentinel wrap; decode err is informational
	}
	for _, e := range d {
		if e.Key == "_id" {
			return e.Value, nil
		}
	}
	return nil, fmt.Errorf("%w: documentKey has no _id", migration.ErrPoison)
}

// decodeExtJSONDoc decodes a relaxed-extJSON document into an opaque, type-preserving bson.D.
func decodeExtJSONDoc(raw json.RawMessage) (bson.D, error) {
	var d bson.D
	if err := bson.UnmarshalExtJSON(raw, false, &d); err != nil {
		return nil, fmt.Errorf("%w: decode source doc: %v", migration.ErrPoison, err) //nolint:errorlint // single-%w sentinel wrap; decode err is informational
	}
	return d, nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `make test SERVICE=data-migration/oplog-direct-transfer`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add data-migration/oplog-direct-transfer/event.go data-migration/oplog-direct-transfer/event_test.go
git commit -m "feat(oplog-direct-transfer): event decode + any-typed _id extraction"
```

---

### Task 3: Metrics

**Files:**
- Create: `data-migration/oplog-direct-transfer/metrics.go`
- Test: `data-migration/oplog-direct-transfer/metrics_test.go`

- [ ] **Step 1: Copy the sibling metrics as a starting point**

Run:
```bash
cp data-migration/oplog-collections-transformer/metrics.go data-migration/oplog-direct-transfer/metrics.go
cp data-migration/oplog-collections-transformer/metrics_test.go data-migration/oplog-direct-transfer/metrics_test.go
```

- [ ] **Step 2: Adapt `metrics.go`** — rename the meter/metric names and drop the two counters this service doesn't use (`userSeed`, `resolveMiss`); add a `writes` counter labelled by op+collection+action (upsert/delete). Final `metrics.go`:

```go
package main

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// metrics holds the service's instruments. Nil-safe (tests run without a meter).
type metrics struct {
	processed metric.Int64Counter
	naks      metric.Int64Counter
	terms     metric.Int64Counter
	skipped   metric.Int64Counter
	exhausted metric.Int64Counter
	writes    metric.Int64Counter
}

func newMetrics() (*metrics, error) {
	m := otel.Meter("oplog-direct-transfer")
	processed, err := m.Int64Counter("oplog_direct_transfer_events_processed_total",
		metric.WithDescription("oplog events handled and acked, by op+collection"))
	if err != nil {
		return nil, fmt.Errorf("processed counter: %w", err)
	}
	naks, err := m.Int64Counter("oplog_direct_transfer_naks_total",
		metric.WithDescription("transient failures naked for redelivery, by op+collection"))
	if err != nil {
		return nil, fmt.Errorf("naks counter: %w", err)
	}
	terms, err := m.Int64Counter("oplog_direct_transfer_terms_total",
		metric.WithDescription("poison/undecodable events termed, by op+collection"))
	if err != nil {
		return nil, fmt.Errorf("terms counter: %w", err)
	}
	skipped, err := m.Int64Counter("oplog_direct_transfer_events_skipped_total",
		metric.WithDescription("events deliberately skipped, by reason"))
	if err != nil {
		return nil, fmt.Errorf("skipped counter: %w", err)
	}
	exhausted, err := m.Int64Counter("oplog_direct_transfer_exhausted_total",
		metric.WithDescription("events termed after reaching MaxDeliver, by op+collection"))
	if err != nil {
		return nil, fmt.Errorf("exhausted counter: %w", err)
	}
	writes, err := m.Int64Counter("oplog_direct_transfer_writes_total",
		metric.WithDescription("target writes, by collection+action (upsert/delete)"))
	if err != nil {
		return nil, fmt.Errorf("writes counter: %w", err)
	}
	return &metrics{processed: processed, naks: naks, terms: terms, skipped: skipped, exhausted: exhausted, writes: writes}, nil
}

func opCollAttr(op, collection string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("op", op), attribute.String("collection", collection))
}

func (m *metrics) onProcessed(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.processed.Add(ctx, 1, opCollAttr(op, collection))
}

func (m *metrics) onNak(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.naks.Add(ctx, 1, opCollAttr(op, collection))
}

func (m *metrics) onTerm(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.terms.Add(ctx, 1, opCollAttr(op, collection))
}

func (m *metrics) onSkipped(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	m.skipped.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func (m *metrics) onExhausted(ctx context.Context, op, collection string) {
	if m == nil {
		return
	}
	m.exhausted.Add(ctx, 1, opCollAttr(op, collection))
}

func (m *metrics) onWrite(ctx context.Context, collection, action string) {
	if m == nil {
		return
	}
	m.writes.Add(ctx, 1, metric.WithAttributes(attribute.String("collection", collection), attribute.String("action", action)))
}
```

- [ ] **Step 3: Adapt `metrics_test.go`** — replace the metric-name assertions to match the new names, drop `onUserSeed`/`onResolveMiss` calls, add `onWrite`. Final `metrics_test.go`:

```go
package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewMetrics(t *testing.T) {
	m, err := newMetrics()
	require.NoError(t, err)
	require.NotNil(t, m)
	m.onProcessed(context.Background(), "insert", "rocketchat_avatar")
	m.onNak(context.Background(), "update", "rocketchat_avatar")
	m.onTerm(context.Background(), "insert", "ufsTokens")
	m.onSkipped(context.Background(), "other_collection")
	m.onExhausted(context.Background(), "update", "rocketchat_avatar")
	m.onWrite(context.Background(), "rocketchat_avatar", "upsert")
}

func TestMetrics_NilSafe(t *testing.T) {
	var m *metrics
	require.NotPanics(t, func() {
		m.onProcessed(context.Background(), "insert", "rocketchat_avatar")
		m.onNak(context.Background(), "update", "rocketchat_avatar")
		m.onTerm(context.Background(), "insert", "ufsTokens")
		m.onSkipped(context.Background(), "other_collection")
		m.onExhausted(context.Background(), "update", "rocketchat_avatar")
		m.onWrite(context.Background(), "rocketchat_avatar", "delete")
	})
}

func TestMetrics_WriteCounterCarriesCollectionAndAction(t *testing.T) {
	prev := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	m, err := newMetrics()
	require.NoError(t, err)
	ctx := context.Background()
	m.onWrite(ctx, "user_devices", "delete")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "oplog_direct_transfer_writes_total" {
				continue
			}
			sum, ok := md.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			attrs := sum.DataPoints[0].Attributes
			coll, _ := attrs.Value(attribute.Key("collection"))
			action, _ := attrs.Value(attribute.Key("action"))
			assert.Equal(t, "user_devices", coll.AsString())
			assert.Equal(t, "delete", action.AsString())
			found = true
		}
	}
	assert.True(t, found, "writes counter recorded")
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `make test SERVICE=data-migration/oplog-direct-transfer`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add data-migration/oplog-direct-transfer/metrics.go data-migration/oplog-direct-transfer/metrics_test.go
git commit -m "feat(oplog-direct-transfer): metrics"
```

---

### Task 4: Handler (core dispatch)

**Files:**
- Create: `data-migration/oplog-direct-transfer/handler.go`
- Test: `data-migration/oplog-direct-transfer/handler_test.go`

- [ ] **Step 1: Write the failing test**

Create `data-migration/oplog-direct-transfer/handler_test.go`:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/migration"
)

// fakeTarget records upserts/deletes.
type fakeTarget struct {
	upserts   []writeCall
	deletes   []writeCall
	upsertErr error
	deleteErr error
}

type writeCall struct {
	collection string
	id         any
	doc        bson.D
}

func (f *fakeTarget) UpsertByID(_ context.Context, collection string, id any, doc bson.D) error {
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.upserts = append(f.upserts, writeCall{collection, id, doc})
	return nil
}

func (f *fakeTarget) DeleteByID(_ context.Context, collection string, id any) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deletes = append(f.deletes, writeCall{collection: collection, id: id})
	return nil
}

// fakeLookup returns a fixed doc (or nil = vanished) for the update re-read.
type fakeLookup struct {
	doc []byte
	err error
}

func (f *fakeLookup) FindByID(_ context.Context, _ string) ([]byte, error) {
	return f.doc, f.err
}

const testColl = "rocketchat_avatar"

func newTestHandler(target targetStore, lk migration.SourceLookup) *handler {
	return &handler{
		collections: map[string]struct{}{testColl: {}},
		lookups:     map[string]migration.SourceLookup{testColl: lk},
		target:      target,
	}
}

func TestHandle_Insert_Upserts(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{
		Op: "insert", Collection: testColl,
		DocumentKey:  json.RawMessage(`{"_id":"a1"}`),
		FullDocument: json.RawMessage(`{"_id":"a1","blob":"x"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.upserts, 1)
	assert.Equal(t, testColl, tgt.upserts[0].collection)
	assert.Equal(t, "a1", tgt.upserts[0].id)
	assert.Empty(t, tgt.deletes)
}

func TestHandle_Replace_Upserts(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{
		Op: "replace", Collection: testColl,
		DocumentKey:  json.RawMessage(`{"_id":"a1"}`),
		FullDocument: json.RawMessage(`{"_id":"a1","blob":"y"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.upserts, 1)
}

func TestHandle_Update_ReReadsThenUpserts(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{doc: []byte(`{"_id":"a1","blob":"fresh"}`)})
	ev := oplogEvent{
		Op: "update", Collection: testColl,
		DocumentKey: json.RawMessage(`{"_id":"a1"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.upserts, 1)
	assert.Equal(t, "a1", tgt.upserts[0].id)
}

func TestHandle_Update_DocVanished_Skips(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{doc: nil}) // vanished
	ev := oplogEvent{
		Op: "update", Collection: testColl,
		DocumentKey: json.RawMessage(`{"_id":"a1"}`),
	}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrSkipped)
	assert.Empty(t, tgt.upserts)
}

func TestHandle_Delete_DeletesByID(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{
		Op: "delete", Collection: testColl,
		DocumentKey: json.RawMessage(`{"_id":"a1"}`),
	}
	require.NoError(t, h.handle(context.Background(), ev))
	require.Len(t, tgt.deletes, 1)
	assert.Equal(t, "a1", tgt.deletes[0].id)
	assert.Empty(t, tgt.upserts)
}

func TestHandle_OtherCollection_Skips(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "insert", Collection: "not_watched", DocumentKey: json.RawMessage(`{"_id":"a1"}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrSkipped)
}

func TestHandle_Insert_NoFullDocument_Poison(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "insert", Collection: testColl, DocumentKey: json.RawMessage(`{"_id":"a1"}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
}

func TestHandle_BadDocumentKey_Poison(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "delete", Collection: testColl, DocumentKey: json.RawMessage(`{}`)}
	err := h.handle(context.Background(), ev)
	assert.ErrorIs(t, err, migration.ErrPoison)
}

func TestHandle_Delete_TargetError_Nak(t *testing.T) {
	tgt := &fakeTarget{deleteErr: errors.New("mongo down")}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "delete", Collection: testColl, DocumentKey: json.RawMessage(`{"_id":"a1"}`)}
	err := h.handle(context.Background(), ev)
	require.Error(t, err)
	assert.NotErrorIs(t, err, migration.ErrPoison)
	assert.NotErrorIs(t, err, migration.ErrSkipped)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `make test SERVICE=data-migration/oplog-direct-transfer`
Expected: FAIL — `handler.go` / `handler` / `targetStore` don't exist.

- [ ] **Step 3: Write `handler.go`**

```go
package main

import (
	"context"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// targetStore is the verbatim per-collection write surface, keyed by native-typed _id.
type targetStore interface {
	UpsertByID(ctx context.Context, collection string, id any, doc bson.D) error
	DeleteByID(ctx context.Context, collection string, id any) error
}

type handler struct {
	collections map[string]struct{}                // watched direct-transfer collections (defense-in-depth vs the subject filter)
	lookups     map[string]migration.SourceLookup  // one per collection, for the update re-read
	target      targetStore
	metrics     *metrics // nil-safe
}

// handle maps one decoded event to a verbatim target write. nil = ack+count; ErrSkipped =
// ack-without-count (already metered); ErrPoison => Term; any other error => Nak (transient).
//
//nolint:gocritic // ev passed by value: one per message off the consume loop, off the hot path.
func (h *handler) handle(ctx context.Context, ev oplogEvent) error {
	if _, ok := h.collections[ev.Collection]; !ok {
		// The consumer's subject filter should prevent this; skip defensively without a write.
		h.metrics.onSkipped(ctx, "other_collection")
		return migration.ErrSkipped
	}

	id, err := documentID(ev.DocumentKey)
	if err != nil {
		return err // poison
	}

	if ev.Op == "delete" {
		if derr := h.target.DeleteByID(ctx, ev.Collection, id); derr != nil {
			return fmt.Errorf("delete %s: %w", ev.Collection, derr)
		}
		h.metrics.onWrite(ctx, ev.Collection, "delete")
		return nil
	}

	doc, skip, err := h.resolveDoc(ctx, ev, id)
	if err != nil {
		return err
	}
	if skip {
		h.metrics.onSkipped(ctx, ev.Op+"_gone")
		return migration.ErrSkipped
	}
	if derr := h.target.UpsertByID(ctx, ev.Collection, id, doc); derr != nil {
		return fmt.Errorf("upsert %s: %w", ev.Collection, derr)
	}
	h.metrics.onWrite(ctx, ev.Collection, "upsert")
	return nil
}

// resolveDoc returns the verbatim doc to upsert. insert/replace carry it inline; update re-reads
// the current source doc by _id (skip=true when it vanished between event and re-read).
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func (h *handler) resolveDoc(ctx context.Context, ev oplogEvent, id any) (bson.D, bool, error) {
	switch ev.Op {
	case "insert", "replace":
		if len(ev.FullDocument) == 0 {
			return nil, false, fmt.Errorf("%w: %s without fullDocument", migration.ErrPoison, ev.Op)
		}
		doc, err := decodeExtJSONDoc(ev.FullDocument)
		return doc, false, err
	case "update":
		lk := h.lookups[ev.Collection]
		if lk == nil {
			return nil, false, fmt.Errorf("%w: no source lookup for collection %q", migration.ErrPoison, ev.Collection)
		}
		got, err := lk.FindByID(ctx, fmt.Sprintf("%v", id))
		if err != nil {
			return nil, false, fmt.Errorf("lookup %v: %w", id, err)
		}
		if got == nil {
			slog.Debug("skip update — source doc vanished", "collection", ev.Collection,
				"eventId", ev.EventID, "request_id", natsutil.RequestIDFromContext(ctx))
			return nil, true, nil
		}
		doc, derr := decodeExtJSONDoc(got)
		return doc, false, derr
	default:
		// Unknown op — skip (metered by caller).
		return nil, true, nil
	}
}
```

> **Note on `SourceLookup.FindByID(ctx, id string)`:** the shared interface takes a `string` id. For string `_id`s this is exact. For non-string `_id`s, `fmt.Sprintf("%v", id)` is a lossy key — see Task 9's integration note. If any direct-transfer collection turns out to use a non-string `_id` that receives `update` events, extend `pkg/migration.SourceLookup` with a `FindByRawID(ctx, id any)` in a follow-up. For the initial 8 (RocketChat/Company app collections, string-keyed), the string path is correct.

- [ ] **Step 4: Run to verify it passes**

Run: `make test SERVICE=data-migration/oplog-direct-transfer`
Expected: PASS (all handler tests).

- [ ] **Step 5: Commit**

```bash
git add data-migration/oplog-direct-transfer/handler.go data-migration/oplog-direct-transfer/handler_test.go
git commit -m "feat(oplog-direct-transfer): op dispatch (upsert/re-read/delete)"
```

---

### Task 5: Target store (Mongo)

**Files:**
- Create: `data-migration/oplog-direct-transfer/store_mongo.go`
- Test: covered by the integration test in Task 8 (needs a real Mongo).

- [ ] **Step 1: Write `store_mongo.go`**

```go
package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mongoTargetStore writes verbatim docs into arbitrary collections of the target DB, keyed by _id.
type mongoTargetStore struct {
	db *mongo.Database
}

// Compile-time assertion that *mongoTargetStore satisfies targetStore.
var _ targetStore = (*mongoTargetStore)(nil)

// NewMongoTargetStore binds the target database; collections are resolved per-call by name.
func NewMongoTargetStore(db *mongo.Database) *mongoTargetStore {
	return &mongoTargetStore{db: db}
}

// UpsertByID replaces (or inserts) the doc keyed by _id. Idempotent under redelivery.
func (s *mongoTargetStore) UpsertByID(ctx context.Context, collection string, id any, doc bson.D) error {
	_, err := s.db.Collection(collection).ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}}, doc, options.Replace().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("upsert into %s: %w", collection, err)
	}
	return nil
}

// DeleteByID removes the doc keyed by _id. A missing row deletes nothing and is not an error.
func (s *mongoTargetStore) DeleteByID(ctx context.Context, collection string, id any) error {
	if _, err := s.db.Collection(collection).DeleteOne(ctx, bson.D{{Key: "_id", Value: id}}); err != nil {
		return fmt.Errorf("delete from %s: %w", collection, err)
	}
	return nil
}
```

- [ ] **Step 2: Verify it builds**

Run: `make build SERVICE=data-migration/oplog-direct-transfer`
Expected: build fails — `main.go` not written yet (no `main` func). That's OK; instead verify the package compiles via vet:
Run: `go vet ./data-migration/oplog-direct-transfer/...`
Expected: no errors (or only "function main is undefined" if vet requires it — proceed to Task 6 which adds main).

- [ ] **Step 3: Commit**

```bash
git add data-migration/oplog-direct-transfer/store_mongo.go
git commit -m "feat(oplog-direct-transfer): mongo target store (upsert/delete by _id)"
```

---

### Task 6: Bootstrap + main wiring + processOne

**Files:**
- Create: `data-migration/oplog-direct-transfer/bootstrap.go`
- Create: `data-migration/oplog-direct-transfer/main.go`
- Test: `data-migration/oplog-direct-transfer/processone_test.go`

- [ ] **Step 1: Copy bootstrap from the sibling (identical)**

Run:
```bash
cp data-migration/oplog-collections-transformer/bootstrap.go data-migration/oplog-direct-transfer/bootstrap.go
```
No edits needed — it references only `stream.MigrationOplog` + `bootstrapConfig`, both present.

- [ ] **Step 2: Write `main.go`** — adapt the sibling's `main.go`. Differences: build a `collections` set + a per-collection `lookups` map from `cfg.DirectCollections`; `FilterSubjects` built by looping the collections; no `DeleteMaxDeliver` (delete is a successful no-op here, never Naks on "unrecognized"). Write:

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/otelutil"
	"github.com/hmchangw/chat/pkg/shutdown"
	"github.com/hmchangw/chat/pkg/stream"
	"github.com/hmchangw/chat/pkg/subject"
)

func main() {
	cfg, err := parseConfig()
	if err != nil {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
		slog.Error("parse config", "error", err)
		os.Exit(1)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)})))

	ctx := context.Background()

	tracerShutdown, err := otelutil.InitTracer(ctx, "oplog-direct-transfer")
	if err != nil {
		slog.Error("init tracer failed", "error", err)
		os.Exit(1)
	}
	meterShutdown, err := otelutil.InitMeter("oplog-direct-transfer")
	if err != nil {
		slog.Error("init meter failed", "error", err)
		os.Exit(1)
	}
	m, err := newMetrics()
	if err != nil {
		slog.Error("init metrics failed", "error", err)
		os.Exit(1)
	}

	metricsServer := newMetricsServer()
	ln, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		slog.Error("metrics listen failed", "addr", cfg.MetricsAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		slog.Info("metrics+health server listening", "addr", cfg.MetricsAddr)
		if err := metricsServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", err)
		}
	}()

	source, err := mongoutil.Connect(ctx, cfg.SourceMongoURI, cfg.SourceUsername, cfg.SourcePassword)
	if err != nil {
		slog.Error("source mongo connect failed", "error", err)
		os.Exit(1)
	}
	rp, err := readPreference(cfg.SourceReadPreference)
	if err != nil {
		slog.Error("read preference invalid", "error", err)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}
	sourceDB := source.Database(cfg.SourceDB)

	targetClient, err := mongoutil.Connect(ctx, cfg.TargetMongoURI, cfg.TargetUsername, cfg.TargetPassword)
	if err != nil {
		slog.Error("target mongo connect failed", "error", err)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	// Per-collection source lookups (update re-read) + the watched-collection set + subject filters.
	collections := make(map[string]struct{}, len(cfg.DirectCollections))
	lookups := make(map[string]migration.SourceLookup, len(cfg.DirectCollections))
	filterSubjects := make([]string, 0, len(cfg.DirectCollections))
	for _, coll := range cfg.DirectCollections {
		collections[coll] = struct{}{}
		lookups[coll] = migration.NewMongoSourceLookup(sourceDB.Collection(coll, options.Collection().SetReadPreference(rp)))
		filterSubjects = append(filterSubjects, subject.MigrationOplog(cfg.SiteID, coll, "*"))
	}

	nc, err := natsutil.Connect(cfg.NatsURL, cfg.NatsCredsFile)
	if err != nil {
		slog.Error("nats connect failed", "error", err)
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}
	js, err := oteljetstream.New(nc)
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	if err := bootstrapStreams(ctx, js, cfg.SiteID, cfg.Bootstrap.Enabled); err != nil {
		slog.Error("bootstrap streams failed", "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	h := &handler{
		collections: collections,
		lookups:     lookups,
		target:      NewMongoTargetStore(targetClient.Database(cfg.TargetDB)),
		metrics:     m,
	}

	streamName := stream.MigrationOplog(cfg.SiteID).Name
	cons, err := createConsumerWithRetry(ctx, js, streamName, jetstream.ConsumerConfig{
		Durable:        cfg.ConsumerDurable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		MaxDeliver:     cfg.MaxDeliver,
		FilterSubjects: filterSubjects,
	})
	if err != nil {
		slog.Error("create consumer failed", "stream", streamName, "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	cc, err := cons.Consume(func(msg oteljetstream.Msg) {
		processOne(msg.Context(), h, msg, m, cfg.MaxDeliver)
	})
	if err != nil {
		slog.Error("consume failed", "stream", streamName, "error", err)
		_ = nc.Drain()
		mongoutil.Disconnect(ctx, targetClient)
		mongoutil.Disconnect(ctx, source)
		os.Exit(1)
	}

	slog.Info("oplog-direct-transfer started", "site", cfg.SiteID, "stream", streamName, "collections", cfg.DirectCollections)

	shutdown.Wait(ctx, 25*time.Second,
		func(context.Context) error { cc.Stop(); return nil },
		func(ctx context.Context) error { return metricsServer.Shutdown(ctx) },
		func(ctx context.Context) error { return tracerShutdown(ctx) },
		func(ctx context.Context) error { return meterShutdown(ctx) },
		func(context.Context) error { return nc.Drain() },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, targetClient); return nil },
		func(ctx context.Context) error { mongoutil.Disconnect(ctx, source); return nil },
	)
}

// processOne decodes one event and maps its handler outcome to a JetStream disposition.
func processOne(ctx context.Context, h *handler, m jetstream.Msg, mtr *metrics, maxDeliver int) {
	ctx, reqID := natsutil.StampRequestID(ctx, m.Headers(), m.Subject())
	dispose := func(action string, fn func() error) {
		if derr := fn(); derr != nil {
			slog.Error("jetstream disposition failed", "action", action, "error", derr, "request_id", reqID)
		}
	}
	var ev oplogEvent
	if err := json.Unmarshal(m.Data(), &ev); err != nil {
		slog.Error("decode oplog event — term", "error", err, "request_id", reqID)
		mtr.onTerm(ctx, "unknown", "unknown")
		dispose("term", m.Term)
		return
	}
	var numDelivered uint64
	if meta, err := m.Metadata(); err == nil {
		numDelivered = meta.NumDelivered
	}
	isFinal := migration.IsFinalDelivery(numDelivered, maxDeliver)
	err := h.handle(ctx, ev)
	switch migration.Classify(err, isFinal) {
	case migration.ActionAck:
		mtr.onProcessed(ctx, ev.Op, ev.Collection)
		dispose("ack", m.Ack)
	case migration.ActionTerm:
		slog.Error("poison event — term (skipping)", "eventId", ev.EventID, "error", err, "request_id", reqID)
		mtr.onTerm(ctx, ev.Op, ev.Collection)
		dispose("term", m.Term)
	case migration.ActionAckSkip:
		dispose("ack", m.Ack)
	case migration.ActionTermExhausted:
		slog.Error("delivery limit reached — terming (dropping)", "eventId", ev.EventID, "op", ev.Op, "cap", maxDeliver, "error", err, "request_id", reqID)
		mtr.onExhausted(ctx, ev.Op, ev.Collection)
		dispose("term", m.Term)
	default:
		slog.Error("transient failure — nak", "eventId", ev.EventID, "error", err, "request_id", reqID)
		mtr.onNak(ctx, ev.Op, ev.Collection)
		dispose("nak", func() error { return m.NakWithDelay(2 * time.Second) })
	}
}

// streamWaitTimeout bounds how long startup waits for the connector to bootstrap MIGRATION_OPLOG.
const streamWaitTimeout = 60 * time.Second

//nolint:gocritic // hugeParam: cfg passed by value to match jetstream.CreateOrUpdateConsumer's signature.
func createConsumerWithRetry(ctx context.Context, js oteljetstream.JetStream, streamName string, cfg jetstream.ConsumerConfig) (oteljetstream.Consumer, error) {
	deadline := time.Now().Add(streamWaitTimeout)
	for {
		cons, err := js.CreateOrUpdateConsumer(ctx, streamName, cfg)
		if err == nil {
			return cons, nil
		}
		if !errors.Is(err, jetstream.ErrStreamNotFound) || time.Now().After(deadline) {
			return nil, err
		}
		slog.Warn("waiting for stream to be created by the connector", "stream", streamName)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func newMetricsServer() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func readPreference(s string) (*readpref.ReadPref, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "primary":
		return readpref.Primary(), nil
	case "primarypreferred", "":
		return readpref.PrimaryPreferred(), nil
	case "secondary":
		return readpref.Secondary(), nil
	case "secondarypreferred":
		return readpref.SecondaryPreferred(), nil
	case "nearest":
		return readpref.Nearest(), nil
	default:
		return nil, fmt.Errorf("invalid SOURCE_READ_PREFERENCE: %s", s)
	}
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
```

- [ ] **Step 3: Write `processone_test.go`** (verify disposition wiring with a fake jetstream.Msg). Model it on `data-migration/oplog-collections-transformer/processone_test.go` — open that file, copy its `fakeMsg` helper and structure, and write:

```go
package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessOne_Insert_Acks(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "insert", Collection: testColl,
		DocumentKey: json.RawMessage(`{"_id":"a1"}`), FullDocument: json.RawMessage(`{"_id":"a1"}`)}
	data, _ := json.Marshal(ev)
	msg := newFakeMsg(data) // from the copied helper
	processOne(context.Background(), h, msg, nil, 1000)
	require.Len(t, tgt.upserts, 1)
	assert.Equal(t, "ack", msg.disposition())
}

func TestProcessOne_BadJSON_Terms(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	msg := newFakeMsg([]byte(`{bad`))
	processOne(context.Background(), h, msg, nil, 1000)
	assert.Equal(t, "term", msg.disposition())
}
```

> If the sibling's fake-msg helper is not directly reusable, write a minimal `oteljetstream.Msg`-compatible fake in this file that records which of Ack/Term/Nak was called and returns the string via `disposition()`. Keep `metrics` nil in these tests (nil-safe).

- [ ] **Step 4: Run to verify build + tests pass**

Run: `make build SERVICE=data-migration/oplog-direct-transfer`
Expected: builds a binary.
Run: `make test SERVICE=data-migration/oplog-direct-transfer`
Expected: PASS (config, event, metrics, handler, processone).

- [ ] **Step 5: Commit**

```bash
git add data-migration/oplog-direct-transfer/bootstrap.go data-migration/oplog-direct-transfer/main.go data-migration/oplog-direct-transfer/processone_test.go
git commit -m "feat(oplog-direct-transfer): main wiring, consumer, disposition"
```

---

### Task 7: Integration test (testcontainers)

**Files:**
- Create: `data-migration/oplog-direct-transfer/integration_test.go`
- Create: `data-migration/oplog-direct-transfer/main_test.go` (TestMain)

- [ ] **Step 1: Write `main_test.go`**

```go
//go:build integration

package main

import (
	"testing"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }
```

- [ ] **Step 2: Write `integration_test.go`** — exercise the store directly against a real Mongo from `testutil`, covering upsert (insert), upsert-again (idempotent), and delete:

```go
//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMongoTargetStore_UpsertAndDelete(t *testing.T) {
	db := testutil.MongoDB(t, "directxfer")
	store := NewMongoTargetStore(db)
	ctx := context.Background()
	const coll = "rocketchat_avatar"

	// Insert.
	require.NoError(t, store.UpsertByID(ctx, coll, "a1", bson.D{{Key: "_id", Value: "a1"}, {Key: "blob", Value: "x"}}))
	var got bson.M
	require.NoError(t, db.Collection(coll).FindOne(ctx, bson.M{"_id": "a1"}).Decode(&got))
	assert.Equal(t, "x", got["blob"])

	// Idempotent re-upsert (redelivery) with new content — one doc, replaced.
	require.NoError(t, store.UpsertByID(ctx, coll, "a1", bson.D{{Key: "_id", Value: "a1"}, {Key: "blob", Value: "y"}}))
	n, err := db.Collection(coll).CountDocuments(ctx, bson.M{"_id": "a1"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	require.NoError(t, db.Collection(coll).FindOne(ctx, bson.M{"_id": "a1"}).Decode(&got))
	assert.Equal(t, "y", got["blob"])

	// Delete.
	require.NoError(t, store.DeleteByID(ctx, coll, "a1"))
	n, err = db.Collection(coll).CountDocuments(ctx, bson.M{"_id": "a1"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// Delete of a missing row is a no-op, not an error.
	require.NoError(t, store.DeleteByID(ctx, coll, "ghost"))
}

func TestHandle_EndToEnd_ThroughStore(t *testing.T) {
	db := testutil.MongoDB(t, "directxfer-e2e")
	h := &handler{
		collections: map[string]struct{}{"rocketchat_avatar": {}},
		lookups:     map[string]migration.SourceLookup{},
		target:      NewMongoTargetStore(db),
	}
	ctx := context.Background()
	ev := oplogEvent{Op: "insert", Collection: "rocketchat_avatar",
		DocumentKey:  []byte(`{"_id":"u9"}`),
		FullDocument: []byte(`{"_id":"u9","username":"neo"}`)}
	require.NoError(t, h.handle(ctx, ev))
	var got bson.M
	require.NoError(t, db.Collection("rocketchat_avatar").FindOne(ctx, bson.M{"_id": "u9"}).Decode(&got))
	assert.Equal(t, "neo", got["username"])
}
```

> Add the `migration` import to `integration_test.go` (`github.com/hmchangw/chat/pkg/migration`).

- [ ] **Step 3: Verify it compiles (integration tag)**

Run: `go vet -tags=integration ./data-migration/oplog-direct-transfer/...`
Expected: no errors.

- [ ] **Step 4: Run the integration tests (requires Docker)**

Run: `make test-integration SERVICE=data-migration/oplog-direct-transfer`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add data-migration/oplog-direct-transfer/integration_test.go data-migration/oplog-direct-transfer/main_test.go
git commit -m "test(oplog-direct-transfer): store + end-to-end integration"
```

---

### Task 8: Deploy files + connector `WATCH_COLLECTIONS`

**Files:**
- Create: `data-migration/oplog-direct-transfer/deploy/Dockerfile`
- Create: `data-migration/oplog-direct-transfer/deploy/docker-compose.yml`
- Create: `data-migration/oplog-direct-transfer/deploy/azure-pipelines.yml`
- Modify: `data-migration/oplog-connector/deploy/docker-compose.yml` (WATCH_COLLECTIONS)

- [ ] **Step 1: Dockerfile** — copy the sibling's and swap the service name:

```dockerfile
FROM golang:1.25.11-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY pkg/ pkg/
COPY data-migration/oplog-direct-transfer/ data-migration/oplog-direct-transfer/

RUN CGO_ENABLED=0 go build -o /oplog-direct-transfer ./data-migration/oplog-direct-transfer/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app

COPY --from=builder /oplog-direct-transfer /oplog-direct-transfer

USER app
ENTRYPOINT ["/oplog-direct-transfer"]
```

- [ ] **Step 2: docker-compose.yml + azure-pipelines.yml** — copy the sibling's two files and adapt: service/image names → `oplog-direct-transfer`, build `dockerfile:` path → `data-migration/oplog-direct-transfer/deploy/Dockerfile`, env block → the direct-transfer vars (`DIRECT_COLLECTIONS`, source/target Mongo, `BOOTSTRAP_STREAMS=true` for local dev). Read `data-migration/oplog-collections-transformer/deploy/docker-compose.yml` and `.../azure-pipelines.yml` and mirror them exactly, changing only the names, dockerfile path, and env keys.

- [ ] **Step 3: Extend the connector's `WATCH_COLLECTIONS`** — the connector rejects duplicates, and `rocketchat_uploads` is already present. Add the other 7 direct-transfer collections. In `data-migration/oplog-connector/deploy/docker-compose.yml`, change the `WATCH_COLLECTIONS=` line to append:

`rocketchat_avatar,company_apps_v,company_bot_cmd_men,company_tsso_tokens,company_bot_authorization,ufsTokens,user_devices`

(i.e. every direct-transfer collection except `rocketchat_uploads`, which is already listed). Verify no duplicate remains in the final comma list.

- [ ] **Step 4: Verify compose is valid + build the image locally**

Run: `docker compose -f data-migration/oplog-direct-transfer/deploy/docker-compose.yml config -q`
Expected: no output (valid).
Run: `make build SERVICE=data-migration/oplog-direct-transfer`
Expected: builds.

- [ ] **Step 5: Commit**

```bash
git add data-migration/oplog-direct-transfer/deploy/ data-migration/oplog-connector/deploy/docker-compose.yml
git commit -m "build(oplog-direct-transfer): deploy files + connector WATCH_COLLECTIONS"
```

---

### Task 9: Docs

**Files:**
- Modify: `data-migration/CDC_COVERAGE.md`
- Modify: `data-migration/SOURCE_DATA.md`

- [ ] **Step 1: `CDC_COVERAGE.md`** — add a new section after the users rows:

```markdown
## Direct-transfer collections (oplog-direct-transfer)

Copied **verbatim** by source `_id` into the same-named new-stack collection — no mapping. Because
the destination adopts the source `_id`, **delete is actionable** (unlike the re-keyed collections above).

| Op | Handling |
|---|---|
| `insert` / `replace` | upsert the full doc verbatim by `_id` |
| `update` | re-read the full current source doc by `_id`, upsert; vanished → skip |
| `delete` | delete by `_id` (idempotent) |
| collection-level (`drop`/`rename`/`invalidate`) | ⚠️ out of scope, deferred |

Collections: `rocketchat_avatar`, `company_apps_v`, `company_bot_cmd_men`, `company_tsso_tokens`,
`rocketchat_uploads`, `company_bot_authorization`, `ufsTokens`, `user_devices`.
**Metadata only** — file/blob bytes (UFS/GridFS) are out of scope. No destination indexes or TTL
(removal is CDC-driven). Design: `docs/superpowers/specs/2026-07-01-oplog-direct-transfer-design.md`.
```

- [ ] **Step 2: `SOURCE_DATA.md`** — under the "Collection for direct transfer:" list, append:

```markdown
Handled by **`oplog-direct-transfer`**: copied verbatim (whole doc, same `_id`) into the same-named
new-stack collection, mirroring insert/update/replace/delete. Metadata only — the actual file/blob
bytes for `rocketchat_uploads`/`ufsTokens`/`rocketchat_avatar` (UFS/GridFS) are a separate owner's
concern. See `docs/superpowers/specs/2026-07-01-oplog-direct-transfer-design.md`.
```

- [ ] **Step 3: Commit**

```bash
git add data-migration/CDC_COVERAGE.md data-migration/SOURCE_DATA.md
git commit -m "docs(oplog-direct-transfer): CDC coverage + source-data notes"
```

---

### Task 10: Full verification

- [ ] **Step 1: Lint**

Run: `make lint`
Expected: `0 issues.` — fix any (common: unused imports, `gocritic hugeParam` — the `//nolint:gocritic` comments above cover the known ones).

- [ ] **Step 2: Unit tests (whole repo)**

Run: `make test`
Expected: all `ok`.

- [ ] **Step 3: SAST**

Run: `make sast-gosec`
Expected: exit 0. (Confirm no doc contents are logged — only `_id`/`collection`/`op`/`request_id`.)

- [ ] **Step 4: Integration compile**

Run: `go vet -tags=integration ./data-migration/oplog-direct-transfer/...`
Expected: no errors.

- [ ] **Step 5: Push**

```bash
git push -u origin claude/oplog-collections-directwrite
```

---

## Self-review notes (author)

- **Spec coverage:** §1 collections → config default (Task 1) + connector `WATCH_COLLECTIONS` (Task 8); §3 op handling → handler + resolveDoc (Task 4); §4 idempotency → store upsert/delete + integration idempotency test (Tasks 5/7); §5 disposition/metrics/security → processOne + metrics + logging discipline (Tasks 3/6/10); §6 config → Task 1; §7 file org → all; §8 testing → Tasks 4/7; §10 docs → Task 9; §11 no indexes/TTL → store creates none (Task 5). All covered.
- **Non-string `_id` caveat:** `SourceLookup.FindByID` takes a `string`; the initial 8 are string-keyed so `update` re-read is exact. Flagged inline in Task 4 for a `FindByRawID` follow-up if a non-string-keyed collection with updates is ever added. `insert`/`replace`/`delete` already handle any `_id` type (they don't go through the string lookup).
- **No `DeleteMaxDeliver`:** unlike the collections-transformer, delete here is a real, idempotent write (no "unrecognized foreign-origin" churn), so a single `MaxDeliver` cap is correct.
