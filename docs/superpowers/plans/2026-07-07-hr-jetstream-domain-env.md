# HR JetStream Domain From Env Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `search-sync-worker`'s HR (`spotlight-org`) consumer target a remote NATS JetStream domain read from `HR_JETSTREAM_DOMAIN`, while local collections keep the shared otel-traced context and unset behaves exactly as today.

**Architecture:** Introduce a minimal `msgFetcher`/`msgBatch` seam (new `consumer_source.go`) that both the otel-wrapped local consumer and a raw domain-scoped consumer satisfy, normalized to raw `jetstream.Msg`. `runConsumer` takes the seam instead of `oteljetstream.Consumer`. `main.go` builds a raw `jetstream.NewWithDomain` context only when the env var is set and routes the HR collection's consumer creation to it.

**Tech Stack:** Go 1.25, `github.com/nats-io/nats.go/jetstream`, `github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream`, `caarlos0/env/v11`, testify.

## Global Constraints

- Go 1.25. Single `go.mod` at repo root. Service is flat `package main` in `search-sync-worker/`.
- Use `make` targets only — never raw `go` commands. Unit tests: `make test SERVICE=search-sync-worker` (race detector on). Lint: `make lint`.
- No new third-party dependencies. All new symbols use existing imports.
- Config only via `caarlos0/env` into the typed `config` struct; `SCREAMING_SNAKE_CASE` names; provide `envDefault` for non-critical config; never `os.Getenv` in service code.
- Logging: `log/slog` JSON, structured key-value fields — never interpolated strings; never log tokens/bodies.
- TDD: write the failing test first, watch it fail, implement minimally, watch it pass, commit.
- Test files live in `package main`. Generated mocks are never hand-edited (not applicable here — no store interface change).
- No client-API doc impact (this is internal consumer wiring, not a `chat.user.` handler).
- Commit trailer on every commit:
  ```
  Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01Sgh3aGBn7fPpK3kF13xqqc
  ```
  Committer/author identity must be `Claude <noreply@anthropic.com>`.

---

### Task 1: Consumer-source seam + adapters

**Files:**
- Create: `search-sync-worker/consumer_source.go`
- Test: `search-sync-worker/consumer_source_test.go`

**Interfaces:**
- Consumes: `github.com/nats-io/nats.go/jetstream` (`Msg`, `MessageBatch`, `Consumer`, `FetchOpt`), `oteljetstream` (`Consumer`, `MessageBatch`, `Msg`).
- Produces (later tasks rely on these exact names/signatures):
  - `type msgFetcher interface { Fetch(n int, opts ...jetstream.FetchOpt) (msgBatch, error) }`
  - `type msgBatch interface { Messages() <-chan jetstream.Msg }`
  - `type rawConsumerAdapter struct{ c jetstream.Consumer }` implementing `msgFetcher`
  - `type otelConsumerAdapter struct{ c oteljetstream.Consumer }` implementing `msgFetcher`

- [ ] **Step 1: Write the failing tests**

Create `search-sync-worker/consumer_source_test.go`:

```go
package main

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"
)

// fakeMsg is a sentinel jetstream.Msg used only for identity comparison. It
// embeds the interface (nil) so it satisfies jetstream.Msg without implementing
// every method; the tests never call those methods.
type fakeMsg struct {
	jetstream.Msg
	id string
}

// fakeOtelBatch is a stand-in oteljetstream.MessageBatch yielding a fixed slice.
type fakeOtelBatch struct {
	msgs []oteljetstream.Msg
}

func (f fakeOtelBatch) Messages() <-chan oteljetstream.Msg {
	ch := make(chan oteljetstream.Msg, len(f.msgs))
	for _, m := range f.msgs {
		ch <- m
	}
	close(ch)
	return ch
}

func (f fakeOtelBatch) Error() error { return nil }

// fakeOtelConsumer embeds oteljetstream.Consumer (nil) and overrides Fetch.
type fakeOtelConsumer struct {
	oteljetstream.Consumer
	batch oteljetstream.MessageBatch
	err   error
}

func (f fakeOtelConsumer) Fetch(int, ...jetstream.FetchOpt) (oteljetstream.MessageBatch, error) {
	return f.batch, f.err
}

// fakeRawBatch is a stand-in jetstream.MessageBatch yielding a fixed slice.
type fakeRawBatch struct {
	msgs []jetstream.Msg
}

func (f fakeRawBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg, len(f.msgs))
	for _, m := range f.msgs {
		ch <- m
	}
	close(ch)
	return ch
}

func (f fakeRawBatch) Error() error { return nil }

// fakeRawConsumer embeds jetstream.Consumer (nil) and overrides Fetch.
type fakeRawConsumer struct {
	jetstream.Consumer
	batch jetstream.MessageBatch
	err   error
}

func (f fakeRawConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return f.batch, f.err
}

func TestOtelConsumerAdapter_Fetch_UnwrapsRawMsgInOrder(t *testing.T) {
	m1 := &fakeMsg{id: "1"}
	m2 := &fakeMsg{id: "2"}
	m3 := &fakeMsg{id: "3"}
	adapter := otelConsumerAdapter{c: fakeOtelConsumer{batch: fakeOtelBatch{msgs: []oteljetstream.Msg{
		{Msg: m1}, {Msg: m2}, {Msg: m3},
	}}}}

	batch, err := adapter.Fetch(10)
	require.NoError(t, err)

	var got []jetstream.Msg
	for m := range batch.Messages() {
		got = append(got, m)
	}
	require.Equal(t, []jetstream.Msg{m1, m2, m3}, got)
}

func TestOtelConsumerAdapter_Fetch_ReturnsError(t *testing.T) {
	wantErr := errors.New("fetch failed")
	adapter := otelConsumerAdapter{c: fakeOtelConsumer{err: wantErr}}

	_, err := adapter.Fetch(10)
	require.ErrorIs(t, err, wantErr)
}

func TestRawConsumerAdapter_Fetch_PassesThroughRawMsg(t *testing.T) {
	m1 := &fakeMsg{id: "1"}
	m2 := &fakeMsg{id: "2"}
	adapter := rawConsumerAdapter{c: fakeRawConsumer{batch: fakeRawBatch{msgs: []jetstream.Msg{m1, m2}}}}

	batch, err := adapter.Fetch(10)
	require.NoError(t, err)

	var got []jetstream.Msg
	for m := range batch.Messages() {
		got = append(got, m)
	}
	require.Equal(t, []jetstream.Msg{m1, m2}, got)
}

func TestRawConsumerAdapter_Fetch_ReturnsError(t *testing.T) {
	wantErr := errors.New("fetch failed")
	adapter := rawConsumerAdapter{c: fakeRawConsumer{err: wantErr}}

	_, err := adapter.Fetch(10)
	require.ErrorIs(t, err, wantErr)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test SERVICE=search-sync-worker`
Expected: build failure — `undefined: otelConsumerAdapter`, `undefined: rawConsumerAdapter` (the implementation file does not exist yet).

- [ ] **Step 3: Write the implementation**

Create `search-sync-worker/consumer_source.go`:

```go
package main

import (
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"
)

// msgFetcher is the subset of a JetStream pull consumer that runConsumer needs,
// normalized so one loop drives both the otel-wrapped local consumers and the
// raw domain-scoped HR consumer. Both adapters yield raw jetstream.Msg, which is
// exactly what handler.Add consumes.
type msgFetcher interface {
	Fetch(n int, opts ...jetstream.FetchOpt) (msgBatch, error)
}

// msgBatch yields already-unwrapped raw jetstream.Msg values for one Fetch.
type msgBatch interface {
	Messages() <-chan jetstream.Msg
}

// rawConsumerAdapter wraps a raw (domain-scoped) jetstream.Consumer. A
// jetstream.MessageBatch already yields raw jetstream.Msg, so it satisfies
// msgBatch directly and the batch passes through unchanged.
type rawConsumerAdapter struct{ c jetstream.Consumer }

func (a rawConsumerAdapter) Fetch(n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	b, err := a.c.Fetch(n, opts...)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// otelConsumerAdapter wraps an oteljetstream.Consumer. Its batch yields
// oteljetstream.Msg (which embeds jetstream.Msg plus a trace context); otelBatch
// unwraps each to the embedded raw interface.
type otelConsumerAdapter struct{ c oteljetstream.Consumer }

func (a otelConsumerAdapter) Fetch(n int, opts ...jetstream.FetchOpt) (msgBatch, error) {
	b, err := a.c.Fetch(n, opts...)
	if err != nil {
		return nil, err
	}
	return otelBatch{b}, nil
}

// otelBatch re-channels an oteljetstream.MessageBatch as raw jetstream.Msg. The
// goroutine is leak-safe: runConsumer always drains Messages() to completion, so
// when the source channel closes the goroutine closes out and exits.
type otelBatch struct{ b oteljetstream.MessageBatch }

func (o otelBatch) Messages() <-chan jetstream.Msg {
	out := make(chan jetstream.Msg)
	go func() {
		defer close(out)
		for m := range o.b.Messages() {
			out <- m.Msg
		}
	}()
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=search-sync-worker`
Expected: PASS — all four new tests green; existing service tests still pass.

- [ ] **Step 5: Commit**

```bash
git add search-sync-worker/consumer_source.go search-sync-worker/consumer_source_test.go
git commit -m "feat(search-sync-worker): add msgFetcher seam and consumer adapters

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Sgh3aGBn7fPpK3kF13xqqc"
```

---

### Task 2: Route runConsumer through the seam

**Files:**
- Modify: `search-sync-worker/main.go` (`runConsumer` signature ~L348-355; loop body ~L406-407; call site ~L275)

**Interfaces:**
- Consumes: `msgFetcher`, `otelConsumerAdapter` from Task 1.
- Produces: `runConsumer(ctx, cons msgFetcher, handler *Handler, fetchBatchSize, bulkBatchSize int, bulkFlushInterval time.Duration, stopCh <-chan struct{}, doneCh chan<- struct{})` — later task passes an `msgFetcher` (either adapter) here.

This is a behavior-preserving refactor: all collections still flow through the otel-wrapped consumer, now via `otelConsumerAdapter`. Verified by build + existing tests.

- [ ] **Step 1: Change `runConsumer`'s consumer parameter type**

In `search-sync-worker/main.go`, in the `runConsumer` signature, replace the parameter:

```go
	cons oteljetstream.Consumer,
```

with:

```go
	cons msgFetcher,
```

- [ ] **Step 2: Unwrap in the message loop**

In `runConsumer`, the inner loop currently reads:

```go
		for msg := range batch.Messages() {
			add(msg.Msg)
```

Change `add(msg.Msg)` to `add(msg)` (the seam already yields raw `jetstream.Msg`):

```go
		for msg := range batch.Messages() {
			add(msg)
```

- [ ] **Step 3: Wrap the consumer at the call site**

In `main()`, the consumer is launched with:

```go
		go runConsumer(ctx, cons, handler, cfg.FetchBatchSize, cfg.BulkBatchSize, bulkFlushInterval, stopCh, doneCh)
```

Change `cons` to `otelConsumerAdapter{cons}`:

```go
		go runConsumer(ctx, otelConsumerAdapter{cons}, handler, cfg.FetchBatchSize, cfg.BulkBatchSize, bulkFlushInterval, stopCh, doneCh)
```

- [ ] **Step 4: Format, build, and run the service tests**

Run: `make fmt`
Expected: no diff of consequence (formats the edited `main.go`).

Run: `make build SERVICE=search-sync-worker`
Expected: builds cleanly.

Run: `make test SERVICE=search-sync-worker`
Expected: PASS — the refactor changes no behavior; all existing and Task-1 tests stay green.

- [ ] **Step 5: Commit**

```bash
git add search-sync-worker/main.go
git commit -m "refactor(search-sync-worker): drive runConsumer through msgFetcher seam

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Sgh3aGBn7fPpK3kF13xqqc"
```

---

### Task 3: HR JetStream domain config, context, and routing

**Files:**
- Modify: `search-sync-worker/main.go` (config struct ~L57; `hrJS` construction after `js` ~L221; collection loop consumer creation ~L254-275)
- Test: `search-sync-worker/config_test.go` (create)
- Modify: `search-sync-worker/deploy/docker-compose.yml` (after `HR_CENTRAL_SITE_ID` ~L22)

**Interfaces:**
- Consumes: `msgFetcher`, `rawConsumerAdapter`, `otelConsumerAdapter` (Task 1); `runConsumer(..., cons msgFetcher, ...)` (Task 2); existing `hrName := stream.OrgSyncStream(cfg.HRCentralSiteID).Name` and `nc.NatsConn()`.
- Produces: `config.HRJetStreamDomain string` (env `HR_JETSTREAM_DOMAIN`, default `""`).

- [ ] **Step 1: Write the failing config-parse test**

Create `search-sync-worker/config_test.go`:

```go
package main

import (
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredConfigEnv sets every `required` env var so env.ParseAs[config]
// succeeds; individual tests then vary only the field under test.
func setRequiredConfigEnv(t *testing.T) {
	t.Helper()
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("SITE_ID", "site-test")
	t.Setenv("SEARCH_URL", "http://localhost:9200")
	t.Setenv("MSG_INDEX_PREFIX", "messages-site-test-v1")
	t.Setenv("SPOTLIGHT_INDEX", "spotlight-site-test-v1")
	t.Setenv("SPOTLIGHT_ORG_INDEX", "spotlightorg-site-test-v1")
	t.Setenv("HR_CENTRAL_SITE_ID", "site-central")
	t.Setenv("USER_ROOM_INDEX", "user-room-mv-site-test")
}

func TestConfig_HRJetStreamDomain(t *testing.T) {
	t.Run("defaults to empty when unset", func(t *testing.T) {
		setRequiredConfigEnv(t)

		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, "", cfg.HRJetStreamDomain)
	})

	t.Run("reads HR_JETSTREAM_DOMAIN when set", func(t *testing.T) {
		setRequiredConfigEnv(t)
		t.Setenv("HR_JETSTREAM_DOMAIN", "hr-hub")

		cfg, err := env.ParseAs[config]()
		require.NoError(t, err)
		assert.Equal(t, "hr-hub", cfg.HRJetStreamDomain)
	})
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `make test SERVICE=search-sync-worker`
Expected: build failure — `cfg.HRJetStreamDomain undefined (type config has no field or method HRJetStreamDomain)`.

- [ ] **Step 3: Add the config field**

In `search-sync-worker/main.go`, in the `config` struct, immediately after:

```go
	HRCentralSiteID     string `env:"HR_CENTRAL_SITE_ID,required"`
```

add:

```go
	// HRJetStreamDomain, when set, is the JetStream domain of the remote NATS
	// cluster that owns OrgSyncStream (hr-syncer's HR stream). The spotlight-org
	// consumer is created against a domain-scoped JetStream context so a worker
	// at one site can consume the HR stream in another site's domain. Empty
	// (default) means the HR stream is in this worker's local domain and the
	// shared, otel-traced JetStream context is used.
	HRJetStreamDomain   string `env:"HR_JETSTREAM_DOMAIN" envDefault:""`
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `make test SERVICE=search-sync-worker`
Expected: PASS — both `TestConfig_HRJetStreamDomain` subtests green.

- [ ] **Step 5: Build the domain-scoped context**

In `main()`, the JetStream context is created with:

```go
	js, err := oteljetstream.New(nc)
	if err != nil {
		slog.Error("jetstream init failed", "error", err)
		os.Exit(1)
	}
```

Immediately after that block, add:

```go
	// When HR_JETSTREAM_DOMAIN is set, the HR stream (OrgSyncStream) lives in a
	// remote NATS domain. Build a raw domain-scoped JetStream context for the
	// spotlight-org consumer: oteljetstream has no domain variant, and this
	// worker already discards the per-message otel trace context on the consume
	// path, so the raw context loses nothing in use. NewWithDomain only sets the
	// API prefix (no I/O), so an error here is a config error, not a
	// reachability failure. Empty domain keeps the shared js for the HR consumer.
	var hrJS jetstream.JetStream
	if cfg.HRJetStreamDomain != "" {
		hrJS, err = jetstream.NewWithDomain(nc.NatsConn(), cfg.HRJetStreamDomain)
		if err != nil {
			slog.Error("jetstream HR-domain init failed",
				"domain", cfg.HRJetStreamDomain, "error", err)
			os.Exit(1)
		}
	}
```

(`github.com/nats-io/nats.go/jetstream` is already imported in `main.go`.)

- [ ] **Step 6: Route the HR consumer to the domain-scoped context**

In the `for _, coll := range collections {` loop, replace this block:

```go
		consumerCfg := buildConsumerConfig(cfg.Consumer, coll, cfg.SiteID)
		cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
		if err != nil {
			slog.Error("create consumer failed",
				"stream", streamCfg.Name,
				"consumer", coll.ConsumerName(),
				"error", err,
			)
			os.Exit(1)
		}

		handler := NewHandler(&engineAdapter{engine: engine}, coll, cfg.BulkBatchSize)
		doneCh := make(chan struct{})
		doneChs = append(doneChs, doneCh)

		slog.Info("collection wired",
			"stream", streamCfg.Name,
			"consumer", coll.ConsumerName(),
			"filters", consumerCfg.FilterSubjects,
		)

		go runConsumer(ctx, otelConsumerAdapter{cons}, handler, cfg.FetchBatchSize, cfg.BulkBatchSize, bulkFlushInterval, stopCh, doneCh)
```

with:

```go
		consumerCfg := buildConsumerConfig(cfg.Consumer, coll, cfg.SiteID)

		// The HR (spotlight-org) collection reads OrgSyncStream. When a remote HR
		// domain is configured, create its consumer against the domain-scoped
		// context; every other collection uses the shared otel-traced js.
		var fetcher msgFetcher
		if streamCfg.Name == hrName && hrJS != nil {
			cons, err := hrJS.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
			if err != nil {
				slog.Error("create consumer failed",
					"stream", streamCfg.Name,
					"consumer", coll.ConsumerName(),
					"domain", cfg.HRJetStreamDomain,
					"error", err,
				)
				os.Exit(1)
			}
			fetcher = rawConsumerAdapter{cons}
			slog.Info("HR consumer bound to remote JetStream domain",
				"domain", cfg.HRJetStreamDomain,
				"stream", streamCfg.Name,
				"consumer", coll.ConsumerName(),
			)
		} else {
			cons, err := js.CreateOrUpdateConsumer(ctx, streamCfg.Name, consumerCfg)
			if err != nil {
				slog.Error("create consumer failed",
					"stream", streamCfg.Name,
					"consumer", coll.ConsumerName(),
					"error", err,
				)
				os.Exit(1)
			}
			fetcher = otelConsumerAdapter{cons}
		}

		handler := NewHandler(&engineAdapter{engine: engine}, coll, cfg.BulkBatchSize)
		doneCh := make(chan struct{})
		doneChs = append(doneChs, doneCh)

		slog.Info("collection wired",
			"stream", streamCfg.Name,
			"consumer", coll.ConsumerName(),
			"filters", consumerCfg.FilterSubjects,
		)

		go runConsumer(ctx, fetcher, handler, cfg.FetchBatchSize, cfg.BulkBatchSize, bulkFlushInterval, stopCh, doneCh)
```

- [ ] **Step 7: Document the env var in docker-compose**

In `search-sync-worker/deploy/docker-compose.yml`, after:

```yaml
      - HR_CENTRAL_SITE_ID=site-local
```

add:

```yaml
      # Optional JetStream domain of the remote NATS cluster that owns the HR
      # OrgSyncStream. Set when hr-syncer runs in a different domain so this
      # worker's spotlight-org consumer targets that domain. Leave unset for
      # single-cluster local dev (the HR stream is in this cluster's domain).
      # - HR_JETSTREAM_DOMAIN=hr-hub
```

- [ ] **Step 8: Format, build, test, and lint**

Run: `make fmt`
Expected: realigns the `config` struct tags after the new field; no other changes.

Run: `make build SERVICE=search-sync-worker`
Expected: builds cleanly.

Run: `make test SERVICE=search-sync-worker`
Expected: PASS — config tests, Task-1 adapter tests, and all pre-existing service tests green. (Domain routing itself is validated in a real multi-cluster env; a single-node NATS testcontainer cannot model domains, and the empty-domain path is unchanged.)

Run: `make lint`
Expected: no findings in `search-sync-worker`.

- [ ] **Step 9: Commit**

```bash
git add search-sync-worker/main.go search-sync-worker/config_test.go search-sync-worker/deploy/docker-compose.yml
git commit -m "feat(search-sync-worker): read HR JetStream domain from HR_JETSTREAM_DOMAIN

Route the spotlight-org (OrgSyncStream) consumer to a domain-scoped JetStream
context when HR_JETSTREAM_DOMAIN is set, so a worker at one site can consume the
HR stream in a remote NATS domain. Empty domain keeps the shared otel-traced
context, matching current behavior.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Sgh3aGBn7fPpK3kF13xqqc"
```

---

## Verification checklist (whole feature)

- [ ] `HR_JETSTREAM_DOMAIN` unset → HR consumer uses shared `js` (via `otelConsumerAdapter`); no behavior change.
- [ ] `HR_JETSTREAM_DOMAIN` set → `hrJS` built with `NewWithDomain`; HR consumer created against it (via `rawConsumerAdapter`); one `slog.Info` "HR consumer bound to remote JetStream domain".
- [ ] Local collections (messages / spotlight / user-room) always use `js`.
- [ ] HR stream is never bootstrapped by this worker (existing `hrName` skip unchanged).
- [ ] `make test SERVICE=search-sync-worker` and `make lint` green.
- [ ] Design doc's "Files touched" table all accounted for. No client-API doc change (not a `chat.user.` handler).
