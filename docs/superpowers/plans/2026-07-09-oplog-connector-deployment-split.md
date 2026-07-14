# oplog-connector Deployment Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the same `oplog-connector` binary run as two independent deployments — a message-role pod and a collections-role pod with disjoint `WATCH_COLLECTIONS` — so a fault on the collection side can't stall message CDC.

**Architecture:** Config-only split per the approved spec (`docs/superpowers/specs/2026-07-09-oplog-connector-deployment-split-design.md`). One production-code change: relax the `MESSAGE_COLLECTION ∈ WATCH_COLLECTIONS` hard guard in `parseConfig` (keep the non-empty check), add a `watchesMessages()` helper, and make the startup federation-filter log truthful. Plus: an integration test for the collections role, a two-service local docker-compose, and doc updates.

**Tech Stack:** Go 1.25, `caarlos0/env`, testify, testcontainers (integration), NATS JetStream, MongoDB change streams.

## Global Constraints

- Worktree: `/home/user/chat-oplog-connector`, branch `claude/dreamy-clarke-prpl3` (off `origin/main`). All commands below run from that directory.
- Always use `make` targets, never raw `go` commands: `make fmt`, `make lint`, `make test SERVICE=data-migration/oplog-connector`, `make test-integration SERVICE=data-migration/oplog-connector`.
- TDD (CLAUDE.md §4): write the failing test first, watch it fail, then implement.
- Coverage floor 80% for the package (CI enforces via `azure-pipelines.yml`).
- Never log tokens/secrets; no behavior change to envelope, subjects, or checkpoints.
- Commit as `user.email=noreply@anthropic.com`, `user.name=Claude` (already set repo-wide). End every commit message with:
  `Co-Authored-By: Claude <noreply@anthropic.com>`
- Docker may be unavailable in the sandbox; if `make test-integration` cannot run locally, state so and rely on CI (the pipeline runs `-tags=integration`).

---

### Task 1: Relax the message-collection guard + `watchesMessages()` (config.go)

**Files:**
- Modify: `data-migration/oplog-connector/config.go` (guard at ~lines 86-95)
- Test: `data-migration/oplog-connector/config_test.go` (replace test at lines 61-70, add one)

**Interfaces:**
- Consumes: existing `config` struct, `parseConfig() (config, error)`.
- Produces: `func (c *config) watchesMessages() bool` — Task 2 calls this from `main.go`. `parseConfig` now ACCEPTS a config whose `MessageCollection` is not in `WatchCollections` (still rejects empty `MessageCollection`).

- [ ] **Step 1: Rewrite the failing tests**

In `data-migration/oplog-connector/config_test.go`, **replace** the existing `TestParseConfig_MessageCollectionNotWatchedFails` (lines 61-70) with:

```go
func TestParseConfig_MessageCollectionNotWatchedPasses(t *testing.T) {
	setRequiredEnv(t)
	// Collections-role deployment: the message collection is tailed by a separate
	// deployment (spec 2026-07-09-oplog-connector-deployment-split), so its absence
	// here is legitimate — no longer a fail-fast.
	t.Setenv("WATCH_COLLECTIONS", "rocketchat_room,users")
	t.Setenv("MESSAGE_COLLECTION", "rocketchat_message")
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.False(t, cfg.watchesMessages())
}

func TestParseConfig_WatchesMessagesWhenPresent(t *testing.T) {
	setRequiredEnv(t) // WATCH_COLLECTIONS includes rocketchat_message
	cfg, err := parseConfig()
	require.NoError(t, err)
	assert.True(t, cfg.watchesMessages())
}
```

Leave `TestParseConfig_EmptyMessageCollectionFails` (lines 72-79) untouched — the non-empty check stays.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/user/chat-oplog-connector && make test SERVICE=data-migration/oplog-connector`
Expected: FAIL — `TestParseConfig_MessageCollectionNotWatchedPasses` errors (parseConfig still rejects), and both new tests fail to compile until `watchesMessages` exists (`cfg.watchesMessages undefined`). A compile error IS the red state here.

- [ ] **Step 3: Implement in config.go**

In `data-migration/oplog-connector/config.go`, replace this block (currently ~lines 86-95):

```go
	// Fail-open guard: if MESSAGE_COLLECTION is empty or isn't actually watched, the
	// federation-origin $match never runs and ALL foreign messages migrate silently
	// (double-deliver). Fail fast instead.
	cfg.MessageCollection = strings.TrimSpace(cfg.MessageCollection)
	if cfg.MessageCollection == "" {
		return config{}, fmt.Errorf("MESSAGE_COLLECTION must be non-empty (the federation-origin $match would never run)")
	}
	if !slices.Contains(cfg.WatchCollections, cfg.MessageCollection) {
		return config{}, fmt.Errorf("MESSAGE_COLLECTION %q is not present in WATCH_COLLECTIONS — the federation-origin $match would never run", cfg.MessageCollection)
	}
```

with:

```go
	// MESSAGE_COLLECTION is the identity of the federated collection and must always be
	// defined. It need NOT be watched: a collections-role deployment legitimately runs
	// without it (deployment split) — "watched ⟹ filtered" holds by construction because
	// the $match is applied to whichever watcher's name equals MESSAGE_COLLECTION.
	cfg.MessageCollection = strings.TrimSpace(cfg.MessageCollection)
	if cfg.MessageCollection == "" {
		return config{}, fmt.Errorf("MESSAGE_COLLECTION must be non-empty (the federation-origin $match would never run)")
	}
```

Then add this method after `parseConfig` (before `firstDuplicate`):

```go
// watchesMessages reports whether this deployment watches the federated message
// collection (message role); drives the truthful federation-filter startup log.
func (c *config) watchesMessages() bool {
	return slices.Contains(c.WatchCollections, c.MessageCollection)
}
```

The `slices` import stays (now used by `watchesMessages`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test SERVICE=data-migration/oplog-connector`
Expected: PASS (all tests, including the untouched empty-MESSAGE_COLLECTION rejection).

- [ ] **Step 5: Lint and commit**

```bash
cd /home/user/chat-oplog-connector
make fmt && make lint
git add data-migration/oplog-connector/config.go data-migration/oplog-connector/config_test.go
git commit -m "feat(oplog-connector): allow deployments that don't watch the message collection

Collections-role deployments (deployment split) legitimately omit
rocketchat_message from WATCH_COLLECTIONS. Keep the MESSAGE_COLLECTION
non-empty check; 'watched implies filtered' holds by construction.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: Truthful federation-filter startup log (main.go)

**Files:**
- Modify: `data-migration/oplog-connector/main.go:77-78`

**Interfaces:**
- Consumes: `cfg.watchesMessages()` from Task 1.
- Produces: role-truthful startup logs (no API surface).

- [ ] **Step 1: Replace the unconditional log**

In `data-migration/oplog-connector/main.go`, replace (lines 77-78):

```go
	slog.Info("oplog-connector started", "site", cfg.SiteID, "collections", cfg.WatchCollections)
	slog.Info("federation-origin filter active", "message_collection", cfg.MessageCollection)
```

with:

```go
	slog.Info("oplog-connector started", "site", cfg.SiteID, "collections", cfg.WatchCollections)
	if cfg.watchesMessages() {
		slog.Info("federation-origin filter active", "message_collection", cfg.MessageCollection)
	} else {
		slog.Info("no message collection watched — federation-origin filter inactive (collections role)",
			"message_collection", cfg.MessageCollection)
	}
```

This is log wiring over Task 1's tested predicate — no new unit test; the existing suite plus build must stay green.

- [ ] **Step 2: Verify build, tests, lint**

Run: `make test SERVICE=data-migration/oplog-connector && make lint`
Expected: PASS / 0 issues.

- [ ] **Step 3: Commit**

```bash
git add data-migration/oplog-connector/main.go
git commit -m "feat(oplog-connector): log federation-filter state truthfully per role

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: Integration test — collections-role deployment publishes only its subjects

**Files:**
- Modify: `data-migration/oplog-connector/integration_test.go` (append test; add `"slices"` to imports)

**Interfaces:**
- Consumes: existing helpers `startReplicaSet(t)`, `createSourceCollection(t, db, coll)`, `start(ctx, &cfg, nil)`, `testutil.NATS(t)` — same pattern as `TestConnector_RealPublishEndToEnd` (integration_test.go:68).
- Produces: regression coverage for the disjoint-set invariant (spec §4).

- [ ] **Step 1: Write the test (failing-by-construction check below)**

Append to `data-migration/oplog-connector/integration_test.go` (it already has `//go:build integration`):

```go
// TestConnector_CollectionsRole_DisjointSet starts a collections-role connector — the
// federated message collection is configured but NOT watched (deployment split) — and
// asserts it tails only its own collections and never publishes message subjects.
func TestConnector_CollectionsRole_DisjointSet(t *testing.T) {
	client, uri := startReplicaSet(t)
	rooms := createSourceCollection(t, client.Database("rocketchat"), "rocketchat_room")
	msgs := createSourceCollection(t, client.Database("rocketchat"), "rocketchat_message")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := config{
		SiteID:            "sitecr",
		SourceMongoURI:    uri,
		SourceDB:          "rocketchat",
		CheckpointDB:      "migration",
		NatsURL:           testutil.NATS(t),
		WatchCollections:  []string{"rocketchat_room"},
		MessageCollection: "rocketchat_message", // not watched — collections role
		ReadPreference:    "primaryPreferred",
		CheckpointEvery:   1,
		StartMode:         "now",
		Bootstrap:         bootstrapConfig{Enabled: true},
	}
	conn, err := start(ctx, &cfg, nil)
	require.NoError(t, err)
	defer conn.Close()

	_, err = rooms.InsertOne(ctx, bson.M{"_id": "r1", "name": "general"})
	require.NoError(t, err)
	_, err = msgs.InsertOne(ctx, bson.M{"_id": "m1", "msg": "hi"}) // no watcher — must not be forwarded
	require.NoError(t, err)

	nc, err := natsutil.Connect(cfg.NatsURL, "")
	require.NoError(t, err)
	defer func() { assert.NoError(t, nc.Drain()) }()
	js, err := oteljetstream.New(nc)
	require.NoError(t, err)

	var subjects []string
	require.Eventually(t, func() bool {
		cons, cerr := js.CreateOrUpdateConsumer(ctx, "MIGRATION_OPLOG_sitecr", jetstream.ConsumerConfig{
			AckPolicy:      jetstream.AckExplicitPolicy,
			FilterSubjects: []string{"chat.migration.oplog.sitecr.>"},
		})
		if cerr != nil {
			return false
		}
		batch, berr := cons.Fetch(10, jetstream.FetchMaxWait(500*time.Millisecond))
		if berr != nil {
			return false
		}
		for m := range batch.Messages() {
			assert.NoError(t, m.Ack())
			subjects = append(subjects, m.Subject())
		}
		return slices.Contains(subjects, "chat.migration.oplog.sitecr.rocketchat_room.insert")
	}, 40*time.Second, 500*time.Millisecond, "room insert must land on MIGRATION_OPLOG")

	for _, s := range subjects {
		assert.NotContains(t, s, "rocketchat_message",
			"collections-role deployment must never publish message subjects")
	}
}
```

Add `"slices"` to the standard-library import block of `integration_test.go`.

- [ ] **Step 2: Red-state check**

The meaningful red state for this test is against **Task 1 unapplied** (old guard) the config would have failed `parseConfig` — but this test builds `config{}` directly, so its red/green value is behavioral: it fails if `start()` refuses a message-less set or if message subjects appear. Verify it at least compiles under the integration tag:

Run: `cd /home/user/chat-oplog-connector && GOFLAGS=-tags=integration go vet ./data-migration/oplog-connector/` (vet is the one raw-go exception the Makefile has no target for; alternatively `make lint` covers vet without tags — run both)
Expected: no errors.

- [ ] **Step 3: Run the integration suite (if Docker available)**

Run: `make test-integration SERVICE=data-migration/oplog-connector`
Expected: PASS. If Docker is unavailable in the sandbox, note it and rely on CI (`azure-pipelines.yml` runs `-tags=integration`).

- [ ] **Step 4: Commit**

```bash
git add data-migration/oplog-connector/integration_test.go
git commit -m "test(oplog-connector): collections-role deployment tails only its collections

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: Split local docker-compose into the two roles

**Files:**
- Modify: `data-migration/oplog-connector/deploy/docker-compose.yml` (replace the single `oplog-connector` service, lines 30-50)

**Interfaces:**
- Consumes: nothing from other tasks (pure config; validates Task 1 end-to-end in dev).
- Produces: the reference two-role topology ops mirrors in prod.

- [ ] **Step 1: Replace the connector service**

Replace the `oplog-connector:` service block with (keep `source-mongo`, `nats`, and `networks` unchanged):

```yaml
  # Deployment split (spec 2026-07-09): the message pump runs alone so a fault on the
  # collection side can't stall message CDC. Watch sets MUST stay disjoint, and exactly
  # one service includes rocketchat_message. No host ports are published, so both keep
  # the default METRICS_ADDR :9090 inside their own container.
  oplog-connector-messages:
    build:
      context: ../..
      dockerfile: data-migration/oplog-connector/deploy/Dockerfile
    depends_on:
      source-mongo:
        condition: service_healthy
      nats:
        condition: service_started
    environment:
      - SITE_ID=site-local
      - SOURCE_MONGO_URI=mongodb://source-mongo:27017/?replicaSet=rs0
      - SOURCE_DB=rocketchat
      - CHECKPOINT_DB=migration
      - NATS_URL=nats://nats:4222
      - WATCH_COLLECTIONS=rocketchat_message
      - READ_PREFERENCE=primaryPreferred
      - BOOTSTRAP_STREAMS=true
      - LOG_LEVEL=info
    networks:
      - oplog-local

  oplog-connector-collections:
    build:
      context: ../..
      dockerfile: data-migration/oplog-connector/deploy/Dockerfile
    depends_on:
      source-mongo:
        condition: service_healthy
      nats:
        condition: service_started
    environment:
      - SITE_ID=site-local
      - SOURCE_MONGO_URI=mongodb://source-mongo:27017/?replicaSet=rs0
      - SOURCE_DB=rocketchat
      - CHECKPOINT_DB=migration
      - NATS_URL=nats://nats:4222
      - WATCH_COLLECTIONS=rocketchat_room,rocketchat_subscription,rocketchat_uploads,company_room_members,company_thread_subscriptions,company_hr_acct_org,users,rocketchat_avatar,company_apps_v,company_bot_cmd_men,company_tsso_tokens,company_bot_authorization,ufsTokens,user_devices
      - READ_PREFERENCE=primaryPreferred
      - BOOTSTRAP_STREAMS=true
      - LOG_LEVEL=info
    networks:
      - oplog-local
```

(The collections list = the current combined 15 minus `rocketchat_message`. Both keep `BOOTSTRAP_STREAMS=true` — `CreateOrUpdateStream` is idempotent, either may start first.)

- [ ] **Step 2: Validate the YAML**

Run: `docker compose -f data-migration/oplog-connector/deploy/docker-compose.yml config >/dev/null && echo OK` (if Docker CLI is unavailable, validate with `python3 -c "import yaml,sys; yaml.safe_load(open('data-migration/oplog-connector/deploy/docker-compose.yml')); print('OK')"`)
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add data-migration/oplog-connector/deploy/docker-compose.yml
git commit -m "deploy(oplog-connector): split local compose into message and collections roles

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: Docs — parent spec addendum + CDC_COVERAGE note

**Files:**
- Modify: `docs/superpowers/specs/2026-06-08-oplog-connector-design.md` (append to §7 "HA, error handling & lifecycle")
- Modify: `data-migration/CDC_COVERAGE.md` (note near the connector intro, ~line 11)

**Interfaces:** none (docs only).

- [ ] **Step 1: Append the split to the parent spec's §7**

At the end of section `## 7. HA, error handling & lifecycle` in `docs/superpowers/specs/2026-06-08-oplog-connector-design.md`, append:

```markdown
### 7.1 Deployment topology — message vs collection split (2026-07-09)

The connector runs as **two deployments of the same binary** with disjoint `WATCH_COLLECTIONS`
(see `2026-07-09-oplog-connector-deployment-split-design.md`):

- `oplog-connector-messages` — watches only `rocketchat_message`; federation-origin `$match` active.
- `oplog-connector-collections` — watches everything else; filter inactive (nothing to filter).

Rationale: a fatal watcher error tears down its whole process, so message CDC must not share a
failure domain with low-value operational collections. Checkpoints are per-collection and the
stream's subjects are per-collection, so the split needs no data or contract change.

**Cross-deployment invariant (ops/IaC-owned, not code-enforced):** the two watch sets are
disjoint, and exactly one deployment includes the message collection. `MESSAGE_COLLECTION` must
be non-empty everywhere but need not be watched; each pod's startup log states its role
("federation-origin filter active" vs "… filter inactive (collections role)").
```

- [ ] **Step 2: Add the CDC_COVERAGE note**

In `data-migration/CDC_COVERAGE.md`, directly after the intro sentence about the connector (~line 11), add:

```markdown
> **Deployment note:** the connector runs as two deployments — `oplog-connector-messages`
> (only `rocketchat_message`) and `oplog-connector-collections` (all other watched
> collections) — with disjoint `WATCH_COLLECTIONS`, so a collection-side fault cannot stall
> message CDC. Coverage below is unchanged by the split.
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-06-08-oplog-connector-design.md data-migration/CDC_COVERAGE.md
git commit -m "docs: record oplog-connector message/collections deployment split

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 6: Full verification + push

**Files:** none new.

**Interfaces:** none.

- [ ] **Step 1: Full local gate**

```bash
cd /home/user/chat-oplog-connector
make fmt && make lint && make test SERVICE=data-migration/oplog-connector
```
Expected: 0 issues, all tests PASS. Run `make test-integration SERVICE=data-migration/oplog-connector` too if Docker is available; otherwise note reliance on CI.

- [ ] **Step 2: SAST (best effort in sandbox)**

Run: `make sast` — gosec must pass; govulncheck/semgrep may be proxy-blocked (note for CI if so).

- [ ] **Step 3: Coverage spot-check**

```bash
go test ./data-migration/oplog-connector/... -coverprofile=/tmp/claude-0/-home-user-chat/29850d10-3f3c-5dec-9b92-aae126045dba/scratchpad/conn-cover.out >/dev/null 2>&1 || true
go tool cover -func=/tmp/claude-0/-home-user-chat/29850d10-3f3c-5dec-9b92-aae126045dba/scratchpad/conn-cover.out | tail -1
```
Expected: unit-only number is informational (CI's floor includes `-tags=integration`); confirm it did not DROP vs before the change (the diff removes one guard branch and adds a tested method).

- [ ] **Step 4: Push**

```bash
git push -u origin claude/dreamy-clarke-prpl3
```
(On network failure retry up to 4 times with 2s/4s/8s/16s backoff.) Do NOT open a PR unless the user asks.
