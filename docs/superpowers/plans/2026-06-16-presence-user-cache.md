# Cache user→siteID lookups in user-presence-service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Front `user-presence-service`'s user-directory lookups with the existing in-memory `userstore.Cache`, mirroring `broadcast-worker`, so repeated account→siteID resolutions are served from memory instead of hitting MongoDB on every presence query.

**Architecture:** Wrap the existing `userstore.NewMongoStore(...)` in `userstore.NewCache(store, size, ttl)` in `main.go`. `*userstore.Cache` already satisfies the service's `UserDirectory` interface (`FindUsersByAccounts`), so it's a transparent drop-in — no handler, store, or interface changes. Two config knobs (`USER_CACHE_SIZE`, `USER_CACHE_TTL`) are added, identical to broadcast-worker.

**Tech Stack:** Go 1.25, `caarlos0/env` config, `pkg/userstore` (hashicorp golang-lru), `log/slog`.

---

## File Structure

- Modify: `user-presence-service/main.go` — add two `Config` fields; replace the direct `NewMongoStore` wiring with a cache-wrapped version + fail-fast + startup log.
- Modify: `user-presence-service/deploy/docker-compose.yml` — surface the two env vars in local dev.

No new files. No test files (the only new code is `main.go` DI/config wiring, which neither this service nor broadcast-worker unit-tests; `userstore.Cache` is already tested in `pkg/userstore`, and handler tests use a mocked `UserDirectory`).

---

### Task 1: Add user-cache config + wiring in main.go

**Files:**
- Modify: `user-presence-service/main.go` (Config struct + the `userDir` wiring line, currently `main.go:104`)

- [ ] **Step 1: Add the two config fields to the top-level `Config` struct**

In `user-presence-service/main.go`, the `Config` struct currently ends with the
`Presence PresenceConfig` field. Add the two cache fields directly below
`SiteID` (keep them flat, matching broadcast-worker — not under a sub-prefix):

```go
type Config struct {
	SiteID        string         `env:"SITE_ID,required"`
	UserCacheSize int            `env:"USER_CACHE_SIZE" envDefault:"10000"`
	UserCacheTTL  time.Duration  `env:"USER_CACHE_TTL"  envDefault:"5m"`
	NATS          NATSConfig     `envPrefix:"NATS_"`
	Valkey        ValkeyConfig   `envPrefix:"VALKEY_"`
	Mongo         MongoConfig    `envPrefix:"MONGO_"`
	Presence      PresenceConfig `envPrefix:"PRESENCE_"`
}
```

(`time` is already imported — it's used by `PresenceConfig`.)

- [ ] **Step 2: Replace the direct `NewMongoStore` wiring with a cache-wrapped version**

Find this line (currently `main.go:104`):

```go
	userDir := userstore.NewMongoStore(mongoClient.Database(cfg.Mongo.DB).Collection("users"))
```

Replace it with:

```go
	userDir, err := userstore.NewCache(
		userstore.NewMongoStore(mongoClient.Database(cfg.Mongo.DB).Collection("users")),
		cfg.UserCacheSize, cfg.UserCacheTTL)
	if err != nil {
		slog.Error("init user cache failed", "error", err)
		os.Exit(1)
	}
	slog.Info("user-cache enabled", "size", cfg.UserCacheSize, "ttl", cfg.UserCacheTTL)
```

Note: `err` is already declared earlier in `main` (reused by the surrounding
`mongoutil.Connect` / `natsutil.Connect` calls), so `userDir, err := ...` uses
`:=` correctly because `userDir` is new. `os.Exit` and `slog` are already
imported (used by the adjacent connect-failure branches).

- [ ] **Step 3: Build to verify it compiles**

Run: `make build SERVICE=user-presence-service`
Expected: builds successfully, no errors.

- [ ] **Step 4: Run unit tests + lint**

Run: `make test SERVICE=user-presence-service && make lint`
Expected: tests PASS (handler tests use a mocked `UserDirectory`, unaffected); lint clean.

- [ ] **Step 5: Commit**

```bash
git add user-presence-service/main.go
git commit -m "feat(user-presence-service): cache user→siteID lookups via userstore.Cache"
```

---

### Task 2: Surface the cache knobs in local docker-compose

**Files:**
- Modify: `user-presence-service/deploy/docker-compose.yml` (the `environment:` block)

- [ ] **Step 1: Add the two env vars to the `environment` block**

In `user-presence-service/deploy/docker-compose.yml`, the `environment:` block
currently ends with `- PRESENCE_PEER_TIMEOUT=3s`. Add the two cache vars after
`- SITE_ID=site-local` (grouping them near other top-level service config),
matching broadcast-worker's values:

```yaml
      - SITE_ID=site-local
      - USER_CACHE_SIZE=10000
      - USER_CACHE_TTL=5m
```

- [ ] **Step 2: Validate compose syntax**

Run: `docker compose -f user-presence-service/deploy/docker-compose.yml config -q`
Expected: no output (valid). If `docker` is unavailable in this environment,
visually confirm the YAML indentation matches the surrounding `-` list items.

- [ ] **Step 3: Commit**

```bash
git add user-presence-service/deploy/docker-compose.yml
git commit -m "chore(user-presence-service): surface USER_CACHE_* env vars in compose"
```

---

## Verification (after both tasks)

- `make build SERVICE=user-presence-service` — compiles.
- `make test SERVICE=user-presence-service` — unit tests pass.
- `make lint` — clean.
- Confirm `userDir` is consumed unchanged by `NewHandler(...)` (interface drop-in
  — no handler signature change).
