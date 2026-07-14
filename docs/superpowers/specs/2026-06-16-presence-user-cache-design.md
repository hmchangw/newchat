# Design: Cache user→siteID lookups in user-presence-service

**Date:** 2026-06-16
**Service:** `user-presence-service`
**Status:** Approved

## Problem

`user-presence-service` resolves each account's home site by querying MongoDB on
every `QueryBatch` request. The handler holds a `UserDirectory` whose only method
is `FindUsersByAccounts`, and `main.go` wires it directly to the uncached
`userstore.NewMongoStore(...)`. Every presence query therefore hits Mongo to
resolve sites, even though account→site mappings change rarely.

`broadcast-worker` already solved this: it fronts the same `userstore.MongoStore`
with `userstore.NewCache(...)`, an in-memory LRU+TTL cache that returns
`model.User` values (carrying `SiteID`). We mirror that here.

## Goal

Front the presence service's user-directory lookups with the existing
`userstore.Cache`, identical in shape and tuning to `broadcast-worker`, so
repeated site resolutions are served from memory.

## Non-Goals

- No changes to handler logic, the `UserDirectory` interface, or the store layer.
- No new cache implementation — reuse `pkg/userstore.Cache` as-is.
- No cache invalidation hooks (broadcast-worker relies on TTL expiry; same here).
- No client-facing RPC schema change → no `docs/client-api.md` edit.

## Design

The change is isolated to `user-presence-service/main.go` plus its local
docker-compose env surface.

`*userstore.Cache` already satisfies the existing `UserDirectory` interface
(`FindUsersByAccounts(ctx, accounts) ([]model.User, error)`), so the cache is a
transparent drop-in — `NewHandler(...)` and every test are untouched.

### 1. Config (`main.go`)

Add two fields to the top-level `Config` struct, matching broadcast-worker's
names, env vars, and defaults (flat, not under a sub-prefix):

```go
UserCacheSize int           `env:"USER_CACHE_SIZE" envDefault:"10000"`
UserCacheTTL  time.Duration `env:"USER_CACHE_TTL"  envDefault:"5m"`
```

### 2. Wiring (`main.go`, replacing the direct `NewMongoStore` line)

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

Fail-fast on error mirrors broadcast-worker (`NewCache` returns an error for a
nil store or non-positive size/ttl).

### 3. Local dev (`deploy/docker-compose.yml`)

Add the two env vars to the service's `environment` block so the knobs are
visible in local dev, matching broadcast-worker's compose:

```yaml
      - USER_CACHE_SIZE=10000
      - USER_CACHE_TTL=5m
```

## Testing

- `userstore.Cache` is already unit-tested in `pkg/userstore`; handler tests use
  a mocked `UserDirectory`. The only new code is `main.go` DI/config wiring,
  which neither service unit-tests — so there is no meaningful new TDD unit.
- Verification: `make build SERVICE=user-presence-service`, `make test
  SERVICE=user-presence-service`, and `make lint`.

## Trade-offs

- A user's site reassignment can be stale in presence routing for up to the TTL
  (5m) — the exact trade-off broadcast-worker already accepts. Acceptable given
  account→site mappings change rarely.
