# tcard-service Design Spec

**Status: ACCEPTED 2026-07-16** — rewritten from the shipped service by re-examining every design decision. All decisions below are settled.

## Problem

Frontend clients need to fetch versioned card template JSON documents. Templates are authored into the MongoDB `cards` collection; each document is identified by a `(path, cardVersion)` pair. Clients must be able to (a) fetch a specific template by path **and** version, and (b) trigger a re-sync so newly authored documents become servable.

## Shape

A small Gin HTTP service, portal-service's shape: flat `package main` at `tcard-service/`, an in-memory cache in front of Mongo, standard probes, no NATS.

```
client ──HTTP──> tcard-service ──(load/refresh)──> MongoDB `cards`
                     │
              cardCache (atomic snapshot, (path, cardVersion) → template JSON)
```

## Document model

Each `cards` document carries:

| Field | Type | Role |
|---|---|---|
| `path` | string | Routing key. **Stripped from the served payload** — it identifies the card, it is not template content. |
| `cardVersion` | string | The document's version. Part of the routing key and **retained in the payload**. |
| `cardUsage` | (schemaless) | Template content. |
| `type` | (schemaless) | Template content. |
| `schema` | (schemaless) | Template content. |
| `version` | (schemaless) | Data-type / format version of the template — **not** the document version, and **not** indexed. |
| `body` | (schemaless) | Template content. |
| `_id` | Mongo | Mongo-internal. **Stripped from the served payload.** |

The served payload is therefore `{cardVersion, cardUsage, type, schema, version, body}` — the whole document minus `_id` and `path`. Aside from `path`/`cardVersion` (which must be non-empty strings so the document can be keyed), the content is schemaless: the store never imposes a typed struct on it.

## API contract

| Route | Method | Behavior |
|---|---|---|
| `POST /api/v1/cards/register` | POST | Validate a card document and insert it into `cards`, then reload the cache so it is servable at once. `201 {"success":true}` on success. `400 bad_request` on a field/format failure (see Card registration below); `409 conflict` if `cardVersion` is not strictly the highest for its `path`, or `(path, cardVersion)` already exists; `500 internal` on a Mongo failure. Internal callers, not browsers. |
| `POST /api/v1/cards/refresh` | POST | Reload the **entire** `cards` collection into the cache (full snapshot swap). `200 {"status":"ok","count":N}` where `N` is the number of distinct `(path, cardVersion)` entries now cached; `500 internal` envelope if Mongo fails (the previous snapshot keeps serving). Callers are other services, not end-user browsers. |
| `GET /api/v1/cards/{path}@{cardVersion}.template.json` | GET | Serve the cached document for `(path, cardVersion)`, verbatim JSON, no per-request Mongo read. `400 bad_request` if the filename doesn't end in `.template.json`, if there's no `@` separator (**version is required**), or if either `path` or `cardVersion` is empty. `404 not_found` if no document matches that `(path, cardVersion)`. |
| `GET /healthz` | GET | Liveness. |
| `GET /readyz` | GET | 503 until the first successful cache load; an **empty** loaded snapshot is ready. |

The request filename is a single URL segment (`:file`). The handler strips the `.template.json` suffix, then splits the remainder on the **last** `@` into `path` and `cardVersion`. Keeping it one segment means the flat-path routing (below) is unaffected.

Errors use the standard `pkg/errcode` envelope via `errhttp.Write`. The two client-facing endpoints — `GET .../{path}@{cardVersion}.template.json` (frontend) and `POST /register` (admin client) — are documented in `docs/client-api.md` §11, alongside the other client-facing HTTP services (e.g. portal-service). `POST /refresh` is service-to-service internal and stays out of that doc. (CLAUDE.md's hard rule only *mandates* `client-api.md` for `chat.user.*` NATS and auth-service HTTP; this follows the doc's broader "every API a client can call" scope. The derived NATS views `request-reply.md`/`events.md` are unaffected — these are HTTP, like the auth/portal entries.)

## Card registration

`POST /api/v1/cards/register` accepts the full card JSON `{path, cardVersion, cardUsage?, type, schema, version, body}` and validates it before insert. Validation runs in two tiers:

**Field/format checks — `400 bad_request`:**

1. All fields are **required except `cardUsage`** — `path`, `cardVersion`, `type`, `schema`, `version` must be present and non-empty, and `body` must be a **non-empty array** (the AdaptiveCard body).
2. `cardVersion` is a semantic version `a.b.c` — exactly three dot-separated non-negative integers, **no leading zeros**.
3. `type` equals `"AdaptiveCard"`.
4. `schema` equals `"http://adaptivecards.io/schemas/adaptive-card.json"` (pinned).
5. `version` equals the string `"1.5"` (pinned).
6. `path` must **not contain `/`** — the GET route is a single URL segment, so a slashed path could never be served (matches the flat-path decision below). `@` and a `.template.json` suffix in `path` are allowed: the GET parser splits on the last `@` and strips one `.template.json`, so they remain servable.

**Ordering check — `409 conflict`:**

7. `cardVersion` must be **strictly greater** (semver order) than the highest existing `cardVersion` for the same `path`. A duplicate `(path, cardVersion)` is also a conflict (the unique index rejects it). Existing versions that are not valid `a.b.c` semver are ignored when computing the max — the first well-formed version for a new path is trivially the highest.

On success the document is inserted, the card is **added to the cache** (copy-on-write into the current snapshot, no full reload) so it is servable at once, and the response is `201 {"success":true}`. The cache's writers (`Add` and the refresh `Load`, including its Mongo read) **serialize on a write mutex**, so a register that overlaps a `refresh`/daily reload can never revert it — nor be reverted by a stale reload. Reads stay lock-free. If the fetch-back for the cache fails — or the cache has not completed its first load yet — the insert still stands (`201`) and the card appears on the next refresh; the miss is logged, not surfaced.

The ordering check (7) reads the current versions and then inserts as two separate steps and is **not** serialized, so two concurrent registers for the same `path` can both pass and insert **different** versions; the compound unique index still rejects an exact `(path, cardVersion)` duplicate (one gets `409`). Both resulting cards are valid and servable, so this is accepted — registration is an admin operation (~5–10/week), which makes the window effectively unreachable.

Semver comparison is a hand-rolled `a.b.c` integer parse and compare (major, then minor, then patch) — no third-party dependency. Registration mutates state and is internal-only (network policy, no app-level auth), the same trust boundary as `POST /refresh`.

## Decisions

1. **Payload is the whole document minus `_id` and `path`.** `_id` is Mongo-internal and `path` is the routing key echoed in the URL; neither is template content, so both are removed. `cardVersion` **stays** in the payload — it is meaningful content the client may display. `_id` is dropped via the Find projection; `path` is removed from the decoded document before it is rendered to relaxed extended JSON.

2. **Version is required and lives in the request.** A card is addressed by `(path, cardVersion)`, carried as `{path}@{cardVersion}.template.json`. A request without a version (no `@`) is a `400`, never a "latest wins" guess — the service does not choose a version on the client's behalf.

3. **`cardVersion` is a string.** It flows through as a string end-to-end: the URL value (always a string) compares directly against the string field, and the cache key holds it verbatim. No numeric parsing or ordering is performed.

4. **Cache key is `(path, cardVersion)`.** The snapshot is `map[cardKey]json.RawMessage` where `cardKey` is a `{path, cardVersion string}` struct — a struct key avoids any delimiter-collision risk a concatenated string key would carry.

5. **Cache semantics — sync, not protect.** Refresh swaps in exactly what Mongo holds: new documents appear, deleted ones drop out, an empty collection is a valid (ready) snapshot. This intentionally differs from portal-service's directory cache, which rejects empty refreshes because a cron rewrites its collection wholesale. `cards` has no such wholesale rewriter — documents are authored incrementally — so "sync to Mongo" is correct here. **Do not "fix" this to match portal.**

6. **Concurrency.** `atomic.Pointer[map[cardKey]json.RawMessage]` gives lock-free reads; writers (`Load` and `Add`) serialize on a write mutex held across `Load`'s Mongo read, so a register's `Add` and a refresh can't clobber each other. The map is never mutated in place, so readers never need a lock.

7. **Startup + daily + on-demand refresh.** `RefreshLoop` populates the cache at startup (30s retry after a failed attempt), then re-syncs **once a day at a fixed wall-clock time** — `TCARD_CACHE_REFRESH_AT`, default `08:00+08:00` (08:00 UTC+8), parsed with layout `15:04Z07:00` — in addition to the on-demand API. `nextDailyRefresh(now, at)` is a pure, table-tested scheduling function. The daily pass is a safety net for a missed on-demand `POST`; the fixed local time suits a daily authoring cadence.

8. **Flat single-segment paths.** The route is `/api/v1/cards/:file`; the `:file` value (`{path}@{cardVersion}.template.json`) is one URL segment, so a `path` containing `/` is not reachable. Card paths are flat tokens (`home`, `profile`).

9. **Refresh is POST-only, internal-only.** It mutates server state (a full collection scan) and is invoked by other services, not browsers. There is no app-level auth: the endpoint relies on network policy being internal-only (not internet-exposed). A `GET` to `/api/v1/cards/refresh` falls through to the `:file` route and gets a `400` (no `@`/`.template.json`). This matches portal-service; app-level auth is intentionally out of scope.

10. **Rows that can't key the cache are skipped, not fatal.** A document missing a non-empty string `path` **or** a non-empty string `cardVersion` is logged and skipped. A duplicate `(path, cardVersion)` → first wins with a warning. `EnsureIndexes` adds a **unique compound index on `(path, cardVersion)`** at startup so duplicates become insert-time errors upstream. The `version` field is a data-type/format version, semantically unrelated to document identity, so it is **not** indexed.

11. **Unique index, fail-fast on boot.** If the existing collection already holds two documents with the same `(path, cardVersion)`, `EnsureIndexes` fails and the service exits non-zero on startup. A duplicate key pair is a data bug that must be fixed upstream; crashing surfaces it loudly rather than silently serving whichever row won cursor order.

12. **Config.** `PORT` (default `8087` — 8085 is portal, 8086 upload host-side), `MONGO_URI` (required), `MONGO_DB` (default `chat`), `MONGO_USERNAME`/`MONGO_PASSWORD`, `TCARD_CACHE_REFRESH_AT` (default `08:00+08:00`). Collection name is fixed to `cards`.

13. **Registration validates then writes, then adds to the cache.** `POST /register` (see Card registration) is the write path: it validates the field/format and ordering rules, inserts the document, then fetches that one card back and copy-on-writes it into the cache snapshot for instant serviceability (no full reload). The pinned `type`/`schema`/`version` values and the "strictly-highest `cardVersion` per path" rule keep the collection well-formed so the read path stays a simple `(path, cardVersion)` lookup. The store gains an insert, a per-path version query, and a single-card fetch; the compound unique index (Decision 10) still blocks an exact duplicate under the accepted concurrent-register race.

## Testing

- **Unit** (mocked `CardStore`, table-driven, race detector): cache load / refresh / error / empty / duplicate-`(path, cardVersion)` / concurrent-read semantics; the served payload excludes `_id` and `path` and retains `cardVersion`; every handler status path — `200` hit, `404` miss, and `400` for each malformed-filename case (no `.template.json` suffix, no `@`, empty `path`, empty `cardVersion`). **Register**: `201` happy path; `400` for each field/format failure (missing required field, `path` containing `/`, non-array/empty-array `body`, non-semver or leading-zero `cardVersion`, wrong `type`/`schema`/`version`); `409` for a non-highest or duplicate `cardVersion`; a failed cache fetch-back after a successful insert still returns `201`; the semver parse/compare helper is table-tested (valid/invalid/leading-zero forms, ordering, equal-not-greater). Written **first** (Red) per CLAUDE.md TDD.
- **Integration** (`//go:build integration`, `testutil.MongoDB`): `ListCards` round-trips nested JSON documents, skips documents missing a string `path` or `cardVersion`, excludes `_id` **and** `path` while retaining `cardVersion`; unique compound-index enforcement on `(path, cardVersion)`. **Register**: a valid card inserts and is then servable via GET; the ordering check rejects a lower/equal `cardVersion` for an existing `path` with `409`; the unique index rejects a duplicate.
- Coverage target ≥80% (CI-gated like portal's pipeline; `main.go` and `store_mongo.go` excluded from the **unit** gate — both are integration-covered, and the service is small enough that the Docker-only store would otherwise dominate the denominator).

## Implementation plan

See `docs/superpowers/plans/2026-07-14-tcard-service.md`.
