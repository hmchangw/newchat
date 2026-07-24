# tcard-service: `_tcardVersion` rename, slash paths, and directory listings

Date: 2026-07-24
Status: Approved by user (design review in session)

## Problem

1. The MongoDB `cards` collection stores the version field as `_tcardVersion`, but the
   service reads/writes `cardVersion` — the code and the data disagree.
2. Card paths in MongoDB contain `/` (always exactly three segments, `a/b/c`), but the
   route `GET /api/v1/cards/:file` uses a single-segment Gin param, so
   `GET /api/v1/cards/a/b/c@0.0.1.template.json` cannot match and fails.
3. There is no way to browse the card hierarchy; clients need a directory-listing
   response when the URL does not end in `.template.json`.

## Scope

All changes are inside `tcard-service/` (handler, routes, cache, store, validation,
tests). No other service, no `pkg/` change, no `docs/client-api.md` change (tcard-service
is not a client-facing NATS handler nor auth-service HTTP).

## Design

### 1. Rename `cardVersion` → `_tcardVersion` (all wire/storage surfaces)

- **Mongo document field**: `InsertCard` writes `_tcardVersion`; `docToCard` keys on
  `_tcardVersion`; `ListVersions` projects and reads `_tcardVersion`; `GetCard` filters
  on `_tcardVersion`.
- **Unique index**: `EnsureIndexes` creates the unique index on `(path, _tcardVersion)`.
  No legacy-index handling: the old `(path, cardVersion)` index was removed manually from
  the production collection (post-review decision), so the code only ensures the new
  index exists.
- **Register API**: `cardDoc` JSON tag becomes `_tcardVersion`; validation error messages
  say `_tcardVersion`.
- **Served template JSON**: the version field embedded in the template payload comes from
  the stored document, so it is `_tcardVersion` automatically.
- Go identifiers (`CardVersion`, `cardKey.cardVersion`, variable names) do not change —
  only JSON/BSON names.

### 2. Routing

`routes.go`: replace `r.GET("/api/v1/cards/:file", ...)` with
`r.GET("/api/v1/cards/*file", ...)`. Gin wildcards match `/`; `POST /register` and
`POST /refresh` live in the POST method tree, so there is no route conflict. The handler
trims the leading `/` that Gin includes in wildcard param values.

### 3. Template flow — URL ends with `.template.json`

Unchanged semantics, but the path part may contain `/`:

- Strip the `.template.json` suffix, split on the **last** `@` into `(path, version)`.
- Lock-free cache lookup `Get(path, version)`; hit → 200 with the raw template JSON.
- Cache miss → 404 `errcode.NotFound("card template not found")`.
- Malformed input keeps today's 400s: missing `@`, empty spec, empty path or version.

### 4. Listing flow — no `.template.json` suffix

Data-driven scan of the in-memory cache snapshot (no Mongo read). The depth-3 invariant
(`a/b/c`) is confirmed by the data owner and enforced at registration (see §5), but the
listing algorithm is generic — it hard-codes no depth.

Let `prefix` be the wildcard remainder trimmed of leading/trailing `/` (empty at root).
Scan every cached `(path, version)` key:

- `path == prefix` → the request names a full card path with no version
  → 400 `errcode.BadRequest`, message: `no version specified for card "<prefix>"`
  (the request is malformed — a version is required — so 400, not 404;
  decided in post-review follow-up).
- Otherwise, when `prefix` is empty or `path` starts with `prefix + "/"`, look at the
  remainder after the prefix:
  - remainder has more than one segment → contribute a **folder** entry:
    `prefix + "/" + firstSegment` (just `firstSegment` at root), deduped.
  - remainder is exactly one segment → contribute a **card** entry:
    `path + "@" + version` — one entry per cached version.
- Entries are full paths from root. Sorting: folders lexicographic; cards by path
  lexicographic, then by semver order (existing `parseSemver`) within a path.

Responses:

- Match(es) found → `200` body `{"statusCode": 200, "cards": [...], "folders": [...]}`
  (either array may be empty; with depth-3 data at most one is non-empty).
- No match and prefix non-empty → 404 `errcode.NotFound`, message:
  `given path "<prefix>" for card list not found`.
- Root (empty prefix) → always 200 once the cache has loaded, even when it is empty
  (both arrays empty, serialized as `[]`, not `null`).
- Never-loaded cache (before the first successful Load) → 404 `errcode.NotFound`,
  message: `no paths or cards exist` — for every listing request including root
  (decided in post-review follow-up; a loaded-but-empty cache still lists 200).
- Precedence: an exact card-path match wins — if `prefix` equals a cached card path, the
  400 "no version specified" is returned even if deeper children also exist under it
  (unreachable with the depth-3 invariant, but the generic algorithm defines it
  explicitly).

With depth-3 data the four agreed rules fall out:

| Request path | Result |
|---|---|
| `` (root) | 200, `folders` = all unique `a` |
| `a` | 200, `folders` = all `a/b` under `a` |
| `a/b` | 200, `cards` = all `a/b/c@version` under `a/b` |
| `a/b/c` (no version) | 400, `no version specified for card "a/b/c"` |

### 5. Registration validation

- `path` must now contain `/`: exactly **3 non-empty segments** (`a/b/c`), no leading or
  trailing slash (implied by non-empty segments), and no `@` anywhere in the path (an
  `@` would break template-URL parsing).
- All other checks unchanged: semver `_tcardVersion`, pinned type/schema/version,
  non-empty body array, highest-version conflict check, duplicate-key conflict.

### 6. Cache

New read method on `cardCache` (e.g. `List(prefix string)`) implementing §4's scan over
the current snapshot — lock-free via the existing `atomic.Pointer` load, no new state,
automatically consistent with `Load`/`Add`. Returned struct distinguishes
"exact card path hit" / "children found" / "nothing" so the handler picks 200 vs the two
404 messages.

### 7. Error handling

Tier-1 errcode discipline as today: handlers return `errcode.BadRequest` /
`errcode.NotFound` / `errcode.Conflict` via `errhttp.Write`; infra failures stay raw
wrapped errors.

### 8. Testing (TDD)

- `cache_test.go`: unit tests for `List` — root, one-segment, two-segment prefixes;
  multi-version cards; dedup of folders; sorting (incl. semver order `0.0.2 < 0.0.10`);
  exact-path detection; empty cache; nil (never-loaded) snapshot.
- `handler_test.go` (table-driven):
  - Template flow: slash paths (`a/b/c@0.0.1.template.json`), 404 on miss, existing 400
    cases still pass.
  - Listing flow: the four rules above, unknown prefix 404 with exact message,
    exact-path-no-version 404 with exact message, root-on-empty-cache 200,
    trailing-slash normalization (`a/b/` ≡ `a/b`), `@`-containing listing prefix → 404,
    empty arrays serialize as `[]`.
  - Register: `_tcardVersion` JSON field accepted (`cardVersion` no longer recognized →
    400 "_tcardVersion is required"), 3-segment path enforcement (0/1/2/4 segments and
    empty segments rejected, `@` in path rejected).
- `integration_test.go`: store round-trip with `_tcardVersion` field; `EnsureIndexes`
  enforces uniqueness on `(path, _tcardVersion)` and stays idempotent.
- Coverage floor 80% (target 90%+) per project rules; `make generate` if `store.go`
  interface changes (it does not — interface signatures are unchanged).

## Out of scope

- Data migration (the DB already stores `_tcardVersion`; existing slash paths already
  conform to depth 3).
- Latest-version filtering in listings (all versions are listed by agreement).
- Any caching/tree precomputation for listings (rejected as YAGNI in design review).
