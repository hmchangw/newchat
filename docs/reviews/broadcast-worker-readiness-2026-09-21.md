# broadcast-worker — production readiness review

**Date:** 2026-09-21  
**Branch:** `claude/production-readiness-report-2026-09-21` @ `1a95503` (base `main`)  
**Overall score:** 3.3 / 5 (baseline 2026-09-01: 3.5, Δ -0.2)  
**Method:** six independent expert reviews (one per dimension) against `CLAUDE.md` and Go-at-scale practice; every finding cites `file:line` on this tree.

## Executive summary

The CLAUDE.md hot-path contract holds under inspection: `sonic` on every per-message codec call with `jsonwarm.Pretouch` at boot, exactly one MongoDB write and it is buffered, coalesced and never returned to the handler, `jsretry.Settle` only, every subject from `pkg/subject`, member resolution batched with no N+1 — code quality and architecture both score 4. The high finding is a client contract regression: `buildRoomEvent` never sets `systemMsg`, a field #382 added to this exact function and #188 dropped without touching `docs/client-api.md` or `events.md`, so in encrypted rooms a rename or member-add now advances unread state and reorders every member's sidebar; no test asserts the field. Three reviewers flagged the same fleet pattern from here: the "must match across services" knobs (`ROOM_KEY_RETIRED_TTL`, `ROOM_LOCALITY_GRACE`, `USER_CACHE_*`) are re-declared per service because no owning config type exists. Performance is sound in sizing and batching but has no per-message deadline or heartbeat, so a slow Mongo read holds a `MAX_WORKERS` slot past `AckWait` and the redelivery fans the same message out twice; thread fan-out spawns an unbounded goroutine per follower; the preview seal is awaited before fan-out. DM mutations read membership from a different, uncached, differently-gated source than creates, so an unsubscribed botDM member gets edits but not messages. Coverage is 68.1%, every mutation handler's store-error branch is untested, the preview-writer shed path is asserted only by comment, and the documented dev harness cannot start.

| Dimension | Score |
|---|---|
| Code quality | 4 |
| Architecture | 4 |
| Test coverage | 2 |
| Maintainability | 3 |
| Integration | 3 |
| Performance | 4 |

**Findings by severity:** 0 critical, 4 high, 16 medium, 19 low, 11 nitpick (50 total).  
**Highest-risk dimension:** Test coverage (2).

**Repo-wide inputs used by every dimension:** `make generate` → no stale mocks; gosec (medium+) and the 20 repo-owned semgrep rules → 0 findings; govulncheck and the semgrep registry packs could not run (sandbox egress 403) so dependency-CVE status is unverified; unit coverage from one `go test -race -covermode=atomic ./...` run with 0 failing packages.


## 2. Code quality — score 4

### Evidence

- [low] `encoding/json` on a publish path of a sonic-mandated hot-path worker — `broadcast-worker/roomactivity.go:187` — `json.Marshal(evt)` for `RoomActivityEvent`; CLAUDE.md names broadcast-worker as a sonic codec worker. Impact is small (throttled to one publish per room per `ROOM_ACTIVITY_REFRESH_INTERVAL`), but it is the one codec inconsistency in the service. (`handler.go:7` imports `encoding/json` only for the `json.RawMessage` type at :1004/:1044, which is fine.)
- [low] Bare `return err` in five handler call sites — `broadcast-worker/handler.go:294`, `:298`, `:427`, `:698`, `:855` — CLAUDE.md §3 says never return bare `err`. The callees (`publishChannelEvent`, `publishDMEvents`, `publishRoomEvent`, `publishMutation`) already wrap with room/subject context so no diagnostic is lost, but the pattern is inconsistent with the sibling sites (`:335`, `:564`, `:627`, `:634`, `:736`) that do wrap.
- [low] Mixed structured-log key casing in one file — `broadcast-worker/handler.go:208` (`messageID`), `:289` (`room_id`), `:818-821` (`roomID`, `siteID`) — 17 `room_id` vs 4 `roomID`, 9 `messageID`, 4 `siteID`, 4 `parentMessageID`. A log query on `room_id` misses the `handleReacted` drop lines; CLAUDE.md asks for structured fields but the key vocabulary is not stable.
- [low] `skipped` from `cassandra.DecodeAttachments` discarded without a comment — `broadcast-worker/handler.go:1264` — `decoded, _ := cassandra.DecodeAttachments(msg.Attachments)`. The identical condition is treated as an error one file over (`preview.go:64-68`, "a skip means malformed upstream"), so the client event silently drops malformed attachments while the preview refuses them. CLAUDE.md: comment if intentionally discarded.
- [nitpick] Cross-service notification type is a magic string — `broadcast-worker/handler.go:867` — `Type: "reaction"`; `pkg/model/event.go:155` only documents it in a comment, so the producer and notification-worker cannot share a constant and drift is uncatchable at compile time.
- [nitpick] Redundant loop-variable copy — `broadcast-worker/handler.go:1295` — `account := account` is a no-op under go 1.25 (`store_mongo.go:141` already relies on per-iteration loop vars in its own `#nosec G601` note).
- [nitpick] Duplicated sender-composition logic — `broadcast-worker/handler.go:1253-1263` vs `broadcast-worker/preview.go:87-95` — both build a `model.Participant` from `msg` + `userMap`, with different name fallbacks (account string vs blank + `BotAwareDisplayName`). The divergence looks intentional (preview mirrors history-service's walk) but is undocumented at the client-message site.
- [nitpick] Anonymous projection struct declared twice — `broadcast-worker/preview.go:137-139` and `:143-145` — `struct{ Name string \`bson:"name"\` }` should be a named type.
- [nitpick] Pretouch list omits two sonic-marshalled types — `broadcast-worker/pretouch.go:11-19` vs `handler.go:470` (`SubscriptionMentionEvent`) and `:874` (`NotificationEvent`) — first reaction/remote-mention message pays codec compilation on the fan-out goroutine.

### Recommendations

- [low] Switch `roomactivity.go:187` to `sonic.Marshal` — `broadcast-worker/roomactivity.go:187` — closes the codec split; `RoomActivityEvent` has no map fields and no byte-identity consumer, so the sonic wire-compat caveat does not apply.
- [low] Wrap the five bare returns with the operation being performed — `broadcast-worker/handler.go:294,298,427,698,855` — e.g. `fmt.Errorf("fan out created event for room %s: %w", meta.ID, err)`, matching the sibling sites.
- [low] Normalise log keys to snake_case (`room_id`, `message_id`, `site_id`, `parent_message_id`) — `broadcast-worker/handler.go` (all 21 camelCase sites) — makes room/message log correlation a single query across the service; consider enabling `sloglint` key-naming in `.golangci.yml` to hold it.
- [low] Decide one policy for malformed attachment blobs and comment it — `broadcast-worker/handler.go:1264` — either log `skipped` with `room_id`/`message_id` (lenient, matches `DecodeAttachments`' stated intent) or fail like `preview.go:67`; today the same bad blob is silently dropped for clients and rejected for the preview.
- [nitpick] Add `model.NotificationTypeReaction = "reaction"` in `pkg/model/event.go:155` and use it at `handler.go:867` and in notification-worker.
- [nitpick] Name the apps projection type once and delete `account := account` — `broadcast-worker/preview.go:137`, `handler.go:1295`.
