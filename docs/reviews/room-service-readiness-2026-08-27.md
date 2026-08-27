# Production-Readiness Review — `room-service`

| | |
|---|---|
| **Service** | `room-service` |
| **Date** | 2026-08-27 |
| **Branch** | `claude/new-session-439bi9` |
| **Overall score** | **2.83 / 5** |
| **Method** | Six independent expert audits (code quality + SAST, architecture, test coverage, maintainability, integration, performance) against `CLAUDE.md` and general Go-microservice practice |

## Executive summary

`room-service` is a well-built service carrying a large and mostly undisclosed amount of structural debt. The things that are easy to get wrong in this codebase are right: every one of its 27 NATS registrations and 12 publishes goes through `pkg/subject` (zero raw `fmt.Sprintf` subjects), stream configs come from `pkg/stream`, `bootstrapStreams` respects the `BOOTSTRAP_STREAMS` opt-in and sets only `Name + Subjects`, all cross-site federation goes through `outbox.Publish` to the local OUTBOX rather than a direct remote INBOX publish, every federated event type is registered in exactly one `pkg/outbox` partition set, every published event carries a `Timestamp` set at the publish site, config is a typed `caarlos0/env` struct with no `os.Getenv` anywhere, shutdown uses `pkg/shutdown.Wait` with the documented ordering, and `gosec` is clean. Handler-level error-path testing is genuinely strong and mocks are fresh.

What holds it back is scale and verification. The service is 6,314 production LOC in nine flat files, with a 2,643-line `handler.go` spanning eight unrelated domains and a 47-method `RoomStore` god interface — both peers of comparable size (`user-service`, `history-service`) already moved to the sanctioned sub-package layout and keep no file over 993 lines. Unit coverage is **57.7%**, below the repo's 60% critical threshold, because the 2,046-line `store_mongo.go` sits at 2.6% and is verified only behind the `integration` build tag that `make test` does not run; no CI job enforces the documented 80% floor. On the runtime side the store leaks `mongo.ErrNoDocuments`/`IsDuplicateKeyError` into handlers, a missing room returns 500 from four RPCs and 404 from two others, several hot reads fetch whole documents against the "always project precisely" rule, mention autocomplete joins an entire room before `$limit`, and a per-bot `GetApp` loop N+1s without a request-size cap. One correctness bug stands out: rebalanced `section_moved` federation events all share a `Nats-Msg-Id` and are silently deduplicated by JetStream, leaving cross-site users with stale `sectionOrder`.

None of this blocks the service from running — it runs, and its tests pass. It blocks confident change. Close the coverage gap and enforce it, fix the dedup-ID bug and the not-found/500 inconsistency, then decompose.

## Dimension scores

| # | Dimension | Score |
|---|-----------|:-----:|
| 1 | Go code quality (incl. SAST) | 4 / 5 |
| 2 | Architecture | 4 / 5 |
| 3 | Test coverage | **1 / 5** |
| 4 | Maintainability | **2 / 5** |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |
| | **Average** | **2.83 / 5** |

## Findings by severity

| Severity | Count |
|----------|:-----:|
| `critical` | 1 |
| `high` | 16 |
| `medium` | 30 |
| `low` | 19 |
| `nitpick` | 14 |
| **Total** | **80** |

The single `critical` is unit coverage at 57.7%, below the repo minimum of 80% and below the 60% threshold at which CLAUDE.md Section 4 floors the dimension at 1.

---

# 1. Go code quality — 4 / 5

Idiomatic, consistently formatted, and clean under `gosec`. The deductions are a genuine error-classification bug in the store, an inconsistent not-found contract across RPCs, and observability discipline that slipped in about a fifth of the log sites.

## SAST

`make sast` exits 2 — `gosec=PASS govulncheck=FAIL semgrep=FAIL`.

- **gosec v2** (`-severity medium -confidence medium -tests=true`): **PASS**, repo-wide and scoped to `./room-service/...` — 0 findings. Both `#nosec` suppressions in the service (`handler.go:487`, `handler.go:506`, G117 on `RoomKeyGetResponse.PrivateKey`) use the correct gosec-native form with justifications.
- **semgrep**, repo-local rules only (`--config=.semgrep/ room-service/`): 9 rules, **0 findings** — includes the `errcode` `WithCause` guard and `room-subject-publish-must-route`.
- **govulncheck** and the `p/golang` / `p/security-audit` semgrep registry rulesets **could not run**: the sandbox proxy returns 403 for `https://vuln.go.dev/index/modules.json.gz` and `https://semgrep.dev/c/p/golang`. `make tools` also failed (`pipx needs uv>=0.9.17, /root/.local/bin/uv reports 0.8.17`); gosec/govulncheck installed anyway, semgrep manually via `pipx install --backend pip`.

**No room-service SAST finding to fold in.** CVE reachability and the registry rulesets remain unverified in this environment — re-run `make sast` on a runner with network egress before treating the gate as green.

## Findings

- **[high] `GetRoom` mislabels every infra failure as "not found"** — `room-service/store_mongo.go:238` — `if err := s.rooms.FindOne(...).Decode(&room); err != nil { return nil, fmt.Errorf("room %q not found: %w", id, err) }` wraps Mongo timeouts and network errors in a "not found" message. `GetSubscription` immediately below (`store_mongo.go:281`) does it correctly by branching on `mongo.ErrNoDocuments`. `GetRoomAppRead` (`store_mongo.go:247`) repeats the bug.
- **[high] The same condition returns two different wire codes across RPCs** — `room-service/handler.go:663`, `:757`, `:885`, `:1385` — `removeMember`, `updateRole`, `addMembers` and `messageRead` all `return nil, fmt.Errorf("get room: %w", err)`, so a deleted room surfaces as `internal` (500). `roomRename` (`handler.go:1976`) and `roomRestricted` (`handler.go:2048`) branch to `errRoomNotFound` (404) for the identical case. Clients cannot distinguish "room gone" from "server broken".
- **[medium] 21 `slog.*` calls in request paths drop the request ID and trace correlation** — `room-service/handler.go:1165,1226,1406,1444,1495,1508,1521,1527,1533,1708,1850,1884,1895,1900,1906,1960,2032,2209,2566,2573` — e.g. `slog.Error("publish message_read DM event failed", "error", err, "roomId", roomID, "account", account)` with `ctx` in scope. CLAUDE.md Section 3 requires the correlation ID in all log lines; the non-`Context` variants also bypass the o11y handler's trace correlation. The service uses `ErrorContext`/`DebugContext` elsewhere (`:1745`, `:2242`), so this is inconsistency, not policy.
- **[medium] Three casings for the same structured-log key** — `room-service/handler.go:1495` (`"roomId"`) vs `:1960` (`"roomID"`) vs `:1230` (`"site_id"`) — counted across slog calls: `roomId` ×13, `roomID` ×4, `room_id` ×2, `siteId` ×1, `site_id` ×2. A log query for one room must union three field names.
- **[medium] Four hot reads fetch whole documents** — `room-service/store_mongo.go:875` (`GetUser`), `:887` (`GetApp`), `:899` (`FindDMSubscription`), `:723` (`getRoomSubscriptions`: `s.subscriptions.Find(ctx, bson.M{"roomId": roomID}, opts)` with no projection) — against CLAUDE.md Section 6's "always project precisely". The file's own `subscriptionReadProjection` comment calls the full Subscription doc "reflection-heavy", yet `getRoomSubscriptions` decodes it in full for every member on the `ListRoomMembers` fallback path.
- **[medium] Marshal error silently discarded** — `room-service/handler.go:2207` — `if canonData, err := json.Marshal(canonEvt); err == nil { ... }` has no `else`; a marshal failure drops the notification-worker cache-invalidation event with no log line. CLAUDE.md Section 3: "Never ignore errors silently — comment if intentionally discarded."
- **[medium] Unbounded member fetch in the Teams RPCs** — `room-service/handler_teams.go:62`, `:128` — `h.store.ListRoomMembers(ctx, roomID, nil, nil, false)` passes `limit=nil`, loading every member (up to `MAX_ROOM_SIZE`=1000) *before* `countIndividualMembers(...) > h.roomMembersLimit` (500) or `len(emails) > h.roomMembersCallLimit` (20) rejects. `expandChannelRefs` (`handler.go:1101`) shows the right pattern with `listLimit := h.maxRoomSize + 1`.
- **[low] `RoomStore` is a 52-method god interface** — `room-service/store.go:61` — satisfies "define interfaces in the consumer" only technically; no call site needs more than a handful, and it forces a 958-line regenerated mock.
- **[low] Store DTOs carry `bson` but no `json` tags** — `room-service/store.go:47` (`ReadReceiptRow`), `:56` (`RoomBotAppEntry`) — CLAUDE.md Section 3 requires both.
- **[low] Hardcoded 5s deadlines duplicate the configurable guard** — `room-service/handler.go:1194,1246,2538,2589` — `context.WithTimeout(ctx, 5*time.Second)` while `natsrouter.GuardConfig.RequestTimeout` (`REQUEST_TIMEOUT`, default 10s) already bounds every handler. Two un-reconciled deadline sources, one untunable.
- **[low] Numeric config accepted without validation** — `room-service/main.go:55-57,86-87` — `MAX_ROOM_SIZE`, `MAX_BATCH_SIZE`, `ROOM_MEMBERS_LIMIT`, `ROOM_MEMBERS_CALL_LIMIT` are never checked, unlike `MemberListTimeout` (`main.go:157`) and `RestrictedRoomMinMembers` (`:161`). `MAX_ROOM_SIZE=0` silently makes `expandChannelRefs` use `listLimit=1`.
- **[low] Six `$lookup` sites lack the required inline justification** — `room-service/store_mongo.go:289,302,317,656,673,1617` — five other sites (`:1395,:1453,:1543,:1678,:1693`) carry `// $lookup justification:` correctly.
- **[nitpick] `var ctx context.Context = c` appears 24×** — `room-service/handler.go:145` et al. — only one other occurrence exists in the repo; `user-service/service/me.go:18` passes `c` directly.
- **[nitpick] `c.WithLogValues` used once** — `room-service/handler.go:1739` — vs 28× in `user-service/service/`, so most room-service router log lines lack account/roomID.
- **[nitpick] Stale doc comment** — `room-service/helper.go:105` — says `errAppAccessDenied` covers "neither a room member nor a platform admin", but `authorizeRoomAppRead` (`handler.go:2468`) checks only `model.IsRoomMember(sub)`.
- **[nitpick] `boundedReply` marshals twice** — `room-service/helper.go:227` — discards the body from `marshalBounded`, then the router re-marshals the same struct.

## Recommendations

- **[high]** Fix `GetRoom`/`GetRoomAppRead` to branch on `mongo.ErrNoDocuments` the way `GetSubscription` (`store_mongo.go:281`) does, returning a `model.ErrRoomNotFound`-style sentinel; then map that sentinel to `errRoomNotFound` at all six `GetRoom` call sites so every RPC returns 404, not 500, for a missing room.
- **[medium]** Convert the 21 bare `slog.*` calls in `handler.go` to their `*Context` variants and normalize log keys to one casing (`roomId`/`siteId` are the repo-wide majority). A `forbidigo` rule on `slog.Error(`/`slog.Warn(` in handler files prevents regression.
- **[medium]** Add explicit projections to `GetUser`, `GetApp`, `FindDMSubscription`, `GetTeamsMeeting` and `getRoomSubscriptions`, mirroring the `roomReadProjection`/`subscriptionReadProjection` pattern already in the file, and extend the existing projection-drift integration test to cover them.
- **[medium]** Pass `&h.roomMembersLimit`/`&h.roomMembersCallLimit` (plus one, to detect overflow) as the `limit` argument in `handler_teams.go:62,128` so the cap is enforced at the query rather than after the full load.
- **[medium]** Handle the discarded marshal error at `handler.go:2207` — log via `slog.ErrorContext` or restructure as an explicit `if err != nil` — so a dropped canonical member event is observable.
- **[low]** Validate `MAX_ROOM_SIZE`, `MAX_BATCH_SIZE`, `ROOM_MEMBERS_LIMIT`, `ROOM_MEMBERS_CALL_LIMIT` (`> 0`) in `main.go` alongside the existing guards, and replace the four hardcoded `5*time.Second` deadlines with a fraction of `cfg.Guard.RequestTimeout` or a dedicated env var.
- **[low]** Add `json` tags to `ReadReceiptRow`/`RoomBotAppEntry`, add the missing `// $lookup justification:` comments at the six sites listed above, and correct the stale `errAppAccessDenied` comment at `helper.go:105`.
