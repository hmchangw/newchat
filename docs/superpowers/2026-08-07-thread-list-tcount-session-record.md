# Session record: `ThreadListItem.tcount` — thread.list reply count

**Date:** 2026-08-07
**Branch:** `claude/thread-reply-count-rpc-7wk4or` (base `main` @ `9a9f764`)
**Purpose of this file:** a complete, self-contained account of what was changed, how, and why — written so the outcome can be compared line-by-line against the equivalent change produced by the internal agent in the legacy (tchat2, monolith) repository.

---

## 1. The ask

Return the number of replies (thread messages) per thread in the thread-list RPC — the cross-site thread inbox `chat.user.{account}.request.user.{siteID}.thread.list`.

## 2. What existed before (the trace)

The count already existed in the response — but only buried inside an opaque payload, not as a field:

```
client
  └─ user-service  ListUserThreads                     user-service/service/threads.go
       ├─ fan-out over ALL_SITE_IDS (local direct, remote concurrent, bounded)
       │    └─ historyclient.GetThreadList             chat.server.request.thread.{siteID}.subscription.list
       └─ merge → sort (lastMsgAt, threadRoomId) DESC → enrichThreadPage (HRInfo / botDM name only)

history-service  ListThreadSubscriptions  (per-site leaf)
  ├─ Mongo keyset page: thread_subscriptions ⋈ subscriptions ⋈ thread_rooms  → ThreadSubRow keys
  └─ buildThreadItems:
       GetMessagesByIDs(dedup(parents ∪ lastMsgs))     ONE Cassandra batch, ≤16 in flight
       → per row: item + pre-marshaled ParentMessage/LastMessage (json.RawMessage, never decoded upstream)
```

`tcount` lives on the Cassandra parent row (`messages_by_id.tcount`, also mirrored in `messages_by_room`), maintained by the two authoritative writers — message-worker (reply add) and history-service (reply delete) — through the shared `pkg/threadcount` helper (bounded soft-delete-aware partition scan, `Cap = 99`).

## 3. The diagnosis (why "it's already in parentMessage" wasn't enough)

1. **The carrier is opaque by design.** `ParentMessage` is `json.RawMessage`: history-service pre-marshals it and user-service forwards the bytes verbatim, never decoding (decoding would hit `cassandra.Message.Reactions`, a struct-keyed map with no JSON decoder). No server stage could read or act on the count; Go consumers needed projection decodes.
2. **Both hops were documented Optional** (`parentMessage` optional on the item; `tcount` optional on Message) — so per contract the count could legally be absent, though a thread-inbox row only exists because replies exist.
3. **It genuinely was absent for migrated threads.** The oplog transformer writes `thread_rooms`/`thread_subscriptions` to Mongo and never touches Cassandra, so the parent's `tcount` column was never written to Cassandra → `*int` + `omitempty` dropped the key. Clients couldn't distinguish *unknown* from *zero*.
4. **The 99-cap semantics were undocumented at thread-list level.**
5. **Nothing pinned the count across the aggregator** — one leaf test asserted it; no user-service or model test did.

## 4. Cross-check against the legacy repo's suggestions

Three suggestions were relayed from the legacy (monolith) agent; each was verified against this codebase before adoption:

| Legacy suggestion | Verdict here | Why |
|---|---|---|
| `tcount` lives on the Message document (Cassandra `messages_by_id.tcount`) | **Confirmed** | Matches the trace. Key difference: in the monolith the message doc was in **Mongo**, same DB as the subscription rows. |
| Join the parent message in the aggregation pipeline (`$lookup`), then a `TransformToThreadSubscription` step converts it | **Right shape, wrong mechanism — the equivalent already existed in code** | Messages here are **Cassandra-only**; there is no production Mongo `messages` collection to `$lookup` (the only one in-repo is a dev seeder). Mongo cannot join across engines. The aggregation returns row *keys*; the join happens in Go one hop later: `threadListLookupMsgIDs` (dedup parents ∪ lasts) → `GetMessagesByIDs` (token-aware single-partition point reads, ≤16 in flight — deliberately not an `IN` scatter). `buildThreadItems` *is* the transform step. This is cheaper than the legacy per-row lookup: one bounded batch per page, duplicates fetched once. CLAUDE.md also bans new unjustified `$lookup`s. |
| Stay with `tcount`, not `replyCount` | **Adopted** | The ecosystem is already tcount-shaped: `docs/client-api.md` Message schema, frontend `types.ts` (`tcount?: number`, the "💬 N replies" badge), optimistic reducer bumps, and the `newTcount` broadcast field. A second name for the same number buys nothing; the frontend type being optional makes an always-present item field purely additive. |

## 5. The design decision

**Lift the count onto the item.** `ThreadListItem` gains:

```go
TCount int `json:"tcount" bson:"tcount"`   // no omitempty — zero serializes
```

populated in `buildThreadItems` from the parent the page already hydrates:

```go
if parent.TCount != nil {
    item.TCount = *parent.TCount
}
```

Properties, all deliberate:

- **Zero added reads / writes / schema changes** — the value was already fetched (`tcount` is in the Cassandra `baseColumns` projection); the change is a nil-check plus an 8-byte copy per row.
- **Plain `int`, always present** — kills the client's `undefined` branch. Cost knowingly accepted: `0` conflates "all replies deleted", "migrated thread (column never written to Cassandra)", and a transient just-created-thread window; the migration README already accepted stale/zero counts for migrated data.
- **Cap passes through untouched** — `pkg/threadcount.Cap` (= 99, "99 or more"); no arithmetic anywhere in the change.
- **`parentMessage.tcount` retained** — backward compatibility; removing it would break existing readers.
- **user-service production code untouched** — the aggregator copies items verbatim; tests pin that property instead of code enforcing it.
- **Item-level `tcount` and embedded `parentMessage.tcount` cannot disagree within one row/version** — both come from the same in-memory `parent` value in `buildThreadItems`.

**Rejected alternatives** (recorded in the spec):

- *Denormalize a count onto `thread_rooms` (Mongo)* — the closest true port of the legacy "get it from the aggregation" approach, and uncapped; rejected because it adds a write to the reply hot path, creates a second source of truth that can drift from `tcount`, and needs a backfill.
- *Count from Cassandra at read time* — up to ~100 bounded partition scans per page per site; precisely what the cached `tcount` exists to avoid.

## 6. How it was executed

Four-task TDD plan (each task Red → Green → lint → commit, reviewed by an independent reviewer agent before the next task started; fresh implementer agent per task; one fix-round loop available per task — none was needed):

| # | Commit | Content | Red evidence style |
|---|---|---|---|
| 1 | `705fc14` | `TCount` field + model tests (`pkg/model/threadlist.go`, `threadlist_test.go`): round-trip with `TCount: 7`; `TestThreadListItemJSON_ZeroTCountSerialized` pins the key present and `== 0` when zero | Compile failure, **plus** a deliberate wrong-tag run (`omitempty` inserted then removed) proving the zero-serialization test is non-vacuous |
| 2 | `8f2c53a` | The lift in `buildThreadItems` + tests: extended `_Success` (items 0/1 → 4/0) and table test `_TCountFromParent` over `intPtr(4)→4`, `nil→0`, `intPtr(0)→0`, `intPtr(99)→99` | New assertions failed with 0-where-4/99-expected before the implementation existed |
| 3 | `a1939ad` | user-service regression pins (test-only; verified only `threads_test.go` changed): `_TCountSurvivesAggregation` — rows from two sites, interleaved `lastMsgAt` forcing a genuine cross-site re-sort, one row per enrichment branch (channel untouched / dm gains `HRInfo` / botDM `RoomName` rewritten), each with `TCount` asserted intact; `_TCountSurvivesDegradedEnrichment` — both lookups erroring, counts intact | Green-on-write by design (regression pin), so a mandated **mutation check**: all five TCount assertions mutated to 999, each observed failing, restored |
| 4 | `59823f4` | Docs: `client-api.md` ThreadListItem table row + both JSON examples + softened `parentMessage` note; design-doc supersede bullets; spec status flip. Derived views verified as link-only (no edit needed) | n/a (docs) |

Then a **four-lens parallel review panel** over the whole branch (spec alignment / code quality + comment discipline / adversarial bug hunt / dead-duplicated-unnecessary code), which found **zero code defects** and produced one consolidated fix wave:

| 5 | `3a838dc` | Fix wave (12 sub-items, all verified by a scoped re-review): design-doc §5 self-contradictory parenthetical reworded; §6 struct snippet synced field-for-field with the real struct (it had drifted on 3 fields); **mixed-version rollout sentence** added to the client-api `tcount` row; second JSON example given hydrated bodies (its previous shape — `tcount` with no `parentMessage` — is unemittable, since `buildThreadItems` drops rows that don't hydrate both); `TCount` godoc compressed 4→2 lines and the test comment 3→2 (comment rule: ≤2 lines preferred); test style (use in-scope `first`/`second` locals; reuse `dmItem`/`botDMItem` helpers; `helper.bot` spelling); **new test dimension** `wantParentKey` pinning the one intra-row divergence — parent `tcount` key *absent* when the column is nil vs *present with 0* when explicitly zero (guards `cassandra.Message.TCount`'s `omitempty` contract); spec annotations (pre-change-snapshot note, open question 1 resolved) |

The panel's most valuable find (bug-hunt lens): **mixed-version federation** — during a rolling deploy, a not-yet-upgraded leaf site's rows decode to `tcount: 0` at the new aggregator, and an old aggregator drops the key from a new leaf. Judged a docs/rollout note (not a code change) because `*int` had been deliberately rejected; now documented in the client-api row.

## 7. Final verification

- `make test SERVICE=pkg/model`, `SERVICE=history-service`, `SERVICE=user-service` — green, race detector on (Makefile default).
- `make lint` — 0 issues.
- Coverage on the touched packages: `history-service/internal/service` **93.3%**, `user-service/service` **93.7%** (target ≥90% for handlers); the new production block executes 4× in the profile.
- `make sast`: gosec **PASS (0 findings)**; repo-local semgrep rules **0 findings / 516 files**; govulncheck and the semgrep registry rulesets could not run in the sandbox (egress-denied) — **re-run `make sast` on CI before merge**.
- A 7-agent branch review (per-service generalists for history-service and user-service + Go / test-automation / bug-security / performance / observability lenses) returned **0 critical, 0 high, 0 medium**; residual lows and nitpicks are listed below.

## 8. Final state of the branch

```
11f0e96  docs: design sketch (problem diagnosis, fix proposal)
2c9c294  docs: legacy-repo suggestions reviewed into the sketch
84ada9b  docs: Cassandra-join rationale + aggregator test plan sharpened
5c6fcf0  docs: implementation plan (4 tasks)
705fc14  feat(model): ThreadListItem.tcount
8f2c53a  feat(history-service): populate tcount from the hydrated parent
a1939ad  test(user-service): pin tcount pass-through across aggregation
59823f4  docs(client-api): document ThreadListItem.tcount
3a838dc  docs,test: final-review fixes
```

Files: `pkg/model/threadlist.go` (+field, comment updates), `history-service/internal/service/threads.go` (+5 lines), three test files, `docs/client-api.md`, `docs/design/user-thread-list.md`, plus the spec and plan under `docs/superpowers/`.

## 9. Known limitations & open follow-ups (all deliberate, none blocking)

1. **`0` is overloaded**: all-replies-deleted, migrated-thread (never written), transient just-created-thread mid-write, and old-leaf-during-rollout all read `0`. Chosen over `*int` to keep the field unconditional. A backfill job (recompute per `thread_rooms` row via `pkg/threadcount`) would fix the migrated case — follow-up ticket material.
2. **Old aggregator + new leaf** drops the key (unknown field) — self-heals when the aggregator upgrades; documented.
3. Unpinned by tests: decoding leaf JSON that *lacks* the `tcount` key (Go zero-value makes the documented old-leaf behavior trivially true; a one-line unmarshal pin would guard it).
4. Nitpick-level polish left as-is: `TCount` godoc lines run ~113/127 chars (file norm ~85; no lll linter); the aggregation test's "channel" row is untyped (`RoomType: ""` — provably same enrichment path; typed-channel arm covered by a neighbouring test); `search-service/integration_index_test.go` has pre-existing gofmt drift unrelated to this branch.

## 10. Comparison checklist vs the legacy implementation

When diffing against the legacy (tchat2) agent's output, compare on these axes:

| Axis | This repo's answer |
|---|---|
| Field name / placement | `tcount` on the list item itself (plus the pre-existing copy inside `parentMessage`) |
| Type / presence | plain `int`, always serialized; `0` = none-or-unknown |
| Source of truth | Cassandra `messages_by_id.tcount`, maintained by the two writers via `pkg/threadcount`; **not** recomputed and **not** denormalized into Mongo |
| Join mechanism | in-code batch (`GetMessagesByIDs`, deduped parents ∪ lasts, ≤16 concurrent point reads) — *not* a `$lookup`; Mongo agg returns keys only |
| Cap | 99 pass-through ("99 or more"); no arithmetic in this change |
| Cross-site | leaf computes, aggregator forwards verbatim (pinned by tests, not code) |
| Migrated data | reads `0` (accepted; backfill possible later) |
| Docs | client-api field table + examples + mixed-version note; derived views untouched by design (link-only) |
| Test surface | 3 packages: model round-trip + zero-serialization; leaf table test incl. parent-body key-presence divergence; aggregator merge/sort/enrich + degraded-path pins |
