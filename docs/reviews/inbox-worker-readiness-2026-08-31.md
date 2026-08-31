# inbox-worker — Production Readiness Review

**Service:** `inbox-worker` · **Date:** 2026-08-31 · **Branch:** `claude/production-readiness-report-hwcw8i`
**Method:** 6 independent expert agents scoring against `CLAUDE.md` and industry practice for Go microservices at scale.

## Overall score: 2.8 / 5

**The lowest-scoring service in the audit — and the most consequential position in the fleet**, since it is the destination side of *all* federation and the sole owner of the INBOX stream. It gets ownership exactly right: `bootstrap.go` sets only `Name + Subjects`, contains no gateway config, fail-fast-verifies in production, and every event type any producer emits is dispatched here. But the ordering guarantee the origin pays for is **thrown away at the destination**: `room_renamed` rides the origin's FIFO lane yet is routed to the concurrent fan-out pool here, so a rename can be applied before the subscription it renames exists — permanently. `subscription_opened` is applied with **no high-water-mark guard** despite its concurrent lane being justified on the claim that one exists. Coverage is **44.1%**, the worst in the fleet, with the entire store and all of `main()` at 0%.

| # | Dimension | Score |
|---|-----------|-------|
| 1 | Go code quality | 4 / 5 |
| 2 | Architecture | 3 / 5 |
| 3 | Test coverage | 1 / 5 |
| 4 | Maintainability | 3 / 5 |
| 5 | Integration | 3 / 5 |
| 6 | Performance | 3 / 5 |

| Severity | critical | high | medium | low | nitpick | Total |
|----------|---|---|---|---|---|---|
| Count | 1 | 13 | 20 | 12 | 6 | **52** |

---

## 2. Go code quality — 4 / 5

Error wrapping, logging discipline and guard documentation are consistently above average for this repo; deductions are a stringly-typed dispatch that silently drops events, one dead store method, a bare `return err`, and two log-and-return sites.

| Sev | Finding | Evidence |
|-----|---------|----------|
| medium | **`HandleEvent`'s dispatch uses raw string literals for 10 of 21 event types** while the other 11 use `model.Inbox*` constants that exist for every one of them. **The `default` branch returns `nil`, so `jsretry.Settle` Acks**: any drift between a publisher's constant and a literal here **silently discards a federated event** with only a `Warn`. There is no compile-time link — and `"room_sync"` has no constant at all, at either end | `handler.go:226-246`; `pkg/model/event.go:167-187` |
| medium | `CreateSubscription` is **dead**: declared on `InboxStore`, implemented against Mongo, and mocked, but no handler ever calls it. §3 requires the consumer-side store interface to carry "only the methods it needs" | `handler.go:23`; `main.go:132`; `mock_store_test.go:144` |
| medium | `CreateSubscription` returns a bare `err` from `InsertOne` with no context — the **only unwrapped store error in the file**, where every other method wraps precisely | `main.go:134` |
| medium | **Malformed-payload handling is inconsistent**: 19 of 22 `json.Unmarshal` failures return a plain wrapped error (transient → NAK), while 3 return `errcode.Permanent`. A payload that fails to parse **will never parse on redelivery**, so the transient sites burn the full `DefaultBackoff` budget (~12.6 min over `MaxDeliver`) before being dropped anyway | `handler.go:611`, `:633`, `:644` vs `:277`, `:377`, `:398`, `:455`, `:503` |
| low | Two handlers log **and** return the same failure, and `main.go` settles with `jsretry.Settle` (which logs the business error) — each poison event produces two log lines | `handler.go:463-465`, `:618-622` |
| low | `slog.Warn` (no `Context`) on the **unknown-event-type drop path** — the one log line emitted when an event is silently discarded is also the one line that loses trace correlation and `request_id` | `handler.go:271` |
| low | `main.go` is 1,046 lines and holds the entire Mongo store (~35 methods); the `InboxStore` interface lives in `handler.go`. Owned by D2/D4; noted here because it is why `main.go` mixes wiring with query construction | `main.go:68` |
| low | Audit-coverage gap (environmental): `govulncheck` + registry packs blocked | — |
| nitpick | `badge` and `valkey` are assigned post-construction even though `NewHandler` already accepts `HandlerOption`s; `fmt.Errorf` with no format verb where `errors.New` is idiomatic | `main.go:893-894`; `handler.go:186-192`, `:203` |

### Recommendations
- `medium` — Replace all 10 string literals in `HandleEvent` with the `model.Inbox*` constants; add `InboxRoomSync` to `pkg/model/event.go` and use it in **both** `inbox-worker` and the migration publisher, so the two sides cannot drift.
- `medium` — Delete `CreateSubscription` from `InboxStore`, the Mongo store and the test double, then `make generate SERVICE=inbox-worker`. If it survives instead, wrap its error.
- `medium` — Make every `json.Unmarshal` failure return `errcode.Permanent(errcode.BadRequest("unmarshal <type> payload"))`, matching the three sites that already do and the `broadcast-worker` pattern.
- `low` — Drop the two `slog.WarnContext` calls before a returned `Permanent`; change `handler.go:271` to `WarnContext` and include `evt.SiteID`, since that line is the **only trace of a dropped federated event**.
- `nitpick` — Split the store out of `main.go` into `store.go` + `store_mongo.go` (see Chapter 3); use the existing `HandlerOption`s for `badge`/`valkey`.

