# Federation-origin filter — migrate only locally-authored messages (Design)

> **Status:** IMPLEMENTED — follow-on to `oplog-connector` (stage 1, merged) + `oplog-transformer` (stage 2). Adds a `federation.origin` filter so each site's migration pump ingests **only messages authored at that site**. Connector `$match` + transformer origin-skip, always-on (no toggle).

*Each site runs its own connector + new stack. The source DB at a site holds both locally-authored messages and **foreign copies** received via legacy federation (carrying `federation.origin`). Migrating foreign copies would double-migrate them and collide with the new app's own cross-site federation. So we filter to local-origin and let the new app re-federate.*

---

## 1. The rule

**Migrate a message iff it has no `federation.origin`.** Field-absent ⇒ locally authored ⇒ keep. `federation.origin` present ⇒ foreign ⇒ drop.

Confirmed from the source: `federation.origin` is set once at creation, on remote-originated messages only, and never modified (`SOURCE_DATA.md`).

This is a **correctness** requirement, not just dedup: without it a foreign message would enter the new stack twice — once via this pump, once via the new app's federation from its home site.

## 2. Constraint — no pump latency

`fullDocument: "updateLookup"` would let the pump filter `update` events at the source, but it forces a **primary read inside the change-stream cursor per update** — latency on the throughput-critical, single-cursor pump. **Rejected.** The pump stays lookup-free; the update-origin check moves to the transformer, which already does that lookup.

## 3. Split design

| Op | Filtered where | Field source | Added cost |
|---|---|---|---|
| **insert / replace** | **pump** — `$match` | native `fullDocument` | none (no `updateLookup`) |
| **update** (edit + soft-delete `t:"rm"`) | **transformer** | the `FindByID` it *already* does for the edit | none (piggyback) |
| **hard delete** (`op=delete`) | not filtered | — (no doc) | accepted churn (§3.3) |

### 3.1 Pump — `$match` on native `fullDocument`

In `openMongoChangeSource` (`oplog-connector/source_mongo.go`): replace the empty `mongo.Pipeline{}` with a `$match`; **do not** call `SetFullDocument`.

```js
{ $match: { $or: [
    { operationType: { $nin: ["insert", "replace"] } },        // updates/deletes pass through
    { "fullDocument.federation.origin": { $exists: false } }   // foreign insert/replace dropped
] } }
```

- Drops foreign `insert`/`replace` before they ever enter `MIGRATION_OPLOG` (removes the bulk of foreign volume).
- `update`/`delete` pass through (they carry no `fullDocument` to match on).
- Fail-open: a `replace` with an unexpectedly-absent `fullDocument` passes (kept), never silently dropped. `$in: [null, ""]` matches absent, null, **and** empty origin — so the check is robust to the §9 open question either way.
- **Scope: the message collection only** (`config.MessageCollection`, env `MESSAGE_COLLECTION`, default `rocketchat_message`). The filter is applied per-watcher in `main.go` (`coll == cfg.MessageCollection`); other watched collections (`rooms`, `users`, …) are forwarded **unfiltered**, so a foreign room/user is never silently dropped before its migration path exists. `federation.origin` is a message-scoped concern for stage 2; extending the filter to other collections is a deliberate future decision, not an accident of a presence-based match.

### 3.2 Transformer — piggyback the existing update lookup

`handleUpdate` already calls `FindByID` to fetch the current source doc (it needs `roomId`/`createdAt`). That doc carries `federation.origin`.

- Add `Federation struct { Origin string \`bson:"origin"\` } \`bson:"federation"\`` to `rocketchatMessage` (`messagemap.go`).
- In `applyUpdate` (after decode), if `rc.Federation.Origin != ""` → **skip-and-Ack** with `onSkipped{foreign_origin}`. No history RPC. Zero added query — the lookup already happened.
- **Defense-in-depth (recommended):** also check origin on the **insert** path (`handleInsert` already decodes the full doc) so a foreign insert that ever slips the `$match` is skipped rather than ingested.

### 3.3 Hard delete — unfiltered (accepted)

`op=delete` carries only `documentKey._id`; the source doc is gone, so origin is unknowable without pre-images (which would tax the legacy source's write path — declined). A foreign hard-delete targets a never-migrated row → `GetMessageByID` nil → `NotFound` → Nak → at the **shorter `DELETE_MAX_DELIVER` cap** (default 60 ≈ 2 min, vs the global `MAX_DELIVER` 1000 ≈ 33 min) it is **Term'd with `oplog_transformer_exhausted_total{op="delete"}`** (no longer a silent JetStream drop — `processOne`/`isFinalDelivery`). **No duplicate, no corruption** — bounded, now-observable churn, ~16× cheaper than the global cap. Tradeoff: a *local* hard-delete whose insert takes longer than the cap to persist would be Term-dropped; tune `DELETE_MAX_DELIVER` up if message-worker insert lag can exceed it. Soft-deletes (the common case, `t:"rm"` as updates) **are** filtered via §3.2, so only rare hard-deletes churn.

## 4. Checkpoint advancement under `$match` (known limitation — accepted, not fixed in this PR)

With a `$match`, `Next()` returns only **matching** events. During a long run of foreign-only inserts (all filtered), no event is delivered → the resume token doesn't advance → the checkpoint stalls → on restart the connector replays from an older token (deduped — **no loss**, just rescan; worst case ≈ the §3.3 reseed window).

- **Status:** **Accepted, NOT fixed in this PR.** It is a **connector-internal** concern (the checkpoint/resume-token path), out of scope for the federation-filter change, which only adds the `$match`.
- **Cause:** the `$match` makes the cursor surface only matching events, so a foreign-only burst stalls the resume-token frontier.
- **Why it's safe:** dedup-protected (`Nats-Msg-Id` = change-stream `_id._data`), so the worst case is **replay, not loss**.
- **Mitigation already in place:** the filter is **scoped to one collection** (`MESSAGE_COLLECTION`); other watchers advance normally and bound the blast radius.
- **Future connector change (not here):** advance the checkpoint from the change stream's **post-batch resume token (PBRT)** even when no event is emitted — e.g. a `max-await` + `TryNext` loop that periodically surfaces the PBRT, recorded via the existing time-based flush (`CHECKPOINT_MAX_AGE`). This is a connector-internal follow-up, separate from this PR.

## 5. What does NOT change

- **No `updateLookup`, no pre-images** — the pump's "no lookups" principle holds (a `$match` is a subscription filter, not a lookup).
- Hard-delete handling, and the transformer's Ack/Nak/Term disposition, are unchanged except for the new `foreign_origin` skip.
- The wire envelope (`model.OplogEvent`) is unchanged.

## 6. Observability

- Transformer: `onSkipped{reason="foreign_origin"}` counter on every foreign-update (and foreign-insert defense) skip.
- Pump: events dropped by the `$match` never reach `Next()`, so the connector **cannot count them** directly — a `$match` filters silently. The PBRT/lag is the pump-side signal that it's progressing; document this so operators don't expect a "filtered" counter from the pump.
- Foreign hard-delete churn → `oplog_transformer_exhausted_total{op}` + an ERROR log at the delivery cap.

## 7. Testing

- **Connector (integration):** foreign `insert` is **not** published; local `insert` is; `update`/`delete` pass the `$match` regardless of origin; the checkpoint advances across a foreign-only burst (PBRT).
- **Transformer (unit):** foreign-origin update → `onSkipped{foreign_origin}`, no history call; local update → edit/delete as today; (defense) foreign insert → skip; non-foreign insert → published.

## 8. Docs to update (same PR as the impl)

- `data-migration/README.md` — connector "at a glance": document the `federation.origin` `$match` (carve out the "all ops … identically" / "no field-level filtering" wording); **keep** "No lookups" (still true). Transformer "at a glance": add the foreign-origin skip.
- `docs/superpowers/specs/2026-06-08-oplog-connector-design.md` (merged) — note the `$match` exception to "forwards everything."

## 9. Open

- **Confirm with the source-data team:** is `federation.origin` *absent* the only marker of local authorship, or can a local message carry an empty/null `origin`? (Resolves the `$exists:false` check; ties into backlog item E.) If empty-string origins exist locally, the `$match` and the transformer check must use "absent **or** empty," not just `$exists`.

## 10. Rollout

Per-site: every site runs the connector and migrates only its own-origin messages; cross-site delivery is the new app's federation, not the pump. The filter is what makes "deploy the pump at every site" safe — without it, every site re-migrates every other site's messages.
